#!/usr/bin/env bash
# 새 Ubuntu 24.04 EC2 에서 1회 실행 (push.py 가 호출). 멱등: 다시 돌려도 안전.
#   bash deploy/setup-server.sh [PUBLIC_HOST] [ADMIN_ALLOW_CIDR]
# PUBLIC_HOST 우선순위: 인자 > 환경변수 PUBLIC_HOST > 기존 /etc/caddy/env > <EIP를 -로 바꾼 값>.sslip.io
# ADMIN_ALLOW_CIDR: /state, /admin/* 를 외부에서 허용할 운영자 CIDR (기본 127.0.0.1/32 = 외부 차단, 서버 로컬만)
#
# 하는 일
#   1) python3-venv 설치, .venv 생성, requirements.txt 설치
#   2) Caddy 공식 apt 저장소(cloudsmith) 등록 후 apt-get 설치
#   3) /etc/caddy/env 에 PUBLIC_HOST 기록 + caddy.service 드롭인(EnvironmentFile) → Caddyfile 의 {$PUBLIC_HOST} 치환
#   4) /etc/caddy/Caddyfile, /etc/systemd/system/lake-executor.service 설치, daemon-reload, caddy enable
#   lake-executor 서비스는 시작하지 않는다 (.env/config.json 업로드 후 finalize.py 가 시작).
set -euo pipefail

APP_DIR="/home/ubuntu/lake-executor"
APP_USER="ubuntu"
CADDY_ENV="/etc/caddy/env"
export DEBIAN_FRONTEND=noninteractive

cd "$APP_DIR"

# ---------- PUBLIC_HOST 결정 ----------
PUBLIC_HOST="${1:-${PUBLIC_HOST:-}}"
if [ -z "$PUBLIC_HOST" ] && [ -f "$CADDY_ENV" ]; then
  PUBLIC_HOST="$(sed -n 's/^PUBLIC_HOST=//p' "$CADDY_ENV" | head -n1 || true)"
fi
if [ -z "$PUBLIC_HOST" ]; then
  # IMDSv2 로 공인 IP 조회, 실패 시 외부 서비스
  TOKEN="$(curl -s -m 3 -X PUT http://169.254.169.254/latest/api/token \
            -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' || true)"
  EIP="$(curl -s -m 3 -H "X-aws-ec2-metadata-token: $TOKEN" \
            http://169.254.169.254/latest/meta-data/public-ipv4 || true)"
  if [ -z "$EIP" ]; then
    EIP="$(curl -s -m 10 https://checkip.amazonaws.com | tr -d '[:space:]' || true)"
  fi
  if [ -z "$EIP" ]; then
    echo "!! cannot determine public IP; pass PUBLIC_HOST as argument" >&2
    exit 1
  fi
  PUBLIC_HOST="${EIP//./-}.sslip.io"
fi
echo ">> PUBLIC_HOST=$PUBLIC_HOST"

ADMIN_ALLOW_CIDR="${2:-${ADMIN_ALLOW_CIDR:-}}"
if [ -z "$ADMIN_ALLOW_CIDR" ] && [ -f "$CADDY_ENV" ]; then
  ADMIN_ALLOW_CIDR="$(sed -n 's/^ADMIN_ALLOW_CIDR=//p' "$CADDY_ENV" | head -n1 || true)"
fi
ADMIN_ALLOW_CIDR="${ADMIN_ALLOW_CIDR:-127.0.0.1/32}"
echo ">> ADMIN_ALLOW_CIDR=$ADMIN_ALLOW_CIDR (/state, /admin/* reachable only from here)"

# ---------- 1) python ----------
echo ">> apt: python venv + base tools"
sudo apt-get update -y -q
sudo apt-get install -y -q python3-venv python3-pip curl ca-certificates gnupg \
  debian-keyring debian-archive-keyring apt-transport-https

if [ ! -x .venv/bin/python ]; then
  python3 -m venv .venv
fi
./.venv/bin/pip install -q --upgrade pip
./.venv/bin/pip install -q -r requirements.txt
mkdir -p state
chmod 700 state
echo ">> venv ready: $(./.venv/bin/python --version)"

# ---------- 2) Caddy (official apt repo via cloudsmith) ----------
if ! command -v caddy >/dev/null 2>&1; then
  echo ">> installing Caddy"
  curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' \
    | sudo gpg --dearmor --yes -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
  curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' \
    | sudo tee /etc/apt/sources.list.d/caddy-stable.list >/dev/null
  sudo chmod o+r /usr/share/keyrings/caddy-stable-archive-keyring.gpg
  sudo chmod o+r /etc/apt/sources.list.d/caddy-stable.list
  sudo apt-get update -y -q
  sudo apt-get install -y -q caddy
fi
echo ">> caddy: $(caddy version | head -n1)"

# ---------- 3) /etc/caddy/env + drop-in ----------
sudo mkdir -p /etc/caddy /var/log/caddy
printf 'PUBLIC_HOST=%s\nADMIN_ALLOW_CIDR=%s\n' "$PUBLIC_HOST" "$ADMIN_ALLOW_CIDR" | sudo tee "$CADDY_ENV" >/dev/null
sudo chmod 644 "$CADDY_ENV"
sudo mkdir -p /etc/systemd/system/caddy.service.d
sudo tee /etc/systemd/system/caddy.service.d/10-env.conf >/dev/null <<'EOF'
[Service]
EnvironmentFile=-/etc/caddy/env
EOF
sudo chown caddy:caddy /var/log/caddy 2>/dev/null || true

# ---------- 4) Caddyfile + systemd units ----------
sudo cp "$APP_DIR/deploy/Caddyfile" /etc/caddy/Caddyfile
sudo chmod 644 /etc/caddy/Caddyfile
echo ">> validating Caddyfile"
PUBLIC_HOST="$PUBLIC_HOST" ADMIN_ALLOW_CIDR="$ADMIN_ALLOW_CIDR" caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile

sudo cp "$APP_DIR/deploy/lake-executor.service" /etc/systemd/system/lake-executor.service
sudo chmod 644 /etc/systemd/system/lake-executor.service
sudo chown -R "$APP_USER:$APP_USER" "$APP_DIR"

sudo systemctl daemon-reload
sudo systemctl enable caddy >/dev/null 2>&1 || true
# PUBLIC_HOST/드롭인 반영 (인증서 발급을 미리 시작해 둔다; 백엔드가 없는 동안은 502)
sudo systemctl restart caddy || true

echo
echo "==== SETUP DONE ===="
echo "public host : $PUBLIC_HOST"
echo "signal URL  : https://$PUBLIC_HOST/lake/signal"
echo "lake-executor service installed but NOT started. Upload .env/config.json and run deploy/finalize.py."
