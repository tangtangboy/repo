#!/usr/bin/env bash
# lake-executor 자체 배포(GitOps pull): 서버가 GitHub 를 보고 새 커밋이 있으면 스스로 받아 검증 후 재시작한다.
#
#   systemd 가 돌린다: lake-executor-update.timer (2분 주기) / lake-executor-update.path (state/DEPLOY_NOW 생성 시 즉시)
#   수동:              sudo systemctl start lake-executor-update        또는  touch state/DEPLOY_NOW
#   설정:              /etc/lake-executor/deploy.env
#                        DEPLOY_REPO_URL=https://github.com/<owner>/<repo>.git   (비공개면 deploy key/토큰 URL)
#                        DEPLOY_BRANCH=                                        (비우면 원격 기본 브랜치를 따라간다)
#                        DEPLOY_RUN_TESTS=1                                    (1 = pytest 통과해야 배포)
#                        DEPLOY_ENABLED=1                                      (0 = 아무것도 하지 않음)
#   상태:              state/last_deploy.json (대시보드 Overview/Controls 가 읽는다), journalctl -u lake-executor-update
#
# 절차: fetch → 새 커밋이면 reset --hard → (requirements 바뀌면) pip → compileall → pytest → `lake_executor check`
#       → (deploy/* 바뀌면) setup-server.sh → systemctl restart lake-executor → /healthz 확인.
#       어느 단계든 실패하면 직전 커밋으로 되돌리고(rollback) 서비스를 다시 올린 뒤 실패를 기록·알림한다.
# 보존: .env / config.json / state/ / .venv 는 git 이 무시하므로 reset --hard 에 영향받지 않는다.
set -uo pipefail

APP_DIR="/home/ubuntu/lake-executor"
DEPLOY_ENV="/etc/lake-executor/deploy.env"
STATE_DIR="$APP_DIR/state"
STATUS_FILE="$STATE_DIR/last_deploy.json"
FLAG_FILE="$STATE_DIR/DEPLOY_NOW"
LOCK_FILE="$STATE_DIR/.update.lock"
LOG_FILE="$STATE_DIR/last_deploy.log"
SERVICE="lake-executor"
PY="$APP_DIR/.venv/bin/python"
PIP="$APP_DIR/.venv/bin/pip"

DEPLOY_REPO_URL=""
DEPLOY_BRANCH=""
DEPLOY_RUN_TESTS="1"
DEPLOY_ENABLED="1"
# shellcheck disable=SC1090
[ -f "$DEPLOY_ENV" ] && . "$DEPLOY_ENV"

mkdir -p "$STATE_DIR"
cd "$APP_DIR" || exit 1

TRIGGER="timer"
if [ -f "$FLAG_FILE" ]; then
  TRIGGER="manual"
  rm -f "$FLAG_FILE"          # path 유닛이 다시 발화하지 않도록 잠금 전에 지운다
fi
[ "${1:-}" = "--manual" ] && TRIGGER="manual"

if [ "$DEPLOY_ENABLED" != "1" ]; then
  echo "auto-deploy disabled (DEPLOY_ENABLED=$DEPLOY_ENABLED)"
  exit 0
fi
if [ -z "$DEPLOY_REPO_URL" ]; then
  echo "DEPLOY_REPO_URL not set in $DEPLOY_ENV; nothing to do"
  exit 0
fi

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "another update run is in progress; skipping"
  exit 0
fi

START_TS=$(date +%s)
: > "$LOG_FILE"
log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$LOG_FILE"; }

# ---------- 상태 파일 (대시보드가 읽음). 이전 deployed_* 는 noop 때 유지 ----------
write_status() {   # result step message [deployed_commit] [deployed_subject]
  RESULT="$1" STEP="$2" MESSAGE="$3" DEPLOYED_COMMIT="${4:-}" DEPLOYED_SUBJECT="${5:-}" \
  BRANCH="${BRANCH:-}" HEAD_COMMIT="${CURRENT:-}" TARGET_COMMIT="${TARGET:-}" TRIGGER="$TRIGGER" \
  START_TS="$START_TS" STATUS_FILE="$STATUS_FILE" LOG_FILE="$LOG_FILE" "$PY" - <<'PY'
import json, os, time
p = os.environ["STATUS_FILE"]
try:
    prev = json.load(open(p, encoding="utf-8"))
except Exception:
    prev = {}
now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
cur = {
    "checked_at": now,
    "trigger": os.environ.get("TRIGGER", ""),
    "branch": os.environ.get("BRANCH", ""),
    "head": os.environ.get("HEAD_COMMIT", ""),
    "target": os.environ.get("TARGET_COMMIT", ""),
    "result": os.environ["RESULT"],
    "step": os.environ.get("STEP", ""),
    "message": os.environ.get("MESSAGE", ""),
    "duration_s": int(time.time()) - int(os.environ.get("START_TS") or time.time()),
    "deployed_commit": prev.get("deployed_commit", ""),
    "deployed_subject": prev.get("deployed_subject", ""),
    "deployed_at": prev.get("deployed_at", ""),
    "last_failure_at": prev.get("last_failure_at", ""),
}
if os.environ["RESULT"] == "deployed":
    cur["deployed_commit"] = os.environ.get("DEPLOYED_COMMIT", "")
    cur["deployed_subject"] = os.environ.get("DEPLOYED_SUBJECT", "")
    cur["deployed_at"] = now
if os.environ["RESULT"] in ("failed", "rolled_back"):
    cur["last_failure_at"] = now
try:
    lines = open(os.environ["LOG_FILE"], encoding="utf-8", errors="replace").read().splitlines()
except Exception:
    lines = []
cur["log_tail"] = lines[-40:]
tmp = p + ".tmp"
with open(tmp, "w", encoding="utf-8") as f:
    json.dump(cur, f, ensure_ascii=False, indent=1)
os.replace(tmp, p)
PY
}

# ---------- 텔레그램 알림 (.env 의 TELEGRAM_* 가 있을 때만) ----------
notify() {   # text
  MSG="$1" ENV_FILE="$APP_DIR/.env" "$PY" - <<'PY' >/dev/null 2>&1 || true
import os, urllib.parse, urllib.request
env = {}
try:
    for line in open(os.environ["ENV_FILE"], encoding="utf-8"):
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
except Exception:
    pass
tok, chat = env.get("TELEGRAM_BOT_TOKEN"), env.get("TELEGRAM_CHAT_ID")
if tok and chat:
    data = urllib.parse.urlencode({"chat_id": chat, "text": os.environ["MSG"][:3500]}).encode()
    urllib.request.urlopen(f"https://api.telegram.org/bot{tok}/sendMessage", data=data, timeout=10)
PY
}

fail() {   # step message  → 롤백 후 종료
  local step="$1" msg="$2"
  log "!! $step failed: $msg"
  if [ -n "${CURRENT:-}" ] && [ "$CURRENT" != "none" ] && [ "$(git rev-parse HEAD 2>/dev/null)" != "$CURRENT" ]; then
    log "rolling back to $CURRENT"
    git reset -q --hard "$CURRENT" && git clean -fdq lake_executor tests tools deploy || true
    if [ "${REQ_CHANGED:-0}" = "1" ]; then
      timeout 600 "$PIP" install -q -r requirements.txt >>"$LOG_FILE" 2>&1 || log "pip rollback install failed"
    fi
    sudo systemctl restart "$SERVICE" || true
    write_status "rolled_back" "$step" "$msg (rolled back to ${CURRENT:0:7})"
    notify "[deploy] FAILED at $step on $(hostname): $msg — rolled back to ${CURRENT:0:7}"
  else
    write_status "failed" "$step" "$msg"
    notify "[deploy] FAILED at $step on $(hostname): $msg"
  fi
  exit 1
}

# ---------- git 준비 (처음 한 번은 SFTP 사본을 git 체크아웃으로 바꾼다) ----------
if [ ! -d .git ]; then
  log "bootstrap: turning $APP_DIR into a git checkout of $DEPLOY_REPO_URL"
  git init -q || fail "bootstrap" "git init failed"
  git config --local core.autocrlf false
  git remote add origin "$DEPLOY_REPO_URL" || fail "bootstrap" "git remote add failed"
  TRIGGER="bootstrap"
fi
if [ "$(git remote get-url origin 2>/dev/null)" != "$DEPLOY_REPO_URL" ]; then
  git remote set-url origin "$DEPLOY_REPO_URL"
fi

if ! timeout 120 git fetch -q --prune origin >>"$LOG_FILE" 2>&1; then
  CURRENT="$(git rev-parse HEAD 2>/dev/null || echo none)"
  write_status "failed" "fetch" "git fetch failed (network/auth?)"
  log "git fetch failed"
  exit 1
fi

BRANCH="$DEPLOY_BRANCH"
if [ -z "$BRANCH" ]; then
  BRANCH="$(timeout 60 git ls-remote --symref origin HEAD 2>/dev/null | awk '/^ref:/ {sub("refs/heads/", "", $2); print $2; exit}')"
fi
[ -z "$BRANCH" ] && BRANCH="main"
if ! git rev-parse -q --verify "origin/$BRANCH" >/dev/null 2>&1; then
  CURRENT="$(git rev-parse HEAD 2>/dev/null || echo none)"
  write_status "failed" "fetch" "branch origin/$BRANCH not found"
  log "branch origin/$BRANCH not found"
  exit 1
fi

TARGET="$(git rev-parse "origin/$BRANCH")"
CURRENT="$(git rev-parse HEAD 2>/dev/null || echo none)"
if [ "$TARGET" = "$CURRENT" ]; then
  write_status "noop" "fetch" "up to date at ${CURRENT:0:7} ($BRANCH)"
  exit 0
fi

SUBJECT="$(git log -1 --format=%s "$TARGET" 2>/dev/null | cut -c1-120)"
log "deploying $BRANCH: ${CURRENT:0:7} -> ${TARGET:0:7} ($SUBJECT) [trigger=$TRIGGER]"

REQ_CHANGED=0
DEPLOY_CHANGED=0
if [ "$CURRENT" = "none" ]; then
  REQ_CHANGED=1
  DEPLOY_CHANGED=1
else
  if ! git diff --quiet "$CURRENT" "$TARGET" -- requirements.txt; then REQ_CHANGED=1; fi
  if ! git diff --quiet "$CURRENT" "$TARGET" -- deploy/Caddyfile deploy/lake-executor.service deploy/setup-server.sh \
        deploy/lake-executor-update.timer deploy/lake-executor-update.path; then
    DEPLOY_CHANGED=1
  fi
fi

git reset -q --hard "$TARGET" || fail "checkout" "git reset --hard failed"
git clean -fdq lake_executor tests tools deploy || true

if [ "$REQ_CHANGED" = "1" ]; then
  log "requirements.txt changed: pip install"
  timeout 600 "$PIP" install -q -r requirements.txt >>"$LOG_FILE" 2>&1 || fail "install" "pip install -r requirements.txt failed"
fi

log "compileall"
"$PY" -m compileall -q lake_executor >>"$LOG_FILE" 2>&1 || fail "compile" "python -m compileall failed"

if [ "$DEPLOY_RUN_TESTS" = "1" ] && [ -d tests ]; then
  "$PY" -c "import pytest" 2>/dev/null || { log "installing pytest"; timeout 300 "$PIP" install -q pytest >>"$LOG_FILE" 2>&1 || fail "tests" "pip install pytest failed"; }
  log "pytest"
  if ! timeout 900 "$PY" -m pytest -q -x -p no:cacheprovider tests >>"$LOG_FILE" 2>&1; then
    fail "tests" "pytest failed: $(tail -n 3 "$LOG_FILE" | tr '\n' ' ' | cut -c1-200)"
  fi
  log "pytest: $(tail -n 1 "$LOG_FILE")"
fi

log "lake_executor check"
if ! timeout 180 "$PY" -m lake_executor check >>"$LOG_FILE" 2>&1; then
  fail "check" "python -m lake_executor check failed (see log)"
fi

if [ "$DEPLOY_CHANGED" = "1" ] && [ "$CURRENT" != "none" ]; then
  log "deploy/* changed: re-running setup-server.sh"
  if ! bash deploy/setup-server.sh >>"$LOG_FILE" 2>&1; then
    fail "setup" "setup-server.sh failed"
  fi
fi

log "restart $SERVICE"
sudo systemctl restart "$SERVICE" || fail "restart" "systemctl restart failed"
HEALTHY=0
for _ in $(seq 1 15); do
  sleep 2
  if curl -s -m 3 http://127.0.0.1:8787/healthz | grep -q '"ok":true'; then HEALTHY=1; break; fi
done
if [ "$HEALTHY" != "1" ]; then
  fail "healthz" "service did not become healthy within 30 s: $(journalctl -u "$SERVICE" -n 5 --no-pager 2>/dev/null | tail -n 2 | cut -c1-200)"
fi

log "deployed ${TARGET:0:7} ($SUBJECT) in $(( $(date +%s) - START_TS ))s"
write_status "deployed" "done" "deployed ${TARGET:0:7} from $BRANCH" "$TARGET" "$SUBJECT"
notify "[deploy] OK $(hostname): ${TARGET:0:7} $SUBJECT (branch $BRANCH, trigger $TRIGGER)"
exit 0
