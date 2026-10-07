# deploy/ — AWS 배포 런북 (Windows PC 기준)

lake-executor 를 AWS EC2(서울, ap-northeast-2) 에 올려 24시간 돌리는 절차.
구성: **EC2 t3.micro (Ubuntu 24.04) + Elastic IP + Caddy(자동 HTTPS, 443) → 127.0.0.1:8787 lake-executor (systemd)**.
외부에 열리는 건 443(+80, ACME 용) 뿐이고, SSH(22) 는 배포 PC 의 공인 IP 에서만 허용된다.

| 파일 | 역할 |
|---|---|
| `provision.py` | 키페어·보안그룹·EC2·Elastic IP 생성 → `aws_state.json` 기록 |
| `push.py` | 소스 업로드 + `setup-server.sh` 실행 (venv, 의존성, Caddy, systemd 유닛) |
| `finalize.py` | `.env`/`config.json` 업로드(600) → `check` → 서비스 기동 → 로그 출력 |
| `ssh_run.py` | 서버에서 명령 하나 실행 (상태 확인·HALT·로그) |
| `sshutil.py` | 공용 SSH 연결: 호스트 키를 `deploy/known_hosts` 에 고정 (첫 접속 지문 확인, 이후 불일치 거부) |
| `setup-server.sh` | 서버 쪽 1회 설정 스크립트 (push.py 가 호출, 재실행 안전) |
| `Caddyfile` | `{$PUBLIC_HOST}` → `reverse_proxy 127.0.0.1:8787`; `/state`,`/admin/*` 는 `ADMIN_ALLOW_CIDR`, `/ui` 는 `UI_ALLOW_CIDR` 로 거른다 |
| `lake-executor.service` | systemd 유닛 (`User=ubuntu`, `Restart=always`) |

**절대 커밋 금지** (`.gitignore` 처리됨): `deploy/aws_state.json`, `deploy/*.pem`, `.env`, `config.json`.

---

## 0. 자격증명을 어디서 읽나

`deploy/provision.py` 는 다음 순서로 AWS 자격증명을 찾는다.

1. 환경변수 `DEPLOY_AWS_ACCESS_KEY_ID` / `DEPLOY_AWS_SECRET_ACCESS_KEY` (Claude 클라우드 세션의 환경 설정에 넣을 때 사용. `AWS_*` 이름은 세션 프록시가 더미 값으로 점유하므로 쓰지 않는다)
2. `AWS_PROFILE` 환경변수로 지정한 `~/.aws/credentials` 프로필
3. boto3 기본 체인 (`default` 프로필 등)

필요한 IAM 권한: EC2 (인스턴스·키페어·보안그룹·Elastic IP 생성/조회). 관리형 정책 `AmazonEC2FullAccess` 면 충분하다.

## 0. 로컬 준비 (한 번만)

1. Python 3.11+ 와 의존성
   ```powershell
   pip install boto3 paramiko
   ```
2. AWS 자격증명 `C:\Users\<me>\.aws\credentials`
   ```ini
   [default]
   aws_access_key_id = AKIA...
   aws_secret_access_key = ...
   ```
   다른 프로필을 쓰려면 실행 전에 `AWS_PROFILE` 을 지정한다.
   ```powershell
   $env:AWS_PROFILE = "lake-deploy"     # PowerShell
   set AWS_PROFILE=lake-deploy          # cmd
   ```
   IAM 권한: EC2 (RunInstances, CreateKeyPair, CreateSecurityGroup, AuthorizeSecurityGroupIngress,
   AllocateAddress, AssociateAddress, Describe*, CreateTags) 정도면 충분.
3. 명령은 **레포 루트**에서 실행한다 (`python deploy/xxx.py`).

## 1. 인프라 생성 — `provision.py`

```powershell
python deploy/provision.py
```
- 배포 PC 의 공인 IP 를 조회해 SG 22 번을 그 IP `/32` 로만 연다. (PC IP 가 바뀌면 §7 참고)
- 끝나면 아래가 출력된다. **Elastic IP** 와 **signal URL** 을 메모.
  ```
  ELASTIC IP            : 3.xx.xx.xx
  default public host   : 3-xx-xx-xx.sslip.io
  signal URL (for lake) : https://3-xx-xx-xx.sslip.io/lake/signal
  REMINDER: whitelist the Elastic IP 3.xx.xx.xx on the Bybit API key
  ```
- 개인키는 `deploy/lake-executor-key.pem` 에 저장된다. Windows 에서 OpenSSH `ssh` 로 직접 접속하려면
  권한을 조여야 한다 (paramiko 기반 스크립트에는 불필요):
  ```powershell
  icacls deploy\lake-executor-key.pem /inheritance:r /grant:r "$env:USERNAME:R"
  ```
- 이미 `aws_state.json` 에 `instance_id` 가 있으면 중복 생성을 막기 위해 바로 종료한다.

## 2. 거래소 API 키 준비 (Bybit 필수, OKX/Toobit 은 계정을 켤 때)

공통: lake 전용 **서브계정**, 권한은 **선물 주문·포지션 읽기/쓰기** 만(**출금·이체 없음**), **IP 제한 = 위 Elastic IP** 한 개
(서버에서만 작동, 로컬에서는 `check` 가 거래소 단계에서 실패하는 게 정상). 모든 계정은 **헤지 모드**로 쓴다(서비스 시작 시
`ensure_account_setup` 이 맞추므로 첫 기동은 포지션 없는 상태에서). 상세·검증 순서는 `docs/RUNBOOK.md` §6.

| 거래소 | `.env` 키 | 비고 |
|---|---|---|
| Bybit | `BYBIT_API_KEY` / `BYBIT_API_SECRET` | UTA, Contract 주문·포지션 읽기/쓰기. USDT 무기한 BTCUSDT |
| OKX | `OKX_API_KEY` / `OKX_API_SECRET` / `OKX_API_PASSPHRASE` | 권한 Trade+Read. **passphrase 필수**. `testnet:true` 는 데모 트레이딩(데모 전용 키 따로 발급) |
| Toobit | `TOOBIT_API_KEY` / `TOOBIT_API_SECRET` | USDT-M 선물, LONG/SHORT 양방향 포지션 전용. 테스트넷 없음(소액 실계정으로 검증) |

`config.json` 의 `accounts[]` 에서 계정을 켠다(`enabled`). 예시 파일은 bybit 만 켜져 있고 OKX/Toobit 은 꺼져 있다.
접두사는 `accounts[].env_prefix` 로 바꿀 수 있다(예: `OKX_SUB_API_KEY`).

## 3. 소스 업로드 + 서버 설정 — `push.py`

```powershell
python deploy/push.py
```
- 올리는 것: `lake_executor/`, `requirements.txt`, `config.example.json`, `tools/`, `deploy/{setup-server.sh,Caddyfile,lake-executor.service}`.
- 서버에서 `setup-server.sh` 가 돈다: `python3-venv` → `.venv` → pip 설치 → **Caddy 공식 apt 저장소(cloudsmith) 설치**
  → `/etc/caddy/env` 에 `PUBLIC_HOST=<EIP를 -로 바꾼 값>.sslip.io` → Caddyfile·systemd 유닛 설치 → `daemon-reload` → caddy enable.
- lake-executor 서비스는 **아직 시작하지 않는다** (비밀값이 없으므로).
- 막 만든 인스턴스는 SSH 가 뜨는 데 30~60초 걸린다. "ssh not ready … waiting" 이 몇 번 찍히는 건 정상.
- **첫 접속**에는 서버 호스트 키 지문이 출력되고 확인을 묻는다 (EC2 콘솔 > 인스턴스 > 모니터링 및 문제 해결 > 시스템 로그 가져오기
  의 `ssh-keygen` 지문과 대조). 확인하면 `deploy/known_hosts` 에 기록되고 이후에는 그 키만 신뢰한다. 비대화형이면
  `--trust-new-host-key` (또는 `LAKE_TRUST_NEW_HOST_KEY=1`). 인스턴스를 재생성해 키가 바뀌면 "HOST KEY MISMATCH" 로 거부된다
  → `deploy/known_hosts` 의 해당 줄을 지우고 다시 확인한다. `.env` 를 SFTP 로 올리는 `finalize.py` 도 같은 규칙이다.
- `/state`, `/admin/*` 는 Caddy 에서 기본적으로 서버 로컬(127.0.0.1/32)만 허용한다. 외부 운영 PC 에서 쓰려면
  `python deploy/push.py --admin-cidr <PC공인IP>/32` (aws_state.json 에 기억된다).
- 운영 대시보드 `https://<PUBLIC_HOST>/ui` 는 기본적으로 어디서나 열리고 `ADMIN_TOKEN` 로그인이 보호한다(`UI_ALLOW_CIDR` 기본 `0.0.0.0/0 ::/0`,
  IPv4+IPv6). 운영 PC IP 가 고정이면 `python deploy/push.py --ui-cidr <PC공인IP>/32` 로 잠근다(`--admin-cidr` 와 별개; 둘 다 aws_state.json 에 기억).
  `--ui-cidr` 에 IPv4 만 적으면 IPv6 로 접속하는 운영자는 404 를 받는다(AAAA 레코드가 있는 도메인을 쓸 때는 IPv6 접두사도 함께).
  Caddy 접근 로그는 `Cookie`/`Set-Cookie`(`resp_headers>Set-Cookie`)/`Authorization` 헤더를 지우도록 필터링된다(토큰·서명 헤더와 같은 취급).
  `/ui` 는 별도 `reverse_proxy` 블록으로 응답 대기 120초(Check/Reconcile 의 거래소 왕복), 나머지는 10초.
- **`config.json` 의 `listen.host` 는 `127.0.0.1` 로 둔다.** 앱은 `X-Forwarded-For`/`X-Forwarded-Proto` 를 TCP 피어가 루프백(=같은 호스트의
  Caddy) 일 때만 믿는다. 다른 주소로 듣게 하면 헤더를 믿지 않으므로 로그인 실패 예산은 피어 IP 기준이 되고, 쿠키의 `Secure` 는 붙지 않는다.
- 코드를 고친 뒤 다시 올릴 때도 `push.py` (또는 `finalize.py` — 패키지를 같이 올린다) 를 쓴다.

## 4. 비밀값 작성 (로컬)

```powershell
copy .env.example .env
copy config.example.json config.json
```
- `.env`: `BYBIT_API_KEY/SECRET`(+ 켤 계정의 `OKX_API_KEY/SECRET/PASSPHRASE`, `TOOBIT_API_KEY/SECRET`),
  `LAKE_SIGNAL_SECRET_TEST/LIVE`(수신 검증, 32바이트 이상), `LAKE_REPORT_SECRET_TEST/LIVE` + `LAKE_REPORT_URL_TEST/LIVE`(회신 기본값;
  계정별 덮어쓰기 `LAKE_REPORT_URL_{MODE}_{NAME}` / `LAKE_REPORT_SECRET_{MODE}_{NAME}`), `ADMIN_TOKEN`(32바이트 이상,
  `python -c "import secrets;print(secrets.token_urlsafe(32))"`), (선택) 텔레그램.
- `config.json`: 처음엔 `"live": {"enabled": false}` 로 두고 TEST 모드부터. `"test": {"simulate_fills": true}` 로 하면
  TEST 신호를 계정별 PaperExchange 로 체결시켜 회신·스냅샷 흐름까지 확인할 수 있다 (실거래소 호출 없음).
  `accounts[]` 는 bybit 만 `enabled:true`, `report:true` 로 시작한다. OKX/Toobit 은 `docs/RUNBOOK.md` §6.0 순서로 한 계정씩 켠다.
  `accounts` 키를 지우면 v0.1 과 같은 단일 bybit 설정이다.
- 실거래 전환은 **lake 와 TEST 왕복이 끝난 뒤** `live.enabled=true` 로 바꾸고 다시 `finalize.py`.

## 5. 기동 — `finalize.py`

```powershell
python deploy/finalize.py
```
순서: 소스 동기화 + `.env`/`config.json` 업로드(`chmod 600`) → `python -m lake_executor check`(읽기 전용, 주문 없음)
→ 통과 시 `systemctl enable --now lake-executor` + restart, `systemctl restart caddy` → `is-active`, `journalctl -n 30`, `/healthz` 출력.

- **대시보드(/ui) 에서 키를 넣은 뒤**에는 서버 `.env`/`config.json` 이 진실이다. 코드만 다시 올릴 때는
  `python deploy/finalize.py --keep-remote-env` (`--keep-remote-config`) 로 돌려 서버 값을 덮어쓰지 않는다 — 플래그 없이 돌리면
  로컬 파일로 덮어쓴다(실행 시작 때 REMINDER 로 알려 준다).
- `check` 가 실패하면 서비스를 켜지 않는다. 흔한 원인: Bybit 키 IP 화이트리스트 누락, 시크릿 길이 < 32, 회신 URL 오타,
  OKX 키는 있는데 `OKX_API_PASSPHRASE` 누락, Toobit 계정에 `position_mode` 가 `hedge` 가 아님(설정 오류 exit 2).
  `check` 는 실키가 있는 모든 계정(Bybit/OKX/Toobit)에 대해 읽기 전용으로 instrument·시세·포지션을 읽으므로 키·IP 화이트리스트
  문제가 여기서 드러난다; 기동 후에는 `/state` 의 `accounts.{name}.modes.live.exchange_positions` 로 한 번 더 확인한다.
- HTTPS 인증서는 Caddy 가 첫 요청/기동 때 발급한다 (보통 1분 이내). 외부 `healthz` 가 바로 안 뜨면 잠시 후:
  ```powershell
  python deploy/ssh_run.py "curl -s https://3-xx-xx-xx.sslip.io/healthz"
  ```
  기대 응답: `{"ok":true,"halted":false,"inconsistent":{"test":false,"live":false}}`
- lake 측에 전달할 것: **signal URL** `https://<PUBLIC_HOST>/lake/signal`, TEST/LIVE 수신 시크릿, 우리 회신 서명 시크릿.
  lake 가 보낸 신호를 재현해 보려면 로컬에서 `python -m lake_executor sign --file signal.json --mode test` 로 헤더를 만들어 curl.

## 5a. GitHub 자동 배포 — 서버가 pull (pem 없이 코드 반영)

> **현재 상태: 준비 중.** `deploy/self_update.sh` 와 `.timer`/`.path` 유닛 파일은 저장소에 있지만, 서버 설치 단계(`setup-server.sh` 의
> 유닛 생성·활성화), `push.py --repo-url/--deploy-branch` 연동, 대시보드 **Deploy from GitHub now** 버튼은 아직 적용되지 않았다.
> 그 전까지 코드 반영은 §5 `finalize.py --keep-remote-env --keep-remote-config` 로 한다.

`push.py` 가 처음 한 번 설치해 두면 그 뒤 **코드 반영은 `git push` 만으로 끝난다**. 어느 PC·클라우드 세션에서든 GitHub 에
푸시만 하면 되고, pem 파일은 `.env`/`config.json` 업로드(`finalize.py`)와 SSH 점검에만 필요하다. 서버의 systemd 유닛 세 개가 일한다.

| 유닛 | 역할 |
|---|---|
| `lake-executor-update.timer` | 2분마다(부팅 2분 뒤부터, ±20초) `lake-executor-update.service` 실행 |
| `lake-executor-update.path` | `state/DEPLOY_NOW` 가 생기면 즉시 실행 — 대시보드 Controls → **Deploy from GitHub now** 가 이 파일을 만든다 |
| `lake-executor-update.service` | `deploy/self_update.sh` 를 `/run` 에 복사해 실행 (oneshot, `ubuntu`). 유닛 본문은 `setup-server.sh` 가 생성 |

`self_update.sh` 절차: `git fetch` → 추적 브랜치(`DEPLOY_BRANCH`; 비우면 **GitHub 의 기본 브랜치**)의 머리가 HEAD 와 다르면
`git reset --hard` → `requirements.txt` 가 바뀌었으면 pip → `compileall` → **pytest**(`DEPLOY_RUN_TESTS=1`, 처음 한 번 pytest 설치)
→ `python -m lake_executor check` → `deploy/*` 가 바뀌었으면 `setup-server.sh` 재실행 → `systemctl restart lake-executor`
→ 30초 안에 `/healthz` 확인. **어느 단계든 실패하면 직전 커밋으로 되돌리고 서비스를 다시 올린 뒤** `state/last_deploy.json` 에
`failed`/`rolled_back` 으로 남기고 `.env` 에 텔레그램이 있으면 알린다. `.env`·`config.json`·`state/`·`.venv` 는 git 이 무시하므로
건드리지 않는다. 처음 실행 때 SFTP 사본 디렉터리를 git 체크아웃으로 바꾼다(bootstrap). 같은 커밋이면 아무것도 하지 않는다(`noop`).

설정은 `/etc/lake-executor/deploy.env` (`setup-server.sh` 가 쓴다; `push.py --repo-url/--deploy-branch` 로 지정, 기본값은 로컬 체크아웃의 `origin`):
```
DEPLOY_REPO_URL=https://github.com/<owner>/<repo>.git
DEPLOY_BRANCH=            # 비우면 GitHub 기본 브랜치를 따라간다
DEPLOY_RUN_TESTS=1        # 0 이면 pytest 생략 (권장하지 않음)
DEPLOY_ENABLED=1          # 0 이면 아무것도 하지 않음 (push.py --no-auto-deploy)
```
자주 쓰는 명령:
```powershell
python deploy/ssh_run.py "systemctl list-timers lake-executor-update.timer --no-pager"
python deploy/ssh_run.py "journalctl -u lake-executor-update -n 60 --no-pager"        # 배포 로그
python deploy/ssh_run.py "cat /home/ubuntu/lake-executor/state/last_deploy.json"       # 마지막 결과 (대시보드와 같은 내용)
python deploy/ssh_run.py "sudo systemctl start lake-executor-update"                   # 지금 바로 (대시보드 버튼과 같음)
python deploy/ssh_run.py "cd /home/ubuntu/lake-executor && git log --oneline -3"       # 서버가 돌리는 커밋
python deploy/ssh_run.py "sudo systemctl disable --now lake-executor-update.timer lake-executor-update.path"   # 끄기
```
- 비공개 저장소로 바꾸면 `DEPLOY_REPO_URL` 에 읽기 전용 토큰 URL(`https://<token>@github.com/...`) 또는 deploy key(ssh URL +
  `~ubuntu/.ssh`)를 넣는다. 파일 권한은 `640 root:ubuntu` 다.
- 보안: **GitHub 에 push 할 수 있는 사람 = 서버에서 코드를 실행할 수 있는 사람**이다(서비스는 `ubuntu`, 비밀번호 없는 sudo).
  GitHub 계정 2FA 와 기본 브랜치 보호를 켜 둔다.
- `finalize.py` 로 소스를 다시 올리면 다음 pull 때 `reset --hard` 로 덮인다 — 코드는 git 으로만, `finalize.py` 는
  `.env`/`config.json` 업로드(`--keep-remote-*` 주의)에만 쓴다.

## 5b. 원장을 Postgres(Supabase) 로 — `DATABASE_URL`

기본 원장은 서버 디스크의 `state/lake.db` 다. 인스턴스가 사라지면 원장도 사라지므로 운영에서는 Postgres 를 권장한다.

1. Supabase 프로젝트(서버와 같은 리전 권장) 의 **Session pooler** 연결 문자열을 받는다: `postgresql://postgres.<ref>:<pw>@aws-1-<region>.pooler.supabase.com:5432/postgres`
   (`?pgbouncer=true` 같은 쿼리 파라미터는 떼고, 포트는 5432).
2. `.env` 에 `DATABASE_URL=…` 한 줄 추가 (대시보드 Secrets 페이지에서도 넣을 수 있다; 저장 → Apply & restart). 스키마 이름은 `config.json` 의
   `database.schema`(기본 `lake_executor`) — 같은 프로젝트의 다른 앱 테이블과 격리된다. 테이블은 기동 때 자동 생성.
3. `pip install -r requirements.txt` 가 `psycopg[binary]` 를 넣었는지 확인하고(처음 한 번: `ssh_run.py "cd /home/ubuntu/lake-executor && ./.venv/bin/pip install -r requirements.txt"`),
   `python -m lake_executor check` 로 `ledger : postgres host:5432/postgres schema=… round-trip N ms` 를 본 뒤 `finalize.py --keep-remote-config` (또는 대시보드 재시작).
4. 열린 lot/pending 회신이 없는 때 전환한다. 기존 SQLite 행은 옮기지 않으며 회신 sequence 는 1부터 다시 시작한다 (lake 에 미리 알린다).

왕복이 500ms 를 넘으면 `check` 가 경고한다 — 서버(서울) 와 다른 리전의 DB 는 신호 하나에 수십 쿼리라 수 초가 걸릴 수 있다. 되돌리려면 `DATABASE_URL` 을 비우고 재시작.

## 6. 운영 명령 — `ssh_run.py`

```powershell
python deploy/ssh_run.py "systemctl is-active lake-executor"
python deploy/ssh_run.py "journalctl -u lake-executor -n 50 --no-pager"
python deploy/ssh_run.py "journalctl -u lake-executor -f"              # Ctrl+C 로 종료
python deploy/ssh_run.py "sudo systemctl restart lake-executor"
python deploy/ssh_run.py "sudo systemctl status caddy --no-pager"
python deploy/ssh_run.py "sudo tail -n 20 /var/log/caddy/access.log"
python deploy/ssh_run.py "cd /home/ubuntu/lake-executor && ./.venv/bin/python -m lake_executor check"
```

### 긴급 정지 (HALT) / 완전 정지
- **HALT (권장 1순위)** — 새 신호를 전부 `rejected/OPERATOR_HALT` 로 회신하고, 거래소의 기존 보호주문(SL/TP)은 그대로 둔다.
  서비스는 계속 떠 있어 스냅샷·회신은 이어진다.
  ```powershell
  python deploy/ssh_run.py "touch /home/ubuntu/lake-executor/state/HALT"
  python deploy/ssh_run.py "rm -f /home/ubuntu/lake-executor/state/HALT"     # 재개
  ```
  (`ADMIN_TOKEN` 을 설정했다면 `POST https://<host>/admin/halt` / `/admin/resume`, 또는 브라우저 `https://<host>/ui/controls` 의 HALT/Resume 도 같은 효과.)
- **서비스 정지** — 수신(HTTPS 는 502)·실행·회신 모두 멈춘다. 거래소 포지션/보호주문은 건드리지 않는다.
  ```powershell
  python deploy/ssh_run.py "sudo systemctl stop lake-executor"
  python deploy/ssh_run.py "sudo systemctl start lake-executor"
  ```
- **인스턴스 정지(요금 절약)** — AWS 콘솔 또는 `aws ec2 stop-instances --instance-ids <id>`.
  Elastic IP 는 유지되므로 다시 시작해도 IP·URL 그대로. (정지 중에도 EIP 는 소액 과금)
- 포지션을 실제로 닫는 건 이 봇이 아니라 **lake 의 full_exit 신호** 또는 수동(Bybit 앱)이다.
  수동으로 닫았다면 봇의 lot 원장과 어긋나 `RECONCILE_REQUIRED` 가 날 수 있다 → `POST /admin/reconcile?mode=live&account=<name>` (`account` 생략 = 모든 계정).

## 7. 자체 도메인 사용 (sslip.io 대신)

1. DNS 에서 **A 레코드** `hook.example.com → <Elastic IP>` 추가 (Cloudflare 면 프록시 OFF, "DNS only").
2. 전파 확인: `nslookup hook.example.com`.
3. 서버 설정 재실행 (PUBLIC_HOST 만 바뀐다; 멱등):
   ```powershell
   python deploy/push.py --host hook.example.com
   ```
   또는 서버에서 직접: `python deploy/ssh_run.py "cd /home/ubuntu/lake-executor && bash deploy/setup-server.sh hook.example.com"`
4. Caddy 가 새 호스트 인증서를 발급한다. 확인: `python deploy/ssh_run.py "curl -s https://hook.example.com/healthz"`
5. lake 측에 새 signal URL `https://hook.example.com/lake/signal` 을 알린다.

`push.py --host` 는 `aws_state.json` 의 `public_host` 도 갱신하므로 이후 `finalize.py` 출력에 반영된다.

## 8. 자주 겪는 문제

| 증상 | 원인 / 조치 |
|---|---|
| `provision.py` 가 `aws_state.json already has instance` 로 종료 | 이미 만들어짐. 새로 만들려면 AWS 에서 인스턴스·EIP·SG·키페어 삭제 후 `aws_state.json`, `.pem` 삭제 |
| `push.py` 가 계속 `ssh not ready` | 배포 PC 공인 IP 가 바뀜 → AWS 콘솔 SG `lake-executor` 의 22 번 규칙을 현재 IP 로 교체 |
| `check` 에서 Bybit 인증 실패 | API 키 IP 화이트리스트에 Elastic IP 누락, 또는 키 권한 부족 |
| 기동 로그에 `okx … rejected` / `toobit … rejected` (인증·서명) | 그 거래소 키 IP 화이트리스트 누락, OKX passphrase 불일치, Toobit `-1021/-1022`(서버 시계 오차·시크릿) |
| 어떤 계정만 `rejected/QTY_BELOW_MIN` | 그 거래소 최소 수량 미달(OKX 0.01 BTC). `qty_multiplier` 또는 lake 수량 단위 조정 |
| live 신호가 `ACCOUNT_DISABLED` / `NO_TARGET_ACCOUNT` | 그 계정 `enabled:false` / `by_exchange` 라우팅에 맞는 계정 없음 (`config.json` `accounts`·`routing`) |
| 외부 `/healthz` 가 안 열림 | 인증서 발급 대기(1분) / DNS 미전파 / SG 80·443 누락. `sudo journalctl -u caddy -n 50 --no-pager` |
| lake 가 401 받음 | `LAKE_SIGNAL_SECRET_<MODE>` 불일치 또는 시계 오차(±60s). 403 아님에 주의 |
| lake 가 503 받음 | 해당 mode 의 수신 시크릿이 `.env` 에 없음 |
| 회신이 `unsent` 로만 쌓임 | `LAKE_REPORT_URL_<MODE>` / `LAKE_REPORT_SECRET_<MODE>` 미설정 |
| 인스턴스를 재생성했는데 IP 가 바뀜 | EIP 를 재연결(associate) 하면 유지됨. 바뀌었으면 **모든 거래소** 키의 화이트리스트·DNS 를 갱신 |

## 9. 삭제 (전부 정리)

AWS 콘솔(서울 리전)에서 인스턴스 종료 → Elastic IP **릴리스**(안 하면 과금) → 보안그룹 `lake-executor` → 키페어 `lake-executor`.
로컬에서는 `deploy/aws_state.json`, `deploy/lake-executor-key.pem` 삭제. 거래소(Bybit/OKX/Toobit) API 키도 삭제/IP 해제.
