"""최종 배포: 비밀값 업로드 → check → 서비스 기동 → 상태/로그 출력.

1. lake_executor/ 소스 재업로드(서버 코드 동기화) + 로컬 .env, config.json 업로드 (SFTP, 셸 인자로 비밀 전달 안 함)
2. chmod 600 .env config.json
3. `python -m lake_executor check` (읽기 전용: 설정·키·Bybit 연결·회신 URL 확인; 주문 없음)
4. 통과하면 `systemctl enable --now lake-executor` (+restart 로 새 코드 반영), caddy 재시작
5. 상태 + journalctl -u lake-executor -n 30 + /healthz 확인

    python deploy/finalize.py
    python deploy/finalize.py --env .env.live --config config.live.json
    python deploy/finalize.py --skip-check      # check 실패를 무시 (비권장)
"""
from __future__ import annotations

import argparse
import json
import os
import posixpath
import sys

import paramiko

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
STATE_PATH = os.path.join(HERE, "aws_state.json")
REMOTE = "/home/ubuntu/lake-executor"
SERVICE = "lake-executor"
SKIP_DIRS = {"__pycache__", ".pytest_cache"}
SKIP_SUFFIXES = (".pyc", ".pyo", ".log", ".db", ".pem")


def load_state() -> dict:
    if not os.path.exists(STATE_PATH):
        raise SystemExit("deploy/aws_state.json not found — run deploy/provision.py first")
    with open(STATE_PATH, encoding="utf-8") as f:
        return json.load(f)


def load_private_key(path: str):
    last = None
    for cls in (paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey):
        try:
            return cls.from_private_key_file(path)
        except Exception as e:  # noqa: BLE001
            last = e
    raise SystemExit(f"cannot load private key {path}: {type(last).__name__}")


def run(cli: paramiko.SSHClient, cmd: str, echo: bool = True) -> tuple[int, str]:
    if echo:
        print(f"$ {cmd}")
    _, stdout, stderr = cli.exec_command(cmd, get_pty=True)
    out = stdout.read().decode(errors="replace")
    err = stderr.read().decode(errors="replace")
    code = stdout.channel.recv_exit_status()
    if out.strip():
        print(out.rstrip())
    if code != 0 and err.strip():
        print("STDERR:", err.rstrip())
    return code, out


def package_files() -> list[str]:
    rels: list[str] = ["requirements.txt"]
    base = os.path.join(ROOT, "lake_executor")
    for dirpath, dirnames, filenames in os.walk(base):
        dirnames[:] = sorted(n for n in dirnames if n not in SKIP_DIRS)
        for fn in sorted(filenames):
            if fn.endswith(SKIP_SUFFIXES) or fn.startswith(".env") or fn == "config.json":
                continue
            rels.append(os.path.relpath(os.path.join(dirpath, fn), ROOT).replace(os.sep, "/"))
    return rels


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    ap = argparse.ArgumentParser(description="upload secrets, check, start lake-executor")
    ap.add_argument("--env", default=os.path.join(ROOT, ".env"), help="local .env to upload")
    ap.add_argument("--config", default=os.path.join(ROOT, "config.json"), help="local config.json to upload")
    ap.add_argument("--skip-check", action="store_true", help="start service even if check fails (not recommended)")
    args = ap.parse_args()

    for p in (args.env, args.config):
        if not os.path.exists(p):
            raise SystemExit(f"missing {p} — copy from .env.example / config.example.json and fill it in")

    state = load_state()
    cli = paramiko.SSHClient()
    cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    cli.connect(state["public_ip"], username=state["ssh_user"],
                pkey=load_private_key(state["key_path"]), timeout=20)
    print("connected to", state["public_ip"])
    try:
        # 1+2: 소스 + 비밀값 업로드, 권한 잠금
        run(cli, f"mkdir -p {REMOTE}/lake_executor {REMOTE}/state", echo=False)
        sftp = cli.open_sftp()
        try:
            made: set[str] = set()
            for rel in package_files():
                remote = posixpath.join(REMOTE, rel)
                rdir = posixpath.dirname(remote)
                if rdir not in made:
                    run(cli, f"mkdir -p '{rdir}'", echo=False)
                    made.add(rdir)
                sftp.put(os.path.join(ROOT, rel.replace("/", os.sep)), remote)
            sftp.put(args.env, f"{REMOTE}/.env")
            sftp.put(args.config, f"{REMOTE}/config.json")
        finally:
            sftp.close()
        run(cli, f"chmod 600 {REMOTE}/.env {REMOTE}/config.json && echo 'code + .env + config.json uploaded'")
        run(cli, f"find {REMOTE}/lake_executor -name __pycache__ -type d -prune -exec rm -rf {{}} + 2>/dev/null; true",
            echo=False)

        # 3: 읽기 전용 점검 (주문 없음)
        print("\n--- python -m lake_executor check (read-only) ---")
        code, _ = run(cli, f"cd {REMOTE} && ./.venv/bin/python -m lake_executor check")
        if code != 0:
            if not args.skip_check:
                print(f"\n!! check failed (exit {code}). NOT starting the service. "
                      "Fix .env/config.json or the Bybit key IP whitelist, then re-run.")
                sys.exit(1)
            print(f"\n!! check failed (exit {code}) but --skip-check given; continuing.")

        # 4: 서비스 기동 (유닛은 setup-server.sh 가 설치; 혹시 빠졌으면 다시 설치)
        print("\n--- enabling + starting systemd services ---")
        run(cli, f"test -f /etc/systemd/system/{SERVICE}.service || "
                 f"(sudo cp {REMOTE}/deploy/{SERVICE}.service /etc/systemd/system/{SERVICE}.service && "
                 f"sudo systemctl daemon-reload)", echo=False)
        run(cli, f"sudo systemctl enable --now {SERVICE} >/dev/null 2>&1; echo '{SERVICE} enabled'")
        run(cli, f"sudo systemctl restart {SERVICE}; echo '{SERVICE} restarted'")
        run(cli, "sudo systemctl restart caddy; echo 'caddy restarted'")

        # 5: 상태 + 로그
        print("\n--- status ---")
        run(cli, f"sleep 4; systemctl is-active {SERVICE}; systemctl is-active caddy; "
                 f"echo '--- journalctl -u {SERVICE} -n 30 ---'; "
                 f"journalctl -u {SERVICE} -n 30 --no-pager")
        run(cli, "echo '--- healthz (local) ---'; curl -s -m 5 http://127.0.0.1:8787/healthz || echo '(no response yet)'; echo")
        run(cli, "PUBLIC_HOST=$(sed -n 's/^PUBLIC_HOST=//p' /etc/caddy/env 2>/dev/null); "
                 "echo \"--- healthz (https://$PUBLIC_HOST) ---\"; "
                 "curl -s -m 15 \"https://$PUBLIC_HOST/healthz\" || echo '(TLS not ready yet - cert issuance can take ~1 min; retry: python deploy/ssh_run.py \"curl -s https://'$PUBLIC_HOST'/healthz\")'; echo")
    finally:
        cli.close()
    print("\n==== FINALIZE DONE ====")
    print("logs : python deploy/ssh_run.py \"journalctl -u lake-executor -n 50 --no-pager\"")
    print("halt : python deploy/ssh_run.py \"touch /home/ubuntu/lake-executor/state/HALT\"")


if __name__ == "__main__":
    main()
