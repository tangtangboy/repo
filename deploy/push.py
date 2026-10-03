"""소스를 EC2 호스트에 올리고 setup-server.sh 를 실행한다 (서비스는 아직 시작하지 않음).

올리는 것: lake_executor/ 패키지, requirements.txt, config.example.json,
          deploy/setup-server.sh, deploy/Caddyfile, deploy/lake-executor.service, tools/
setup-server.sh: venv + 의존성, Caddy(공식 apt 저장소) 설치, /etc/caddy/env(PUBLIC_HOST),
                 Caddyfile·systemd 유닛 설치, caddy enable. lake-executor 는 finalize.py 에서 시작.

    python deploy/push.py                       # PUBLIC_HOST = aws_state.json 의 public_host(기본 sslip.io)
    python deploy/push.py --host hook.example.com   # 직접 도메인 사용 (DNS A 레코드 → Elastic IP 먼저)
    python deploy/push.py --no-setup            # 파일만 올리고 setup-server.sh 는 생략
    python deploy/push.py --admin-cidr 1.2.3.4/32   # /state, /admin/* 를 외부에서 쓸 운영자 IP (기본: 서버 로컬만)
첫 접속은 호스트 키 지문을 보여 주고 확인을 받아 deploy/known_hosts 에 기록한다 (--trust-new-host-key 로 생략 가능).
"""
from __future__ import annotations

import argparse
import json
import os
import posixpath
import sys

import paramiko

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sshutil import add_trust_arg, connect as _ssh_connect, load_state  # noqa: E402  (호스트 키 고정 SSH)

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
STATE_PATH = os.path.join(HERE, "aws_state.json")
REMOTE = "/home/ubuntu/lake-executor"

UPLOAD_FILES = [
    "requirements.txt",
    "config.example.json",
    "deploy/setup-server.sh",
    "deploy/Caddyfile",
    "deploy/lake-executor.service",
]
UPLOAD_DIRS = ["lake_executor", "tools"]
SKIP_DIRS = {"__pycache__", ".pytest_cache", ".git"}
SKIP_SUFFIXES = (".pyc", ".pyo", ".log", ".db", ".db-wal", ".db-shm", ".pem")
SECRET_NAMES = ("config.json", "aws_state.json")


def is_secret_file(name: str) -> bool:
    """비밀값 파일은 push 로 올리지 않는다 (.env, .env.*, config.json, *.pem). finalize.py 가 명시적으로 올린다."""
    return name.startswith(".env") or name in SECRET_NAMES or name.endswith(".pem")


def connect(state: dict, retries: int = 30, delay: int = 10, trust_new: bool = False) -> paramiko.SSHClient:
    """막 띄운 인스턴스는 SSH 가 뜰 때까지 시간이 걸리므로 재시도한다. 호스트 키는 deploy/known_hosts 로 고정."""
    return _ssh_connect(state, trust_new=trust_new, retries=retries, delay=delay, timeout=15)


def run(cli: paramiko.SSHClient, cmd: str, echo: bool = True) -> tuple[int, str, str]:
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
    return code, out, err


def collect_uploads() -> list[str]:
    """ROOT 기준 상대경로(슬래시 구분) 목록. 디렉터리는 재귀, 캐시/로그/DB 제외."""
    rels: list[str] = []
    for rel in UPLOAD_FILES:
        if os.path.exists(os.path.join(ROOT, rel.replace("/", os.sep))):
            rels.append(rel)
        else:
            print("  (skip, missing)", rel)
    for d in UPLOAD_DIRS:
        base = os.path.join(ROOT, d)
        if not os.path.isdir(base):
            print("  (skip, missing dir)", d)
            continue
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = sorted(n for n in dirnames if n not in SKIP_DIRS)
            for fn in sorted(filenames):
                if fn.endswith(SKIP_SUFFIXES) or is_secret_file(fn):
                    continue
                full = os.path.join(dirpath, fn)
                rels.append(os.path.relpath(full, ROOT).replace(os.sep, "/"))
    return rels


def upload(cli: paramiko.SSHClient, rels: list[str]) -> None:
    sftp = cli.open_sftp()
    try:
        made: set[str] = set()
        for rel in rels:
            remote = posixpath.join(REMOTE, rel)
            rdir = posixpath.dirname(remote)
            if rdir not in made:
                run(cli, f"mkdir -p '{rdir}'", echo=False)
                made.add(rdir)
            sftp.put(os.path.join(ROOT, rel.replace("/", os.sep)), remote)
            print("uploaded", rel)
    finally:
        sftp.close()


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    ap = argparse.ArgumentParser(description="upload source + run setup-server.sh")
    ap.add_argument("--host", default=None,
                    help="PUBLIC_HOST for Caddy (default: aws_state.json public_host, i.e. <eip>.sslip.io)")
    ap.add_argument("--no-setup", action="store_true", help="upload only; do not run setup-server.sh")
    ap.add_argument("--admin-cidr", default=None,
                    help="CIDR allowed to reach /state and /admin/* through Caddy (default 127.0.0.1/32 = server only)")
    add_trust_arg(ap)
    args = ap.parse_args()

    state = load_state()
    public_host = args.host or os.environ.get("PUBLIC_HOST") or state.get("public_host") \
        or state["public_ip"].replace(".", "-") + ".sslip.io"

    print(f"connecting to {state['ssh_user']}@{state['public_ip']} ...")
    cli = connect(state, trust_new=args.trust_new_host_key)
    print("connected.")
    try:
        run(cli, f"mkdir -p {REMOTE}/deploy {REMOTE}/state")
        rels = collect_uploads()
        upload(cli, rels)
        # 서버 쪽 캐시 제거 (옛 모듈 잔재 방지)
        run(cli, f"find {REMOTE}/lake_executor -name __pycache__ -type d -prune -exec rm -rf {{}} + 2>/dev/null; true",
            echo=False)

        if args.no_setup:
            print("\n==== PUSH DONE (upload only) ====")
            return

        admin_cidr = args.admin_cidr or os.environ.get("ADMIN_ALLOW_CIDR") or state.get("admin_allow_cidr") or ""
        print(f"\n--- setup-server.sh (PUBLIC_HOST={public_host} ADMIN_ALLOW_CIDR={admin_cidr or '127.0.0.1/32'}) ---")
        code, _, _ = run(cli, f"cd {REMOTE} && bash deploy/setup-server.sh '{public_host}' '{admin_cidr}'")
        if code != 0:
            print("\n!! setup-server.sh failed (exit", code, ")")
            sys.exit(code)

        changed = False
        if args.host and state.get("public_host") != args.host:
            state["public_host"] = args.host
            changed = True
            print("aws_state.json public_host ->", args.host)
        if admin_cidr and state.get("admin_allow_cidr") != admin_cidr:
            state["admin_allow_cidr"] = admin_cidr
            changed = True
        if changed:
            with open(STATE_PATH, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)

        print("\n==== PUSH DONE ====")
        print("host        :", state["public_ip"])
        print("public host :", public_host)
        print("signal URL  :", f"https://{public_host}/lake/signal")
        print("Next: fill .env + config.json locally, then  python deploy/finalize.py")
    finally:
        cli.close()


if __name__ == "__main__":
    main()
