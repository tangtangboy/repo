"""EC2 호스트에서 임의 명령 실행.

    python deploy/ssh_run.py "<remote shell command>"
예)
    python deploy/ssh_run.py "systemctl is-active lake-executor"
    python deploy/ssh_run.py "journalctl -u lake-executor -n 50 --no-pager"
    python deploy/ssh_run.py "touch /home/ubuntu/lake-executor/state/HALT"     # 신규 신호 거부(OPERATOR_HALT)
    python deploy/ssh_run.py "rm -f /home/ubuntu/lake-executor/state/HALT"     # 재개
호스트/키는 deploy/aws_state.json 에서, 호스트 키는 deploy/known_hosts 에서 읽는다 (없으면 지문 확인 후 기록).
"""
from __future__ import annotations

import os
import sys

import paramiko

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sshutil import connect as _ssh_connect, load_state  # noqa: E402  (호스트 키 고정 SSH)


def connect(state: dict, timeout: int = 20) -> paramiko.SSHClient:
    """deploy/known_hosts 의 호스트 키만 신뢰. 첫 접속은 지문 확인(대화형) 또는 LAKE_TRUST_NEW_HOST_KEY=1."""
    return _ssh_connect(state, timeout=timeout)


def run(cli: paramiko.SSHClient, cmd: str, echo: bool = False) -> tuple[int, str, str]:
    if echo:
        print(f"$ {cmd}")
    _, stdout, stderr = cli.exec_command(cmd, get_pty=True)
    out = stdout.read().decode(errors="replace")
    err = stderr.read().decode(errors="replace")
    code = stdout.channel.recv_exit_status()
    return code, out, err


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    cmd = sys.argv[1] if len(sys.argv) > 1 else "echo hello"
    state = load_state()
    cli = connect(state)
    try:
        code, out, err = run(cli, cmd)
    finally:
        cli.close()
    if out.strip():
        print(out.rstrip())
    if err.strip():
        print("STDERR:", err.rstrip())
    print(f"[exit {code}]")
    sys.exit(code)


if __name__ == "__main__":
    main()
