"""EC2 호스트에서 임의 명령 실행.

    python deploy/ssh_run.py "<remote shell command>"
예)
    python deploy/ssh_run.py "systemctl is-active lake-executor"
    python deploy/ssh_run.py "journalctl -u lake-executor -n 50 --no-pager"
    python deploy/ssh_run.py "touch /home/ubuntu/lake-executor/state/HALT"     # 신규 신호 거부(OPERATOR_HALT)
    python deploy/ssh_run.py "rm -f /home/ubuntu/lake-executor/state/HALT"     # 재개
호스트/키는 deploy/aws_state.json 에서 읽는다.
"""
from __future__ import annotations

import json
import os
import sys

import paramiko

HERE = os.path.dirname(os.path.abspath(__file__))
STATE_PATH = os.path.join(HERE, "aws_state.json")


def load_state() -> dict:
    if not os.path.exists(STATE_PATH):
        raise SystemExit("deploy/aws_state.json not found — run deploy/provision.py first")
    with open(STATE_PATH, encoding="utf-8") as f:
        return json.load(f)


def load_private_key(path: str):
    """provision.py 는 RSA 키를 만들지만, 사용자가 다른 종류의 키를 넣었을 수도 있다."""
    last = None
    for cls in (paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey):
        try:
            return cls.from_private_key_file(path)
        except Exception as e:  # noqa: BLE001 - 다음 키 종류 시도
            last = e
    raise SystemExit(f"cannot load private key {path}: {type(last).__name__}")


def connect(state: dict, timeout: int = 20) -> paramiko.SSHClient:
    cli = paramiko.SSHClient()
    cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    cli.connect(state["public_ip"], username=state["ssh_user"],
                pkey=load_private_key(state["key_path"]), timeout=timeout)
    return cli


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
