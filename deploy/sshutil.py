"""배포 스크립트 공용 SSH 연결 — 호스트 키 고정(known_hosts).

AutoAddPolicy 는 상대가 내미는 아무 키나 신뢰해 중간자 공격에 열려 있다 (finalize.py 는 .env 를 SFTP 로 올린다).
여기서는 `deploy/known_hosts` 에 기록된 키만 신뢰하고, 첫 접속(기록 없음)에는 지문을 보여 주고 운영자 확인을 받은 뒤
기록한다. 자동화에서는 `--trust-new-host-key` (또는 환경변수 LAKE_TRUST_NEW_HOST_KEY=1) 로 첫 접속만 허용한다.
키가 바뀌면(인스턴스 재생성 등) 접속을 거부하므로 `deploy/known_hosts` 의 해당 줄을 지우고 다시 확인한다.

    from sshutil import connect, load_private_key, load_state
    cli = connect(state, trust_new=False)
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import socket
import sys
import time

import paramiko

HERE = os.path.dirname(os.path.abspath(__file__))
STATE_PATH = os.path.join(HERE, "aws_state.json")
KNOWN_HOSTS_PATH = os.path.join(HERE, "known_hosts")
TRUST_ENV = "LAKE_TRUST_NEW_HOST_KEY"


class HostKeyMismatch(SystemExit):
    pass


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


def fingerprint(key: paramiko.PKey) -> str:
    return "SHA256:" + base64.b64encode(hashlib.sha256(key.asbytes()).digest()).decode().rstrip("=")


def _fetch_host_key(host: str, port: int = 22, timeout: float = 15.0) -> paramiko.PKey:
    sock = socket.create_connection((host, port), timeout=timeout)
    t = paramiko.Transport(sock)
    try:
        t.start_client(timeout=timeout)
        return t.get_remote_server_key()
    finally:
        t.close()


def _known_hosts() -> paramiko.HostKeys:
    hk = paramiko.HostKeys()
    if os.path.exists(KNOWN_HOSTS_PATH):
        hk.load(KNOWN_HOSTS_PATH)
    return hk


def ensure_host_key(host: str, trust_new: bool = False, interactive: bool | None = None) -> None:
    """known_hosts 에 host 가 있으면 그대로(연결 시 RejectPolicy 가 대조). 없으면 지문을 보여 주고 확인 뒤 기록."""
    hk = _known_hosts()
    if hk.lookup(host):
        return
    key = _fetch_host_key(host)
    fp = fingerprint(key)
    print(f"!! no recorded host key for {host}")
    print(f"   {key.get_name()} {fp}")
    print("   verify it against the instance (EC2 console > Actions > Monitor and troubleshoot > Get system log)")
    trust = trust_new or os.environ.get(TRUST_ENV, "") == "1"
    if not trust:
        if interactive is None:
            interactive = sys.stdin.isatty()
        if not interactive:
            raise SystemExit("refusing to connect to an unverified host; re-run with --trust-new-host-key "
                             f"or {TRUST_ENV}=1 after checking the fingerprint")
        ans = input("   trust this key and record it in deploy/known_hosts? [y/N] ").strip().lower()
        if ans not in ("y", "yes"):
            raise SystemExit("aborted: host key not trusted")
    hk.add(host, key.get_name(), key)
    hk.save(KNOWN_HOSTS_PATH)
    print(f"   recorded in {KNOWN_HOSTS_PATH}")


def connect(state: dict, *, trust_new: bool = False, retries: int = 1, delay: int = 10,
            timeout: int = 20) -> paramiko.SSHClient:
    """known_hosts 로 호스트 키를 대조해 연결. retries>1 이면 SSH 가 뜰 때까지 재시도 (막 띄운 인스턴스)."""
    host = state["public_ip"]
    key = load_private_key(state["key_path"])
    last: Exception | None = None
    for i in range(max(1, retries)):
        try:
            ensure_host_key(host, trust_new=trust_new)
            cli = paramiko.SSHClient()
            cli.load_host_keys(KNOWN_HOSTS_PATH)
            cli.set_missing_host_key_policy(paramiko.RejectPolicy())
            cli.connect(host, username=state["ssh_user"], pkey=key, timeout=timeout, allow_agent=False,
                        look_for_keys=False)
            return cli
        except paramiko.BadHostKeyException as e:
            raise HostKeyMismatch(f"!! HOST KEY MISMATCH for {host}: {fingerprint(e.key)} != recorded "
                                  f"{fingerprint(e.expected_key)}. Possible MITM or re-created instance. "
                                  f"Remove the line from {KNOWN_HOSTS_PATH} only after verifying.") from None
        except SystemExit:
            raise
        except Exception as e:  # noqa: BLE001 - 네트워크/SSH 미기동
            last = e
            if i + 1 < retries:
                print(f"  ssh not ready ({i + 1}/{retries}): {type(e).__name__}; waiting {delay}s")
                time.sleep(delay)
    raise SystemExit(f"could not SSH to host: {type(last).__name__ if last else 'unknown'}")


def add_trust_arg(parser) -> None:
    parser.add_argument("--trust-new-host-key", action="store_true",
                        help="first connection only: record the presented host key after printing its fingerprint")
