"""작은 공용 유틸: 시각, 해시, 수량/가격 라운딩, .env 파서."""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import tempfile
import time
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP


def now_ms() -> int:
    return int(time.time() * 1000)


def sha256_hex(data: bytes | str) -> str:
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def short_id(prefix: str, *parts: str, n: int = 24) -> str:
    """결정적 가명 ID. 계약 패턴 ^[A-Za-z0-9_.:-]{1,128}$ 을 만족."""
    h = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:n]
    return f"{prefix}-{h}"


def order_link_id(*parts: str) -> str:
    """Bybit orderLinkId (최대 36자, 영숫자/-/_ 만). 같은 입력 → 같은 ID (멱등 주문)."""
    h = hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:32]
    return "lk" + h  # 34자


def alnum_only(s: str, max_len: int = 32) -> str:
    """영숫자만 남기고 max_len 으로 절단 (OKX clOrdId 등 문자 집합이 좁은 거래소 ID 용).
    같은 입력 → 같은 출력이라 멱등 키로 계속 쓸 수 있다."""
    return re.sub(r"[^A-Za-z0-9]", "", s or "")[:max_len]


def env_suffix(name: str) -> str:
    """계정 이름 → 환경변수 접미사. 대문자화하고 영숫자가 아닌 문자는 '_' 로 바꾼다
    (예: "okx-sub" → "OKX_SUB"; LAKE_REPORT_URL_LIVE_OKX_SUB)."""
    return re.sub(r"[^A-Z0-9]", "_", (name or "").upper())


def floor_step(value: float, step: float) -> float:
    """수량을 qtyStep 배수로 내림. 부동소수 오차 방지를 위해 Decimal 사용."""
    if step <= 0:
        return value
    v = Decimal(str(value))
    s = Decimal(str(step))
    return float((v / s).to_integral_value(rounding=ROUND_DOWN) * s)


def round_tick(value: float, tick: float) -> float:
    if tick <= 0:
        return value
    v = Decimal(str(value))
    t = Decimal(str(tick))
    return float((v / t).to_integral_value(rounding=ROUND_HALF_UP) * t)


def fmt_step(value: float, step: float) -> str:
    """step 의 소수 자릿수에 맞춰 문자열화 (지수 표기/부동소수 잡음 제거)."""
    s = Decimal(str(step)).normalize()
    decimals = max(0, -s.as_tuple().exponent)
    return f"{Decimal(str(value)):.{decimals}f}"


def qty_eq(a: float, b: float, step: float) -> bool:
    """qtyStep 절반 이내면 같은 수량으로 본다."""
    return abs(a - b) <= (step / 2 if step > 0 else 1e-9)


def is_finite_number(x) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)


def parse_env_file(path: str) -> dict[str, str]:
    """KEY=VALUE 줄만 읽는 최소 .env 파서 (따옴표 제거, # 주석 무시). 파일이 없으면 {}."""
    out: dict[str, str] = {}
    if not path or not os.path.exists(path):
        return out
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip()
            if len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"'):
                v = v[1:-1]
            if k:
                out[k] = v
    return out


def read_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def canonical_json(obj) -> bytes:
    """회신 본문 직렬화. 서명은 이 바이트 그대로 계산·전송 (재직렬화 금지)."""
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")


# --------------------------------------------------------------------------- #
# .env 쓰기 / 원자적 파일 교체 (대시보드가 쓴다 — web.py §4.1)
# --------------------------------------------------------------------------- #
ENV_KEY_RE = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")
_ENV_ACTIVE_LINE_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=")


def quote_env_value(v: str) -> str:
    """parse_env_file 이 같은 값으로 되읽도록 .env 값을 표현한다.
    빈 값 → ''(KEY=). 앞뒤 공백·내부 공백·'#'·양끝 같은 따옴표가 있으면 따옴표로 감싼다("…" 우선, '"' 가 들면 '…').
    두 종류 따옴표를 모두 품으면서 감싸야 하는 값은 표현할 수 없다(ValueError)."""
    v = str(v)
    if v == "":
        return ""
    needs = (v != v.strip() or any(c.isspace() for c in v) or "#" in v
             or (len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"')))
    if not needs:
        return v
    if '"' not in v:
        return f'"{v}"'
    if "'" not in v:
        return f"'{v}'"
    raise ValueError("value cannot be represented in .env")


def render_env_update(text: str, updates: dict[str, str]) -> str:
    """기존 .env 텍스트에 updates 를 반영한 새 텍스트. 주석·빈 줄·순서는 그대로 두고,
    - 활성 줄(KEY=…) 이 있으면 첫 줄을 제자리에서 바꾸고 뒤의 중복 줄은 지운다(파서는 last-wins 이므로),
    - 없고 주석 템플릿(`# KEY=`) 이 있으면 그 줄을 활성화하고,
    - 둘 다 없으면 끝에 덧붙인다.
    키는 ENV_KEY_RE, 값은 \r \n \0 금지 (ValueError). 출력은 '\n' 줄 끝, BOM 없음.
    불변식: parse_env_file(결과) == {**parse_env_file(이전), **updates}."""
    for k, v in updates.items():
        if not isinstance(k, str) or not ENV_KEY_RE.match(k):
            raise ValueError(f"invalid env key: {k!r}")
        if not isinstance(v, str):
            raise ValueError(f"value for {k} must be a string")
        if "\r" in v or "\n" in v or "\0" in v:
            raise ValueError(f"value for {k} contains a line break or NUL")
    text = (text or "").lstrip("﻿").replace("\r\n", "\n").replace("\r", "\n")
    lines = text.split("\n")
    if text.endswith("\n"):
        lines = lines[:-1]
    if text == "":
        lines = []
    done: set[str] = set()
    out: list[str] = []
    for line in lines:
        m = _ENV_ACTIVE_LINE_RE.match(line)
        if m and m.group(1) in updates:
            k = m.group(1)
            if k in done:
                continue   # 뒤쪽 중복 정의 제거
            out.append(f"{k}={quote_env_value(updates[k])}")
            done.add(k)
            continue
        out.append(line)
    for k in updates:
        if k in done:
            continue
        pat = re.compile(r"^#\s*" + re.escape(k) + r"\s*=\s*$")
        for i, line in enumerate(out):
            if pat.match(line):
                out[i] = f"{k}={quote_env_value(updates[k])}"
                done.add(k)
                break
    for k in updates:
        if k not in done:
            out.append(f"{k}={quote_env_value(updates[k])}")
    return "\n".join(out) + "\n"


def _mkstemp_for(path: str, mode: int) -> tuple[int, str]:
    """path 와 같은 디렉터리에 고유한 이름(<basename>.tmp-XXXXXXXX)의 임시 파일을 O_EXCL, 0600 으로 만든다.
    pid 만으로 이름을 짓지 않으므로 같은 프로세스의 동시 쓰기(스레드풀) 가 서로의 임시 파일을 지우거나 가로채지 않는다."""
    d = os.path.dirname(os.path.abspath(path))
    fd, tmp = tempfile.mkstemp(dir=d, prefix=os.path.basename(path) + ".tmp-")
    if mode != 0o600:
        try:
            os.chmod(tmp, mode)
        except OSError:
            pass
    return fd, tmp


def atomic_write_text(path: str, text: str, *, mode: int = 0o600, backup: bool = True) -> None:
    """같은 디렉터리의 고유한 임시 파일(mkstemp: O_EXCL, 0600)에 쓰고 fsync 한 뒤 os.replace 로 교체한다.
    backup=True 이고 path 가 있으면 먼저 path+'.bak' 에 같은 방식으로 복사한다(백업의 백업은 없음).
    실패 시 임시 파일을 지우고 예외를 그대로 올린다. chmod 실패(Windows)는 무시.
    같은 파일을 여러 스레드가 쓰면 호출자가 잠금으로 직렬화한다(web.py 의 write_lock) — 여기서는 순서를 보장하지 않는다."""
    d = os.path.dirname(os.path.abspath(path))
    if d:
        os.makedirs(d, exist_ok=True)
    if backup and os.path.exists(path):
        with open(path, "r", encoding="utf-8", newline="") as f:
            old = f.read()
        atomic_write_text(path + ".bak", old, mode=mode, backup=False)
    fd, tmp = _mkstemp_for(path, mode)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise
    try:
        os.chmod(path, mode)
    except OSError:
        pass


def write_env_file(path: str, updates: dict[str, str]) -> list[str]:
    """path 의 .env 에 updates 를 반영(render_env_update → atomic_write_text, .bak 보관).
    반환: 실제로 값이 바뀐 키 목록 (이미 같은 값이면 제외; 하나도 없으면 파일을 건드리지 않는다)."""
    before = parse_env_file(path)
    changed = sorted(k for k, v in updates.items() if before.get(k, "") != v)
    if not changed:
        return []
    old_text = ""
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8", newline="") as f:
            old_text = f.read()
    new_text = render_env_update(old_text, updates)
    atomic_write_text(path, new_text, mode=0o600, backup=True)
    return changed


def set_json_value(path: str, dotted_key: str, value, *, validate=None) -> None:
    """JSON 파일의 점 구분 키(예: 'live.enabled') 를 value 로 바꿔 임시 파일(0600)에 쓰고,
    validate(tmp_path) 가 주어지면 교체 전에 호출한다(예외 → 원본 그대로, 임시 파일 삭제). 값은 JSON 타입 그대로(True/False)."""
    obj = read_json(path)
    if not isinstance(obj, dict):
        raise ValueError("JSON root must be an object")
    parts = [p for p in str(dotted_key).split(".") if p]
    if not parts:
        raise ValueError("empty key")
    cur = obj
    for p in parts[:-1]:
        nxt = cur.get(p)
        if not isinstance(nxt, dict):
            nxt = {}
            cur[p] = nxt
        cur = nxt
    cur[parts[-1]] = value
    text = json.dumps(obj, indent=2, ensure_ascii=False) + "\n"
    fd, tmp = _mkstemp_for(path, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        if validate is not None:
            validate(tmp)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise
