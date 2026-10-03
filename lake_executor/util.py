"""작은 공용 유틸: 시각, 해시, 수량/가격 라운딩, .env 파서."""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
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
