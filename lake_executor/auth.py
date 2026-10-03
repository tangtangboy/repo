"""HMAC-SHA256 서명 검증/생성. 원본 바이트 그대로 계산하며 JSON 재직렬화는 절대 하지 않는다.

수신 규격(lake_웹훅_수신안내 / 연동설명서 §4):
  X-Signature : hex(HMAC-SHA256(secret, raw_body)) 소문자 64자
  X-Timestamp : Unix ms, 본문 ts 와 정확히 일치, 수신 시각 ±max_clock_skew_ms 이내
"""
from __future__ import annotations

import hashlib
import hmac


class AuthError(Exception):
    """detail 은 외부로 돌려줄 짧은 코드 (시크릿/본문은 포함하지 않음)."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def sign(raw_body: bytes, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()


def verify(raw_body: bytes, signature_header: str | None, timestamp_header: str | None,
           body_ts: int | None, secret: str | None, now_ms: int, max_skew_ms: int) -> int:
    """검증 통과 시 timestamp(int) 반환, 실패 시 AuthError(code).

    code: SECRET_NOT_CONFIGURED | MISSING_SIGNATURE | MISSING_TIMESTAMP | BAD_TIMESTAMP
          | TIMESTAMP_SKEW | TIMESTAMP_MISMATCH | BAD_SIGNATURE
    """
    if not secret:
        raise AuthError("SECRET_NOT_CONFIGURED")
    if not signature_header:
        raise AuthError("MISSING_SIGNATURE")
    if not timestamp_header:
        raise AuthError("MISSING_TIMESTAMP")
    try:
        ts = int(str(timestamp_header).strip())
    except ValueError:
        raise AuthError("BAD_TIMESTAMP")
    if ts <= 0:
        raise AuthError("BAD_TIMESTAMP")
    if abs(now_ms - ts) > max_skew_ms:
        raise AuthError("TIMESTAMP_SKEW")
    if body_ts is not None and int(body_ts) != ts:
        raise AuthError("TIMESTAMP_MISMATCH")
    sig = str(signature_header).strip().lower()
    if len(sig) != 64:
        raise AuthError("BAD_SIGNATURE")
    expected = sign(raw_body, secret)
    if not hmac.compare_digest(expected, sig):
        raise AuthError("BAD_SIGNATURE")
    return ts


def headers_for(raw_body: bytes, secret: str, ts: int) -> dict[str, str]:
    """회신/테스트 신호 발송용 헤더."""
    return {
        "Content-Type": "application/json",
        "X-Signature": sign(raw_body, secret),
        "X-Timestamp": str(ts),
    }
