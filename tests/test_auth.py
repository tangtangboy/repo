"""auth.py — HMAC 서명/시각 검증 (ARCHITECTURE.md §9: 정상/서명 오류/시각 창/헤더-본문 ts 불일치/시크릿 미설정)."""
from __future__ import annotations

import hashlib
import hmac

import pytest

from lake_executor import auth

SECRET = "unit-test-secret-0123456789abcdef-0123456789"
BODY = b'{"ts":1791024000000,"x":1}'
TS = 1791024000000
SKEW = 60000


_AUTO = object()  # sig 기본값: 올바른 서명을 계산


def _verify(raw=BODY, sig=_AUTO, ts_header=str(TS), body_ts=TS, secret=SECRET, now=TS + 1000, skew=SKEW):
    if sig is _AUTO:
        sig = auth.sign(raw, secret)
    return auth.verify(raw, sig, ts_header, body_ts, secret, now, skew)


def _code(exc_info) -> str:
    return exc_info.value.code


# ---------------------------------------------------------------- sign / headers_for
def test_sign_is_lowercase_hex_hmac_sha256_over_raw_bytes():
    sig = auth.sign(BODY, SECRET)
    assert len(sig) == 64
    assert sig == sig.lower()
    assert sig == hmac.new(SECRET.encode(), BODY, hashlib.sha256).hexdigest()
    # 바이트가 1 비트라도 다르면 다른 서명 (재직렬화 금지 규칙의 근거)
    assert auth.sign(BODY + b" ", SECRET) != sig


def test_headers_for_matches_sign_and_timestamp():
    h = auth.headers_for(BODY, SECRET, TS)
    assert h["Content-Type"] == "application/json"
    assert h["X-Signature"] == auth.sign(BODY, SECRET)
    assert h["X-Timestamp"] == str(TS)
    # headers_for 로 만든 헤더는 verify 를 통과한다
    assert auth.verify(BODY, h["X-Signature"], h["X-Timestamp"], TS, SECRET, TS + 5, SKEW) == TS


# ---------------------------------------------------------------- 정상
def test_verify_ok_returns_timestamp():
    assert _verify() == TS


def test_verify_accepts_uppercase_hex_signature():
    assert _verify(sig=auth.sign(BODY, SECRET).upper()) == TS


def test_verify_accepts_padded_timestamp_header():
    assert _verify(ts_header=f"  {TS} ") == TS


def test_verify_skips_body_ts_check_when_body_ts_none():
    assert _verify(body_ts=None) == TS


def test_verify_boundary_of_clock_window_is_allowed():
    assert _verify(now=TS + SKEW) == TS
    assert _verify(now=TS - SKEW) == TS


# ---------------------------------------------------------------- 서명 오류
def test_wrong_secret_is_bad_signature():
    with pytest.raises(auth.AuthError) as ei:
        _verify(sig=auth.sign(BODY, "other-secret-0123456789abcdef-0123456789"))
    assert _code(ei) == "BAD_SIGNATURE"


def test_tampered_body_is_bad_signature():
    with pytest.raises(auth.AuthError) as ei:
        _verify(raw=BODY.replace(b'"x":1', b'"x":2'), sig=auth.sign(BODY, SECRET))
    assert _code(ei) == "BAD_SIGNATURE"


def test_signature_with_wrong_length_is_bad_signature():
    with pytest.raises(auth.AuthError) as ei:
        _verify(sig=auth.sign(BODY, SECRET)[:63])
    assert _code(ei) == "BAD_SIGNATURE"


def test_missing_signature_header():
    for missing in (None, ""):
        with pytest.raises(auth.AuthError) as ei:
            _verify(sig=missing)
        assert _code(ei) == "MISSING_SIGNATURE"


# ---------------------------------------------------------------- 시각 창
def test_timestamp_outside_window_is_skew():
    with pytest.raises(auth.AuthError) as ei:
        _verify(now=TS + SKEW + 1)
    assert _code(ei) == "TIMESTAMP_SKEW"
    with pytest.raises(auth.AuthError) as ei:
        _verify(now=TS - SKEW - 1)
    assert _code(ei) == "TIMESTAMP_SKEW"


def test_timestamp_header_body_mismatch():
    with pytest.raises(auth.AuthError) as ei:
        _verify(ts_header=str(TS + 1), body_ts=TS, now=TS + 1)
    assert _code(ei) == "TIMESTAMP_MISMATCH"


def test_missing_or_bad_timestamp_header():
    for missing in (None, ""):
        with pytest.raises(auth.AuthError) as ei:
            _verify(ts_header=missing)
        assert _code(ei) == "MISSING_TIMESTAMP"
    for bad in ("abc", "12.5", "0", "-5"):
        with pytest.raises(auth.AuthError) as ei:
            _verify(ts_header=bad)
        assert _code(ei) == "BAD_TIMESTAMP"


# ---------------------------------------------------------------- 시크릿 미설정
def test_secret_not_configured_takes_precedence():
    for secret in (None, ""):
        with pytest.raises(auth.AuthError) as ei:
            _verify(secret=secret, sig="", ts_header=None)
        assert _code(ei) == "SECRET_NOT_CONFIGURED"


def test_auth_error_message_is_only_the_code():
    """AuthError 문자열에 시크릿/본문이 섞이지 않는다."""
    with pytest.raises(auth.AuthError) as ei:
        _verify(sig=auth.sign(BODY, "wrong-secret-0123456789abcdef-0123456789"))
    text = str(ei.value)
    assert text == "BAD_SIGNATURE"
    assert SECRET not in text
