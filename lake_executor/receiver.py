"""HTTP 수신기 (ARCHITECTURE.md §2) — lake 웹훅 접수 + healthz + 관리 엔드포인트.

검증 순서 (POST {signal_path}):
  크기/인코딩(413/415) → JSON 파싱(400, 중복 키·NaN 거부) → 본문 mode 로 시크릿 선택(없으면 503)
  → auth.verify(401) → schemas.Signal(400) → 의미 검증 symbol / position_idx(400)
  → 만료 now_ms > expires_at_ms(410) → store.insert_signal
      new → 202 {"accepted":true,"event_id":...}
      duplicate → 200 {"accepted":true,"duplicate":true}
      conflict | sequence_conflict → 409 {"error":"CONFLICT","code":...}

규칙
  - 수신 경로는 DB 에만 쓴다(executor 직접 호출 없음). 2xx 는 접수 확인일 뿐이다.
  - 본문은 스트리밍으로 읽으며 max_body_bytes 를 넘는 순간 413 (Content-Length 없는 chunked 요청도 메모리 상한).
  - 검증·영속 접수는 스레드풀에서 돈다 (SQLite fsync 가 이벤트 루프를 막지 않도록).
  - **서명 전 거부(400 BAD_JSON/BAD_MODE, 401, 413, 415, 503)는 DB 에 쓰지 않고 메모리 카운터로만 센다**
    (인증 없는 요청 폭주가 디스크/락을 소모하지 못하게). 서명 통과 뒤 거부(400 SCHEMA/의미, 409, 410, 401 TIMESTAMP_MISMATCH)
    는 store.log_ingress(code, event_id, body) 로 남긴다(본문은 sha256 만). ingress_log 는 실행기가 주기적으로 정리한다.
  - 응답/INFO 로그에 시크릿·원본 본문·내부 예외 원문을 넣지 않는다. 로그에 찍는 event_id 는 ID 패턴을 통과한 값만.
  - 관리 엔드포인트는 X-Admin-Token(상수 시간 비교) 필요, ADMIN_TOKEN 미설정 시 404. 같은 IP 의 실패가 잦으면 잠시 비교 없이 401.
  - 서명 통과 뒤 같은 (mode, event_id)+같은 본문의 재수신(200 duplicate) 도 ingress_log 에 DUPLICATE 로 남긴다 (대시보드 추적용).
  - import 시 부작용 없음: create_app(settings, store, services) 로만 앱을 만든다.
  - 운영 대시보드(/ui, web.py) 는 create_app 끝에서 mount 되며 같은 admin_throttle 과 app.state.admin_ops(halt/resume/reconcile) 를 공유한다.

services: executor / reporter / alerts 핸들을 담은 간단한 객체 또는 dict (전부 선택).

2단계 (다계정, ARCHITECTURE_MULTI_EXCHANGE.md §6)
  - 의미 검증: position_idx 는 라우팅 대상 계정들(by_exchange 면 신호 exchange 와 일치하는 enabled 계정, fanout 이면 enabled
    전부)의 허용 집합 합집합. by_exchange 인데 일치하는 enabled 계정이 없으면 400 NO_TARGET_ACCOUNT (접수하지 않는다).
  - GET /healthz 의 inconsistent 는 모드별 '어느 계정이든 불일치' (모양 유지), protection_missing 은 모드별 lot 수.
  - GET /state 는 `accounts` 아래 계정별 섹션(오픈 lot, 거래소 포지션, 최근 run, 회신 상태, inconsistent) 을 더한다.
  - POST /admin/reconcile?mode=&account= — account 생략 시 그 모드에 거래소가 있는 모든 계정.
"""
from __future__ import annotations

import collections
import hmac
import ipaddress
import json
import logging
import os
import re
import threading
from types import SimpleNamespace
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

from . import auth, config as config_mod, store as st
from .schemas import ID_PATTERN, Signal
from .util import now_ms

log = logging.getLogger("lake_executor.receiver")

MODES = ("test", "live")
ADMIN_HEADER = "X-Admin-Token"
STATE_SIGNAL_LIMIT = 30
STATE_REPORT_LIMIT = 15
ID_RE = re.compile(ID_PATTERN)
# 관리 토큰 실패 제한: 같은 IP 가 창 안에서 이만큼 실패하면 창이 지날 때까지 비교 없이 401
ADMIN_FAIL_LIMIT = 10
ADMIN_FAIL_WINDOW_MS = 60_000


# --------------------------------------------------------------------------- #
# 수신 거부 예외 (내부용) — code 는 짧은 코드, 응답에 그대로 노출 가능
# --------------------------------------------------------------------------- #
class _Reject(Exception):
    """authenticated=True 인 거부만 DB(ingress_log) 에 남긴다. 서명 전 거부는 메모리 카운터로만."""

    def __init__(self, status: int, error: str, code: str | None = None, extra: dict | None = None,
                 authenticated: bool = False):
        super().__init__(f"{status} {error} {code or ''}".strip())
        self.status = status
        self.error = error
        self.code = code or error
        self.extra = extra or {}
        self.authenticated = authenticated

    def body(self) -> dict:
        out: dict[str, Any] = {"error": self.error, "code": self.code}
        out.update(self.extra)
        return out


# --------------------------------------------------------------------------- #
# JSON 파싱 — 중복 키·NaN/Infinity 거부
# --------------------------------------------------------------------------- #
class _DuplicateKey(ValueError):
    pass


def _no_duplicate_pairs(pairs: list[tuple[str, Any]]) -> dict:
    out: dict = {}
    for k, v in pairs:
        if k in out:
            raise _DuplicateKey(k)
        out[k] = v
    return out


def _reject_constant(name: str):
    raise ValueError(f"non-finite constant {name}")


def parse_signal_json(raw: bytes) -> dict:
    """원본 바이트를 한 번만 파싱. 중복 키 / NaN·Infinity / 비-객체 / 비-UTF-8 / 과도한 중첩 → ValueError."""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as e:
        raise ValueError("body is not valid UTF-8") from e
    try:
        obj = json.loads(text, object_pairs_hook=_no_duplicate_pairs, parse_constant=_reject_constant)
    except RecursionError as e:
        raise ValueError("body nested too deeply") from e
    if not isinstance(obj, dict):
        raise ValueError("body must be a JSON object")
    return obj


class IngressStats:
    """서명 전 거부 등 DB 에 쓰지 않는 수신 결과의 메모리 카운터 (/state 에 노출)."""

    def __init__(self):
        self._lock = threading.Lock()
        self.counts: dict[str, int] = collections.defaultdict(int)

    def bump(self, code: str) -> None:
        with self._lock:
            self.counts[code] += 1

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return dict(self.counts)


class _AdminThrottle:
    """IP 별 관리 토큰 실패 횟수 (창 안에서 ADMIN_FAIL_LIMIT 초과 시 비교 생략)."""

    def __init__(self):
        self._lock = threading.Lock()
        self._fails: dict[str, collections.deque] = {}

    def blocked(self, ip: str) -> bool:
        now = now_ms()
        with self._lock:
            dq = self._fails.get(ip)
            if not dq:
                return False
            while dq and now - dq[0] > ADMIN_FAIL_WINDOW_MS:
                dq.popleft()
            return len(dq) >= ADMIN_FAIL_LIMIT

    def fail(self, ip: str) -> None:
        with self._lock:
            if len(self._fails) > 1024:
                self._fails.clear()
            self._fails.setdefault(ip, collections.deque()).append(now_ms())


def _peer_is_loopback(request: Request) -> bool:
    """TCP 피어가 루프백(127.0.0.0/8, ::1) 인가. Caddy 는 같은 호스트에서 127.0.0.1:8787 로 프록시하므로
    X-Forwarded-* 헤더는 이 경우에만 믿는다(아니면 누구나 헤더로 IP/프로토콜을 꾸밀 수 있다)."""
    host = request.client.host if request.client else None
    if not host:
        return False
    try:
        return ipaddress.ip_address(host.split("%", 1)[0]).is_loopback
    except ValueError:
        return False


def _client_ip(request: Request) -> str:
    """속도 제한·감사 로그에 쓰는 클라이언트 IP. X-Forwarded-For 의 첫 항목은 피어가 루프백(=Caddy) 일 때만 쓴다.
    listen.host 가 루프백이 아니면 피어 주소를 그대로 쓴다 (헤더를 꾸며도 예산을 피할 수 없다)."""
    host = (request.client.host if request.client else None) or ""
    xff = request.headers.get("x-forwarded-for")
    if xff and _peer_is_loopback(request):
        first = xff.split(",")[0].strip()
        if first:
            return first
    return host or "?"


# --------------------------------------------------------------------------- #
# 도우미
# --------------------------------------------------------------------------- #
def _svc(services: Any, name: str):
    """services 가 dict 든 객체든 name 항목을 꺼낸다 (없으면 None)."""
    if services is None:
        return None
    if isinstance(services, dict):
        return services.get(name)
    return getattr(services, name, None)


def _halted(settings: Any) -> bool:
    try:
        return os.path.exists(settings.halt_file)
    except Exception:  # noqa: BLE001
        return False


def _write_halt(settings: Any, on: bool, note: str = "") -> None:
    """HALT 파일 생성/삭제. ops.halt/resume 가 있으면 그걸 쓴다(동일 의미)."""
    try:
        from . import ops  # 지연 import: receiver 는 ops 없이도 동작
    except Exception:  # noqa: BLE001
        ops = None  # type: ignore[assignment]
    if ops is not None and hasattr(ops, "halt") and hasattr(ops, "resume"):
        if on:
            ops.halt(settings, note)
        else:
            ops.resume(settings)
        return
    path = settings.halt_file
    if on:
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(f"halted_at_ms={now_ms()}\n{note[:500]}\n")
    else:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


def _account_names(settings: Any) -> list[str]:
    accts = getattr(settings, "accounts", None) or []
    return [a.name for a in accts] or [st.DEFAULT_ACCOUNT]


def _inconsistent_map(store: st.Store, settings: Any) -> dict[str, bool]:
    """모드별 '어느 계정이든 불일치' (1단계 모양 유지)."""
    names = _account_names(settings)
    return {m: any(bool(store.is_inconsistent(m, a)) for a in names) for m in MODES}


def _inconsistent_notes(store: st.Store, settings: Any) -> dict[str, str]:
    out: dict[str, str] = {}
    for m in MODES:
        parts = []
        for a in _account_names(settings):
            note = store.inconsistent_note(m, a)
            if note:
                parts.append(f"{a}: {note}")
        out[m] = "; ".join(parts)
    return out


def _target_accounts_for(settings: Any, sig: Signal) -> list:
    """의미 검증용 대상 계정: by_exchange 면 신호 exchange 와 일치하는 enabled 계정, fanout 이면 enabled 전부."""
    route = getattr(settings, "route_accounts", None)
    if callable(route):
        try:
            return list(route(sig.exchange))
        except Exception:  # noqa: BLE001
            return []
    return list(getattr(settings, "accounts", None) or [])


def _json(status: int, body: dict) -> JSONResponse:
    return JSONResponse(status_code=status, content=body)


def _expired_actions_execute(settings: Any) -> set[str]:
    """만료 뒤에도 접수/실행할 action 집합 (config guards.expired_actions_execute)."""
    try:
        return {str(a) for a in (getattr(settings, "expired_actions_execute", None) or ())}
    except TypeError:
        return set()


def _content_length(request: Request) -> int | None:
    v = request.headers.get("content-length")
    if v is None:
        return None
    try:
        return int(v)
    except ValueError:
        return None


def _is_json_content_type(request: Request) -> bool:
    ct = request.headers.get("content-type")
    if ct is None or ct.strip() == "":
        return False  # 계약(§2): Content-Type: application/json 필수
    mt = ct.split(";", 1)[0].strip().lower()
    return mt == "application/json" or mt.endswith("+json")


async def _read_body_capped(request: Request, max_body: int) -> bytes | None:
    """본문을 스트리밍으로 읽되 max_body 를 넘는 순간 중단(None). chunked 요청도 메모리 상한을 지킨다."""
    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        if not chunk:
            continue
        total += len(chunk)
        if total > max_body:
            return None
        chunks.append(chunk)
    return b"".join(chunks)


def _schema_error_summary(e: Exception) -> list[str]:
    """pydantic 오류에서 필드 경로만 추린다 (입력 값은 포함하지 않음)."""
    out: list[str] = []
    errors = getattr(e, "errors", None)
    try:
        items = errors() if callable(errors) else []
    except Exception:  # noqa: BLE001
        items = []
    for it in items[:20]:
        loc = ".".join(str(x) for x in (it.get("loc") or ())) or "body"
        typ = str(it.get("type") or "invalid")
        out.append(f"{loc}:{typ}")
    return out


# --------------------------------------------------------------------------- #
# 앱 팩토리
# --------------------------------------------------------------------------- #
def create_app(settings: Any, store: st.Store, services: Any = None) -> FastAPI:
    app = FastAPI(title="lake-executor", docs_url=None, redoc_url=None, openapi_url=None)
    app.state.settings = settings
    app.state.store = store
    app.state.services = services

    signal_path = str(getattr(settings, "signal_path", "/lake/signal") or "/lake/signal")
    max_body = int(getattr(settings, "max_body_bytes", 65536))
    max_skew = int(getattr(settings, "max_clock_skew_ms", 60000))
    # 신호 symbol 은 lake 표준 심볼 (BTCUSDT). 계정 네이티브 심볼(BTC-USDT-SWAP 등) 과 무관.
    lake_symbol = str(getattr(config_mod, "LAKE_SYMBOL", None) or getattr(settings, "symbol", "BTCUSDT") or "BTCUSDT")
    account_names = _account_names(settings)

    # ------------------------------------------------------------------ 공통 오류 처리
    @app.exception_handler(Exception)
    async def _unhandled(_request: Request, exc: Exception):  # pragma: no cover - 안전망
        log.error("unhandled error: %s", type(exc).__name__)
        return _json(500, {"error": "INTERNAL"})

    # ------------------------------------------------------------------ 신호 수신
    stats = IngressStats()
    app.state.ingress_stats = stats
    admin_throttle = _AdminThrottle()
    app.state.admin_throttle = admin_throttle   # /ui/login 과 같은 IP 별 실패 예산을 공유한다 (web.py)

    def _log_ingress(code: str, event_id: str | None, body: bytes | None, status: int) -> None:
        try:
            store.log_ingress(code, event_id, body, note=f"http={status}")
        except Exception as e:  # noqa: BLE001 - 기록 실패가 응답을 막지 않는다
            log.warning("log_ingress failed: %s", type(e).__name__)

    def _reject_response(r: _Reject, raw: bytes | None) -> JSONResponse:
        ev = r.extra.get("event_id")
        ev = ev if isinstance(ev, str) and ID_RE.fullmatch(ev) else None
        if r.authenticated:
            _log_ingress(r.code, ev, raw, r.status)
            log.info("signal rejected http=%s code=%s event_id=%s", r.status, r.code, ev)
        else:
            stats.bump(r.code)
            log.info("signal rejected (pre-auth) http=%s code=%s", r.status, r.code)
        return _json(r.status, r.body())

    def _validate(raw: bytes, headers: dict[str, str | None]) -> tuple[Signal, dict]:
        """검증 통과 시 (Signal, parsed dict). 실패 시 _Reject."""
        # 1) JSON 파싱 (한 번만)
        try:
            data = parse_signal_json(raw)
        except _DuplicateKey:
            raise _Reject(400, "BAD_JSON", "DUPLICATE_KEY")
        except ValueError:
            raise _Reject(400, "BAD_JSON", "PARSE_ERROR")

        # 서명 전 단계에서 응답/로그에 되돌리는 event_id 는 ID 패턴을 통과한 값만 (로그 주입 방지)
        event_id = data.get("event_id")
        event_id = event_id if isinstance(event_id, str) and ID_RE.fullmatch(event_id) else None

        # 2) mode → 시크릿 선택
        mode = data.get("mode")
        if mode not in MODES:
            raise _Reject(400, "INVALID_SIGNAL", "BAD_MODE", {"event_id": event_id})
        secret = (getattr(settings.secrets, "signal_secret", None) or {}).get(mode)
        if not secret:
            raise _Reject(503, "SECRET_NOT_CONFIGURED", "SECRET_NOT_CONFIGURED", {"mode": mode, "event_id": event_id})

        # 3) 서명/시각 검증 (원본 바이트 그대로). 본문 ts 가 정수가 아니면 서명 확인 뒤 TIMESTAMP_MISMATCH
        body_ts = data.get("ts")
        ts_is_int = isinstance(body_ts, int) and not isinstance(body_ts, bool)
        try:
            auth.verify(raw, headers.get("x-signature"), headers.get("x-timestamp"),
                        body_ts if ts_is_int else None, secret, now_ms(), max_skew)
        except auth.AuthError as e:
            if e.code == "SECRET_NOT_CONFIGURED":
                raise _Reject(503, "SECRET_NOT_CONFIGURED", e.code, {"mode": mode, "event_id": event_id})
            raise _Reject(401, "UNAUTHORIZED", e.code, {"event_id": event_id})
        if not ts_is_int:
            raise _Reject(401, "UNAUTHORIZED", "TIMESTAMP_MISMATCH", {"event_id": event_id}, authenticated=True)

        # 4) 스키마
        try:
            sig = Signal.model_validate(data)
        except Exception as e:  # pydantic ValidationError (값은 노출하지 않음)
            raise _Reject(400, "INVALID_SIGNAL", "SCHEMA",
                          {"event_id": event_id, "fields": _schema_error_summary(e)}, authenticated=True)

        # 5) 의미 검증 — symbol 은 lake 표준(BTCUSDT), position_idx 는 대상 계정들의 허용 집합 합집합
        if sig.symbol != lake_symbol:
            raise _Reject(400, "INVALID_SIGNAL", "SYMBOL_MISMATCH", {"event_id": sig.event_id}, authenticated=True)
        targets = _target_accounts_for(settings, sig)
        if getattr(settings, "routing", "fanout") == "by_exchange" and not targets:
            raise _Reject(400, "INVALID_SIGNAL", "NO_TARGET_ACCOUNT",
                          {"event_id": sig.event_id, "exchange": sig.exchange}, authenticated=True)
        allowed: set[int] = set()
        for a in targets:
            try:
                allowed |= set(a.allowed_position_idx())
            except Exception:  # noqa: BLE001
                pass
        if not allowed:
            allowed = set(settings.allowed_position_idx())
        if sig.position_idx not in allowed:
            raise _Reject(400, "INVALID_SIGNAL", "POSITION_MODE_MISMATCH", {"event_id": sig.event_id},
                          authenticated=True)

        # 6) 만료 — guards.expired_actions_execute 에 든 action(기본 partial_exit/full_exit/protection_update) 은
        #    늦게 도착해도 접수한다 (우리 서버 끊김 동안 lake 가 재전송한 청산/보호가격 변경을 버리지 않는다; 실행기가 stale 로 처리)
        if now_ms() > int(sig.expires_at_ms) and sig.action.value not in _expired_actions_execute(settings):
            raise _Reject(410, "EXPIRED", "EXPIRED", {"event_id": sig.event_id}, authenticated=True)
        return sig, data

    def _handle_signal(raw: bytes, headers: dict[str, str | None]) -> JSONResponse:
        """스레드풀에서 실행: 검증 → 영속 접수. 어떤 예외도 500 으로 새지 않게 한다."""
        try:
            sig, _data = _validate(raw, headers)
        except _Reject as r:
            return _reject_response(r, raw)
        except Exception as e:  # noqa: BLE001 - 예상 못 한 본문 → 400 (500/트레이스백 금지)
            log.warning("signal validation error: %s", type(e).__name__)
            return _reject_response(_Reject(400, "INVALID_SIGNAL", "UNPROCESSABLE"), raw)

        # 7) 영속 접수
        received_at = now_ms()
        try:
            result = store.insert_signal(sig, raw, received_at)
        except st.LedgerUnavailable as e:
            # 원장(Postgres) 불통: 접수하지 못했으니 503 + Retry-After — lake 는 재전송한다 (2xx 가 아니므로 접수된 게 아님)
            log.error("insert_signal: ledger unavailable (%s)", e)
            resp = _json(503, {"error": "LEDGER_UNAVAILABLE", "code": "LEDGER_UNAVAILABLE"})
            resp.headers["Retry-After"] = "5"
            return resp
        except Exception as e:  # noqa: BLE001 - DB 장애: 상대는 재전송해야 하므로 5xx
            log.error("insert_signal failed: %s", type(e).__name__)
            return _json(500, {"error": "INTERNAL", "code": "STORE_ERROR"})

        if result == "new":                                   # 실행기를 깨운다 (폴링 대기 없이 바로 처리)
            ex = _svc(services, "executor")
            wake = getattr(ex, "wake", None)
            if wake is not None:
                wake.set()

        # 라이브 신호 로그 (백테스트용): 큐에 넣기만 한다 — 주문 경로를 기다리게 하지 않고, 실패해도 응답에 영향 없음
        if result in ("new", "duplicate"):
            slog = _svc(services, "signal_log")
            if slog is not None:
                try:
                    slog.record(sig, result, received_at)
                except Exception as e:  # noqa: BLE001
                    log.warning("signal_log.record failed: %s", type(e).__name__)

        if result == "new":
            log.info("signal accepted mode=%s event_id=%s position_id=%s seq=%s action=%s",
                     sig.mode.value, sig.event_id, sig.position_id, sig.event_sequence, sig.action.value)
            return _json(202, {"accepted": True, "duplicate": False, "mode": sig.mode.value, "event_id": sig.event_id})
        if result == "duplicate":
            _log_ingress("DUPLICATE", sig.event_id, raw, 200)   # 서명 통과 뒤이므로 DB 기록 (대시보드 추적용)
            log.info("signal duplicate mode=%s event_id=%s", sig.mode.value, sig.event_id)
            return _json(200, {"accepted": True, "duplicate": True, "mode": sig.mode.value, "event_id": sig.event_id})
        code = "EVENT_ID_CONFLICT" if result == "conflict" else "SEQUENCE_CONFLICT"
        _log_ingress(code, sig.event_id, raw, 409)
        log.warning("signal conflict code=%s mode=%s event_id=%s position_id=%s seq=%s",
                    code, sig.mode.value, sig.event_id, sig.position_id, sig.event_sequence)
        return _json(409, {"error": "CONFLICT", "code": code})

    @app.post(signal_path)
    async def receive_signal(request: Request):
        # 인코딩/타입/크기 — 본문을 읽기 전에 거를 수 있는 것부터 (서명 전 거부: DB 에 쓰지 않는다)
        enc = (request.headers.get("content-encoding") or "").strip().lower()
        if enc and enc != "identity":
            stats.bump("UNSUPPORTED_ENCODING")
            return _json(415, {"error": "UNSUPPORTED_MEDIA_TYPE", "code": "CONTENT_ENCODING_NOT_ALLOWED"})
        if not _is_json_content_type(request):
            stats.bump("UNSUPPORTED_MEDIA_TYPE")
            return _json(415, {"error": "UNSUPPORTED_MEDIA_TYPE", "code": "CONTENT_TYPE_NOT_JSON"})
        cl = _content_length(request)
        if cl is not None and cl > max_body:
            stats.bump("PAYLOAD_TOO_LARGE")
            return _json(413, {"error": "PAYLOAD_TOO_LARGE", "code": "PAYLOAD_TOO_LARGE", "max_bytes": max_body})

        raw = await _read_body_capped(request, max_body)
        if raw is None:
            stats.bump("PAYLOAD_TOO_LARGE")
            return _json(413, {"error": "PAYLOAD_TOO_LARGE", "code": "PAYLOAD_TOO_LARGE", "max_bytes": max_body})

        headers = {"x-signature": request.headers.get("x-signature"), "x-timestamp": request.headers.get("x-timestamp")}
        return await run_in_threadpool(_handle_signal, raw, headers)

    # ------------------------------------------------------------------ healthz
    def _protection_missing_map(executor: Any, account: str | None = None) -> dict[str, list[str]]:
        """모드별 보호주문 누락 lot (account=None 이면 모든 계정)."""
        out: dict[str, list[str]] = {}
        for m in MODES:
            try:
                out[m] = list(executor.protection_missing(m, account)) if executor is not None else []
            except Exception as e:  # noqa: BLE001
                log.warning("protection_missing(%s) failed: %s", m, type(e).__name__)
                out[m] = []
        return out

    @app.get("/healthz")
    def healthz():
        try:
            inconsistent = _inconsistent_map(store, settings)
            missing = _protection_missing_map(_executor())
        except Exception as e:  # noqa: BLE001
            log.warning("healthz store error: %s", type(e).__name__)
            return _json(503, {"ok": False})
        return _json(200, {"ok": True, "halted": _halted(settings), "inconsistent": inconsistent,
                           "protection_missing": {m: len(v) for m, v in missing.items()}})

    # ------------------------------------------------------------------ 관리
    def _executor():
        return _svc(services, "executor")

    def _admin_gate(request: Request) -> JSONResponse | None:
        """ADMIN_TOKEN 미설정 → 404, 토큰 불일치 → 401(IP 별 실패 제한), 통과 → None."""
        token = str(getattr(settings.secrets, "admin_token", "") or "")
        if not token:
            return _json(404, {"error": "NOT_FOUND"})
        ip = _client_ip(request)
        if admin_throttle.blocked(ip):
            return _json(401, {"error": "UNAUTHORIZED"})
        given = request.headers.get(ADMIN_HEADER) or ""
        if not given or not hmac.compare_digest(given.encode("utf-8"), token.encode("utf-8")):
            admin_throttle.fail(ip)
            log.warning("admin: bad token from %s", ip)
            return _json(401, {"error": "UNAUTHORIZED"})
        return None

    def _exchange_of(executor: Any, mode: str, account: str) -> Any:
        exchanges = getattr(executor, "exchanges", None) or {}
        by_mode = exchanges.get(mode) if isinstance(exchanges, dict) else None
        if isinstance(by_mode, dict):
            return by_mode.get(account)
        return by_mode if account == account_names[0] else None   # 1단계 모양 호환

    def _exchange_positions(executor: Any, mode: str, account: str) -> Any:
        """거래소 포지션 요약 (executor.exchanges[mode][account].positions()). 없거나 실패하면 None."""
        ex = _exchange_of(executor, mode, account) if executor is not None else None
        if ex is None:
            return None
        try:
            return {str(k): v for k, v in (ex.positions() or {}).items()}
        except Exception as e:  # noqa: BLE001
            log.warning("state: positions(%s/%s) failed: %s", mode, account, type(e).__name__)
            return {"error": type(e).__name__}

    def _snapshot_positions(executor: Any, mode: str, account: str) -> Any:
        if executor is None:
            return None
        try:
            return executor.build_snapshot_positions(mode, account)
        except Exception as e:  # noqa: BLE001
            log.warning("state: build_snapshot_positions(%s/%s) failed: %s", mode, account, type(e).__name__)
            return {"error": type(e).__name__}

    def _account_section(executor: Any, acct: Any) -> dict:
        """/state 의 계정별 섹션."""
        name = acct.name
        ok, reason = settings.live_execution_possible(acct)
        sec: dict[str, Any] = {
            "exchange": acct.exchange,
            "display_name": acct.display_name,
            "symbol": acct.symbol,
            "enabled": bool(acct.enabled),
            "report": bool(acct.report),
            "position_mode": acct.position_mode,
            "leverage": acct.leverage,
            "margin_mode": acct.margin_mode,
            "testnet": bool(acct.testnet),
            "qty_multiplier": acct.qty_multiplier,
            "has_real_keys": bool(acct.has_real_keys()),
            "live_execution_possible": ok,
            "live_block_reason": reason or None,
            "report_url_configured": {m: bool((acct.report_url or {}).get(m)) for m in MODES},
            "modes": {},
        }
        for m in MODES:
            sec["modes"][m] = {
                "exchange_ready": _exchange_of(executor, m, name) is not None,
                "inconsistent": bool(store.is_inconsistent(m, name)),
                "inconsistent_note": store.inconsistent_note(m, name),
                "open_lots": store.open_lots(m, name),
                "reports": store.recent_reports(m, name, STATE_REPORT_LIMIT),
                "runs": store.recent_runs(m, name, STATE_SIGNAL_LIMIT),
                "protection_missing": _protection_missing_map(executor, name)[m],
                "snapshot_positions": _snapshot_positions(executor, m, name),
                "exchange_positions": _exchange_positions(executor, m, name),
            }
        return sec

    @app.get("/state")
    def state(request: Request):
        denied = _admin_gate(request)
        if denied is not None:
            return denied
        executor = _executor()
        ok, reason = settings.live_execution_possible()
        accounts = list(getattr(settings, "accounts", None) or [])
        out: dict[str, Any] = {
            "now_ms": now_ms(),
            "symbol": settings.symbol,
            "position_mode": settings.position_mode,
            "routing": getattr(settings, "routing", "fanout"),
            "halted": _halted(settings),
            "live_execution_possible": ok,
            "live_block_reason": reason or None,
            "test_simulate_fills": bool(getattr(settings, "test_simulate_fills", False)),
            "inconsistent": _inconsistent_map(store, settings),
            "inconsistent_note": _inconsistent_notes(store, settings),
            "signals": store.recent_signals(STATE_SIGNAL_LIMIT),
            "reports": {m: store.recent_reports(m, None, STATE_REPORT_LIMIT) for m in MODES},
            "open_lots": {m: store.open_lots(m) for m in MODES},
            "protection_missing": _protection_missing_map(executor),
            "ingress_rejections": stats.snapshot(),
            "snapshot_positions": {m: {} for m in MODES},
            "exchange_positions": {m: {} for m in MODES},
            "accounts": {},
        }
        for acct in accounts:
            try:
                out["accounts"][acct.name] = _account_section(executor, acct)
            except Exception as e:  # noqa: BLE001
                log.warning("state: account section %s failed: %s", acct.name, type(e).__name__)
                out["accounts"][acct.name] = {"error": type(e).__name__}
                continue
            for m in MODES:
                out["snapshot_positions"][m][acct.name] = out["accounts"][acct.name]["modes"][m]["snapshot_positions"]
                out["exchange_positions"][m][acct.name] = out["accounts"][acct.name]["modes"][m]["exchange_positions"]
        return _json(200, out)

    def _alert(text: str) -> None:
        alerts = _svc(services, "alerts")
        if alerts is not None:
            try:
                alerts.send(text)
            except Exception:  # noqa: BLE001
                pass

    # ---- 관리 동작 본체 (JSON 엔드포인트와 /ui 대시보드가 공유; source 는 "/admin/halt" | "dashboard" 등 출처 표시) ----
    def _do_halt(source: str) -> dict:
        _write_halt(settings, True, note=source)
        log.warning("admin: HALT set")
        _alert(f"[admin] HALT set via {source}")
        return {"ok": True, "halted": True}

    def _do_resume(source: str) -> dict:
        _write_halt(settings, False)
        log.warning("admin: HALT cleared")
        _alert(f"[admin] HALT cleared via {source}")
        return {"ok": True, "halted": False}

    def _do_reconcile(mode: str, account: str | None) -> tuple[int, dict]:
        """(http status, body). mode ∉ MODES / 모르는 account → 400, executor 없음 → 503, 대사 예외 → 500."""
        if mode not in MODES:
            return 400, {"error": "BAD_REQUEST", "code": "BAD_MODE"}
        if account is not None and account not in account_names:
            return 400, {"error": "BAD_REQUEST", "code": "BAD_ACCOUNT"}
        executor = _executor()
        if executor is None:
            return 503, {"error": "EXECUTOR_UNAVAILABLE"}
        if account is None:
            names = [n for n in account_names if _exchange_of(executor, mode, n) is not None]
        else:
            names = [account]
        results: dict[str, Any] = {}
        all_ok = bool(names)
        for name in names:
            try:
                consistent = bool(executor.reconcile(mode, name))
            except Exception as e:  # noqa: BLE001
                log.error("admin reconcile(%s/%s) failed: %s", mode, name, type(e).__name__)
                return 500, {"error": "INTERNAL", "code": "RECONCILE_FAILED", "account": name}
            all_ok = all_ok and consistent
            results[name] = {
                "consistent": consistent,
                "inconsistent": bool(store.is_inconsistent(mode, name)),
                "inconsistent_note": store.inconsistent_note(mode, name),
                "positions": _snapshot_positions(executor, mode, name),
                "exchange_positions": _exchange_positions(executor, mode, name),
            }
            log.warning("admin: reconcile(%s/%s) -> consistent=%s", mode, name, consistent)
        return 200, {
            "ok": all_ok,
            "mode": mode,
            "account": account,
            "consistent": all_ok,
            "inconsistent": any(r["inconsistent"] for r in results.values()),
            "inconsistent_note": "; ".join(f"{n}: {r['inconsistent_note']}" for n, r in results.items() if r["inconsistent_note"]),
            "accounts": results,
            "positions": {n: r["positions"] for n, r in results.items()},
            "exchange_positions": {n: r["exchange_positions"] for n, r in results.items()},
        }

    app.state.admin_ops = SimpleNamespace(halt=_do_halt, resume=_do_resume, reconcile=_do_reconcile)

    @app.post("/admin/halt")
    def admin_halt(request: Request):
        denied = _admin_gate(request)
        if denied is not None:
            return denied
        return _json(200, _do_halt("/admin/halt"))

    @app.post("/admin/resume")
    def admin_resume(request: Request):
        denied = _admin_gate(request)
        if denied is not None:
            return denied
        return _json(200, _do_resume("/admin/resume"))

    @app.post("/admin/reconcile")
    def admin_reconcile(request: Request, mode: str = "live", account: str | None = None):
        """?mode=test|live[&account=name] — account 생략 시 그 모드에 거래소가 있는 모든 계정을 차례로 대사."""
        denied = _admin_gate(request)
        if denied is not None:
            return denied
        status, body = _do_reconcile(mode, account)
        return _json(status, body)

    # ------------------------------------------------------------------ 운영 대시보드 (/ui)
    from .web import mount  # 함수 안 import: web → receiver 순환 import 회피
    mount(app, settings, store, services, admin_throttle)

    return app
