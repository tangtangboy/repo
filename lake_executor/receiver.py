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
  - import 시 부작용 없음: create_app(settings, store, services) 로만 앱을 만든다.

services: executor / reporter / alerts 핸들을 담은 간단한 객체 또는 dict (전부 선택).
"""
from __future__ import annotations

import collections
import hmac
import json
import logging
import os
import re
import threading
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

from . import auth, store as st
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


def _client_ip(request: Request) -> str:
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip() or "?"
    return (request.client.host if request.client else None) or "?"


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


def _inconsistent_map(store: st.Store) -> dict[str, bool]:
    return {m: bool(store.is_inconsistent(m)) for m in MODES}


def _json(status: int, body: dict) -> JSONResponse:
    return JSONResponse(status_code=status, content=body)


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

    # ------------------------------------------------------------------ 공통 오류 처리
    @app.exception_handler(Exception)
    async def _unhandled(_request: Request, exc: Exception):  # pragma: no cover - 안전망
        log.error("unhandled error: %s", type(exc).__name__)
        return _json(500, {"error": "INTERNAL"})

    # ------------------------------------------------------------------ 신호 수신
    stats = IngressStats()
    app.state.ingress_stats = stats
    admin_throttle = _AdminThrottle()

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

        # 5) 의미 검증
        if sig.symbol != settings.symbol:
            raise _Reject(400, "INVALID_SIGNAL", "SYMBOL_MISMATCH", {"event_id": sig.event_id}, authenticated=True)
        if sig.position_idx not in settings.allowed_position_idx():
            raise _Reject(400, "INVALID_SIGNAL", "POSITION_MODE_MISMATCH", {"event_id": sig.event_id},
                          authenticated=True)

        # 6) 만료
        if now_ms() > int(sig.expires_at_ms):
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
        try:
            result = store.insert_signal(sig, raw, now_ms())
        except Exception as e:  # noqa: BLE001 - DB 장애: 상대는 재전송해야 하므로 5xx
            log.error("insert_signal failed: %s", type(e).__name__)
            return _json(500, {"error": "INTERNAL", "code": "STORE_ERROR"})

        if result == "new":
            log.info("signal accepted mode=%s event_id=%s position_id=%s seq=%s action=%s",
                     sig.mode.value, sig.event_id, sig.position_id, sig.event_sequence, sig.action.value)
            return _json(202, {"accepted": True, "event_id": sig.event_id})
        if result == "duplicate":
            log.info("signal duplicate mode=%s event_id=%s", sig.mode.value, sig.event_id)
            return _json(200, {"accepted": True, "duplicate": True})
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
    def _protection_missing_map(executor: Any) -> dict[str, list[str]]:
        out: dict[str, list[str]] = {}
        for m in MODES:
            try:
                out[m] = list(executor.protection_missing(m)) if executor is not None else []
            except Exception as e:  # noqa: BLE001
                log.warning("protection_missing(%s) failed: %s", m, type(e).__name__)
                out[m] = []
        return out

    @app.get("/healthz")
    def healthz():
        try:
            inconsistent = _inconsistent_map(store)
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

    def _exchange_positions(executor: Any, mode: str) -> Any:
        """거래소 포지션 요약 (executor.exchanges[mode].positions()). 없거나 실패하면 None."""
        exchanges = getattr(executor, "exchanges", None) or {}
        ex = exchanges.get(mode) if isinstance(exchanges, dict) else None
        if ex is None:
            return None
        try:
            return {str(k): v for k, v in (ex.positions() or {}).items()}
        except Exception as e:  # noqa: BLE001
            log.warning("state: positions(%s) failed: %s", mode, type(e).__name__)
            return {"error": type(e).__name__}

    @app.get("/state")
    def state(request: Request):
        denied = _admin_gate(request)
        if denied is not None:
            return denied
        executor = _executor()
        ok, reason = settings.live_execution_possible()
        out: dict[str, Any] = {
            "now_ms": now_ms(),
            "symbol": settings.symbol,
            "position_mode": settings.position_mode,
            "halted": _halted(settings),
            "live_execution_possible": ok,
            "live_block_reason": reason or None,
            "test_simulate_fills": bool(getattr(settings, "test_simulate_fills", False)),
            "inconsistent": _inconsistent_map(store),
            "inconsistent_note": {m: store.get_meta(f"inconsistent_note:{m}", "") for m in MODES},
            "signals": store.recent_signals(STATE_SIGNAL_LIMIT),
            "reports": {m: store.recent_reports(m, STATE_REPORT_LIMIT) for m in MODES},
            "open_lots": {m: store.open_lots(m) for m in MODES},
            "protection_missing": _protection_missing_map(executor),
            "ingress_rejections": stats.snapshot(),
            "snapshot_positions": {},
            "exchange_positions": {},
        }
        if executor is not None:
            for m in MODES:
                try:
                    out["snapshot_positions"][m] = executor.build_snapshot_positions(m)
                except Exception as e:  # noqa: BLE001
                    log.warning("state: build_snapshot_positions(%s) failed: %s", m, type(e).__name__)
                    out["snapshot_positions"][m] = {"error": type(e).__name__}
                out["exchange_positions"][m] = _exchange_positions(executor, m)
        return _json(200, out)

    @app.post("/admin/halt")
    def admin_halt(request: Request):
        denied = _admin_gate(request)
        if denied is not None:
            return denied
        _write_halt(settings, True, note="admin")
        log.warning("admin: HALT set")
        alerts = _svc(services, "alerts")
        if alerts is not None:
            try:
                alerts.send("[admin] HALT set via /admin/halt")
            except Exception:  # noqa: BLE001
                pass
        return _json(200, {"ok": True, "halted": True})

    @app.post("/admin/resume")
    def admin_resume(request: Request):
        denied = _admin_gate(request)
        if denied is not None:
            return denied
        _write_halt(settings, False)
        log.warning("admin: HALT cleared")
        alerts = _svc(services, "alerts")
        if alerts is not None:
            try:
                alerts.send("[admin] HALT cleared via /admin/resume")
            except Exception:  # noqa: BLE001
                pass
        return _json(200, {"ok": True, "halted": False})

    @app.post("/admin/reconcile")
    def admin_reconcile(request: Request, mode: str = "live"):
        denied = _admin_gate(request)
        if denied is not None:
            return denied
        if mode not in MODES:
            return _json(400, {"error": "BAD_REQUEST", "code": "BAD_MODE"})
        executor = _executor()
        if executor is None:
            return _json(503, {"error": "EXECUTOR_UNAVAILABLE"})
        try:
            consistent = bool(executor.reconcile(mode))
        except Exception as e:  # noqa: BLE001
            log.error("admin reconcile(%s) failed: %s", mode, type(e).__name__)
            return _json(500, {"error": "INTERNAL", "code": "RECONCILE_FAILED"})
        try:
            positions = executor.build_snapshot_positions(mode)
        except Exception as e:  # noqa: BLE001
            log.warning("admin reconcile: build_snapshot_positions(%s) failed: %s", mode, type(e).__name__)
            positions = None
        log.warning("admin: reconcile(%s) -> consistent=%s", mode, consistent)
        return _json(200, {
            "ok": consistent,
            "mode": mode,
            "consistent": consistent,
            "inconsistent": bool(store.is_inconsistent(mode)),
            "inconsistent_note": store.get_meta(f"inconsistent_note:{mode}", ""),
            "positions": positions,
            "exchange_positions": _exchange_positions(executor, mode),
        })

    return app
