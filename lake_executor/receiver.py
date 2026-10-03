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
  - 거부(400/401/409/410/413/415/503)는 store.log_ingress(code, event_id, body) 로 남긴다(본문은 sha256 만).
  - 응답/INFO 로그에 시크릿·원본 본문·내부 예외 원문을 넣지 않는다.
  - 관리 엔드포인트는 X-Admin-Token(상수 시간 비교) 필요, ADMIN_TOKEN 미설정 시 404.
  - import 시 부작용 없음: create_app(settings, store, services) 로만 앱을 만든다.

services: executor / reporter / alerts 핸들을 담은 간단한 객체 또는 dict (전부 선택).
"""
from __future__ import annotations

import hmac
import json
import logging
import os
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from . import auth, store as st
from .schemas import Signal
from .util import now_ms

log = logging.getLogger("lake_executor.receiver")

MODES = ("test", "live")
ADMIN_HEADER = "X-Admin-Token"
STATE_SIGNAL_LIMIT = 30
STATE_REPORT_LIMIT = 15


# --------------------------------------------------------------------------- #
# 수신 거부 예외 (내부용) — code 는 짧은 코드, 응답에 그대로 노출 가능
# --------------------------------------------------------------------------- #
class _Reject(Exception):
    def __init__(self, status: int, error: str, code: str | None = None, extra: dict | None = None):
        super().__init__(f"{status} {error} {code or ''}".strip())
        self.status = status
        self.error = error
        self.code = code or error
        self.extra = extra or {}

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
    """원본 바이트를 한 번만 파싱. 중복 키 / NaN·Infinity / 비-객체 / 비-UTF-8 → ValueError."""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as e:
        raise ValueError("body is not valid UTF-8") from e
    obj = json.loads(text, object_pairs_hook=_no_duplicate_pairs, parse_constant=_reject_constant)
    if not isinstance(obj, dict):
        raise ValueError("body must be a JSON object")
    return obj


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
        return True  # 헤더가 아예 없으면 본문 파싱으로 판단 (명시적으로 다른 타입이면 거부)
    mt = ct.split(";", 1)[0].strip().lower()
    return mt == "application/json" or mt.endswith("+json")


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
    def _log_ingress(code: str, event_id: str | None, body: bytes | None, status: int) -> None:
        try:
            store.log_ingress(code, event_id, body, note=f"http={status}")
        except Exception as e:  # noqa: BLE001 - 기록 실패가 응답을 막지 않는다
            log.warning("log_ingress failed: %s", type(e).__name__)

    def _validate(raw: bytes, request: Request) -> tuple[Signal, dict]:
        """검증 통과 시 (Signal, parsed dict). 실패 시 _Reject."""
        # 1) JSON 파싱 (한 번만)
        try:
            data = parse_signal_json(raw)
        except _DuplicateKey:
            raise _Reject(400, "BAD_JSON", "DUPLICATE_KEY")
        except ValueError:
            raise _Reject(400, "BAD_JSON", "PARSE_ERROR")

        event_id = data.get("event_id")
        event_id = event_id if isinstance(event_id, str) and 0 < len(event_id) <= 128 else None

        # 2) mode → 시크릿 선택
        mode = data.get("mode")
        if mode not in MODES:
            raise _Reject(400, "INVALID_SIGNAL", "BAD_MODE", {"event_id": event_id})
        secret = (getattr(settings.secrets, "signal_secret", None) or {}).get(mode)
        if not secret:
            raise _Reject(503, "SECRET_NOT_CONFIGURED", "SECRET_NOT_CONFIGURED", {"mode": mode, "event_id": event_id})

        # 3) 서명/시각 검증 (원본 바이트 그대로)
        body_ts = data.get("ts")
        if isinstance(body_ts, bool) or not isinstance(body_ts, int):
            body_ts = None  # 스키마 단계에서 400 으로 걸러진다
        try:
            auth.verify(raw, request.headers.get("x-signature"), request.headers.get("x-timestamp"),
                        body_ts, secret, now_ms(), max_skew)
        except auth.AuthError as e:
            if e.code == "SECRET_NOT_CONFIGURED":
                raise _Reject(503, "SECRET_NOT_CONFIGURED", e.code, {"mode": mode, "event_id": event_id})
            raise _Reject(401, "UNAUTHORIZED", e.code, {"event_id": event_id})

        # 4) 스키마
        try:
            sig = Signal.model_validate(data)
        except Exception as e:  # pydantic ValidationError (값은 노출하지 않음)
            raise _Reject(400, "INVALID_SIGNAL", "SCHEMA",
                          {"event_id": event_id, "fields": _schema_error_summary(e)})

        # 5) 의미 검증
        if sig.symbol != settings.symbol:
            raise _Reject(400, "INVALID_SIGNAL", "SYMBOL_MISMATCH", {"event_id": sig.event_id})
        if sig.position_idx not in settings.allowed_position_idx():
            raise _Reject(400, "INVALID_SIGNAL", "POSITION_MODE_MISMATCH", {"event_id": sig.event_id})

        # 6) 만료
        if now_ms() > int(sig.expires_at_ms):
            raise _Reject(410, "EXPIRED", "EXPIRED", {"event_id": sig.event_id})
        return sig, data

    @app.post(signal_path)
    async def receive_signal(request: Request):
        # 인코딩/타입/크기 — 본문을 읽기 전에 거를 수 있는 것부터
        enc = (request.headers.get("content-encoding") or "").strip().lower()
        if enc and enc != "identity":
            _log_ingress("UNSUPPORTED_ENCODING", None, None, 415)
            return _json(415, {"error": "UNSUPPORTED_MEDIA_TYPE", "code": "CONTENT_ENCODING_NOT_ALLOWED"})
        if not _is_json_content_type(request):
            _log_ingress("UNSUPPORTED_MEDIA_TYPE", None, None, 415)
            return _json(415, {"error": "UNSUPPORTED_MEDIA_TYPE", "code": "CONTENT_TYPE_NOT_JSON"})
        cl = _content_length(request)
        if cl is not None and cl > max_body:
            _log_ingress("PAYLOAD_TOO_LARGE", None, None, 413)
            return _json(413, {"error": "PAYLOAD_TOO_LARGE", "code": "PAYLOAD_TOO_LARGE", "max_bytes": max_body})

        raw = await request.body()
        if len(raw) > max_body:
            _log_ingress("PAYLOAD_TOO_LARGE", None, raw, 413)
            return _json(413, {"error": "PAYLOAD_TOO_LARGE", "code": "PAYLOAD_TOO_LARGE", "max_bytes": max_body})

        try:
            sig, _data = _validate(raw, request)
        except _Reject as r:
            ev = r.extra.get("event_id")
            _log_ingress(r.code, ev if isinstance(ev, str) else None, raw, r.status)
            log.info("signal rejected http=%s code=%s event_id=%s", r.status, r.code, ev)
            return _json(r.status, r.body())

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
            log.info("signal duplicate event_id=%s", sig.event_id)
            return _json(200, {"accepted": True, "duplicate": True})
        code = "EVENT_ID_CONFLICT" if result == "conflict" else "SEQUENCE_CONFLICT"
        _log_ingress(code, sig.event_id, raw, 409)
        log.warning("signal conflict code=%s event_id=%s position_id=%s seq=%s",
                    code, sig.event_id, sig.position_id, sig.event_sequence)
        return _json(409, {"error": "CONFLICT", "code": code})

    # ------------------------------------------------------------------ healthz
    @app.get("/healthz")
    def healthz():
        try:
            inconsistent = _inconsistent_map(store)
        except Exception as e:  # noqa: BLE001
            log.warning("healthz store error: %s", type(e).__name__)
            return _json(503, {"ok": False})
        return _json(200, {"ok": True, "halted": _halted(settings), "inconsistent": inconsistent})

    # ------------------------------------------------------------------ 관리
    def _admin_gate(request: Request) -> JSONResponse | None:
        """ADMIN_TOKEN 미설정 → 404, 토큰 불일치 → 401, 통과 → None."""
        token = str(getattr(settings.secrets, "admin_token", "") or "")
        if not token:
            return _json(404, {"error": "NOT_FOUND"})
        given = request.headers.get(ADMIN_HEADER) or ""
        if not given or not hmac.compare_digest(given.encode("utf-8"), token.encode("utf-8")):
            return _json(401, {"error": "UNAUTHORIZED"})
        return None

    def _executor():
        return _svc(services, "executor")

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
