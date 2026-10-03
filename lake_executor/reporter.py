"""회신 (ARCHITECTURE.md §5) — lake 회신 URL 로 execution / snapshot 보고.

계약: docs/lake_handoff/lake_execution_contract.json  incoming_execution_reports.schema
  - 본문은 계약 키 집합 그대로(추가 필드 금지). schema_version 1, exchange Bybit, category linear, symbol BTCUSDT.
  - execution: 체결(partially_filled|filled) 만 qty/fill_price/fill_id 가 값, 그 외 상태는 전부 null.
    protection_updated 는 action 이 protection_update 여야 한다. order_id/fill_id 는 SHA256 가명("o-"/"f-").
  - snapshot: complete:true, account_scope lake_dedicated_BTCUSDT, 각 position 의 updated_at_ms <= observed_at_ms,
    take_profit 은 [](확인된 익절 없음) / 가격 배열 / null(미확인), stop_loss 는 확인된 가격 또는 null.
  - sequence/report_id/ts/observed 클램프는 store.allocate_report 가 원자적으로 배정한다.
    본문 바이트는 저장된 그대로 전송(재직렬화 금지) — 서명도 그 바이트로 계산.

전송 상태 (store 상수)
  202 → sent / 200 → duplicate(note=duplicate) / 409 → conflict + 알림 / 그 외 4xx → failed + 알림
  5xx·타임아웃·연결오류 → 재시도(최대 report_max_attempts, 생성 후 report_attempt_window_ms 안에서만;
  창을 넘기면 failed + 알림) / URL 또는 키 미설정 → unsent(저장만).
같은 mode 의 전송은 sequence 오름차순으로 직렬화하며, 앞 번호가 재시도 대기 중이면 뒤 번호를 보내지 않는다.
"""
from __future__ import annotations

import json
import logging
import math
import re
import threading
from enum import Enum
from typing import Any, Callable

from . import store as st
from .auth import headers_for
from .util import canonical_json, now_ms, sha256_hex

log = logging.getLogger("lake_executor.reporter")

MODES = ("test", "live")
SCHEMA_VERSION = 1
EXCHANGE = "Bybit"
CATEGORY = "linear"
SYMBOL = "BTCUSDT"
ACCOUNT_SCOPE = "lake_dedicated_BTCUSDT"
KIND_EXECUTION = "execution"
KIND_SNAPSHOT = "snapshot"

ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
STRATEGIES = ("basic", "overheat", "range")
LEGS = ("long", "short")
ACTIONS = ("entry", "add", "partial_exit", "full_exit", "protection_update")
STATUSES = ("acknowledged", "submitted", "partially_filled", "filled", "rejected", "cancelled",
            "protection_updated", "error")
FILL_STATUSES = ("partially_filled", "filled")
MAX_TP = 20
MAX_POSITIONS = 100

DRAIN_INTERVAL_S = 0.5
_LOG_TRUNC = 200


class ReportError(ValueError):
    """계약을 만족할 수 없는 입력 (호출자 버그). 회신은 생성되지 않는다."""


# --------------------------------------------------------------------------- #
# 정규화 도우미
# --------------------------------------------------------------------------- #
def _s(v: Any) -> Any:
    """Enum → .value, 그 외는 그대로 (None 유지)."""
    if isinstance(v, Enum):
        return v.value
    return v


def _enum(v: Any, allowed: tuple, name: str) -> str:
    v = _s(v)
    if not isinstance(v, str) or v not in allowed:
        raise ReportError(f"{name} must be one of {allowed}")
    return v


def _ident(v: Any, name: str) -> str:
    v = _s(v)
    if not isinstance(v, str) or not ID_RE.match(v):
        raise ReportError(f"{name} must match {ID_RE.pattern}")
    return v


def _opt_ident(v: Any, name: str) -> str | None:
    return None if v is None else _ident(v, name)


def _pos_num(v: Any, name: str) -> float:
    if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or v <= 0:
        raise ReportError(f"{name} must be a finite number > 0")
    return float(v)


def _opt_pos_num(v: Any, name: str) -> float | None:
    return None if v is None else _pos_num(v, name)


def _pos_idx(v: Any, leg: str) -> int:
    v = _s(v)
    if isinstance(v, bool) or not isinstance(v, int) or v not in (0, 1, 2):
        raise ReportError("position_idx must be 0, 1 or 2")
    if v == 1 and leg != "long":
        raise ReportError("position_idx 1 requires leg long")
    if v == 2 and leg != "short":
        raise ReportError("position_idx 2 requires leg short")
    return v


def _ts_int(v: Any, name: str) -> int:
    if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v):
        raise ReportError(f"{name} must be an integer timestamp (ms)")
    v = int(v)
    if v < 1:
        raise ReportError(f"{name} must be >= 1")
    return v


def _trunc(e: BaseException) -> str:
    return f"{type(e).__name__}: {str(e)[:_LOG_TRUNC]}"


# --------------------------------------------------------------------------- #
# Reporter
# --------------------------------------------------------------------------- #
class Reporter:
    def __init__(self, settings: Any, store: st.Store, alerts: Any = None, client: Any = None):
        """client: httpx.Client 호환 객체 (.post(url, content=bytes, headers=dict, timeout=float) → .status_code).
        None 이면 첫 전송 시 httpx.Client 를 만든다."""
        self.settings = settings
        self.store = store
        self.alerts = alerts
        self._client = client
        self._owns_client = client is None
        self._mode_locks = {m: threading.Lock() for m in MODES}
        self._retry_after: dict[str, int] = {m: 0 for m in MODES}
        # 재시도 간격(ms): run_forever 만 참고한다. deliver_pending 직접 호출은 항상 즉시 시도(테스트 결정성).
        self.retry_backoff_ms = 2000

    # ------------------------------------------------------------------ 가명
    @staticmethod
    def pseudonym(kind: str, real_id: str | None) -> str | None:
        """거래소 주문/체결 ID → 'o-'/'f-' + sha256 hex 24자. None 은 None."""
        if real_id is None:
            return None
        k = str(_s(kind)).lower()
        if k in ("o", "order", "order_id"):
            prefix = "o"
        elif k in ("f", "fill", "fill_id", "exec", "exec_id"):
            prefix = "f"
        else:
            raise ReportError("pseudonym kind must be 'o' (order) or 'f' (fill)")
        return f"{prefix}-{sha256_hex(str(real_id))[:24]}"

    # ------------------------------------------------------------------ 본문 생성
    def execution(self, mode: Any, *, event_id: Any, position_id: Any, strategy: Any, leg: Any, position_idx: Any,
                  action: Any, status: Any, qty: float | None = None, fill_price: float | None = None,
                  order_id: str | None = None, fill_id: str | None = None, reason_code: Any = None,
                  observed_at_ms: int | None = None, mark_fill_reported: str | None = None) -> dict:
        """execution 회신을 pending 으로 저장. 반환: allocate_report 결과 + 'payload'(본문 dict).

        status/action/reason_code 는 Enum 이든 문자열이든 받는다. 체결 상태가 아니면 qty/fill_price/fill_id 는 null 로
        강제되고, 체결 상태인데 값이 없으면 ReportError.
        mark_fill_reported: 체결 회신이면 그 체결(exec_id)의 fills.reported=1 을 회신 생성과 같은 트랜잭션에서 기록한다.
        """
        mode = _enum(mode, MODES, "mode")
        leg_v = _enum(leg, LEGS, "leg")
        execution = {
            "position_id": _ident(position_id, "position_id"),
            "strategy": _enum(strategy, STRATEGIES, "strategy"),
            "leg": leg_v,
            "position_idx": _pos_idx(position_idx, leg_v),
            "event_id": _ident(event_id, "event_id"),
            "action": _enum(action, ACTIONS, "action"),
            "status": _enum(status, STATUSES, "status"),
            "qty": None,
            "fill_price": None,
            "order_id": self.pseudonym("o", _s(order_id)) if order_id is not None else None,
            "fill_id": None,
            "reason_code": _opt_ident(reason_code, "reason_code"),
        }
        if execution["status"] in FILL_STATUSES:
            if qty is None or fill_price is None or fill_id is None:
                raise ReportError("fill report requires qty, fill_price and fill_id")
            execution["qty"] = _pos_num(qty, "qty")
            execution["fill_price"] = _pos_num(fill_price, "fill_price")
            execution["fill_id"] = self.pseudonym("f", str(_s(fill_id)))
        if execution["status"] == "protection_updated" and execution["action"] != "protection_update":
            raise ReportError("protection_updated requires action protection_update")
        if observed_at_ms is None:
            observed_at_ms = now_ms()
        observed_at_ms = _ts_int(observed_at_ms, "observed_at_ms")

        def build(report_id: str, sequence: int, ts: int, observed: int) -> dict:
            return {
                **self._header(report_id, mode, sequence, ts, observed, KIND_EXECUTION),
                "execution": dict(execution),
            }

        return self._allocate(mode, KIND_EXECUTION, observed_at_ms, build, mark_fill_reported=mark_fill_reported)

    def snapshot(self, mode: Any, positions: list[dict], observed_at_ms: int) -> dict:
        """complete 스냅샷을 pending 으로 저장. positions 항목 키:
        position_id, strategy, leg, position_idx, qty(>0), entry_price(>0), mark_price(>0|None),
        stop_loss(>0|None), take_profit(list|number|None), updated_at_ms.
        계약 위반(수량 0, 평균가 없음, position_id 중복, idx 0 과 1/2 혼용 …)은 ReportError — 불완전한
        스냅샷을 complete:true 로 보내지 않기 위함."""
        mode = _enum(mode, MODES, "mode")
        observed_at_ms = _ts_int(observed_at_ms, "observed_at_ms")
        if positions is None:
            positions = []
        if len(positions) > MAX_POSITIONS:
            raise ReportError(f"snapshot positions exceed {MAX_POSITIONS}")
        items: list[dict] = []
        seen: set[str] = set()
        idx_kinds: set[str] = set()
        one_way_legs: set[str] = set()
        for p in positions:
            item = self._snapshot_position(p)
            pid = item["position_id"]
            if pid in seen:
                raise ReportError(f"duplicate position_id in snapshot: {pid}")
            seen.add(pid)
            idx_kinds.add("one_way" if item["position_idx"] == 0 else "hedge")
            if item["position_idx"] == 0:
                one_way_legs.add(item["leg"])
            items.append(item)
        if len(idx_kinds) > 1:
            raise ReportError("snapshot must not mix position_idx 0 with 1/2")
        if len(one_way_legs) > 1:
            raise ReportError("one-way mode (position_idx 0) cannot hold opposing long/short lots")

        def build(report_id: str, sequence: int, ts: int, observed: int) -> dict:
            out_positions = []
            for it in items:
                it2 = dict(it)
                it2["updated_at_ms"] = min(it2["updated_at_ms"], observed)  # 계약: updated_at_ms <= observed_at_ms
                out_positions.append(it2)
            return {
                **self._header(report_id, mode, sequence, ts, observed, KIND_SNAPSHOT),
                "complete": True,
                "account_scope": ACCOUNT_SCOPE,
                "positions": out_positions,
            }

        return self._allocate(mode, KIND_SNAPSHOT, observed_at_ms, build)

    @staticmethod
    def _snapshot_position(p: dict) -> dict:
        leg = _enum(p.get("leg"), LEGS, "leg")
        tp = p.get("take_profit")
        if tp is None:
            tp_out = None
        else:
            if isinstance(tp, (int, float)) and not isinstance(tp, bool):
                tp = [tp]
            if not isinstance(tp, (list, tuple)):
                raise ReportError("take_profit must be null, a number or a list of numbers")
            if len(tp) > MAX_TP:
                raise ReportError(f"take_profit: at most {MAX_TP} prices")
            tp_out = [_pos_num(x, "take_profit[]") for x in tp]
        return {
            "position_id": _ident(p.get("position_id"), "position_id"),
            "strategy": _enum(p.get("strategy"), STRATEGIES, "strategy"),
            "leg": leg,
            "position_idx": _pos_idx(p.get("position_idx"), leg),
            "qty": _pos_num(p.get("qty"), "qty"),
            "entry_price": _pos_num(p.get("entry_price"), "entry_price"),
            "mark_price": _opt_pos_num(p.get("mark_price"), "mark_price"),
            "stop_loss": _opt_pos_num(p.get("stop_loss"), "stop_loss"),
            "take_profit": tp_out,
            "updated_at_ms": _ts_int(p.get("updated_at_ms"), "updated_at_ms"),
        }

    @staticmethod
    def _header(report_id: str, mode: str, sequence: int, ts: int, observed: int, kind: str) -> dict:
        return {
            "schema_version": SCHEMA_VERSION,
            "report_id": report_id,
            "mode": mode,
            "sequence": int(sequence),
            "ts": int(ts),
            "observed_at_ms": int(observed),
            "exchange": EXCHANGE,
            "category": CATEGORY,
            "symbol": SYMBOL,
            "kind": kind,
        }

    def _allocate(self, mode: str, kind: str, observed_at_ms: int, build: Callable[[str, int, int, int], dict],
                  mark_fill_reported: str | None = None) -> dict:
        payload_box: dict = {}

        def build_and_keep(report_id: str, sequence: int, ts: int, observed: int) -> dict:
            body = build(report_id, sequence, ts, observed)
            payload_box["payload"] = body
            return body

        alloc = self.store.allocate_report(mode, kind, observed_at_ms, build_and_keep, canonical_json,
                                           mark_fill_reported=mark_fill_reported)
        alloc = dict(alloc)
        alloc["payload"] = payload_box.get("payload")
        log.info("report %s %s seq=%s id=%s", mode, kind, alloc.get("sequence"), alloc.get("report_id"))
        return alloc

    # ------------------------------------------------------------------ 전송
    def _configured(self, mode: str) -> bool:
        sec = self.settings.secrets
        return bool(sec.report_url.get(mode)) and bool(sec.report_secret.get(mode))

    def _get_client(self):
        if self._client is None:
            import httpx  # 지연 import: 테스트에서 가짜 client 주입 시 불필요
            self._client = httpx.Client()
        return self._client

    def deliver_pending(self, mode: Any) -> int:
        """mode 의 pending 회신을 sequence 순서대로 전송. 반환: 이번 호출에서 sent/duplicate 가 된 개수.

        앞 번호가 재시도 상태로 남으면(5xx/타임아웃) 거기서 멈춘다. failed/conflict 로 끝난 번호는 뒤를 막지 않는다.
        """
        mode = _enum(mode, MODES, "mode")
        lock = self._mode_locks[mode]
        if not lock.acquire(blocking=False):
            return 0  # 같은 mode 를 다른 스레드가 전송 중 — 순서 보장을 위해 겹치지 않는다
        try:
            return self._deliver_locked(mode)
        finally:
            lock.release()

    def _deliver_locked(self, mode: str) -> int:
        rows = self.store.pending_reports(mode)
        if not rows:
            return 0
        if not self._configured(mode):
            for row in rows:
                self.store.update_report(row["report_id"], st.REPORT_UNSENT, note="report url/secret not configured")
            log.info("report %s: %d pending marked unsent (url/secret not configured)", mode, len(rows))
            return 0

        sec = self.settings.secrets
        url = sec.report_url[mode]
        secret = sec.report_secret[mode]
        timeout = float(self.settings.report_http_timeout_s)
        max_attempts = max(1, int(self.settings.report_max_attempts))
        window_ms = int(self.settings.report_attempt_window_ms)
        sent = 0

        for row in rows:
            rid = row["report_id"]
            seq = row["sequence"]
            attempts = int(row.get("attempts") or 0)
            body = row["body"]
            if isinstance(body, memoryview):
                body = body.tobytes()
            elif isinstance(body, str):
                body = body.encode("utf-8")
            created = int(row["created_at_ms"])
            now = now_ms()

            if now - created > window_ms:
                self._finish(rid, st.REPORT_FAILED, None, attempts,
                             f"attempt window exceeded ({attempts} attempts)")
                self._alert(f"report {mode} seq={seq} {row.get('kind')} failed: attempt window exceeded "
                            f"after {attempts} attempts")
                continue
            if attempts >= max_attempts:
                self._finish(rid, st.REPORT_FAILED, row.get("http_status"), attempts, "max attempts exceeded")
                self._alert(f"report {mode} seq={seq} {row.get('kind')} failed: max attempts exceeded")
                continue

            ts = self._body_ts(body, created)
            headers = headers_for(body, secret, ts)
            attempts += 1
            try:
                resp = self._get_client().post(url, content=body, headers=headers, timeout=timeout)
                status = int(getattr(resp, "status_code"))
            except Exception as e:  # 타임아웃/연결오류 등 → 재시도 대상
                log.warning("report %s seq=%s post error (attempt %d/%d): %s", mode, seq, attempts, max_attempts, _trunc(e))
                if not self._retry_or_fail(mode, row, attempts, max_attempts, None, "transport error"):
                    break
                continue

            if status == 200:
                self._finish(rid, st.REPORT_DUPLICATE, status, attempts, "duplicate", sent=True)
                log.info("report %s seq=%s id=%s -> 200 duplicate", mode, seq, rid)
                sent += 1
            elif 200 < status < 300:
                self._finish(rid, st.REPORT_SENT, status, attempts, "", sent=True)
                log.info("report %s seq=%s id=%s -> %d sent", mode, seq, rid, status)
                sent += 1
            elif status == 409:
                self._finish(rid, st.REPORT_CONFLICT, status, attempts, "conflict")
                self._alert(f"report {mode} seq={seq} {row.get('kind')} conflict (409) id={rid}")
            elif status >= 500:
                log.warning("report %s seq=%s -> %d (attempt %d/%d)", mode, seq, status, attempts, max_attempts)
                if not self._retry_or_fail(mode, row, attempts, max_attempts, status, f"http {status}"):
                    break
            else:  # 그 외 4xx, 1xx/3xx — 재시도해도 같은 결과
                self._finish(rid, st.REPORT_FAILED, status, attempts, f"http {status}")
                self._alert(f"report {mode} seq={seq} {row.get('kind')} failed: http {status} id={rid}")
        return sent

    def _retry_or_fail(self, mode: str, row: dict, attempts: int, max_attempts: int,
                       http_status: int | None, why: str) -> bool:
        """재시도 가능한 실패 처리. True = 종결됨(failed) 이라 다음 번호 진행 가능, False = pending 유지 → 멈춤."""
        rid, seq = row["report_id"], row["sequence"]
        if attempts >= max_attempts:
            self._finish(rid, st.REPORT_FAILED, http_status, attempts, f"{why}; max attempts")
            self._alert(f"report {mode} seq={seq} {row.get('kind')} failed after {attempts} attempts ({why})")
            return True
        self.store.update_report(rid, st.REPORT_PENDING, http_status=http_status, attempts=attempts,
                                 note=f"retry: {why}")
        self._retry_after[mode] = now_ms() + int(self.retry_backoff_ms) * attempts
        return False

    def _finish(self, report_id: str, state: str, http_status: int | None, attempts: int, note: str,
                sent: bool = False) -> None:
        self.store.update_report(report_id, state, http_status=http_status, attempts=attempts, sent=sent, note=note)

    @staticmethod
    def _body_ts(body: bytes, fallback: int) -> int:
        """본문 ts 를 읽는다 (서명 헤더 X-Timestamp = 본문 ts). 파싱만 하고 바이트는 건드리지 않는다."""
        try:
            ts = json.loads(body.decode("utf-8")).get("ts")
            return int(ts) if ts else int(fallback)
        except Exception:
            return int(fallback)

    def _alert(self, text: str) -> None:
        if self.alerts is None:
            log.error("ALERT: %s", text)
            return
        try:
            self.alerts.send(text)
        except Exception as e:
            log.warning("alert failed: %s", _trunc(e))

    # ------------------------------------------------------------------ 루프
    def run_forever(self, stop_event: threading.Event) -> None:
        """두 모드의 pending 을 0.5s 주기로 drain. 재시도 대기 중인 mode 는 backoff 뒤에 다시 시도."""
        log.info("reporter loop start")
        while not stop_event.is_set():
            for mode in MODES:
                if now_ms() < self._retry_after.get(mode, 0):
                    continue
                try:
                    self.deliver_pending(mode)
                except Exception as e:
                    log.error("deliver_pending(%s) crashed: %s", mode, _trunc(e))
            stop_event.wait(DRAIN_INTERVAL_S)
        log.info("reporter loop stop")

    def close(self) -> None:
        if self._owns_client and self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None
