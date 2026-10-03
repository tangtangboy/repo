"""실행기 (ARCHITECTURE.md §1 게이트, §3 실행기).

접수된 신호를 FIFO 로 하나씩 꺼내(단일 워커) 거래소 주문/보호주문을 내고,
lot 원장·체결을 갱신하며 회신(reporter)을 쌓는다. 주기 스냅샷 전에는 `reconcile(mode)` 로
보호주문 체결(auto:sl/auto:tp)과 거래소 포지션 합계를 대조한다.

설계 메모
  - process / reconcile / snapshot_now / recover_processing 은 하나의 RLock 으로 직렬화한다
    (워커 스레드와 스냅샷 스레드가 같은 lot 을 만지므로).
  - 체결은 한 건씩 `fills` 에 기록 → lot 반영 → 회신 순으로 처리해 재시작 시 이중 반영을 막는다.
  - 보호주문 orderLinkId 는 `util.order_link_id(mode, position_id, "sl"|"tp", revision, i)` 이고,
    같은 revision 안에서 수량 재설정(취소 후 재생성)을 할 때는 거래소의 orderLinkId 중복 거부를 피하기 위해
    세대 번호(`protection_orders["gen"]`)를 뒤에 덧붙인다 (첫 생성은 계약 형태 그대로).
  - 원본 본문·시크릿·거래소 오류 원문은 로그/회신에 넣지 않는다 (코드만).
"""
from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Callable

from . import store as st
from .exchange import ExchangeBase, ExchangeError, ExchangeRejected
from .ops import halted
from .schemas import Action, Signal
from .util import floor_step, fmt_step, now_ms, order_link_id, qty_eq, sha256_hex

log = logging.getLogger("lake_executor.executor")

# 거래소 주문 상태 분류
_TERMINAL = {"Filled", "Cancelled", "Rejected", "Deactivated"}
_PROTECTION_OPEN = {"New", "Untriggered"}
_PROTECTION_FILLING = {"Filled", "PartiallyFilled"}

# 보호주문 기본 구조 (lot.protection_orders)
def _empty_protection() -> dict:
    return {"sl": None, "tp": [], "gen": 0, "tp_done": []}


@dataclass
class _Ctx:
    """한 신호(또는 auto 이벤트) 처리 문맥. 회신에 들어가는 정체성 필드 + 거래소 핸들."""
    mode: str
    event_id: str
    position_id: str
    strategy: str
    leg: str
    position_idx: int
    action: str
    exchange: ExchangeBase | None = None
    sig: Signal | None = None

    @property
    def is_long(self) -> bool:
        return self.leg == "long"

    def open_side(self) -> str:
        return "Buy" if self.is_long else "Sell"

    def close_side(self) -> str:
        return "Sell" if self.is_long else "Buy"

    def tag(self) -> str:
        return f"{self.mode}/{self.action} pos={self.position_id} ev={self.event_id}"


@dataclass
class _OrderResult:
    """시장가 주문 하나의 정산 결과."""
    state: str                      # filled | partial | cancelled | rejected | timeout
    order_id: str | None
    filled_qty: float = 0.0         # 이번 처리에서 새로 lot 에 반영한 체결 수량
    fills: list[dict] = field(default_factory=list)


class _ProtectionError(Exception):
    """보호주문 취소/재생성 실패 (거래소 거부가 아닌 논리 실패). → error/EXCHANGE_ERROR."""


class Executor:
    """`Executor(settings, store, exchanges, reporter, alerts)` — §3."""

    def __init__(self, settings, store: st.Store, exchanges: dict[str, ExchangeBase | None], reporter, alerts):
        self.settings = settings
        self.store = store
        self.exchanges = exchanges or {}
        self.reporter = reporter
        self.alerts = alerts
        self._lock = threading.RLock()

    # ------------------------------------------------------------------ 루프
    def run_once(self) -> bool:
        """accepted 신호 하나를 집어 처리. 처리한 게 있으면 True."""
        row = self.store.claim_next_signal()
        if row is None:
            return False
        self.process(row)
        return True

    def run_forever(self, stop_event: threading.Event) -> None:
        """0.2s 폴링 루프. process 내부 예외는 process 가 삼키고, claim 자체의 예외도 루프를 죽이지 않는다."""
        log.info("executor loop started")
        while not stop_event.is_set():
            try:
                worked = self.run_once()
            except Exception as e:  # noqa: BLE001 - 루프는 절대 죽지 않는다
                log.exception("executor loop error: %s", type(e).__name__)
                self._alert(f"[executor] loop error: {type(e).__name__}")
                stop_event.wait(1.0)
                continue
            if not worked:
                stop_event.wait(0.2)
        log.info("executor loop stopped")

    # ------------------------------------------------------------------ 시작
    def ensure_account_setup(self) -> None:
        """live 거래소가 준비돼 있으면 포지션 모드/레버리지/마진 모드를 맞춘다 (live 에서만, §3 마지막 줄)."""
        ex = self.exchanges.get("live")
        ok, _ = self.settings.live_execution_possible()
        if ex is None or not ok:
            return
        ex.ensure_account_setup(self.settings.position_mode, self.settings.leverage, self.settings.margin_mode)
        log.info("live account setup ensured (mode=%s lev=%s margin=%s)",
                 self.settings.position_mode, self.settings.leverage, self.settings.margin_mode)

    def start(self) -> None:
        """편의: ensure_account_setup() → recover_processing()."""
        self.ensure_account_setup()
        self.recover_processing()

    def recover_processing(self) -> None:
        """재시작 복구. status=processing 신호는 재실행하지 않고, 그 신호의 주문이 거래소에 있으면
        체결 수집을 이어서 마무리하고, 없으면 error/UNKNOWN_STATE 로 닫고 알린다."""
        with self._lock:
            rows = self.store.processing_signals()
            if not rows:
                return
            log.warning("recover_processing: %d signal(s) left in processing", len(rows))
            for row in rows:
                self._recover_one(row)

    def _recover_one(self, row: dict) -> None:
        event_id = row.get("event_id", "")
        sig = self._load_signal(row)
        if sig is None:
            return
        ctx = self._ctx_from_signal(sig)
        mode = ctx.mode
        ex = self.exchanges.get(mode)
        if mode == "test" and ex is None:
            # 기록 전용 모드: 아무것도 실행되지 않았음
            self.store.set_signal_result(event_id, st.SIGNAL_DONE, "TEST_RECORD_ONLY", "recovered: record only")
            return
        if mode == "live":
            ok, _ = self.settings.live_execution_possible()
            if not ok or ex is None:
                self._unknown_state(ctx, "recovered: live exchange unavailable")
                return
        ctx.exchange = ex
        if sig.action not in (Action.entry, Action.add, Action.partial_exit, Action.full_exit):
            # protection_update 는 시장가 주문이 없어 중간 상태를 판정할 수 없음
            self._unknown_state(ctx, "recovered: no resumable order for this action")
            return

        link = order_link_id(mode, event_id)
        order_row = self.store.get_order(link)
        try:
            o = ex.get_order(link)
        except ExchangeError as e:
            log.warning("recover %s: get_order failed (%s)", ctx.tag(), e.code)
            self._unknown_state(ctx, f"recovered: get_order failed {e.code}")
            return
        if o is None:
            if order_row is not None:
                self.store.update_order(link, status="unknown")
            self._unknown_state(ctx, "recovered: order not found on exchange")
            return

        purpose = "exit" if sig.action in (Action.partial_exit, Action.full_exit) else sig.action.value
        reduce_only = purpose == "exit"
        side = ctx.close_side() if reduce_only else ctx.open_side()
        order_qty = float(o.get("qty") or (order_row or {}).get("qty") or 0.0)
        if order_row is None:
            self.store.insert_order(link, mode, ctx.position_id, purpose, side, order_qty, reduce_only,
                                    event_id=event_id, status="submitted", order_id=o.get("order_id"))
        else:
            self.store.update_order(link, order_id=o.get("order_id"), status="submitted")
        log.info("recover %s: resuming fill collection for link=%s", ctx.tag(), link)

        apply = self._apply_close_fill if reduce_only else self._apply_open_fill
        finalize = self._finalize_close if reduce_only else self._finalize_open

        def _resume() -> None:
            res = self._settle_order(ctx, link, o.get("order_id"), order_qty, apply)
            self._finish_market(ctx, res, finalize)

        self._guarded(ctx, _resume)

    # ------------------------------------------------------------------ 신호 처리
    def process(self, row: dict) -> None:
        """claim 된 신호 한 건 처리. 어떤 예외도 밖으로 내보내지 않는다."""
        with self._lock:
            sig = self._load_signal(row)
            if sig is None:
                return
            ctx = self._ctx_from_signal(sig)
            log.info("process %s seq=%s", ctx.tag(), sig.event_sequence)
            self._guarded(ctx, lambda: self._process_signal(ctx))

    def _load_signal(self, row: dict) -> Signal | None:
        event_id = row.get("event_id", "")
        try:
            return Signal.model_validate_json(row["raw_body"])
        except Exception as e:  # noqa: BLE001
            log.error("signal %s: stored body cannot be reconstructed (%s)", event_id, type(e).__name__)
            self.store.set_signal_result(event_id, st.SIGNAL_ERROR, "UNKNOWN_STATE", "stored body invalid")
            self._alert(f"[executor] signal {event_id}: stored body invalid")
            return None

    def _guarded(self, ctx: _Ctx, fn: Callable[[], None]) -> None:
        """예외 → 회신(rejected|error) + 신호 결과 + 알림. 루프는 절대 죽지 않는다."""
        try:
            fn()
        except ExchangeRejected as e:
            log.warning("%s: exchange rejected (ret_code=%s)", ctx.tag(), e.ret_code)
            self._report(ctx, "rejected", reason_code="EXCHANGE_REJECTED")
            self.store.set_signal_result(ctx.event_id, st.SIGNAL_REJECTED, "EXCHANGE_REJECTED",
                                         f"ret_code={e.ret_code}")
            self._alert(f"[executor:{ctx.mode}] {ctx.action} {ctx.position_id} {ctx.event_id}: "
                        f"EXCHANGE_REJECTED ret_code={e.ret_code}")
            self._snapshot_quiet(ctx.mode)
        except ExchangeError as e:
            code = e.code if e.code in ("EXCHANGE_ERROR", "EXCHANGE_TIMEOUT") else "EXCHANGE_ERROR"
            log.warning("%s: exchange error %s", ctx.tag(), code)
            self._report(ctx, "error", reason_code=code)
            self.store.set_signal_result(ctx.event_id, st.SIGNAL_ERROR, code, "")
            self._alert(f"[executor:{ctx.mode}] {ctx.action} {ctx.position_id} {ctx.event_id}: {code}")
            self._snapshot_quiet(ctx.mode)
        except _ProtectionError as e:
            log.error("%s: protection handling failed: %s", ctx.tag(), e)
            self._report(ctx, "error", reason_code="EXCHANGE_ERROR")
            self.store.set_signal_result(ctx.event_id, st.SIGNAL_ERROR, "EXCHANGE_ERROR", str(e)[:200])
            self._alert(f"[executor:{ctx.mode}] {ctx.action} {ctx.position_id} {ctx.event_id}: "
                        f"protection orders need attention ({e})")
            self._snapshot_quiet(ctx.mode)
        except Exception as e:  # noqa: BLE001
            log.exception("%s: unexpected %s", ctx.tag(), type(e).__name__)
            self._report(ctx, "error", reason_code="UNKNOWN_STATE")
            try:
                self.store.set_signal_result(ctx.event_id, st.SIGNAL_ERROR, "UNKNOWN_STATE", type(e).__name__)
            except Exception:  # noqa: BLE001
                log.exception("set_signal_result failed for %s", ctx.event_id)
            self._alert(f"[executor:{ctx.mode}] {ctx.action} {ctx.position_id} {ctx.event_id}: "
                        f"UNKNOWN_STATE ({type(e).__name__})")

    def _process_signal(self, ctx: _Ctx) -> None:
        sig = ctx.sig
        assert sig is not None
        mode = ctx.mode
        self._report(ctx, "acknowledged")

        # ---- 모드 게이트 (§1)
        if mode == "test":
            ex = self.exchanges.get("test")
            if ex is None:
                # 기록 전용: acknowledged 하나만 보내고 끝
                self.store.set_signal_result(ctx.event_id, st.SIGNAL_DONE, "TEST_RECORD_ONLY", "record only")
                log.info("%s: test record only", ctx.tag())
                return
        else:
            ok, reason = self.settings.live_execution_possible()
            ex = self.exchanges.get("live")
            if not ok or ex is None:
                self._reject(ctx, reason or "LIVE_DISABLED")
                return
        ctx.exchange = ex

        # ---- 공통 게이트
        if self._halted():
            self._reject(ctx, "OPERATOR_HALT")
            return
        if sig.position_idx not in self.settings.allowed_position_idx():
            self._reject(ctx, "POSITION_MODE_MISMATCH")
            return
        if sig.action in (Action.entry, Action.add) and self.store.is_inconsistent(mode):
            self._reject(ctx, "RECONCILE_REQUIRED")
            return

        # ---- 액션
        if sig.action == Action.entry:
            self._do_entry(ctx)
        elif sig.action == Action.add:
            self._do_add(ctx)
        elif sig.action == Action.partial_exit:
            self._do_partial_exit(ctx)
        elif sig.action == Action.full_exit:
            self._do_full_exit(ctx)
        elif sig.action == Action.protection_update:
            self._do_protection_update(ctx)
        else:  # pragma: no cover - 스키마가 막음
            self._reject(ctx, "UNKNOWN_STATE", "unsupported action")

    # ------------------------------------------------------------------ 액션
    def _do_entry(self, ctx: _Ctx) -> None:
        sig = ctx.sig
        assert sig is not None and ctx.exchange is not None
        lot = self.store.get_lot(ctx.mode, ctx.position_id)
        if lot is not None and lot["status"] == st.LOT_OPEN:
            self._reject(ctx, "POSITION_EXISTS")
            return
        qty, code = self._check_open_qty(ctx, float(sig.qty_btc or 0.0))
        if code:
            self._reject(ctx, code)
            return
        code = self._slippage_guard(ctx)
        if code:
            self._reject(ctx, code)
            return
        res = self._execute_market(ctx, ctx.open_side(), qty, reduce_only=False, purpose="entry",
                                   apply=self._apply_open_fill)
        self._finish_market(ctx, res, self._finalize_open)

    def _do_add(self, ctx: _Ctx) -> None:
        sig = ctx.sig
        assert sig is not None and ctx.exchange is not None
        lot = self.store.get_lot(ctx.mode, ctx.position_id)
        if lot is None or lot["status"] != st.LOT_OPEN:
            self._reject(ctx, "POSITION_NOT_FOUND")
            return
        qty, code = self._check_open_qty(ctx, float(sig.qty_btc or 0.0))
        if code:
            self._reject(ctx, code)
            return
        code = self._slippage_guard(ctx)
        if code:
            self._reject(ctx, code)
            return
        res = self._execute_market(ctx, ctx.open_side(), qty, reduce_only=False, purpose="add",
                                   apply=self._apply_open_fill)
        self._finish_market(ctx, res, self._finalize_open)

    def _do_partial_exit(self, ctx: _Ctx) -> None:
        sig = ctx.sig
        assert sig is not None and ctx.exchange is not None
        lot = self.store.get_lot(ctx.mode, ctx.position_id)
        if lot is None or lot["status"] != st.LOT_OPEN:
            self._reject(ctx, "POSITION_NOT_FOUND")
            return
        inst = ctx.exchange.instrument()
        step, min_qty = float(inst["qty_step"]), float(inst["min_qty"])
        qty = floor_step(float(sig.qty_btc or 0.0), step)
        lot_qty = float(lot["qty"])
        if qty > lot_qty + step / 2:
            self._reject(ctx, "QTY_EXCEEDS_LOT", f"qty={fmt_step(qty, step)} lot={fmt_step(lot_qty, step)}")
            return
        qty = min(qty, floor_step(lot_qty, step))
        if qty <= 0 or qty < min_qty:
            self._reject(ctx, "QTY_BELOW_MIN")
            return
        res = self._execute_market(ctx, ctx.close_side(), qty, reduce_only=True, purpose="exit",
                                   apply=self._apply_close_fill)
        self._finish_market(ctx, res, self._finalize_close)

    def _do_full_exit(self, ctx: _Ctx) -> None:
        assert ctx.exchange is not None
        lot = self.store.get_lot(ctx.mode, ctx.position_id)
        if lot is None or lot["status"] != st.LOT_OPEN:
            self._reject(ctx, "POSITION_NOT_FOUND")
            return
        inst = ctx.exchange.instrument()
        step, min_qty = float(inst["qty_step"]), float(inst["min_qty"])
        qty = floor_step(float(lot["qty"]), step)   # lot 잔량만 (심볼 전량 아님)
        if qty <= 0 or qty < min_qty:
            self._reject(ctx, "QTY_BELOW_MIN", f"lot remainder {fmt_step(qty, step)} below min")
            self._alert(f"[executor:{ctx.mode}] full_exit {ctx.position_id}: lot remainder below min qty, "
                        f"manual cleanup needed")
            return
        res = self._execute_market(ctx, ctx.close_side(), qty, reduce_only=True, purpose="exit",
                                   apply=self._apply_close_fill)
        self._finish_market(ctx, res, self._finalize_close)

    def _do_protection_update(self, ctx: _Ctx) -> None:
        sig = ctx.sig
        assert sig is not None and ctx.exchange is not None
        lot = self.store.get_lot(ctx.mode, ctx.position_id)
        if lot is None or lot["status"] != st.LOT_OPEN:
            self._reject(ctx, "POSITION_NOT_FOUND")
            return
        if int(sig.protection_revision) <= int(lot.get("protection_revision") or 0):
            self._reject(ctx, "STALE_PROTECTION_REVISION",
                         f"rev={sig.protection_revision} lot_rev={lot.get('protection_revision')}")
            return
        # 기존 보호주문 취소 → lot 의 SL/TP/revision 갱신 → 재생성
        self._cancel_protections(ctx, lot)
        po = dict(lot.get("protection_orders") or _empty_protection())
        po.update({"sl": None, "tp": [], "gen": 0, "tp_done": []})   # 새 revision: 세대/완료 레벨 초기화
        lot["protection_orders"] = po
        lot["stop_loss"] = sig.stop_loss
        lot["take_profit"] = sig.tp_list   # None | [] | [가격...]
        lot["protection_revision"] = int(sig.protection_revision)
        lot["last_event_id"] = ctx.event_id
        lot["updated_at_ms"] = now_ms()
        self.store.upsert_lot(lot)
        self._place_protections(ctx, lot)
        self._report(ctx, "protection_updated")
        self.store.set_signal_result(ctx.event_id, st.SIGNAL_DONE, None, "")
        log.info("%s: protection updated rev=%s sl=%s tp=%s", ctx.tag(), sig.protection_revision,
                 sig.stop_loss, sig.tp_list)
        self._snapshot_quiet(ctx.mode)

    # ------------------------------------------------------------------ 게이트/가드 헬퍼
    def _halted(self) -> bool:
        try:
            return bool(halted(self.settings))
        except Exception:  # noqa: BLE001 - ops 가 어떤 이유로든 실패하면 파일 존재로 판단
            return os.path.exists(self.settings.halt_file)

    def _reject(self, ctx: _Ctx, code: str, note: str = "") -> None:
        log.info("%s: rejected %s %s", ctx.tag(), code, note)
        self._report(ctx, "rejected", reason_code=code)
        self.store.set_signal_result(ctx.event_id, st.SIGNAL_REJECTED, code, note)

    def _check_open_qty(self, ctx: _Ctx, qty_btc: float) -> tuple[float, str | None]:
        """entry/add 수량 검증. (qty, reason_code|None)."""
        assert ctx.exchange is not None
        inst = ctx.exchange.instrument()
        step, min_qty = float(inst["qty_step"]), float(inst["min_qty"])
        qty = floor_step(qty_btc, step)
        if qty <= 0 or qty < min_qty:
            return qty, "QTY_BELOW_MIN"
        if qty > float(self.settings.max_order_qty_btc) + step / 2:
            return qty, "QTY_LIMIT"
        leg_sum = sum(float(l["qty"]) for l in self.store.open_lots(ctx.mode)
                      if int(l["position_idx"]) == int(ctx.position_idx))
        if leg_sum + qty > float(self.settings.max_leg_qty_btc) + step / 2:
            return qty, "LEG_LIMIT"
        return qty, None

    def _slippage_guard(self, ctx: _Ctx) -> str | None:
        sig = ctx.sig
        assert sig is not None and ctx.exchange is not None
        ref = sig.reference_price
        if ref is None or ref <= 0:
            return None
        last = float(ctx.exchange.last_price())
        dev = abs(last - ref) / ref * 100.0
        if dev > float(self.settings.max_entry_slippage_pct):
            log.info("%s: slippage %.3f%% > %.3f%%", ctx.tag(), dev, self.settings.max_entry_slippage_pct)
            return "SLIPPAGE_GUARD"
        return None

    # ------------------------------------------------------------------ 시장가 주문 + 체결 수집
    def _execute_market(self, ctx: _Ctx, side: str, qty: float, *, reduce_only: bool, purpose: str,
                        apply: Callable[[_Ctx, dict], None]) -> _OrderResult:
        ex = ctx.exchange
        assert ex is not None
        link = order_link_id(ctx.mode, ctx.event_id)
        self.store.insert_order(link, ctx.mode, ctx.position_id, purpose, side, qty, reduce_only,
                                event_id=ctx.event_id, status="new")
        order_id: str | None = None
        try:
            r = ex.place_market(side, qty, ctx.position_idx, reduce_only, link)
            order_id = str(r.get("order_id") or "") or None
        except ExchangeRejected:
            self.store.update_order(link, status="Rejected")
            raise
        except ExchangeError as e:
            # 네트워크/타임아웃: 주문이 들어갔을 수 있다 → 한 번 조회해 있으면 이어서 정산
            log.warning("%s: place_market %s, checking whether order exists", ctx.tag(), e.code)
            o = ex.get_order(link)
            if o is None:
                self.store.update_order(link, status="unknown")
                raise
            order_id = str(o.get("order_id") or "") or None
        self.store.update_order(link, order_id=order_id, status="submitted")
        self._report(ctx, "submitted", order_id=order_id)
        log.info("%s: submitted %s %s qty=%s ro=%s order=%s", ctx.tag(), purpose, side, qty, reduce_only, order_id)
        return self._settle_order(ctx, link, order_id, qty, apply)

    def _settle_order(self, ctx: _Ctx, link: str, order_id: str | None, order_qty: float,
                      apply: Callable[[_Ctx, dict], None]) -> _OrderResult:
        """종결 상태까지 폴링 → 체결 수집/반영/회신 → 결과."""
        ex = ctx.exchange
        assert ex is not None
        step = float(ex.instrument()["qty_step"])
        state, o = self._await_terminal(ex, link)
        if o is not None and not order_id:
            order_id = str(o.get("order_id") or "") or None
            self.store.update_order(link, order_id=order_id)
        if o is not None and o.get("qty"):
            order_qty = float(o["qty"])

        if state == "rejected":
            self.store.update_order(link, status="Rejected")
            return _OrderResult("rejected", order_id)

        cum = float((o or {}).get("cum_qty") or 0.0)
        fills: list[dict] = []
        if order_id and (cum > 0 or state == "filled"):
            expect = cum if cum > 0 else order_qty
            execs = self._fetch_executions(ex, order_id, expect, step)
            fills = self._ingest_fills(ctx, link, order_id, execs, order_qty, step, apply)
        if o is not None:
            self.store.update_order(link, status=str(o.get("status") or state))
        new_qty = sum(float(f["qty"]) for f in fills)

        if state == "filled":
            return _OrderResult("filled", order_id, new_qty, fills)
        if state == "cancelled":
            return _OrderResult("partial" if cum > 0 else "cancelled", order_id, new_qty, fills)
        return _OrderResult("timeout", order_id, new_qty, fills)

    def _await_terminal(self, ex: ExchangeBase, link: str) -> tuple[str, dict | None]:
        """fill_poll_timeout_s 동안 get_order 폴링. (filled|cancelled|rejected|timeout, 마지막 주문)"""
        timeout = float(self.settings.fill_poll_timeout_s)
        interval = max(0.05, float(self.settings.fill_poll_interval_s))
        deadline = time.monotonic() + timeout
        last: dict | None = None
        while True:
            try:
                o = ex.get_order(link)
            except ExchangeError as e:
                log.warning("get_order %s failed (%s), retrying", link, e.code)
                o = None
            if o is not None:
                last = o
                status = str(o.get("status") or "")
                if status == "Filled":
                    return "filled", o
                if status == "Rejected":
                    return "rejected", o
                if status in ("Cancelled", "Deactivated"):
                    return "cancelled", o
            if time.monotonic() >= deadline:
                return "timeout", last
            time.sleep(interval)

    def _fetch_executions(self, ex: ExchangeBase, order_id: str, expect_qty: float, step: float) -> list[dict]:
        """체결 목록이 주문 누적수량에 도달할 때까지(지연 대비) 잠시 재조회."""
        deadline = time.monotonic() + max(1.0, float(self.settings.fill_poll_timeout_s) / 2)
        interval = max(0.05, float(self.settings.fill_poll_interval_s))
        execs: list[dict] = []
        while True:
            execs = list(ex.executions(order_id))
            total = sum(float(e.get("qty") or 0.0) for e in execs)
            if total + step / 2 >= expect_qty:
                return execs
            if time.monotonic() >= deadline:
                log.warning("executions for %s incomplete: %s < %s", order_id, total, expect_qty)
                return execs
            time.sleep(interval)

    def _ingest_fills(self, ctx: _Ctx, link: str, order_id: str, execs: list[dict], order_qty: float, step: float,
                      apply: Callable[[_Ctx, dict], None], reason_code: str | None = None) -> list[dict]:
        """체결을 시간순으로 fills 에 기록(새 것만) → lot 반영 → 회신. 돌려주는 값 = 이번에 새로 반영한 체결."""
        known = {f["exec_id"]: f for f in self.store.fills_for_order(link)}
        execs = sorted(execs, key=lambda d: (int(d.get("exec_time_ms") or 0), str(d.get("exec_id") or "")))
        cum = 0.0
        new_fills: list[dict] = []
        for e in execs:
            exec_id = str(e.get("exec_id") or "")
            qty = float(e.get("qty") or 0.0)
            price = float(e.get("price") or 0.0)
            t = int(e.get("exec_time_ms") or now_ms())
            if not exec_id or qty <= 0 or price <= 0:
                continue
            cum += qty
            status = "filled" if qty_eq(cum, order_qty, step) or cum > order_qty else "partially_filled"
            fill = {"exec_id": exec_id, "qty": qty, "price": price, "exec_time_ms": t, "order_id": order_id}
            prior = known.get(exec_id)
            if prior is None:
                inserted = self.store.insert_fill(exec_id, ctx.mode, ctx.position_id, qty, price, t,
                                                  order_id=order_id, order_link_id=link, event_id=ctx.event_id)
                if inserted:
                    apply(ctx, fill)
                    new_fills.append(fill)
                    self._report(ctx, status, qty=qty, fill_price=price, order_id=order_id, fill_id=exec_id,
                                 reason_code=reason_code, observed_at_ms=t)
                    self.store.mark_fill_reported(exec_id)
            elif not int(prior.get("reported") or 0):
                # 재시작 복구: 기록은 됐지만 회신 전이었던 체결
                self._report(ctx, status, qty=qty, fill_price=price, order_id=order_id, fill_id=exec_id,
                             reason_code=reason_code, observed_at_ms=t)
                self.store.mark_fill_reported(exec_id)
        if new_fills:
            log.info("%s: %d fill(s) qty=%s", ctx.tag(), len(new_fills), sum(f["qty"] for f in new_fills))
        return new_fills

    # ------------------------------------------------------------------ lot 반영 (체결 1건 단위)
    def _apply_open_fill(self, ctx: _Ctx, fill: dict) -> None:
        """entry/add 체결 → lot 생성 또는 수량/가중평균 갱신."""
        sig = ctx.sig
        lot = self.store.get_lot(ctx.mode, ctx.position_id)
        t = now_ms()
        qty, price = float(fill["qty"]), float(fill["price"])
        if lot is None or lot["status"] != st.LOT_OPEN:
            lot = {
                "mode": ctx.mode, "position_id": ctx.position_id, "strategy": ctx.strategy, "leg": ctx.leg,
                "position_idx": int(ctx.position_idx), "qty": qty, "avg_entry": price,
                "stop_loss": sig.stop_loss if sig is not None else None,
                "take_profit": sig.tp_list if sig is not None else None,
                "protection_revision": int(sig.protection_revision) if sig is not None else 0,
                "protection_orders": _empty_protection(), "status": st.LOT_OPEN,
                "opened_at_ms": t, "updated_at_ms": t, "closed_at_ms": None, "last_event_id": ctx.event_id,
            }
        else:
            old_qty = float(lot["qty"])
            old_avg = float(lot.get("avg_entry") or price)
            new_qty = old_qty + qty
            lot["avg_entry"] = (old_avg * old_qty + price * qty) / new_qty if new_qty > 0 else price
            lot["qty"] = new_qty
            lot["updated_at_ms"] = t
            lot["last_event_id"] = ctx.event_id
        self.store.upsert_lot(lot)

    def _apply_close_fill(self, ctx: _Ctx, fill: dict) -> None:
        """exit / 보호주문 체결 → lot 수량 감소 (닫힘 판정은 finalize 에서)."""
        lot = self.store.get_lot(ctx.mode, ctx.position_id)
        if lot is None:
            log.error("%s: close fill for unknown lot", ctx.tag())
            return
        step = float(ctx.exchange.instrument()["qty_step"]) if ctx.exchange is not None else 0.0
        remain = float(lot["qty"]) - float(fill["qty"])
        if remain < step / 2 or remain <= 0:
            remain = 0.0
        lot["qty"] = remain
        lot["updated_at_ms"] = now_ms()
        lot["last_event_id"] = ctx.event_id
        self.store.upsert_lot(lot)

    def _finalize_open(self, ctx: _Ctx) -> None:
        lot = self.store.get_lot(ctx.mode, ctx.position_id)
        if lot is None or lot["status"] != st.LOT_OPEN or float(lot["qty"]) <= 0:
            return
        self._reset_protections(ctx, lot)

    def _finalize_close(self, ctx: _Ctx) -> None:
        lot = self.store.get_lot(ctx.mode, ctx.position_id)
        if lot is None or lot["status"] != st.LOT_OPEN:
            return
        if float(lot["qty"]) <= 0:
            self._close_lot(ctx, lot)
        else:
            self._reset_protections(ctx, lot)

    def _close_lot(self, ctx: _Ctx, lot: dict) -> None:
        self._cancel_protections(ctx, lot)
        t = now_ms()
        lot["qty"] = 0.0
        lot["status"] = st.LOT_CLOSED
        lot["closed_at_ms"] = t
        lot["updated_at_ms"] = t
        lot["last_event_id"] = ctx.event_id
        self.store.upsert_lot(lot)
        log.info("%s: lot closed", ctx.tag())

    def _finish_market(self, ctx: _Ctx, res: _OrderResult, finalize: Callable[[_Ctx], None]) -> None:
        """시장가 결과 → 보호주문 정리 → 회신(실패 상태) → 신호 결과 → 스냅샷."""
        if res.state == "rejected":
            log.warning("%s: order rejected by exchange", ctx.tag())
            self._report(ctx, "rejected", reason_code="EXCHANGE_REJECTED")
            self.store.set_signal_result(ctx.event_id, st.SIGNAL_REJECTED, "EXCHANGE_REJECTED", "")
            self._alert(f"[executor:{ctx.mode}] {ctx.action} {ctx.position_id} {ctx.event_id}: EXCHANGE_REJECTED")
            return
        if res.state == "cancelled":
            log.warning("%s: order cancelled without fills", ctx.tag())
            self._report(ctx, "cancelled")
            self.store.set_signal_result(ctx.event_id, st.SIGNAL_REJECTED, None, "cancelled without fills")
            self._snapshot_quiet(ctx.mode)
            return

        finalize(ctx)   # 체결이 반영된 lot 기준으로 보호주문 생성/재설정/취소 (실패 시 예외 → error)

        notes: list[str] = []
        if res.state == "partial":
            self._report(ctx, "cancelled")   # IOC 잔량 취소
            notes.append("IOC_PARTIAL")
        if res.state == "timeout":
            self._report(ctx, "error", reason_code="EXCHANGE_TIMEOUT")
            self.store.set_signal_result(ctx.event_id, st.SIGNAL_ERROR, "EXCHANGE_TIMEOUT", " ".join(notes))
            self._alert(f"[executor:{ctx.mode}] {ctx.action} {ctx.position_id} {ctx.event_id}: EXCHANGE_TIMEOUT "
                        f"(order may still exist; reconcile will verify)")
            self._snapshot_quiet(ctx.mode)
            return
        mismatch = self._check_expected_qty(ctx)
        if mismatch:
            notes.append(mismatch)
        self.store.set_signal_result(ctx.event_id, st.SIGNAL_DONE, None, " ".join(notes))
        log.info("%s: done (%s filled=%s)", ctx.tag(), res.state, res.filled_qty)
        self._snapshot_quiet(ctx.mode)

    def _check_expected_qty(self, ctx: _Ctx) -> str | None:
        sig = ctx.sig
        if sig is None or sig.expected_qty_btc_after is None or ctx.exchange is None:
            return None
        lot = self.store.get_lot(ctx.mode, ctx.position_id)
        actual = float(lot["qty"]) if lot is not None and lot["status"] == st.LOT_OPEN else 0.0
        step = float(ctx.exchange.instrument()["qty_step"])
        if qty_eq(actual, float(sig.expected_qty_btc_after), step):
            return None
        self._alert(f"[executor:{ctx.mode}] {ctx.position_id} {ctx.event_id}: QTY_MISMATCH "
                    f"expected={fmt_step(sig.expected_qty_btc_after, step)} actual={fmt_step(actual, step)}")
        return "QTY_MISMATCH"

    # ------------------------------------------------------------------ 보호주문
    @staticmethod
    def _protection_link_id(mode: str, position_id: str, kind: str, revision: int, i: int, gen: int) -> str:
        parts = [mode, position_id, kind, str(int(revision)), str(int(i))]
        if gen > 0:
            parts.append(f"g{int(gen)}")
        return order_link_id(*parts)

    def _protection_entries(self, lot: dict) -> list[tuple[str, int, dict]]:
        po = lot.get("protection_orders") or {}
        out: list[tuple[str, int, dict]] = []
        if po.get("sl"):
            out.append(("sl", 0, po["sl"]))
        for e in po.get("tp") or []:
            out.append(("tp", int(e.get("i", 0)), e))
        return out

    def _cancel_protections(self, ctx: _Ctx, lot: dict) -> None:
        """lot 에 기록된 보호주문 전부 취소. 취소 실패(False/예외)는 _ProtectionError — 이중 보호주문을 막는다."""
        ex = ctx.exchange
        assert ex is not None
        po = dict(lot.get("protection_orders") or _empty_protection())
        for kind, i, e in self._protection_entries(lot):
            link = e.get("order_link_id")
            if not link:
                continue
            try:
                ok = ex.cancel_order(link)
            except ExchangeError as err:
                raise _ProtectionError(f"cancel {kind}[{i}] failed: {err.code}") from None
            if not ok:
                raise _ProtectionError(f"cancel {kind}[{i}] refused")
            self.store.update_order(link, status="Cancelled")
        po["sl"] = None
        po["tp"] = []
        lot["protection_orders"] = po
        lot["updated_at_ms"] = now_ms()
        self.store.upsert_lot(lot)

    def _reset_protections(self, ctx: _Ctx, lot: dict) -> None:
        """수량 변경 후: 취소 → 같은 revision 으로 재생성(세대 번호 증가)."""
        self._cancel_protections(ctx, lot)
        po = dict(lot.get("protection_orders") or _empty_protection())
        po["gen"] = int(po.get("gen") or 0) + 1
        lot["protection_orders"] = po
        self._place_protections(ctx, lot)

    def _place_protections(self, ctx: _Ctx, lot: dict) -> None:
        """lot.stop_loss / lot.take_profit 기준으로 조건부 reduceOnly 시장가 보호주문 생성.
        TP 는 남은 레벨(tp_done 제외)에 lot 수량을 균등 분할, 나머지는 마지막 레벨. min_qty 미만 레벨은 건너뜀."""
        ex = ctx.exchange
        assert ex is not None
        inst = ex.instrument()
        step, min_qty = float(inst["qty_step"]), float(inst["min_qty"])
        po = dict(lot.get("protection_orders") or _empty_protection())
        po.setdefault("gen", 0)
        po.setdefault("tp_done", [])
        po["sl"] = None
        po["tp"] = []
        lot["protection_orders"] = po
        qty = floor_step(float(lot["qty"]), step)
        if qty <= 0:
            self.store.upsert_lot(lot)
            return
        is_long = lot["leg"] == "long"

        sl_price = lot.get("stop_loss")
        if sl_price is not None and float(sl_price) > 0:
            if qty >= min_qty:
                po["sl"] = self._place_one_protection(ctx, lot, "sl", 0, float(sl_price), qty,
                                                      trigger_direction=2 if is_long else 1)
                self.store.upsert_lot(lot)
            else:
                log.warning("%s: SL skipped, qty %s below min", ctx.tag(), qty)

        tps = list(lot.get("take_profit") or [])
        done = {int(x) for x in po.get("tp_done") or []}
        levels = [(i, float(p)) for i, p in enumerate(tps) if i not in done and float(p) > 0]
        if levels:
            n = len(levels)
            per = floor_step(qty / n, step)
            qtys = [per] * (n - 1) + [floor_step(qty - per * (n - 1) + step / 4, step)]
            for (i, price), q in zip(levels, qtys):
                if q < min_qty or q <= 0:
                    log.warning("%s: TP[%d] skipped, qty %s below min", ctx.tag(), i, q)
                    continue
                entry = self._place_one_protection(ctx, lot, "tp", i, price, q, trigger_direction=1 if is_long else 2)
                po["tp"].append(entry)
                self.store.upsert_lot(lot)
        lot["updated_at_ms"] = now_ms()
        self.store.upsert_lot(lot)
        log.info("%s: protections placed sl=%s tp=%d (rev=%s gen=%s)", ctx.tag(),
                 (po["sl"] or {}).get("price"), len(po["tp"]), lot.get("protection_revision"), po.get("gen"))

    def _place_one_protection(self, ctx: _Ctx, lot: dict, kind: str, i: int, price: float, qty: float, *,
                              trigger_direction: int) -> dict:
        ex = ctx.exchange
        assert ex is not None
        po = lot["protection_orders"]
        side = "Sell" if lot["leg"] == "long" else "Buy"
        rev = int(lot.get("protection_revision") or 0)
        last_err: ExchangeRejected | None = None
        for attempt in range(2):
            gen = int(po.get("gen") or 0)
            link = self._protection_link_id(ctx.mode, lot["position_id"], kind, rev, i, gen)
            self.store.insert_order(link, ctx.mode, lot["position_id"], kind, side, qty, True,
                                    event_id=ctx.event_id, status="new", trigger_price=price)
            try:
                r = ex.place_conditional(side, qty, int(lot["position_idx"]), price, trigger_direction, link,
                                        self.settings.protection_trigger_by)
            except ExchangeRejected as e:
                self.store.update_order(link, status="Rejected")
                last_err = e
                if attempt == 0 and (e.ret_code == 110072 or "duplicate" in (e.message or "").lower()):
                    po["gen"] = gen + 1   # orderLinkId 중복 → 세대 올려 한 번 더
                    continue
                raise
            order_id = str(r.get("order_id") or "")
            self.store.update_order(link, order_id=order_id, status="Untriggered")
            return {"order_link_id": link, "order_id": order_id, "price": float(price), "qty": float(qty), "i": int(i)}
        assert last_err is not None  # pragma: no cover
        raise last_err

    # ------------------------------------------------------------------ reconcile / snapshot
    def reconcile(self, mode: str) -> bool:
        """① 보호주문 체결 수집(auto:sl/auto:tp 회신) ② 거래소 포지션 vs open lots 합계 대조.
        일치하면 True(스냅샷 가능), 불일치/오류면 False."""
        with self._lock:
            ex = self.exchanges.get(mode)
            if ex is None:
                log.debug("reconcile %s: no exchange for mode", mode)
                return False
            try:
                self._reconcile_protections(mode, ex)
                return self._reconcile_positions(mode, ex)
            except ExchangeError as e:
                log.warning("reconcile %s: exchange error %s", mode, e.code)
                return False
            except Exception as e:  # noqa: BLE001
                log.exception("reconcile %s: unexpected %s", mode, type(e).__name__)
                self._alert(f"[executor:{mode}] reconcile failed: {type(e).__name__}")
                return False

    def _reconcile_protections(self, mode: str, ex: ExchangeBase) -> None:
        for lot in self.store.open_lots(mode):
            changed = False
            for kind, i, e in self._protection_entries(lot):
                link = e.get("order_link_id")
                if not link:
                    continue
                o = ex.get_order(link)
                if o is None:
                    log.warning("reconcile %s: protection %s[%d] of %s not found on exchange", mode, kind, i,
                                lot["position_id"])
                    continue
                if e.get("order_id") and o.get("order_id") and str(o["order_id"]) != str(e["order_id"]):
                    log.warning("reconcile %s: protection %s[%d] of %s order_id mismatch, skipping", mode, kind, i,
                                lot["position_id"])
                    continue
                status = str(o.get("status") or "")
                if status in _PROTECTION_FILLING:
                    self._handle_protection_fill(mode, ex, lot, kind, i, e, o)
                    changed = True
                    lot = self.store.get_lot(mode, lot["position_id"]) or lot
                    if lot["status"] != st.LOT_OPEN:
                        break
                elif status not in _PROTECTION_OPEN and status != "Triggered":
                    log.warning("reconcile %s: protection %s[%d] of %s is %s", mode, kind, i, lot["position_id"], status)
            if changed and lot["status"] == st.LOT_OPEN and float(lot["qty"]) > 0:
                ctx = self._ctx_from_lot(mode, lot, lot.get("last_event_id") or "", "partial_exit", ex)
                self._reset_protections(ctx, lot)

    def _handle_protection_fill(self, mode: str, ex: ExchangeBase, lot: dict, kind: str, i: int, entry: dict,
                                o: dict) -> None:
        pid = lot["position_id"]
        rev = int(lot.get("protection_revision") or 0)
        event_id = self._auto_event_id(kind, pid, rev, i)
        order_id = str(o.get("order_id") or entry.get("order_id") or "")
        order_qty = float(o.get("qty") or entry.get("qty") or 0.0)
        step = float(ex.instrument()["qty_step"])
        execs = ex.executions(order_id) if order_id else []
        known = {f["exec_id"] for f in self.store.fills_for_order(entry["order_link_id"])}
        new_total = sum(float(x.get("qty") or 0.0) for x in execs if str(x.get("exec_id") or "") not in known)
        remaining = float(lot["qty"]) - new_total
        action = "full_exit" if remaining < step / 2 else "partial_exit"
        reason = "STOP_LOSS_TRIGGERED" if kind == "sl" else "TAKE_PROFIT_TRIGGERED"
        ctx = self._ctx_from_lot(mode, lot, event_id, action, ex)
        log.info("reconcile %s: %s[%d] of %s %s (new fill qty=%s)", mode, kind, i, pid, o.get("status"), new_total)
        fills = self._ingest_fills(ctx, entry["order_link_id"], order_id, execs, order_qty, step,
                                   self._apply_close_fill, reason_code=reason)
        self.store.update_order(entry["order_link_id"], status=str(o.get("status") or ""))

        lot = self.store.get_lot(mode, pid) or lot
        po = dict(lot.get("protection_orders") or _empty_protection())
        if str(o.get("status")) == "Filled":
            if kind == "tp":
                done = [int(x) for x in po.get("tp_done") or []]
                if i not in done:
                    done.append(i)
                po["tp_done"] = done
                po["tp"] = [t for t in po.get("tp") or [] if t.get("order_link_id") != entry["order_link_id"]]
            else:
                po["sl"] = None
            lot["protection_orders"] = po
            self.store.upsert_lot(lot)
        if fills:
            self._alert(f"[executor:{mode}] {reason} {pid}: qty={fmt_step(sum(f['qty'] for f in fills), step)} "
                        f"remaining={fmt_step(float(lot['qty']), step)}")
        if float(lot["qty"]) <= 0:
            self._close_lot(ctx, lot)

    @staticmethod
    def _auto_event_id(kind: str, position_id: str, revision: int, i: int) -> str:
        eid = f"auto:{kind}:{position_id}:{revision}" if kind == "sl" else f"auto:{kind}:{position_id}:{revision}:{i}"
        if len(eid) > 128:   # ID 패턴 길이 상한 (position_id 가 아주 길 때만)
            short = sha256_hex(position_id)[:24]
            eid = f"auto:{kind}:{short}:{revision}" if kind == "sl" else f"auto:{kind}:{short}:{revision}:{i}"
        return eid

    def _reconcile_positions(self, mode: str, ex: ExchangeBase) -> bool:
        step = float(ex.instrument()["qty_step"])
        positions = ex.positions()
        expected: dict[int, float] = {}
        for lot in self.store.open_lots(mode):
            idx = int(lot["position_idx"])
            q = float(lot["qty"])
            if idx == 0:
                q = q if lot["leg"] == "long" else -q
            expected[idx] = expected.get(idx, 0.0) + q
        mismatches: list[str] = []
        for idx in sorted(set(expected) | set(positions)):
            p = positions.get(idx)
            size = float(p["size"]) if p else 0.0
            if idx == 0 and p and str(p.get("side")) == "Sell":
                size = -size
            exp = expected.get(idx, 0.0)
            if not qty_eq(exp, size, step):
                mismatches.append(f"idx{idx}: lots={fmt_step(exp, step)} exchange={fmt_step(size, step)}")
        was = self.store.is_inconsistent(mode)
        if mismatches:
            note = "; ".join(mismatches)[:400]
            self.store.set_inconsistent(mode, True, note)
            if not was:
                self._alert(f"[executor:{mode}] RECONCILE_REQUIRED: {note}")
            else:
                log.warning("reconcile %s: still inconsistent (%s)", mode, note)
            return False
        self.store.set_inconsistent(mode, False, "")
        if was:
            log.info("reconcile %s: consistent again", mode)
            self._alert(f"[executor:{mode}] reconcile OK: positions consistent again")
        return True

    def build_snapshot_positions(self, mode: str) -> list[dict]:
        """계약 snapshot.positions 항목 (open lot 기준, 보호가격은 거래소에서 확인된 것만)."""
        with self._lock:
            ex = self.exchanges.get(mode)
            lots = self.store.open_lots(mode)
            mark: float | None = None
            if ex is not None and lots:
                try:
                    m = ex.mark_price()
                    mark = float(m) if m and float(m) > 0 else None
                except Exception as e:  # noqa: BLE001
                    log.warning("snapshot %s: mark_price failed (%s)", mode, type(e).__name__)
            out: list[dict] = []
            for lot in lots:
                qty = float(lot["qty"])
                entry = float(lot.get("avg_entry") or 0.0)
                if qty <= 0 or entry <= 0:
                    log.warning("snapshot %s: skipping lot %s (qty=%s entry=%s)", mode, lot["position_id"], qty, entry)
                    continue
                stop_loss, take_profit = self._confirmed_protection(ex, lot)
                out.append({
                    "position_id": lot["position_id"],
                    "strategy": lot["strategy"],
                    "leg": lot["leg"],
                    "position_idx": int(lot["position_idx"]),
                    "qty": qty,
                    "entry_price": entry,
                    "mark_price": mark,
                    "stop_loss": stop_loss,
                    "take_profit": take_profit,
                    "updated_at_ms": int(lot["updated_at_ms"]),
                })
            return out

    def _confirmed_protection(self, ex: ExchangeBase | None, lot: dict) -> tuple[float | None, list[float] | None]:
        """(stop_loss, take_profit): 거래소에서 열려 있음(New|Untriggered)이 확인된 가격만.
        확인 불가(거래소 없음/오류) → (None, None). TP 주문이 하나도 없으면 []."""
        if ex is None:
            return None, None
        po = lot.get("protection_orders") or {}
        try:
            sl: float | None = None
            e = po.get("sl")
            if e and e.get("order_link_id"):
                o = ex.get_order(e["order_link_id"])
                if o is not None and str(o.get("status")) in _PROTECTION_OPEN:
                    sl = float(e["price"])
            tps: list[float] = []
            for e in po.get("tp") or []:
                if not e.get("order_link_id"):
                    continue
                o = ex.get_order(e["order_link_id"])
                if o is not None and str(o.get("status")) in _PROTECTION_OPEN:
                    tps.append(float(e["price"]))
            return sl, tps
        except Exception as e:  # noqa: BLE001
            log.warning("snapshot: protection check failed for %s (%s)", lot.get("position_id"), type(e).__name__)
            return None, None

    def snapshot_now(self, mode: str) -> dict | None:
        """reconcile(mode) 가 일치하면 전체 포지션 스냅샷 회신을 쌓고 그 결과를 돌려준다. 아니면 None."""
        with self._lock:
            if not self.reconcile(mode):
                return None
            positions = self.build_snapshot_positions(mode)
            return self.reporter.snapshot(mode, positions, observed_at_ms=now_ms())

    def _snapshot_quiet(self, mode: str) -> None:
        try:
            self.snapshot_now(mode)
        except Exception as e:  # noqa: BLE001
            log.exception("snapshot %s failed: %s", mode, type(e).__name__)
            self._alert(f"[executor:{mode}] snapshot failed: {type(e).__name__}")

    # ------------------------------------------------------------------ 문맥/회신/알림
    @staticmethod
    def _ctx_from_signal(sig: Signal) -> _Ctx:
        return _Ctx(mode=sig.mode.value, event_id=sig.event_id, position_id=sig.position_id,
                    strategy=sig.strategy.value, leg=sig.leg.value, position_idx=int(sig.position_idx),
                    action=sig.action.value, sig=sig)

    @staticmethod
    def _ctx_from_lot(mode: str, lot: dict, event_id: str, action: str, ex: ExchangeBase | None) -> _Ctx:
        return _Ctx(mode=mode, event_id=event_id, position_id=lot["position_id"], strategy=lot["strategy"],
                    leg=lot["leg"], position_idx=int(lot["position_idx"]), action=action, exchange=ex)

    def _unknown_state(self, ctx: _Ctx, note: str) -> None:
        log.error("%s: UNKNOWN_STATE (%s)", ctx.tag(), note)
        self._report(ctx, "error", reason_code="UNKNOWN_STATE")
        self.store.set_signal_result(ctx.event_id, st.SIGNAL_ERROR, "UNKNOWN_STATE", note)
        self._alert(f"[executor:{ctx.mode}] {ctx.action} {ctx.position_id} {ctx.event_id}: UNKNOWN_STATE ({note})")

    def _report(self, ctx: _Ctx, status: str, *, qty: float | None = None, fill_price: float | None = None,
                order_id: str | None = None, fill_id: str | None = None, reason_code: str | None = None,
                observed_at_ms: int | None = None) -> None:
        """reporter.execution 래퍼. 회신 적재 실패가 매매 흐름을 끊지 않도록 예외는 로그+알림으로 흡수."""
        try:
            self.reporter.execution(
                ctx.mode, event_id=ctx.event_id, position_id=ctx.position_id, strategy=ctx.strategy, leg=ctx.leg,
                position_idx=ctx.position_idx, action=ctx.action, status=status, qty=qty, fill_price=fill_price,
                order_id=order_id, fill_id=fill_id, reason_code=reason_code,
                observed_at_ms=observed_at_ms if observed_at_ms is not None else now_ms())
        except Exception as e:  # noqa: BLE001
            log.exception("report %s for %s failed: %s", status, ctx.tag(), type(e).__name__)
            self._alert(f"[executor:{ctx.mode}] report {status} for {ctx.event_id} failed: {type(e).__name__}")

    def _alert(self, text: str) -> None:
        try:
            self.alerts.send(text)
        except Exception:  # noqa: BLE001
            log.error("alert failed: %s", text)
