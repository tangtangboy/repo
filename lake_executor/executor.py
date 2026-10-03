"""실행기 (ARCHITECTURE.md §1 게이트, §3 실행기).

접수된 신호를 FIFO 로 하나씩 꺼내(단일 워커) 거래소 주문/보호주문을 내고,
lot 원장·체결을 갱신하며 회신(reporter)을 쌓는다. 주기 스냅샷 전에는 `reconcile(mode)` 로
보호주문 체결(auto:sl/auto:tp)과 거래소 포지션 합계를 대조하고, 빠진 보호주문·불명 주문을 자가 복구한다.

설계 메모
  - process / reconcile / snapshot_now / recover_processing 은 하나의 RLock 으로 직렬화한다
    (워커 스레드와 스냅샷 스레드가 같은 lot 을 만지므로).
  - 체결은 한 건씩 `fills` 에 기록(applied=0) → lot 반영(applied=1) → 회신(reported=1, 회신 생성과 같은 트랜잭션)
    순으로 처리해 재시작 시 이중 반영·이중 보고를 막는다.
  - **체결 결과와 보호주문 결과는 분리한다.** 시장가가 체결됐으면 그 신호는 체결대로 종결(done) 하고, 보호주문
    생성/취소 실패는 note `PROTECTION_FAILED` + 알림으로만 남긴다(체결 뒤에 rejected/error 를 보내지 않는다).
    빠진 보호주문은 reconcile 이 lot.stop_loss/take_profit 로 다시 만든다(자가 복구).
  - 보호주문 트리거 가격이 이미 지나 있어 거래소가 조건부 주문을 거부하면(Bybit 110092/110093, "expected Rising/Falling"),
    그 보호를 **지금 reduceOnly 시장가로 실행**한다(auto:sl/auto:tp 회신). 포지션을 보호 없이 두지 않는다.
  - 보호주문을 취소할 때는 취소 응답과 무관하게 최종 상태를 다시 읽는다. 이미 체결(Filled/PartiallyFilled[Canceled])
    이면 체결로 처리한다("too late" 를 취소로 오인해 체결을 잃지 않도록).
  - 시장가 주문이 타임아웃/불명으로 끝나면 orders 행을 미확정 상태로 남기고 reconcile 이 종결될 때까지 재확인한다.
  - 보호주문 orderLinkId 는 `util.order_link_id(mode, position_id, "sl"|"tp", revision, i)` 이고(첫 생성),
    같은 revision 안에서 수량 재설정(취소 후 재생성)을 할 때는 거래소의 orderLinkId 중복 거부(110072)를 피하기 위해
    세대 번호(`protection_orders["gen"]` = 이 revision 에서 이미 낸 생성 횟수)를 뒤에 덧붙인다("g<n>").
    이미 쓴 ID(재진입 등)는 orders 원장에서 먼저 걸러 거래소 왕복 없이 세대를 올린다.
  - auto 이벤트 ID 는 lot 인스턴스(opened_at_ms)를 포함해 같은 position_id 재진입 때 재사용되지 않고,
    한 이벤트의 action 은 첫 보고 때 고정(meta)된다.
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
_TERMINAL = {"Filled", "Cancelled", "Rejected", "Deactivated", "PartiallyFilledCanceled"}
_PROTECTION_OPEN = {"New", "Untriggered"}
_PROTECTION_FILLING = {"Filled", "PartiallyFilled", "PartiallyFilledCanceled"}
# 시장가 주문의 종결을 확인하지 못한 orders.status (reconcile 이 거래소에서 재확인)
_UNCERTAIN_MARKET = ("unknown", "submitted", "New", "PartiallyFilled")
_MARKET_PURPOSES = ("entry", "add", "exit")
_UNCERTAIN_MAX_CHECKS = 20          # 이만큼 재확인해도 거래소에 없으면 absent 로 닫는다
# 보호주문 트리거가 이미 지났다는 거래소 거부 (Bybit 110092/110093, 메시지 "expected Rising/Falling ...")
_CROSSED_RET_CODES = {110092, 110093}
_CROSSED_WORDS = ("expected rising", "expected falling", "expect rising", "expect falling", "crossed")
_MAX_PROTECTION_DEPTH = 3           # crossed 실행 → 재설정 재귀 상한

# 보호주문 orderLinkId 세대 탐색 상한 (원장 사전 검사 / 거래소 110072 재시도)
_MAX_LINK_GEN_ATTEMPTS = 64
_MAX_LINK_DUP_RETRIES = 3

# ingress_log 보존 (수신기 거부 기록 무한 증가 방지)
_INGRESS_LOG_MAX_AGE_MS = 7 * 86_400_000
_INGRESS_LOG_MAX_ROWS = 50_000
_INGRESS_PRUNE_EVERY_MS = 600_000


# 보호주문 기본 구조 (lot.protection_orders)
def _empty_protection() -> dict:
    return {"sl": None, "tp": [], "gen": 0, "tp_done": [], "skipped": [], "failed": False}


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
    """보호주문 취소/재생성 실패 (거래소 거부가 아닌 논리 실패)."""


class _CrossedTrigger(Exception):
    """조건부 주문 트리거가 이미 지나 거래소가 거부 → 지금 시장가로 실행해야 하는 보호."""

    def __init__(self, kind: str, i: int, price: float, qty: float):
        super().__init__(f"{kind}[{i}] trigger {price} already crossed")
        self.kind, self.i, self.price, self.qty = kind, int(i), float(price), float(qty)


def _is_crossed_trigger(e: ExchangeRejected) -> bool:
    if e.ret_code in _CROSSED_RET_CODES:
        return True
    msg = (e.message or "").lower()
    return any(w in msg for w in _CROSSED_WORDS)


class Executor:
    """`Executor(settings, store, exchanges, reporter, alerts)` — §3."""

    def __init__(self, settings, store: st.Store, exchanges: dict[str, ExchangeBase | None], reporter, alerts):
        self.settings = settings
        self.store = store
        self.exchanges = exchanges or {}
        self.reporter = reporter
        self.alerts = alerts
        self._lock = threading.RLock()
        # test(paper) 모드: 신호의 reference_price 로 모의 시세를 움직인다 (lake TEST 신호가 SLIPPAGE_GUARD 로 죽지 않도록)
        self.follow_reference_price_on_paper = True
        self._orphan_alerted: set[str] = set()
        self._last_ingress_prune_ms = 0

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
        """live 거래소가 준비돼 있으면 포지션 모드/레버리지/마진 모드를 맞춘다 (live 에서만, §3 마지막 줄).
        같은 설정을 이미 적용했으면(meta) 거래소 쓰기를 반복하지 않는다 (재시작 루프 보호)."""
        ex = self.exchanges.get("live")
        ok, _ = self.settings.live_execution_possible()
        if ex is None or not ok:
            return
        s = self.settings
        signature = f"{s.position_mode}|{s.leverage}|{s.margin_mode}|{s.testnet}|{s.symbol}"
        if self.store.get_meta("account_setup:live") == signature:
            log.info("live account setup already applied (mode=%s lev=%s margin=%s)", s.position_mode, s.leverage, s.margin_mode)
            return
        ex.ensure_account_setup(s.position_mode, s.leverage, s.margin_mode)
        self.store.set_meta("account_setup:live", signature)
        log.info("live account setup ensured (mode=%s lev=%s margin=%s)", s.position_mode, s.leverage, s.margin_mode)

    def start(self) -> None:
        """편의: ensure_account_setup() → recover_processing()."""
        self.ensure_account_setup()
        self.recover_processing()

    def recover_processing(self) -> None:
        """재시작 복구. status=processing 신호는 재실행하지 않고, 그 신호의 주문이 거래소에 있으면
        체결 수집을 이어서 마무리하고, 없으면 error/UNKNOWN_STATE 로 닫고 알린다.
        타임아웃/불명으로 남은 주문(orders.status 미확정)은 reconcile 의 재확인 경로가 이어서 처리한다."""
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
            self.store.set_signal_result(event_id, st.SIGNAL_DONE, "TEST_RECORD_ONLY", "recovered: record only", mode=mode)
            return
        if mode == "live":
            ok, _ = self.settings.live_execution_possible()
            if not ok or ex is None:
                self._unknown_state(ctx, "recovered: live exchange unavailable")
                return
        ctx.exchange = ex
        if sig.action not in (Action.entry, Action.add, Action.partial_exit, Action.full_exit):
            # protection_update 는 시장가 주문이 없어 중간 상태를 판정할 수 없음.
            # lot 이 이미 새 revision 으로 갱신돼 있었다면 보호주문이 빠졌을 수 있으니 reconcile 자가 복구 대상으로 표시한다.
            lot = self.store.get_lot(mode, ctx.position_id)
            if lot is not None and lot["status"] == st.LOT_OPEN and \
                    int(lot.get("protection_revision") or 0) == int(sig.protection_revision):
                po = dict(lot.get("protection_orders") or _empty_protection())
                po["failed"] = True
                lot["protection_orders"] = po
                self.store.upsert_lot(lot)
            self._unknown_state(ctx, "recovered: no resumable order for this action")
            return

        link = order_link_id(mode, event_id)
        order_row = self.store.get_order(link)
        try:
            o = ex.get_order(link)
        except ExchangeError as e:
            log.warning("recover %s: get_order failed (%s)", ctx.tag(), e.code)
            if order_row is not None:
                self.store.update_order(link, status="unknown")   # reconcile 재확인 대상
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
            self.store.set_signal_result(event_id, st.SIGNAL_ERROR, "UNKNOWN_STATE", "stored body invalid",
                                         mode=row.get("mode"))
            self._alert(f"[executor] signal {event_id}: stored body invalid")
            return None

    def _guarded(self, ctx: _Ctx, fn: Callable[[], None]) -> None:
        """예외 → 회신(rejected|error) + 신호 결과 + 알림. 루프는 절대 죽지 않는다.
        (체결 뒤의 보호주문 실패는 _finish_market 이 흡수하므로 여기 오는 예외는 체결 전 실패다.)"""
        try:
            fn()
        except ExchangeRejected as e:
            log.warning("%s: exchange rejected (ret_code=%s)", ctx.tag(), e.ret_code)
            self._report(ctx, "rejected", reason_code="EXCHANGE_REJECTED")
            self._set_result(ctx, st.SIGNAL_REJECTED, "EXCHANGE_REJECTED", f"ret_code={e.ret_code}")
            self._alert(f"[executor:{ctx.mode}] {ctx.action} {ctx.position_id} {ctx.event_id}: "
                        f"EXCHANGE_REJECTED ret_code={e.ret_code}")
            self._snapshot_quiet(ctx.mode)
        except ExchangeError as e:
            code = e.code if e.code in ("EXCHANGE_ERROR", "EXCHANGE_TIMEOUT") else "EXCHANGE_ERROR"
            log.warning("%s: exchange error %s", ctx.tag(), code)
            self._report(ctx, "error", reason_code=code)
            self._set_result(ctx, st.SIGNAL_ERROR, code, "")
            self._alert(f"[executor:{ctx.mode}] {ctx.action} {ctx.position_id} {ctx.event_id}: {code}")
            self._snapshot_quiet(ctx.mode)
        except _ProtectionError as e:
            log.error("%s: protection handling failed: %s", ctx.tag(), e)
            self._report(ctx, "error", reason_code="EXCHANGE_ERROR")
            self._set_result(ctx, st.SIGNAL_ERROR, "EXCHANGE_ERROR", str(e)[:200])
            self._alert(f"[executor:{ctx.mode}] {ctx.action} {ctx.position_id} {ctx.event_id}: "
                        f"protection orders need attention ({e})")
            self._snapshot_quiet(ctx.mode)
        except Exception as e:  # noqa: BLE001
            log.exception("%s: unexpected %s", ctx.tag(), type(e).__name__)
            self._report(ctx, "error", reason_code="UNKNOWN_STATE")
            try:
                self._set_result(ctx, st.SIGNAL_ERROR, "UNKNOWN_STATE", type(e).__name__)
            except Exception:  # noqa: BLE001
                log.exception("set_signal_result failed for %s", ctx.event_id)
            self._alert(f"[executor:{ctx.mode}] {ctx.action} {ctx.position_id} {ctx.event_id}: "
                        f"UNKNOWN_STATE ({type(e).__name__})")

    def _process_signal(self, ctx: _Ctx) -> None:
        sig = ctx.sig
        assert sig is not None
        mode = ctx.mode
        self._report(ctx, "acknowledged")

        # ---- 만료 (접수 뒤 지연: 재시작/백로그/HALT 해제 뒤 오래된 신호를 현재가로 실행하지 않는다)
        if now_ms() > int(sig.expires_at_ms):
            self._reject(ctx, "EXPIRED", f"expired_at_ms={sig.expires_at_ms}")
            return

        # ---- 모드 게이트 (§1)
        if mode == "test":
            ex = self.exchanges.get("test")
            if ex is None:
                # 기록 전용: acknowledged 하나만 보내고 끝
                self._set_result(ctx, st.SIGNAL_DONE, "TEST_RECORD_ONLY", "record only")
                log.info("%s: test record only", ctx.tag())
                return
            self._follow_reference_price(ex, sig)
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

    def _follow_reference_price(self, ex: ExchangeBase, sig: Signal) -> None:
        """PaperExchange 는 시세 피드가 없다. test 신호의 reference_price 를 모의 시세로 반영한다
        (트리거된 모의 SL/TP 는 다음 reconcile 이 체결로 처리)."""
        if not self.follow_reference_price_on_paper or getattr(ex, "name", "") != "paper":
            return
        ref = sig.reference_price
        set_price = getattr(ex, "set_price", None)
        if ref is None or ref <= 0 or not callable(set_price):
            return
        try:
            fired = set_price(float(ref))
            if fired:
                log.info("paper price -> %s (reference_price); %d conditional(s) triggered", ref, len(fired))
        except Exception as e:  # noqa: BLE001
            log.warning("paper set_price failed: %s", type(e).__name__)

    # ------------------------------------------------------------------ 액션
    def _do_entry(self, ctx: _Ctx) -> None:
        sig = ctx.sig
        assert sig is not None and ctx.exchange is not None
        lot = self.store.get_lot(ctx.mode, ctx.position_id)
        if lot is not None and lot["status"] == st.LOT_OPEN:
            self._reject(ctx, "POSITION_EXISTS")
            return
        if self._opposing_leg_open(ctx):
            self._reject(ctx, "OPPOSING_LEG")
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
        if self._opposing_leg_open(ctx):
            self._reject(ctx, "OPPOSING_LEG")
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
        rev, lot_rev = int(sig.protection_revision), int(lot.get("protection_revision") or 0)
        po_now = lot.get("protection_orders") or {}
        # 같은 revision 재전송은 그 revision 의 보호주문이 실패해 비어 있을 때만 멱등 재시도로 받는다
        if rev < lot_rev or (rev == lot_rev and not po_now.get("failed")):
            self._reject(ctx, "STALE_PROTECTION_REVISION", f"rev={rev} lot_rev={lot_rev}")
            return
        # 기존 보호주문 취소(체결 경합 확인 포함) → lot 의 SL/TP/revision 갱신 → 재생성
        lot = self._cancel_protections(ctx, lot)
        if lot["status"] != st.LOT_OPEN or float(lot["qty"]) <= 0:
            # 취소 중 보호주문 체결이 확인돼 lot 이 닫혔다
            self._reject(ctx, "POSITION_CLOSED", "lot closed by protection fill during update")
            self._snapshot_quiet(ctx.mode)
            return
        po = dict(lot.get("protection_orders") or _empty_protection())
        po.update({"sl": None, "tp": [], "gen": 0, "tp_done": [], "skipped": [], "failed": False})   # 새 revision 초기화
        lot["protection_orders"] = po
        lot["stop_loss"] = sig.stop_loss
        lot["take_profit"] = sig.tp_list   # None | [] | [가격...]
        lot["protection_revision"] = rev
        lot["last_event_id"] = ctx.event_id
        lot["updated_at_ms"] = now_ms()
        self.store.upsert_lot(lot)
        try:
            self._place_protections(ctx, lot)
        except (ExchangeError, _ProtectionError) as e:
            # 의도(새 SL/TP/revision)는 lot 에 남았고 po.failed=True → reconcile 이 재생성, 같은 revision 재전송도 허용
            self._protection_failed(ctx, e)
            self._report(ctx, "error", reason_code="PROTECTION_FAILED")
            self._set_result(ctx, st.SIGNAL_ERROR, "PROTECTION_FAILED", self._err_note(e))
            self._snapshot_quiet(ctx.mode)
            return
        self._report(ctx, "protection_updated")
        self._set_result(ctx, st.SIGNAL_DONE, None, "")
        log.info("%s: protection updated rev=%s sl=%s tp=%s", ctx.tag(), rev, sig.stop_loss, sig.tp_list)
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
        self._set_result(ctx, st.SIGNAL_REJECTED, code, note)

    def _set_result(self, ctx: _Ctx, status: str, reason_code: str | None, note: str) -> None:
        self.store.set_signal_result(ctx.event_id, status, reason_code, note, mode=ctx.mode)

    def _opposing_leg_open(self, ctx: _Ctx) -> bool:
        """단방향(idx 0) 에서는 반대 방향 lot 과 공존할 수 없다 (거래소가 네팅해 버린다)."""
        if int(ctx.position_idx) != 0:
            return False
        return any(l["leg"] != ctx.leg and float(l["qty"]) > 0
                   for l in self.store.open_lots(ctx.mode) if int(l["position_idx"]) == 0)

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
                        apply: Callable[[_Ctx, dict], None], link: str | None = None,
                        reason_code: str | None = None) -> _OrderResult:
        ex = ctx.exchange
        assert ex is not None
        link = link or order_link_id(ctx.mode, ctx.event_id)
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
            # 네트워크/타임아웃: 주문이 들어갔을 수 있다 → 한 번 조회해 있으면 이어서 정산, 아니면 미확정(unknown) 으로 남겨
            # reconcile 이 종결될 때까지 재확인한다
            log.warning("%s: place_market %s, checking whether order exists", ctx.tag(), e.code)
            try:
                o = ex.get_order(link)
            except ExchangeError as e2:
                log.warning("%s: get_order after place failure also failed (%s)", ctx.tag(), e2.code)
                o = None
            if o is None:
                self.store.update_order(link, status="unknown")
                raise
            order_id = str(o.get("order_id") or "") or None
        self.store.update_order(link, order_id=order_id, status="submitted")
        self._report(ctx, "submitted", order_id=order_id)
        log.info("%s: submitted %s %s qty=%s ro=%s order=%s", ctx.tag(), purpose, side, qty, reduce_only, order_id)
        return self._settle_order(ctx, link, order_id, qty, apply, reason_code=reason_code)

    def _settle_order(self, ctx: _Ctx, link: str, order_id: str | None, order_qty: float,
                      apply: Callable[[_Ctx, dict], None], reason_code: str | None = None) -> _OrderResult:
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
            fills = self._ingest_fills(ctx, link, order_id, execs, order_qty, step, apply, reason_code=reason_code)
        if o is not None:
            self.store.update_order(link, status=str(o.get("status") or state))
        new_qty = sum(float(f["qty"]) for f in fills)

        if state == "filled":
            return _OrderResult("filled", order_id, new_qty, fills)
        if state == "cancelled":
            return _OrderResult("partial" if cum > 0 else "cancelled", order_id, new_qty, fills)
        return _OrderResult("timeout", order_id, new_qty, fills)

    def _await_terminal(self, ex: ExchangeBase, link: str) -> tuple[str, dict | None]:
        """fill_poll_timeout_s 동안 get_order 폴링. (filled|cancelled|rejected|timeout, 마지막 주문)
        IOC 부분 체결 뒤 잔량 취소(PartiallyFilledCanceled) 는 cancelled 로 분류한다 (체결은 cum_qty 로 수집)."""
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
                if status in ("Cancelled", "Deactivated", "PartiallyFilledCanceled"):
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
        """체결을 시간순으로 fills 에 기록(새 것만) → lot 반영(applied) → 회신(reported, 회신 생성과 원자적).
        재시작 복구: 기록만 되고 반영/회신이 안 된 체결은 이어서 처리한다. 돌려주는 값 = 이번에 새로 lot 에 반영한 체결."""
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
                if not inserted:
                    continue
                applied, reported = False, False
            else:
                applied = bool(int(prior.get("applied") or 0))
                reported = bool(int(prior.get("reported") or 0))
            if not applied:
                apply(ctx, fill)
                self.store.mark_fill_applied(exec_id)
                new_fills.append(fill)
            if not reported:
                # fills.reported=1 은 회신 행 생성과 같은 트랜잭션에서 기록된다 (같은 fill_id 이중 보고 방지)
                self._report(ctx, status, qty=qty, fill_price=price, order_id=order_id, fill_id=exec_id,
                             reason_code=reason_code, observed_at_ms=t, mark_fill_reported=exec_id)
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
            if lot is not None and int(lot.get("opened_at_ms") or 0) >= t:
                t = int(lot["opened_at_ms"]) + 1   # lot 인스턴스(opened_at_ms) 는 같은 position_id 안에서 단조 증가
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
        """lot 종료. closed 상태를 **먼저** 기록하고 보호주문을 취소한다 — 취소가 실패해도 qty 0 lot 이 open 으로
        남지 않으며, 남은 보호주문 항목은 reconcile 이 취소될 때까지 재시도한다."""
        t = now_ms()
        lot["qty"] = 0.0
        lot["status"] = st.LOT_CLOSED
        lot["closed_at_ms"] = t
        lot["updated_at_ms"] = t
        lot["last_event_id"] = ctx.event_id
        self.store.upsert_lot(lot)
        log.info("%s: lot closed", ctx.tag())
        try:
            self._cancel_protections(ctx, lot)
        except (ExchangeError, _ProtectionError) as e:
            self._protection_failed(ctx, e, "cancel of leftover protections failed; reconcile will retry")

    def _finish_market(self, ctx: _Ctx, res: _OrderResult, finalize: Callable[[_Ctx], None]) -> None:
        """시장가 결과 → 보호주문 정리 → 회신(실패 상태) → 신호 결과 → 스냅샷.
        체결이 있었던 신호의 상태는 체결이 결정한다: 보호주문 실패는 note PROTECTION_FAILED + 알림일 뿐이다."""
        if res.state == "rejected":
            log.warning("%s: order rejected by exchange", ctx.tag())
            self._report(ctx, "rejected", reason_code="EXCHANGE_REJECTED")
            self._set_result(ctx, st.SIGNAL_REJECTED, "EXCHANGE_REJECTED", "")
            self._alert(f"[executor:{ctx.mode}] {ctx.action} {ctx.position_id} {ctx.event_id}: EXCHANGE_REJECTED")
            return
        if res.state == "cancelled":
            log.warning("%s: order cancelled without fills", ctx.tag())
            self._report(ctx, "cancelled")
            self._set_result(ctx, st.SIGNAL_REJECTED, None, "cancelled without fills")
            self._snapshot_quiet(ctx.mode)
            return

        notes: list[str] = []
        try:
            finalize(ctx)   # 체결이 반영된 lot 기준으로 보호주문 생성/재설정/취소
        except (ExchangeError, _ProtectionError) as e:
            self._protection_failed(ctx, e)
            notes.append("PROTECTION_FAILED")

        if res.state == "partial":
            self._report(ctx, "cancelled")   # IOC 잔량 취소
            notes.append("IOC_PARTIAL")
        if res.state == "timeout":
            self._report(ctx, "error", reason_code="EXCHANGE_TIMEOUT")
            self._set_result(ctx, st.SIGNAL_ERROR, "EXCHANGE_TIMEOUT", " ".join(notes))
            self._alert(f"[executor:{ctx.mode}] {ctx.action} {ctx.position_id} {ctx.event_id}: EXCHANGE_TIMEOUT "
                        f"(order may still exist; reconcile will verify)")
            self._snapshot_quiet(ctx.mode)
            return
        mismatch = self._check_expected_qty(ctx)
        if mismatch:
            notes.append(mismatch)
        self._set_result(ctx, st.SIGNAL_DONE, None, " ".join(notes))
        log.info("%s: done (%s filled=%s)", ctx.tag(), res.state, res.filled_qty)
        self._snapshot_quiet(ctx.mode)

    def _finish_verified(self, ctx: _Ctx, res: _OrderResult, finalize: Callable[[_Ctx], None]) -> None:
        """타임아웃/불명으로 끝났던 주문을 뒤늦게 종결 확인한 뒤의 마무리. 새 체결이 있으면 보호주문을 맞추고 신호를
        done 으로 바꾼다(체결 회신은 _ingest_fills 가 보냈다). 체결이 없었으면 주문 상태만 닫는다."""
        if res.state == "rejected" or (res.state == "cancelled" and not res.fills):
            self._set_result(ctx, st.SIGNAL_REJECTED, "EXCHANGE_REJECTED" if res.state == "rejected" else None,
                             f"verified late: {res.state} without fills")
            log.info("%s: verified late — %s, no fills", ctx.tag(), res.state)
            return
        if res.state == "timeout":
            return   # 아직 종결 아님: 다음 reconcile 에서 다시
        notes = ["LATE_VERIFIED"]
        try:
            finalize(ctx)
        except (ExchangeError, _ProtectionError) as e:
            self._protection_failed(ctx, e)
            notes.append("PROTECTION_FAILED")
        if res.state == "partial":
            notes.append("IOC_PARTIAL")
        mismatch = self._check_expected_qty(ctx)
        if mismatch:
            notes.append(mismatch)
        self._set_result(ctx, st.SIGNAL_DONE, None, " ".join(notes))
        self._alert(f"[executor:{ctx.mode}] {ctx.action} {ctx.position_id} {ctx.event_id}: late fills verified "
                    f"(qty={res.filled_qty}); lot and protections updated")

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

    @staticmethod
    def _drop_entry(lot: dict, kind: str, link: str) -> None:
        po = dict(lot.get("protection_orders") or _empty_protection())
        if kind == "sl":
            if (po.get("sl") or {}).get("order_link_id") == link:
                po["sl"] = None
        else:
            po["tp"] = [t for t in po.get("tp") or [] if t.get("order_link_id") != link]
        lot["protection_orders"] = po

    def _cancel_protections(self, ctx: _Ctx, lot: dict) -> dict:
        """lot 에 기록된 보호주문 전부 취소. 취소 응답과 무관하게 최종 상태를 다시 읽어, 이미 체결(부분 포함)된
        보호주문은 체결로 처리한다(auto:sl/tp 회신, lot 차감). 취소 실패/미확인은 _ProtectionError — 이중 보호주문을 막는다.
        반환: 갱신된 lot (체결 처리로 닫혔을 수 있다)."""
        ex = ctx.exchange
        assert ex is not None
        pid = lot["position_id"]
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
            o = self._read_final_status(ex, link)
            status = str((o or {}).get("status") or "")
            if o is not None and status in _PROTECTION_FILLING and (float(o.get("cum_qty") or 0.0) > 0 or status == "Filled"):
                # 취소보다 체결이 먼저였다 → 체결 수집 (lot 차감, 회신). 항목 제거는 _handle_protection_fill 이 한다.
                log.warning("%s: protection %s[%d] already %s when cancelling; ingesting fill", ctx.tag(), kind, i, status)
                self._handle_protection_fill(ctx.mode, ex, lot, kind, i, e, o)
                lot = self.store.get_lot(ctx.mode, pid) or lot
                if lot["status"] != st.LOT_OPEN:
                    return lot
                continue
            if o is not None and status not in _TERMINAL:
                raise _ProtectionError(f"cancel {kind}[{i}] not confirmed (status {status})")
            self.store.update_order(link, status=status or "Cancelled")
            self._drop_entry(lot, kind, link)
            lot["updated_at_ms"] = now_ms()
            self.store.upsert_lot(lot)
        return lot

    @staticmethod
    def _read_final_status(ex: ExchangeBase, link: str) -> dict | None:
        """취소 직후 상태 확인. Triggered(트리거됐지만 체결 전) 는 잠시 기다려 다시 읽는다."""
        o: dict | None = None
        for _ in range(4):
            try:
                o = ex.get_order(link)
            except ExchangeError as err:
                raise _ProtectionError(f"verify after cancel failed: {err.code}") from None
            if o is None or str(o.get("status") or "") != "Triggered":
                return o
            time.sleep(0.25)
        return o

    def _reset_protections(self, ctx: _Ctx, lot: dict, depth: int = 0) -> None:
        """수량 변경 후: 취소 → 같은 revision 으로 재생성. 세대 번호는 _place_protections 가 생성 후 올린다
        (이 revision 의 첫 생성은 계약 형태 그대로, 재생성부터 "g<n>" 접미)."""
        lot = self._cancel_protections(ctx, lot)
        if lot["status"] != st.LOT_OPEN or float(lot["qty"]) <= 0:
            return
        self._place_protections(ctx, lot, depth)

    def _place_protections(self, ctx: _Ctx, lot: dict, depth: int = 0) -> None:
        """lot.stop_loss / lot.take_profit 기준으로 조건부 reduceOnly 시장가 보호주문 생성.
        TP 는 남은 레벨(tp_done 제외)에 lot 수량을 균등 분할, 나머지는 마지막 레벨. min_qty 미만 레벨은 건너뛰고 skipped 에 기록.
        트리거가 이미 지난 가격(거래소 거부)은 지금 reduceOnly 시장가로 실행한다. 실패하면 po.failed=True 로 남기고 예외."""
        ex = ctx.exchange
        assert ex is not None
        if depth > _MAX_PROTECTION_DEPTH:
            raise _ProtectionError("protection placement recursion limit")
        inst = ex.instrument()
        step, min_qty = float(inst["qty_step"]), float(inst["min_qty"])
        po = dict(lot.get("protection_orders") or _empty_protection())
        po.setdefault("gen", 0)
        po.setdefault("tp_done", [])
        po["sl"] = None
        po["tp"] = []
        po["skipped"] = []
        po["failed"] = False
        lot["protection_orders"] = po
        qty = floor_step(float(lot["qty"]), step)
        if qty <= 0:
            self.store.upsert_lot(lot)
            return
        is_long = lot["leg"] == "long"
        try:
            sl_price = lot.get("stop_loss")
            if sl_price is not None and float(sl_price) > 0:
                if qty >= min_qty:
                    po["sl"] = self._place_one_protection(ctx, lot, "sl", 0, float(sl_price), qty,
                                                          trigger_direction=2 if is_long else 1)
                    self.store.upsert_lot(lot)
                else:
                    log.warning("%s: SL skipped, qty %s below min", ctx.tag(), qty)
                    po["skipped"].append("sl")

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
                        po["skipped"].append(f"tp{i}")
                        continue
                    entry = self._place_one_protection(ctx, lot, "tp", i, price, q, trigger_direction=1 if is_long else 2)
                    po["tp"].append(entry)
                    self.store.upsert_lot(lot)
        except _CrossedTrigger as c:
            self.store.upsert_lot(lot)
            self._execute_crossed_protection(ctx, lot, c, depth)
            return
        except (ExchangeError, _ProtectionError):
            po["failed"] = True
            lot["updated_at_ms"] = now_ms()
            self.store.upsert_lot(lot)
            raise
        placed_gen = int(po.get("gen") or 0)
        if po["sl"] is not None or po["tp"]:
            po["gen"] = placed_gen + 1   # 다음 재생성은 새 세대 ID 를 쓴다
        lot["updated_at_ms"] = now_ms()
        self.store.upsert_lot(lot)
        log.info("%s: protections placed sl=%s tp=%d (rev=%s gen=%s)", ctx.tag(),
                 (po["sl"] or {}).get("price"), len(po["tp"]), lot.get("protection_revision"), placed_gen)

    def _place_one_protection(self, ctx: _Ctx, lot: dict, kind: str, i: int, price: float, qty: float, *,
                              trigger_direction: int) -> dict:
        ex = ctx.exchange
        assert ex is not None
        po = lot["protection_orders"]
        side = "Sell" if lot["leg"] == "long" else "Buy"
        rev = int(lot.get("protection_revision") or 0)
        last_err: ExchangeRejected | None = None
        exchange_retries = 0
        for _ in range(_MAX_LINK_GEN_ATTEMPTS):
            gen = int(po.get("gen") or 0)
            link = self._protection_link_id(ctx.mode, lot["position_id"], kind, rev, i, gen)
            if self.store.get_order(link) is not None:
                # 같은 (position_id, revision, i) 로 이미 낸 적 있는 ID (닫힌 lot 재진입 등) → 거래소 왕복 없이 세대 올림
                po["gen"] = gen + 1
                continue
            self.store.insert_order(link, ctx.mode, lot["position_id"], kind, side, qty, True,
                                    event_id=ctx.event_id, status="new", trigger_price=price)
            try:
                r = ex.place_conditional(side, qty, int(lot["position_idx"]), price, trigger_direction, link,
                                        self.settings.protection_trigger_by)
            except ExchangeRejected as e:
                self.store.update_order(link, status="Rejected")
                last_err = e
                is_dup = e.ret_code == 110072 or "duplicate" in (e.message or "").lower()
                if is_dup and exchange_retries < _MAX_LINK_DUP_RETRIES:
                    exchange_retries += 1
                    po["gen"] = gen + 1   # 거래소가 orderLinkId 중복이라 함 → 세대 올려 다시
                    continue
                if _is_crossed_trigger(e):
                    po["gen"] = gen + 1
                    raise _CrossedTrigger(kind, i, price, qty) from None
                raise
            except ExchangeError as e:
                # 네트워크/타임아웃: 조건부 주문이 들어갔을 수 있다 → 조회해 있으면 그대로 기록 (고아 스탑 방지)
                log.warning("%s: place_conditional %s[%d] %s, checking whether order exists", ctx.tag(), kind, i, e.code)
                try:
                    o = ex.get_order(link)
                except ExchangeError:
                    o = None
                if o is None or str(o.get("status") or "") in ("Rejected", "Cancelled", "Deactivated"):
                    self.store.update_order(link, status="unknown" if o is None else str(o.get("status")))
                    raise
                order_id = str(o.get("order_id") or "")
                self.store.update_order(link, order_id=order_id, status=str(o.get("status") or "Untriggered"))
                po["gen"] = gen + 1
                return {"order_link_id": link, "order_id": order_id, "price": float(price), "qty": float(qty), "i": int(i)}
            order_id = str(r.get("order_id") or "")
            self.store.update_order(link, order_id=order_id, status="Untriggered")
            return {"order_link_id": link, "order_id": order_id, "price": float(price), "qty": float(qty), "i": int(i)}
        if last_err is not None:
            raise last_err
        raise _ProtectionError(f"no free orderLinkId for {kind}[{i}] after {_MAX_LINK_GEN_ATTEMPTS} generations")

    def _execute_crossed_protection(self, ctx: _Ctx, lot: dict, c: _CrossedTrigger, depth: int) -> None:
        """트리거가 이미 지난 SL/TP: 조건부 주문 대신 지금 reduceOnly 시장가로 그 보호를 실행한다.
        SL 은 lot 전량, TP[i] 는 그 레벨 수량. 회신은 auto:sl/auto:tp 이벤트로, 남은 수량의 보호주문은 다시 맞춘다."""
        ex = ctx.exchange
        assert ex is not None
        mode, pid = ctx.mode, lot["position_id"]
        step = float(ex.instrument()["qty_step"])
        rev = int(lot.get("protection_revision") or 0)
        lot_qty = floor_step(float(lot["qty"]), step)
        qty = lot_qty if c.kind == "sl" else min(floor_step(c.qty, step), lot_qty)
        if qty <= 0:
            return
        event_id = self._auto_event_id(c.kind, pid, rev, c.i, lot)
        action = self._auto_action(mode, event_id, "full_exit" if qty_eq(qty, lot_qty, step) else "partial_exit")
        reason = "STOP_LOSS_TRIGGERED" if c.kind == "sl" else "TAKE_PROFIT_TRIGGERED"
        actx = self._ctx_from_lot(mode, lot, event_id, action, ex)
        gen = int((lot.get("protection_orders") or {}).get("gen") or 0)
        link = order_link_id(mode, pid, c.kind, str(rev), str(c.i), f"g{gen}", "mkt")
        self._alert(f"[executor:{mode}] {pid}: {c.kind.upper()} trigger {c.price} already crossed at placement; "
                    f"executing reduceOnly market {action} qty={fmt_step(qty, step)}")
        log.warning("%s: %s", ctx.tag(), c)
        res = self._execute_market(actx, actx.close_side(), qty, reduce_only=True, purpose="exit",
                                   apply=self._apply_close_fill, link=link, reason_code=reason)
        if res.state == "rejected":
            raise _ProtectionError(f"crossed {c.kind}[{c.i}] market close rejected")
        if res.state == "timeout":
            raise _ProtectionError(f"crossed {c.kind}[{c.i}] market close not confirmed (reconcile will verify)")
        lot = self.store.get_lot(mode, pid) or lot
        po = dict(lot.get("protection_orders") or _empty_protection())
        if c.kind == "tp":
            done = [int(x) for x in po.get("tp_done") or []]
            if c.i not in done:
                done.append(c.i)
            po["tp_done"] = done
            lot["protection_orders"] = po
            self.store.upsert_lot(lot)
        if float(lot["qty"]) <= 0:
            self._close_lot(actx, lot)
            return
        # 남은 수량으로 보호주문 재설정 (이미 낸 것은 취소 후 재생성)
        self._reset_protections(ctx, lot, depth + 1)

    def _protection_failed(self, ctx: _Ctx, e: BaseException, extra: str = "") -> None:
        note = self._err_note(e)
        log.error("%s: PROTECTION_FAILED %s %s", ctx.tag(), note, extra)
        self._alert(f"[executor:{ctx.mode}] {ctx.action} {ctx.position_id} {ctx.event_id}: PROTECTION_FAILED "
                    f"({note}) — position may be unprotected; reconcile will retry{(' — ' + extra) if extra else ''}")

    @staticmethod
    def _err_note(e: BaseException) -> str:
        if isinstance(e, ExchangeRejected):
            return f"EXCHANGE_REJECTED ret_code={e.ret_code}"
        if isinstance(e, ExchangeError):
            return e.code
        return str(e)[:120]

    @staticmethod
    def _protection_missing(lot: dict) -> bool:
        """설정된 보호가격에 비해 기록된 보호주문 항목이 빠져 있는가 (min_qty 로 건너뛴 것은 제외)."""
        po = lot.get("protection_orders") or {}
        if po.get("failed"):
            return True
        skipped = set(po.get("skipped") or [])
        sl = lot.get("stop_loss")
        if sl is not None and float(sl) > 0 and not po.get("sl") and "sl" not in skipped:
            return True
        done = {int(x) for x in po.get("tp_done") or []}
        have = {int(t.get("i", -1)) for t in po.get("tp") or []}
        for i, p in enumerate(lot.get("take_profit") or []):
            if i in done or float(p) <= 0 or f"tp{i}" in skipped:
                continue
            if i not in have:
                return True
        return False

    def protection_missing(self, mode: str) -> list[str]:
        """보호가격이 설정돼 있는데 보호주문이 빠진 open lot 의 position_id 목록 (/healthz, /state 노출용)."""
        return [lot["position_id"] for lot in self.store.open_lots(mode)
                if float(lot["qty"]) > 0 and self._protection_missing(lot)]

    # ------------------------------------------------------------------ reconcile / snapshot
    def reconcile(self, mode: str) -> bool:
        """① 보호주문 체결 수집(auto:sl/auto:tp 회신) + 빠진/죽은 보호주문 재생성 ② 미확정 시장가 주문 재확인
        ③ 고아 조건부 주문 정리 ④ 닫힌 lot 의 잔여 보호주문 취소 재시도 ⑤ 거래소 포지션 vs open lots 합계 대조.
        ①~④ 의 실패는 알림만 하고 ⑤ 를 막지 않는다. ⑤ 가 일치하면 True(스냅샷 가능), 불일치/오류면 False."""
        with self._lock:
            ex = self.exchanges.get(mode)
            if ex is None:
                log.debug("reconcile %s: no exchange for mode", mode)
                return False
            for name, fn in (("protections", self._reconcile_protections),
                             ("uncertain orders", self._verify_uncertain_orders),
                             ("orphan conditionals", self._sweep_orphan_conditionals),
                             ("leftover cancels", self._retry_pending_cancels)):
                try:
                    fn(mode, ex)
                except ExchangeError as e:
                    log.warning("reconcile %s: %s step exchange error %s", mode, name, e.code)
                    self._alert(f"[executor:{mode}] reconcile ({name}) exchange error {e.code}")
                except Exception as e:  # noqa: BLE001
                    log.exception("reconcile %s: %s step unexpected %s", mode, name, type(e).__name__)
                    self._alert(f"[executor:{mode}] reconcile ({name}) failed: {type(e).__name__}")
            try:
                ok = self._reconcile_positions(mode, ex)
            except ExchangeError as e:
                log.warning("reconcile %s: positions exchange error %s", mode, e.code)
                self._alert(f"[executor:{mode}] reconcile (positions) exchange error {e.code}")
                return False
            except Exception as e:  # noqa: BLE001
                log.exception("reconcile %s: unexpected %s", mode, type(e).__name__)
                self._alert(f"[executor:{mode}] reconcile failed: {type(e).__name__}")
                return False
            self._maybe_prune_ingress_log()
            return ok

    def _reconcile_protections(self, mode: str, ex: ExchangeBase) -> None:
        for lot in self.store.open_lots(mode):
            pid = lot["position_id"]
            changed = False
            gone: list[str] = []
            for kind, i, e in self._protection_entries(lot):
                link = e.get("order_link_id")
                if not link:
                    continue
                o = ex.get_order(link)
                if o is None:
                    # 두 번 연속 안 보일 때만 '없음' 으로 확정 (조회 지연/필터 차이로 인한 취소·재생성 반복 방지)
                    key = f"absent:{link}"
                    strikes = int(self.store.get_meta(key, "0") or 0) + 1
                    self.store.set_meta(key, str(strikes))
                    log.warning("reconcile %s: protection %s[%d] of %s not found on exchange (%d)", mode, kind, i, pid, strikes)
                    if strikes < 2:
                        continue
                    self._drop_entry(lot, kind, link)
                    self.store.update_order(link, status="absent")
                    gone.append(f"{kind}[{i}] absent")
                    continue
                if e.get("order_id") and o.get("order_id") and str(o["order_id"]) != str(e["order_id"]):
                    log.warning("reconcile %s: protection %s[%d] of %s order_id mismatch, skipping", mode, kind, i, pid)
                    continue
                status = str(o.get("status") or "")
                if status in _PROTECTION_FILLING:
                    self._handle_protection_fill(mode, ex, lot, kind, i, e, o)
                    changed = True
                    lot = self.store.get_lot(mode, pid) or lot
                    if lot["status"] != st.LOT_OPEN:
                        break
                elif status in _PROTECTION_OPEN or status == "Triggered":
                    continue
                else:
                    # Cancelled/Deactivated/Rejected: 거래소 쪽 reduce-only 조정, 수동 취소 등 → 항목 제거 후 재생성
                    log.warning("reconcile %s: protection %s[%d] of %s is %s", mode, kind, i, pid, status)
                    self._drop_entry(lot, kind, link)
                    self.store.update_order(link, status=status)
                    gone.append(f"{kind}[{i}] {status}")
            if gone:
                lot["updated_at_ms"] = now_ms()
                self.store.upsert_lot(lot)
            lot = self.store.get_lot(mode, pid) or lot
            if lot["status"] != st.LOT_OPEN or float(lot["qty"]) <= 0:
                continue
            missing = self._protection_missing(lot)
            if not (changed or gone or missing):
                continue
            if gone or (missing and not changed):
                self._alert(f"[executor:{mode}] {pid}: protection orders missing on exchange "
                            f"({', '.join(gone) if gone else 'not placed'}); re-placing")
            ctx = self._ctx_from_lot(mode, lot, lot.get("last_event_id") or "", "partial_exit", ex)
            try:
                self._reset_protections(ctx, lot)
            except (ExchangeError, _ProtectionError) as e:
                self._protection_failed(ctx, e, "re-placement during reconcile failed")

    def _handle_protection_fill(self, mode: str, ex: ExchangeBase, lot: dict, kind: str, i: int, entry: dict,
                                o: dict) -> None:
        pid = lot["position_id"]
        rev = int(lot.get("protection_revision") or 0)
        event_id = self._auto_event_id(kind, pid, rev, i, lot)
        order_id = str(o.get("order_id") or entry.get("order_id") or "")
        order_qty = float(o.get("qty") or entry.get("qty") or 0.0)
        step = float(ex.instrument()["qty_step"])
        execs = ex.executions(order_id) if order_id else []
        known = {f["exec_id"] for f in self.store.fills_for_order(entry["order_link_id"])}
        new_total = sum(float(x.get("qty") or 0.0) for x in execs if str(x.get("exec_id") or "") not in known)
        remaining = float(lot["qty"]) - new_total
        action = self._auto_action(mode, event_id, "full_exit" if remaining < step / 2 else "partial_exit")
        reason = "STOP_LOSS_TRIGGERED" if kind == "sl" else "TAKE_PROFIT_TRIGGERED"
        ctx = self._ctx_from_lot(mode, lot, event_id, action, ex)
        log.info("reconcile %s: %s[%d] of %s %s (new fill qty=%s)", mode, kind, i, pid, o.get("status"), new_total)
        fills = self._ingest_fills(ctx, entry["order_link_id"], order_id, execs, order_qty, step,
                                   self._apply_close_fill, reason_code=reason)
        status = str(o.get("status") or "")
        self.store.update_order(entry["order_link_id"], status=status)

        lot = self.store.get_lot(mode, pid) or lot
        if status in _TERMINAL:
            # 더 이상 살아있는 보호주문이 아니다 (Filled / PartiallyFilledCanceled 등) → 항목 제거
            po = dict(lot.get("protection_orders") or _empty_protection())
            if kind == "tp" and status == "Filled":
                done = [int(x) for x in po.get("tp_done") or []]
                if i not in done:
                    done.append(i)
                po["tp_done"] = done
            lot["protection_orders"] = po
            self._drop_entry(lot, kind, entry["order_link_id"])
            self.store.upsert_lot(lot)
        if fills:
            self._alert(f"[executor:{mode}] {reason} {pid}: qty={fmt_step(sum(f['qty'] for f in fills), step)} "
                        f"remaining={fmt_step(float(lot['qty']), step)}")
        if float(lot["qty"]) <= 0 and lot["status"] == st.LOT_OPEN:
            self._close_lot(ctx, lot)

    @staticmethod
    def _auto_event_id(kind: str, position_id: str, revision: int, i: int, lot: dict | None = None) -> str:
        """auto:sl:<pid>:<rev>:L<opened_at_ms> / auto:tp:<pid>:<rev>:<i>:L<opened_at_ms>.
        L<opened_at_ms> 는 lot 인스턴스 — 같은 position_id 로 재진입해도 이전 lot 의 이벤트 ID 와 겹치지 않는다."""
        inst = f":L{int(lot['opened_at_ms'])}" if lot and lot.get("opened_at_ms") else ""
        base = f"{position_id}:{revision}" if kind == "sl" else f"{position_id}:{revision}:{i}"
        eid = f"auto:{kind}:{base}{inst}"
        if len(eid) > 128:   # ID 패턴 길이 상한 (position_id 가 아주 길 때만)
            short = sha256_hex(position_id)[:24]
            base = f"{short}:{revision}" if kind == "sl" else f"{short}:{revision}:{i}"
            eid = f"auto:{kind}:{base}{inst}"
        return eid

    def _auto_action(self, mode: str, event_id: str, proposed: str) -> str:
        """auto 이벤트의 action 은 첫 보고에서 정해져 바뀌지 않는다 (같은 event_id 의 후속 보고 정체성 유지)."""
        key = f"auto_action:{mode}:{event_id}"
        stored = self.store.get_meta(key)
        if stored in ("partial_exit", "full_exit"):
            return stored
        self.store.set_meta(key, proposed)
        return proposed

    def _verify_uncertain_orders(self, mode: str, ex: ExchangeBase) -> None:
        """타임아웃/불명으로 끝난 시장가 주문을 orderLinkId 로 재조회해 종결되면 체결을 수집·반영한다.
        거래소에 끝내 없으면(_UNCERTAIN_MAX_CHECKS 회) absent 로 닫는다."""
        for row in self.store.orders_with_status(mode, _UNCERTAIN_MARKET, _MARKET_PURPOSES):
            link = row["order_link_id"]
            key = f"verify:{link}"
            checks = int(self.store.get_meta(key, "0") or 0) + 1
            try:
                o = ex.get_order(link)
            except ExchangeError as e:
                log.warning("verify %s: get_order failed (%s)", link, e.code)
                continue
            if o is None:
                if checks >= _UNCERTAIN_MAX_CHECKS:
                    self.store.update_order(link, status="absent")
                    self._alert(f"[executor:{mode}] order {row.get('event_id')} never appeared on exchange "
                                f"after {checks} checks; closed as absent")
                else:
                    self.store.set_meta(key, str(checks))
                continue
            status = str(o.get("status") or "")
            if status not in _TERMINAL:
                self.store.set_meta(key, str(checks))
                self.store.update_order(link, order_id=o.get("order_id"), status=status or "submitted")
                continue
            ctx = self._ctx_for_order_row(mode, row, ex)
            if ctx is None:
                self.store.update_order(link, status=status)
                self._alert(f"[executor:{mode}] uncertain order {link[:12]} terminal ({status}) but no context; "
                            f"manual check")
                continue
            self.store.update_order(link, order_id=o.get("order_id"), status="submitted")
            reduce_only = bool(int(row.get("reduce_only") or 0))
            apply = self._apply_close_fill if reduce_only else self._apply_open_fill
            finalize = self._finalize_close if reduce_only else self._finalize_open
            reason = None
            if str(ctx.event_id).startswith("auto:"):
                reason = "STOP_LOSS_TRIGGERED" if ":sl:" in ctx.event_id else "TAKE_PROFIT_TRIGGERED"
            log.warning("verify %s: order %s now %s; settling late", ctx.tag(), link[:12], status)

            def _resume(ctx=ctx, link=link, o=o, apply=apply, finalize=finalize, reason=reason) -> None:
                res = self._settle_order(ctx, link, o.get("order_id"), float(o.get("qty") or row["qty"]), apply,
                                         reason_code=reason)
                self._finish_verified(ctx, res, finalize)

            self._guarded(ctx, _resume)

    def _ctx_for_order_row(self, mode: str, row: dict, ex: ExchangeBase) -> _Ctx | None:
        event_id = row.get("event_id") or ""
        sig_row = self.store.get_signal(event_id, mode) if event_id and not event_id.startswith("auto:") else None
        if sig_row is not None:
            try:
                sig = Signal.model_validate_json(sig_row["raw_body"])
            except Exception:  # noqa: BLE001
                return None
            ctx = self._ctx_from_signal(sig)
            ctx.exchange = ex
            return ctx
        lot = self.store.get_lot(mode, row["position_id"])
        if lot is None:
            return None
        action = self.store.get_meta(f"auto_action:{mode}:{event_id}") or "partial_exit"
        return self._ctx_from_lot(mode, lot, event_id or f"auto:verify:{row['position_id']}", action, ex)

    def _sweep_orphan_conditionals(self, mode: str, ex: ExchangeBase) -> None:
        """거래소에 열린 조건부 주문 중 어떤 lot 도 참조하지 않는 우리 보호주문(orders 에 sl/tp 로 기록) 은 취소한다.
        우리 기록에 없는 주문은 손대지 않고 1회 알림만."""
        referenced: set[str] = set()
        for lot in self.store.open_lots(mode) + self.store.lots_with_pending_cancel(mode):
            for _, _, e in self._protection_entries(lot):
                if e.get("order_link_id"):
                    referenced.add(e["order_link_id"])
        for idx in sorted(self.settings.allowed_position_idx()):
            for o in ex.open_conditional_orders(idx):
                link = str(o.get("order_link_id") or "")
                if not link or link in referenced:
                    continue
                row = self.store.get_order(link)
                if row is not None and row.get("purpose") in ("sl", "tp") and row.get("mode") == mode:
                    log.warning("reconcile %s: orphan protection %s on idx%d (not referenced by any lot); cancelling",
                                mode, link[:12], idx)
                    try:
                        if ex.cancel_order(link):
                            self.store.update_order(link, status="Cancelled")
                        self._alert(f"[executor:{mode}] cancelled orphan protection order {link[:12]} (idx{idx}, "
                                    f"trigger={o.get('trigger_price')} qty={o.get('qty')})")
                    except ExchangeError as e:
                        self._alert(f"[executor:{mode}] orphan protection order {link[:12]} cancel failed {e.code}")
                elif link not in self._orphan_alerted:
                    self._orphan_alerted.add(link)
                    self._alert(f"[executor:{mode}] unknown conditional order on exchange idx{idx} "
                                f"(trigger={o.get('trigger_price')} qty={o.get('qty')} side={o.get('side')}); not ours — check")

    def _retry_pending_cancels(self, mode: str, ex: ExchangeBase) -> None:
        """닫힌 lot 에 남은 보호주문 항목(취소 실패분) 을 다시 취소한다. 남아 있는 동안 알림."""
        for lot in self.store.lots_with_pending_cancel(mode):
            ctx = self._ctx_from_lot(mode, lot, lot.get("last_event_id") or "", "full_exit", ex)
            try:
                self._cancel_protections(ctx, lot)
                log.info("reconcile %s: leftover protections of closed lot %s cancelled", mode, lot["position_id"])
            except (ExchangeError, _ProtectionError) as e:
                self._alert(f"[executor:{mode}] closed lot {lot['position_id']} still has live protection orders "
                            f"({self._err_note(e)}); retrying")

    def _maybe_prune_ingress_log(self) -> None:
        now = now_ms()
        if now - self._last_ingress_prune_ms < _INGRESS_PRUNE_EVERY_MS:
            return
        self._last_ingress_prune_ms = now
        try:
            n = self.store.prune_ingress_log(_INGRESS_LOG_MAX_AGE_MS, _INGRESS_LOG_MAX_ROWS)
            if n:
                log.info("ingress_log pruned: %d row(s)", n)
        except Exception as e:  # noqa: BLE001
            log.warning("ingress_log prune failed: %s", type(e).__name__)

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
        """reconcile(mode) 가 일치하면 전체 포지션 스냅샷 회신을 쌓고 그 결과를 돌려준다. 아니면 None.
        observed_at_ms 는 락을 잡기 전에 찍는다 (락 대기 시간이 '관측 시각' 을 늦추지 않도록)."""
        observed = now_ms()
        with self._lock:
            if not self.reconcile(mode):
                return None
            positions = self.build_snapshot_positions(mode)
            return self.reporter.snapshot(mode, positions, observed_at_ms=observed)

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
        self._set_result(ctx, st.SIGNAL_ERROR, "UNKNOWN_STATE", note)
        self._alert(f"[executor:{ctx.mode}] {ctx.action} {ctx.position_id} {ctx.event_id}: UNKNOWN_STATE ({note})")

    def _report(self, ctx: _Ctx, status: str, *, qty: float | None = None, fill_price: float | None = None,
                order_id: str | None = None, fill_id: str | None = None, reason_code: str | None = None,
                observed_at_ms: int | None = None, mark_fill_reported: str | None = None) -> None:
        """reporter.execution 래퍼. 회신 적재 실패가 매매 흐름을 끊지 않도록 예외는 로그+알림으로 흡수."""
        try:
            self.reporter.execution(
                ctx.mode, event_id=ctx.event_id, position_id=ctx.position_id, strategy=ctx.strategy, leg=ctx.leg,
                position_idx=ctx.position_idx, action=ctx.action, status=status, qty=qty, fill_price=fill_price,
                order_id=order_id, fill_id=fill_id, reason_code=reason_code,
                observed_at_ms=observed_at_ms if observed_at_ms is not None else now_ms(),
                mark_fill_reported=mark_fill_reported)
        except Exception as e:  # noqa: BLE001
            log.exception("report %s for %s failed: %s", status, ctx.tag(), type(e).__name__)
            self._alert(f"[executor:{ctx.mode}] report {status} for {ctx.event_id} failed: {type(e).__name__}")

    def _alert(self, text: str) -> None:
        try:
            self.alerts.send(text)
        except Exception:  # noqa: BLE001
            log.error("alert failed: %s", text)
