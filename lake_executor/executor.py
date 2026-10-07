"""실행기 (ARCHITECTURE.md §1 게이트, §3 실행기; ARCHITECTURE_MULTI_EXCHANGE.md §5 다계정).

접수된 신호를 FIFO 로 하나씩 꺼내(단일 워커) **대상 계정마다** 거래소 주문/보호주문을 내고,
계정별 lot 원장·체결을 갱신하며 계정별 회신(reporter)을 쌓는다. 주기 스냅샷 전에는 `reconcile(mode, account)` 로
보호주문 체결(auto:sl/auto:tp)과 거래소 포지션 합계를 대조하고, 빠진 보호주문·불명 주문을 자가 복구한다.

2단계 (다계정)
  - `Executor(settings, store, exchanges, reporter, alerts)` — exchanges 는 `{mode: {account_name: ExchangeBase}}`
    (한 프로세스가 같은 계정의 test(Paper) 와 live(실거래소) 를 함께 섬기므로 모드별 → 계정별).
    1단계 모양 `{"test": ExchangeBase|None, "live": ExchangeBase|None}` 도 받는다 (→ accounts[0] 의 거래소).
  - `process(row)`: 대상 계정 = `settings.route_accounts(sig.exchange)` (routing fanout: enabled 전부 / by_exchange:
    신호 exchange 와 일치하는 enabled 계정). live 는 계정마다 `live_execution_possible(account)` 게이트.
    계정마다 1단계 로직을 그대로 돌리고 `store.set_run_result(mode, event_id, account, ...)` 에 기록한 뒤
    모든 계정이 끝나면 signals.status 를 종합한다(하나라도 error → error, 전부 rejected → rejected, 그 외 done).
    **한 계정의 예외가 다른 계정 실행을 막지 않는다.** 대상 계정이 없으면 rejected/NO_TARGET_ACCOUNT (회신 없음).
  - lot/order/fill/report/inconsistent 는 전부 (mode, account) 범위. 실행기 전용 meta 키도 계정 범위:
    `account_setup:live:{account}`, `auto_action:{mode}:{account}:{event_id}`, `absent:{account}:{link}`, `verify:{account}:{link}`.
  - `position_idx` 매핑: 계정이 one_way 면 신호 1/2 → 0 (방향은 leg), hedge 는 그대로(0 이면 POSITION_MODE_MISMATCH).
    회신·lot 의 position_idx 는 **매핑된 값**(그 계정에서 실제로 쓴 값).
  - `qty_multiplier`: 신호 qty_btc(entry/add/partial_exit) 와 expected_qty_btc_after 에 계정 배수를 곱한다.
    full_exit 는 lot 잔량이므로 배수 없음.
  - 보호주문: `exchange.supports_lot_protection` 이면 1단계 방식(lot 단위 조건부 reduceOnly 시장가),
    아니면 `set_position_protection(position_idx, sl, tp[0])` 로 **포지션(레그) 단위** 설정.
    포지션 단위는 레그 전체에 하나뿐이므로 같은 레그에 lot 이 여럿이면 **마지막 갱신이 레그 전체에 적용**된다
    (경고 로그). TP 는 첫 미완료 레벨만 쓰고 나머지는 skipped 에 기록. 취소(해제)·체결 경합 감지는 불가능하므로
    포지션 단위 보호가 발동하면 reconcile ⑤ 가 불일치(RECONCILE_REQUIRED)로 드러내고 운영자가 정리한다.
    **레그 보호는 (mode, account, position_idx) 상태**로 store meta `leg_protection:…` 에 한 곳에만 기록한다
    (set/clear 때마다 갱신). 레그에 걸 값은 **lot 들의 의도를 합친 것**: 설정하는 lot 의 SL/TP 가 우선, 그 lot 이 정하지
    않은 부분은 같은 레그의 최근 형제 lot 의 의도로 채운다(`_leg_intent`). 한 lot 을 닫거나 수량을 바꿀 때 같은 레그에
    다른 open lot 이 있으면 레그를 비우지 않고 **형제 lot 의 SL/TP 를 다시 적용**한다(형제가 보호 없이 남지 않게).
    스냅샷의 stop_loss/take_profit 과 protection_missing 은 lot 별 po.position 이 아니라 이 레그 상태
    (읽기 API 가 있으면 거래소 값)를 본다.
    해제 실패로 po.position 이 남은 닫힌 lot 은 lots_with_pending_cancel 에 들어 reconcile ④ 가 해제를 재시도한다.
  - 만료(expires_at_ms) 판정은 **팬아웃 전에 한 번**만 한다 — 앞 계정의 체결 대기가 길어져 뒤 계정만 EXPIRED 가 되는
    (계정 간 lot 이 갈라지는) 일을 막는다.
  - `reconcile(mode, account=None)`(None=모든 계정, AND), `snapshot_now(mode, account)`, `build_snapshot_positions(mode, account)`,
    `recover_processing()` 은 processing 신호의 대상 계정마다 run 결과가 없는 계정만 복구한다.

설계 메모 (1단계 그대로)
  - process / reconcile / snapshot_now / recover_processing 은 하나의 RLock 으로 직렬화한다.
  - 체결은 한 건씩 `fills` 에 기록(applied=0) → lot 반영(applied=1) → 회신(reported=1, 회신 생성과 같은 트랜잭션)
    순으로 처리해 재시작 시 이중 반영·이중 보고를 막는다.
  - **체결 결과와 보호주문 결과는 분리한다.** 시장가가 체결됐으면 그 신호는 체결대로 종결(done) 하고, 보호주문
    생성/취소 실패는 note `PROTECTION_FAILED` + 알림으로만 남긴다. 빠진 보호주문은 reconcile 이 다시 만든다.
  - 보호주문 트리거 가격이 이미 지나 있어 거래소가 거부하면(Bybit 110092/110093, "crossed"), 그 보호를 **지금 reduceOnly
    시장가로 실행**한다(auto:sl/auto:tp 회신). 포지션을 보호 없이 두지 않는다.
  - 보호주문을 취소할 때는 취소 응답과 무관하게 최종 상태를 다시 읽는다. 이미 체결이면 체결로 처리한다.
  - 시장가 주문이 타임아웃/불명으로 끝나면 orders 행을 미확정 상태로 남기고 reconcile 이 종결될 때까지 재확인한다.
  - 보호주문 orderLinkId 는 `util.order_link_id(mode, position_id, "sl"|"tp", revision, i)` (+ 재생성 세대 "g<n>").
    시장가 orderLinkId 는 `util.order_link_id(mode, event_id)` — 계정마다 같은 값이지만 거래소가 다르고 orders PK 가
    (account, link) 라 충돌하지 않는다 (1단계 DB 의 링크 ID 와도 호환).
  - auto 이벤트 ID 는 lot 인스턴스(opened_at_ms)를 포함하고, 한 이벤트의 action 은 첫 보고 때 고정(meta)된다.
  - 원본 본문·시크릿·거래소 오류 원문은 로그/회신에 넣지 않는다 (코드만).
"""
from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from . import store as st
from .exchange import ExchangeBase, ExchangeError, ExchangeRejected
from .ops import halted
from .schemas import Action, Signal
from .util import floor_step, fmt_step, now_ms, order_link_id, qty_eq, sha256_hex

log = logging.getLogger("lake_executor.executor")

MODES = ("test", "live")

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

_RUN_TERMINAL = (st.SIGNAL_DONE, st.SIGNAL_REJECTED, st.SIGNAL_ERROR)
_PROTECTION_KIND_POSITION = "position"   # lot.protection_orders["kind"] — 포지션 단위 보호를 쓰는 lot


# 보호주문 기본 구조 (lot.protection_orders)
def _empty_protection() -> dict:
    return {"sl": None, "tp": [], "gen": 0, "tp_done": [], "skipped": [], "failed": False}


@dataclass
class _Ctx:
    """한 신호(또는 auto 이벤트) 의 **한 계정** 처리 문맥. 회신에 들어가는 정체성 필드 + 거래소 핸들."""
    mode: str
    event_id: str
    position_id: str
    strategy: str
    leg: str
    position_idx: int                   # 이 계정에서 쓰는 값 (one_way 계정은 0 으로 매핑됨)
    action: str
    account: str = st.DEFAULT_ACCOUNT
    acct: Any = None                    # config.AccountSettings | None
    exchange: ExchangeBase | None = None
    sig: Signal | None = None
    expired: bool = False               # process() 가 팬아웃 전에 한 번 판정한 만료 여부 (계정 간 일관)
    stale: bool = False                 # 만료됐지만 정책(guards.expired_actions_execute) 에 따라 실행하는 중 (run note 에 남긴다)

    @property
    def is_long(self) -> bool:
        return self.leg == "long"

    @property
    def qty_multiplier(self) -> float:
        try:
            m = float(getattr(self.acct, "qty_multiplier", 1.0) or 1.0)
        except (TypeError, ValueError):
            m = 1.0
        return m if m > 0 else 1.0

    def open_side(self) -> str:
        return "Buy" if self.is_long else "Sell"

    def close_side(self) -> str:
        return "Sell" if self.is_long else "Buy"

    def tag(self) -> str:
        return f"{self.mode}/{self.account}/{self.action} pos={self.position_id} ev={self.event_id}"


@dataclass
class _OrderResult:
    """시장가 주문 하나의 정산 결과."""
    state: str                      # filled | partial | cancelled | rejected | timeout
    order_id: str | None
    filled_qty: float = 0.0         # 이번 처리에서 새로 lot 에 반영한 체결 수량
    fills: list[dict] = field(default_factory=list)


@dataclass
class _LegIntent:
    """포지션 단위 레그에 걸 SL/TP (lot 들의 의도를 합친 결과) 와 각 부분의 출처 lot."""
    sl: float | None = None
    tp: float | None = None
    tp_i: int | None = None
    sl_from: str | None = None
    tp_from: str | None = None
    sl_lot: dict | None = None
    tp_lot: dict | None = None


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


def _is_position_protection(lot: dict) -> bool:
    return (lot.get("protection_orders") or {}).get("kind") == _PROTECTION_KIND_POSITION


class Executor:
    """`Executor(settings, store, exchanges, reporter, alerts)` — §3 / 다계정 §5."""

    def __init__(self, settings, store: st.Store, exchanges: dict | None, reporter, alerts):
        self.settings = settings
        self.store = store
        self.exchanges: dict[str, dict[str, ExchangeBase]] = self._normalize_exchanges(exchanges)
        self.reporter = reporter
        self.alerts = alerts
        self.wake = threading.Event()          # 수신기가 접수 직후 set → 0.2초 폴링을 기다리지 않고 바로 집어간다
        self._lock = threading.RLock()
        # test(paper) 모드: 신호의 reference_price 로 모의 시세를 움직인다 (lake TEST 신호가 SLIPPAGE_GUARD 로 죽지 않도록)
        self.follow_reference_price_on_paper = True
        self._orphan_alerted: set[str] = set()
        self._last_ingress_prune_ms = 0
        self._wire_link_resolvers()

    def _wire_link_resolvers(self) -> None:
        """clOrdId 를 절단하는 거래소(OKX) 에 원장 기반 역매핑을 꽂는다: 재기동 뒤 처음 보는 32자 id → 저장된 34자 link."""
        for mode, accts in self.exchanges.items():
            for name, ex in accts.items():
                if hasattr(ex, "link_resolver") and getattr(ex, "link_resolver", None) is None:
                    ex.link_resolver = (lambda cid, _a=name: self._resolve_link_prefix(_a, cid))

    def _resolve_link_prefix(self, account: str, prefix: str) -> str | None:
        """orders 에서 order_link_id 가 prefix 로 시작하는 유일한 주문의 link (없거나 모호하면 None)."""
        rows = self.store.orders_by_link_prefix(account, prefix, limit=2)
        if len(rows) != 1:
            return None
        return str(rows[0]["order_link_id"])

    # ------------------------------------------------------------------ 계정/거래소
    def _normalize_exchanges(self, exchanges: dict | None) -> dict[str, dict[str, ExchangeBase]]:
        """{mode: {account: ex}} 로 정규화. 1단계 모양 {mode: ex|None} 은 accounts[0] 의 거래소로 해석한다."""
        out: dict[str, dict[str, ExchangeBase]] = {m: {} for m in MODES}
        default_name = self._default_account_name()
        for mode, v in (exchanges or {}).items():
            if mode not in out:
                out[mode] = {}
            if v is None:
                continue
            if isinstance(v, dict):
                out[mode].update({str(k): ex for k, ex in v.items() if ex is not None})
            else:
                out[mode][default_name] = v
        return out

    def _default_account_name(self) -> str:
        accts = getattr(self.settings, "accounts", None) or []
        return accts[0].name if accts else st.DEFAULT_ACCOUNT

    def _account_names(self) -> list[str]:
        accts = getattr(self.settings, "accounts", None) or []
        return [a.name for a in accts] or [st.DEFAULT_ACCOUNT]

    def _acct(self, name: str | None):
        if name is None:
            return None
        fn = getattr(self.settings, "account_or_none", None)
        return fn(name) if callable(fn) else None

    def _exchange(self, mode: str, account: str) -> ExchangeBase | None:
        return (self.exchanges.get(mode) or {}).get(account)

    def _accounts_with_exchange(self, mode: str) -> list[str]:
        """mode 에 거래소가 준비된 계정 이름 (settings 순서, 설정에 없는 이름은 뒤에)."""
        have = self.exchanges.get(mode) or {}
        names = [n for n in self._account_names() if n in have]
        names += [n for n in have if n not in names]
        return names

    def _resolve_account(self, account: str | None, what: str) -> str:
        """account=None 은 계정이 하나뿐일 때만 허용 (1단계 호출 호환)."""
        if account is not None:
            return account
        names = self._account_names()
        if len(names) == 1:
            return names[0]
        raise ValueError(f"{what}: account is required when more than one account is configured")

    # ------------------------------------------------------------------ 루프
    def run_once(self) -> bool:
        """accepted 신호 하나를 집어 처리. 처리한 게 있으면 True."""
        row = self.store.claim_next_signal()
        if row is None:
            return False
        self.process(row)
        return True

    def run_forever(self, stop_event: threading.Event) -> None:
        """0.2s 폴링 루프. process 내부 예외는 process 가 삼키고, claim 자체의 예외도 루프를 죽이지 않는다.
        원장(Postgres) 이 닿지 않으면(LedgerUnavailable) 1→2→4→…→30초 백오프로 재시도하고, 같은 종류의 오류 알림은
        5분에 한 번만 보낸다 (장애 동안 알림 폭주 방지). 복구되면 '회복' 알림 한 번."""
        log.info("executor loop started")
        backoff = 1.0
        last_alert: dict[str, int] = {}
        failing: str | None = None
        while not stop_event.is_set():
            try:
                worked = self.run_once()
            except Exception as e:  # noqa: BLE001 - 루프는 절대 죽지 않는다
                kind = type(e).__name__
                unavailable = isinstance(e, st.LedgerUnavailable)
                if unavailable:
                    log.warning("executor loop: ledger unavailable (%s); retry in %.0fs", e, backoff)
                else:
                    log.exception("executor loop error: %s", kind)
                now = now_ms()
                if now - last_alert.get(kind, 0) >= 300_000:
                    last_alert[kind] = now
                    self._alert(f"[executor] loop error: {kind}" + (" (ledger unavailable, retrying)" if unavailable else ""))
                failing = kind
                stop_event.wait(backoff)
                backoff = min(backoff * 2, 30.0)
                continue
            if failing is not None:
                log.info("executor loop recovered after %s", failing)
                self._alert(f"[executor] loop recovered after {failing}")
                failing = None
            backoff = 1.0
            if not worked:
                self.wake.wait(0.2)
                self.wake.clear()
        log.info("executor loop stopped")

    # ------------------------------------------------------------------ 시작
    def ensure_account_setup(self) -> None:
        """live 거래소가 준비된 계정마다 포지션 모드/레버리지/마진 모드를 맞춘다 (live 에서만, §3 마지막 줄).
        같은 설정을 이미 적용했으면(meta `account_setup:live:{account}`) 거래소 쓰기를 반복하지 않는다 (재시작 루프 보호).
        한 계정의 실패가 다른 계정 설정을 막지 않는다 (예외는 마지막에 모아 올린다)."""
        errors: list[str] = []
        for name in self._accounts_with_exchange("live"):
            acct = self._acct(name)
            ex = self._exchange("live", name)
            if acct is None or ex is None:
                continue
            ok, _ = self.settings.live_execution_possible(acct)
            if not ok:
                continue
            signature = f"{acct.position_mode}|{acct.leverage}|{acct.margin_mode}|{acct.testnet}|{acct.symbol}"
            key = f"account_setup:live:{name}"
            if self.store.get_meta(key) == signature:
                log.info("live account %s setup already applied (mode=%s lev=%s margin=%s)",
                         name, acct.position_mode, acct.leverage, acct.margin_mode)
                continue
            try:
                ex.ensure_account_setup(acct.position_mode, acct.leverage, acct.margin_mode)
            except Exception as e:  # noqa: BLE001
                log.error("live account %s setup failed: %s", name, type(e).__name__)
                self._alert(f"[executor:live/{name}] ensure_account_setup failed: {type(e).__name__}")
                errors.append(f"{name}: {type(e).__name__}")
                continue
            self.store.set_meta(key, signature)
            log.info("live account %s setup ensured (mode=%s lev=%s margin=%s)",
                     name, acct.position_mode, acct.leverage, acct.margin_mode)
        if errors:
            raise RuntimeError("account setup failed for " + "; ".join(errors))

    def start(self) -> None:
        """편의: ensure_account_setup() → recover_processing()."""
        try:
            self.ensure_account_setup()
        except Exception as e:  # noqa: BLE001
            log.error("ensure_account_setup: %s", e)
        self.recover_processing()

    def recover_processing(self) -> None:
        """재시작 복구. status=processing 신호는 재실행하지 않고, 대상 계정마다 (아직 run 결과가 없는 계정만)
        그 신호의 주문이 거래소에 있으면 체결 수집을 이어서 마무리하고, 없으면 error/UNKNOWN_STATE 로 닫고 알린다.
        타임아웃/불명으로 남은 주문(orders.status 미확정)은 reconcile 의 재확인 경로가 이어서 처리한다."""
        with self._lock:
            rows = self.store.processing_signals()
            if not rows:
                return
            log.warning("recover_processing: %d signal(s) left in processing", len(rows))
            for row in rows:
                sig = self._load_signal(row)
                if sig is None:
                    continue
                mode, event_id = sig.mode.value, sig.event_id
                targets = self._target_accounts(sig)
                if not targets:
                    self._no_target(sig, "recovered")
                    continue
                done = {r["account"]: r for r in self.store.get_runs(mode, event_id)}
                for acct in targets:
                    prior = done.get(acct.name)
                    if prior is not None and prior.get("status") in _RUN_TERMINAL:
                        continue
                    ctx = self._ctx_for_account(sig, acct)
                    try:
                        self._recover_one(ctx)
                    except Exception as e:  # noqa: BLE001 - 한 계정의 실패가 다른 계정을 막지 않는다
                        log.exception("recover %s: unexpected %s", ctx.tag(), type(e).__name__)
                        self._set_result(ctx, st.SIGNAL_ERROR, "UNKNOWN_STATE", f"recover failed: {type(e).__name__}")
                self._finalize_signal(mode, event_id)

    def _recover_one(self, ctx: _Ctx) -> None:
        sig = ctx.sig
        assert sig is not None
        mode, event_id, account = ctx.mode, ctx.event_id, ctx.account
        ex = self._exchange(mode, account)
        if mode == "test" and ex is None:
            # 기록 전용 모드: 아무것도 실행되지 않았음
            self._set_result(ctx, st.SIGNAL_DONE, "TEST_RECORD_ONLY", "recovered: record only")
            return
        if mode == "live":
            ok, _ = self.settings.live_execution_possible(ctx.acct if ctx.acct is not None else account)
            if not ok or ex is None:
                self._unknown_state(ctx, "recovered: live exchange unavailable")
                return
        ctx.exchange = ex
        if sig.action not in (Action.entry, Action.add, Action.partial_exit, Action.full_exit):
            # protection_update 는 시장가 주문이 없어 중간 상태를 판정할 수 없음.
            # lot 이 이미 새 revision 으로 갱신돼 있었다면 보호주문이 빠졌을 수 있으니 reconcile 자가 복구 대상으로 표시한다.
            lot = self.store.get_lot(mode, account, ctx.position_id)
            if lot is not None and lot["status"] == st.LOT_OPEN and \
                    int(lot.get("protection_revision") or 0) == int(sig.protection_revision):
                po = dict(lot.get("protection_orders") or _empty_protection())
                po["failed"] = True
                lot["protection_orders"] = po
                self.store.upsert_lot(lot)
            self._unknown_state(ctx, "recovered: no resumable order for this action")
            return

        link = order_link_id(mode, event_id)
        order_row = self.store.get_order(link, account)
        try:
            o = ex.get_order(link)
        except ExchangeError as e:
            log.warning("recover %s: get_order failed (%s)", ctx.tag(), e.code)
            if order_row is not None:
                self.store.update_order(link, account, status="unknown")   # reconcile 재확인 대상
            self._unknown_state(ctx, f"recovered: get_order failed {e.code}")
            return
        if o is None:
            if order_row is not None:
                self.store.update_order(link, account, status="unknown")
            self._unknown_state(ctx, "recovered: order not found on exchange")
            return

        purpose = "exit" if sig.action in (Action.partial_exit, Action.full_exit) else sig.action.value
        reduce_only = purpose == "exit"
        side = ctx.close_side() if reduce_only else ctx.open_side()
        order_qty = float(o.get("qty") or (order_row or {}).get("qty") or 0.0)
        if order_row is None:
            self.store.insert_order(link, mode, ctx.position_id, purpose, side, order_qty, reduce_only,
                                    event_id=event_id, status="submitted", order_id=o.get("order_id"), account=account)
        else:
            self.store.update_order(link, account, order_id=o.get("order_id"), status="submitted")
        log.info("recover %s: resuming fill collection for link=%s", ctx.tag(), link)

        apply = self._apply_close_fill if reduce_only else self._apply_open_fill
        finalize = self._finalize_close if reduce_only else self._finalize_open

        def _resume() -> None:
            res = self._settle_order(ctx, link, o.get("order_id"), order_qty, apply)
            self._finish_market(ctx, res, finalize)

        self._guarded(ctx, _resume)

    # ------------------------------------------------------------------ 신호 처리 (계정 팬아웃)
    def process(self, row: dict) -> None:
        """claim 된 신호 한 건을 대상 계정마다 처리하고 signals.status 를 종합한다. 어떤 예외도 밖으로 내보내지 않는다."""
        with self._lock:
            sig = self._load_signal(row)
            if sig is None:
                return
            mode, event_id = sig.mode.value, sig.event_id
            targets = self._target_accounts(sig)
            log.info("process %s/%s pos=%s ev=%s seq=%s -> accounts=%s", mode, sig.action.value, sig.position_id,
                     event_id, sig.event_sequence, [a.name for a in targets])
            if not targets:
                self._no_target(sig, "")
                return
            # 만료는 팬아웃 전에 한 번만 판정한다 (계정별로 다시 재면 앞 계정의 체결 대기 때문에 뒤 계정만 EXPIRED 가 되어
            # 같은 position_id 의 lot 이 계정 간에 갈라진다)
            expired = now_ms() > int(sig.expires_at_ms)
            for acct in targets:
                try:
                    ctx = self._ctx_for_account(sig, acct)
                    ctx.expired = expired
                    self.store.set_run_result(mode, event_id, acct.name, st.SIGNAL_PROCESSING)
                    self._guarded(ctx, lambda c=ctx: self._process_signal(c))
                except Exception as e:  # noqa: BLE001 - 한 계정의 실패가 다른 계정 실행을 막지 않는다
                    log.exception("process %s/%s account %s: unexpected %s", mode, event_id, acct.name, type(e).__name__)
                    try:
                        self.store.set_run_result(mode, event_id, acct.name, st.SIGNAL_ERROR, "UNKNOWN_STATE",
                                                  type(e).__name__)
                    except Exception:  # noqa: BLE001
                        log.exception("set_run_result failed for %s/%s", event_id, acct.name)
                    self._alert(f"[executor:{mode}/{acct.name}] {sig.action.value} {sig.position_id} {event_id}: "
                                f"UNKNOWN_STATE ({type(e).__name__})")
            try:
                self._finalize_signal(mode, event_id)
            except Exception:  # noqa: BLE001
                log.exception("finalize_signal failed for %s/%s", mode, event_id)

    def _target_accounts(self, sig: Signal) -> list:
        fn = getattr(self.settings, "route_accounts", None)
        if callable(fn):
            return list(fn(sig.exchange))
        return list(getattr(self.settings, "accounts", None) or [])

    def _no_target(self, sig: Signal, prefix: str) -> None:
        """라우팅 결과 실행할 계정이 없다 (by_exchange 불일치 / 전부 disabled). 회신할 스트림이 없으므로 기록+알림만."""
        routing = getattr(self.settings, "routing", "fanout")
        note = f"{prefix + ': ' if prefix else ''}routing={routing} exchange={sig.exchange}"
        log.warning("%s/%s %s: NO_TARGET_ACCOUNT (%s)", sig.mode.value, sig.event_id, sig.action.value, note)
        self.store.set_signal_result(sig.event_id, st.SIGNAL_REJECTED, "NO_TARGET_ACCOUNT", note, mode=sig.mode.value)
        self._alert(f"[executor:{sig.mode.value}] {sig.action.value} {sig.position_id} {sig.event_id}: "
                    f"NO_TARGET_ACCOUNT ({note})")

    def _finalize_signal(self, mode: str, event_id: str) -> None:
        """signal_runs 를 종합해 signals.status 를 확정한다. 계정이 하나면 그 run 의 note 를 그대로 쓴다(1단계 호환:
        LATE_VERIFIED / QTY_MISMATCH / IOC_PARTIAL / PROTECTION_FAILED 가 신호 note 에 남는다)."""
        runs = self.store.get_runs(mode, event_id)
        if not runs:
            return
        status, reason, _ = self.store.summarize_runs(runs)
        if len(runs) == 1:
            # 계정 하나: run 결과를 그대로 (done 이어도 reason_code 유지 — TEST_RECORD_ONLY 등 1단계 호환)
            status = str(runs[0].get("status") or status)
            reason = runs[0].get("reason_code")
            note = str(runs[0].get("note") or "")
        else:
            parts = []
            for r in runs:
                p = f"{r['account']}={r['status']}"
                if r.get("reason_code"):
                    p += f"/{r['reason_code']}"
                if r.get("note"):
                    p += f" {r['note']}"
                parts.append(p)
            note = ";".join(parts)
        self.store.set_signal_result(event_id, status, reason, note, mode=mode)

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
        """예외 → 회신(rejected|error) + 계정 run 결과 + 알림. 루프는 절대 죽지 않는다.
        (체결 뒤의 보호주문 실패는 _finish_market 이 흡수하므로 여기 오는 예외는 체결 전 실패다.)"""
        try:
            fn()
        except ExchangeRejected as e:
            log.warning("%s: exchange rejected (ret_code=%s)", ctx.tag(), e.ret_code)
            self._report(ctx, "rejected", reason_code="EXCHANGE_REJECTED")
            self._set_result(ctx, st.SIGNAL_REJECTED, "EXCHANGE_REJECTED", f"ret_code={e.ret_code}")
            self._alert(f"[executor:{ctx.mode}/{ctx.account}] {ctx.action} {ctx.position_id} {ctx.event_id}: "
                        f"EXCHANGE_REJECTED ret_code={e.ret_code}")
            self._snapshot_quiet(ctx.mode, ctx.account)
        except ExchangeError as e:
            code = e.code if e.code in ("EXCHANGE_ERROR", "EXCHANGE_TIMEOUT") else "EXCHANGE_ERROR"
            log.warning("%s: exchange error %s", ctx.tag(), code)
            self._report(ctx, "error", reason_code=code)
            self._set_result(ctx, st.SIGNAL_ERROR, code, "")
            self._alert(f"[executor:{ctx.mode}/{ctx.account}] {ctx.action} {ctx.position_id} {ctx.event_id}: {code}")
            self._snapshot_quiet(ctx.mode, ctx.account)
        except _ProtectionError as e:
            log.error("%s: protection handling failed: %s", ctx.tag(), e)
            self._report(ctx, "error", reason_code="EXCHANGE_ERROR")
            self._set_result(ctx, st.SIGNAL_ERROR, "EXCHANGE_ERROR", str(e)[:200])
            self._alert(f"[executor:{ctx.mode}/{ctx.account}] {ctx.action} {ctx.position_id} {ctx.event_id}: "
                        f"protection orders need attention ({e})")
            self._snapshot_quiet(ctx.mode, ctx.account)
        except Exception as e:  # noqa: BLE001
            log.exception("%s: unexpected %s", ctx.tag(), type(e).__name__)
            self._report(ctx, "error", reason_code="UNKNOWN_STATE")
            try:
                self._set_result(ctx, st.SIGNAL_ERROR, "UNKNOWN_STATE", type(e).__name__)
            except Exception:  # noqa: BLE001
                log.exception("set_run_result failed for %s", ctx.tag())
            self._alert(f"[executor:{ctx.mode}/{ctx.account}] {ctx.action} {ctx.position_id} {ctx.event_id}: "
                        f"UNKNOWN_STATE ({type(e).__name__})")

    def _process_signal(self, ctx: _Ctx) -> None:
        sig = ctx.sig
        assert sig is not None
        mode, account = ctx.mode, ctx.account
        self._report(ctx, "acknowledged")

        # ---- 만료 (접수 뒤 지연: 재시작/백로그/HALT 해제 뒤 오래된 신호를 현재가로 실행하지 않는다)
        # 판정은 process() 가 팬아웃 전에 한 번 했다 (ctx.expired) — 모든 대상 계정에 같은 결과.
        # 예외: guards.expired_actions_execute 에 든 action(기본 partial_exit/full_exit/protection_update) 은 늦게라도 실행한다
        # (서버 끊김·재시작 뒤 재연결 복구: 청산·보호가격 변경을 버리는 쪽이 더 위험). run note 에 stale 로 남는다.
        if ctx.expired:
            if sig.action.value in self._expired_actions_execute():
                ctx.stale = True
                log.warning("%s: expired (expires_at_ms=%s) but executing per expired_actions_execute policy",
                            ctx.tag(), sig.expires_at_ms)
            else:
                self._reject(ctx, "EXPIRED", f"expired_at_ms={sig.expires_at_ms}")
                return

        # ---- 모드 게이트 (§1) — 계정별
        if mode == "test":
            ex = self._exchange("test", account)
            if ex is None:
                # 기록 전용: acknowledged 하나만 보내고 끝
                self._set_result(ctx, st.SIGNAL_DONE, "TEST_RECORD_ONLY", "record only")
                log.info("%s: test record only", ctx.tag())
                return
            self._follow_reference_price(ex, sig)
        else:
            ok, reason = self.settings.live_execution_possible(ctx.acct if ctx.acct is not None else account)
            ex = self._exchange("live", account)
            if not ok or ex is None:
                self._reject(ctx, reason or "LIVE_DISABLED")
                return
        ctx.exchange = ex

        # ---- 공통 게이트
        if self._halted():
            self._reject(ctx, "OPERATOR_HALT")
            return
        if ctx.position_idx not in self._allowed_idx(ctx):
            self._reject(ctx, "POSITION_MODE_MISMATCH",
                         f"signal idx={sig.position_idx} account position_mode={getattr(ctx.acct, 'position_mode', '?')}")
            return
        if sig.action in (Action.entry, Action.add) and self.store.is_inconsistent(mode, account):
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

    def _allowed_idx(self, ctx: _Ctx) -> set[int]:
        acct = ctx.acct
        if acct is not None:
            try:
                return set(acct.allowed_position_idx())
            except Exception:  # noqa: BLE001
                pass
        return set(self.settings.allowed_position_idx())

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
                log.info("paper price -> %s (reference_price); %d protection(s) triggered", ref, len(fired))
        except Exception as e:  # noqa: BLE001
            log.warning("paper set_price failed: %s", type(e).__name__)

    # ------------------------------------------------------------------ 액션
    def _do_entry(self, ctx: _Ctx) -> None:
        sig = ctx.sig
        assert sig is not None and ctx.exchange is not None
        lot = self.store.get_lot(ctx.mode, ctx.account, ctx.position_id)
        if lot is not None and lot["status"] == st.LOT_OPEN:
            self._reject(ctx, "POSITION_EXISTS")
            return
        if self._opposing_leg_open(ctx):
            self._reject(ctx, "OPPOSING_LEG")
            return
        qty, code = self._check_open_qty(ctx, float(sig.qty_btc or 0.0) * ctx.qty_multiplier)
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
        lot = self.store.get_lot(ctx.mode, ctx.account, ctx.position_id)
        if lot is None or lot["status"] != st.LOT_OPEN:
            self._reject(ctx, "POSITION_NOT_FOUND")
            return
        if self._opposing_leg_open(ctx):
            self._reject(ctx, "OPPOSING_LEG")
            return
        qty, code = self._check_open_qty(ctx, float(sig.qty_btc or 0.0) * ctx.qty_multiplier)
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
        lot = self.store.get_lot(ctx.mode, ctx.account, ctx.position_id)
        if lot is None or lot["status"] != st.LOT_OPEN:
            self._reject(ctx, "POSITION_NOT_FOUND")
            return
        inst = ctx.exchange.instrument()
        step, min_qty = float(inst["qty_step"]), float(inst["min_qty"])
        qty = floor_step(float(sig.qty_btc or 0.0) * ctx.qty_multiplier, step)
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
        lot = self.store.get_lot(ctx.mode, ctx.account, ctx.position_id)
        if lot is None or lot["status"] != st.LOT_OPEN:
            self._reject(ctx, "POSITION_NOT_FOUND")
            return
        inst = ctx.exchange.instrument()
        step, min_qty = float(inst["qty_step"]), float(inst["min_qty"])
        qty = floor_step(float(lot["qty"]), step)   # lot 잔량만 (심볼 전량 아님)
        if qty <= 0 or qty < min_qty:
            self._reject(ctx, "QTY_BELOW_MIN", f"lot remainder {fmt_step(qty, step)} below min")
            self._alert(f"[executor:{ctx.mode}/{ctx.account}] full_exit {ctx.position_id}: lot remainder below min qty, "
                        f"manual cleanup needed")
            return
        res = self._execute_market(ctx, ctx.close_side(), qty, reduce_only=True, purpose="exit",
                                   apply=self._apply_close_fill)
        self._finish_market(ctx, res, self._finalize_close)

    def _do_protection_update(self, ctx: _Ctx) -> None:
        sig = ctx.sig
        assert sig is not None and ctx.exchange is not None
        lot = self.store.get_lot(ctx.mode, ctx.account, ctx.position_id)
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
            self._snapshot_quiet(ctx.mode, ctx.account)
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
            self._snapshot_quiet(ctx.mode, ctx.account)
            return
        self._report(ctx, "protection_updated")
        self._set_result(ctx, st.SIGNAL_DONE, None, "")
        log.info("%s: protection updated rev=%s sl=%s tp=%s", ctx.tag(), rev, sig.stop_loss, sig.tp_list)
        self._snapshot_quiet(ctx.mode, ctx.account)

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
        """계정별 run 결과. signals.status 종합은 process()/recover_processing() 끝 또는 _finalize_signal 호출자가 한다.
        만료 뒤 정책으로 실행한 신호(ctx.stale) 는 note 앞에 'stale' 표시를 남긴다 (대시보드/원장에서 구분)."""
        if getattr(ctx, "stale", False):
            note = f"stale(executed after expires_at_ms) {note}".strip()
        self.store.set_run_result(ctx.mode, ctx.event_id, ctx.account, status, reason_code, note)

    def _expired_actions_execute(self) -> set[str]:
        try:
            return {str(a) for a in (getattr(self.settings, "expired_actions_execute", None) or ())}
        except TypeError:
            return set()

    def _opposing_leg_open(self, ctx: _Ctx) -> bool:
        """단방향(idx 0) 에서 반대 방향 lot 이 열려 있으면 True (거래소가 네팅해 버리므로 entry/add 거부)."""
        if int(ctx.position_idx) != 0:
            return False
        return any(l["leg"] != ctx.leg and float(l["qty"]) > 0
                   for l in self.store.open_lots(ctx.mode, ctx.account) if int(l["position_idx"]) == 0)

    def _check_open_qty(self, ctx: _Ctx, qty_btc: float) -> tuple[float, str | None]:
        """entry/add 수량 검증 (qty_btc 는 이미 계정 배수를 곱한 값). (qty, reason_code|None)."""
        assert ctx.exchange is not None
        inst = ctx.exchange.instrument()
        step, min_qty = float(inst["qty_step"]), float(inst["min_qty"])
        qty = floor_step(qty_btc, step)
        if qty <= 0 or qty < min_qty:
            return qty, "QTY_BELOW_MIN"
        if qty > float(self.settings.max_order_qty_btc) + step / 2:
            return qty, "QTY_LIMIT"
        leg_sum = sum(float(l["qty"]) for l in self.store.open_lots(ctx.mode, ctx.account)
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
        account = ctx.account
        link = link or order_link_id(ctx.mode, ctx.event_id)
        self.store.insert_order(link, ctx.mode, ctx.position_id, purpose, side, qty, reduce_only,
                                event_id=ctx.event_id, status="new", account=account)
        order_id: str | None = None
        try:
            r = ex.place_market(side, qty, ctx.position_idx, reduce_only, link)
            order_id = str(r.get("order_id") or "") or None
        except ExchangeRejected:
            self.store.update_order(link, account, status="Rejected")
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
                self.store.update_order(link, account, status="unknown")
                raise
            order_id = str(o.get("order_id") or "") or None
        self.store.update_order(link, account, order_id=order_id, status="submitted")
        self._report(ctx, "submitted", order_id=order_id)
        log.info("%s: submitted %s %s qty=%s ro=%s order=%s", ctx.tag(), purpose, side, qty, reduce_only, order_id)
        return self._settle_order(ctx, link, order_id, qty, apply, reason_code=reason_code)

    def _settle_order(self, ctx: _Ctx, link: str, order_id: str | None, order_qty: float,
                      apply: Callable[[_Ctx, dict], None], reason_code: str | None = None) -> _OrderResult:
        """종결 상태까지 폴링 → 체결 수집/반영/회신 → 결과."""
        ex = ctx.exchange
        assert ex is not None
        account = ctx.account
        step = float(ex.instrument()["qty_step"])
        state, o = self._await_terminal(ex, link)
        if o is not None and not order_id:
            order_id = str(o.get("order_id") or "") or None
            self.store.update_order(link, account, order_id=order_id)
        if o is not None and o.get("qty"):
            order_qty = float(o["qty"])

        if state == "rejected":
            self.store.update_order(link, account, status="Rejected")
            return _OrderResult("rejected", order_id)

        cum = float((o or {}).get("cum_qty") or 0.0)
        fills: list[dict] = []
        if order_id and (cum > 0 or state == "filled"):
            expect = cum if cum > 0 else order_qty
            execs = self._fetch_executions(ex, order_id, expect, step)
            fills = self._ingest_fills(ctx, link, order_id, execs, order_qty, step, apply, reason_code=reason_code)
        if o is not None:
            self.store.update_order(link, account, status=str(o.get("status") or state))
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
        account = ctx.account
        known = {f["exec_id"]: f for f in self.store.fills_for_order(link, account)}
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
                                                  order_id=order_id, order_link_id=link, event_id=ctx.event_id,
                                                  account=account)
                if not inserted:
                    continue
                applied, reported = False, False
            else:
                applied = bool(int(prior.get("applied") or 0))
                reported = bool(int(prior.get("reported") or 0))
            if not applied:
                apply(ctx, fill)
                self.store.mark_fill_applied(exec_id, account)
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
        lot = self.store.get_lot(ctx.mode, ctx.account, ctx.position_id)
        t = now_ms()
        qty, price = float(fill["qty"]), float(fill["price"])
        if lot is None or lot["status"] != st.LOT_OPEN:
            if lot is not None and int(lot.get("opened_at_ms") or 0) >= t:
                t = int(lot["opened_at_ms"]) + 1   # lot 인스턴스(opened_at_ms) 는 같은 position_id 안에서 단조 증가
            lot = {
                "mode": ctx.mode, "account": ctx.account, "position_id": ctx.position_id, "strategy": ctx.strategy,
                "leg": ctx.leg, "position_idx": int(ctx.position_idx), "qty": qty, "avg_entry": price,
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
        lot = self.store.get_lot(ctx.mode, ctx.account, ctx.position_id)
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
        lot = self.store.get_lot(ctx.mode, ctx.account, ctx.position_id)
        if lot is None or lot["status"] != st.LOT_OPEN or float(lot["qty"]) <= 0:
            return
        self._reset_protections(ctx, lot)

    def _finalize_close(self, ctx: _Ctx) -> None:
        lot = self.store.get_lot(ctx.mode, ctx.account, ctx.position_id)
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
        """시장가 결과 → 보호주문 정리 → 회신(실패 상태) → run 결과 → 스냅샷.
        체결이 있었던 신호의 상태는 체결이 결정한다: 보호주문 실패는 note PROTECTION_FAILED + 알림일 뿐이다."""
        if res.state == "rejected":
            log.warning("%s: order rejected by exchange", ctx.tag())
            self._report(ctx, "rejected", reason_code="EXCHANGE_REJECTED")
            self._set_result(ctx, st.SIGNAL_REJECTED, "EXCHANGE_REJECTED", "")
            self._alert(f"[executor:{ctx.mode}/{ctx.account}] {ctx.action} {ctx.position_id} {ctx.event_id}: EXCHANGE_REJECTED")
            return
        if res.state == "cancelled":
            log.warning("%s: order cancelled without fills", ctx.tag())
            self._report(ctx, "cancelled")
            self._set_result(ctx, st.SIGNAL_REJECTED, None, "cancelled without fills")
            self._snapshot_quiet(ctx.mode, ctx.account)
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
            self._alert(f"[executor:{ctx.mode}/{ctx.account}] {ctx.action} {ctx.position_id} {ctx.event_id}: EXCHANGE_TIMEOUT "
                        f"(order may still exist; reconcile will verify)")
            self._snapshot_quiet(ctx.mode, ctx.account)
            return
        mismatch = self._check_expected_qty(ctx)
        if mismatch:
            notes.append(mismatch)
        self._set_result(ctx, st.SIGNAL_DONE, None, " ".join(notes))
        log.info("%s: done (%s filled=%s)", ctx.tag(), res.state, res.filled_qty)
        self._snapshot_quiet(ctx.mode, ctx.account)

    def _finish_verified(self, ctx: _Ctx, res: _OrderResult, finalize: Callable[[_Ctx], None]) -> None:
        """타임아웃/불명으로 끝났던 주문을 뒤늦게 종결 확인한 뒤의 마무리. 새 체결이 있으면 보호주문을 맞추고 신호를
        done 으로 바꾼다(체결 회신은 _ingest_fills 가 보냈다). 체결이 없었으면 주문 상태만 닫는다."""
        if res.state == "rejected" or (res.state == "cancelled" and not res.fills):
            self._set_result(ctx, st.SIGNAL_REJECTED, "EXCHANGE_REJECTED" if res.state == "rejected" else None,
                             f"verified late: {res.state} without fills")
            self._finalize_signal(ctx.mode, ctx.event_id)
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
        self._finalize_signal(ctx.mode, ctx.event_id)
        self._alert(f"[executor:{ctx.mode}/{ctx.account}] {ctx.action} {ctx.position_id} {ctx.event_id}: late fills verified "
                    f"(qty={res.filled_qty}); lot and protections updated")

    def _check_expected_qty(self, ctx: _Ctx) -> str | None:
        sig = ctx.sig
        if sig is None or sig.expected_qty_btc_after is None or ctx.exchange is None:
            return None
        lot = self.store.get_lot(ctx.mode, ctx.account, ctx.position_id)
        actual = float(lot["qty"]) if lot is not None and lot["status"] == st.LOT_OPEN else 0.0
        step = float(ctx.exchange.instrument()["qty_step"])
        expected = float(sig.expected_qty_btc_after) * ctx.qty_multiplier
        if qty_eq(actual, expected, step):
            return None
        self._alert(f"[executor:{ctx.mode}/{ctx.account}] {ctx.position_id} {ctx.event_id}: QTY_MISMATCH "
                    f"expected={fmt_step(expected, step)} actual={fmt_step(actual, step)}")
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
        포지션 단위 보호(lot_protection 미지원 거래소)는 레그의 SL/TP 를 해제한다 (체결 경합 감지 불가).
        반환: 갱신된 lot (체결 처리로 닫혔을 수 있다)."""
        ex = ctx.exchange
        assert ex is not None
        account = ctx.account
        pid = lot["position_id"]
        if _is_position_protection(lot):
            return self._clear_position_protection(ctx, lot)
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
                self._handle_protection_fill(ctx.mode, account, ex, lot, kind, i, e, o)
                lot = self.store.get_lot(ctx.mode, account, pid) or lot
                if lot["status"] != st.LOT_OPEN:
                    return lot
                continue
            if o is not None and status not in _TERMINAL:
                raise _ProtectionError(f"cancel {kind}[{i}] not confirmed (status {status})")
            self.store.update_order(link, account, status=status or "Cancelled")
            self._drop_entry(lot, kind, link)
            lot["updated_at_ms"] = now_ms()
            self.store.upsert_lot(lot)
        return lot

    def _clear_position_protection(self, ctx: _Ctx, lot: dict) -> dict:
        """포지션 단위 보호 '해제'. 레그 보호는 계정+레그 상태이므로 이 lot 의 기록만 지우고 끝내지 않는다:
        같은 레그에 보호 의도(SL/TP)가 있는 다른 open lot 이 있으면 **그 형제의 SL/TP 를 다시 적용**하고(레그를 비우지 않음),
        없으면 set_position_protection(idx, None, None) 으로 비운다. 둘 다 레그 상태(store)를 갱신한다.
        이 lot 에 기록(po.position)이 없으면 거래소 호출 없이 끝."""
        ex = ctx.exchange
        assert ex is not None
        mode, account = ctx.mode, ctx.account
        po = dict(lot.get("protection_orders") or _empty_protection())
        if not po.get("position"):
            return lot
        idx = int(lot["position_idx"])
        intent = self._leg_intent(mode, account, idx, None, exclude_pid=lot["position_id"])
        if intent.sl is not None or intent.tp is not None:
            sources = {pid: l for pid, l in ((intent.sl_from, intent.sl_lot), (intent.tp_from, intent.tp_lot)) if l is not None}
            try:
                r = ex.set_position_protection(idx, intent.sl, intent.tp) or {}
            except ExchangeError as err:
                # 형제 보호 재적용 실패: 형제를 failed 로 표시해 reconcile 이 다시 걸게 하고, 이 lot 의 기록은 남겨 재시도한다
                for sib in sources.values():
                    self._mark_failed(sib)
                raise _ProtectionError(f"re-applying sibling {sorted(sources)} leg protection failed: "
                                       f"{self._err_note(err)}") from None
            state = self._leg_state_dict(idx, r, intent, position_id=intent.sl_from or intent.tp_from)
            self.store.set_leg_protection(mode, account, idx, state)
            for sib in sources.values():
                spo = dict(sib.get("protection_orders") or _empty_protection())
                spo.update({"kind": _PROTECTION_KIND_POSITION, "position": dict(state), "failed": False})
                sib["protection_orders"] = spo
                sib["updated_at_ms"] = now_ms()
                self.store.upsert_lot(sib)
            log.info("%s: leg idx%d protection re-applied from sibling(s) %s (sl=%s tp=%s) instead of clearing",
                     ctx.tag(), idx, sorted(sources), intent.sl, intent.tp)
        else:
            try:
                ex.set_position_protection(idx, None, None)
            except ExchangeError as err:
                raise _ProtectionError(f"clear position protection failed: {err.code}") from None
            self.store.set_leg_protection(mode, account, idx, None)
            log.info("%s: position-level protection cleared (idx%s)", ctx.tag(), idx)
        po["position"] = None
        lot["protection_orders"] = po
        lot["updated_at_ms"] = now_ms()
        self.store.upsert_lot(lot)
        return lot

    @staticmethod
    def _position_intent(lot: dict) -> tuple[float | None, int | None, float | None]:
        """lot 의 보호 의도 → 포지션 단위 (sl, tp_i, tp): SL 과 첫 미완료 TP 레벨."""
        po = lot.get("protection_orders") or {}
        sl_price = lot.get("stop_loss")
        sl = float(sl_price) if sl_price is not None and float(sl_price) > 0 else None
        done = {int(x) for x in po.get("tp_done") or []}
        levels = [(i, float(p)) for i, p in enumerate(lot.get("take_profit") or []) if i not in done and float(p) > 0]
        tp_i, tp = (levels[0] if levels else (None, None))
        return sl, tp_i, tp

    def _leg_intent(self, mode: str, account: str, idx: int, lot: dict | None, exclude_pid: str | None = None) -> "_LegIntent":
        """포지션 단위 레그에 걸 SL/TP 를 **lot 들의 의도를 합쳐** 정한다 (레그는 SL 하나 + TP 하나뿐).
        lot 자신의 의도가 우선이고, lot 이 정하지 않은 부분(SL 또는 TP)은 같은 레그의 다른 open lot 중 가장 최근 갱신된
        것의 의도로 채운다 — 한 lot 의 재설정이 형제의 보호를 지우지 않고, reconcile 이 lot 사이를 오가며 되감지 않도록."""
        out = _LegIntent()
        if lot is not None:
            sl, tp_i, tp = self._position_intent(lot)
            if sl is not None:
                out.sl, out.sl_from, out.sl_lot = sl, lot["position_id"], lot
            if tp is not None:
                out.tp, out.tp_i, out.tp_from, out.tp_lot = tp, tp_i, lot["position_id"], lot
        if out.sl is not None and out.tp is not None:
            return out
        skip = {exclude_pid, lot["position_id"] if lot is not None else None}
        sibs = [l for l in self.store.open_lots(mode, account)
                if int(l["position_idx"]) == idx and l["position_id"] not in skip and float(l["qty"]) > 0]
        sibs.sort(key=lambda l: int(l.get("updated_at_ms") or 0), reverse=True)
        for sib in sibs:
            ssl, stp_i, stp = self._position_intent(sib)
            if out.sl is None and ssl is not None:
                out.sl, out.sl_from, out.sl_lot = ssl, sib["position_id"], sib
            if out.tp is None and stp is not None:
                out.tp, out.tp_i, out.tp_from, out.tp_lot = stp, stp_i, sib["position_id"], sib
            if out.sl is not None and out.tp is not None:
                break
        return out

    @staticmethod
    def _leg_state_dict(idx: int, r: dict, intent: "_LegIntent", *, position_id: str | None) -> dict:
        return {"position_idx": idx, "stop_loss": r.get("stop_loss", intent.sl), "take_profit": r.get("take_profit", intent.tp),
                "tp_i": intent.tp_i, "position_id": position_id, "sl_from": intent.sl_from, "tp_from": intent.tp_from,
                "set_at_ms": now_ms()}

    def _mark_failed(self, lot: dict) -> None:
        po = dict(lot.get("protection_orders") or _empty_protection())
        po["failed"] = True
        lot["protection_orders"] = po
        lot["updated_at_ms"] = now_ms()
        self.store.upsert_lot(lot)

    def _leg_state(self, mode: str, account: str, idx: int) -> dict:
        """레그 보호 상태 (store). 기록이 없으면(이 코드 이전 DB) open lot 의 po.position 중 최신으로 한 번 시드한다."""
        state = self.store.get_leg_protection(mode, account, idx)
        if state is not None:
            return state
        latest: dict | None = None
        for lot in self.store.open_lots(mode, account):
            if int(lot["position_idx"]) != idx or not _is_position_protection(lot) or float(lot["qty"]) <= 0:
                continue
            pos = (lot.get("protection_orders") or {}).get("position")
            if not pos:
                continue
            if latest is None or int(pos.get("set_at_ms") or 0) >= int(latest.get("set_at_ms") or 0):
                latest = dict(pos, position_id=lot["position_id"])
        if latest is None:
            self.store.set_leg_protection(mode, account, idx, None)
            return self.store.get_leg_protection(mode, account, idx) or {}
        seeded = {"position_idx": idx, "stop_loss": latest.get("stop_loss"), "take_profit": latest.get("take_profit"),
                  "tp_i": latest.get("tp_i"), "position_id": latest.get("position_id"),
                  "set_at_ms": int(latest.get("set_at_ms") or now_ms())}
        self.store.set_leg_protection(mode, account, idx, seeded)
        return seeded

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
        """lot.stop_loss / lot.take_profit 기준으로 보호를 건다.
        - lot 보호 지원 거래소: 조건부 reduceOnly 시장가 보호주문. TP 는 남은 레벨(tp_done 제외)에 lot 수량을 균등 분할,
          나머지는 마지막 레벨. min_qty 미만 레벨은 건너뛰고 skipped 에 기록.
        - 미지원 거래소: set_position_protection(idx, sl, 첫 미완료 TP) 로 레그 단위 설정 (_place_position_protection).
        트리거가 이미 지난 가격(거래소 거부)은 지금 reduceOnly 시장가로 실행한다. 실패하면 po.failed=True 로 남기고 예외."""
        ex = ctx.exchange
        assert ex is not None
        if depth > _MAX_PROTECTION_DEPTH:
            raise _ProtectionError("protection placement recursion limit")
        if not getattr(ex, "supports_lot_protection", True):
            self._place_position_protection(ctx, lot, depth)
            return
        inst = ex.instrument()
        step, min_qty = float(inst["qty_step"]), float(inst["min_qty"])
        po = dict(lot.get("protection_orders") or _empty_protection())
        po.setdefault("gen", 0)
        po.setdefault("tp_done", [])
        po.pop("kind", None)
        po.pop("position", None)
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

    def _place_position_protection(self, ctx: _Ctx, lot: dict, depth: int = 0) -> None:
        """포지션(레그) 단위 SL/TP (lot 단위 조건부 주문을 못 쓰는 거래소: Toobit trading-stop 등).
        레그 전체에 하나뿐이라 같은 레그의 다른 lot 보호를 덮어쓴다 — 마지막 갱신이 레그 전체에 적용된다.
        TP 는 첫 미완료 레벨만 쓰고 나머지 레벨은 skipped 에 기록한다. 둘 다 없으면 거래소를 건드리지 않는다
        (다른 lot 의 레그 보호를 지우지 않기 위해)."""
        ex = ctx.exchange
        assert ex is not None
        idx = int(lot["position_idx"])
        po = dict(lot.get("protection_orders") or _empty_protection())
        po.setdefault("gen", 0)
        po.setdefault("tp_done", [])
        po.update({"kind": _PROTECTION_KIND_POSITION, "sl": None, "tp": [], "skipped": [], "failed": False})
        po.setdefault("position", None)
        lot["protection_orders"] = po
        step = float(ex.instrument()["qty_step"])
        qty = floor_step(float(lot["qty"]), step)
        if qty <= 0:
            self.store.upsert_lot(lot)
            return
        own_sl, own_tp_i, own_tp = self._position_intent(lot)
        done = {int(x) for x in po.get("tp_done") or []}
        levels = [(i, float(p)) for i, p in enumerate(lot.get("take_profit") or []) if i not in done and float(p) > 0]
        for i, _ in levels[1:]:
            po["skipped"].append(f"tp{i}")
        if len(levels) > 1:
            log.warning("%s: %s supports position-level TP only; using TP[%s]=%s, skipping %s",
                        ctx.tag(), getattr(ex, "display_name", ex.name), own_tp_i, own_tp, [i for i, _ in levels[1:]])
        if own_sl is None and own_tp is None:
            # 이 lot 은 보호 의도가 없다 → 거래소를 건드리지 않는다 (레그에 걸린 형제 보호를 지우지 않기 위해)
            po["position"] = None
            lot["updated_at_ms"] = now_ms()
            self.store.upsert_lot(lot)
            return
        intent = self._leg_intent(ctx.mode, ctx.account, idx, lot)
        borrowed = [p for p in (intent.sl_from, intent.tp_from) if p and p != lot["position_id"]]
        if borrowed:
            log.warning("%s: position-level protection idx%d is one per leg; merging with sibling intent %s "
                        "(sl=%s from %s, tp=%s from %s)", ctx.tag(), idx, sorted(set(borrowed)), intent.sl, intent.sl_from,
                        intent.tp, intent.tp_from)
        for _attempt in range(2):
            try:
                r = ex.set_position_protection(idx, intent.sl, intent.tp)
                break
            except ExchangeRejected as e:
                if _is_crossed_trigger(e):
                    crossed = self._crossed_position_level(ex, lot, intent.sl, intent.tp_i, intent.tp, qty)
                    if crossed is not None:
                        src = intent.sl_from if crossed.kind == "sl" else intent.tp_from
                        if src != lot["position_id"] and _attempt == 0:
                            # 형제에게서 빌린 가격이 이미 지났다 → 그 형제는 failed 로 두어 reconcile 이 자기 문맥(auto:sl/tp)에서
                            # 처리하게 하고, 이 lot 은 빌린 부분을 빼고 다시 건다
                            sib = intent.sl_lot if crossed.kind == "sl" else intent.tp_lot
                            if sib is not None:
                                self._mark_failed(sib)
                            if crossed.kind == "sl":
                                intent.sl, intent.sl_from, intent.sl_lot = None, None, None
                            else:
                                intent.tp, intent.tp_i, intent.tp_from, intent.tp_lot = None, None, None, None
                            if intent.sl is None and intent.tp is None:
                                po["failed"] = True
                                lot["updated_at_ms"] = now_ms()
                                self.store.upsert_lot(lot)
                                raise
                            continue
                        self.store.upsert_lot(lot)
                        self._execute_crossed_protection(ctx, lot, crossed, depth)
                        return
                po["failed"] = True
                lot["updated_at_ms"] = now_ms()
                self.store.upsert_lot(lot)
                raise
            except ExchangeError:
                po["failed"] = True
                lot["updated_at_ms"] = now_ms()
                self.store.upsert_lot(lot)
                raise
        r = r or {}
        state = self._leg_state_dict(idx, r, intent, position_id=lot["position_id"])
        po["position"] = dict(state)
        po["gen"] = int(po.get("gen") or 0) + 1
        lot["updated_at_ms"] = now_ms()
        self.store.upsert_lot(lot)
        # 레그 상태(계정+레그 단위 진실) 갱신 — 스냅샷/protection_missing 은 이것을 본다
        self.store.set_leg_protection(ctx.mode, ctx.account, idx, state)
        for sib in (intent.sl_lot, intent.tp_lot):
            if sib is not None and sib["position_id"] != lot["position_id"]:
                spo = dict(sib.get("protection_orders") or _empty_protection())
                spo.update({"kind": _PROTECTION_KIND_POSITION, "position": dict(state), "failed": False})
                sib["protection_orders"] = spo
                sib["updated_at_ms"] = now_ms()
                self.store.upsert_lot(sib)
        log.info("%s: position-level protection set idx%d sl=%s tp=%s (rev=%s)", ctx.tag(), idx, intent.sl, intent.tp,
                 lot.get("protection_revision"))

    @staticmethod
    def _crossed_position_level(ex: ExchangeBase, lot: dict, sl: float | None, tp_i: int | None, tp: float | None,
                                qty: float) -> _CrossedTrigger | None:
        """포지션 단위 설정이 '이미 지난 가격' 으로 거부됐을 때 어느 쪽(SL/TP)이 지났는지 현재가로 판정."""
        try:
            price = float(ex.last_price())
        except Exception:  # noqa: BLE001
            return None
        is_long = lot["leg"] == "long"
        if sl is not None and ((is_long and price <= sl) or (not is_long and price >= sl)):
            return _CrossedTrigger("sl", 0, sl, qty)
        if tp is not None and ((is_long and price >= tp) or (not is_long and price <= tp)):
            return _CrossedTrigger("tp", int(tp_i or 0), tp, qty)
        return None

    def _place_one_protection(self, ctx: _Ctx, lot: dict, kind: str, i: int, price: float, qty: float, *,
                              trigger_direction: int) -> dict:
        ex = ctx.exchange
        assert ex is not None
        account = ctx.account
        po = lot["protection_orders"]
        side = "Sell" if lot["leg"] == "long" else "Buy"
        rev = int(lot.get("protection_revision") or 0)
        last_err: ExchangeRejected | None = None
        exchange_retries = 0
        for _ in range(_MAX_LINK_GEN_ATTEMPTS):
            gen = int(po.get("gen") or 0)
            link = self._protection_link_id(ctx.mode, lot["position_id"], kind, rev, i, gen)
            if self.store.get_order(link, account) is not None:
                # 같은 (position_id, revision, i) 로 이미 낸 적 있는 ID (닫힌 lot 재진입 등) → 거래소 왕복 없이 세대 올림
                po["gen"] = gen + 1
                continue
            self.store.insert_order(link, ctx.mode, lot["position_id"], kind, side, qty, True,
                                    event_id=ctx.event_id, status="new", trigger_price=price, account=account)
            try:
                r = ex.place_conditional(side, qty, int(lot["position_idx"]), price, trigger_direction, link,
                                        self.settings.protection_trigger_by)
            except ExchangeRejected as e:
                self.store.update_order(link, account, status="Rejected")
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
                    self.store.update_order(link, account, status="unknown" if o is None else str(o.get("status")))
                    raise
                order_id = str(o.get("order_id") or "")
                self.store.update_order(link, account, order_id=order_id, status=str(o.get("status") or "Untriggered"))
                po["gen"] = gen + 1
                return {"order_link_id": link, "order_id": order_id, "price": float(price), "qty": float(qty), "i": int(i)}
            order_id = str(r.get("order_id") or "")
            self.store.update_order(link, account, order_id=order_id, status="Untriggered")
            return {"order_link_id": link, "order_id": order_id, "price": float(price), "qty": float(qty), "i": int(i)}
        if last_err is not None:
            raise last_err
        raise _ProtectionError(f"no free orderLinkId for {kind}[{i}] after {_MAX_LINK_GEN_ATTEMPTS} generations")

    def _execute_crossed_protection(self, ctx: _Ctx, lot: dict, c: _CrossedTrigger, depth: int) -> None:
        """트리거가 이미 지난 SL/TP: 조건부 주문 대신 지금 reduceOnly 시장가로 그 보호를 실행한다.
        SL 은 lot 전량, TP[i] 는 그 레벨 수량. 회신은 auto:sl/auto:tp 이벤트로, 남은 수량의 보호주문은 다시 맞춘다."""
        ex = ctx.exchange
        assert ex is not None
        mode, account, pid = ctx.mode, ctx.account, lot["position_id"]
        step = float(ex.instrument()["qty_step"])
        rev = int(lot.get("protection_revision") or 0)
        lot_qty = floor_step(float(lot["qty"]), step)
        qty = lot_qty if c.kind == "sl" else min(floor_step(c.qty, step), lot_qty)
        if qty <= 0:
            return
        event_id = self._auto_event_id(c.kind, pid, rev, c.i, lot)
        action = self._auto_action(mode, account, event_id, "full_exit" if qty_eq(qty, lot_qty, step) else "partial_exit")
        reason = "STOP_LOSS_TRIGGERED" if c.kind == "sl" else "TAKE_PROFIT_TRIGGERED"
        actx = self._ctx_from_lot(mode, lot, event_id, action, ex)
        gen = int((lot.get("protection_orders") or {}).get("gen") or 0)
        link = order_link_id(mode, pid, c.kind, str(rev), str(c.i), f"g{gen}", "mkt")
        self._alert(f"[executor:{mode}/{account}] {pid}: {c.kind.upper()} trigger {c.price} already crossed at placement; "
                    f"executing reduceOnly market {action} qty={fmt_step(qty, step)}")
        log.warning("%s: %s", ctx.tag(), c)
        res = self._execute_market(actx, actx.close_side(), qty, reduce_only=True, purpose="exit",
                                   apply=self._apply_close_fill, link=link, reason_code=reason)
        if res.state == "rejected":
            raise _ProtectionError(f"crossed {c.kind}[{c.i}] market close rejected")
        if res.state == "timeout":
            raise _ProtectionError(f"crossed {c.kind}[{c.i}] market close not confirmed (reconcile will verify)")
        lot = self.store.get_lot(mode, account, pid) or lot
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
        self._alert(f"[executor:{ctx.mode}/{ctx.account}] {ctx.action} {ctx.position_id} {ctx.event_id}: PROTECTION_FAILED "
                    f"({note}) — position may be unprotected; reconcile will retry{(' — ' + extra) if extra else ''}")

    @staticmethod
    def _err_note(e: BaseException) -> str:
        if isinstance(e, ExchangeRejected):
            return f"EXCHANGE_REJECTED ret_code={e.ret_code}"
        if isinstance(e, ExchangeError):
            return e.code
        return str(e)[:120]

    def _protection_missing(self, lot: dict) -> bool:
        """설정된 보호가격에 비해 기록된 보호 항목이 빠져 있는가 (min_qty 로 건너뛴 것은 제외).
        포지션 단위 보호 lot 은 **레그 상태**(store `leg_protection`, 형제 lot 의 해제/재설정도 반영됨) 기준:
        SL 의도가 있는데 레그에 SL 이 없거나, TP 의도가 있는데 레그에 TP 가 없으면 빠진 것."""
        po = lot.get("protection_orders") or {}
        if po.get("failed"):
            return True
        skipped = set(po.get("skipped") or [])
        sl = lot.get("stop_loss")
        done = {int(x) for x in po.get("tp_done") or []}
        tps = [(i, float(p)) for i, p in enumerate(lot.get("take_profit") or []) if i not in done and float(p) > 0]
        if po.get("kind") == _PROTECTION_KIND_POSITION:
            leg = self._leg_state(str(lot.get("mode") or ""), str(lot.get("account") or self._default_account_name()),
                                  int(lot["position_idx"]))
            if sl is not None and float(sl) > 0 and leg.get("stop_loss") is None:
                return True
            if tps and leg.get("take_profit") is None:
                return True
            return False
        if sl is not None and float(sl) > 0 and not po.get("sl") and "sl" not in skipped:
            return True
        have = {int(t.get("i", -1)) for t in po.get("tp") or []}
        for i, _p in tps:
            if f"tp{i}" in skipped:
                continue
            if i not in have:
                return True
        return False

    def protection_missing(self, mode: str, account: str | None = None) -> list[str]:
        """보호가격이 설정돼 있는데 보호주문이 빠진 open lot 의 position_id 목록 (/healthz, /state 노출용).
        account=None 이면 모든 계정 (중복 position_id 가능 — 개수는 lot 수)."""
        return [lot["position_id"] for lot in self.store.open_lots(mode, account)
                if float(lot["qty"]) > 0 and self._protection_missing(lot)]

    # ------------------------------------------------------------------ reconcile / snapshot
    def reconcile(self, mode: str, account: str | None = None) -> bool:
        """계정 하나의 대사 (account=None 이면 mode 에 거래소가 있는 모든 계정을 차례로; 전부 일치해야 True).
        ① 보호주문 체결 수집(auto:sl/auto:tp 회신) + 빠진/죽은 보호주문 재생성 ② 미확정 시장가 주문 재확인
        ③ 고아 조건부 주문 정리 ④ 닫힌 lot 의 잔여 보호주문 취소 재시도 ⑤ 거래소 포지션 vs open lots 합계 대조.
        ①~④ 의 실패는 알림만 하고 ⑤ 를 막지 않는다. ⑤ 가 일치하면 True(스냅샷 가능), 불일치/오류면 False."""
        with self._lock:
            if account is None:
                names = self._accounts_with_exchange(mode)
                if not names:
                    log.debug("reconcile %s: no exchange for mode", mode)
                    return False
                results = [self._reconcile_account(mode, n) for n in names]
                return all(results)
            return self._reconcile_account(mode, account)

    def _reconcile_account(self, mode: str, account: str) -> bool:
        ex = self._exchange(mode, account)
        if ex is None:
            log.debug("reconcile %s/%s: no exchange", mode, account)
            return False
        tag = f"{mode}/{account}"
        for name, fn in (("protections", self._reconcile_protections),
                         ("uncertain orders", self._verify_uncertain_orders),
                         ("orphan conditionals", self._sweep_orphan_conditionals),
                         ("leftover cancels", self._retry_pending_cancels)):
            try:
                fn(mode, account, ex)
            except ExchangeError as e:
                log.warning("reconcile %s: %s step exchange error %s", tag, name, e.code)
                self._alert(f"[executor:{tag}] reconcile ({name}) exchange error {e.code}")
            except Exception as e:  # noqa: BLE001
                log.exception("reconcile %s: %s step unexpected %s", tag, name, type(e).__name__)
                self._alert(f"[executor:{tag}] reconcile ({name}) failed: {type(e).__name__}")
        try:
            ok = self._reconcile_positions(mode, account, ex)
        except ExchangeError as e:
            log.warning("reconcile %s: positions exchange error %s", tag, e.code)
            self._alert(f"[executor:{tag}] reconcile (positions) exchange error {e.code}")
            return False
        except Exception as e:  # noqa: BLE001
            log.exception("reconcile %s: unexpected %s", tag, type(e).__name__)
            self._alert(f"[executor:{tag}] reconcile failed: {type(e).__name__}")
            return False
        self._maybe_prune_ingress_log()
        return ok

    def _reconcile_protections(self, mode: str, account: str, ex: ExchangeBase) -> None:
        tag = f"{mode}/{account}"
        for lot in self.store.open_lots(mode, account):
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
                    key = f"absent:{account}:{link}"
                    strikes = int(self.store.get_meta(key, "0") or 0) + 1
                    self.store.set_meta(key, str(strikes))
                    log.warning("reconcile %s: protection %s[%d] of %s not found on exchange (%d)", tag, kind, i, pid, strikes)
                    if strikes < 2:
                        continue
                    self._drop_entry(lot, kind, link)
                    self.store.update_order(link, account, status="absent")
                    gone.append(f"{kind}[{i}] absent")
                    continue
                if e.get("order_id") and o.get("order_id") and str(o["order_id"]) != str(e["order_id"]):
                    log.warning("reconcile %s: protection %s[%d] of %s order_id mismatch, skipping", tag, kind, i, pid)
                    continue
                status = str(o.get("status") or "")
                if status in _PROTECTION_FILLING:
                    self._handle_protection_fill(mode, account, ex, lot, kind, i, e, o)
                    changed = True
                    lot = self.store.get_lot(mode, account, pid) or lot
                    if lot["status"] != st.LOT_OPEN:
                        break
                elif status in _PROTECTION_OPEN or status == "Triggered":
                    continue
                else:
                    # Cancelled/Deactivated/Rejected: 거래소 쪽 reduce-only 조정, 수동 취소 등 → 항목 제거 후 재생성
                    log.warning("reconcile %s: protection %s[%d] of %s is %s", tag, kind, i, pid, status)
                    self._drop_entry(lot, kind, link)
                    self.store.update_order(link, account, status=status)
                    gone.append(f"{kind}[{i}] {status}")
            if gone:
                lot["updated_at_ms"] = now_ms()
                self.store.upsert_lot(lot)
            lot = self.store.get_lot(mode, account, pid) or lot
            if lot["status"] != st.LOT_OPEN or float(lot["qty"]) <= 0:
                continue
            missing = self._protection_missing(lot)
            if not (changed or gone or missing):
                continue
            if gone or (missing and not changed):
                self._alert(f"[executor:{tag}] {pid}: protection orders missing on exchange "
                            f"({', '.join(gone) if gone else 'not placed'}); re-placing")
            ctx = self._ctx_from_lot(mode, lot, lot.get("last_event_id") or "", "partial_exit", ex)
            try:
                self._reset_protections(ctx, lot)
            except (ExchangeError, _ProtectionError) as e:
                self._protection_failed(ctx, e, "re-placement during reconcile failed")

    def _handle_protection_fill(self, mode: str, account: str, ex: ExchangeBase, lot: dict, kind: str, i: int,
                                entry: dict, o: dict) -> None:
        pid = lot["position_id"]
        rev = int(lot.get("protection_revision") or 0)
        event_id = self._auto_event_id(kind, pid, rev, i, lot)
        order_id = str(o.get("order_id") or entry.get("order_id") or "")
        order_qty = float(o.get("qty") or entry.get("qty") or 0.0)
        step = float(ex.instrument()["qty_step"])
        execs = ex.executions(order_id) if order_id else []
        known = {f["exec_id"] for f in self.store.fills_for_order(entry["order_link_id"], account)}
        new_total = sum(float(x.get("qty") or 0.0) for x in execs if str(x.get("exec_id") or "") not in known)
        remaining = float(lot["qty"]) - new_total
        action = self._auto_action(mode, account, event_id, "full_exit" if remaining < step / 2 else "partial_exit")
        reason = "STOP_LOSS_TRIGGERED" if kind == "sl" else "TAKE_PROFIT_TRIGGERED"
        ctx = self._ctx_from_lot(mode, lot, event_id, action, ex)
        log.info("reconcile %s/%s: %s[%d] of %s %s (new fill qty=%s)", mode, account, kind, i, pid, o.get("status"), new_total)
        fills = self._ingest_fills(ctx, entry["order_link_id"], order_id, execs, order_qty, step,
                                   self._apply_close_fill, reason_code=reason)
        status = str(o.get("status") or "")
        self.store.update_order(entry["order_link_id"], account, status=status)

        lot = self.store.get_lot(mode, account, pid) or lot
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
            self._alert(f"[executor:{mode}/{account}] {reason} {pid}: qty={fmt_step(sum(f['qty'] for f in fills), step)} "
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

    def _auto_action(self, mode: str, account: str, event_id: str, proposed: str) -> str:
        """auto 이벤트의 action 은 첫 보고에서 정해져 바뀌지 않는다 (같은 event_id 의 후속 보고 정체성 유지).
        키는 계정 범위; 1단계 키(auto_action:{mode}:{event_id}) 는 기본 계정에 한해 읽기만 한다."""
        key = f"auto_action:{mode}:{account}:{event_id}"
        stored = self.store.get_meta(key)
        if stored is None and account == st.DEFAULT_ACCOUNT:
            stored = self.store.get_meta(f"auto_action:{mode}:{event_id}")
        if stored in ("partial_exit", "full_exit"):
            return stored
        self.store.set_meta(key, proposed)
        return proposed

    def _verify_uncertain_orders(self, mode: str, account: str, ex: ExchangeBase) -> None:
        """타임아웃/불명으로 끝난 시장가 주문을 orderLinkId 로 재조회해 종결되면 체결을 수집·반영한다.
        거래소에 끝내 없으면(_UNCERTAIN_MAX_CHECKS 회) absent 로 닫는다."""
        for row in self.store.orders_with_status(mode, account, _UNCERTAIN_MARKET, _MARKET_PURPOSES):
            link = row["order_link_id"]
            key = f"verify:{account}:{link}"
            checks = int(self.store.get_meta(key, "0") or 0) + 1
            try:
                o = ex.get_order(link)
            except ExchangeError as e:
                log.warning("verify %s: get_order failed (%s)", link, e.code)
                continue
            if o is None:
                if checks >= _UNCERTAIN_MAX_CHECKS:
                    self.store.update_order(link, account, status="absent")
                    self._alert(f"[executor:{mode}/{account}] order {row.get('event_id')} never appeared on exchange "
                                f"after {checks} checks; closed as absent")
                else:
                    self.store.set_meta(key, str(checks))
                continue
            status = str(o.get("status") or "")
            if status not in _TERMINAL:
                self.store.set_meta(key, str(checks))
                self.store.update_order(link, account, order_id=o.get("order_id"), status=status or "submitted")
                continue
            ctx = self._ctx_for_order_row(mode, account, row, ex)
            if ctx is None:
                self.store.update_order(link, account, status=status)
                self._alert(f"[executor:{mode}/{account}] uncertain order {link[:12]} terminal ({status}) but no context; "
                            f"manual check")
                continue
            self.store.update_order(link, account, order_id=o.get("order_id"), status="submitted")
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
            if not str(ctx.event_id).startswith("auto:"):
                self._finalize_signal(mode, ctx.event_id)

    def _ctx_for_order_row(self, mode: str, account: str, row: dict, ex: ExchangeBase) -> _Ctx | None:
        event_id = row.get("event_id") or ""
        sig_row = self.store.get_signal(event_id, mode) if event_id and not event_id.startswith("auto:") else None
        if sig_row is not None:
            try:
                sig = Signal.model_validate_json(sig_row["raw_body"])
            except Exception:  # noqa: BLE001
                return None
            acct = self._acct(account)
            ctx = self._ctx_for_account(sig, acct) if acct is not None else self._ctx_from_signal(sig)
            ctx.account = account
            ctx.exchange = ex
            return ctx
        lot = self.store.get_lot(mode, account, row["position_id"])
        if lot is None:
            return None
        action = self.store.get_meta(f"auto_action:{mode}:{account}:{event_id}") or \
            (self.store.get_meta(f"auto_action:{mode}:{event_id}") if account == st.DEFAULT_ACCOUNT else None) or "partial_exit"
        return self._ctx_from_lot(mode, lot, event_id or f"auto:verify:{row['position_id']}", action, ex)

    def _sweep_orphan_conditionals(self, mode: str, account: str, ex: ExchangeBase) -> None:
        """거래소에 열린 조건부 주문 중 어떤 lot 도 참조하지 않는 우리 보호주문(orders 에 sl/tp 로 기록) 은 취소한다.
        우리 기록에 없는 주문은 손대지 않고 1회 알림만."""
        referenced: set[str] = set()
        for lot in self.store.open_lots(mode, account) + self.store.lots_with_pending_cancel(mode, account):
            for _, _, e in self._protection_entries(lot):
                if e.get("order_link_id"):
                    referenced.add(e["order_link_id"])
        acct = self._acct(account)
        idxs = sorted(acct.allowed_position_idx()) if acct is not None else sorted(self.settings.allowed_position_idx())
        for idx in idxs:
            for o in ex.open_conditional_orders(idx):
                link = str(o.get("order_link_id") or "")
                if not link or link in referenced:
                    continue
                row = self.store.get_order(link, account)
                if row is None and link.isalnum() and len(link) < 34:
                    # 거래소가 클라이언트 ID 를 절단해 돌려줬다(OKX 32자): 원장의 link 접두사로 되찾는다
                    cands = self.store.orders_by_link_prefix(account, link, limit=2)
                    if len(cands) == 1:
                        row = cands[0]
                        link = str(row["order_link_id"])
                        if link in referenced:
                            continue
                if row is not None and row.get("purpose") in ("sl", "tp") and row.get("mode") == mode:
                    log.warning("reconcile %s/%s: orphan protection %s on idx%d (not referenced by any lot); cancelling",
                                mode, account, link[:12], idx)
                    try:
                        if ex.cancel_order(link):
                            self.store.update_order(link, account, status="Cancelled")
                        self._alert(f"[executor:{mode}/{account}] cancelled orphan protection order {link[:12]} (idx{idx}, "
                                    f"trigger={o.get('trigger_price')} qty={o.get('qty')})")
                    except ExchangeError as e:
                        self._alert(f"[executor:{mode}/{account}] orphan protection order {link[:12]} cancel failed {e.code}")
                elif f"{account}:{link}" not in self._orphan_alerted:
                    self._orphan_alerted.add(f"{account}:{link}")
                    self._alert(f"[executor:{mode}/{account}] unknown conditional order on exchange idx{idx} "
                                f"(trigger={o.get('trigger_price')} qty={o.get('qty')} side={o.get('side')}); not ours — check")

    def _retry_pending_cancels(self, mode: str, account: str, ex: ExchangeBase) -> None:
        """닫힌 lot 에 남은 보호(취소 실패한 lot 단위 보호주문 항목, 또는 해제 실패한 포지션 단위 보호 기록 po.position)
        를 다시 취소/해제한다 (포지션 단위는 형제 lot 이 있으면 형제 값 재적용). 남아 있는 동안 알림."""
        for lot in self.store.lots_with_pending_cancel(mode, account):
            ctx = self._ctx_from_lot(mode, lot, lot.get("last_event_id") or "", "full_exit", ex)
            try:
                self._cancel_protections(ctx, lot)
                log.info("reconcile %s/%s: leftover protections of closed lot %s cancelled", mode, account, lot["position_id"])
            except (ExchangeError, _ProtectionError) as e:
                self._alert(f"[executor:{mode}/{account}] closed lot {lot['position_id']} still has live protection orders "
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

    def _reconcile_positions(self, mode: str, account: str, ex: ExchangeBase) -> bool:
        step = float(ex.instrument()["qty_step"])
        positions = ex.positions()
        expected: dict[int, float] = {}
        for lot in self.store.open_lots(mode, account):
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
        was = self.store.is_inconsistent(mode, account)
        tag = f"{mode}/{account}"
        if mismatches:
            note = "; ".join(mismatches)[:400]
            self.store.set_inconsistent(mode, account, True, note)
            if not was:
                self._alert(f"[executor:{tag}] RECONCILE_REQUIRED: {note}")
            else:
                log.warning("reconcile %s: still inconsistent (%s)", tag, note)
            return False
        self.store.set_inconsistent(mode, account, False, "")
        if was:
            log.info("reconcile %s: consistent again", tag)
            self._alert(f"[executor:{tag}] reconcile OK: positions consistent again")
        return True

    def build_snapshot_positions(self, mode: str, account: str | None = None) -> list[dict]:
        """계약 snapshot.positions 항목 (open lot 기준, 보호가격은 거래소에서 확인된 것만).
        account=None 은 계정이 하나뿐일 때만 (1단계 호환)."""
        account = self._resolve_account(account, "build_snapshot_positions")
        with self._lock:
            ex = self._exchange(mode, account)
            lots = self.store.open_lots(mode, account)
            mark: float | None = None
            if ex is not None and lots:
                try:
                    m = ex.mark_price()
                    mark = float(m) if m and float(m) > 0 else None
                except Exception as e:  # noqa: BLE001
                    log.warning("snapshot %s/%s: mark_price failed (%s)", mode, account, type(e).__name__)
            leg_prot: dict[int, tuple[float | None, list[float] | None]] = {}
            out: list[dict] = []
            for lot in lots:
                qty = float(lot["qty"])
                entry = float(lot.get("avg_entry") or 0.0)
                if qty <= 0 or entry <= 0:
                    log.warning("snapshot %s/%s: skipping lot %s (qty=%s entry=%s)", mode, account, lot["position_id"], qty, entry)
                    continue
                if _is_position_protection(lot):
                    idx = int(lot["position_idx"])
                    if idx not in leg_prot:
                        leg_prot[idx] = self._leg_position_protection(mode, account, ex, idx)
                    stop_loss, take_profit = leg_prot[idx]
                else:
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

    def _leg_position_protection(self, mode: str, account: str, ex: ExchangeBase | None,
                                 idx: int) -> tuple[float | None, list[float] | None]:
        """포지션 단위 보호의 스냅샷 표시값: 거래소에 읽기 API(get_position_protection) 가 있으면 그 값,
        없으면 **레그 상태**(store `leg_protection` — 마지막 set/clear 가 반영된 값; lot 별 po.position 이 아님).
        해제된 레그는 (None, []) , 기록 자체가 없으면 (None, None)."""
        if ex is None:
            return None, None
        reader = getattr(ex, "get_position_protection", None)
        if callable(reader):
            try:
                v = reader(idx)
            except Exception as e:  # noqa: BLE001
                log.warning("snapshot: position protection read failed for idx%d (%s)", idx, type(e).__name__)
                return None, None
            if not v:
                return None, []
            tp = v.get("take_profit")
            return (float(v["stop_loss"]) if v.get("stop_loss") is not None else None,
                    [float(tp)] if tp is not None else [])
        leg = self._leg_state(mode, account, idx)
        if not leg:
            return None, None
        tp = leg.get("take_profit")
        sl = leg.get("stop_loss")
        if sl is None and tp is None:
            return None, [] if leg.get("cleared") or leg.get("set_at_ms") else None
        return (float(sl) if sl is not None else None, [float(tp)] if tp is not None else [])

    def snapshot_now(self, mode: str, account: str | None = None) -> dict | None:
        """reconcile(mode, account) 가 일치하면 전체 포지션 스냅샷 회신을 쌓고 그 결과를 돌려준다. 아니면 None.
        observed_at_ms 는 락을 잡기 전에 찍는다 (락 대기 시간이 '관측 시각' 을 늦추지 않도록).
        account=None 은 계정이 하나뿐일 때만 (여러 계정은 snapshot_all)."""
        account = self._resolve_account(account, "snapshot_now")
        observed = now_ms()
        with self._lock:
            if not self.reconcile(mode, account):
                return None
            positions = self.build_snapshot_positions(mode, account)
            return self.reporter.snapshot(mode, account, positions, observed_at_ms=observed)

    def snapshot_all(self, mode: str) -> dict[str, dict | None]:
        """mode 에 거래소가 있는 모든 계정의 snapshot_now. {account: 결과|None}."""
        out: dict[str, dict | None] = {}
        for name in self._accounts_with_exchange(mode):
            try:
                out[name] = self.snapshot_now(mode, name)
            except Exception as e:  # noqa: BLE001
                log.exception("snapshot %s/%s failed: %s", mode, name, type(e).__name__)
                self._alert(f"[executor:{mode}/{name}] snapshot failed: {type(e).__name__}")
                out[name] = None
        return out

    def _snapshot_quiet(self, mode: str, account: str) -> None:
        try:
            self.snapshot_now(mode, account)
        except Exception as e:  # noqa: BLE001
            log.exception("snapshot %s/%s failed: %s", mode, account, type(e).__name__)
            self._alert(f"[executor:{mode}/{account}] snapshot failed: {type(e).__name__}")

    # ------------------------------------------------------------------ 문맥/회신/알림
    @staticmethod
    def _ctx_from_signal(sig: Signal) -> _Ctx:
        return _Ctx(mode=sig.mode.value, event_id=sig.event_id, position_id=sig.position_id,
                    strategy=sig.strategy.value, leg=sig.leg.value, position_idx=int(sig.position_idx),
                    action=sig.action.value, sig=sig)

    def _ctx_for_account(self, sig: Signal, acct) -> _Ctx:
        """신호 → 계정 문맥. position_idx 매핑: one_way 계정은 1/2 → 0, hedge 계정은 신호값 그대로
        (hedge 계정에 idx 0 이 오면 게이트가 POSITION_MODE_MISMATCH 로 거부)."""
        ctx = self._ctx_from_signal(sig)
        ctx.account = acct.name
        ctx.acct = acct
        if getattr(acct, "position_mode", "hedge") == "one_way" and ctx.position_idx in (1, 2):
            ctx.position_idx = 0
        return ctx

    def _ctx_from_lot(self, mode: str, lot: dict, event_id: str, action: str, ex: ExchangeBase | None) -> _Ctx:
        account = str(lot.get("account") or self._default_account_name())
        return _Ctx(mode=mode, event_id=event_id, position_id=lot["position_id"], strategy=lot["strategy"],
                    leg=lot["leg"], position_idx=int(lot["position_idx"]), action=action, account=account,
                    acct=self._acct(account), exchange=ex)

    def _unknown_state(self, ctx: _Ctx, note: str) -> None:
        log.error("%s: UNKNOWN_STATE (%s)", ctx.tag(), note)
        self._report(ctx, "error", reason_code="UNKNOWN_STATE")
        self._set_result(ctx, st.SIGNAL_ERROR, "UNKNOWN_STATE", note)
        self._alert(f"[executor:{ctx.mode}/{ctx.account}] {ctx.action} {ctx.position_id} {ctx.event_id}: UNKNOWN_STATE ({note})")

    def _report(self, ctx: _Ctx, status: str, *, qty: float | None = None, fill_price: float | None = None,
                order_id: str | None = None, fill_id: str | None = None, reason_code: str | None = None,
                observed_at_ms: int | None = None, mark_fill_reported: str | None = None) -> None:
        """reporter.execution 래퍼 (계정 스트림). 회신 적재 실패가 매매 흐름을 끊지 않도록 예외는 로그+알림으로 흡수."""
        try:
            self.reporter.execution(
                ctx.mode, ctx.account, event_id=ctx.event_id, position_id=ctx.position_id, strategy=ctx.strategy,
                leg=ctx.leg, position_idx=ctx.position_idx, action=ctx.action, status=status, qty=qty,
                fill_price=fill_price, order_id=order_id, fill_id=fill_id, reason_code=reason_code,
                observed_at_ms=observed_at_ms if observed_at_ms is not None else now_ms(),
                mark_fill_reported=mark_fill_reported)
        except Exception as e:  # noqa: BLE001
            log.exception("report %s for %s failed: %s", status, ctx.tag(), type(e).__name__)
            self._alert(f"[executor:{ctx.mode}/{ctx.account}] report {status} for {ctx.event_id} failed: {type(e).__name__}")

    def _alert(self, text: str) -> None:
        try:
            self.alerts.send(text)
        except Exception:  # noqa: BLE001
            log.error("alert failed: %s", text)
