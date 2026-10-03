"""executor.py 장애/경합 복원력 (리뷰 지적 사항 회귀 테스트).

- 보호주문 취소와 체결의 경합: 이미 체결된 SL 을 '취소됨' 으로 버리지 않고 체결로 처리
- 체결 뒤 보호주문 실패: 체결 결과(done)는 유지, 트리거가 이미 지난 SL 은 즉시 시장가 청산
- 조건부 주문 타임아웃 뒤 거래소에 들어간 주문 채택(고아/중복 스탑 방지), 고아 조건부 주문 정리
- 실행 시점 만료(EXPIRED), protection_update 실패 시 같은 revision 재시도 허용 + reconcile 자가 복구
- 타임아웃/불명 시장가 주문의 늦은 종결 확인(late verify), 거래소에서 끝내 없으면 absent
- reconcile: 취소된 보호주문 재생성, 보호주문 단계 거래소 오류가 스냅샷을 막지 않음
- PartiallyFilledCanceled(IOC 부분 체결) 처리, lot 종료 시 취소 실패 → closed + 재시도
- 단방향(idx 0) 반대 레그 거부, auto 이벤트 ID 의 lot 인스턴스 구분, 모의 시세의 reference_price 추종
전부 PaperExchange(+결함 주입 서브클래스) 로 오프라인 실행.
"""
from __future__ import annotations

import time

import pytest

from lake_executor.exchange import ExchangeError, ExchangeRejected, PaperExchange
from lake_executor.executor import Executor
from lake_executor.reporter import Reporter
from lake_executor.store import Store
from lake_executor.util import order_link_id

from conftest import (
    PAPER_PRICE,
    execution_statuses,
    ingest,
    last_seq,
    load_reports,
    make_signal,
    reports_after,
    run_signal,
)


class FaultyPaper(PaperExchange):
    """결함 주입용 PaperExchange."""

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.conditional_errors: list[Exception] = []      # place_conditional 호출마다 앞에서 하나씩 던진다
        self.conditional_timeout_after_place = False       # 실제로 넣은 뒤 타임아웃 예외
        self.market_timeout_after_place = False            # 실제로 체결시킨 뒤 타임아웃 예외
        self.partial_ioc_qty: float | None = None          # IOC 부분 체결 시뮬레이션
        self.hidden: set[str] = set()                      # get_order 가 None 을 돌려줄 link
        self.get_order_raise: set[str] = set()             # get_order 가 ExchangeError 를 던질 link
        self.cancel_fail = False

    def place_conditional(self, side, qty, position_idx, trigger_price, trigger_direction, order_link_id, trigger_by):
        if self.conditional_errors:
            raise self.conditional_errors.pop(0)
        r = super().place_conditional(side, qty, position_idx, trigger_price, trigger_direction, order_link_id, trigger_by)
        if self.conditional_timeout_after_place:
            self.conditional_timeout_after_place = False
            raise ExchangeError("EXCHANGE_TIMEOUT", "simulated timeout after placement")
        return r

    def place_market(self, side, qty, position_idx, reduce_only, order_link_id):
        if self.partial_ioc_qty is not None:
            with self._lock:
                q = self._validate_new_order(side, qty, position_idx, order_link_id)
                o = self._new_order(order_link_id, side, q, int(position_idx), bool(reduce_only), kind="market")
                o["qty"] = float(self.partial_ioc_qty)
                self._execute(o, self._price)
                o["qty"] = q
                o["status"] = "PartiallyFilledCanceled"
                return {"order_id": o["order_id"], "order_link_id": order_link_id}
        r = super().place_market(side, qty, position_idx, reduce_only, order_link_id)
        if self.market_timeout_after_place:
            self.market_timeout_after_place = False
            raise ExchangeError("EXCHANGE_TIMEOUT", "simulated timeout after placement")
        return r

    def get_order(self, order_link_id):
        if order_link_id in self.get_order_raise:
            raise ExchangeError("EXCHANGE_ERROR", "simulated read failure")
        if order_link_id in self.hidden:
            return None
        return super().get_order(order_link_id)

    def cancel_order(self, order_link_id):
        if self.cancel_fail:
            raise ExchangeError("EXCHANGE_TIMEOUT", "simulated cancel timeout")
        return super().cancel_order(order_link_id)


@pytest.fixture
def faulty(settings):
    return FaultyPaper(settings, price=PAPER_PRICE)


@pytest.fixture
def fexecutor(settings, store, faulty, reporter, alerts):
    return Executor(settings, store, {"test": faulty, "live": None}, reporter, alerts)


def _long_entry(pid: str, **over) -> dict:
    d = dict(position_id=pid, leg="long", position_idx=1, qty_btc=0.002, expected_qty_btc_after=0.002)
    d.update(over)
    return make_signal(**d)


# --------------------------------------------------------------------------- #
# 취소 vs 체결 경합
# --------------------------------------------------------------------------- #
def test_filled_stop_during_cancel_is_ingested_not_discarded(executor, store, paper, alerts):
    """SL 이 체결된 뒤(아직 reconcile 전) add 가 오면: 취소 단계에서 체결을 발견해 lot 차감 + auto:sl 회신,
    새 SL 은 실제 잔량(0.002) 으로만 만든다 — 유령 수량/과대 스탑 없음."""
    pid = "race-sl"
    assert run_signal(executor, store, _long_entry(pid, stop_loss=85000))["status"] == "done"
    lot_inst = store.get_lot("test", "bybit", pid)["opened_at_ms"]
    fired = paper.set_price(84900)          # SL 체결 → 거래소 flat
    assert len(fired) == 1 and paper.positions() == {}
    seq = last_seq(store, "test")

    paper.set_price(86000)
    add = make_signal(position_id=pid, leg="long", position_idx=1, event_sequence=2, action="add", qty_btc=0.002,
                      expected_qty_btc_after=0.002, reference_price=86000)
    row = run_signal(executor, store, add)
    assert row["status"] == "done", row
    lot = store.get_lot("test", "bybit", pid)
    assert lot["status"] == "open" and lot["qty"] == pytest.approx(0.002)
    assert paper.positions()[1]["size"] == pytest.approx(0.002)
    conds = paper.open_conditional_orders(1)
    assert [(c["trigger_price"], c["qty"]) for c in conds] == [(85000.0, 0.002)]
    rs = reports_after(store, "test", seq)
    statuses = execution_statuses(rs)
    assert "rejected" not in statuses and "error" not in statuses
    auto = [r for r in rs if r["kind"] == "execution" and r["execution"]["event_id"].startswith("auto:sl:")]
    assert len(auto) == 1
    e = auto[0]["execution"]
    assert e["event_id"] == f"auto:sl:{pid}:1:L{lot_inst}"
    assert e["status"] == "filled" and e["reason_code"] == "STOP_LOSS_TRIGGERED" and e["action"] == "partial_exit"
    assert e["qty"] == pytest.approx(0.002) and e["fill_price"] == pytest.approx(84900)
    assert statuses[-1] == "snapshot"
    assert executor.reconcile("test") is True and store.is_inconsistent("test", "bybit") is False


# --------------------------------------------------------------------------- #
# 체결 뒤 보호주문 실패 / 트리거 이미 지남
# --------------------------------------------------------------------------- #
def test_entry_with_already_crossed_stop_is_closed_immediately_not_reported_rejected(executor, store, paper, alerts):
    """롱 진입인데 SL(87000) 이 현재가(86000) 위 → 거래소가 조건부 거부. 체결은 filled 로 유지하고
    보호를 즉시 reduceOnly 시장가로 실행(auto:sl full_exit). 같은 event_id 로 rejected/error 를 보내지 않는다."""
    pid = "crossed-sl"
    d = _long_entry(pid, stop_loss=87000)
    row = run_signal(executor, store, d)
    assert row["status"] == "done", row
    assert paper.positions() == {}
    lot = store.get_lot("test", "bybit", pid)
    assert lot["status"] == "closed" and lot["qty"] == 0
    assert paper.open_conditional_orders(1) == []
    rs = load_reports(store, "test")
    by_event: dict[str, list[str]] = {}
    for r in rs:
        if r["kind"] == "execution":
            by_event.setdefault(r["execution"]["event_id"], []).append(r["execution"]["status"])
    assert by_event[d["event_id"]] == ["acknowledged", "submitted", "filled"]
    auto_id = [k for k in by_event if k.startswith("auto:sl:")]
    assert len(auto_id) == 1 and by_event[auto_id[0]] == ["submitted", "filled"]
    auto = [r for r in rs if r["kind"] == "execution" and r["execution"]["event_id"] == auto_id[0]][-1]["execution"]
    assert auto["action"] == "full_exit" and auto["reason_code"] == "STOP_LOSS_TRIGGERED"
    assert auto["qty"] == pytest.approx(0.002)
    assert rs[-1]["kind"] == "snapshot" and rs[-1]["positions"] == []
    assert alerts.contains("already crossed")


def test_crossed_take_profit_level_executes_that_level_and_keeps_rest(executor, store, paper):
    """TP[0]=85000 가 이미 지난 롱(현재가 86000): 그 레벨 수량만 즉시 청산하고 SL + TP[1] 은 잔량으로 재설정."""
    pid = "crossed-tp"
    d = _long_entry(pid, qty_btc=0.004, expected_qty_btc_after=0.004, stop_loss=80000, take_profit=[85000, 90000])
    row = run_signal(executor, store, d)
    assert row["status"] == "done", row
    lot = store.get_lot("test", "bybit", pid)
    assert lot["status"] == "open" and lot["qty"] == pytest.approx(0.002)
    assert lot["protection_orders"]["tp_done"] == [0]
    assert sorted((c["trigger_price"], c["qty"]) for c in paper.open_conditional_orders(1)) == \
        [(80000.0, 0.002), (90000.0, 0.002)]
    assert paper.positions()[1]["size"] == pytest.approx(0.002)
    auto = [r["execution"] for r in load_reports(store, "test")
            if r["kind"] == "execution" and r["execution"]["event_id"].startswith("auto:tp:")]
    assert auto and auto[-1]["status"] == "filled" and auto[-1]["action"] == "partial_exit"
    assert auto[-1]["reason_code"] == "TAKE_PROFIT_TRIGGERED"


def test_protection_failure_after_fill_keeps_done_and_reconcile_self_heals(fexecutor, store, faulty, alerts):
    """체결 뒤 보호주문 생성이 거래소 오류로 실패: 신호는 done + note PROTECTION_FAILED(알림), rejected/error 회신 없음.
    reconcile 이 lot.stop_loss 로 다시 만든다."""
    pid = "prot-fail"
    faulty.conditional_errors = [ExchangeRejected(10016, "rate limit"), ExchangeRejected(10016, "rate limit")]
    row = run_signal(fexecutor, store, _long_entry(pid, stop_loss=85000))
    assert row["status"] == "done" and "PROTECTION_FAILED" in (row["note"] or "")
    assert "rejected" not in execution_statuses(load_reports(store, "test"))
    assert alerts.contains("PROTECTION_FAILED")
    lot = store.get_lot("test", "bybit", pid)
    assert lot["status"] == "open" and lot["qty"] == pytest.approx(0.002)
    assert faulty.open_conditional_orders(1) == []
    assert fexecutor.protection_missing("test") == [pid]

    assert fexecutor.reconcile("test") is True
    assert fexecutor.protection_missing("test") == []
    assert [(c["trigger_price"], c["qty"]) for c in faulty.open_conditional_orders(1)] == [(85000.0, 0.002)]
    assert store.get_lot("test", "bybit", pid)["protection_orders"]["sl"]["price"] == 85000


# --------------------------------------------------------------------------- #
# 조건부 주문 타임아웃 / 고아 스탑
# --------------------------------------------------------------------------- #
def test_conditional_timeout_after_placement_is_adopted_not_duplicated(fexecutor, store, faulty, alerts):
    pid = "cond-timeout"
    faulty.conditional_timeout_after_place = True
    row = run_signal(fexecutor, store, _long_entry(pid, stop_loss=85000))
    assert row["status"] == "done" and "PROTECTION_FAILED" not in (row["note"] or "")
    lot = store.get_lot("test", "bybit", pid)
    sl = lot["protection_orders"]["sl"]
    assert sl and sl["order_id"]
    conds = faulty.open_conditional_orders(1)
    assert len(conds) == 1 and conds[0]["order_link_id"] == sl["order_link_id"]
    assert store.get_order(sl["order_link_id"], "bybit")["status"] == "Untriggered"
    assert fexecutor.reconcile("test") is True
    assert len(faulty.open_conditional_orders(1)) == 1    # reconcile 이 중복 생성하지 않는다


def test_reconcile_cancels_orphan_protection_and_replaces_missing(executor, store, paper, alerts):
    """lot 이 참조하지 않는 우리 보호주문(고아)은 취소하고, 빠진 보호주문은 lot.stop_loss 로 다시 만든다."""
    pid = "orphan"
    run_signal(executor, store, _long_entry(pid, stop_loss=85000))
    lot = store.get_lot("test", "bybit", pid)
    old_link = lot["protection_orders"]["sl"]["order_link_id"]
    lot["protection_orders"]["sl"] = None            # 원장에서만 사라짐 (크래시/버그 상황)
    store.upsert_lot(lot)
    assert executor.protection_missing("test") == [pid]

    assert executor.reconcile("test") is True
    conds = paper.open_conditional_orders(1)
    assert len(conds) == 1 and conds[0]["order_link_id"] != old_link
    assert paper.get_order(old_link)["status"] == "Cancelled"
    assert store.get_order(old_link, "bybit")["status"] == "Cancelled"
    assert alerts.contains("orphan")
    assert executor.protection_missing("test") == []


# --------------------------------------------------------------------------- #
# 만료
# --------------------------------------------------------------------------- #
def test_signal_expired_before_execution_is_rejected_expired(executor, store, paper):
    from lake_executor.util import now_ms
    ts = now_ms()
    d = make_signal(ts=ts, expires_at_ms=ts + 300)
    assert ingest(store, d) == "new"
    time.sleep(0.4)
    assert executor.run_once() is True
    row = store.get_signal(d["event_id"], "test")
    assert row["status"] == "rejected" and row["reason_code"] == "EXPIRED"
    assert paper.positions() == {}
    assert execution_statuses(load_reports(store, "test")) == ["acknowledged", "rejected"]


# --------------------------------------------------------------------------- #
# protection_update 실패 / 같은 revision 재시도
# --------------------------------------------------------------------------- #
def test_protection_update_failure_allows_same_revision_retry(fexecutor, store, faulty, alerts):
    pid = "pu-retry"
    assert run_signal(fexecutor, store, _long_entry(pid, stop_loss=85000))["status"] == "done"
    # 새 revision 의 생성이 두 번(신호 처리 + 직후 reconcile 자가 복구) 모두 실패
    faulty.conditional_errors = [ExchangeRejected(10016, "rate limit")] * 2
    pu = make_signal(position_id=pid, leg="long", position_idx=1, event_sequence=2, action="protection_update",
                     qty_btc=None, expected_qty_btc_after=None, reference_price=None, protection_revision=2,
                     stop_loss=85500, take_profit=None)
    row = run_signal(fexecutor, store, pu)
    assert row["status"] == "error" and row["reason_code"] == "PROTECTION_FAILED"
    lot = store.get_lot("test", "bybit", pid)
    assert lot["protection_revision"] == 2 and lot["stop_loss"] == 85500
    assert lot["protection_orders"]["failed"] is True and faulty.open_conditional_orders(1) == []
    assert fexecutor.protection_missing("test") == [pid]
    rs = load_reports(store, "test")
    assert rs[-1]["kind"] == "snapshot" and rs[-1]["positions"][0]["stop_loss"] is None   # 정직한 스냅샷

    # 같은 revision 재전송 → STALE 이 아니라 멱등 재시도
    retry = make_signal(position_id=pid, leg="long", position_idx=1, event_sequence=3, action="protection_update",
                        qty_btc=None, expected_qty_btc_after=None, reference_price=None, protection_revision=2,
                        stop_loss=85500, take_profit=None)
    row = run_signal(fexecutor, store, retry)
    assert row["status"] == "done", row
    assert [(c["trigger_price"], c["qty"]) for c in faulty.open_conditional_orders(1)] == [(85500.0, 0.002)]
    assert fexecutor.protection_missing("test") == []
    # 보호주문이 살아 있으면 같은 revision 은 다시 STALE
    again = make_signal(position_id=pid, leg="long", position_idx=1, event_sequence=4, action="protection_update",
                        qty_btc=None, expected_qty_btc_after=None, reference_price=None, protection_revision=2,
                        stop_loss=85600, take_profit=None)
    assert run_signal(fexecutor, store, again)["reason_code"] == "STALE_PROTECTION_REVISION"


# --------------------------------------------------------------------------- #
# 타임아웃/불명 주문의 늦은 확인
# --------------------------------------------------------------------------- #
def test_unknown_order_after_timeout_is_verified_late_and_lot_protected(fexecutor, store, faulty, alerts):
    pid = "late-verify"
    d = _long_entry(pid, stop_loss=85000)
    link = order_link_id("test", d["event_id"])
    faulty.market_timeout_after_place = True
    faulty.hidden.add(link)                     # 주문 조회도 당장은 안 됨
    row = run_signal(fexecutor, store, d)
    assert row["status"] == "error" and row["reason_code"] == "EXCHANGE_TIMEOUT"
    assert store.get_order(link, "bybit")["status"] == "unknown"
    assert store.get_lot("test", "bybit", pid) is None
    assert faulty.positions()[1]["size"] == pytest.approx(0.002)   # 실제로는 체결됐다
    assert fexecutor.reconcile("test") is False                     # 아직 숨겨짐 → 불일치
    assert store.is_inconsistent("test", "bybit") is True

    faulty.hidden.discard(link)
    seq = last_seq(store, "test")
    assert fexecutor.reconcile("test") is True                      # 늦은 확인 → 체결 반영 → 일치
    assert store.is_inconsistent("test", "bybit") is False
    lot = store.get_lot("test", "bybit", pid)
    assert lot and lot["status"] == "open" and lot["qty"] == pytest.approx(0.002)
    assert [(c["trigger_price"], c["qty"]) for c in faulty.open_conditional_orders(1)] == [(85000.0, 0.002)]
    row = store.get_signal(d["event_id"], "test")
    assert row["status"] == "done" and "LATE_VERIFIED" in (row["note"] or "")
    assert store.get_order(link, "bybit")["status"] == "Filled"
    rs = reports_after(store, "test", seq)
    assert execution_statuses(rs)[:1] == ["filled"]
    assert rs[0]["execution"]["event_id"] == d["event_id"] and rs[0]["execution"]["qty"] == pytest.approx(0.002)
    assert alerts.contains("late fills verified")
    assert fexecutor.reconcile("test") is True                      # 두 번째 reconcile 은 다시 처리하지 않는다
    assert len(reports_after(store, "test", seq)) == len(rs) or execution_statuses(reports_after(store, "test", seq))[-1] == "snapshot"


def test_order_never_on_exchange_is_closed_absent_after_checks(fexecutor, store, faulty, alerts):
    from lake_executor import executor as exmod
    pid = "absent"
    d = _long_entry(pid, stop_loss=85000)
    link = order_link_id("test", d["event_id"])
    faulty.market_timeout_after_place = True
    faulty.hidden.add(link)
    run_signal(fexecutor, store, d)
    # 거래소에서 영원히 안 보이면(실제로는 체결됐지만 테스트상 숨김) 상한 횟수 뒤 absent
    for _ in range(exmod._UNCERTAIN_MAX_CHECKS):
        fexecutor.reconcile("test")
    assert store.get_order(link, "bybit")["status"] == "absent"
    assert alerts.contains("never appeared")


# --------------------------------------------------------------------------- #
# reconcile: 취소된 보호주문 재생성 / 거래소 오류가 스냅샷을 막지 않음
# --------------------------------------------------------------------------- #
def test_reconcile_replaces_protection_cancelled_on_exchange(executor, store, paper, alerts):
    pid = "cancelled-sl"
    run_signal(executor, store, _long_entry(pid, stop_loss=85000, take_profit=[90000]))
    old = store.get_lot("test", "bybit", pid)["protection_orders"]
    assert paper.cancel_order(old["sl"]["order_link_id"])          # 운영자가 UI 에서 취소한 상황
    assert len(paper.open_conditional_orders(1)) == 1
    assert executor.reconcile("test") is True
    assert alerts.contains("protection orders missing")
    conds = sorted((c["trigger_price"], c["qty"]) for c in paper.open_conditional_orders(1))
    assert conds == [(85000.0, 0.002), (90000.0, 0.002)]
    new = store.get_lot("test", "bybit", pid)["protection_orders"]
    assert new["sl"]["order_link_id"] != old["sl"]["order_link_id"]
    assert executor.protection_missing("test") == []


def test_reconcile_protection_read_error_alerts_but_snapshot_still_sent(fexecutor, store, faulty, alerts):
    pid = "read-err"
    run_signal(fexecutor, store, _long_entry(pid, stop_loss=85000))
    link = store.get_lot("test", "bybit", pid)["protection_orders"]["sl"]["order_link_id"]
    faulty.get_order_raise.add(link)
    seq = last_seq(store, "test")
    snap = fexecutor.snapshot_now("test")
    assert snap is not None
    assert alerts.contains("reconcile (protections)")
    rs = reports_after(store, "test", seq)
    assert execution_statuses(rs) == ["snapshot"]
    assert rs[0]["positions"][0]["stop_loss"] is None        # 확인 불가 → null (거짓 확인 없음)


# --------------------------------------------------------------------------- #
# IOC 부분 체결 / lot 종료 시 취소 실패
# --------------------------------------------------------------------------- #
def test_partially_filled_canceled_is_cancelled_with_ioc_partial_note(fexecutor, store, faulty, settings):
    pid = "ioc-partial"
    faulty.partial_ioc_qty = 0.001
    t0 = time.monotonic()
    row = run_signal(fexecutor, store, _long_entry(pid, stop_loss=85000))
    elapsed = time.monotonic() - t0
    assert elapsed < settings.fill_poll_timeout_s          # 타임아웃까지 기다리지 않는다
    assert row["status"] == "done" and row["reason_code"] is None
    assert "IOC_PARTIAL" in row["note"] and "QTY_MISMATCH" in row["note"]
    lot = store.get_lot("test", "bybit", pid)
    assert lot["qty"] == pytest.approx(0.001)
    assert [(c["trigger_price"], c["qty"]) for c in faulty.open_conditional_orders(1)] == [(85000.0, 0.001)]
    assert execution_statuses(load_reports(store, "test")) == ["acknowledged", "submitted", "partially_filled",
                                                               "cancelled", "snapshot"]


def test_close_lot_with_cancel_failure_is_closed_and_leftovers_retried(fexecutor, store, faulty, alerts):
    pid = "close-cancel-fail"
    run_signal(fexecutor, store, _long_entry(pid, stop_loss=85000, take_profit=[90000]))
    faulty.cancel_fail = True
    fx = make_signal(position_id=pid, leg="long", position_idx=1, event_sequence=2, action="full_exit", qty_btc=0.002,
                     expected_qty_btc_after=0, reference_price=None)
    row = run_signal(fexecutor, store, fx)
    assert row["status"] == "done", row
    lot = store.get_lot("test", "bybit", pid)
    assert lot["status"] == "closed" and lot["qty"] == 0
    assert lot["protection_orders"]["sl"] is not None           # 취소 못 한 항목은 남겨 재시도
    assert len(faulty.open_conditional_orders(1)) == 2
    assert alerts.contains("PROTECTION_FAILED")
    assert store.lots_with_pending_cancel("test", "bybit")[0]["position_id"] == pid
    # 거래소 flat = lot 0 → 포지션 대사는 일치(True, 스냅샷 가능). 잔여 보호주문 취소 재시도는 실패 → 알림만.
    assert fexecutor.reconcile("test") is True
    assert alerts.contains("still has live protection orders")
    assert len(faulty.open_conditional_orders(1)) == 2

    faulty.cancel_fail = False
    assert fexecutor.reconcile("test") is True
    assert faulty.open_conditional_orders(1) == []
    lot = store.get_lot("test", "bybit", pid)
    assert lot["protection_orders"]["sl"] is None and lot["protection_orders"]["tp"] == []
    assert store.lots_with_pending_cancel("test", "bybit") == []
    # 닫힌 lot 에 다시 진입 가능
    assert run_signal(fexecutor, store, _long_entry(pid, event_sequence=3))["status"] == "done"


# --------------------------------------------------------------------------- #
# 단방향 반대 레그 / auto 이벤트 ID 인스턴스 / 모의 시세 추종
# --------------------------------------------------------------------------- #
def test_one_way_mode_rejects_opposing_leg(settings_factory, alerts, fake_client):
    s = settings_factory(config_overrides={"position_mode": "one_way"})
    store = Store(s.db_path)
    try:
        paper = PaperExchange(s, price=PAPER_PRICE)
        ex = Executor(s, store, {"test": paper, "live": None}, Reporter(s, store, alerts, client=fake_client), alerts)
        assert run_signal(ex, store, make_signal(position_id="ow-long", leg="long", position_idx=0))["status"] == "done"
        row = run_signal(ex, store, make_signal(position_id="ow-short", leg="short", position_idx=0))
        assert row["status"] == "rejected" and row["reason_code"] == "OPPOSING_LEG"
        assert paper.positions()[0]["size"] == pytest.approx(0.002) and paper.positions()[0]["side"] == "Buy"
        row = run_signal(ex, store, make_signal(position_id="ow-long-2", leg="long", position_idx=0))
        assert row["status"] == "done"
    finally:
        store.close()


def test_auto_event_id_differs_across_lot_instances(executor, store, paper):
    pid = "reentry"
    run_signal(executor, store, _long_entry(pid, stop_loss=85000))
    paper.set_price(84900)
    assert executor.reconcile("test") is True
    first = [r["execution"]["event_id"] for r in load_reports(store, "test")
             if r["kind"] == "execution" and r["execution"]["event_id"].startswith("auto:sl:")]
    paper.set_price(86000)
    run_signal(executor, store, _long_entry(pid, event_sequence=2, stop_loss=85000))   # 같은 pid, 같은 revision 1
    paper.set_price(84900)
    assert executor.reconcile("test") is True
    second = [r["execution"]["event_id"] for r in load_reports(store, "test")
              if r["kind"] == "execution" and r["execution"]["event_id"].startswith("auto:sl:")]
    assert len(first) == 1 and len(second) == 2 and second[0] != second[1]
    assert all(e.startswith(f"auto:sl:{pid}:1:L") for e in second)


def test_paper_price_follows_reference_price_for_test_signals(executor, store, paper):
    """lake 의 TEST 신호가 실제 시세(reference_price) 로 오면 모의 시세가 따라가 SLIPPAGE_GUARD 로 죽지 않는다."""
    row = run_signal(executor, store, make_signal(reference_price=110000, stop_loss=112000))
    assert row["status"] == "done"
    assert paper.last_price() == pytest.approx(110000)
    assert [(c["trigger_price"], c["qty"]) for c in paper.open_conditional_orders(2)] == [(112000.0, 0.002)]


def test_protection_not_found_requires_two_strikes_before_replacing(fexecutor, store, faulty, alerts):
    """조회 지연/필터 차이로 한 번 안 보인 보호주문은 유지하고, 두 번 연속 없을 때만 항목을 지우고 다시 만든다."""
    pid = "absent-twice"
    run_signal(fexecutor, store, _long_entry(pid, stop_loss=85000))
    link = store.get_lot("test", "bybit", pid)["protection_orders"]["sl"]["order_link_id"]
    faulty.hidden.add(link)
    assert fexecutor.reconcile("test") is True
    assert store.get_lot("test", "bybit", pid)["protection_orders"]["sl"]["order_link_id"] == link   # 1회: 유지
    assert len(faulty.open_conditional_orders(1)) == 1
    assert fexecutor.reconcile("test") is True
    lot = store.get_lot("test", "bybit", pid)
    assert lot["protection_orders"]["sl"]["order_link_id"] != link                        # 2회: 교체
    # 항목은 absent 로 지워지고, 같은 reconcile 의 고아 정리가 거래소에 남아 있던 옛 주문을 취소한다 (하나만 남는다)
    assert store.get_order(link, "bybit")["status"] in ("absent", "Cancelled")
    faulty.hidden.discard(link)
    assert faulty.get_order(link)["status"] == "Cancelled"
    assert len(faulty.open_conditional_orders(1)) == 1
