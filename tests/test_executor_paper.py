"""executor.py × PaperExchange (ARCHITECTURE.md §1, §3, §9).

entry→add→partial_exit→protection_update→full_exit 전체 흐름, lot 수량/평균가, 보호주문 생성·재설정·취소,
QTY_EXCEEDS_LOT / STALE_PROTECTION_REVISION / POSITION_EXISTS / LIVE_DISABLED / HALT,
SL 트리거 후 reconcile 이 lot 을 줄이고 auto 이벤트 회신을 만드는지, 불일치 시 스냅샷 생략.
회신 본문은 계약(lake_execution_contract.json) 필드 단위로 검사한다.
"""
from __future__ import annotations

import re

import pytest

from lake_executor import ops
from lake_executor.executor import Executor
from lake_executor.reporter import Reporter
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

ORDER_ID_RE = re.compile(r"^o-[0-9a-f]{24}$")
FILL_ID_RE = re.compile(r"^f-[0-9a-f]{24}$")
ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
LINK_ID_RE = re.compile(r"^lk[0-9a-f]{32}$")   # util.order_link_id 형식 (Bybit orderLinkId ≤ 36자)

HEADER_KEYS = {"schema_version", "report_id", "mode", "sequence", "ts", "observed_at_ms", "exchange", "category",
               "symbol", "kind"}
EXECUTION_KEYS = {"position_id", "strategy", "leg", "position_idx", "event_id", "action", "status", "qty",
                  "fill_price", "order_id", "fill_id", "reason_code"}
SNAPSHOT_KEYS = HEADER_KEYS | {"complete", "account_scope", "positions"}
POSITION_KEYS = {"position_id", "strategy", "leg", "position_idx", "qty", "entry_price", "mark_price", "stop_loss",
                 "take_profit", "updated_at_ms"}
STEP = 0.001


# --------------------------------------------------------------------------- #
# 계약 검사 헬퍼
# --------------------------------------------------------------------------- #
def check_header(r: dict, mode: str = "test") -> None:
    assert HEADER_KEYS <= set(r)
    assert r["schema_version"] == 1
    assert ID_RE.match(r["report_id"])
    assert r["mode"] == mode
    assert isinstance(r["sequence"], int) and r["sequence"] >= 1
    assert isinstance(r["ts"], int) and r["ts"] >= 1
    assert isinstance(r["observed_at_ms"], int) and 1 <= r["observed_at_ms"] <= r["ts"]
    assert r["exchange"] == "Bybit" and r["category"] == "linear" and r["symbol"] == "BTCUSDT"
    assert r["kind"] in ("execution", "snapshot")


def check_execution(r: dict, *, event_id: str, position_id: str, action: str, status: str, leg: str,
                    position_idx: int, strategy: str = "overheat", reason_code: str | None = None) -> dict:
    check_header(r)
    assert r["kind"] == "execution"
    assert set(r) == HEADER_KEYS | {"execution"}
    e = r["execution"]
    assert set(e) == EXECUTION_KEYS
    assert e["event_id"] == event_id
    assert e["position_id"] == position_id
    assert e["strategy"] == strategy
    assert e["leg"] == leg
    assert e["position_idx"] == position_idx
    assert e["action"] == action
    assert e["status"] == status
    assert e["reason_code"] == reason_code
    if status in ("partially_filled", "filled"):
        assert isinstance(e["qty"], float) and e["qty"] > 0
        assert isinstance(e["fill_price"], float) and e["fill_price"] > 0
        assert FILL_ID_RE.match(e["fill_id"])
        assert ORDER_ID_RE.match(e["order_id"])
    else:
        assert e["qty"] is None and e["fill_price"] is None and e["fill_id"] is None
    if e["order_id"] is not None:
        assert ORDER_ID_RE.match(e["order_id"])
    if status == "protection_updated":
        assert action == "protection_update"
    return e


def check_snapshot(r: dict, expected_positions: list[dict]) -> list[dict]:
    """expected_positions: [{position_id, leg, position_idx, qty, entry_price, stop_loss, take_profit}]"""
    check_header(r)
    assert r["kind"] == "snapshot"
    assert set(r) == SNAPSHOT_KEYS
    assert r["complete"] is True
    assert r["account_scope"] == "lake_dedicated_BTCUSDT"
    positions = r["positions"]
    assert isinstance(positions, list) and len(positions) <= 100
    ids = [p["position_id"] for p in positions]
    assert len(ids) == len(set(ids))
    assert len(positions) == len(expected_positions)
    by_id = {p["position_id"]: p for p in positions}
    for exp in expected_positions:
        p = by_id[exp["position_id"]]
        assert set(p) == POSITION_KEYS
        assert p["strategy"] in ("basic", "overheat", "range")
        assert p["leg"] == exp["leg"] and p["position_idx"] == exp["position_idx"]
        assert p["qty"] == pytest.approx(exp["qty"]) and p["qty"] > 0
        assert p["entry_price"] == pytest.approx(exp["entry_price"]) and p["entry_price"] > 0
        assert p["mark_price"] is None or p["mark_price"] > 0
        assert p["stop_loss"] == exp["stop_loss"]
        if exp["take_profit"] is None:
            assert p["take_profit"] is None
        else:
            assert p["take_profit"] == pytest.approx(exp["take_profit"])
        assert isinstance(p["updated_at_ms"], int) and 1 <= p["updated_at_ms"] <= r["observed_at_ms"]
    return positions


def open_protections(paper, idx: int) -> dict:
    """paper 의 열린 조건부 주문을 {'sl': [...], 'tp': [...]} 로 — SL 은 lot 과 반대 방향(손실 쪽) 트리거."""
    return paper.open_conditional_orders(idx)


# --------------------------------------------------------------------------- #
# 전체 생명주기
# --------------------------------------------------------------------------- #
def test_full_lifecycle_on_paper(executor, store, paper, alerts):
    pid = "position-life-1"
    base = dict(position_id=pid, leg="short", position_idx=2)

    # ---- 1) entry 0.002 @86000, SL 88000, TP [84000]
    e1 = make_signal(**base, event_sequence=1, action="entry", qty_btc=0.002, expected_qty_btc_after=0.002,
                     reference_price=86000, stop_loss=88000, take_profit=[84000])
    row = run_signal(executor, store, e1)
    assert row["status"] == "done" and row["reason_code"] is None, row

    lot = store.get_lot("test", pid)
    assert lot["status"] == "open"
    assert lot["qty"] == pytest.approx(0.002)
    assert lot["avg_entry"] == pytest.approx(86000)
    assert lot["stop_loss"] == 88000 and lot["take_profit"] == [84000.0]
    assert lot["protection_revision"] == 1
    po = lot["protection_orders"]
    assert po["sl"] and LINK_ID_RE.match(po["sl"]["order_link_id"])
    assert po["sl"]["qty"] == pytest.approx(0.002) and po["sl"]["price"] == 88000
    assert len(po["tp"]) == 1 and po["tp"][0]["price"] == 84000 and po["tp"][0]["qty"] == pytest.approx(0.002)
    assert paper.positions()[2]["size"] == pytest.approx(0.002)
    assert paper.positions()[2]["side"] == "Sell"
    conds = open_protections(paper, 2)
    assert sorted((c["trigger_price"], c["qty"]) for c in conds) == [(84000.0, 0.002), (88000.0, 0.002)]
    assert all(c["side"] == "Buy" for c in conds)
    # 시장가 주문 멱등 키
    assert store.get_order(order_link_id("test", e1["event_id"]))["status"] == "Filled"

    rs = load_reports(store, "test")
    assert execution_statuses(rs) == ["acknowledged", "submitted", "filled", "snapshot"]
    ack = check_execution(rs[0], event_id=e1["event_id"], position_id=pid, action="entry", status="acknowledged",
                          leg="short", position_idx=2)
    assert ack["order_id"] is None
    sub = check_execution(rs[1], event_id=e1["event_id"], position_id=pid, action="entry", status="submitted",
                          leg="short", position_idx=2)
    assert sub["order_id"] == Reporter.pseudonym("o", "porder-1")
    fill = check_execution(rs[2], event_id=e1["event_id"], position_id=pid, action="entry", status="filled",
                           leg="short", position_idx=2)
    assert fill["qty"] == pytest.approx(0.002) and fill["fill_price"] == pytest.approx(86000)
    assert fill["order_id"] == sub["order_id"]
    assert fill["fill_id"] == Reporter.pseudonym("f", "pexec-1")
    assert "porder-1" not in str(rs) and "pexec-1" not in str(rs)
    check_snapshot(rs[3], [dict(position_id=pid, leg="short", position_idx=2, qty=0.002, entry_price=86000,
                                stop_loss=88000.0, take_profit=[84000.0])])
    assert rs[3]["positions"][0]["mark_price"] == pytest.approx(PAPER_PRICE)
    seq_after_entry = last_seq(store, "test")

    # ---- 2) add 0.002 @87000 → qty 0.004, avg 86500, 보호주문 수량 재설정
    paper.set_price(87000)
    e2 = make_signal(**base, event_sequence=2, action="add", qty_btc=0.002, expected_qty_btc_after=0.004,
                     reference_price=87000)
    row = run_signal(executor, store, e2)
    assert row["status"] == "done" and row["reason_code"] is None
    lot = store.get_lot("test", pid)
    assert lot["qty"] == pytest.approx(0.004)
    assert lot["avg_entry"] == pytest.approx(86500)
    assert lot["protection_revision"] == 1
    assert lot["stop_loss"] == 88000 and lot["take_profit"] == [84000.0]
    conds = open_protections(paper, 2)
    assert sorted((c["trigger_price"], c["qty"]) for c in conds) == [(84000.0, 0.004), (88000.0, 0.004)]
    # 이전 보호주문은 취소됨
    assert paper.get_order(po["sl"]["order_link_id"])["status"] == "Cancelled"
    assert paper.get_order(po["tp"][0]["order_link_id"])["status"] == "Cancelled"
    rs = reports_after(store, "test", seq_after_entry)
    assert execution_statuses(rs) == ["acknowledged", "submitted", "filled", "snapshot"]
    fill = check_execution(rs[2], event_id=e2["event_id"], position_id=pid, action="add", status="filled",
                           leg="short", position_idx=2)
    assert fill["qty"] == pytest.approx(0.002) and fill["fill_price"] == pytest.approx(87000)
    check_snapshot(rs[3], [dict(position_id=pid, leg="short", position_idx=2, qty=0.004, entry_price=86500,
                                stop_loss=88000.0, take_profit=[84000.0])])
    seq_mark = last_seq(store, "test")

    # ---- 3) partial_exit 0.001 → qty 0.003 (reduceOnly), 보호주문 재설정
    e3 = make_signal(**base, event_sequence=3, action="partial_exit", qty_btc=0.001, expected_qty_btc_after=0.003,
                     reference_price=None)
    row = run_signal(executor, store, e3)
    assert row["status"] == "done" and row["reason_code"] is None
    lot = store.get_lot("test", pid)
    assert lot["qty"] == pytest.approx(0.003)
    assert lot["avg_entry"] == pytest.approx(86500)   # 청산은 평균가를 바꾸지 않는다
    assert paper.positions()[2]["size"] == pytest.approx(0.003)
    o = store.get_order(order_link_id("test", e3["event_id"]))
    assert o["reduce_only"] == 1 and o["side"] == "Buy" and o["status"] == "Filled"
    conds = open_protections(paper, 2)
    assert sorted((c["trigger_price"], c["qty"]) for c in conds) == [(84000.0, 0.003), (88000.0, 0.003)]
    rs = reports_after(store, "test", seq_mark)
    assert execution_statuses(rs) == ["acknowledged", "submitted", "filled", "snapshot"]
    fill = check_execution(rs[2], event_id=e3["event_id"], position_id=pid, action="partial_exit", status="filled",
                           leg="short", position_idx=2)
    assert fill["qty"] == pytest.approx(0.001)
    check_snapshot(rs[3], [dict(position_id=pid, leg="short", position_idx=2, qty=0.003, entry_price=86500,
                                stop_loss=88000.0, take_profit=[84000.0])])
    seq_mark = last_seq(store, "test")

    # ---- 4) partial_exit 초과 수량 → QTY_EXCEEDS_LOT (주문 없음)
    e4 = make_signal(**base, event_sequence=4, action="partial_exit", qty_btc=0.01, expected_qty_btc_after=None,
                     reference_price=None)
    row = run_signal(executor, store, e4)
    assert row["status"] == "rejected" and row["reason_code"] == "QTY_EXCEEDS_LOT"
    assert store.get_lot("test", pid)["qty"] == pytest.approx(0.003)
    assert paper.positions()[2]["size"] == pytest.approx(0.003)
    assert store.get_order(order_link_id("test", e4["event_id"])) is None
    rs = reports_after(store, "test", seq_mark)
    assert execution_statuses(rs) == ["acknowledged", "rejected"]
    check_execution(rs[1], event_id=e4["event_id"], position_id=pid, action="partial_exit", status="rejected",
                    leg="short", position_idx=2, reason_code="QTY_EXCEEDS_LOT")
    seq_mark = last_seq(store, "test")

    # ---- 5) protection_update rev 2: SL 89000, TP [84000, 83000] (0.003 → 0.001 + 0.002)
    e5 = make_signal(**base, event_sequence=5, action="protection_update", qty_btc=None, expected_qty_btc_after=None,
                     reference_price=None, protection_revision=2, stop_loss=89000, take_profit=[84000, 83000])
    row = run_signal(executor, store, e5)
    assert row["status"] == "done" and row["reason_code"] is None
    lot = store.get_lot("test", pid)
    assert lot["protection_revision"] == 2
    assert lot["stop_loss"] == 89000 and lot["take_profit"] == [84000.0, 83000.0]
    assert lot["qty"] == pytest.approx(0.003)
    po = lot["protection_orders"]
    assert po["sl"]["price"] == 89000 and po["sl"]["qty"] == pytest.approx(0.003)
    assert [(t["price"], t["qty"]) for t in po["tp"]] == [(84000.0, pytest.approx(0.001)), (83000.0, pytest.approx(0.002))]
    links = [po["sl"]["order_link_id"]] + [t["order_link_id"] for t in po["tp"]]
    assert all(LINK_ID_RE.match(l) for l in links) and len(set(links)) == 3
    assert all(store.get_order(l)["status"] == "Untriggered" and store.get_order(l)["purpose"] in ("sl", "tp")
               for l in links)
    conds = open_protections(paper, 2)
    assert sorted((c["trigger_price"], c["qty"]) for c in conds) == [(83000.0, 0.002), (84000.0, 0.001), (89000.0, 0.003)]
    rs = reports_after(store, "test", seq_mark)
    assert execution_statuses(rs) == ["acknowledged", "protection_updated", "snapshot"]
    check_execution(rs[1], event_id=e5["event_id"], position_id=pid, action="protection_update",
                    status="protection_updated", leg="short", position_idx=2)
    check_snapshot(rs[2], [dict(position_id=pid, leg="short", position_idx=2, qty=0.003, entry_price=86500,
                                stop_loss=89000.0, take_profit=[84000.0, 83000.0])])
    seq_mark = last_seq(store, "test")

    # ---- 6) 같은 revision 재전송 → STALE_PROTECTION_REVISION (보호주문 유지)
    e6 = make_signal(**base, event_sequence=6, action="protection_update", qty_btc=None, expected_qty_btc_after=None,
                     reference_price=None, protection_revision=2, stop_loss=90000, take_profit=None)
    row = run_signal(executor, store, e6)
    assert row["status"] == "rejected" and row["reason_code"] == "STALE_PROTECTION_REVISION"
    lot = store.get_lot("test", pid)
    assert lot["stop_loss"] == 89000 and lot["protection_revision"] == 2
    assert len(open_protections(paper, 2)) == 3
    rs = reports_after(store, "test", seq_mark)
    assert execution_statuses(rs) == ["acknowledged", "rejected"]
    check_execution(rs[1], event_id=e6["event_id"], position_id=pid, action="protection_update", status="rejected",
                    leg="short", position_idx=2, reason_code="STALE_PROTECTION_REVISION")
    seq_mark = last_seq(store, "test")

    # ---- 7) full_exit → lot 잔량(0.003)만 reduceOnly 청산, 보호주문 전부 취소, lot closed, 스냅샷 []
    paper.place_market("Sell", 0.005, 2, False, "other-strategy-lot")   # 같은 레그의 다른 몫 (심볼 전량 청산 금지 검증)
    store_lot_before = store.get_lot("test", pid)
    assert store_lot_before["qty"] == pytest.approx(0.003)
    e7 = make_signal(**base, event_sequence=7, action="full_exit", qty_btc=0.003, expected_qty_btc_after=0,
                     reference_price=None)
    row = run_signal(executor, store, e7)
    assert row["status"] == "done" and row["reason_code"] is None, row
    lot = store.get_lot("test", pid)
    assert lot["status"] == "closed" and lot["qty"] == 0 and lot["closed_at_ms"]
    assert lot["protection_orders"]["sl"] is None and lot["protection_orders"]["tp"] == []
    assert open_protections(paper, 2) == []
    assert paper.positions()[2]["size"] == pytest.approx(0.005)   # 다른 몫은 건드리지 않았다
    o = store.get_order(order_link_id("test", e7["event_id"]))
    assert o["reduce_only"] == 1 and o["qty"] == pytest.approx(0.003)
    rs = reports_after(store, "test", seq_mark)
    # 다른 몫 때문에 거래소 합계가 lot 합과 달라 스냅샷은 생략된다 (reconcile 불일치)
    assert execution_statuses(rs) == ["acknowledged", "submitted", "filled"]
    fill = check_execution(rs[2], event_id=e7["event_id"], position_id=pid, action="full_exit", status="filled",
                           leg="short", position_idx=2)
    assert fill["qty"] == pytest.approx(0.003)
    assert store.is_inconsistent("test") is True
    # 다른 몫을 정리하면 다시 일치 → 빈 스냅샷(flat) 가능
    paper.place_market("Buy", 0.005, 2, True, "other-strategy-close")
    seq_mark = last_seq(store, "test")
    snap = executor.snapshot_now("test")
    assert snap is not None
    assert store.is_inconsistent("test") is False
    rs = reports_after(store, "test", seq_mark)
    assert execution_statuses(rs) == ["snapshot"]
    check_snapshot(rs[0], [])

    # ---- 전체 sequence 연속 1..n, observed 단조
    all_rs = load_reports(store, "test")
    assert [r["sequence"] for r in all_rs] == list(range(1, len(all_rs) + 1))
    obs = [r["observed_at_ms"] for r in all_rs]
    assert obs == sorted(obs)
    # 같은 event_id 의 회신들은 정체성(position/strategy/leg/action) 이 바뀌지 않는다
    ident: dict[str, tuple] = {}
    for r in all_rs:
        if r["kind"] != "execution":
            continue
        e = r["execution"]
        key = (e["position_id"], e["strategy"], e["leg"], e["position_idx"], e["action"])
        assert ident.setdefault(e["event_id"], key) == key

    # ---- 8) closed lot 에 다시 entry 가능 (POSITION_EXISTS 아님)
    e8 = make_signal(**base, event_sequence=8, action="entry", qty_btc=0.001, expected_qty_btc_after=0.001,
                     reference_price=87000)
    row = run_signal(executor, store, e8)
    assert row["status"] == "done"
    assert store.get_lot("test", pid)["status"] == "open"


# --------------------------------------------------------------------------- #
# 게이트/가드
# --------------------------------------------------------------------------- #
def test_entry_on_open_lot_is_position_exists(executor, store, paper):
    pid = "position-exists"
    run_signal(executor, store, make_signal(position_id=pid, event_sequence=1))
    seq = last_seq(store, "test")
    d = make_signal(position_id=pid, event_sequence=2)
    row = run_signal(executor, store, d)
    assert row["status"] == "rejected" and row["reason_code"] == "POSITION_EXISTS"
    assert store.get_lot("test", pid)["qty"] == pytest.approx(0.002)
    assert paper.positions()[2]["size"] == pytest.approx(0.002)
    rs = reports_after(store, "test", seq)
    assert execution_statuses(rs) == ["acknowledged", "rejected"]
    assert rs[1]["execution"]["reason_code"] == "POSITION_EXISTS"


def test_add_exit_protection_without_lot_is_position_not_found(executor, store):
    pid = "position-missing"
    for i, (action, extra) in enumerate([
        ("add", dict(qty_btc=0.001)),
        ("partial_exit", dict(qty_btc=0.001)),
        ("full_exit", dict(qty_btc=0.001)),
        ("protection_update", dict(qty_btc=None, protection_revision=2, stop_loss=1)),
    ], start=1):
        d = make_signal(position_id=pid, event_sequence=i, action=action, expected_qty_btc_after=None,
                        reference_price=None, **extra)
        row = run_signal(executor, store, d)
        assert row["status"] == "rejected" and row["reason_code"] == "POSITION_NOT_FOUND", (action, row)
    rs = load_reports(store, "test")
    assert [r["execution"]["reason_code"] for r in rs if r["execution"]["status"] == "rejected"] == \
        ["POSITION_NOT_FOUND"] * 4


def test_quantity_guards(executor, store, paper, settings_factory, alerts):
    executor.follow_reference_price_on_paper = False   # 모의 시세를 고정해 슬리피지 가드를 검증
    # QTY_BELOW_MIN (0.0005 → step 0.001 내림 → 0)
    row = run_signal(executor, store, make_signal(qty_btc=0.0005, expected_qty_btc_after=None))
    assert row["reason_code"] == "QTY_BELOW_MIN"
    # QTY_LIMIT (> max_order_qty_btc 0.05)
    row = run_signal(executor, store, make_signal(qty_btc=0.051, expected_qty_btc_after=None))
    assert row["reason_code"] == "QTY_LIMIT"
    # SLIPPAGE_GUARD (reference 80000 vs 86000 = 7.5% > 1.5%)
    row = run_signal(executor, store, make_signal(reference_price=80000))
    assert row["reason_code"] == "SLIPPAGE_GUARD"
    assert paper.positions() == {}
    # reference_price null → 슬리피지 가드 생략
    row = run_signal(executor, store, make_signal(reference_price=None))
    assert row["status"] == "done"


def test_leg_limit(settings_factory, alerts, fake_client):
    from lake_executor.exchange import PaperExchange
    from lake_executor.store import Store
    s = settings_factory(config_overrides={"guards": {"max_leg_qty_btc": 0.003}})
    store = Store(s.db_path)
    try:
        paper = PaperExchange(s, price=PAPER_PRICE)
        reporter = Reporter(s, store, alerts, client=fake_client)
        ex = Executor(s, store, {"test": paper, "live": None}, reporter, alerts)
        assert run_signal(ex, store, make_signal(position_id="leg-a"))["status"] == "done"          # 0.002
        row = run_signal(ex, store, make_signal(position_id="leg-b"))                                # +0.002 > 0.003
        assert row["status"] == "rejected" and row["reason_code"] == "LEG_LIMIT"
        # 반대 레그는 별도 합계
        row = run_signal(ex, store, make_signal(position_id="leg-c", leg="long", position_idx=1))
        assert row["status"] == "done"
    finally:
        store.close()


def test_expected_qty_mismatch_is_noted_and_alerted(executor, store, alerts):
    d = make_signal(qty_btc=0.002, expected_qty_btc_after=0.005)
    row = run_signal(executor, store, d)
    assert row["status"] == "done"               # 회신 status 는 실제 체결 그대로
    assert "QTY_MISMATCH" in (row["note"] or "")
    assert alerts.contains("QTY_MISMATCH")
    rs = load_reports(store, "test")
    assert execution_statuses(rs) == ["acknowledged", "submitted", "filled", "snapshot"]


def test_halt_file_rejects_new_signals_but_keeps_protections(executor, store, paper, settings, alerts):
    pid = "position-halt"
    run_signal(executor, store, make_signal(position_id=pid, event_sequence=1, stop_loss=88000))
    assert len(paper.open_conditional_orders(2)) == 1
    seq = last_seq(store, "test")

    ops.halt(settings, "test")
    d = make_signal(position_id=pid, event_sequence=2, action="add", qty_btc=0.001, expected_qty_btc_after=0.003)
    row = run_signal(executor, store, d)
    assert row["status"] == "rejected" and row["reason_code"] == "OPERATOR_HALT"
    assert store.get_lot("test", pid)["qty"] == pytest.approx(0.002)
    assert len(paper.open_conditional_orders(2)) == 1   # 기존 보호주문 유지
    rs = reports_after(store, "test", seq)
    assert execution_statuses(rs) == ["acknowledged", "rejected"]
    check_execution(rs[1], event_id=d["event_id"], position_id=pid, action="add", status="rejected",
                    leg="short", position_idx=2, reason_code="OPERATOR_HALT")

    ops.resume(settings)
    d2 = make_signal(position_id=pid, event_sequence=3, action="add", qty_btc=0.001, expected_qty_btc_after=0.003)
    assert run_signal(executor, store, d2)["status"] == "done"
    assert store.get_lot("test", pid)["qty"] == pytest.approx(0.003)


def test_live_signal_with_live_disabled_is_rejected_live_disabled(executor, store, paper, settings):
    assert settings.live_execution_possible() == (False, "LIVE_DISABLED")
    d = make_signal(mode="live", position_id="position-live-1")
    row = run_signal(executor, store, d)
    assert row["status"] == "rejected" and row["reason_code"] == "LIVE_DISABLED"
    assert paper.positions() == {}                       # test 거래소도 건드리지 않는다
    assert store.get_lot("live", "position-live-1") is None
    assert load_reports(store, "test") == []
    rs = load_reports(store, "live")
    assert execution_statuses(rs) == ["acknowledged", "rejected"]
    check_header(rs[0], mode="live")
    e = rs[1]["execution"]
    assert e["status"] == "rejected" and e["reason_code"] == "LIVE_DISABLED"
    assert e["qty"] is None and e["fill_price"] is None and e["fill_id"] is None and e["order_id"] is None
    assert [r["sequence"] for r in rs] == [1, 2]       # live sequence 는 test 와 독립


def test_test_record_only_sends_acknowledged_only(settings, store, reporter, alerts):
    ex = Executor(settings, store, {"test": None, "live": None}, reporter, alerts)
    d = make_signal()
    row = run_signal(ex, store, d)
    assert row["status"] == "done" and row["reason_code"] == "TEST_RECORD_ONLY"
    rs = load_reports(store, "test")
    assert execution_statuses(rs) == ["acknowledged"]
    assert store.get_lot("test", d["position_id"]) is None


# --------------------------------------------------------------------------- #
# reconcile: 보호주문 트리거 / 불일치
# --------------------------------------------------------------------------- #
def test_stop_loss_trigger_closes_lot_via_reconcile(executor, store, paper, alerts):
    pid = "position-sl"
    d = make_signal(position_id=pid, leg="long", position_idx=1, qty_btc=0.002, expected_qty_btc_after=0.002,
                    stop_loss=85000, take_profit=[90000])
    assert run_signal(executor, store, d)["status"] == "done"
    assert paper.positions()[1]["size"] == pytest.approx(0.002)
    lot_inst = store.get_lot("test", pid)["opened_at_ms"]
    seq = last_seq(store, "test")

    fired = paper.set_price(84900)
    assert len(fired) == 1 and fired[0]["status"] == "Filled" and fired[0]["trigger_price"] == 85000
    assert paper.positions() == {}

    assert executor.reconcile("test") is True
    lot = store.get_lot("test", pid)
    assert lot["status"] == "closed" and lot["qty"] == 0
    assert lot["protection_orders"]["sl"] is None and lot["protection_orders"]["tp"] == []
    assert paper.open_conditional_orders(1) == []       # 형제 TP 취소
    assert store.is_inconsistent("test") is False
    assert alerts.contains("STOP_LOSS_TRIGGERED")

    rs = reports_after(store, "test", seq)
    assert execution_statuses(rs) == ["filled"]
    e = check_execution(rs[0], event_id=f"auto:sl:{pid}:1:L{lot_inst}", position_id=pid, action="full_exit", status="filled",
                        leg="long", position_idx=1, reason_code="STOP_LOSS_TRIGGERED")
    assert e["qty"] == pytest.approx(0.002) and e["fill_price"] == pytest.approx(84900)
    assert e["order_id"] == Reporter.pseudonym("o", fired[0]["order_id"])

    # 스냅샷 경로(reconcile → snapshot): flat 스냅샷이 나간다
    snap = executor.snapshot_now("test")
    assert snap is not None and snap["kind"] == "snapshot"
    rs = reports_after(store, "test", seq)
    assert execution_statuses(rs) == ["filled", "snapshot"]
    check_snapshot(rs[1], [])
    # 두 번째 reconcile 은 같은 체결을 다시 회신하지 않는다
    assert executor.reconcile("test") is True
    assert execution_statuses(reports_after(store, "test", seq)) == ["filled", "snapshot"]


def test_take_profit_partial_trigger_reduces_lot_and_resets_protections(executor, store, paper, alerts):
    pid = "position-tp"
    d = make_signal(position_id=pid, leg="long", position_idx=1, qty_btc=0.004, expected_qty_btc_after=0.004,
                    stop_loss=85000, take_profit=[87000, 88000])
    assert run_signal(executor, store, d)["status"] == "done"
    assert sorted((c["trigger_price"], c["qty"]) for c in paper.open_conditional_orders(1)) == \
        [(85000.0, 0.004), (87000.0, 0.002), (88000.0, 0.002)]
    lot_inst = store.get_lot("test", pid)["opened_at_ms"]
    seq = last_seq(store, "test")

    fired = paper.set_price(87000)
    assert len(fired) == 1 and fired[0]["trigger_price"] == 87000
    snap = executor.snapshot_now("test")
    assert snap is not None

    lot = store.get_lot("test", pid)
    assert lot["status"] == "open" and lot["qty"] == pytest.approx(0.002)
    assert lot["protection_orders"]["tp_done"] == [0]
    # 남은 수량으로 SL + TP[1] 재설정 (완료된 TP[0] 은 다시 만들지 않는다)
    assert sorted((c["trigger_price"], c["qty"]) for c in paper.open_conditional_orders(1)) == \
        [(85000.0, 0.002), (88000.0, 0.002)]
    assert paper.positions()[1]["size"] == pytest.approx(0.002)

    rs = reports_after(store, "test", seq)
    assert execution_statuses(rs) == ["filled", "snapshot"]
    e = check_execution(rs[0], event_id=f"auto:tp:{pid}:1:0:L{lot_inst}", position_id=pid, action="partial_exit",
                        status="filled", leg="long", position_idx=1, reason_code="TAKE_PROFIT_TRIGGERED")
    assert e["qty"] == pytest.approx(0.002) and e["fill_price"] == pytest.approx(87000)
    check_snapshot(rs[1], [dict(position_id=pid, leg="long", position_idx=1, qty=0.002, entry_price=86000,
                                stop_loss=85000.0, take_profit=[88000.0])])
    assert alerts.contains("TAKE_PROFIT_TRIGGERED")


def test_reconcile_mismatch_sets_inconsistent_and_skips_snapshot(executor, store, paper, alerts):
    pid = "position-recon"
    assert run_signal(executor, store, make_signal(position_id=pid, event_sequence=1))["status"] == "done"
    assert executor.reconcile("test") is True
    seq = last_seq(store, "test")

    # 거래소 포지션을 수동으로 바꿔 lot 합(0.002) 과 어긋나게 만든다
    paper.place_market("Sell", 0.001, 2, False, "manual-extra")
    assert paper.positions()[2]["size"] == pytest.approx(0.003)

    assert executor.reconcile("test") is False
    assert store.is_inconsistent("test") is True
    assert "idx2" in store.get_meta("inconsistent_note:test", "")
    assert alerts.contains("RECONCILE_REQUIRED")
    assert executor.snapshot_now("test") is None
    assert reports_after(store, "test", seq) == []        # 스냅샷 생략

    # 불일치 중 entry/add 는 거부, 청산류는 허용
    row = run_signal(executor, store, make_signal(position_id="position-recon-2"))
    assert row["status"] == "rejected" and row["reason_code"] == "RECONCILE_REQUIRED"
    row = run_signal(executor, store, make_signal(position_id=pid, event_sequence=2, action="add", qty_btc=0.001,
                                                  expected_qty_btc_after=None))
    assert row["reason_code"] == "RECONCILE_REQUIRED"
    row = run_signal(executor, store, make_signal(position_id=pid, event_sequence=3, action="partial_exit",
                                                  qty_btc=0.001, expected_qty_btc_after=0.001, reference_price=None))
    assert row["status"] == "done"
    assert store.get_lot("test", pid)["qty"] == pytest.approx(0.001)
    snaps = [r for r in reports_after(store, "test", seq) if r["kind"] == "snapshot"]
    assert snaps == []                                     # 여전히 불일치 → 스냅샷 없음

    # 수동 포지션을 정리하면 다시 일치
    paper.place_market("Buy", 0.001, 2, True, "manual-fix")
    seq = last_seq(store, "test")
    assert executor.reconcile("test") is True
    assert store.is_inconsistent("test") is False
    snap = executor.snapshot_now("test")
    assert snap is not None
    rs = reports_after(store, "test", seq)
    assert execution_statuses(rs) == ["snapshot"]
    check_snapshot(rs[0], [dict(position_id=pid, leg="short", position_idx=2, qty=0.001, entry_price=86000,
                                stop_loss=None, take_profit=[])])


def test_reconcile_without_exchange_returns_false(executor, store):
    assert executor.reconcile("live") is False
    assert executor.snapshot_now("live") is None
    assert load_reports(store, "live") == []


def test_entry_without_protection_reports_confirmed_no_tp(executor, store, paper):
    """SL/TP 없는 entry: stop_loss null, take_profit [] (확인된 익절 없음)."""
    d = make_signal(stop_loss=None, take_profit=None)
    run_signal(executor, store, d)
    rs = load_reports(store, "test")
    snap = rs[-1]
    assert snap["kind"] == "snapshot"
    p = snap["positions"][0]
    assert p["stop_loss"] is None and p["take_profit"] == []
    assert paper.open_conditional_orders(2) == []


def test_recover_processing_closes_unknown_signal(executor, store, paper, alerts):
    """재시작 복구: processing 상태로 남은 신호의 주문이 거래소에 없으면 error/UNKNOWN_STATE 로 닫는다 (재실행 금지)."""
    d = make_signal()
    assert ingest(store, d) == "new"
    row = store.claim_next_signal()
    assert row["event_id"] == d["event_id"]
    executor.recover_processing()
    row = store.get_signal(d["event_id"])
    assert row["status"] == "error" and row["reason_code"] == "UNKNOWN_STATE"
    assert paper.positions() == {}
    assert alerts.contains("UNKNOWN_STATE")
    rs = load_reports(store, "test")
    assert execution_statuses(rs) == ["error"]
    assert rs[0]["execution"]["reason_code"] == "UNKNOWN_STATE"
    assert executor.run_once() is False
