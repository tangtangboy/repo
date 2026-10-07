"""test 모드 재시작 복구: 가상거래소(PaperExchange) 포지션을 장부의 열린 lot 으로 되살린다.

배경(2026-10-08): 서버 재시작으로 가상 포지션이 사라진 뒤 lot(0.002) ≠ 거래소(0.001) 불일치 → RECONCILE_REQUIRED 로 상대의 정상 TEST entry 가
거부됐다. 이제 serve 가 기동 때 open lot 으로 포지션을 seed 하므로 재시작 뒤에도 대사가 일치하고 full_exit 까지 이어진다."""
from __future__ import annotations

import uuid

import pytest

from lake_executor.exchange import PaperExchange
from lake_executor.executor import Executor
from lake_executor.reporter import Reporter

from conftest import PAPER_PRICE, AlertsStub, FakeClient, make_signal, run_signal


def _entry(executor, store, pid, leg, idx, qty, price):
    e = make_signal(position_id=pid, leg=leg, position_idx=idx, strategy="overheat", event_sequence=1, action="entry",
                    qty_btc=qty, expected_qty_btc_after=qty, reference_price=price)
    assert run_signal(executor, store, e)["status"] == "done"


def test_restart_reseeds_paper_positions_and_keeps_reconcile_consistent(settings, store):
    paper = PaperExchange(settings.accounts[0], price=PAPER_PRICE, id_tag="a")
    alerts = AlertsStub()
    reporter = Reporter(settings, store, alerts, client=FakeClient())
    ex = Executor(settings, store, {"test": {"bybit": paper}, "live": {}}, reporter, alerts)
    p1, p2, p3 = (f"seed-{uuid.uuid4().hex[:6]}" for _ in range(3))
    _entry(ex, store, p1, "short", 2, 0.001, 85000.0)
    _entry(ex, store, p2, "short", 2, 0.002, 86000.0)
    _entry(ex, store, p3, "long", 1, 0.003, 84000.0)
    assert ex.reconcile("test", "bybit") is True
    reporter.close()

    # "재시작": 새 가상거래소(빈 메모리) — seed 없이면 불일치
    fresh = PaperExchange(settings.accounts[0], price=PAPER_PRICE, id_tag="b")
    reporter2 = Reporter(settings, store, alerts, client=FakeClient())
    ex2 = Executor(settings, store, {"test": {"bybit": fresh}, "live": {}}, reporter2, alerts)
    assert ex2.reconcile("test", "bybit") is False and store.is_inconsistent("test", "bybit")

    pos = fresh.seed_positions(store.open_lots("test", "bybit"))
    assert pos[2]["side"] == "Sell" and pos[2]["size"] == pytest.approx(0.003)
    assert pos[2]["avg_price"] == pytest.approx((85000.0 * 0.001 + 86000.0 * 0.002) / 0.003)
    assert pos[1]["side"] == "Buy" and pos[1]["size"] == pytest.approx(0.003) and pos[1]["avg_price"] == pytest.approx(84000.0)
    assert ex2.reconcile("test", "bybit") is True and not store.is_inconsistent("test", "bybit")

    # seed 된 포지션으로 full_exit 이 정상 체결되고 lot 이 닫힌다
    fresh.set_price(85500.0)
    x = make_signal(position_id=p1, leg="short", position_idx=2, strategy="overheat", event_sequence=2, action="full_exit",
                    qty_btc=0.001, expected_qty_btc_after=0, reference_price=None)
    assert run_signal(ex2, store, x)["status"] == "done"
    assert store.get_lot("test", "bybit", p1)["status"] == "closed"
    assert fresh.positions()[2]["size"] == pytest.approx(0.002)
    assert ex2.reconcile("test", "bybit") is True
    reporter2.close()


def test_seed_ignores_closed_and_zero_lots_and_handles_empty(settings):
    paper = PaperExchange(settings.accounts[0], price=PAPER_PRICE)
    assert paper.seed_positions([]) == {} and paper.positions() == {}
    lots = [{"position_idx": 2, "leg": "short", "qty": 0.001, "avg_entry": 80000.0, "status": "closed"},
            {"position_idx": 1, "leg": "long", "qty": 0.0, "avg_entry": 80000.0, "status": "open"},
            {"position_idx": 1, "leg": "long", "qty": 0.0015, "avg_entry": None, "status": "open"}]
    pos = paper.seed_positions(lots)
    assert list(pos) == [1] and pos[1]["size"] == pytest.approx(0.001) and pos[1]["avg_price"] == PAPER_PRICE   # 스텝 내림, 평균가 없으면 현재가
