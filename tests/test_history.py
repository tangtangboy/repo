"""계정 트레이드 히스토리 적재 (history.py) · 성과 지표 (metrics.py) · CLI backfill/performance · 대시보드 Performance.

- store: 5개 테이블 멱등 적재(INSERT OR IGNORE), 범위 조회, sync_state upsert
- HistorySync: 첫 동기화(backfill_days) / 증분(last_ts − OVERLAP) / 명시 백필 / 창 분할 + 커서 페이지 / 자산 스냅샷 주기 /
  계정별 예외 격리 + 알림 5분 1회 / run_forever 정지
- BybitHistory: v5 응답 → 행 매핑 (execId / closedPnl / transaction-log 타입 / wallet)
- PaperHistory: 실제 entry→full_exit 흐름의 청산손익·자산 = 시드 + 실현, 원장 fills 의 전략별 실현손익과 일치
- metrics: seed(설정 / 첫 스냅샷) · ROI · 입출금 · 낙폭 · 승률/profit factor · 일별 · 범위 필터
- CLI performance --json / backfill (test 모드 Paper), 대시보드 /ui/performance · /ui/api/performance.json · Backfill now
"""
from __future__ import annotations

import json
import threading
import time
import uuid
from pathlib import Path

import pytest

from lake_executor import metrics
from lake_executor.history import DAY_MS, OVERLAP_MS, BybitHistory, HistorySync, PaperHistory, build_sources
from lake_executor.main import main as cli_main
from lake_executor.store import Store
from lake_executor.util import now_ms

from conftest import PAPER_PRICE, make_signal, run_signal
from test_web import login

NOW = now_ms()
H = 3600 * 1000


def _factory(store):
    return lambda: Store(store.path, schema=getattr(store, "schema", "") or "lake_executor")


def _exec(i: int, t: int, qty=0.001, price=80000.0, fee=0.01, side="Buy") -> dict:
    return {"exec_id": f"e{i}", "exchange": "bybit", "symbol": "BTCUSDT", "order_id": f"o{i}", "order_link_id": None,
            "side": side, "qty": qty, "price": price, "fee": fee, "fee_currency": "USDT", "exec_type": "Trade",
            "closed_size": None, "position_idx": 1, "exec_time_ms": t, "raw": {"isMaker": False}}


def _pnl(i: int, t: int, pnl: float, qty=0.001) -> dict:
    return {"pnl_id": f"p{i}", "exchange": "bybit", "symbol": "BTCUSDT", "order_id": f"o{i}", "side": "Sell", "qty": qty,
            "avg_entry_price": 80000.0, "avg_exit_price": 80000.0 + pnl / qty, "closed_pnl": pnl, "leverage": 5.0,
            "position_idx": 1, "created_at_ms": t, "updated_at_ms": t, "raw": None}


def _flow(i: int, t: int, typ: str, amount: float) -> dict:
    return {"flow_id": f"f{i}", "ts_ms": t, "type": typ, "amount": amount, "currency": "USDT", "raw": None}


class FakeSource:
    """HistorySource 프로토콜의 메모리 구현. 창/커서 호출을 기록한다."""
    name = "fake"

    def __init__(self, execs=(), closed=(), cash=(), equity=None, page_size=100, max_window_ms=10 * 365 * DAY_MS,
                 fail=False):
        self.execs, self.closed, self.cash = list(execs), list(closed), list(cash)
        self.equity = equity
        self.page_size = page_size
        self.max_window_ms = max_window_ms
        self.fail = fail
        self.calls: list[tuple] = []
        self.equity_calls = 0

    def _page(self, kind, rows, tcol, since, until, cursor):
        self.calls.append((kind, since, until, cursor))
        if self.fail:
            raise RuntimeError("exchange down")
        sel = [dict(r) for r in rows if since <= r[tcol] < until]
        start = int(cursor or 0)
        page = sel[start:start + self.page_size]
        nxt = str(start + self.page_size) if start + self.page_size < len(sel) else None
        return page, nxt

    def fetch_executions(self, since, until, cursor):
        return self._page("executions", self.execs, "exec_time_ms", since, until, cursor)

    def fetch_closed_pnl(self, since, until, cursor):
        return self._page("closed_pnl", self.closed, "created_at_ms", since, until, cursor)

    def fetch_cashflows(self, since, until, cursor):
        return self._page("cashflow", self.cash, "ts_ms", since, until, cursor)

    def fetch_equity(self):
        self.equity_calls += 1
        return self.equity


def _sync(settings, store, sources, alerts=None) -> HistorySync:
    return HistorySync(settings, store_factory=_factory(store), sources=sources, alerts=alerts)


def _slow(store) -> float:
    """원격 Postgres 면 연결(DDL)·쿼리마다 왕복이 느리다 → 대기 예산 배수."""
    return 12.0 if getattr(store, "backend", "sqlite") == "postgres" else 1.0


# ---------------------------------------------------------------- store
def test_store_upserts_are_idempotent_and_range_queries(store):
    t = NOW - H
    rows = [dict(_exec(1, t), mode="live", account="bybit"), dict(_exec(2, t + 1000), mode="live", account="bybit")]
    assert store.upsert_executions(rows) == 2
    assert store.upsert_executions(rows[:1]) == 0                                   # 같은 (mode, account, exec_id)
    assert store.upsert_closed_pnl([dict(_pnl(1, t, 5.0), mode="live", account="bybit")]) == 1
    assert store.upsert_closed_pnl([dict(_pnl(1, t, 5.0), mode="live", account="bybit")]) == 0
    assert store.upsert_cashflows([dict(_flow(1, t, "deposit", 100.0), mode="live", account="bybit")]) == 1
    store.insert_equity("live", "bybit", t, 1000.0, 990.0, 10.0, 900.0)
    store.insert_equity("live", "bybit", t + 5000, 1010.0)

    assert [r["exec_id"] for r in store.account_executions("live", "bybit")] == ["e1", "e2"]
    assert [r["exec_id"] for r in store.account_executions("live", "bybit", since_ms=t + 1000)] == ["e2"]
    assert [r["exec_id"] for r in store.account_executions("live", "bybit", until_ms=t + 1000)] == ["e1"]
    assert store.account_executions("test", "bybit") == [] and store.account_executions("live", "okx") == []
    assert json.loads(store.account_executions("live", "bybit")[0]["raw"]) == {"isMaker": False}
    assert store.account_closed_pnl("live", "bybit")[0]["closed_pnl"] == 5.0
    assert store.account_cashflows("live", "bybit")[0]["type"] == "deposit"
    assert store.account_equity_first("live", "bybit")["total_equity"] == 1000.0
    assert store.account_equity_latest("live", "bybit")["total_equity"] == 1010.0
    assert len(store.account_equity_series("live", "bybit")) == 2
    assert store.account_equity_latest("live", "nobody") is None

    assert store.get_sync_state("live", "bybit", "executions") is None
    store.set_sync_state("live", "bybit", "executions", t + 1000, note="x")
    store.set_sync_state("live", "bybit", "executions", t + 2000, cursor="c1", note="y")
    st = store.get_sync_state("live", "bybit", "executions")
    assert st["last_ts_ms"] == t + 2000 and st["cursor"] == "c1" and st["note"] == "y"
    assert [s["kind"] for s in store.sync_states("live", "bybit")] == ["executions"]
    assert store.sync_states("test") == []


# ---------------------------------------------------------------- HistorySync
def test_sync_first_run_then_incremental_with_overlap(store, settings):
    t0 = NOW - 2 * H
    src = FakeSource(execs=[_exec(1, t0), _exec(2, t0 + H)], closed=[_pnl(1, t0 + H, 3.0)],
                     cash=[_flow(1, t0, "deposit", 500.0)],
                     equity={"total_equity": 503.0, "wallet_balance": 503.0, "unrealised_pnl": 0.0, "available": 503.0})
    sync = _sync(settings, store, {"live": {"bybit": src}})
    try:
        res = sync.sync_account("live", "bybit", src)
        assert res["inserted"] == {"executions": 2, "closed_pnl": 1, "cashflow": 1} and res["equity"] is True
        first = [c for c in src.calls if c[0] == "executions"]
        assert len(first) == 1 and first[0][3] is None
        # 첫 동기화는 history.backfill_days 전부터
        assert first[0][1] == pytest.approx(NOW - settings.history_backfill_days * DAY_MS, abs=5 * 60 * 1000)
        assert store.get_sync_state("live", "bybit", "executions")["last_ts_ms"] == t0 + H
        assert store.get_sync_state("live", "bybit", "cashflow")["last_ts_ms"] == t0
        assert len(store.account_equity_series("live", "bybit")) == 1 and src.equity_calls == 1

        src.execs.append(_exec(3, t0 + H + 1000))
        res2 = sync.sync_account("live", "bybit", src)
        assert res2["inserted"] == {"executions": 1, "closed_pnl": 0, "cashflow": 0}
        assert res2["equity"] is False and src.equity_calls == 1                      # 자산 스냅샷은 주기(equity_interval_s) 전
        second = [c for c in src.calls if c[0] == "executions"][1]
        assert second[1] == t0 + H - OVERLAP_MS                                       # 마지막 시각 − 5분부터 다시 (중복은 PK)
        assert len(store.account_executions("live", "bybit")) == 3
        snap = sync.snapshot()["live/bybit"]
        assert snap["inserted"]["executions"] == 3 and snap["errors"] == 0 and snap["last_sync_ms"] > 0
    finally:
        sync.close()


def test_backfill_splits_windows_follows_cursors_and_is_idempotent(store, settings):
    base = NOW - 20 * DAY_MS
    execs = [_exec(i, base + i * 12 * H) for i in range(30)]                 # 15일에 걸쳐 30건
    src = FakeSource(execs=execs, page_size=4, max_window_ms=7 * DAY_MS)
    sync = _sync(settings, store, {"live": {"bybit": src}})
    try:
        res = sync.backfill("live", "bybit", since_ms=base - DAY_MS)
        assert res["inserted"]["executions"] == 30 and res["equity"] is False        # equity None → 스냅샷 없음
        calls = [c for c in src.calls if c[0] == "executions"]
        windows = sorted({(c[1], c[2]) for c in calls})
        assert len(windows) >= 3 and all(w[1] - w[0] <= 7 * DAY_MS for w in windows)
        assert windows[0][0] == base - DAY_MS and windows[-1][1] >= NOW
        assert all(windows[i][1] == windows[i + 1][0] for i in range(len(windows) - 1))   # 창 사이에 틈이 없다
        assert any(c[3] is not None for c in calls)                                   # 커서 페이지를 따라갔다
        assert sync.backfill("live", "bybit", since_ms=base - DAY_MS)["inserted"]["executions"] == 0   # 멱등
        assert len(store.account_executions("live", "bybit")) == 30
        until = base + 5 * DAY_MS
        assert sync.backfill("live", "bybit", since_ms=base - DAY_MS, until_ms=until)["inserted"]["executions"] == 0
        assert store.get_sync_state("live", "bybit", "executions")["note"] == f"until={until}"
        with pytest.raises(KeyError):
            sync.backfill("live", "nope", since_ms=0)
    finally:
        sync.close()


def test_sync_all_isolates_failures_and_throttles_alerts(store, settings, alerts):
    bad = FakeSource(fail=True)
    good = FakeSource(execs=[_exec(1, NOW - H)])
    sync = _sync(settings, store, {"live": {"bad": bad, "bybit": good}}, alerts=alerts)
    try:
        sync.sync_all()
        sync.sync_all()
        snap = sync.snapshot()
        assert snap["live/bad"]["errors"] == 2 and snap["live/bad"]["last_error"] == "RuntimeError"
        assert snap["live/bybit"]["inserted"]["executions"] == 1 and snap["live/bybit"]["errors"] == 0
        assert sum("[history] live/bad" in m for m in alerts.messages) == 1          # 알림은 5분에 한 번
        assert store.get_sync_state("live", "bad", "executions") is None            # 실패한 계정은 커서가 전진하지 않는다
    finally:
        sync.close()


def test_run_forever_syncs_periodically_and_stops(store, settings):
    src = FakeSource(execs=[_exec(1, NOW - H)], equity={"total_equity": 1.0})
    sync = _sync(settings, store, {"live": {"bybit": src}})
    sync.startup_delay_s = 0.0
    sync.interval_s = 0.05
    stop = threading.Event()
    t = threading.Thread(target=sync.run_forever, args=(stop,), daemon=True)
    t.start()
    deadline = time.monotonic() + 5 * _slow(store)
    while time.monotonic() < deadline and len([c for c in src.calls if c[0] == "executions"]) < 2:
        time.sleep(0.02)
    stop.set()
    t.join(3 * _slow(store))
    assert not t.is_alive()
    assert len([c for c in src.calls if c[0] == "executions"]) >= 2
    assert len(store.account_executions("live", "bybit")) == 1
    assert src.equity_calls == 1                                                      # 주기 전이라 한 번만


# ---------------------------------------------------------------- Bybit 매핑
class FakeBybitHttp:
    def __init__(self):
        self.calls: list[tuple[str, dict]] = []

    def get_executions(self, **kw):
        self.calls.append(("get_executions", kw))
        return {"result": {"list": [
            {"execId": "x1", "symbol": "BTCUSDT", "orderId": "o1", "orderLinkId": "lk1", "side": "Sell", "execQty": "0.002",
             "execPrice": "81000.5", "execFee": "0.0891", "feeCurrency": "USDT", "execType": "Trade", "closedSize": "0",
             "execTime": "1700000000000", "isMaker": False},
            {"execId": "", "execQty": "1"}],                                           # id 없는 항목은 버린다
            "nextPageCursor": "" if kw.get("cursor") else "c2"}}

    def get_closed_pnl(self, **kw):
        self.calls.append(("get_closed_pnl", kw))
        return {"result": {"list": [
            {"orderId": "o9", "symbol": "BTCUSDT", "side": "Buy", "qty": "0.002", "avgEntryPrice": "80000", "avgExitPrice": "81000",
             "closedPnl": "1.9", "leverage": "5", "createdTime": "1700000001000", "updatedTime": "1700000002000"}],
            "nextPageCursor": ""}}

    def get_transaction_log(self, **kw):
        self.calls.append(("get_transaction_log", kw))
        return {"result": {"list": [
            {"id": "t1", "type": "TRANSFER_IN", "cashFlow": "100", "transactionTime": "1700000000000", "currency": "USDT"},
            {"id": "t2", "type": "SETTLEMENT", "cashFlow": "-0.5", "transactionTime": "1700000000500", "currency": "USDT"},
            {"id": "t3", "type": "TRADE", "cashFlow": "1.9", "transactionTime": "1700000000600", "currency": "USDT"},
            {"id": "t4", "type": "TRANSFER_OUT", "cashFlow": "-30", "transactionTime": "1700000000700", "currency": "USDT"}],
            "nextPageCursor": ""}}

    def get_wallet_balance(self, **kw):
        self.calls.append(("get_wallet_balance", kw))
        return {"result": {"list": [{"totalEquity": "1071.4", "totalWalletBalance": "1070", "totalPerpUPL": "1.4",
                                     "totalAvailableBalance": "900"}]}}


def test_bybit_history_maps_v5_payloads(settings):
    http = FakeBybitHttp()
    acct = settings.accounts[0]
    src = BybitHistory(account=acct, http=http)
    rows, nxt = src.fetch_executions(1, 2, None)
    assert nxt == "c2" and len(rows) == 1
    r = rows[0]
    assert r["exec_id"] == "x1" and r["qty"] == 0.002 and r["price"] == 81000.5 and r["fee"] == pytest.approx(0.0891)
    assert r["exec_time_ms"] == 1700000000000 and r["order_link_id"] == "lk1" and r["side"] == "Sell" and r["exchange"] == "bybit"
    kw = http.calls[-1][1]
    assert kw["category"] == "linear" and "symbol" not in kw and kw["startTime"] == 1 and kw["endTime"] == 2 and "cursor" not in kw
    rows, nxt = src.fetch_executions(1, 2, "c2")
    assert nxt is None and http.calls[-1][1]["cursor"] == "c2"

    rows, nxt = src.fetch_closed_pnl(1, 2, None)
    assert nxt is None and rows[0]["pnl_id"] == "o9:1700000001000" and rows[0]["closed_pnl"] == pytest.approx(1.9)
    assert rows[0]["leverage"] == 5.0 and rows[0]["avg_exit_price"] == 81000.0 and rows[0]["updated_at_ms"] == 1700000002000

    rows, _ = src.fetch_cashflows(1, 2, None)
    assert [(r["flow_id"], r["type"], r["amount"]) for r in rows] == [("t1", "deposit", 100.0), ("t2", "funding", -0.5),
                                                                       ("t4", "withdraw", -30.0)]
    assert http.calls[-1][1]["accountType"] == "UNIFIED"
    assert src.fetch_equity() == {"total_equity": 1071.4, "wallet_balance": 1070.0, "unrealised_pnl": 1.4, "available": 900.0}
    assert src.max_window_ms <= 7 * DAY_MS


def test_build_sources_picks_adapters(settings, paper):
    class FakeBybit:
        name = "bybit"

        def __init__(self, acct):
            self.account, self.http = acct, FakeBybitHttp()

    class FakeOkx:
        name = "okx"
        account = settings.accounts[0]

    out = build_sources(settings, {"test": {"bybit": paper},
                                   "live": {"bybit": FakeBybit(settings.accounts[0]), "okx": FakeOkx()}})
    assert isinstance(out["test"]["bybit"], PaperHistory) and isinstance(out["live"]["bybit"], BybitHistory)
    assert "okx" not in out["live"]                                                   # 어댑터 없음 → 건너뜀
    assert build_sources(settings, {}) == {"live": {}, "test": {}}


# ---------------------------------------------------------------- Paper 끝까지 + 성과 지표
def test_paper_flow_closed_pnl_equity_and_metrics(settings, store, executor, paper):
    pid = f"hist-{uuid.uuid4().hex[:8]}"
    base = dict(position_id=pid, leg="short", position_idx=2, strategy="overheat")
    e1 = make_signal(**base, event_sequence=1, action="entry", qty_btc=0.002, expected_qty_btc_after=0.002,
                     reference_price=PAPER_PRICE)
    assert run_signal(executor, store, e1)["status"] == "done"
    paper.set_price(PAPER_PRICE - 1000.0)                                             # 숏 → 1000 하락 = 이익
    e2 = make_signal(**base, event_sequence=2, action="full_exit", qty_btc=0.002, expected_qty_btc_after=0,
                     reference_price=None)
    assert run_signal(executor, store, e2)["status"] == "done"

    src = PaperHistory(paper)
    closed, _ = src.fetch_closed_pnl(0, now_ms() + 1, None)
    assert len(closed) == 1 and closed[0]["closed_pnl"] == pytest.approx(2.0) and closed[0]["side"] == "Sell"
    assert closed[0]["avg_entry_price"] == pytest.approx(PAPER_PRICE) and closed[0]["avg_exit_price"] == pytest.approx(PAPER_PRICE - 1000)
    execs, _ = src.fetch_executions(0, now_ms() + 1, None)
    assert sorted(e["qty"] for e in execs) == [0.002, 0.002] and {e["side"] for e in execs} == {"Buy", "Sell"}
    assert src.fetch_executions(now_ms() + 10_000, now_ms() + 20_000, None) == ([], None)
    eq = src.fetch_equity()
    assert eq["total_equity"] == pytest.approx(paper.seed_usdt + 2.0) and eq["unrealised_pnl"] == 0.0
    assert paper.realized_pnl == pytest.approx(2.0)

    settings.accounts[0].seed_usdt = 10000.0                                          # 설정 시드 → ROI 기준
    sync = _sync(settings, store, {"test": {"bybit": src}})
    try:
        res = sync.sync_account("test", "bybit", src)
    finally:
        sync.close()
    assert res["inserted"] == {"executions": 2, "closed_pnl": 1, "cashflow": 0} and res["equity"] is True

    perf = metrics.account_performance(store, settings, "test", "bybit")
    assert perf["seed"] == 10000.0 and perf["seed_source"] == "config"
    assert perf["equity_now"] == pytest.approx(paper.seed_usdt + 2.0)
    assert perf["pnl_total"] == pytest.approx(paper.seed_usdt + 2.0 - 10000.0)
    assert perf["roi_vs_seed"] == pytest.approx((paper.seed_usdt + 2.0 - 10000.0) / 10000.0)
    assert perf["closed"]["trades"] == 1 and perf["closed"]["wins"] == 1 and perf["closed"]["win_rate"] == 1.0
    assert perf["closed"]["realized_closed_pnl"] == pytest.approx(2.0) and perf["executions"] == 2
    led = perf["ledger"]
    assert led["realized"] == pytest.approx(2.0) and led["by_strategy"] == pytest.approx({"overheat": 2.0})
    assert led["positions_closed"] == 1 and led["open_positions"] == [] and led["fills"] == 2
    assert perf["daily"][-1]["trades"] == 1 and perf["daily"][-1]["closed_pnl"] == pytest.approx(2.0)
    assert set(perf["sync"]) == {"executions", "closed_pnl", "cashflow"}

    # 열린 포지션은 ledger.open_positions 에, 청산 전이라 실현손익은 그대로
    e3 = make_signal(position_id=f"{pid}-2", leg="long", position_idx=1, strategy="basic", event_sequence=1, action="entry",
                     qty_btc=0.001, expected_qty_btc_after=0.001, reference_price=PAPER_PRICE - 1000.0)
    assert run_signal(executor, store, e3)["status"] == "done"
    led2 = metrics.ledger_pnl(store, "test", "bybit")
    assert led2["realized"] == pytest.approx(2.0) and len(led2["open_positions"]) == 1
    assert led2["open_positions"][0]["strategy"] == "basic" and led2["open_positions"][0]["leg"] == "long"
    assert led2["open_positions"][0]["qty"] == pytest.approx(0.001)


def test_metrics_seed_sources_roi_drawdown_and_daily(store, settings):
    d0 = NOW - 3 * DAY_MS
    for dt, v in [(0, 1000.0), (DAY_MS, 1200.0), (2 * DAY_MS, 900.0), (3 * DAY_MS, 1100.0)]:
        store.insert_equity("live", "bybit", d0 + dt, v)
    store.upsert_cashflows([dict(_flow(1, d0 + DAY_MS, "deposit", 100.0), mode="live", account="bybit"),
                            dict(_flow(2, d0 + 2 * DAY_MS, "withdraw", -40.0), mode="live", account="bybit"),
                            dict(_flow(3, d0 + 2 * DAY_MS, "funding", -1.5), mode="live", account="bybit")])
    store.upsert_closed_pnl([dict(_pnl(1, d0 + DAY_MS, 50.0), mode="live", account="bybit"),
                             dict(_pnl(2, d0 + 2 * DAY_MS, -20.0), mode="live", account="bybit"),
                             dict(_pnl(3, d0 + 3 * DAY_MS, 30.0), mode="live", account="bybit")])
    store.upsert_executions([dict(_exec(i, d0 + i * DAY_MS, fee=0.25), mode="live", account="bybit") for i in range(1, 4)])

    settings.accounts[0].seed_usdt = None
    p = metrics.account_performance(store, settings, "live", "bybit")
    assert p["seed"] == 1000.0 and p["seed_source"] == "first_equity_snapshot"
    assert p["equity_now"] == 1100.0 and p["deposits"] == 100.0 and p["withdrawals"] == 40.0 and p["net_deposits"] == 1060.0
    assert p["pnl_total"] == pytest.approx(40.0) and p["roi_vs_seed"] == pytest.approx(0.1)
    assert p["roi_vs_net_deposits"] == pytest.approx(40 / 1060)
    assert p["fees"] == pytest.approx(0.75) and p["funding"] == -1.5 and p["executions"] == 3
    c = p["closed"]
    assert c["trades"] == 3 and c["wins"] == 2 and c["losses"] == 1 and c["win_rate"] == pytest.approx(2 / 3)
    assert c["realized_closed_pnl"] == 60.0 and c["profit_factor"] == 4.0 and c["largest_loss"] == -20.0 and c["avg_win"] == 40.0
    assert c["gross_profit"] == 80.0 and c["gross_loss"] == -20.0 and c["volume_qty"] == pytest.approx(0.003)
    e = p["equity"]
    assert e["first"] == 1000.0 and e["latest"] == 1100.0 and e["peak"] == 1200.0 and e["points"] == 4
    assert e["max_drawdown"] == 300.0 and e["max_drawdown_pct"] == 0.25
    d = p["daily"]
    assert len(d) == 4 and d[0]["trades"] == 0 and d[0]["equity_close"] == 1000.0
    assert d[1]["deposits"] == 100.0 and d[1]["closed_pnl"] == 50.0 and d[1]["fees"] == 0.25
    assert d[2]["withdrawals"] == 40.0 and d[2]["funding"] == -1.5 and d[3]["equity_close"] == 1100.0
    assert p["ledger"] == {"realized": 0.0, "by_strategy": {}, "by_position": {}, "fills": 0, "positions_closed": 0,
                           "open_positions": []}

    settings.accounts[0].seed_usdt = 800.0
    p2 = metrics.account_performance(store, settings, "live", "bybit")
    assert p2["seed"] == 800.0 and p2["seed_source"] == "config" and p2["net_deposits"] == 860.0
    assert p2["pnl_total"] == pytest.approx(240.0) and p2["roi_vs_seed"] == pytest.approx(0.375)

    p3 = metrics.account_performance(store, settings, "live", "bybit", since_ms=d0 + 3 * DAY_MS)   # 마지막 날만
    assert p3["closed"]["trades"] == 1 and p3["equity"]["points"] == 1 and p3["seed"] == 800.0
    empty = metrics.account_performance(store, settings, "live", "nobody")
    assert empty["equity_now"] is None and empty["seed"] is None and empty["roi_vs_seed"] is None and empty["pnl_total"] is None
    assert empty["closed"]["trades"] == 0 and empty["closed"]["win_rate"] is None and empty["equity"]["points"] == 0
    assert [r["account"] for r in metrics.performance_all(store, settings, "live")] == [a.name for a in settings.accounts]
    assert metrics.equity_stats([]) == {"first": None, "latest": None, "peak": None, "max_drawdown": None,
                                        "max_drawdown_pct": None, "points": 0}


# ---------------------------------------------------------------- CLI
def test_cli_performance_json_and_backfill_paper(settings, capsys):
    root = Path(settings.state_dir).parent
    argv = ["--config", str(root / "config.json"), "--env", str(root / ".env")]
    assert cli_main(argv + ["backfill", "--mode", "test", "--since", "2026-01-01"]) == 0
    assert "test/bybit: executions +0 closed_pnl +0 cashflow +0 equity_snapshot=yes" in capsys.readouterr().out
    assert cli_main(argv + ["performance", "--mode", "test", "--json"]) == 0
    data = json.loads(capsys.readouterr().out)
    acc = data["accounts"][0]
    assert data["mode"] == "test" and acc["account"] == "bybit" and acc["equity_now"] == 10000.0
    assert acc["seed_source"] == "first_equity_snapshot" and acc["roi_vs_seed"] == 0.0 and acc["sync"]["executions"]["last_ts_ms"] >= 0
    assert cli_main(argv + ["performance", "--mode", "test"]) == 0
    out = capsys.readouterr().out
    assert "ROI vs seed +0.00%" in out and "== test/bybit" in out
    assert cli_main(argv + ["backfill", "--mode", "live", "--since", "2026-01-01"]) == 2          # 실키 없음 → 소스 없음
    assert "no history source" in capsys.readouterr().err
    assert cli_main(argv + ["backfill", "--mode", "test", "--since", "bad-date"]) == 2
    assert cli_main(argv + ["backfill", "--mode", "test", "--account", "nope", "--since", "2026-01-01"]) == 2


# ---------------------------------------------------------------- 대시보드
def test_dashboard_performance_page_json_and_backfill(client, services, settings, store, alerts):
    assert client.get("/ui/api/performance.json", follow_redirects=False).status_code == 303   # 로그인 전
    csrf = login(client)
    r = client.get("/ui/performance")
    assert r.status_code == 200 and "Account performance" in r.text and "history sync: disabled" in r.text
    src = FakeSource(execs=[_exec(1, NOW - H)], closed=[_pnl(1, NOW - H, 7.5)],
                     equity={"total_equity": 1007.5, "wallet_balance": 1007.5, "unrealised_pnl": 0.0, "available": 1007.5})
    services.history = _sync(settings, store, {"test": {"bybit": src}}, alerts=alerts)
    try:
        r = client.post("/ui/performance/backfill", data={"_csrf": csrf, "mode": "test", "account": "bybit", "since": "2026-01-01"},
                        follow_redirects=False)
        assert r.status_code == 303 and "flash=backfill+test%2Fbybit+started" in r.headers["location"]
        for t in services.history._backfill_threads:
            t.join(5 * _slow(store))
        assert len(store.account_closed_pnl("test", "bybit")) == 1

        r = client.get("/ui/api/performance.json?mode=test&account=bybit")
        assert r.status_code == 200
        data = r.json()
        acc = data["accounts"][0]
        assert len(data["accounts"]) == 1 and acc["account"] == "bybit" and acc["closed"]["trades"] == 1
        assert acc["equity_now"] == 1007.5 and acc["seed_source"] == "first_equity_snapshot"
        assert data["sync"]["test/bybit"]["inserted"]["closed_pnl"] == 1

        r = client.get("/ui/performance?mode=test&account=bybit")
        assert r.status_code == 200 and "7.5000" in r.text and "Backfill now" in r.text and "test/bybit" in r.text
        assert client.get("/ui/performance?mode=nope").status_code == 400
        assert client.get("/ui/performance?account=nope").status_code == 400
        assert client.get("/ui/performance?since=bad").status_code == 400
        bad = [({"mode": "test", "account": "nope", "since": "2026-01-01"}, 404),
               ({"mode": "live", "account": "bybit", "since": "2026-01-01"}, 409),       # live 소스 없음
               ({"mode": "test", "account": "bybit", "since": ""}, 400),
               ({"mode": "test", "account": "bybit", "since": "x"}, 400),
               ({"mode": "x", "account": "bybit", "since": "2026-01-01"}, 400)]
        for form, status in bad:
            assert client.post("/ui/performance/backfill", data=dict(form, _csrf=csrf), follow_redirects=False).status_code == status
        assert client.post("/ui/performance/backfill", data={"mode": "test", "account": "bybit", "since": "2026-01-01"},
                           follow_redirects=False).status_code == 403                    # CSRF 없음
        r = client.get("/ui")
        assert r.status_code == 200 and "trade history sync" in r.text
    finally:
        services.history.close()
