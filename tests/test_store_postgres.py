"""Postgres(Supabase) 원장 백엔드 통합 테스트 — 실제 DB 가 필요하므로 LAKE_TEST_DATABASE_URL 이 없으면 전부 건너뛴다.

    LAKE_TEST_DATABASE_URL=postgresql://user:pw@host:5432/postgres python -m pytest -q tests/test_store_postgres.py

테스트마다 임시 스키마(lake_test_<random>) 를 만들고 끝나면 DROP SCHEMA … CASCADE 로 지운다 (다른 앱의 테이블은 건드리지 않는다).
SQLite 테스트(나머지 전부) 와 같은 Store API 를 Postgres 에서 돌려 SQL 변환(?→%s, INSERT OR IGNORE, LIKE 의 %, rowid 정렬,
bytea 포함 검색, ON CONFLICT upsert, BIGSERIAL) 과 재접속/트랜잭션 복구를 검증한다.
"""
from __future__ import annotations

import json
import os
import uuid

import pytest

from lake_executor import store as st
from lake_executor.schemas import Signal
from lake_executor.store import LedgerUnavailable, Store
from lake_executor.util import now_ms

URL = os.environ.get("LAKE_TEST_DATABASE_URL", "").strip()
pytestmark = pytest.mark.skipif(not URL, reason="LAKE_TEST_DATABASE_URL not set (needs a real Postgres)")


def _signal(**over) -> tuple[Signal, bytes]:
    ts = now_ms()
    tag = uuid.uuid4().hex[:8]
    d = {
        "schema_version": 1, "strategy_name": "pg-test", "strategy": "basic", "mode": "test",
        "event_id": f"ev-{tag}", "event_sequence": 1, "ts": ts, "expires_at_ms": ts + 15000,
        "exchange": "Bybit", "category": "linear", "symbol": "BTCUSDT", "position_id": f"pos-{tag}",
        "leg": "long", "position_idx": 1, "action": "entry", "qty_btc": 0.001, "expected_qty_btc_after": 0.001,
        "reference_price": 80000, "protection_revision": 1, "stop_loss": None, "take_profit": None,
    }
    d.update(over)
    raw = json.dumps(d, separators=(",", ":")).encode("utf-8")
    return Signal.model_validate(d), raw


@pytest.fixture
def pg():
    schema = "lake_test_" + uuid.uuid4().hex[:8]
    s = Store(URL, schema=schema)
    try:
        yield s
    finally:
        try:
            s._conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            s.close()


def test_backend_describe_and_ping(pg):
    assert pg.backend == "postgres"
    d = pg.describe()
    assert d.startswith("postgres ") and "schema=lake_test_" in d
    assert "@" not in d and ":" in d          # host:port 만, 자격증명 없음
    assert pg.ping_ms() < 5000
    assert pg.get_meta("schema_version") == "2"
    assert pg.reconnects == 0


def test_signal_lifecycle_dup_conflict_sequence(pg):
    sig, raw = _signal()
    assert pg.insert_signal(sig, raw, now_ms()) == "new"
    assert pg.insert_signal(sig, raw, now_ms()) == "duplicate"
    sig2, raw2 = _signal(event_id=sig.event_id, position_id=sig.position_id, qty_btc=0.002, expected_qty_btc_after=0.002)
    assert pg.insert_signal(sig2, raw2, now_ms()) == "conflict"
    sig3, raw3 = _signal(position_id=sig.position_id, event_sequence=1, action="add")
    assert pg.insert_signal(sig3, raw3, now_ms()) == "sequence_conflict"
    sig4, raw4 = _signal(position_id=sig.position_id, event_sequence=2, action="add")
    assert pg.insert_signal(sig4, raw4, now_ms()) == "new"

    row = pg.claim_next_signal()
    assert row is not None and row["event_id"] == sig.event_id and row["status"] == "processing"
    assert bytes(row["raw_body"]) == raw
    assert [r["event_id"] for r in pg.processing_signals()] == [sig.event_id]
    pg.set_run_result("test", sig.event_id, "bybit", st.SIGNAL_DONE, None, "ok")
    assert pg.get_run("test", sig.event_id, "bybit")["status"] == "done"
    status, reason, note = pg.finalize_signal("test", sig.event_id)
    assert (status, reason) == ("done", None) and "bybit=done" in note
    assert pg.get_signal(sig.event_id, "test")["status"] == "done"
    assert pg.get_signal(sig.event_id)["mode"] == "test"
    rows = pg.list_signals(mode="test", limit=10)
    assert [r["event_id"] for r in rows][:2] == [sig4.event_id, sig.event_id]     # received_at_ms DESC, event_id DESC
    assert {(r["mode"], r["status"]): r["n"] for r in pg.signal_counts()} == {("test", "done"): 1, ("test", "accepted"): 1}
    nxt = pg.claim_next_signal()
    assert nxt["event_id"] == sig4.event_id and pg.claim_next_signal() is None


def test_lots_orders_fills(pg):
    lot = {"mode": "test", "account": "bybit", "position_id": "p1", "strategy": "basic", "leg": "long", "position_idx": 1,
           "qty": 0.003, "avg_entry": 80000.5, "stop_loss": 79000.0, "take_profit": [81000.0, 82000.0],
           "protection_revision": 1, "protection_orders": {"sl": {"order_link_id": "sl-1", "price": 79000.0}, "tp": []},
           "status": st.LOT_OPEN, "opened_at_ms": now_ms(), "updated_at_ms": now_ms(), "closed_at_ms": None,
           "last_event_id": "e1"}
    pg.upsert_lot(lot)
    got = pg.get_lot("test", "bybit", "p1")
    assert got["qty"] == 0.003 and got["take_profit"] == [81000.0, 82000.0]
    assert got["protection_orders"]["sl"]["order_link_id"] == "sl-1"
    assert [l["position_id"] for l in pg.open_lots("test", "bybit")] == ["p1"]
    lot["qty"], lot["status"], lot["closed_at_ms"] = 0.0, st.LOT_CLOSED, now_ms()
    pg.upsert_lot(lot)                                   # ON CONFLICT upsert
    assert pg.open_lots("test") == []
    pend = pg.lots_with_pending_cancel("test", "bybit")   # LIKE '%order_link_id%' → '%%' 이스케이프 경로
    assert [l["position_id"] for l in pend] == ["p1"]
    assert pg.ledger_accounts()["bybit"]["rows"] >= 1

    pg.insert_order("lk-1", "test", "p1", "entry", "Buy", 0.003, False, event_id="e1", account="bybit")
    pg.insert_order("lk-1", "test", "p1", "entry", "Buy", 0.003, False, event_id="e1", status="Filled", order_id="o1",
                    account="bybit")                                     # 같은 키 → 갱신
    o = pg.get_order("lk-1", "bybit")
    assert o["status"] == "Filled" and o["order_id"] == "o1"
    pg.update_order("lk-1", "bybit", status="Cancelled", raw='{"x":1}')
    assert pg.get_order("lk-1", "bybit")["status"] == "Cancelled"
    assert [o["order_link_id"] for o in pg.orders_by_link_prefix("bybit", "lk")] == ["lk-1"]
    assert pg.orders_by_link_prefix("bybit", "no-%") == []               # 영숫자 아님 → 빈 결과 (LIKE 와일드카드 주입 방지)
    assert len(pg.orders_for_position("test", "bybit", "p1", "entry")) == 1
    assert [o["order_link_id"] for o in pg.orders_with_status("test", "bybit", ("Cancelled", "unknown"), ("entry",))] == ["lk-1"]
    assert len(pg.orders_for_event("test", "e1")) == 1 and len(pg.list_orders(mode="test", account="bybit")) == 1

    assert pg.insert_fill("x1", "test", "p1", 0.003, 80000.5, now_ms(), order_id="o1", order_link_id="lk-1", event_id="e1",
                          account="bybit") is True
    assert pg.insert_fill("x1", "test", "p1", 0.003, 80000.5, now_ms(), account="bybit") is False   # INSERT OR IGNORE → DO NOTHING
    pg.mark_fill_reported("x1", "bybit")
    pg.mark_fill_applied("x1", "bybit")
    f = pg.fills_for_order("lk-1", "bybit")[0]
    assert f["reported"] == 1 and f["applied"] == 1 and f["price"] == 80000.5
    assert len(pg.fills_for_event("test", "e1")) == 1


def test_reports_meta_protection_and_search(pg):
    def build(report_id, seq, ts, obs):
        return {"report_id": report_id, "sequence": seq, "ts": ts, "observed_at_ms": obs, "kind": "execution",
                "execution": {"event_id": "ev-search-1", "status": "filled"}}
    ser = lambda d: json.dumps(d, separators=(",", ":")).encode("utf-8")   # noqa: E731
    r1 = pg.allocate_report("test", "bybit", "execution", now_ms(), build, ser)
    r2 = pg.allocate_report("test", "bybit", "execution", now_ms(), build, ser)
    assert (r1["sequence"], r2["sequence"]) == (1, 2) and r2["observed_at_ms"] >= r1["observed_at_ms"]
    assert [r["sequence"] for r in pg.pending_reports("test", "bybit")] == [1, 2]
    pg.update_report(r1["report_id"], st.REPORT_SENT, http_status=200, attempts=1, sent=True, note="ok")
    assert [r["sequence"] for r in pg.pending_reports("test")] == [2]
    rec = pg.recent_reports("test", "bybit", limit=5)
    assert rec[0]["sequence"] == 2 and "body" not in rec[0]
    lst = pg.list_reports(mode="test", state="sent")
    assert len(lst) == 1 and lst[0]["body_len"] > 10 and "body" not in lst[0]
    found = pg.reports_for_event("test", "ev-search-1")                 # position(bytea in bytea) 경로
    assert [r["sequence"] for r in found] == [1, 2] and found[0]["execution_status"] == "filled"
    assert pg.reports_for_event("test", "ev-none") == []
    assert {(r["mode"], r["state"]): r["n"] for r in pg.report_counts()} == {("test", "sent"): 1, ("test", "pending"): 1}

    pg.set_meta("k", 5)
    assert pg.get_meta("k") == "5" and pg.get_meta("missing", "dflt") == "dflt"
    pg.set_inconsistent("test", "bybit", True, "reason")
    assert pg.is_inconsistent("test", "bybit") and pg.inconsistent_note("test", "bybit") == "reason"
    pg.set_inconsistent("test", "bybit", False)
    assert not pg.is_inconsistent("test", "bybit")
    pg.set_leg_protection("test", "bybit", 1, {"stop_loss": 1.0, "take_profit": [2.0], "tp_i": 0, "position_id": "p", "set_at_ms": 1})
    assert pg.get_leg_protection("test", "bybit", 1)["stop_loss"] == 1.0
    pg.set_leg_protection("test", "bybit", 1, None)
    assert pg.get_leg_protection("test", "bybit", 1)["cleared"] is True


def test_ingress_log_and_prune(pg):
    for i in range(3):
        pg.log_ingress("DUPLICATE", f"ev-{i}", b'{"x":1}', note=f"n{i}")
    rows = pg.recent_ingress(limit=10)
    assert [r["event_id"] for r in rows] == ["ev-2", "ev-1", "ev-0"] and rows[0]["id"] > rows[2]["id"]   # BIGSERIAL
    assert len(pg.ingress_for_event("ev-1")) == 1
    assert pg.prune_ingress_log(max_age_ms=10**12, max_rows=1) == 2
    assert [r["event_id"] for r in pg.recent_ingress()] == ["ev-2"]


def test_transaction_rolls_back_on_error(pg):
    with pytest.raises(RuntimeError):
        with pg._tx():
            pg._conn.execute("INSERT INTO meta(key,value) VALUES(?,?)", ("rollback-me", "1"))
            raise RuntimeError("boom")
    assert pg.get_meta("rollback-me") is None
    pg.set_meta("after", "ok")                             # 롤백 뒤에도 연결은 정상
    assert pg.get_meta("after") == "ok"


def test_reconnects_after_connection_drop(pg):
    pg._conn._conn.close()                                 # 서버/네트워크 단절을 흉내 낸다
    assert pg.get_meta("schema_version") == "2"            # 트랜잭션 밖: 재접속 후 재시도
    assert pg.reconnects == 1
    with pytest.raises(LedgerUnavailable):
        with pg._tx():
            pg._conn._conn.close()                         # 트랜잭션 중 단절 → LedgerUnavailable, 조용히 정리
            pg._conn.execute("SELECT 1")
    assert pg.get_meta("schema_version") == "2"            # 그 뒤 정상
    assert pg.reconnects == 2


def test_signal_log_append_rows_and_export(pg):
    from lake_executor import signal_log as sl
    sig, raw = _signal()
    accounts = [{"name": "bybit", "exchange": "bybit", "enabled": True, "symbol": "BTCUSDT", "leverage": 5,
                 "margin_mode": "isolated", "position_mode": "hedge", "qty_multiplier": 1.0, "live_possible": False}]
    row = sl.signal_to_row(sig, "new", now_ms(), accounts)
    row.update({"mark_price": 80000.0, "last_price": 80001.0, "price_at_ms": now_ms(), "price_source": "stub"})
    assert pg.append_signal_log(row) is True and pg.append_signal_log(row) is False
    got = pg.signal_log_rows(mode="test")[0]
    assert got["mark_price"] == 80000.0 and got["accounts"][0]["name"] == "bybit" and pg.signal_log_count() == 1
    assert pg.insert_signal(sig, raw, now_ms()) == "new"
    pg.set_run_result("test", sig.event_id, "bybit", st.SIGNAL_DONE, None, "ok")
    pg.finalize_signal("test", sig.event_id)
    pg.insert_fill("x9", "test", sig.position_id, 0.001, 80010.0, now_ms(), order_id="o9", order_link_id="l9",
                   event_id=sig.event_id, account="bybit")
    ex = pg.export_signal_rows(mode="test")
    assert len(ex) == 1 and ex[0]["fill_avg_price"] == 80010.0 and ex[0]["run_status"] == "done"
    assert ex[0]["account_leverage"] == 5 and ex[0]["signal_status"] == "done"
    assert sl.rows_to_csv(ex).splitlines()[0] == ",".join(sl.EXPORT_COLUMNS)
