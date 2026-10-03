"""store.py 스키마 마이그레이션 (1단계 → 2, ARCHITECTURE_MULTI_EXCHANGE.md §4).

1단계 CREATE 문을 그대로 재현한 DB 파일에 lot/order/fill/report/meta 를 넣고 새 Store 로 열어
  - 행이 account='bybit' 로 옮겨지고 값이 보존되는지
  - meta 의 seq/observed/inconsistent 키가 `:bybit` 접미사로 이전되고 schema_version=2 가 기록되는지
  - 새 시그니처의 insert/조회가 동작하고 계정별로 분리되는지
  - 두 번 열어도 멱등인지
를 확인한다.
"""
from __future__ import annotations

import json
import sqlite3

import pytest

from lake_executor import store as st
from lake_executor.store import Store
from lake_executor.util import canonical_json, now_ms

V1_SCHEMA = """
CREATE TABLE IF NOT EXISTS signals (
  event_id        TEXT NOT NULL,
  mode            TEXT NOT NULL,
  body_sha256     TEXT NOT NULL,
  raw_body        BLOB NOT NULL,
  received_at_ms  INTEGER NOT NULL,
  position_id     TEXT NOT NULL,
  event_sequence  INTEGER NOT NULL,
  action          TEXT NOT NULL,
  strategy        TEXT NOT NULL,
  leg             TEXT NOT NULL,
  position_idx    INTEGER NOT NULL,
  qty_btc         REAL,
  status          TEXT NOT NULL,
  reason_code     TEXT,
  processed_at_ms INTEGER,
  note            TEXT,
  PRIMARY KEY (mode, event_id)
);
CREATE INDEX IF NOT EXISTS ix_signals_status ON signals(status, received_at_ms);
CREATE TABLE IF NOT EXISTS position_seq (
  mode TEXT NOT NULL, position_id TEXT NOT NULL, last_sequence INTEGER NOT NULL,
  PRIMARY KEY (mode, position_id)
);
CREATE TABLE IF NOT EXISTS lots (
  mode                TEXT NOT NULL,
  position_id         TEXT NOT NULL,
  strategy            TEXT NOT NULL,
  leg                 TEXT NOT NULL,
  position_idx        INTEGER NOT NULL,
  qty                 REAL NOT NULL,
  avg_entry           REAL,
  stop_loss           REAL,
  take_profit         TEXT,
  protection_revision INTEGER NOT NULL DEFAULT 0,
  protection_orders   TEXT,
  status              TEXT NOT NULL,
  opened_at_ms        INTEGER NOT NULL,
  updated_at_ms       INTEGER NOT NULL,
  closed_at_ms        INTEGER,
  last_event_id       TEXT,
  PRIMARY KEY (mode, position_id)
);
CREATE TABLE IF NOT EXISTS orders (
  order_link_id TEXT PRIMARY KEY,
  mode          TEXT NOT NULL,
  event_id      TEXT,
  position_id   TEXT NOT NULL,
  purpose       TEXT NOT NULL,
  side          TEXT NOT NULL,
  qty           REAL NOT NULL,
  reduce_only   INTEGER NOT NULL DEFAULT 0,
  order_id      TEXT,
  status        TEXT NOT NULL,
  trigger_price REAL,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  raw           TEXT
);
CREATE INDEX IF NOT EXISTS ix_orders_pos ON orders(mode, position_id, status);
CREATE TABLE IF NOT EXISTS fills (
  exec_id       TEXT PRIMARY KEY,
  mode          TEXT NOT NULL,
  order_id      TEXT,
  order_link_id TEXT,
  event_id      TEXT,
  position_id   TEXT NOT NULL,
  qty           REAL NOT NULL,
  price         REAL NOT NULL,
  exec_time_ms  INTEGER NOT NULL,
  reported      INTEGER NOT NULL DEFAULT 0,
  applied       INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS reports (
  report_id     TEXT PRIMARY KEY,
  mode          TEXT NOT NULL,
  sequence      INTEGER NOT NULL,
  kind          TEXT NOT NULL,
  body          BLOB NOT NULL,
  created_at_ms INTEGER NOT NULL,
  sent_at_ms    INTEGER,
  http_status   INTEGER,
  attempts      INTEGER NOT NULL DEFAULT 0,
  state         TEXT NOT NULL,
  note          TEXT,
  UNIQUE (mode, sequence)
);
CREATE INDEX IF NOT EXISTS ix_reports_state ON reports(mode, state, sequence);
CREATE TABLE IF NOT EXISTS ingress_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  received_at_ms INTEGER NOT NULL,
  code TEXT NOT NULL,
  event_id TEXT,
  body_sha256 TEXT,
  note TEXT
);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
"""

PID = "position-v1"
LINK = "lk0123456789abcdef0123456789abcdef"
EXEC = "exec-v1-1"
BODY = canonical_json({"schema_version": 1, "kind": "execution", "sequence": 7})


def make_v1_db(path: str) -> None:
    """1단계 스키마 DB 를 만들고 lot/order/fill/report/signal/meta 를 한 건씩 넣는다."""
    conn = sqlite3.connect(path)
    conn.executescript(V1_SCHEMA)
    t = now_ms()
    conn.execute("INSERT INTO signals(event_id,mode,body_sha256,raw_body,received_at_ms,position_id,event_sequence,action,"
                 "strategy,leg,position_idx,qty_btc,status,reason_code,processed_at_ms,note) "
                 "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 ("ev-1", "test", "ab", b"{}", t, PID, 1, "entry", "overheat", "short", 2, 0.002, "done", None, t, ""))
    conn.execute("INSERT INTO position_seq(mode,position_id,last_sequence) VALUES(?,?,?)", ("test", PID, 1))
    conn.execute("INSERT INTO lots(mode,position_id,strategy,leg,position_idx,qty,avg_entry,stop_loss,take_profit,"
                 "protection_revision,protection_orders,status,opened_at_ms,updated_at_ms,closed_at_ms,last_event_id) "
                 "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 ("test", PID, "overheat", "short", 2, 0.002, 86000.0, 90000.0, json.dumps([80000.0]), 1,
                  json.dumps({"sl": {"order_link_id": LINK + "s", "order_id": "o-sl", "price": 90000.0, "qty": 0.002},
                              "tp": []}),
                  "open", t, t, None, "ev-1"))
    conn.execute("INSERT INTO orders(order_link_id,mode,event_id,position_id,purpose,side,qty,reduce_only,order_id,status,"
                 "trigger_price,created_at_ms,updated_at_ms,raw) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 (LINK, "test", "ev-1", PID, "entry", "Sell", 0.002, 0, "o-1", "Filled", None, t, t, None))
    conn.execute("INSERT INTO fills(exec_id,mode,order_id,order_link_id,event_id,position_id,qty,price,exec_time_ms,"
                 "reported,applied) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                 (EXEC, "test", "o-1", LINK, "ev-1", PID, 0.002, 86000.0, t, 1, 1))
    conn.execute("INSERT INTO reports(report_id,mode,sequence,kind,body,created_at_ms,attempts,state) "
                 "VALUES(?,?,?,?,?,?,?,?)", ("r-test-7-abc", "test", 7, "execution", BODY, t, 1, "sent"))
    for k, v in (("seq:test", "7"), ("observed:test", str(t)), ("inconsistent:test", "1"),
                 ("inconsistent_note:test", "idx2 mismatch"), ("account_setup:live", "sig")):
        conn.execute("INSERT INTO meta(key,value) VALUES(?,?)", (k, v))
    conn.commit()
    conn.close()


@pytest.fixture
def v1_db_path(tmp_path):
    path = str(tmp_path / "lake.db")
    make_v1_db(path)
    return path


def _cols(store: Store, table: str) -> list[str]:
    return [r["name"] for r in store._q(f"PRAGMA table_info({table})")]


def test_v1_rows_are_migrated_to_account_bybit(v1_db_path):
    s = Store(v1_db_path)
    try:
        assert s.get_meta("schema_version") == "2"
        for t in ("lots", "orders", "fills", "reports"):
            assert "account" in _cols(s, t), t
        assert "signal_runs" in {r["name"] for r in s._q("SELECT name FROM sqlite_master WHERE type='table'")}

        lot = s.get_lot("test", "bybit", PID)
        assert lot is not None and lot["account"] == "bybit"
        assert lot["qty"] == pytest.approx(0.002) and lot["avg_entry"] == 86000.0 and lot["stop_loss"] == 90000.0
        assert lot["take_profit"] == [80000.0] and lot["protection_revision"] == 1 and lot["status"] == "open"
        assert lot["protection_orders"]["sl"]["order_id"] == "o-sl"
        assert s.open_lots("test", "bybit") == [lot] and s.open_lots("test", "okx") == []
        assert s.get_lot("test", "okx", PID) is None

        o = s.get_order(LINK, "bybit")
        assert o is not None and o["account"] == "bybit" and o["status"] == "Filled" and o["event_id"] == "ev-1"
        assert s.get_order(LINK, "okx") is None
        assert [r["order_link_id"] for r in s.orders_for_position("test", "bybit", PID, "entry")] == [LINK]

        fills = s.fills_for_order(LINK, "bybit")
        assert len(fills) == 1 and fills[0]["exec_id"] == EXEC and fills[0]["account"] == "bybit"
        assert fills[0]["reported"] == 1 and fills[0]["applied"] == 1

        rep = s._q("SELECT * FROM reports WHERE report_id=?", ("r-test-7-abc",))[0]
        assert rep["account"] == "bybit" and rep["sequence"] == 7 and rep["state"] == "sent"
        assert bytes(rep["body"]) == BODY
        assert s.recent_reports("test", "bybit")[0]["report_id"] == "r-test-7-abc"

        # 1단계 테이블은 그대로
        assert s.get_signal("ev-1", "test")["status"] == "done"
        assert s._q("SELECT last_sequence FROM position_seq WHERE mode=? AND position_id=?", ("test", PID))[0]["last_sequence"] == 1
    finally:
        s.close()


def test_meta_keys_get_account_suffix(v1_db_path):
    s = Store(v1_db_path)
    try:
        assert s.get_meta("seq:test:bybit") == "7" and s.get_meta("seq:test") is None
        assert s.get_meta("observed:test:bybit") is not None and s.get_meta("observed:test") is None
        assert s.is_inconsistent("test", "bybit") is True and s.is_inconsistent("test", "okx") is False
        assert s.inconsistent_note("test", "bybit") == "idx2 mismatch"
        assert s.get_meta("inconsistent:test") is None
        assert s.get_meta("account_setup:live") == "sig"   # store 가 관리하지 않는 키는 손대지 않는다
    finally:
        s.close()


def test_sequence_continues_per_account_after_migration(v1_db_path):
    s = Store(v1_db_path)
    try:
        a = s.allocate_report("test", "bybit", "snapshot", now_ms(), lambda rid, seq, ts, obs: {"sequence": seq}, canonical_json)
        assert a["sequence"] == 8 and a["account"] == "bybit"
        b = s.allocate_report("test", "okx", "snapshot", now_ms(), lambda rid, seq, ts, obs: {"sequence": seq}, canonical_json)
        assert b["sequence"] == 1 and b["account"] == "okx"
        assert a["report_id"] != b["report_id"]
        assert [r["sequence"] for r in s.pending_reports("test", "bybit")] == [8]
        assert [r["sequence"] for r in s.pending_reports("test", "okx")] == [1]
        assert len(s.pending_reports("test")) == 2
    finally:
        s.close()


def test_new_inserts_work_and_are_account_scoped(v1_db_path):
    s = Store(v1_db_path)
    try:
        t = now_ms()
        s.upsert_lot({"mode": "test", "account": "okx", "position_id": PID, "strategy": "overheat", "leg": "short",
                      "position_idx": 2, "qty": 0.01, "avg_entry": 86000.0, "status": "open", "opened_at_ms": t,
                      "updated_at_ms": t})
        assert s.get_lot("test", "okx", PID)["qty"] == pytest.approx(0.01)
        assert s.get_lot("test", "bybit", PID)["qty"] == pytest.approx(0.002)   # 다른 계정 lot 은 그대로
        assert {l["account"] for l in s.open_lots("test")} == {"bybit", "okx"}

        # 같은 order_link_id / exec_id 가 계정별로 공존한다 (거래소가 다르므로 충돌이 아님)
        s.insert_order(LINK, "test", PID, "entry", "Sell", 0.01, False, event_id="ev-1", account="okx")
        assert s.get_order(LINK, "okx")["qty"] == pytest.approx(0.01)
        assert s.get_order(LINK, "bybit")["qty"] == pytest.approx(0.002)
        s.update_order(LINK, "okx", status="submitted", order_id="okx-o-1")
        assert s.get_order(LINK, "okx")["status"] == "submitted" and s.get_order(LINK, "bybit")["status"] == "Filled"

        assert s.insert_fill(EXEC, "test", PID, 0.01, 86000.0, t, order_id="okx-o-1", order_link_id=LINK, account="okx") is True
        assert s.insert_fill(EXEC, "test", PID, 0.01, 86000.0, t, order_id="okx-o-1", order_link_id=LINK, account="okx") is False
        assert s.fills_for_order(LINK, "okx")[0]["reported"] == 0
        s.mark_fill_reported(EXEC, "okx")
        s.mark_fill_applied(EXEC, "okx")
        f = s.fills_for_order(LINK, "okx")[0]
        assert f["reported"] == 1 and f["applied"] == 1
        assert s.orders_with_status("test", "okx", ("submitted",)) and not s.orders_with_status("test", "bybit", ("submitted",))

        s.set_inconsistent("test", "okx", True, "okx mismatch")
        assert s.is_inconsistent("test", "okx") and s.inconsistent_note("test", "okx") == "okx mismatch"
        s.set_inconsistent("test", "bybit", False)
        assert s.is_inconsistent("test", "bybit") is False
    finally:
        s.close()


def test_signal_runs_and_finalize(v1_db_path):
    s = Store(v1_db_path)
    try:
        s.set_run_result("test", "ev-1", "bybit", st.SIGNAL_DONE)
        s.set_run_result("test", "ev-1", "okx", st.SIGNAL_REJECTED, "QTY_BELOW_MIN", "0.002 < 0.01")
        s.set_run_result("test", "ev-1", "okx", st.SIGNAL_REJECTED, "QTY_BELOW_MIN", "0.002 < 0.01")   # upsert 멱등
        runs = s.get_runs("test", "ev-1")
        assert [(r["account"], r["status"]) for r in runs] == [("bybit", "done"), ("okx", "rejected")]
        assert s.get_run("test", "ev-1", "okx")["reason_code"] == "QTY_BELOW_MIN"
        assert s.get_runs("live", "ev-1") == []
        status, reason, note = s.finalize_signal("test", "ev-1")
        assert status == "done" and reason is None and "okx=rejected/QTY_BELOW_MIN" in note
        assert s.get_signal("ev-1", "test")["status"] == "done"

        s.set_run_result("test", "ev-1", "toobit", st.SIGNAL_ERROR, "EXCHANGE_TIMEOUT")
        assert s.finalize_signal("test", "ev-1")[0:2] == ("error", "EXCHANGE_TIMEOUT")
        assert Store.summarize_runs([{"account": "a", "status": "rejected", "reason_code": "LIVE_DISABLED"}])[0:2] == \
            ("rejected", "LIVE_DISABLED")
        assert Store.summarize_runs([])[0:2] == ("error", "NO_TARGET_ACCOUNT")
        assert s.recent_runs("test", "okx")[0]["event_id"] == "ev-1" and len(s.recent_runs("test")) == 3
    finally:
        s.close()


def test_migration_is_idempotent_and_fresh_db_has_version(v1_db_path, tmp_path):
    s = Store(v1_db_path)
    s.close()
    s = Store(v1_db_path)   # 두 번째 열기: 마이그레이션 재실행 없음, 데이터 유지
    try:
        assert s.get_meta("schema_version") == "2"
        assert s.get_lot("test", "bybit", PID)["qty"] == pytest.approx(0.002)
        assert len(s._q("SELECT * FROM reports")) == 1
        assert not [r for r in s._q("SELECT name FROM sqlite_master WHERE type='table'") if r["name"].endswith("_old")]
    finally:
        s.close()
    fresh = Store(str(tmp_path / "fresh.db"))
    try:
        assert fresh.get_meta("schema_version") == "2"
        assert "account" in _cols(fresh, "lots")
    finally:
        fresh.close()


def test_v0_db_without_fills_applied_and_old_signals_pk(tmp_path):
    """더 오래된 DB(signals PK 가 event_id 단독, fills 에 applied 없음) 도 한 번에 올라온다."""
    path = str(tmp_path / "lake.db")
    conn = sqlite3.connect(path)
    conn.executescript(V1_SCHEMA.replace("PRIMARY KEY (mode, event_id)", "PRIMARY KEY (event_id)")
                       .replace(",\n  applied       INTEGER NOT NULL DEFAULT 0", ""))
    t = now_ms()
    conn.execute("INSERT INTO fills(exec_id,mode,order_id,order_link_id,event_id,position_id,qty,price,exec_time_ms,reported) "
                 "VALUES(?,?,?,?,?,?,?,?,?,?)", (EXEC, "live", "o-1", LINK, "ev-1", PID, 0.002, 86000.0, t, 1))
    conn.commit()
    conn.close()
    s = Store(path)
    try:
        f = s.fills_for_order(LINK, "bybit")[0]
        assert f["applied"] == 1 and f["account"] == "bybit"
        pk = [r["name"] for r in s._q("PRAGMA table_info(signals)") if r["pk"]]
        assert sorted(pk) == ["event_id", "mode"]
        assert s.get_meta("schema_version") == "2"
    finally:
        s.close()
