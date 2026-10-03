"""SQLite 영속 원장. 모든 쓰기는 단일 Lock + BEGIN IMMEDIATE 로 직렬화한다.

테이블
  signals      수신 신호 (event_id PK). status: accepted → processing → done | rejected | error
  position_seq (mode, position_id) 별 마지막 event_sequence (순서 역전 거부)
  lots         전략 포지션 몫 원장 (mode, position_id) PK. Bybit 는 같은 레그를 합산하므로 몫은 여기서 구분
  orders       우리가 낸 주문 (order_link_id PK; 멱등 키)
  fills        체결 (exec_id PK; 같은 체결 중복 보고 방지)
  reports      회신 (report_id PK, (mode, sequence) UNIQUE). state: pending → sent | duplicate | failed | conflict
  ingress_log  인증/스키마/만료 실패 등 접수 거부 기록 (원본 본문은 저장하지 않음)
  meta         key/value (seq:<mode>, observed:<mode>, inconsistent:<mode>, ...)
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from typing import Callable

from .util import now_ms, sha256_hex

SCHEMA = """
CREATE TABLE IF NOT EXISTS signals (
  event_id        TEXT PRIMARY KEY,
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
  note            TEXT
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
  reported      INTEGER NOT NULL DEFAULT 0
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

SIGNAL_ACCEPTED = "accepted"
SIGNAL_PROCESSING = "processing"
SIGNAL_DONE = "done"
SIGNAL_REJECTED = "rejected"
SIGNAL_ERROR = "error"

LOT_OPEN = "open"
LOT_CLOSED = "closed"

REPORT_PENDING = "pending"
REPORT_SENT = "sent"
REPORT_DUPLICATE = "duplicate"
REPORT_FAILED = "failed"
REPORT_CONFLICT = "conflict"
REPORT_UNSENT = "unsent"   # 회신 URL 미설정: 저장만


def _row(cur_row, cur) -> dict | None:
    if cur_row is None:
        return None
    return {d[0]: cur_row[i] for i, d in enumerate(cur.description)}


def _loads(v):
    return json.loads(v) if v not in (None, "") else None


class Store:
    def __init__(self, path: str):
        self.path = path
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None, timeout=30)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        with self._lock:
            self._conn.executescript(SCHEMA)

    def close(self):
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------ helpers
    def _tx(self):
        """with store._tx(): ... — BEGIN IMMEDIATE / COMMIT / ROLLBACK."""
        return _Tx(self)

    def _q(self, sql, params=()) -> list[dict]:
        with self._lock:
            cur = self._conn.execute(sql, params)
            rows = cur.fetchall()
            return [{d[0]: r[i] for i, d in enumerate(cur.description)} for r in rows]

    def _q1(self, sql, params=()) -> dict | None:
        rows = self._q(sql, params)
        return rows[0] if rows else None

    # ------------------------------------------------------------------ meta
    def get_meta(self, key: str, default=None):
        r = self._q1("SELECT value FROM meta WHERE key=?", (key,))
        return r["value"] if r else default

    def set_meta(self, key: str, value) -> None:
        with self._tx():
            self._conn.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                               (key, str(value)))

    def is_inconsistent(self, mode: str) -> bool:
        return self.get_meta(f"inconsistent:{mode}", "0") == "1"

    def set_inconsistent(self, mode: str, flag: bool, note: str = "") -> None:
        self.set_meta(f"inconsistent:{mode}", "1" if flag else "0")
        self.set_meta(f"inconsistent_note:{mode}", note)

    # ------------------------------------------------------------------ ingress log
    def log_ingress(self, code: str, event_id: str | None = None, body: bytes | None = None, note: str = "") -> None:
        with self._tx():
            self._conn.execute("INSERT INTO ingress_log(received_at_ms,code,event_id,body_sha256,note) VALUES(?,?,?,?,?)",
                               (now_ms(), code, event_id, sha256_hex(body) if body is not None else None, note[:500]))

    # ------------------------------------------------------------------ signals
    def insert_signal(self, sig, raw: bytes, received_at_ms: int) -> str:
        """영속 접수. 반환: 'new' | 'duplicate' | 'conflict' | 'sequence_conflict'.

        - 같은 event_id + 같은 본문 → duplicate (호출자는 200, 재실행 없음)
        - 같은 event_id + 다른 본문 → conflict (409)
        - event_sequence <= (mode, position_id) 의 마지막 값 → sequence_conflict (409)
        """
        sha = sha256_hex(raw)
        mode = sig.mode.value
        with self._tx():
            cur = self._conn.execute("SELECT body_sha256 FROM signals WHERE event_id=?", (sig.event_id,))
            r = cur.fetchone()
            if r is not None:
                return "duplicate" if r[0] == sha else "conflict"
            cur = self._conn.execute("SELECT last_sequence FROM position_seq WHERE mode=? AND position_id=?",
                                     (mode, sig.position_id))
            r = cur.fetchone()
            last = r[0] if r else 0
            if sig.event_sequence <= last:
                return "sequence_conflict"
            self._conn.execute(
                "INSERT INTO signals(event_id,mode,body_sha256,raw_body,received_at_ms,position_id,event_sequence,"
                "action,strategy,leg,position_idx,qty_btc,status) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (sig.event_id, mode, sha, raw, received_at_ms, sig.position_id, sig.event_sequence,
                 sig.action.value, sig.strategy.value, sig.leg.value, sig.position_idx, sig.qty_btc, SIGNAL_ACCEPTED))
            self._conn.execute(
                "INSERT INTO position_seq(mode,position_id,last_sequence) VALUES(?,?,?) "
                "ON CONFLICT(mode,position_id) DO UPDATE SET last_sequence=excluded.last_sequence",
                (mode, sig.position_id, sig.event_sequence))
            return "new"

    def get_signal(self, event_id: str) -> dict | None:
        return self._q1("SELECT * FROM signals WHERE event_id=?", (event_id,))

    def claim_next_signal(self) -> dict | None:
        """가장 오래된 accepted 신호를 processing 으로 바꾸고 반환 (없으면 None)."""
        with self._tx():
            cur = self._conn.execute(
                "SELECT * FROM signals WHERE status=? ORDER BY received_at_ms ASC, rowid ASC LIMIT 1", (SIGNAL_ACCEPTED,))
            r = cur.fetchone()
            if r is None:
                return None
            row = {d[0]: r[i] for i, d in enumerate(cur.description)}
            self._conn.execute("UPDATE signals SET status=? WHERE event_id=?", (SIGNAL_PROCESSING, row["event_id"]))
            row["status"] = SIGNAL_PROCESSING
            return row

    def processing_signals(self) -> list[dict]:
        return self._q("SELECT * FROM signals WHERE status=? ORDER BY received_at_ms", (SIGNAL_PROCESSING,))

    def set_signal_result(self, event_id: str, status: str, reason_code: str | None = None, note: str = "") -> None:
        with self._tx():
            self._conn.execute("UPDATE signals SET status=?, reason_code=?, processed_at_ms=?, note=? WHERE event_id=?",
                               (status, reason_code, now_ms(), note[:500], event_id))

    def recent_signals(self, limit: int = 50) -> list[dict]:
        rows = self._q("SELECT event_id,mode,received_at_ms,position_id,event_sequence,action,strategy,leg,position_idx,"
                       "qty_btc,status,reason_code,processed_at_ms FROM signals ORDER BY received_at_ms DESC LIMIT ?", (limit,))
        return rows

    # ------------------------------------------------------------------ lots
    def get_lot(self, mode: str, position_id: str) -> dict | None:
        r = self._q1("SELECT * FROM lots WHERE mode=? AND position_id=?", (mode, position_id))
        return self._lot_out(r)

    def open_lots(self, mode: str) -> list[dict]:
        return [self._lot_out(r) for r in self._q("SELECT * FROM lots WHERE mode=? AND status=? ORDER BY opened_at_ms",
                                                   (mode, LOT_OPEN))]

    @staticmethod
    def _lot_out(r):
        if r is None:
            return None
        r = dict(r)
        r["take_profit"] = _loads(r.get("take_profit"))
        r["protection_orders"] = _loads(r.get("protection_orders")) or {"sl": None, "tp": []}
        return r

    def upsert_lot(self, lot: dict) -> None:
        """lot 키: mode, position_id, strategy, leg, position_idx, qty, avg_entry, stop_loss, take_profit(list|None),
        protection_revision, protection_orders(dict), status, opened_at_ms, updated_at_ms, closed_at_ms, last_event_id"""
        with self._tx():
            self._conn.execute(
                "INSERT INTO lots(mode,position_id,strategy,leg,position_idx,qty,avg_entry,stop_loss,take_profit,"
                "protection_revision,protection_orders,status,opened_at_ms,updated_at_ms,closed_at_ms,last_event_id) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(mode,position_id) DO UPDATE SET "
                "strategy=excluded.strategy, leg=excluded.leg, position_idx=excluded.position_idx, qty=excluded.qty, "
                "avg_entry=excluded.avg_entry, stop_loss=excluded.stop_loss, take_profit=excluded.take_profit, "
                "protection_revision=excluded.protection_revision, protection_orders=excluded.protection_orders, "
                "status=excluded.status, updated_at_ms=excluded.updated_at_ms, closed_at_ms=excluded.closed_at_ms, "
                "last_event_id=excluded.last_event_id",
                (lot["mode"], lot["position_id"], lot["strategy"], lot["leg"], int(lot["position_idx"]), float(lot["qty"]),
                 lot.get("avg_entry"), lot.get("stop_loss"),
                 json.dumps(lot.get("take_profit")) if lot.get("take_profit") is not None else None,
                 int(lot.get("protection_revision", 0)),
                 json.dumps(lot.get("protection_orders") or {"sl": None, "tp": []}),
                 lot.get("status", LOT_OPEN), int(lot.get("opened_at_ms") or now_ms()), int(lot.get("updated_at_ms") or now_ms()),
                 lot.get("closed_at_ms"), lot.get("last_event_id")))

    # ------------------------------------------------------------------ orders
    def insert_order(self, order_link_id: str, mode: str, position_id: str, purpose: str, side: str, qty: float,
                     reduce_only: bool, event_id: str | None = None, status: str = "new", trigger_price: float | None = None,
                     order_id: str | None = None, raw: str | None = None) -> None:
        t = now_ms()
        with self._tx():
            self._conn.execute(
                "INSERT OR REPLACE INTO orders(order_link_id,mode,event_id,position_id,purpose,side,qty,reduce_only,order_id,"
                "status,trigger_price,created_at_ms,updated_at_ms,raw) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (order_link_id, mode, event_id, position_id, purpose, side, float(qty), 1 if reduce_only else 0, order_id,
                 status, trigger_price, t, t, raw))

    def update_order(self, order_link_id: str, **fields) -> None:
        if not fields:
            return
        cols = ", ".join(f"{k}=?" for k in fields)
        with self._tx():
            self._conn.execute(f"UPDATE orders SET {cols}, updated_at_ms=? WHERE order_link_id=?",
                               (*fields.values(), now_ms(), order_link_id))

    def get_order(self, order_link_id: str) -> dict | None:
        return self._q1("SELECT * FROM orders WHERE order_link_id=?", (order_link_id,))

    def orders_for_position(self, mode: str, position_id: str, purpose: str | None = None) -> list[dict]:
        if purpose:
            return self._q("SELECT * FROM orders WHERE mode=? AND position_id=? AND purpose=? ORDER BY created_at_ms",
                           (mode, position_id, purpose))
        return self._q("SELECT * FROM orders WHERE mode=? AND position_id=? ORDER BY created_at_ms", (mode, position_id))

    # ------------------------------------------------------------------ fills
    def insert_fill(self, exec_id: str, mode: str, position_id: str, qty: float, price: float, exec_time_ms: int,
                    order_id: str | None = None, order_link_id: str | None = None, event_id: str | None = None) -> bool:
        """새 체결이면 True, 이미 있던 exec_id 면 False."""
        with self._tx():
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO fills(exec_id,mode,order_id,order_link_id,event_id,position_id,qty,price,exec_time_ms) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (exec_id, mode, order_id, order_link_id, event_id, position_id, float(qty), float(price), int(exec_time_ms)))
            return cur.rowcount == 1

    def mark_fill_reported(self, exec_id: str) -> None:
        with self._tx():
            self._conn.execute("UPDATE fills SET reported=1 WHERE exec_id=?", (exec_id,))

    def fills_for_order(self, order_link_id: str) -> list[dict]:
        return self._q("SELECT * FROM fills WHERE order_link_id=? ORDER BY exec_time_ms", (order_link_id,))

    # ------------------------------------------------------------------ reports
    def allocate_report(self, mode: str, kind: str, observed_at_ms: int,
                        build: Callable[[str, int, int, int], dict], serialize: Callable[[dict], bytes]) -> dict:
        """mode 별 sequence 를 원자적으로 증가시키고 회신을 pending 으로 영속 저장.

        build(report_id, sequence, ts, observed_at_ms) -> 본문 dict
        observed_at_ms 는 같은 mode 안에서 단조 증가하도록 클램프하고 ts 를 넘지 않게 한다.
        반환: {report_id, mode, sequence, kind, body(bytes), ts, observed_at_ms}
        """
        with self._tx():
            cur = self._conn.execute("SELECT value FROM meta WHERE key=?", (f"seq:{mode}",))
            r = cur.fetchone()
            seq = (int(r[0]) if r else 0) + 1
            cur = self._conn.execute("SELECT value FROM meta WHERE key=?", (f"observed:{mode}",))
            r = cur.fetchone()
            prev_obs = int(r[0]) if r else 0
            ts = now_ms()
            obs = min(int(observed_at_ms), ts)
            obs = max(obs, prev_obs)
            report_id = f"r-{mode}-{seq}-{sha256_hex(f'{mode}|{seq}|{ts}')[:12]}"
            body = build(report_id, seq, ts, obs)
            raw = serialize(body)
            self._conn.execute(
                "INSERT INTO reports(report_id,mode,sequence,kind,body,created_at_ms,attempts,state) VALUES(?,?,?,?,?,?,0,?)",
                (report_id, mode, seq, kind, raw, ts, REPORT_PENDING))
            self._conn.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                               (f"seq:{mode}", str(seq)))
            self._conn.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                               (f"observed:{mode}", str(obs)))
            return {"report_id": report_id, "mode": mode, "sequence": seq, "kind": kind, "body": raw, "ts": ts,
                    "observed_at_ms": obs}

    def update_report(self, report_id: str, state: str, http_status: int | None = None, attempts: int | None = None,
                      sent: bool = False, note: str = "") -> None:
        with self._tx():
            sets = ["state=?", "note=?"]
            params: list = [state, note[:500]]
            if http_status is not None:
                sets.append("http_status=?"); params.append(int(http_status))
            if attempts is not None:
                sets.append("attempts=?"); params.append(int(attempts))
            if sent:
                sets.append("sent_at_ms=?"); params.append(now_ms())
            params.append(report_id)
            self._conn.execute(f"UPDATE reports SET {', '.join(sets)} WHERE report_id=?", params)

    def pending_reports(self, mode: str, limit: int = 100) -> list[dict]:
        return self._q("SELECT * FROM reports WHERE mode=? AND state=? ORDER BY sequence LIMIT ?",
                       (mode, REPORT_PENDING, limit))

    def recent_reports(self, mode: str, limit: int = 30) -> list[dict]:
        return self._q("SELECT report_id,mode,sequence,kind,created_at_ms,sent_at_ms,http_status,attempts,state,note "
                       "FROM reports WHERE mode=? ORDER BY sequence DESC LIMIT ?", (mode, limit))


class _Tx:
    def __init__(self, store: Store):
        self.s = store

    def __enter__(self):
        self.s._lock.acquire()
        try:
            self.s._conn.execute("BEGIN IMMEDIATE")
        except Exception:
            self.s._lock.release()
            raise
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                self.s._conn.execute("COMMIT")
            else:
                self.s._conn.execute("ROLLBACK")
        finally:
            self.s._lock.release()
        return False
