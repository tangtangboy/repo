"""SQLite 영속 원장. 모든 쓰기는 단일 Lock + BEGIN IMMEDIATE 로 직렬화한다.

스키마 버전 2 (meta `schema_version`) — 계정 단위 원장 (ARCHITECTURE_MULTI_EXCHANGE.md §4).

테이블
  signals      수신 신호 ((mode, event_id) PK — event_id 는 모드별로 유일). status: accepted → processing → done | rejected | error
               (여러 계정을 처리하면 모든 계정 run 이 끝난 뒤 종합: 하나라도 error 면 error)
  signal_runs  계정별 실행 결과 ((mode, event_id, account) PK). status/reason_code/processed_at_ms/note
  position_seq (mode, position_id) 별 마지막 event_sequence (순서 역전 거부)
  lots         전략 포지션 몫 원장 (mode, account, position_id) PK. 거래소는 같은 레그를 합산하므로 몫은 여기서 구분
  orders       우리가 낸 주문 ((account, order_link_id) PK; 멱등 키는 계정 안에서 유일)
  fills        체결 ((account, exec_id) PK; 같은 체결 중복 보고 방지). applied/reported 플래그로 재시작 복구 시 lot 반영·회신을 이어간다
  reports      회신 (report_id PK, (mode, account, sequence) UNIQUE). state: pending → sent | duplicate | failed | conflict
  ingress_log  인증/스키마/만료 실패 등 접수 거부 기록 (원본 본문은 저장하지 않음)
  meta         key/value (seq:<mode>:<account>, observed:<mode>:<account>, inconsistent:<mode>:<account>, schema_version, ...)

마이그레이션 (1단계 → 2): lots/orders/fills/reports 에 account 컬럼이 없으면 임시 이름으로 바꾸고 새 테이블을 만든 뒤
`Store(path, legacy_account=...)` 의 계정(기본 'bybit' — serve/check 는 `settings.legacy_account_name()` 을 넘긴다) 으로 복사한다.
meta 의 seq/observed/inconsistent 키도 같은 접미사로 옮긴다. 한 트랜잭션. 마이그레이션에 쓴 이름은 meta `legacy_account` 에 남긴다.

레그 단위 보호 상태 (`leg_protection:{mode}:{account}:{idx}` meta, JSON): 포지션 단위 TP/SL 을 쓰는 거래소에서
그 레그에 **지금 걸려 있다고 우리가 아는** SL/TP. 실행기가 set/clear 때마다 갱신하고 스냅샷·protection_missing 이 읽는다.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from typing import Callable

from .util import now_ms, sha256_hex

SCHEMA_VERSION = 2
DEFAULT_ACCOUNT = "bybit"   # 1단계 DB 의 모든 행이 속하는 계정

# 테이블별 DDL (마이그레이션이 개별 테이블을 다시 만들 수 있도록 분리)
TABLES: dict[str, str] = {
    "signals": """
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
)""",
    "signal_runs": """
CREATE TABLE IF NOT EXISTS signal_runs (
  mode            TEXT NOT NULL,
  event_id        TEXT NOT NULL,
  account         TEXT NOT NULL,
  status          TEXT NOT NULL,
  reason_code     TEXT,
  processed_at_ms INTEGER NOT NULL,
  note            TEXT,
  PRIMARY KEY (mode, event_id, account)
)""",
    "position_seq": """
CREATE TABLE IF NOT EXISTS position_seq (
  mode TEXT NOT NULL, position_id TEXT NOT NULL, last_sequence INTEGER NOT NULL,
  PRIMARY KEY (mode, position_id)
)""",
    "lots": """
CREATE TABLE IF NOT EXISTS lots (
  mode                TEXT NOT NULL,
  account             TEXT NOT NULL DEFAULT 'bybit',
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
  PRIMARY KEY (mode, account, position_id)
)""",
    "orders": """
CREATE TABLE IF NOT EXISTS orders (
  order_link_id TEXT NOT NULL,
  account       TEXT NOT NULL DEFAULT 'bybit',
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
  raw           TEXT,
  PRIMARY KEY (account, order_link_id)
)""",
    "fills": """
CREATE TABLE IF NOT EXISTS fills (
  exec_id       TEXT NOT NULL,
  account       TEXT NOT NULL DEFAULT 'bybit',
  mode          TEXT NOT NULL,
  order_id      TEXT,
  order_link_id TEXT,
  event_id      TEXT,
  position_id   TEXT NOT NULL,
  qty           REAL NOT NULL,
  price         REAL NOT NULL,
  exec_time_ms  INTEGER NOT NULL,
  reported      INTEGER NOT NULL DEFAULT 0,
  applied       INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (account, exec_id)
)""",
    "reports": """
CREATE TABLE IF NOT EXISTS reports (
  report_id     TEXT PRIMARY KEY,
  mode          TEXT NOT NULL,
  account       TEXT NOT NULL DEFAULT 'bybit',
  sequence      INTEGER NOT NULL,
  kind          TEXT NOT NULL,
  body          BLOB NOT NULL,
  created_at_ms INTEGER NOT NULL,
  sent_at_ms    INTEGER,
  http_status   INTEGER,
  attempts      INTEGER NOT NULL DEFAULT 0,
  state         TEXT NOT NULL,
  note          TEXT,
  UNIQUE (mode, account, sequence)
)""",
    "ingress_log": """
CREATE TABLE IF NOT EXISTS ingress_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  received_at_ms INTEGER NOT NULL,
  code TEXT NOT NULL,
  event_id TEXT,
  body_sha256 TEXT,
  note TEXT
)""",
    "meta": "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)",
}

INDEXES: dict[str, list[tuple[str, str]]] = {   # table -> [(index name, DDL)]
    "signals": [("ix_signals_status", "CREATE INDEX IF NOT EXISTS ix_signals_status ON signals(status, received_at_ms)")],
    "signal_runs": [("ix_runs_account", "CREATE INDEX IF NOT EXISTS ix_runs_account ON signal_runs(mode, account, processed_at_ms)")],
    "orders": [("ix_orders_pos", "CREATE INDEX IF NOT EXISTS ix_orders_pos ON orders(mode, account, position_id, status)")],
    "fills": [("ix_fills_link", "CREATE INDEX IF NOT EXISTS ix_fills_link ON fills(account, order_link_id)")],
    "reports": [("ix_reports_state", "CREATE INDEX IF NOT EXISTS ix_reports_state ON reports(mode, account, state, sequence)")],
}

# 계정 컬럼이 추가된 테이블 (1단계 → 2 마이그레이션 대상, 이 순서로)
_ACCOUNT_TABLES = ("lots", "orders", "fills", "reports")
# 계정 접미사가 붙는 meta 키 접두사
_ACCOUNT_META_PREFIXES = ("seq", "observed", "inconsistent", "inconsistent_note")

SCHEMA = ";\n".join([*TABLES.values(), *(ddl for lst in INDEXES.values() for _, ddl in lst)]) + ";\n"

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
REPORT_UNSENT = "unsent"   # 회신 URL 미설정 / report:false 계정: 저장만


def _row(cur_row, cur) -> dict | None:
    if cur_row is None:
        return None
    return {d[0]: cur_row[i] for i, d in enumerate(cur.description)}


def _loads(v):
    return json.loads(v) if v not in (None, "") else None


class Store:
    def __init__(self, path: str, legacy_account: str | None = None):
        """legacy_account: 1단계(account 컬럼 없음) DB 를 열 때 기존 행이 귀속될 계정 이름 (기본 DEFAULT_ACCOUNT).
        이미 마이그레이션된 DB 에는 영향이 없다."""
        self.path = path
        self.legacy_account = str(legacy_account or DEFAULT_ACCOUNT)
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None, timeout=30)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        with self._lock:
            self._migrate_signals_pk()
            self._migrate_fills_applied()
            self._migrate_accounts_v2()
            self._conn.executescript(SCHEMA)
            if self.get_meta("schema_version") is None:
                self.set_meta("schema_version", SCHEMA_VERSION)

    # ------------------------------------------------------------------ migrations
    def _table_exists(self, name: str) -> bool:
        cur = self._conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
        return cur.fetchone() is not None

    def _columns(self, table: str) -> list[str]:
        return [r[1] for r in self._conn.execute(f"PRAGMA table_info({table})").fetchall()]

    def _migrate_signals_pk(self) -> None:
        """구 스키마(event_id 단독 PK) → (mode, event_id) PK 로 테이블 재생성. 데이터는 그대로 복사."""
        if not self._table_exists("signals"):
            return
        pk_cols = sorted((r[5], r[1]) for r in self._conn.execute("PRAGMA table_info(signals)").fetchall() if r[5])
        if [c for _, c in pk_cols] == ["mode", "event_id"]:
            return
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            self._recreate_table("signals", extra_cols={})
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def _migrate_fills_applied(self) -> None:
        if not self._table_exists("fills"):
            return
        if "applied" not in self._columns("fills"):
            # 기존 행은 전부 lot 에 반영된 뒤 기록된 것으로 본다 (구 버전은 insert 직후 apply)
            self._conn.execute("ALTER TABLE fills ADD COLUMN applied INTEGER NOT NULL DEFAULT 1")

    def _migrate_accounts_v2(self) -> None:
        """1단계(account 컬럼 없음) → 2: lots/orders/fills/reports 재생성 + account='bybit' 복사, meta 키 이전."""
        todo = [t for t in _ACCOUNT_TABLES if self._table_exists(t) and "account" not in self._columns(t)]
        if not todo:
            return
        account = self.legacy_account
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            for t in todo:
                self._recreate_table(t, extra_cols={"account": account})
            # meta 키: seq:<mode> → seq:<mode>:<account> 등 (이미 새 키가 있으면 그대로 둔다)
            rows = self._conn.execute("SELECT key, value FROM meta").fetchall() if self._table_exists("meta") else []
            for key, value in rows:
                parts = key.split(":")
                if len(parts) == 2 and parts[0] in _ACCOUNT_META_PREFIXES:
                    new_key = f"{key}:{account}"
                    self._conn.execute("INSERT OR IGNORE INTO meta(key,value) VALUES(?,?)", (new_key, value))
                    self._conn.execute("DELETE FROM meta WHERE key=?", (key,))
            self._conn.execute(TABLES["meta"])
            self._conn.execute("INSERT INTO meta(key,value) VALUES('schema_version',?) "
                               "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(SCHEMA_VERSION),))
            self._conn.execute("INSERT INTO meta(key,value) VALUES('legacy_account',?) "
                               "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (account,))
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def _recreate_table(self, table: str, extra_cols: dict[str, str]) -> None:
        """table → table_old 로 이름을 바꾸고 현재 DDL 로 새로 만든 뒤 공통 컬럼 + extra_cols(상수) 를 복사한다.
        호출자가 트랜잭션을 연다."""
        old = f"{table}_old"
        self._conn.execute(f"DROP TABLE IF EXISTS {old}")
        self._conn.execute(f"ALTER TABLE {table} RENAME TO {old}")
        for ix_name, _ in INDEXES.get(table, []):
            self._conn.execute(f"DROP INDEX IF EXISTS {ix_name}")
        self._conn.execute(TABLES[table])
        for _, ddl in INDEXES.get(table, []):
            self._conn.execute(ddl)
        new_cols = self._columns(table)
        old_cols = [c for c in self._columns(old) if c in new_cols]
        cols = list(old_cols) + [c for c in extra_cols if c not in old_cols]
        selects = list(old_cols) + ["?" for c in extra_cols if c not in old_cols]
        params = [v for c, v in extra_cols.items() if c not in old_cols]
        self._conn.execute(f"INSERT INTO {table}({','.join(cols)}) SELECT {','.join(selects)} FROM {old}", params)
        self._conn.execute(f"DROP TABLE {old}")

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

    def is_inconsistent(self, mode: str, account: str) -> bool:
        return self.get_meta(f"inconsistent:{mode}:{account}", "0") == "1"

    def set_inconsistent(self, mode: str, account: str, flag: bool, note: str = "") -> None:
        with self._tx():
            for k, v in ((f"inconsistent:{mode}:{account}", "1" if flag else "0"),
                         (f"inconsistent_note:{mode}:{account}", note)):
                self._conn.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                                   (k, str(v)))

    def inconsistent_note(self, mode: str, account: str) -> str:
        return str(self.get_meta(f"inconsistent_note:{mode}:{account}", "") or "")

    def ledger_accounts(self) -> dict[str, dict]:
        """원장(lots/orders/fills/reports)에 나타나는 계정 이름 → {"open_lots": n, "pending_reports": n, "rows": n}.
        기동 시 설정의 accounts 와 대조해 고아 계정(이름이 바뀐 1단계 계정 등)을 잡는 용도."""
        out: dict[str, dict] = {}
        for t in _ACCOUNT_TABLES:
            for r in self._q(f"SELECT account, COUNT(*) AS n FROM {t} GROUP BY account"):
                d = out.setdefault(str(r["account"]), {"open_lots": 0, "pending_reports": 0, "rows": 0})
                d["rows"] += int(r["n"] or 0)
        for r in self._q("SELECT account, COUNT(*) AS n FROM lots WHERE status=? GROUP BY account", (LOT_OPEN,)):
            out.setdefault(str(r["account"]), {"open_lots": 0, "pending_reports": 0, "rows": 0})["open_lots"] = int(r["n"] or 0)
        for r in self._q("SELECT account, COUNT(*) AS n FROM reports WHERE state=? GROUP BY account", (REPORT_PENDING,)):
            out.setdefault(str(r["account"]), {"open_lots": 0, "pending_reports": 0, "rows": 0})["pending_reports"] = int(r["n"] or 0)
        return out

    # ------------------------------------------------------------------ 레그 단위 보호 상태 (포지션 단위 TP/SL 거래소)
    @staticmethod
    def _leg_key(mode: str, account: str, position_idx: int) -> str:
        return f"leg_protection:{mode}:{account}:{int(position_idx)}"

    def get_leg_protection(self, mode: str, account: str, position_idx: int) -> dict | None:
        """그 레그에 지금 걸려 있다고 아는 포지션 단위 보호
        {"stop_loss","take_profit","tp_i","position_id","set_at_ms"[,"cleared":true]}.
        해제된 레그는 stop_loss/take_profit=None 인 기록으로 남는다(기록 자체가 없는 None 은 '이 코드가 그 레그를 본 적 없음' —
        실행기가 open lot 의 po.position 으로 한 번 시드한다)."""
        v = self.get_meta(self._leg_key(mode, account, position_idx))
        if not v:
            return None
        try:
            d = json.loads(v)
        except (TypeError, ValueError):
            return None
        return d if isinstance(d, dict) else None

    def set_leg_protection(self, mode: str, account: str, position_idx: int, state: dict | None) -> None:
        """state=None → 해제 기록 (stop_loss/take_profit=None, cleared=true)."""
        key = self._leg_key(mode, account, position_idx)
        if state is None:
            state = {"stop_loss": None, "take_profit": None, "tp_i": None, "position_id": None,
                     "set_at_ms": now_ms(), "cleared": True}
        with self._tx():
            self._conn.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                               (key, json.dumps(state)))

    # ------------------------------------------------------------------ ingress log
    def log_ingress(self, code: str, event_id: str | None = None, body: bytes | None = None, note: str = "") -> None:
        with self._tx():
            self._conn.execute("INSERT INTO ingress_log(received_at_ms,code,event_id,body_sha256,note) VALUES(?,?,?,?,?)",
                               (now_ms(), code, event_id, sha256_hex(body) if body is not None else None, note[:500]))

    def prune_ingress_log(self, max_age_ms: int, max_rows: int) -> int:
        """오래된/초과분 ingress_log 삭제 (무한 증가 방지). 삭제 행 수 반환."""
        removed = 0
        with self._tx():
            cur = self._conn.execute("DELETE FROM ingress_log WHERE received_at_ms < ?", (now_ms() - int(max_age_ms),))
            removed += cur.rowcount
            cur = self._conn.execute(
                "DELETE FROM ingress_log WHERE id <= (SELECT id FROM ingress_log ORDER BY id DESC LIMIT 1 OFFSET ?)",
                (int(max_rows),))
            removed += max(0, cur.rowcount)
        return removed

    # ------------------------------------------------------------------ signals
    def insert_signal(self, sig, raw: bytes, received_at_ms: int) -> str:
        """영속 접수. 반환: 'new' | 'duplicate' | 'conflict' | 'sequence_conflict'.

        event_id 유일성은 **mode 별**이다 (TEST 키 보유자가 LIVE event_id 를 선점할 수 없도록).
        - 같은 (mode, event_id) + 같은 본문 → duplicate (호출자는 200, 재실행 없음)
        - 같은 (mode, event_id) + 다른 본문 → conflict (409)
        - event_sequence <= (mode, position_id) 의 마지막 값 → sequence_conflict (409)
        """
        sha = sha256_hex(raw)
        mode = sig.mode.value
        with self._tx():
            cur = self._conn.execute("SELECT body_sha256 FROM signals WHERE mode=? AND event_id=?", (mode, sig.event_id))
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

    def get_signal(self, event_id: str, mode: str | None = None) -> dict | None:
        """mode 가 None 이면 어느 모드든 첫 행 (운영 조회 편의). 실행 경로는 mode 를 넘긴다."""
        if mode is None:
            return self._q1("SELECT * FROM signals WHERE event_id=? ORDER BY received_at_ms LIMIT 1", (event_id,))
        return self._q1("SELECT * FROM signals WHERE mode=? AND event_id=?", (mode, event_id))

    def claim_next_signal(self) -> dict | None:
        """가장 오래된 accepted 신호를 processing 으로 바꾸고 반환 (없으면 None)."""
        with self._tx():
            cur = self._conn.execute(
                "SELECT * FROM signals WHERE status=? ORDER BY received_at_ms ASC, rowid ASC LIMIT 1", (SIGNAL_ACCEPTED,))
            r = cur.fetchone()
            if r is None:
                return None
            row = {d[0]: r[i] for i, d in enumerate(cur.description)}
            self._conn.execute("UPDATE signals SET status=? WHERE mode=? AND event_id=?",
                               (SIGNAL_PROCESSING, row["mode"], row["event_id"]))
            row["status"] = SIGNAL_PROCESSING
            return row

    def processing_signals(self) -> list[dict]:
        return self._q("SELECT * FROM signals WHERE status=? ORDER BY received_at_ms", (SIGNAL_PROCESSING,))

    def set_signal_result(self, event_id: str, status: str, reason_code: str | None = None, note: str = "",
                          mode: str | None = None) -> None:
        """mode 를 주면 그 (mode, event_id) 행만, None 이면 event_id 가 같은 모든 행 (하위 호환)."""
        with self._tx():
            if mode is None:
                self._conn.execute("UPDATE signals SET status=?, reason_code=?, processed_at_ms=?, note=? WHERE event_id=?",
                                   (status, reason_code, now_ms(), note[:500], event_id))
            else:
                self._conn.execute("UPDATE signals SET status=?, reason_code=?, processed_at_ms=?, note=? "
                                   "WHERE mode=? AND event_id=?",
                                   (status, reason_code, now_ms(), note[:500], mode, event_id))

    def recent_signals(self, limit: int = 50) -> list[dict]:
        rows = self._q("SELECT event_id,mode,received_at_ms,position_id,event_sequence,action,strategy,leg,position_idx,"
                       "qty_btc,status,reason_code,processed_at_ms FROM signals ORDER BY received_at_ms DESC LIMIT ?", (limit,))
        return rows

    # ------------------------------------------------------------------ signal runs (계정별 실행 결과)
    def set_run_result(self, mode: str, event_id: str, account: str, status: str, reason_code: str | None = None,
                       note: str = "") -> None:
        """(mode, event_id, account) 의 실행 결과를 기록/갱신한다. status ∈ done | rejected | error (| processing)."""
        with self._tx():
            self._conn.execute(
                "INSERT INTO signal_runs(mode,event_id,account,status,reason_code,processed_at_ms,note) VALUES(?,?,?,?,?,?,?) "
                "ON CONFLICT(mode,event_id,account) DO UPDATE SET status=excluded.status, reason_code=excluded.reason_code, "
                "processed_at_ms=excluded.processed_at_ms, note=excluded.note",
                (mode, event_id, account, status, reason_code, now_ms(), note[:500]))

    def get_runs(self, mode: str, event_id: str) -> list[dict]:
        """한 신호의 계정별 실행 결과 (account 순)."""
        return self._q("SELECT * FROM signal_runs WHERE mode=? AND event_id=? ORDER BY account", (mode, event_id))

    def get_run(self, mode: str, event_id: str, account: str) -> dict | None:
        return self._q1("SELECT * FROM signal_runs WHERE mode=? AND event_id=? AND account=?", (mode, event_id, account))

    def recent_runs(self, mode: str, account: str | None = None, limit: int = 50) -> list[dict]:
        if account is None:
            return self._q("SELECT * FROM signal_runs WHERE mode=? ORDER BY processed_at_ms DESC LIMIT ?", (mode, limit))
        return self._q("SELECT * FROM signal_runs WHERE mode=? AND account=? ORDER BY processed_at_ms DESC LIMIT ?",
                       (mode, account, limit))

    @staticmethod
    def summarize_runs(runs: list[dict]) -> tuple[str, str | None, str]:
        """계정별 run → signals 종합 (status, reason_code, note).
        하나라도 error → error; 전부 rejected → rejected(첫 reason_code); 그 외(하나라도 done) → done.
        note 는 'account=status/reason' 을 ';' 로 이어 붙인다 (계정별 결과 추적용)."""
        if not runs:
            return SIGNAL_ERROR, "NO_TARGET_ACCOUNT", ""
        statuses = [r["status"] for r in runs]
        note = ";".join(f"{r['account']}={r['status']}" + (f"/{r['reason_code']}" if r.get("reason_code") else "")
                        for r in runs)
        if any(s == SIGNAL_ERROR for s in statuses):
            first = next(r for r in runs if r["status"] == SIGNAL_ERROR)
            return SIGNAL_ERROR, first.get("reason_code"), note
        if all(s == SIGNAL_REJECTED for s in statuses):
            return SIGNAL_REJECTED, runs[0].get("reason_code"), note
        return SIGNAL_DONE, None, note

    def finalize_signal(self, mode: str, event_id: str) -> tuple[str, str | None, str]:
        """signal_runs 를 종합해 signals.status 를 확정하고 (status, reason_code, note) 를 돌려준다."""
        status, reason, note = self.summarize_runs(self.get_runs(mode, event_id))
        self.set_signal_result(event_id, status, reason, note, mode=mode)
        return status, reason, note

    # ------------------------------------------------------------------ lots
    def get_lot(self, mode: str, account: str, position_id: str) -> dict | None:
        r = self._q1("SELECT * FROM lots WHERE mode=? AND account=? AND position_id=?", (mode, account, position_id))
        return self._lot_out(r)

    def open_lots(self, mode: str, account: str | None = None) -> list[dict]:
        """account=None 이면 모든 계정 (운영 조회용). 실행 경로는 계정을 넘긴다."""
        return self.lots(mode, account, LOT_OPEN)

    def lots(self, mode: str, account: str | None = None, status: str | None = None) -> list[dict]:
        q = "SELECT * FROM lots WHERE mode=?"
        params: list = [mode]
        if account is not None:
            q += " AND account=?"
            params.append(account)
        if status is not None:
            q += " AND status=?"
            params.append(status)
        q += " ORDER BY opened_at_ms, account"
        return [self._lot_out(r) for r in self._q(q, params)]

    def lots_with_pending_cancel(self, mode: str, account: str | None = None) -> list[dict]:
        """닫혔지만 보호가 남아 있는 lot (취소/해제 실패 → reconcile 이 재시도):
        lot 단위 보호주문 항목(sl/tp 에 order_link_id) 또는 포지션 단위 보호(kind=position, position 이 아직 설정값)."""
        q = ("SELECT * FROM lots WHERE mode=? AND status=? AND "
             "(protection_orders LIKE '%order_link_id%' OR protection_orders LIKE '%position%')")
        params: list = [mode, LOT_CLOSED]
        if account is not None:
            q += " AND account=?"
            params.append(account)
        q += " ORDER BY closed_at_ms"
        out = []
        for r in self._q(q, params):
            lot = self._lot_out(r)
            po = lot.get("protection_orders") or {}
            if po.get("sl") or po.get("tp"):
                out.append(lot)
            elif po.get("kind") == "position" and po.get("position"):
                out.append(lot)
        return out

    @staticmethod
    def _lot_out(r):
        if r is None:
            return None
        r = dict(r)
        r["take_profit"] = _loads(r.get("take_profit"))
        r["protection_orders"] = _loads(r.get("protection_orders")) or {"sl": None, "tp": []}
        return r

    def upsert_lot(self, lot: dict) -> None:
        """lot 키: mode, account, position_id, strategy, leg, position_idx, qty, avg_entry, stop_loss, take_profit(list|None),
        protection_revision, protection_orders(dict), status, opened_at_ms, updated_at_ms, closed_at_ms, last_event_id.
        opened_at_ms 도 갱신한다 (닫힌 lot 을 같은 position_id 로 다시 열면 새 lot 인스턴스)."""
        account = lot.get("account")
        if not account:
            raise ValueError("lot requires account")
        with self._tx():
            self._conn.execute(
                "INSERT INTO lots(mode,account,position_id,strategy,leg,position_idx,qty,avg_entry,stop_loss,take_profit,"
                "protection_revision,protection_orders,status,opened_at_ms,updated_at_ms,closed_at_ms,last_event_id) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(mode,account,position_id) DO UPDATE SET "
                "strategy=excluded.strategy, leg=excluded.leg, position_idx=excluded.position_idx, qty=excluded.qty, "
                "avg_entry=excluded.avg_entry, stop_loss=excluded.stop_loss, take_profit=excluded.take_profit, "
                "protection_revision=excluded.protection_revision, protection_orders=excluded.protection_orders, "
                "status=excluded.status, opened_at_ms=excluded.opened_at_ms, updated_at_ms=excluded.updated_at_ms, "
                "closed_at_ms=excluded.closed_at_ms, last_event_id=excluded.last_event_id",
                (lot["mode"], account, lot["position_id"], lot["strategy"], lot["leg"], int(lot["position_idx"]),
                 float(lot["qty"]), lot.get("avg_entry"), lot.get("stop_loss"),
                 json.dumps(lot.get("take_profit")) if lot.get("take_profit") is not None else None,
                 int(lot.get("protection_revision", 0)),
                 json.dumps(lot.get("protection_orders") or {"sl": None, "tp": []}),
                 lot.get("status", LOT_OPEN), int(lot.get("opened_at_ms") or now_ms()), int(lot.get("updated_at_ms") or now_ms()),
                 lot.get("closed_at_ms"), lot.get("last_event_id")))

    # ------------------------------------------------------------------ orders
    def insert_order(self, order_link_id: str, mode: str, position_id: str, purpose: str, side: str, qty: float,
                     reduce_only: bool, event_id: str | None = None, status: str = "new", trigger_price: float | None = None,
                     order_id: str | None = None, raw: str | None = None, *, account: str) -> None:
        t = now_ms()
        with self._tx():
            self._conn.execute(
                "INSERT OR REPLACE INTO orders(order_link_id,account,mode,event_id,position_id,purpose,side,qty,reduce_only,"
                "order_id,status,trigger_price,created_at_ms,updated_at_ms,raw) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (order_link_id, account, mode, event_id, position_id, purpose, side, float(qty), 1 if reduce_only else 0,
                 order_id, status, trigger_price, t, t, raw))

    def update_order(self, order_link_id: str, account: str, **fields) -> None:
        if not fields:
            return
        cols = ", ".join(f"{k}=?" for k in fields)
        with self._tx():
            self._conn.execute(f"UPDATE orders SET {cols}, updated_at_ms=? WHERE account=? AND order_link_id=?",
                               (*fields.values(), now_ms(), account, order_link_id))

    def get_order(self, order_link_id: str, account: str) -> dict | None:
        return self._q1("SELECT * FROM orders WHERE account=? AND order_link_id=?", (account, order_link_id))

    def orders_by_link_prefix(self, account: str, prefix: str, limit: int = 5) -> list[dict]:
        """order_link_id 가 prefix 로 시작하는 주문 (OKX clOrdId 32자 절단 ↔ 34자 link 역매핑용). prefix 는 영숫자만."""
        if not prefix or not prefix.isalnum():
            return []
        return self._q("SELECT * FROM orders WHERE account=? AND order_link_id LIKE ? ORDER BY created_at_ms DESC LIMIT ?",
                       (account, prefix + "%", int(limit)))

    def orders_for_position(self, mode: str, account: str, position_id: str, purpose: str | None = None) -> list[dict]:
        if purpose:
            return self._q("SELECT * FROM orders WHERE mode=? AND account=? AND position_id=? AND purpose=? "
                           "ORDER BY created_at_ms", (mode, account, position_id, purpose))
        return self._q("SELECT * FROM orders WHERE mode=? AND account=? AND position_id=? ORDER BY created_at_ms",
                       (mode, account, position_id))

    def orders_with_status(self, mode: str, account: str, statuses: tuple[str, ...] | list[str],
                           purposes: tuple[str, ...] | list[str] | None = None) -> list[dict]:
        """상태가 statuses 에 드는 주문 (재확인 대상: 타임아웃/불명 주문)."""
        if not statuses:
            return []
        q = f"SELECT * FROM orders WHERE mode=? AND account=? AND status IN ({','.join('?' * len(statuses))})"
        params: list = [mode, account, *statuses]
        if purposes:
            q += f" AND purpose IN ({','.join('?' * len(purposes))})"
            params.extend(purposes)
        q += " ORDER BY created_at_ms"
        return self._q(q, params)

    # ------------------------------------------------------------------ fills
    def insert_fill(self, exec_id: str, mode: str, position_id: str, qty: float, price: float, exec_time_ms: int,
                    order_id: str | None = None, order_link_id: str | None = None, event_id: str | None = None, *,
                    account: str) -> bool:
        """새 체결이면 True, 이미 있던 (account, exec_id) 면 False."""
        with self._tx():
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO fills(exec_id,account,mode,order_id,order_link_id,event_id,position_id,qty,price,"
                "exec_time_ms) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (exec_id, account, mode, order_id, order_link_id, event_id, position_id, float(qty), float(price),
                 int(exec_time_ms)))
            return cur.rowcount == 1

    def mark_fill_reported(self, exec_id: str, account: str) -> None:
        with self._tx():
            self._conn.execute("UPDATE fills SET reported=1 WHERE account=? AND exec_id=?", (account, exec_id))

    def mark_fill_applied(self, exec_id: str, account: str) -> None:
        with self._tx():
            self._conn.execute("UPDATE fills SET applied=1 WHERE account=? AND exec_id=?", (account, exec_id))

    def fills_for_order(self, order_link_id: str, account: str) -> list[dict]:
        return self._q("SELECT * FROM fills WHERE account=? AND order_link_id=? ORDER BY exec_time_ms",
                       (account, order_link_id))

    # ------------------------------------------------------------------ reports
    def allocate_report(self, mode: str, account: str, kind: str, observed_at_ms: int,
                        build: Callable[[str, int, int, int], dict], serialize: Callable[[dict], bytes],
                        mark_fill_reported: str | None = None) -> dict:
        """(mode, account) 별 sequence 를 원자적으로 증가시키고 회신을 pending 으로 영속 저장.

        build(report_id, sequence, ts, observed_at_ms) -> 본문 dict
        observed_at_ms 는 같은 (mode, account) 안에서 단조 증가하도록 클램프하고 ts 를 넘지 않게 한다.
        mark_fill_reported 가 주어지면 같은 트랜잭션에서 그 (account, exec_id) 의 fills.reported=1 을 기록한다
        (회신 생성과 '보고됨' 표시 사이에 죽어도 같은 fill_id 가 새 report_id 로 다시 나가지 않도록).
        반환: {report_id, mode, account, sequence, kind, body(bytes), ts, observed_at_ms}
        """
        with self._tx():
            cur = self._conn.execute("SELECT value FROM meta WHERE key=?", (f"seq:{mode}:{account}",))
            r = cur.fetchone()
            seq = (int(r[0]) if r else 0) + 1
            cur = self._conn.execute("SELECT value FROM meta WHERE key=?", (f"observed:{mode}:{account}",))
            r = cur.fetchone()
            prev_obs = int(r[0]) if r else 0
            ts = now_ms()
            obs = min(int(observed_at_ms), ts)
            obs = max(obs, prev_obs)
            report_id = f"r-{mode}-{account}-{seq}-{sha256_hex(f'{mode}|{account}|{seq}|{ts}')[:12]}"
            body = build(report_id, seq, ts, obs)
            raw = serialize(body)
            self._conn.execute(
                "INSERT INTO reports(report_id,mode,account,sequence,kind,body,created_at_ms,attempts,state) "
                "VALUES(?,?,?,?,?,?,?,0,?)",
                (report_id, mode, account, seq, kind, raw, ts, REPORT_PENDING))
            self._conn.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                               (f"seq:{mode}:{account}", str(seq)))
            self._conn.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                               (f"observed:{mode}:{account}", str(obs)))
            if mark_fill_reported:
                self._conn.execute("UPDATE fills SET reported=1 WHERE account=? AND exec_id=?", (account, mark_fill_reported))
            return {"report_id": report_id, "mode": mode, "account": account, "sequence": seq, "kind": kind, "body": raw,
                    "ts": ts, "observed_at_ms": obs}

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

    def pending_reports(self, mode: str, account: str | None = None, limit: int = 100) -> list[dict]:
        """account=None 이면 모든 계정 (sequence 는 계정별이므로 전송 직렬화는 계정을 넘겨 호출한다)."""
        if account is None:
            return self._q("SELECT * FROM reports WHERE mode=? AND state=? ORDER BY account, sequence LIMIT ?",
                           (mode, REPORT_PENDING, limit))
        return self._q("SELECT * FROM reports WHERE mode=? AND account=? AND state=? ORDER BY sequence LIMIT ?",
                       (mode, account, REPORT_PENDING, limit))

    def recent_reports(self, mode: str, account: str | None = None, limit: int = 30) -> list[dict]:
        cols = "report_id,mode,account,sequence,kind,created_at_ms,sent_at_ms,http_status,attempts,state,note"
        if account is None:
            return self._q(f"SELECT {cols} FROM reports WHERE mode=? ORDER BY created_at_ms DESC, sequence DESC LIMIT ?",
                           (mode, limit))
        return self._q(f"SELECT {cols} FROM reports WHERE mode=? AND account=? ORDER BY sequence DESC LIMIT ?",
                       (mode, account, limit))


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
