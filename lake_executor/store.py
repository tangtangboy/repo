"""영속 원장. SQLite(파일) 또는 Postgres(DATABASE_URL, 예: Supabase) 두 백엔드를 같은 API 로 쓴다.
모든 쓰기는 단일 Lock + 트랜잭션(BEGIN IMMEDIATE / BEGIN) 으로 직렬화한다.

백엔드 선택: `Store(path_or_url)` — `postgres://` / `postgresql://` 로 시작하면 Postgres(psycopg3), 아니면 SQLite 파일.
SQL 은 SQLite 문법으로 쓰고 `_PgConn` 이 실행 직전에 변환한다(`translate_pg`): `?`→`%s`, `INSERT OR IGNORE`→`ON CONFLICT DO NOTHING`,
`BEGIN IMMEDIATE`→`BEGIN`, DDL 타입(INTEGER→BIGINT, REAL→DOUBLE PRECISION, BLOB→BYTEA, AUTOINCREMENT→BIGSERIAL).
방언이 갈리는 두 곳(rowid 정렬, 바이트열 포함 검색)은 `Store` 가 백엔드별 문장을 고른다.
Postgres 는 전용 스키마(기본 `lake_executor`) 에 테이블을 만들고 세션마다 `search_path` 를 고정한다 — **세션 풀러(5432) 전용**
(트랜잭션 풀러 6543 은 SET 이 유지되지 않는다). 연결이 끊기면 트랜잭션 밖 문장은 재접속 후 한 번 재시도하고,
트랜잭션 안이면 `LedgerUnavailable` 을 올린다 (수신기 → 503, 실행기 → 백오프). 1단계→2 마이그레이션은 SQLite 에만 있다.

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
import logging
import os
import re
import sqlite3
import threading
import time
import urllib.parse
from typing import Callable

from .util import now_ms, sha256_hex

log = logging.getLogger("lake_executor.store")

SCHEMA_VERSION = 2
DEFAULT_ACCOUNT = "bybit"   # 1단계 DB 의 모든 행이 속하는 계정
DEFAULT_PG_SCHEMA = "lake_executor"
_PG_SCHEMA_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


class LedgerUnavailable(RuntimeError):
    """원장 DB(Postgres) 에 닿지 않는다. 수신기는 503(상대가 재전송), 실행기는 백오프 뒤 재시도."""


def is_postgres_url(target: str | None) -> bool:
    return str(target or "").strip().lower().startswith(("postgres://", "postgresql://"))


def describe_target(target: str) -> str:
    """로그/대시보드용 표시 문자열. 비밀번호는 절대 포함하지 않는다."""
    if not is_postgres_url(target):
        return f"sqlite {target}"
    try:
        u = urllib.parse.urlsplit(target)
        host = u.hostname or "?"
        port = u.port or 5432
        db = (u.path or "/").lstrip("/") or "postgres"
        return f"postgres {host}:{port}/{db}"
    except ValueError:
        return "postgres (unparseable url)"


def _pg_ddl(sql: str) -> str:
    """SQLite DDL → Postgres DDL (타입만 바꾼다)."""
    sql = re.sub(r"\bINTEGER PRIMARY KEY AUTOINCREMENT\b", "BIGSERIAL PRIMARY KEY", sql)
    sql = re.sub(r"\bBLOB\b", "BYTEA", sql)
    sql = re.sub(r"\bREAL\b", "DOUBLE PRECISION", sql)
    sql = re.sub(r"\bINTEGER\b", "BIGINT", sql)
    return sql


def translate_pg(sql: str, has_params: bool) -> str:
    """SQLite 문장 → Postgres 문장. 저장소의 SQL 은 SQLite 문법으로 쓰고 실행 직전에만 바꾼다.
    - BEGIN IMMEDIATE → BEGIN
    - INSERT OR IGNORE INTO … → INSERT INTO … ON CONFLICT DO NOTHING
    - CREATE TABLE/INDEX 의 타입 이름
    - 파라미터가 있으면 '%' → '%%'(LIKE 리터럴 보호) 뒤 '?' → '%s'"""
    s = sql.strip().rstrip(";")
    up = s.upper()
    if up.startswith("BEGIN"):
        return "BEGIN"
    if up.startswith("CREATE TABLE") or up.startswith("CREATE INDEX"):
        return _pg_ddl(s)
    if up.startswith("INSERT OR IGNORE INTO"):
        s = "INSERT INTO" + s[len("INSERT OR IGNORE INTO"):] + " ON CONFLICT DO NOTHING"
    if has_params:
        s = s.replace("%", "%%").replace("?", "%s")
    return s


class _PgConn:
    """psycopg 연결을 sqlite3.Connection 처럼 쓰게 하는 얇은 어댑터 (execute / executescript / close).
    autocommit 연결에 BEGIN/COMMIT/ROLLBACK 을 SQL 로 직접 보내므로 _Tx 가 그대로 동작한다."""

    def __init__(self, url: str, schema: str = DEFAULT_PG_SCHEMA, connect_timeout: int = 15):
        try:
            import psycopg  # noqa: F401
        except ImportError as e:  # pragma: no cover - 배포 환경 의존성
            raise RuntimeError("DATABASE_URL is set but psycopg is not installed: pip install 'psycopg[binary]'") from e
        if not _PG_SCHEMA_RE.match(schema or ""):
            raise ValueError(f"invalid database schema name: {schema!r}")
        self.url, self.schema, self.timeout = url, schema, int(connect_timeout)
        self._conn = None
        self.in_tx = False
        self.broken = False
        self.reconnects = 0
        self._connect(create_schema=True)

    def _connect(self, create_schema: bool = False) -> None:
        import psycopg
        last: Exception | None = None
        for attempt in range(3):
            try:
                conn = psycopg.connect(self.url, autocommit=True, connect_timeout=self.timeout,
                                       application_name="lake-executor", keepalives=1, keepalives_idle=30,
                                       keepalives_interval=10, keepalives_count=3)
                with conn.cursor() as cur:
                    if create_schema:
                        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{self.schema}"')
                    cur.execute(f'SET search_path TO "{self.schema}"')
                    cur.execute("SET extra_float_digits = 3")   # Supabase 기본 0 → float8 이 15자리로 잘려 읽힘; 3 = 정확한 왕복
                if getattr(self, "_connected_once", False):
                    self.reconnects += 1
                self._connected_once = True
                self._conn, self.in_tx, self.broken = conn, False, False
                return
            except psycopg.Error as e:
                last = e
                time.sleep(0.5 * (attempt + 1))
        raise LedgerUnavailable(f"postgres connect failed: {type(last).__name__}")

    def execute(self, sql: str, params=()):
        import psycopg
        params = tuple(params or ())
        q = translate_pg(sql, bool(params))
        up = q.upper()
        for attempt in (0, 1):
            if self._conn is None or self.broken:
                if self.in_tx:
                    self.in_tx = False
                    raise LedgerUnavailable("postgres connection lost inside a transaction")
                self._connect()
            try:
                cur = self._conn.cursor()
                cur.execute(q, params or None)
                if up == "BEGIN":
                    self.in_tx = True
                elif up in ("COMMIT", "ROLLBACK"):
                    self.in_tx = False
                return cur
            except (psycopg.OperationalError, psycopg.InterfaceError) as e:
                self.broken = True
                try:
                    self._conn.close()
                except Exception:  # noqa: BLE001
                    pass
                self._conn = None
                log.warning("postgres connection error (%s); %s", type(e).__name__,
                            "aborting transaction" if self.in_tx else "reconnecting")
                if self.in_tx or attempt == 1:
                    self.in_tx = False
                    raise LedgerUnavailable(f"postgres error: {type(e).__name__}") from e
        raise LedgerUnavailable("postgres unreachable")  # pragma: no cover

    def executemany(self, sql: str, seq_of_params) -> None:
        """여러 행을 한 문장으로 (psycopg 가 가능하면 파이프라인으로 보낸다 — 복제 배치용)."""
        import psycopg
        rows = [tuple(p) for p in seq_of_params]
        if not rows:
            return
        q = translate_pg(sql, True)
        for attempt in (0, 1):
            if self._conn is None or self.broken:
                if self.in_tx:
                    self.in_tx = False
                    raise LedgerUnavailable("postgres connection lost inside a transaction")
                self._connect()
            try:
                with self._conn.cursor() as cur:
                    cur.executemany(q, rows)
                return
            except (psycopg.OperationalError, psycopg.InterfaceError) as e:
                self.broken = True
                try:
                    self._conn.close()
                except Exception:  # noqa: BLE001
                    pass
                self._conn = None
                if self.in_tx or attempt == 1:
                    self.in_tx = False
                    raise LedgerUnavailable(f"postgres error: {type(e).__name__}") from e
        raise LedgerUnavailable("postgres unreachable")  # pragma: no cover

    def executescript(self, script: str) -> None:
        for stmt in script.split(";"):
            if stmt.strip():
                self.execute(stmt)

    def rollback_quiet(self) -> None:
        """트랜잭션 중 연결이 끊긴 뒤의 ROLLBACK: 끊긴 연결에는 보낼 수 없으므로 상태만 정리한다."""
        if self.broken or self._conn is None:
            self.in_tx = False
            return
        try:
            self.execute("ROLLBACK")
        except LedgerUnavailable:
            self.in_tx = False

    def ping_ms(self) -> float:
        t = time.monotonic()
        self.execute("SELECT 1").fetchone()
        return (time.monotonic() - t) * 1000.0

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass
        self._conn = None

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
    # 라이브 신호 로그 (백테스트용 데이터셋). 주문 경로 밖에서 signal_log.py 의 백그라운드 스레드가 쓴다 (append-only).
    # 수신 시각·신호가 말한 가격·수신 순간의 시장가·수량·계정 사이징 문맥을 한 행에 담는다. (mode, event_id) 로 중복 제거.
    "signal_log": """
CREATE TABLE IF NOT EXISTS signal_log (
  id                     INTEGER PRIMARY KEY AUTOINCREMENT,
  received_at_ms         INTEGER NOT NULL,
  mode                   TEXT NOT NULL,
  event_id               TEXT NOT NULL,
  position_id            TEXT NOT NULL,
  event_sequence         INTEGER NOT NULL,
  strategy               TEXT NOT NULL,
  strategy_name          TEXT,
  action                 TEXT NOT NULL,
  leg                    TEXT NOT NULL,
  position_idx           INTEGER NOT NULL,
  exchange               TEXT,
  symbol                 TEXT,
  qty_btc                REAL,
  expected_qty_btc_after REAL,
  reference_price        REAL,
  stop_loss              REAL,
  take_profit            TEXT,
  protection_revision    INTEGER,
  signal_ts              INTEGER,
  expires_at_ms          INTEGER,
  ingest_result          TEXT NOT NULL,
  mark_price             REAL,
  last_price             REAL,
  price_at_ms            INTEGER,
  price_source           TEXT,
  accounts               TEXT,
  UNIQUE (mode, event_id)
)""",
}

# ---- 계정(사용자) 트레이드 히스토리 적재 (history.py 가 쓰고 metrics.py/대시보드가 읽는다). 전부 (mode, account) 단위.
TABLES.update({
    "account_executions": """
CREATE TABLE IF NOT EXISTS account_executions (
  mode           TEXT NOT NULL,
  account        TEXT NOT NULL,
  exec_id        TEXT NOT NULL,
  exchange       TEXT,
  symbol         TEXT,
  order_id       TEXT,
  order_link_id  TEXT,
  side           TEXT,
  qty            REAL NOT NULL,
  price          REAL NOT NULL,
  fee            REAL,
  fee_currency   TEXT,
  exec_type      TEXT,
  closed_size    REAL,
  position_idx   INTEGER,
  exec_time_ms   INTEGER NOT NULL,
  ingested_at_ms INTEGER NOT NULL,
  raw            TEXT,
  PRIMARY KEY (mode, account, exec_id)
)""",
    "account_closed_pnl": """
CREATE TABLE IF NOT EXISTS account_closed_pnl (
  mode            TEXT NOT NULL,
  account         TEXT NOT NULL,
  pnl_id          TEXT NOT NULL,
  exchange        TEXT,
  symbol          TEXT,
  order_id        TEXT,
  side            TEXT,
  qty             REAL NOT NULL,
  avg_entry_price REAL,
  avg_exit_price  REAL,
  closed_pnl      REAL NOT NULL,
  leverage        REAL,
  position_idx    INTEGER,
  created_at_ms   INTEGER NOT NULL,
  updated_at_ms   INTEGER,
  ingested_at_ms  INTEGER NOT NULL,
  raw             TEXT,
  PRIMARY KEY (mode, account, pnl_id)
)""",
    "account_equity": """
CREATE TABLE IF NOT EXISTS account_equity (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  mode           TEXT NOT NULL,
  account        TEXT NOT NULL,
  ts_ms          INTEGER NOT NULL,
  total_equity   REAL NOT NULL,
  wallet_balance REAL,
  unrealised_pnl REAL,
  available      REAL,
  source         TEXT
)""",
    "account_cashflow": """
CREATE TABLE IF NOT EXISTS account_cashflow (
  mode           TEXT NOT NULL,
  account        TEXT NOT NULL,
  flow_id        TEXT NOT NULL,
  ts_ms          INTEGER NOT NULL,
  type           TEXT NOT NULL,
  amount         REAL NOT NULL,
  currency       TEXT,
  raw            TEXT,
  ingested_at_ms INTEGER NOT NULL,
  PRIMARY KEY (mode, account, flow_id)
)""",
    "sync_state": """
CREATE TABLE IF NOT EXISTS sync_state (
  mode          TEXT NOT NULL,
  account       TEXT NOT NULL,
  kind          TEXT NOT NULL,
  last_ts_ms    INTEGER NOT NULL DEFAULT 0,
  cursor        TEXT,
  updated_at_ms INTEGER NOT NULL,
  note          TEXT,
  PRIMARY KEY (mode, account, kind)
)""",
})

EXEC_COLS = ("mode", "account", "exec_id", "exchange", "symbol", "order_id", "order_link_id", "side", "qty", "price", "fee",
             "fee_currency", "exec_type", "closed_size", "position_idx", "exec_time_ms", "ingested_at_ms", "raw")
CLOSED_PNL_COLS = ("mode", "account", "pnl_id", "exchange", "symbol", "order_id", "side", "qty", "avg_entry_price",
                   "avg_exit_price", "closed_pnl", "leverage", "position_idx", "created_at_ms", "updated_at_ms",
                   "ingested_at_ms", "raw")
CASHFLOW_COLS = ("mode", "account", "flow_id", "ts_ms", "type", "amount", "currency", "raw", "ingested_at_ms")

REPL_QUEUE_DDL = ("CREATE TABLE IF NOT EXISTS repl_queue (id INTEGER PRIMARY KEY AUTOINCREMENT, tbl TEXT NOT NULL, "
                  "op TEXT NOT NULL, row TEXT NOT NULL, created_at_ms INTEGER NOT NULL)")
REPL_DEAD_DDL = ("CREATE TABLE IF NOT EXISTS repl_dead (id INTEGER PRIMARY KEY, tbl TEXT NOT NULL, op TEXT NOT NULL, "
                 "row TEXT NOT NULL, created_at_ms INTEGER NOT NULL, error TEXT NOT NULL, dead_at_ms INTEGER NOT NULL)")

SIGNAL_LOG_COLS = ("received_at_ms", "mode", "event_id", "position_id", "event_sequence", "strategy", "strategy_name",
                   "action", "leg", "position_idx", "exchange", "symbol", "qty_btc", "expected_qty_btc_after",
                   "reference_price", "stop_loss", "take_profit", "protection_revision", "signal_ts", "expires_at_ms",
                   "ingest_result", "mark_price", "last_price", "price_at_ms", "price_source", "accounts")

INDEXES: dict[str, list[tuple[str, str]]] = {   # table -> [(index name, DDL)]
    "signals": [("ix_signals_status", "CREATE INDEX IF NOT EXISTS ix_signals_status ON signals(status, received_at_ms)")],
    "signal_runs": [("ix_runs_account", "CREATE INDEX IF NOT EXISTS ix_runs_account ON signal_runs(mode, account, processed_at_ms)")],
    "orders": [("ix_orders_pos", "CREATE INDEX IF NOT EXISTS ix_orders_pos ON orders(mode, account, position_id, status)")],
    "fills": [("ix_fills_link", "CREATE INDEX IF NOT EXISTS ix_fills_link ON fills(account, order_link_id)")],
    "reports": [("ix_reports_state", "CREATE INDEX IF NOT EXISTS ix_reports_state ON reports(mode, account, state, sequence)")],
    "signal_log": [("ix_signal_log_time", "CREATE INDEX IF NOT EXISTS ix_signal_log_time ON signal_log(mode, received_at_ms)")],
    "account_executions": [("ix_acct_exec_time", "CREATE INDEX IF NOT EXISTS ix_acct_exec_time ON account_executions(mode, account, exec_time_ms)")],
    "account_closed_pnl": [("ix_acct_pnl_time", "CREATE INDEX IF NOT EXISTS ix_acct_pnl_time ON account_closed_pnl(mode, account, created_at_ms)")],
    "account_equity": [("ix_acct_equity_time", "CREATE INDEX IF NOT EXISTS ix_acct_equity_time ON account_equity(mode, account, ts_ms)")],
    "account_cashflow": [("ix_acct_cash_time", "CREATE INDEX IF NOT EXISTS ix_acct_cash_time ON account_cashflow(mode, account, ts_ms)")],
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
    def __init__(self, path: str, legacy_account: str | None = None, schema: str = DEFAULT_PG_SCHEMA):
        """path: SQLite 파일 경로 또는 Postgres URL(postgres://…). schema: Postgres 전용 스키마 이름.
        legacy_account: 1단계(account 컬럼 없음) SQLite DB 를 열 때 기존 행이 귀속될 계정 이름 (기본 DEFAULT_ACCOUNT).
        이미 마이그레이션된 DB 에는 영향이 없다."""
        self.path = path
        self.legacy_account = str(legacy_account or DEFAULT_ACCOUNT)
        self._lock = threading.RLock()
        if is_postgres_url(path):
            self.backend = "postgres"
            self.schema = schema or DEFAULT_PG_SCHEMA
            self._conn = _PgConn(path, self.schema)
            self._rowid = "event_id"                       # Postgres 에는 rowid 가 없다 → 같은 ms 안에서는 event_id 순
            self._contains_body = "position(? in body) > 0"   # bytea 포함 검색
        else:
            self.backend = "sqlite"
            self.schema = ""
            d = os.path.dirname(path)
            if d:
                os.makedirs(d, exist_ok=True)
            self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None, timeout=30)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.execute("PRAGMA recursive_triggers=ON")   # INSERT OR REPLACE 의 암묵 DELETE 도 복제 트리거에 잡히게
            self._rowid = "rowid"
            self._contains_body = "instr(body, ?) > 0"
        with self._lock:
            if self.backend == "sqlite":
                self._migrate_signals_pk()
                self._migrate_fills_applied()
                self._migrate_accounts_v2()
            self._conn.executescript(SCHEMA)
            if self.get_meta("schema_version") is None:
                self.set_meta("schema_version", SCHEMA_VERSION)

    # ------------------------------------------------------------------ 복제 로그 (replica.py; SQLite 장부 전용)
    REPL_EXCLUDE = ("repl_queue", "repl_dead", "sqlite_sequence")

    def replicated_tables(self) -> list[str]:
        """복제 대상 테이블 (sqlite 내부 테이블과 큐 자체 제외), 이름순."""
        with self._lock:
            if self.backend == "sqlite":
                rows = self._conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
                names = [r[0] for r in rows]
            else:
                names = sorted(TABLES.keys())
        # 알려진(TABLES) 테이블만: 마이그레이션이 남긴 *_old 같은 임시 테이블은 복제본에 없다
        return [n for n in names if n not in self.REPL_EXCLUDE and not n.startswith("sqlite_") and n in TABLES]

    def table_schema(self, table: str) -> dict:
        """{"cols": [...], "types": {col: declared type}, "pk": [pk cols in key order]} (PRAGMA table_info; SQLite 전용)."""
        if self.backend != "sqlite":
            raise RuntimeError("table_schema is only available on the SQLite ledger")
        with self._lock:
            info = self._conn.execute(f"PRAGMA table_info({table})").fetchall()
        cols = [r[1] for r in info]
        types = {r[1]: (r[2] or "").upper() for r in info}
        pk = [r[1] for r in sorted((r for r in info if r[5]), key=lambda r: r[5])]
        return {"cols": cols, "types": types, "pk": pk}

    def count_rows(self, table: str) -> int:
        with self._lock:
            return int(self._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])

    def _repl_triggers(self) -> list[str]:
        with self._lock:
            rows = self._conn.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'repl\\_%' ESCAPE '\\'").fetchall()
        return [r[0] for r in rows]

    def enable_replication_log(self) -> list[str]:
        """repl_queue 테이블 + 테이블마다 INSERT/UPDATE/DELETE(+PK 변경) 트리거. 매번 다시 만든다(열 변경 대비). 반환: 테이블 목록."""
        if self.backend != "sqlite":
            raise RuntimeError("replication log is only available on the SQLite ledger")
        with self._lock:
            self._conn.execute(REPL_QUEUE_DDL)
            self._conn.execute(REPL_DEAD_DDL)
            self._conn.execute("DROP INDEX IF EXISTS ix_repl_queue_id")      # INTEGER PRIMARY KEY 가 곧 rowid — 인덱스 불필요
            for trg in self._repl_triggers():
                self._conn.execute(f"DROP TRIGGER IF EXISTS {trg}")
            tables = self.replicated_tables()
            for t in tables:
                s = self.table_schema(t)
                if not s["pk"]:
                    continue
                if len(s["cols"]) > 60:
                    raise RuntimeError(f"table {t} has {len(s['cols'])} columns; json_object() supports at most 63 pairs")

                def jexpr(prefix: str, cols: list[str]) -> str:
                    # REAL 은 17자리로 찍어(정확히 복원) 문자열로, BLOB 은 hex 로, 그 외는 값 그대로 (blob 이 섞여 들어와도 트리거가 죽지 않게 hex)
                    parts = []
                    for c in cols:
                        v = f"{prefix}.{c}"
                        if s["types"].get(c) == "BLOB":
                            parts.append(f"'{c}', CASE WHEN {v} IS NULL THEN NULL ELSE hex({v}) END")
                        else:
                            # 선언 타입이 BLOB 이 아닌 열에 bytes 가 들어오면 {"$hex": …} 로 표시해 복제기가 복원한다 (트리거가 죽지 않게)
                            parts.append(f"'{c}', CASE WHEN typeof({v})='real' THEN printf('%!.17g', {v}) "
                                         f"WHEN typeof({v})='blob' THEN json_object('$hex', hex({v})) ELSE {v} END")
                    return "json_object(" + ", ".join(parts) + ")"
                ts = "CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)"
                ins = f"INSERT INTO repl_queue(tbl, op, row, created_at_ms) VALUES('{t}', '%s', %s, {ts})"
                # meta.replica_* (복제 링크 등 이 장부-복제본 쌍의 메타) 는 복제하지 않는다
                when_new = " WHEN (NEW.key NOT LIKE 'replica\\_%' ESCAPE '\\')" if t == "meta" else ""
                when_old = " WHEN (OLD.key NOT LIKE 'replica\\_%' ESCAPE '\\')" if t == "meta" else ""
                self._conn.execute(f"CREATE TRIGGER repl_{t}_i AFTER INSERT ON {t}{when_new} BEGIN {ins % ('upsert', jexpr('NEW', s['cols']))}; END")
                self._conn.execute(f"CREATE TRIGGER repl_{t}_u AFTER UPDATE ON {t}{when_new} BEGIN {ins % ('upsert', jexpr('NEW', s['cols']))}; END")
                self._conn.execute(f"CREATE TRIGGER repl_{t}_d AFTER DELETE ON {t}{when_old} BEGIN {ins % ('delete', jexpr('OLD', s['pk']))}; END")
                pk_changed = " OR ".join(f"OLD.{c} IS NOT NEW.{c}" for c in s["pk"])
                when_k = f" WHEN (({pk_changed})" + (" AND OLD.key NOT LIKE 'replica\\_%' ESCAPE '\\'" if t == "meta" else "") + ")"
                self._conn.execute(f"CREATE TRIGGER repl_{t}_k AFTER UPDATE OF {', '.join(s['pk'])} ON {t}{when_k} "
                                   f"BEGIN {ins % ('delete', jexpr('OLD', s['pk']))}; END")
        return tables

    def disable_replication_log(self) -> None:
        """트리거 제거 + 큐 비우기 + **링크 제거**. 복제 없이 돈 뒤에는 복제본이 뒤처져 있으므로, 다시 복제를 켤 때
        `replica pull`/`init` 으로 새로 맞추게 강제한다 (링크가 남아 있으면 그 공백이 조용히 사라진다)."""
        if self.backend != "sqlite":
            return
        with self._lock:
            for trg in self._repl_triggers():
                self._conn.execute(f"DROP TRIGGER IF EXISTS {trg}")
            if self._table_exists("repl_queue"):
                self._conn.execute("DELETE FROM repl_queue")
            if self._table_exists("meta"):
                self._conn.execute("DELETE FROM meta WHERE key='replica_link'")

    def replication_enabled(self) -> bool:
        return self.backend == "sqlite" and bool(self._repl_triggers())

    def repl_fetch(self, limit: int) -> list[dict]:
        with self._lock:
            rows = self._conn.execute("SELECT id, tbl, op, row, created_at_ms FROM repl_queue ORDER BY id LIMIT ?",
                                      (int(limit),)).fetchall()
        return [{"id": int(r[0]), "tbl": r[1], "op": r[2], "row": json.loads(r[3]), "created_at_ms": int(r[4] or 0)} for r in rows]

    def repl_ack(self, max_id: int) -> int:
        with self._tx():
            cur = self._conn.execute("DELETE FROM repl_queue WHERE id<=?", (int(max_id),))
            return int(cur.rowcount or 0)

    def repl_queue_len(self) -> int:
        with self._lock:
            if self.backend != "sqlite" or not self._table_exists("repl_queue"):
                return 0
            return int(self._conn.execute("SELECT COUNT(*) FROM repl_queue").fetchone()[0])

    def repl_queue_seq(self) -> int:
        """repl_queue 의 AUTOINCREMENT 시퀀스(지금까지 발급된 최대 id). 복제본 last_id 와 비교해 '다른/오래된 장부' 를 잡는다."""
        with self._lock:
            if self.backend != "sqlite" or not self._table_exists("sqlite_sequence"):
                return 0
            r = self._conn.execute("SELECT seq FROM sqlite_sequence WHERE name='repl_queue'").fetchone()
            return int(r[0] or 0) if r else 0

    def repl_fetch_same_ms(self, after_id: int, created_at_ms: int, limit: int = 5000) -> list[dict]:
        """배치 경계가 같은 밀리초의 행(대개 같은 트랜잭션) 을 가르지 않도록 이어서 읽는다."""
        with self._lock:
            rows = self._conn.execute("SELECT id, tbl, op, row, created_at_ms FROM repl_queue WHERE id>? AND created_at_ms=? "
                                      "ORDER BY id LIMIT ?", (int(after_id), int(created_at_ms), int(limit))).fetchall()
        return [{"id": int(r[0]), "tbl": r[1], "op": r[2], "row": json.loads(r[3]), "created_at_ms": int(r[4] or 0)} for r in rows]

    def repl_queue_oldest_ms(self) -> int:
        with self._lock:
            if self.backend != "sqlite" or not self._table_exists("repl_queue"):
                return 0
            r = self._conn.execute("SELECT created_at_ms FROM repl_queue ORDER BY id LIMIT 1").fetchone()   # id 는 단조증가
            return int(r[0] or 0) if r else 0

    def repl_dead_add(self, row: dict, error: str) -> None:
        """복제본이 영구적으로 거부한 큐 행을 격리(dead-letter) 하고 큐에서 뺀다 — 뒤의 행들이 막히지 않도록."""
        with self._tx():
            self._conn.execute(REPL_DEAD_DDL)
            self._conn.execute("INSERT OR REPLACE INTO repl_dead(id, tbl, op, row, created_at_ms, error, dead_at_ms) "
                               "VALUES(?,?,?,?,?,?,?)",
                               (int(row["id"]), row["tbl"], row["op"], json.dumps(row["row"], ensure_ascii=False),
                                int(row.get("created_at_ms") or 0), str(error)[:500], now_ms()))
            self._conn.execute("DELETE FROM repl_queue WHERE id=?", (int(row["id"]),))

    def repl_dead_count(self) -> int:
        with self._lock:
            if self.backend != "sqlite" or not self._table_exists("repl_dead"):
                return 0
            return int(self._conn.execute("SELECT COUNT(*) FROM repl_dead").fetchone()[0])

    def repl_dead_rows(self, limit: int = 50) -> list[dict]:
        with self._lock:
            if self.backend != "sqlite" or not self._table_exists("repl_dead"):
                return []
            rows = self._conn.execute("SELECT id, tbl, op, row, created_at_ms, error, dead_at_ms FROM repl_dead ORDER BY id LIMIT ?",
                                      (int(limit),)).fetchall()
        return [{"id": r[0], "tbl": r[1], "op": r[2], "row": json.loads(r[3]), "created_at_ms": r[4], "error": r[5], "dead_at_ms": r[6]}
                for r in rows]

    # ------------------------------------------------------------------ backend info
    def describe(self) -> str:
        """`sqlite state/lake.db` 또는 `postgres host:port/db schema=…` (비밀번호 없음)."""
        if self.backend == "postgres":
            return f"{describe_target(self.path)} schema={self.schema}"
        return describe_target(self.path)

    def ping_ms(self) -> float:
        """원장 왕복 시간(ms). Postgres 는 연결 상태 확인을 겸한다 (LedgerUnavailable 가능)."""
        with self._lock:
            if self.backend == "postgres":
                return self._conn.ping_ms()
            t = time.monotonic()
            self._conn.execute("SELECT 1").fetchone()
            return (time.monotonic() - t) * 1000.0

    @property
    def reconnects(self) -> int:
        return int(getattr(self._conn, "reconnects", 0) or 0)

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
                f"SELECT * FROM signals WHERE status=? ORDER BY received_at_ms ASC, {self._rowid} ASC LIMIT 1",
                (SIGNAL_ACCEPTED,))
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
            # ON CONFLICT … DO UPDATE 는 SQLite(3.24+) 와 Postgres 양쪽에서 같은 의미 (INSERT OR REPLACE 는 SQLite 전용)
            self._conn.execute(
                "INSERT INTO orders(order_link_id,account,mode,event_id,position_id,purpose,side,qty,reduce_only,"
                "order_id,status,trigger_price,created_at_ms,updated_at_ms,raw) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(account,order_link_id) DO UPDATE SET mode=excluded.mode, event_id=excluded.event_id, "
                "position_id=excluded.position_id, purpose=excluded.purpose, side=excluded.side, qty=excluded.qty, "
                "reduce_only=excluded.reduce_only, order_id=excluded.order_id, status=excluded.status, "
                "trigger_price=excluded.trigger_price, created_at_ms=excluded.created_at_ms, "
                "updated_at_ms=excluded.updated_at_ms, raw=excluded.raw",
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


    # ------------------------------------------------------------------ 운영 조회 (web.py 대시보드 전용, 읽기 전용)
    _SIGNAL_COLS = ("event_id,mode,received_at_ms,position_id,event_sequence,action,strategy,leg,position_idx,"
                    "qty_btc,status,reason_code,processed_at_ms,note")
    _ORDER_COLS = ("order_link_id,account,mode,event_id,position_id,purpose,side,qty,reduce_only,order_id,status,"
                   "trigger_price,created_at_ms,updated_at_ms")
    _REPORT_COLS = ("report_id,mode,account,sequence,kind,created_at_ms,sent_at_ms,http_status,attempts,state,note,"
                    "length(body) AS body_len")

    @staticmethod
    def _where(filters: list[tuple[str, object]]) -> tuple[str, list]:
        """[(column, value)] → (' WHERE col=? AND …', params). value 가 None 인 항목은 건너뛴다. 값은 절대 문자열에 섞지 않는다."""
        parts = [f"{col}=?" for col, v in filters if v is not None]
        params = [v for _, v in filters if v is not None]
        return (" WHERE " + " AND ".join(parts)) if parts else "", params

    def recent_ingress(self, limit: int = 100, offset: int = 0) -> list[dict]:
        return self._q("SELECT id,received_at_ms,code,event_id,body_sha256,note FROM ingress_log "
                       "ORDER BY id DESC LIMIT ? OFFSET ?", (int(limit), int(offset)))

    def ingress_for_event(self, event_id: str, limit: int = 20) -> list[dict]:
        return self._q("SELECT id,received_at_ms,code,event_id,body_sha256,note FROM ingress_log WHERE event_id=? "
                       "ORDER BY id DESC LIMIT ?", (event_id, int(limit)))

    def list_signals(self, mode: str | None = None, status: str | None = None, limit: int = 50,
                     offset: int = 0) -> list[dict]:
        where, params = self._where([("mode", mode), ("status", status)])
        return self._q(f"SELECT {self._SIGNAL_COLS} FROM signals{where} ORDER BY received_at_ms DESC, {self._rowid} DESC "
                       "LIMIT ? OFFSET ?", (*params, int(limit), int(offset)))

    def signal_counts(self) -> list[dict]:
        return self._q("SELECT mode,status,COUNT(*) AS n FROM signals GROUP BY mode,status ORDER BY mode,status")

    def list_orders(self, mode: str | None = None, account: str | None = None, status: str | None = None,
                    limit: int = 50, offset: int = 0) -> list[dict]:
        where, params = self._where([("mode", mode), ("account", account), ("status", status)])
        return self._q(f"SELECT {self._ORDER_COLS} FROM orders{where} ORDER BY created_at_ms DESC LIMIT ? OFFSET ?",
                       (*params, int(limit), int(offset)))

    def orders_for_event(self, mode: str, event_id: str) -> list[dict]:
        return self._q(f"SELECT {self._ORDER_COLS} FROM orders WHERE mode=? AND event_id=? ORDER BY created_at_ms",
                       (mode, event_id))

    def fills_for_event(self, mode: str, event_id: str) -> list[dict]:
        return self._q("SELECT * FROM fills WHERE mode=? AND event_id=? ORDER BY exec_time_ms", (mode, event_id))

    def list_reports(self, mode: str | None = None, account: str | None = None, state: str | None = None,
                     limit: int = 50, offset: int = 0) -> list[dict]:
        where, params = self._where([("mode", mode), ("account", account), ("state", state)])
        return self._q(f"SELECT {self._REPORT_COLS} FROM reports{where} ORDER BY created_at_ms DESC, sequence DESC "
                       "LIMIT ? OFFSET ?", (*params, int(limit), int(offset)))

    def reports_for_event(self, mode: str, event_id: str, limit: int = 50) -> list[dict]:
        """본문에 그 event_id 가 실린 회신(execution 류). needle 은 canonical JSON 의 바이트열 그대로(LIKE 와일드카드 없음).
        반환 행에는 body 대신 body_len 과 본문에서 읽은 execution_status 만 싣는다."""
        needle = ('"event_id":"%s"' % event_id).encode("utf-8")
        rows = self._q("SELECT report_id,mode,account,sequence,kind,created_at_ms,sent_at_ms,http_status,attempts,state,"
                       f"note,body FROM reports WHERE mode=? AND {self._contains_body} ORDER BY sequence LIMIT ?",
                       (mode, needle, int(limit)))
        out = []
        for r in rows:
            body = r.pop("body", None)
            if isinstance(body, memoryview):
                body = body.tobytes()
            r["body_len"] = len(body or b"")
            r["execution_status"] = None
            try:
                parsed = json.loads(bytes(body).decode("utf-8")) if body else None
                ex = (parsed or {}).get("execution") if isinstance(parsed, dict) else None
                if isinstance(ex, dict) and ex.get("event_id") == event_id:
                    r["execution_status"] = ex.get("status")
            except (ValueError, TypeError, UnicodeDecodeError):
                pass
            out.append(r)
        return out

    def report_counts(self) -> list[dict]:
        return self._q("SELECT mode,state,COUNT(*) AS n FROM reports GROUP BY mode,state ORDER BY mode,state")

    # ------------------------------------------------------------------ 계정 트레이드 히스토리 (history.py 가 쓰고 metrics.py 가 읽는다)
    def _upsert_rows(self, table: str, cols: tuple[str, ...], rows: list[dict]) -> int:
        """INSERT OR IGNORE 로 여러 행. 새로 들어간 행 수 반환 (재실행/백필 중복은 0)."""
        if not rows:
            return 0
        t = now_ms()
        n = 0
        with self._tx():
            for r in rows:
                vals = []
                for c in cols:
                    v = r.get(c)
                    if c == "ingested_at_ms" and v is None:
                        v = t
                    if c == "raw" and v is not None and not isinstance(v, str):
                        v = json.dumps(v, ensure_ascii=False)[:4000]
                    vals.append(v)
                cur = self._conn.execute(
                    f"INSERT OR IGNORE INTO {table}({','.join(cols)}) VALUES({','.join('?' * len(cols))})", tuple(vals))
                n += 1 if cur.rowcount == 1 else 0
        return n

    def upsert_executions(self, rows: list[dict]) -> int:
        return self._upsert_rows("account_executions", EXEC_COLS, rows)

    def upsert_closed_pnl(self, rows: list[dict]) -> int:
        return self._upsert_rows("account_closed_pnl", CLOSED_PNL_COLS, rows)

    def upsert_cashflows(self, rows: list[dict]) -> int:
        return self._upsert_rows("account_cashflow", CASHFLOW_COLS, rows)

    def insert_equity(self, mode: str, account: str, ts_ms: int, total_equity: float, wallet_balance: float | None = None,
                      unrealised_pnl: float | None = None, available: float | None = None, source: str = "sync") -> None:
        with self._tx():
            self._conn.execute(
                "INSERT INTO account_equity(mode,account,ts_ms,total_equity,wallet_balance,unrealised_pnl,available,source) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (mode, account, int(ts_ms), float(total_equity), wallet_balance, unrealised_pnl, available, source))

    def get_sync_state(self, mode: str, account: str, kind: str) -> dict | None:
        return self._q1("SELECT * FROM sync_state WHERE mode=? AND account=? AND kind=?", (mode, account, kind))

    def set_sync_state(self, mode: str, account: str, kind: str, last_ts_ms: int, cursor: str | None = None,
                       note: str = "") -> None:
        with self._tx():
            self._conn.execute(
                "INSERT INTO sync_state(mode,account,kind,last_ts_ms,cursor,updated_at_ms,note) VALUES(?,?,?,?,?,?,?) "
                "ON CONFLICT(mode,account,kind) DO UPDATE SET last_ts_ms=excluded.last_ts_ms, cursor=excluded.cursor, "
                "updated_at_ms=excluded.updated_at_ms, note=excluded.note",
                (mode, account, kind, int(last_ts_ms), cursor, now_ms(), note[:300]))

    def sync_states(self, mode: str | None = None, account: str | None = None) -> list[dict]:
        where, params = self._where([("mode", mode), ("account", account)])
        return self._q(f"SELECT * FROM sync_state{where} ORDER BY mode, account, kind", tuple(params))

    @staticmethod
    def _range_where(col: str, since_ms: int | None, until_ms: int | None) -> tuple[str, list]:
        parts, params = [], []
        if since_ms is not None:
            parts.append(f"{col}>=?"); params.append(int(since_ms))
        if until_ms is not None:
            parts.append(f"{col}<?"); params.append(int(until_ms))
        return (" AND " + " AND ".join(parts)) if parts else "", params

    def account_executions(self, mode: str, account: str, since_ms: int | None = None, until_ms: int | None = None,
                           limit: int = 100000) -> list[dict]:
        extra, params = self._range_where("exec_time_ms", since_ms, until_ms)
        return self._q(f"SELECT * FROM account_executions WHERE mode=? AND account=?{extra} ORDER BY exec_time_ms, exec_id LIMIT ?",
                       (mode, account, *params, int(limit)))

    def account_closed_pnl(self, mode: str, account: str, since_ms: int | None = None, until_ms: int | None = None,
                           limit: int = 100000) -> list[dict]:
        extra, params = self._range_where("created_at_ms", since_ms, until_ms)
        return self._q(f"SELECT * FROM account_closed_pnl WHERE mode=? AND account=?{extra} ORDER BY created_at_ms, pnl_id LIMIT ?",
                       (mode, account, *params, int(limit)))

    def account_cashflows(self, mode: str, account: str, since_ms: int | None = None, until_ms: int | None = None,
                          limit: int = 100000) -> list[dict]:
        extra, params = self._range_where("ts_ms", since_ms, until_ms)
        return self._q(f"SELECT * FROM account_cashflow WHERE mode=? AND account=?{extra} ORDER BY ts_ms, flow_id LIMIT ?",
                       (mode, account, *params, int(limit)))

    def account_equity_series(self, mode: str, account: str, since_ms: int | None = None, until_ms: int | None = None,
                              limit: int = 100000) -> list[dict]:
        extra, params = self._range_where("ts_ms", since_ms, until_ms)
        return self._q(f"SELECT * FROM account_equity WHERE mode=? AND account=?{extra} ORDER BY ts_ms, id LIMIT ?",
                       (mode, account, *params, int(limit)))

    def account_equity_latest(self, mode: str, account: str) -> dict | None:
        return self._q1("SELECT * FROM account_equity WHERE mode=? AND account=? ORDER BY ts_ms DESC, id DESC LIMIT 1", (mode, account))

    def account_equity_first(self, mode: str, account: str) -> dict | None:
        return self._q1("SELECT * FROM account_equity WHERE mode=? AND account=? ORDER BY ts_ms ASC, id ASC LIMIT 1", (mode, account))

    def ledger_fills_joined(self, mode: str, account: str, since_ms: int | None = None, until_ms: int | None = None,
                            limit: int = 100000) -> list[dict]:
        """우리 체결(fills) 에 주문(side/reduce_only/purpose) 과 lot(strategy/leg/position_idx) 을 붙인다 — 전략별 실현손익 귀속용."""
        extra, params = self._range_where("f.exec_time_ms", since_ms, until_ms)
        return self._q(
            "SELECT f.exec_id, f.qty, f.price, f.exec_time_ms, f.position_id, f.event_id, f.order_link_id, "
            "o.side, o.reduce_only, o.purpose, l.strategy, l.leg, l.position_idx "
            "FROM fills f LEFT JOIN orders o ON o.account=f.account AND o.order_link_id=f.order_link_id "
            "LEFT JOIN lots l ON l.mode=f.mode AND l.account=f.account AND l.position_id=f.position_id "
            f"WHERE f.mode=? AND f.account=?{extra} ORDER BY f.exec_time_ms, f.exec_id LIMIT ?",
            (mode, account, *params, int(limit)))

    # ------------------------------------------------------------------ lake 사후 대조용 내보내기 (수신 원장 / 주문·체결 / 인증 뒤 거부 로그)
    @staticmethod
    def _conds(conds: list[tuple[str, object]]) -> tuple[str, list]:
        """[(sql fragment with ?, value)] → (' WHERE a AND b', params); value None 인 항목은 생략."""
        parts = [c for c, v in conds if v is not None]
        params = [v for _, v in conds if v is not None]
        return (" WHERE " + " AND ".join(parts)) if parts else "", params

    def export_receipts(self, mode: str | None = None, since_ms: int | None = None, until_ms: int | None = None,
                        limit: int = 100000) -> list[dict]:
        """수신 원장: 서명 통과 후 접수된 신호 1행 = event_id 1개 (본문 sha256, 판정/사유, 처리 시각, 같은 ID 중복·충돌 재전달 횟수)."""
        where, params = self._conds([("s.mode=?", mode), ("s.received_at_ms>=?", since_ms), ("s.received_at_ms<?", until_ms)])
        return self._q(
            "SELECT s.received_at_ms, s.mode, s.event_id, s.position_id, s.event_sequence, s.strategy, s.action, s.leg, s.position_idx, "
            "s.qty_btc, s.body_sha256, s.status AS signal_status, s.reason_code AS signal_reason, s.processed_at_ms, s.note AS signal_note, "
            "(SELECT COUNT(*) FROM ingress_log i WHERE i.event_id=s.event_id AND i.code='DUPLICATE') AS duplicate_count, "
            "(SELECT COUNT(*) FROM ingress_log i WHERE i.event_id=s.event_id AND i.code='EVENT_ID_CONFLICT') AS conflict_count "
            f"FROM signals s{where} ORDER BY s.received_at_ms, s.event_id LIMIT ?", (*params, int(limit)))

    def export_ingress(self, since_ms: int | None = None, until_ms: int | None = None, limit: int = 100000) -> list[dict]:
        """서명 통과 뒤 거부/중복 기록 (409/410/400/200-duplicate). 서명 전 거부(401 등) 는 DB 에 없다 — Caddy 접속 로그로."""
        where, params = self._conds([("received_at_ms>=?", since_ms), ("received_at_ms<?", until_ms)])
        rows = self._q(f"SELECT id, received_at_ms, code, event_id, body_sha256, note FROM ingress_log{where} ORDER BY id LIMIT ?",
                       (*params, int(limit)))
        for r in rows:
            note = str(r.get("note") or "")
            r["http"] = int(note.split("http=", 1)[1].split()[0]) if "http=" in note else None
        return rows

    def export_fills(self, mode: str | None = None, since_ms: int | None = None, until_ms: int | None = None,
                     limit: int = 100000) -> list[dict]:
        """주문·체결 원장: 체결 1행 (event_id → order_link_id/order_id → exec_id), 전략/leg/행동, 수수료는 거래소 히스토리(account_executions) 가 있으면."""
        where, params = self._conds([("f.mode=?", mode), ("f.exec_time_ms>=?", since_ms), ("f.exec_time_ms<?", until_ms)])
        return self._q(
            "SELECT f.mode, f.account, f.event_id, f.position_id, l.strategy, l.leg, l.position_idx, s.action, o.purpose, o.side, "
            "o.reduce_only, f.order_link_id, f.order_id, o.status AS order_status, o.qty AS order_qty, f.exec_id, f.qty AS exec_qty, "
            "f.price AS exec_price, f.exec_time_ms, e.fee, e.fee_currency, e.exec_type "
            "FROM fills f "
            "LEFT JOIN orders o ON o.account=f.account AND o.order_link_id=f.order_link_id "
            "LEFT JOIN lots l ON l.mode=f.mode AND l.account=f.account AND l.position_id=f.position_id "
            "LEFT JOIN signals s ON s.mode=f.mode AND s.event_id=f.event_id "
            "LEFT JOIN account_executions e ON e.mode=f.mode AND e.account=f.account AND e.exec_id=f.exec_id"
            f"{where} ORDER BY f.exec_time_ms, f.exec_id LIMIT ?", (*params, int(limit)))

    # ------------------------------------------------------------------ 라이브 신호 로그 (signal_log.py 가 쓰고, export/대시보드가 읽는다)
    def append_signal_log(self, row: dict) -> bool:
        """한 행 추가. 같은 (mode, event_id) 가 이미 있으면 False (재전송/중복 접수는 첫 기록만 남긴다)."""
        vals = []
        for c in SIGNAL_LOG_COLS:
            v = row.get(c)
            if c in ("take_profit", "accounts") and v is not None and not isinstance(v, str):
                v = json.dumps(v, ensure_ascii=False)
            vals.append(v)
        with self._tx():
            cur = self._conn.execute(
                f"INSERT OR IGNORE INTO signal_log({','.join(SIGNAL_LOG_COLS)}) VALUES({','.join('?' * len(SIGNAL_LOG_COLS))})",
                tuple(vals))
            return cur.rowcount == 1

    @staticmethod
    def _signal_log_where(mode: str | None, since_ms: int | None, until_ms: int | None) -> tuple[str, list]:
        parts, params = [], []
        if mode is not None:
            parts.append("mode=?"); params.append(mode)
        if since_ms is not None:
            parts.append("received_at_ms>=?"); params.append(int(since_ms))
        if until_ms is not None:
            parts.append("received_at_ms<?"); params.append(int(until_ms))
        return (" WHERE " + " AND ".join(parts)) if parts else "", params

    def signal_log_rows(self, mode: str | None = None, since_ms: int | None = None, until_ms: int | None = None,
                        limit: int = 200, offset: int = 0, ascending: bool = False) -> list[dict]:
        where, params = self._signal_log_where(mode, since_ms, until_ms)
        order = "ASC" if ascending else "DESC"
        rows = self._q(f"SELECT * FROM signal_log{where} ORDER BY received_at_ms {order}, id {order} LIMIT ? OFFSET ?",
                       (*params, int(limit), int(offset)))
        for r in rows:
            r["take_profit"] = _loads(r.get("take_profit"))
            r["accounts"] = _loads(r.get("accounts")) or []
        return rows

    def signal_log_count(self, mode: str | None = None) -> int:
        where, params = self._signal_log_where(mode, None, None)
        r = self._q1(f"SELECT COUNT(*) AS n FROM signal_log{where}", tuple(params))
        return int(r["n"]) if r else 0

    def export_signal_rows(self, mode: str | None = None, since_ms: int | None = None, until_ms: int | None = None,
                           limit: int = 100000) -> list[dict]:
        """백테스트용 평면 행: signal_log × 계정별 실행(run) + 체결 집계. 계정이 여러 개면 계정마다 한 행,
        run 이 없으면(미처리/거부) account 가 빈 한 행. 열 정의는 signal_log.EXPORT_COLUMNS."""
        base = self.signal_log_rows(mode, since_ms, until_ms, limit=limit, offset=0, ascending=True)
        if not base:
            return []
        modes = sorted({r["mode"] for r in base})
        lo = min(r["received_at_ms"] for r in base)
        sig_status: dict[tuple[str, str], dict] = {}
        runs: dict[tuple[str, str], list[dict]] = {}
        fills: dict[tuple[str, str, str], dict] = {}
        for m in modes:
            for s in self._q("SELECT mode,event_id,status,reason_code,processed_at_ms,note FROM signals "
                             "WHERE mode=? AND received_at_ms>=?", (m, lo)):
                sig_status[(m, s["event_id"])] = s
            for r in self._q("SELECT mode,event_id,account,status,reason_code,note,processed_at_ms FROM signal_runs "
                             "WHERE mode=? AND processed_at_ms>=? ORDER BY account", (m, lo)):
                runs.setdefault((m, r["event_id"]), []).append(r)
            for f in self._q("SELECT mode,event_id,account,SUM(qty) AS fill_qty,SUM(qty*price) AS notional,"
                             "COUNT(*) AS fill_count,MIN(exec_time_ms) AS first_fill_ms,MAX(exec_time_ms) AS last_fill_ms "
                             "FROM fills WHERE mode=? AND event_id IS NOT NULL AND exec_time_ms>=? GROUP BY mode,event_id,account",
                             (m, lo)):
                fills[(m, f["event_id"], f["account"])] = f
        out: list[dict] = []
        for r in base:
            key = (r["mode"], r["event_id"])
            s = sig_status.get(key) or {}
            acct_ctx = {a.get("name"): a for a in (r.get("accounts") or []) if isinstance(a, dict)}
            rlist = runs.get(key) or [{}]
            for run in rlist:
                account = run.get("account") or ""
                f = fills.get((r["mode"], r["event_id"], account)) or {}
                fq = float(f.get("fill_qty") or 0.0)
                ctx = acct_ctx.get(account) or {}
                first_fill = f.get("first_fill_ms")
                row = dict(r)
                row.pop("id", None)
                row.pop("accounts", None)
                row.update({
                    "signal_status": s.get("status"), "signal_reason": s.get("reason_code"),
                    "processed_at_ms": s.get("processed_at_ms"),
                    "processing_ms": (int(s["processed_at_ms"]) - int(r["received_at_ms"])) if s.get("processed_at_ms") else None,
                    "latency_signal_to_receipt_ms": (int(r["received_at_ms"]) - int(r["signal_ts"])) if r.get("signal_ts") else None,
                    "account": account, "run_status": run.get("status"), "run_reason": run.get("reason_code"),
                    "run_note": run.get("note"),
                    "fill_qty": fq if f else None,
                    "fill_avg_price": (float(f["notional"]) / fq) if f and fq > 0 else None,
                    "fill_count": int(f.get("fill_count") or 0) if f else None,
                    "first_fill_ms": first_fill, "last_fill_ms": f.get("last_fill_ms"),
                    "fill_latency_ms": (int(first_fill) - int(r["received_at_ms"])) if first_fill else None,
                    "account_exchange": ctx.get("exchange"), "account_leverage": ctx.get("leverage"),
                    "account_margin_mode": ctx.get("margin_mode"), "account_position_mode": ctx.get("position_mode"),
                    "account_qty_multiplier": ctx.get("qty_multiplier"), "account_live_possible": ctx.get("live_possible"),
                })
                out.append(row)
        return out


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
            elif getattr(self.s._conn, "broken", False):
                # Postgres 연결이 트랜잭션 중에 끊김: 서버가 이미 롤백했으므로 상태만 정리한다
                self.s._conn.rollback_quiet()
            else:
                self.s._conn.execute("ROLLBACK")
        finally:
            self.s._lock.release()
        return False
