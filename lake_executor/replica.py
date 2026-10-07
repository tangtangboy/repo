"""장부 복제 — 로컬 SQLite 장부(주문 경로) 의 변경을 Postgres(DATABASE_URL) 에 비동기로 복제한다.

왜: 장부가 서버 밖(다른 리전의 Postgres) 에 있으면 신호 하나에 수십~백 번의 왕복이 전부 주문 경로에 들어가
(뭄바이 기준 신호당 12~16초, 주문까지 3.5초). 매매는 서버 안의 SQLite(왕복 0.1ms) 로 끝내고, 외부 DB 에는 **뒤에서** 기록한다.
외부 DB 는 대시보드/분석/재해복구용 복제본이고, 끊겨도 매매에 영향이 없다(큐가 로컬에 쌓였다가 따라간다).

구조
  store.enable_replication_log()  SQLite 트리거: 모든 테이블의 INSERT/UPDATE → repl_queue(op=upsert, row=json(NEW)),
                                  DELETE(또는 PK 변경) → (op=delete, row=json(PK)). 같은 SQLite 트랜잭션 안에서 적재되므로
                                  변경과 큐가 원자적이다. BLOB 열은 hex 로 싣는다.
  Replicator                      전용 로컬 연결 + 전용 대상 연결. repl_queue 를 id 순으로 읽어 (table, op) 로 묶고 대상에
                                  upsert(INSERT … ON CONFLICT(pk) DO UPDATE) / delete 를 executemany 로 적용. 같은 대상 트랜잭션에서
                                  lake_replica_state.last_id 를 올리고 COMMIT 한 뒤 로컬 큐를 지운다 → 중간에 죽어도 다음 기동 때
                                  last_id 이하는 버리므로 정확히 한 번 적용된다.
  링크                            로컬 meta `replica_link` == 대상 lake_replica_state.link 일 때만 복제한다. 엉뚱한(오래된/다른 서버의)
                                  SQLite 가 복제본을 덮어쓰지 못하게 한다. 링크는 `replica pull`(복제본 → 새 SQLite 전체 복사, 컷오버·
                                  재해복구) 또는 `replica init`(빈 대상에 로컬 전체 푸시) 가 만든다. 양쪽이 모두 비어 있으면 자동 링크.

CLI: python -m lake_executor replica status | pull [--yes] | init [--yes]
"""
from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
import uuid
from typing import Any, Callable

from .store import REPL_QUEUE_DDL, TABLES, LedgerUnavailable
from .util import now_ms

log = logging.getLogger("lake_executor.replica")

STATE_TABLE = "lake_replica_state"
STATE_DDL = f"CREATE TABLE IF NOT EXISTS {STATE_TABLE} (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
LINK_META_KEY = "replica_link"
ALERT_EVERY_MS = 5 * 60 * 1000
DEFAULT_BATCH = 500


class ReplicaError(Exception):
    pass


class NotLinked(ReplicaError):
    pass


class Fenced(ReplicaError):
    """다른 복제기(쌍둥이 장부) 나 링크 변경이 감지됨 — 이 프로세스는 복제를 멈추고 HALT 한다."""


class Stuck(ReplicaError):
    """영구 오류(스키마/제약 위반 등): 큐의 특정 행에서 더 나아갈 수 없다."""


AUTOINC_TABLES = tuple(t for t, ddl in TABLES.items() if "AUTOINCREMENT" in ddl)
PG_TYPES = {"INTEGER": "BIGINT", "REAL": "DOUBLE PRECISION", "BLOB": "BYTEA", "TEXT": "TEXT", "": "TEXT"}
TRANSIENT_ERRORS = ("LedgerUnavailable", "OperationalError", "InterfaceError", "ConnectionError", "TimeoutError", "OSError",
                    "ConnectionRefusedError", "ConnectionResetError", "BrokenPipeError")


def is_transient(e: BaseException) -> bool:
    return isinstance(e, LedgerUnavailable) or type(e).__name__ in TRANSIENT_ERRORS


# --------------------------------------------------------------------------- #
# SQL 생성 (로컬 스키마 정보 기준; SQLite ≥ 3.24 와 Postgres 양쪽에서 같은 문장이 동작한다)
# --------------------------------------------------------------------------- #
def upsert_sql(table: str, cols: list[str], pk: list[str]) -> str:
    non_pk = [c for c in cols if c not in pk]
    conflict = ", ".join(pk)
    if non_pk:
        action = "DO UPDATE SET " + ", ".join(f"{c}=excluded.{c}" for c in non_pk)
    else:
        action = "DO NOTHING"
    return (f"INSERT INTO {table}({', '.join(cols)}) VALUES({', '.join('?' * len(cols))}) "
            f"ON CONFLICT({conflict}) {action}")


def batch_digest(rows: list[dict]) -> str:
    """큐 행 묶음의 지문: "<첫 id>-<끝 id>-<sha256 앞 16자리>". 복제본은 마지막으로 적용한 묶음의 지문을 기억한다."""
    h = hashlib.sha256()
    for r in rows:
        h.update(f"{r['id']}:{r['tbl']}:{r['op']}:{json.dumps(r['row'], sort_keys=True, ensure_ascii=False)}\n".encode("utf-8"))
    return f"{rows[0]['id']}-{rows[-1]['id']}-{h.hexdigest()[:16]}" if rows else ""


def delete_sql(table: str, pk: list[str]) -> str:
    return f"DELETE FROM {table} WHERE " + " AND ".join(f"{c}=?" for c in pk)


def _coerce(value: Any, declared_type: str) -> Any:
    """json 에서 나온 값을 열 타입에 맞춘다 (BLOB 은 hex 문자열, REAL 은 17자리 문자열로 실려 온다)."""
    if value is None:
        return None
    t = (declared_type or "").upper()
    if isinstance(value, dict) and "$hex" in value:            # 선언 타입과 다른 열에 들어온 bytes
        return bytes.fromhex(value["$hex"])
    if t == "BLOB":
        return bytes.fromhex(value) if isinstance(value, str) else value
    if t == "REAL":
        return float(value)
    if t == "INTEGER":
        if isinstance(value, bool):
            return int(value)
        try:
            return int(value)
        except (TypeError, ValueError):
            f = float(value)
            return int(f) if f.is_integer() else f
    return value


# --------------------------------------------------------------------------- #
# 대상(복제본) 상태
# --------------------------------------------------------------------------- #
class _Target:
    """대상 Store 위의 얇은 도우미: 상태 테이블, 트랜잭션, executemany."""

    def __init__(self, store: Any):
        self.store = store
        with store._lock:
            store._conn.executescript(STATE_DDL)

    def get_state(self, key: str) -> str | None:
        with self.store._lock:
            cur = self.store._conn.execute(f"SELECT value FROM {STATE_TABLE} WHERE key=?", (key,))
            r = cur.fetchone()
        return None if r is None else str(r[0])

    def set_state_sql(self) -> str:
        return (f"INSERT INTO {STATE_TABLE}(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value")

    def set_state(self, key: str, value: str) -> None:
        with self.store._tx():
            self.store._conn.execute(self.set_state_sql(), (key, str(value)))

    def set_states(self, values: dict[str, str]) -> None:
        """여러 키를 한 트랜잭션에 (link 와 last_id 는 항상 함께 바뀐다)."""
        with self.store._tx():
            for k, v in values.items():
                self.store._conn.execute(self.set_state_sql(), (k, str(v)))

    def columns(self, table: str) -> list[str]:
        with self.store._lock:
            if self.store.backend == "postgres":
                rows = self.store._conn.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=? "
                    "ORDER BY ordinal_position", (table,)).fetchall()
            else:
                rows = [(r[1],) for r in self.store._conn.execute(f"PRAGMA table_info({table})").fetchall()]
        return [r[0] for r in rows]

    def add_column(self, table: str, col: str, declared_type: str) -> None:
        typ = PG_TYPES.get((declared_type or "").upper(), "TEXT") if self.store.backend == "postgres" else (declared_type or "TEXT")
        with self.store._lock:
            self.store._conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ}")

    def sync_sequences_sql(self, tables: list[str]) -> list[str]:
        """Postgres BIGSERIAL 시퀀스를 복제된 명시 id 뒤로 옮긴다 (ledger=remote 로 되돌려도 PK 충돌이 없도록)."""
        if self.store.backend != "postgres":
            return []
        return [f"SELECT setval(pg_get_serial_sequence('{t}', 'id'), COALESCE((SELECT MAX(id) FROM {t}), 0) + 1, false)"
                for t in tables]

    def executemany(self, sql: str, rows: list[tuple]) -> None:
        if not rows:
            return
        conn = self.store._conn
        em = getattr(conn, "executemany", None)
        if em is not None:
            em(sql, rows)
        else:  # pragma: no cover - 모든 백엔드가 executemany 를 가진다
            for r in rows:
                conn.execute(sql, r)

    def row_count(self, table: str) -> int:
        with self.store._lock:
            return int(self.store._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])

    def close(self) -> None:
        try:
            self.store.close()
        except Exception:  # noqa: BLE001
            pass


# --------------------------------------------------------------------------- #
# 복제기
# --------------------------------------------------------------------------- #
class Replicator:
    def __init__(self, local_factory: Callable[[], Any], target_factory: Callable[[], Any], alerts: Any = None,
                 clock: Callable[[], int] = now_ms, batch: int = DEFAULT_BATCH, interval_s: float = 0.5):
        self._local_factory = local_factory
        self._target_factory = target_factory
        self.alerts = alerts
        self.clock = clock
        self.batch = int(batch)
        self.interval_s = float(interval_s)
        self.startup_delay_s = 2.0
        self._local: Any = None
        self._target: _Target | None = None
        self._schemas: dict[str, dict] = {}
        self._sql: dict[tuple[str, str], str] = {}
        self._lock = threading.Lock()
        self._last_alert: dict[str, int] = {}
        self.state: dict[str, Any] = {"linked": None, "link": None, "queue": 0, "applied_total": 0, "written_total": 0, "batches": 0,
                                      "last_ok_ms": 0, "last_error": "", "last_error_ms": 0, "errors": 0,
                                      "target": "", "last_id": 0, "fenced": False, "stuck_id": 0}
        self.wake = threading.Event()
        self.halt_file: str | None = None            # Fenced 시 HALT 파일을 만든다 (쌍둥이 장부가 매매를 이어가지 못하게)
        self.stuck_retry_s = 60.0
        self.lag_alert_ms = 10 * 60 * 1000

    # ---- 연결 ----
    def local(self) -> Any:
        if self._local is None:
            self._local = self._local_factory()
            if getattr(self._local, "backend", "sqlite") != "sqlite":
                raise ReplicaError("replication source must be the local SQLite ledger")
            try:   # ack 의 fsync 는 불필요 (ack 유실 = 멱등 재적용) → 주문 경로의 쓰기 락 대기를 줄인다
                self._local._conn.execute("PRAGMA synchronous=NORMAL")
            except Exception:  # noqa: BLE001
                pass
        return self._local

    def target(self) -> _Target:
        if self._target is None:
            st = self._target_factory()
            tgt = _Target(st)
            self.state["target"] = st.describe() if hasattr(st, "describe") else str(st)
            self._ensure_target_columns(tgt)
            self._target = tgt
        return self._target

    def _ensure_target_columns(self, tgt: _Target) -> None:
        """로컬 장부에 생긴 새 열을 복제본에도 만든다 (CREATE TABLE IF NOT EXISTS 는 열을 추가하지 않는다)."""
        local = self.local()
        for t in local.replicated_tables():
            s = local.table_schema(t)
            have = set(tgt.columns(t))
            if not have:
                continue
            for c in s["cols"]:
                if c not in have:
                    log.warning("replica: adding missing column %s.%s to the replica", t, c)
                    tgt.add_column(t, c, s["types"].get(c, ""))

    def close(self) -> None:
        for obj in (self._local, self._target):
            if obj is not None:
                try:
                    obj.close()
                except Exception:  # noqa: BLE001
                    pass
        self._local, self._target = None, None

    def _drop_target(self) -> None:
        if self._target is not None:
            self._target.close()
            self._target = None

    def _alert(self, key: str, text: str) -> None:
        now = self.clock()
        if now - self._last_alert.get(key, 0) < ALERT_EVERY_MS:
            return
        self._last_alert[key] = now
        if self.alerts is not None:
            try:
                self.alerts.send(text)
            except Exception:  # noqa: BLE001
                pass

    # ---- 스키마/문장 캐시 ----
    def _schema(self, table: str) -> dict:
        s = self._schemas.get(table)
        if s is None:
            s = self.local().table_schema(table)
            if not s["cols"] or not s["pk"]:
                raise ReplicaError(f"table {table} has no columns or no primary key; cannot replicate")
            self._schemas[table] = s
        return s

    def _stmt(self, table: str, op: str) -> str:
        k = (table, op)
        q = self._sql.get(k)
        if q is None:
            s = self._schema(table)
            q = upsert_sql(table, s["cols"], s["pk"]) if op == "upsert" else delete_sql(table, s["pk"])
            self._sql[k] = q
        return q

    # ---- 링크 ----
    def check_link(self) -> str:
        """로컬 meta.replica_link 와 대상 lake_replica_state.link 를 대조. 둘 다 비어 있고 양쪽에 데이터가 없으면 자동 링크."""
        local = self.local()
        tgt = self.target()
        mine = local.get_meta(LINK_META_KEY)
        theirs = tgt.get_state("link")
        if mine and theirs and mine == theirs:
            last_id = int(tgt.get_state("last_id") or 0)
            seq = local.repl_queue_seq()
            if last_id > seq:
                self.state.update(linked=False, link=mine)
                raise NotLinked(f"replica is ahead of this ledger (replica last_id {last_id} > local queue seq {seq}): "
                                "this ledger is an older/restored copy — run `replica pull`")
            self.state.update(linked=True, link=mine)
            return mine
        if not mine and not theirs:
            empty_local = all(local.count_rows(t) == 0 for t in local.replicated_tables() if t != "meta")
            empty_target = all(tgt.row_count(t) == 0 for t in local.replicated_tables() if t != "meta")
            if empty_local and empty_target:
                link = uuid.uuid4().hex
                tgt.set_states({"link": link, "last_id": "0", "last_batch": ""})
                local.set_meta(LINK_META_KEY, link)
                self.state.update(linked=True, link=link)
                log.info("replica: auto-linked empty ledger and empty replica (%s)", link[:8])
                return link
        self.state.update(linked=False, link=mine)
        raise NotLinked("local ledger and replica are not linked (run `replica pull` or `replica init`)")

    # ---- 한 배치 ----
    def replicate_once(self) -> int:
        """큐에서 최대 batch 행을 대상에 적용. 반환: 적용한 큐 행 수 (0 = 비어 있음)."""
        if self.state.get("fenced"):
            raise Fenced("replicator is fenced; restart the service after resolving (replica link changed or a second ledger is writing)")
        local = self.local()
        tgt = self.target()
        link = self.check_link()
        rows = local.repl_fetch(self.batch)
        if len(rows) >= self.batch:
            rows += local.repl_fetch_same_ms(rows[-1]["id"], rows[-1]["created_at_ms"])   # 같은 ms 의 행은 한 배치에
        self.state["queue"] = local.repl_queue_len()
        if not rows:
            return 0
        last_applied = int(tgt.get_state("last_id") or 0)
        pending = [r for r in rows if int(r["id"]) > last_applied]
        stale = [r for r in rows if int(r["id"]) <= last_applied]
        if stale:
            # 워터마크 이하의 행은 "COMMIT 뒤 ack 전에 죽은" 바로 그 묶음일 때만 버린다. 지문이 다르면 이 장부는 다른 계보의
            # 복사본(쌍둥이/복원본) 이고 그 변경은 복제본에 간 적이 없다 → 조용히 버리지 않고 펜싱 + HALT.
            expected = tgt.get_state("last_batch") or ""
            got = batch_digest(stale)
            if got != expected:
                self._fence()
                raise Fenced(f"{len(stale)} queued row(s) at or below the replica watermark {last_applied} do not match the "
                             f"replica's last batch ({got} != {expected or '-'}): this ledger is a divergent copy")
            log.warning("replica: %d queued rows (id <= %d) were already applied before a crash; discarding them",
                        len(stale), last_applied)
        max_id = int(rows[-1]["id"])
        if pending:
            # 키별로 마지막 작업만 남긴다 (같은 행을 여러 번 고친 경우). 키가 다르면 순서가 무관하다 (외래키 없음).
            final: dict[tuple, tuple[str, str, dict]] = {}
            for r in pending:
                s = self._schema(r["tbl"])
                key = (r["tbl"], tuple(r["row"].get(c) for c in s["pk"]))
                final[key] = (r["tbl"], r["op"], r["row"])
            groups: dict[tuple[str, str], list[tuple]] = {}
            for tbl, op, row in final.values():
                s = self._schema(tbl)
                if op == "upsert":
                    vals = tuple(_coerce(row.get(c), s["types"].get(c, "")) for c in s["cols"])
                else:
                    vals = tuple(_coerce(row.get(c), s["types"].get(c, "")) for c in s["pk"])
                groups.setdefault((tbl, op), []).append(vals)
            try:
                with tgt.store._tx():
                    for (tbl, op), vals in groups.items():
                        tgt.executemany(self._stmt(tbl, op), vals)
                    for sql in tgt.sync_sequences_sql([t for t in AUTOINC_TABLES if any(k[0] == t for k in groups)]):
                        tgt.store._conn.execute(sql)
                    # 펜싱: 배치 시작에 읽은 last_id/link 가 그대로일 때만 전진 (쌍둥이 장부·링크 변경 감지)
                    cur = tgt.store._conn.execute(f"UPDATE {STATE_TABLE} SET value=? WHERE key='last_id' AND value=?",
                                                  (str(max_id), str(last_applied)))
                    n1 = int(cur.rowcount or 0)
                    cur = tgt.store._conn.execute(f"UPDATE {STATE_TABLE} SET value=? WHERE key='link' AND value=?", (link, link))
                    n2 = int(cur.rowcount or 0)
                    if n1 != 1 or n2 != 1:
                        raise Fenced(f"replica state changed under us (last_id/link rows updated: {n1}/{n2}); "
                                     "another ledger is replicating to this target or the link was replaced")
                    tgt.store._conn.execute(tgt.set_state_sql(), ("last_batch", batch_digest(pending)))
                    tgt.store._conn.execute(tgt.set_state_sql(), ("applied_at_ms", str(self.clock())))
            except Fenced:
                self._fence()
                raise
        local.repl_ack(max_id)
        with self._lock:
            self.state["applied_total"] += len(pending)
            self.state["written_total"] += len(final) if pending else 0
            self.state["batches"] += 1
            self.state["last_ok_ms"] = self.clock()
            self.state["last_id"] = max_id
            self.state["queue"] = local.repl_queue_len()
        return len(rows)

    def drain(self, max_batches: int = 10000) -> int:
        """큐가 빌 때까지 (테스트/CLI). 반환: 적용 행 수."""
        total = 0
        for _ in range(max_batches):
            n = self.replicate_once()
            total += n
            if n < self.batch:
                break
        return total

    def run_forever(self, stop_event: threading.Event) -> None:
        log.info("replicator started (batch=%s, interval=%ss)", self.batch, self.interval_s)
        stop_event.wait(self.startup_delay_s)
        backoff = 1.0
        failing: str | None = None
        while not stop_event.is_set():
            try:
                n = self.replicate_once()
                if failing is not None:
                    log.info("replicator recovered after %s", failing)
                    self._alert("recovered", f"[replica] recovered after {failing}")
                    failing = None
                backoff = 1.0
                oldest = self.local().repl_queue_oldest_ms() if n else 0
                if oldest and self.clock() - oldest > self.lag_alert_ms:
                    self._alert("lag", f"[replica] replication lag {(self.clock() - oldest) // 60000} min "
                                       f"({self.local().repl_queue_len()} queued) — target slow or unreachable")
                if n >= self.batch:
                    continue                       # 밀린 게 더 있다 → 바로 다음 배치
                self.wake.wait(self.interval_s)
                self.wake.clear()
            except Fenced as e:
                self._note_error(e)
                log.error("replicator fenced: %s", e)
                self._alert("fenced", f"[replica] FENCED + HALT: {e}")
                break                                            # 재시작 전까지 복제하지 않는다
            except NotLinked as e:
                self._note_error(e)
                if failing is None:
                    log.warning("replicator: %s", e)
                self._alert("notlinked", f"[replica] {e}")
                failing = "NotLinked"
                stop_event.wait(30.0)
            except Exception as e:  # noqa: BLE001 - 복제 실패는 매매와 무관
                self._note_error(e)
                if is_transient(e):
                    log.warning("replicator error: %s (retry in %.0fs)", type(e).__name__, backoff)
                    self._alert("error", f"[replica] {type(e).__name__}: replication paused, queue grows locally")
                    failing = type(e).__name__
                    self._drop_target()
                    stop_event.wait(backoff)
                    backoff = min(backoff * 2, 30.0)
                else:                                            # 영구 오류(제약/스키마): 행 단위로 다시 적용해 거부되는 행만 격리
                    failing = type(e).__name__
                    self._drop_target()
                    try:
                        dead = self.quarantine_batch()
                    except Exception as e2:  # noqa: BLE001 - 격리도 실패(대상 불통 등) → 백오프
                        self._note_error(e2)
                        log.warning("replicator quarantine failed: %s", type(e2).__name__)
                        self._drop_target()
                        stop_event.wait(self.stuck_retry_s)
                        continue
                    log.error("replicator: %d row(s) quarantined to repl_dead after %s: %s", dead, type(e).__name__, str(e)[:200])
                    self._alert("dead", f"[replica] {dead} change(s) rejected by the replica and quarantined (repl_dead): "
                                        f"{type(e).__name__}: {str(e)[:120]}")
        self.close()
        log.info("replicator stopped")

    def quarantine_batch(self) -> int:
        """한 배치가 영구 오류로 거부됐을 때: 같은 행들을 하나씩 적용해 통과하는 건 ack, 거부되는 건 repl_dead 로 보낸다.
        반환: 격리된 행 수. 뒤의 행들이 막히지 않게 하는 것이 목적이며, 격리된 행은 status()['dead'] 와 `replica status` 에 보인다."""
        local = self.local()
        tgt = self.target()
        self.check_link()
        rows = local.repl_fetch(self.batch)
        if not rows:
            return 0
        last_applied = int(tgt.get_state("last_id") or 0)
        dead = 0
        for r in rows:
            rid = int(r["id"])
            if rid <= last_applied:
                local.repl_ack(rid)
                continue
            try:
                s = self._schema(r["tbl"])
                if r["op"] == "upsert":
                    vals = tuple(_coerce(r["row"].get(c), s["types"].get(c, "")) for c in s["cols"])
                else:
                    vals = tuple(_coerce(r["row"].get(c), s["types"].get(c, "")) for c in s["pk"])
                with tgt.store._tx():
                    tgt.store._conn.execute(self._stmt(r["tbl"], r["op"]), vals)
                    cur = tgt.store._conn.execute(f"UPDATE {STATE_TABLE} SET value=? WHERE key='last_id' AND value=?",
                                                  (str(rid), str(last_applied)))
                    if int(cur.rowcount or 0) != 1:
                        raise Fenced("replica state changed during quarantine")
                    tgt.store._conn.execute(tgt.set_state_sql(), ("last_batch", batch_digest([r])))
                last_applied = rid
                local.repl_ack(rid)
                with self._lock:
                    self.state["applied_total"] += 1
                    self.state["written_total"] += 1
                    self.state["last_id"] = rid
            except Fenced:
                self._fence()
                raise
            except Exception as e:  # noqa: BLE001
                if is_transient(e):
                    raise
                local.repl_dead_add(r, f"{type(e).__name__}: {e}")
                dead += 1
                # 대상이 트랜잭션 중 오류로 멈춰 있지 않게 (sqlite 는 _tx 가 ROLLBACK, pg 도 동일) — 그리고 last_id 는 격리 행을 건너뛴다
                with tgt.store._tx():
                    tgt.store._conn.execute(f"UPDATE {STATE_TABLE} SET value=? WHERE key='last_id' AND value=?",
                                            (str(rid), str(last_applied)))
                    tgt.store._conn.execute(tgt.set_state_sql(), ("last_batch", batch_digest([r])))
                last_applied = rid
        with self._lock:
            self.state["last_ok_ms"] = self.clock()
            self.state["queue"] = local.repl_queue_len()
        return dead

    def _first_pending_id(self) -> int:
        try:
            rows = self.local().repl_fetch(1)
            return int(rows[0]["id"]) if rows else 0
        except Exception:  # noqa: BLE001
            return 0

    def _fence(self) -> None:
        with self._lock:
            self.state["fenced"] = True
        if self.halt_file:
            try:
                with open(self.halt_file, "a", encoding="utf-8"):
                    pass
                log.error("replicator: HALT file created (%s)", self.halt_file)
            except OSError as e:
                log.error("replicator: cannot create HALT file: %s", type(e).__name__)

    def _note_error(self, e: Exception) -> None:
        with self._lock:
            self.state["errors"] += 1
            self.state["last_error"] = f"{type(e).__name__}: {str(e)[:120]}"
            self.state["last_error_ms"] = self.clock()
            try:
                self.state["queue"] = self.local().repl_queue_len()
            except Exception:  # noqa: BLE001
                pass

    def status(self) -> dict:
        with self._lock:
            s = dict(self.state)
        try:
            s["queue"] = self.local().repl_queue_len()
            s["queue_oldest_ms"] = self.local().repl_queue_oldest_ms()
            s["dead"] = self.local().repl_dead_count()
        except Exception:  # noqa: BLE001
            pass
        s["lag_ms"] = (self.clock() - int(s["queue_oldest_ms"])) if s.get("queue_oldest_ms") else 0
        return s


# --------------------------------------------------------------------------- #
# 컷오버 / 재해복구 도구
# --------------------------------------------------------------------------- #
def pull(target_store: Any, local_store: Any, chunk: int = 2000) -> dict:
    """복제본(대상) 의 모든 테이블을 로컬 SQLite 에 통째로 복사하고 새 링크를 양쪽에 기록한다.
    local_store 는 트리거 없이 연 **새/빈** SQLite 여야 한다 (서비스가 멈춘 상태에서). 반환: {table: rows}."""
    if getattr(local_store, "backend", "sqlite") != "sqlite":
        raise ReplicaError("pull target must be a SQLite ledger")
    tgt = _Target(target_store)
    counts: dict[str, int] = {}
    tables = local_store.replicated_tables()
    with local_store._lock:
        local_store.disable_replication_log()
    snapshot: dict[str, list] = {}
    with target_store._tx():                                   # 한 스냅샷에서 모든 테이블을 읽는다 (서로 일관)
        if target_store.backend == "postgres":
            target_store._conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        for t in tables:
            cols = local_store.table_schema(t)["cols"]
            have = set(tgt.columns(t))
            sel = [c if c in have else "NULL" for c in cols]
            snapshot[t] = target_store._conn.execute(f"SELECT {', '.join(sel)} FROM {t}").fetchall()
    for t in tables:
        s = local_store.table_schema(t)
        cols = s["cols"]
        rows = snapshot[t]
        n = 0
        with local_store._tx():
            local_store._conn.execute(f"DELETE FROM {t}")
            ins = f"INSERT OR REPLACE INTO {t}({', '.join(cols)}) VALUES({', '.join('?' * len(cols))})"
            buf: list[tuple] = []
            for r in rows:
                if t == "meta" and str(r[cols.index("key")]).startswith("replica_"):
                    continue
                vals = []
                for i, c in enumerate(cols):
                    v = r[i]
                    if isinstance(v, memoryview):
                        v = bytes(v)
                    vals.append(v)
                buf.append(tuple(vals))
                n += 1
                if len(buf) >= chunk:
                    local_store._conn.executemany(ins, buf)
                    buf = []
            if buf:
                local_store._conn.executemany(ins, buf)
        counts[t] = n
    link = uuid.uuid4().hex
    with local_store._lock:
        local_store._conn.execute(REPL_QUEUE_DDL)
        local_store._conn.execute("DELETE FROM repl_queue")
        local_store._conn.execute("DELETE FROM sqlite_sequence WHERE name='repl_queue'")   # 새 계보: seq 0 ↔ last_id 0
    tgt.set_states({"link": link, "last_id": "0", "last_batch": ""})   # 복제본 먼저 (여기서 죽으면 로컬은 링크 없음 → 거부됨)
    local_store.set_meta(LINK_META_KEY, link)                  # 로컬 링크는 마지막에
    counts["_link"] = link
    return counts


def init_push(local_store: Any, target_store: Any, force: bool = False, chunk: int = 2000) -> dict:
    """로컬 SQLite 의 모든 테이블을 (비어 있는) 대상에 밀어 넣고 링크를 만든다. force 가 아니면 대상에 행이 있을 때 거부."""
    if getattr(local_store, "backend", "sqlite") != "sqlite":
        raise ReplicaError("init source must be the local SQLite ledger")
    tgt = _Target(target_store)
    tables = local_store.replicated_tables()
    if not force:
        busy = [t for t in tables if t != "meta" and tgt.row_count(t) > 0]
        if busy:
            raise ReplicaError(f"replica already has rows in {', '.join(busy)}; use --force to overwrite by primary key")
    counts: dict[str, int] = {}
    link = uuid.uuid4().hex
    with local_store._lock:
        local_store._conn.execute(REPL_QUEUE_DDL)
        max_id = int(local_store._conn.execute("SELECT COALESCE(MAX(id), 0) FROM repl_queue").fetchone()[0] or 0)
    with tgt.store._tx():
        for t in tables:
            s = local_store.table_schema(t)
            cols = s["cols"]
            with local_store._lock:
                rows = local_store._conn.execute(f"SELECT {', '.join(cols)} FROM {t}" +
                                                 (" WHERE key NOT LIKE 'replica%'" if t == "meta" else "")).fetchall()
            q = upsert_sql(t, cols, s["pk"])
            for i in range(0, len(rows), chunk):
                tgt.executemany(q, [tuple(bytes(v) if isinstance(v, memoryview) else v for v in r) for r in rows[i:i + chunk]])
            counts[t] = len(rows)
        for sql in tgt.sync_sequences_sql(list(AUTOINC_TABLES)):
            tgt.store._conn.execute(sql)
        tgt.store._conn.execute(tgt.set_state_sql(), ("link", link))
        tgt.store._conn.execute(tgt.set_state_sql(), ("last_id", str(max_id)))   # 큐에 남은 것까지 포함해 밀었으므로 여기까지 적용됨
        tgt.store._conn.execute(tgt.set_state_sql(), ("last_batch", ""))
    with local_store._lock:
        local_store._conn.execute("DELETE FROM repl_queue")
    local_store.set_meta(LINK_META_KEY, link)
    counts["_link"] = link
    counts["_dropped_queue_rows"] = max_id
    return counts


def compare(local_store: Any, target_store: Any) -> dict[str, tuple[int, int]]:
    """테이블별 (로컬 행 수, 대상 행 수) — status/검증용."""
    tgt = _Target(target_store)
    out = {}
    for t in local_store.replicated_tables():
        out[t] = (local_store.count_rows(t), tgt.row_count(t))
    return out


def startup_check(rep: Replicator) -> str:
    """serve 기동 시: 복제본에 닿으면 링크를 검증한다(불일치/계보 역전 → NotLinked, 기동 거부). 닿지 않으면 로컬에 링크가
    있을 때만 'deferred'(기동, 뒤에서 재시도) 를 돌려주고, 링크조차 없으면 NotLinked (빈 장부로 매매를 시작하지 않는다)."""
    try:
        link = rep.check_link()
        return f"linked {link[:8]}"
    except NotLinked:
        raise
    except Exception as e:  # noqa: BLE001 - 복제본 불통
        local = rep.local()
        if local.get_meta(LINK_META_KEY):
            log.warning("replica: cannot verify link now (%s); starting on the local ledger, will retry in background",
                        type(e).__name__)
            return "deferred"
        raise NotLinked(f"replica unreachable ({type(e).__name__}) and this ledger has no replica link; "
                        "refusing to start trading on an unverified ledger") from e
