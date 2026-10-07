"""복제 안전장치 (설계 리뷰 반영): 계보 검사, 펜싱+HALT, 영구 오류 격리(dead-letter), 복제본 열 진화, 정확한 REAL/BLOB 인코딩,
REPLACE 의 암묵 DELETE, 복제 끄면 링크 제거, 기동 시 링크 검증, 실행기 깨우기, (Postgres) 시퀀스 동기화."""
from __future__ import annotations

import os
import shutil
import sqlite3
import threading
import time
import uuid

import pytest

from lake_executor import replica as rp
from lake_executor.store import Store
from lake_executor.util import now_ms

from conftest import AlertsStub
from test_replica import PG_URL, _assert_same, _replicator, _workload, local, local_path, target  # noqa: F401 (fixtures)


def _linked(local_path, target):
    rep = _replicator(local_path, target)
    rep.check_link()
    return rep


# ---------------------------------------------------------------- 계보 / 펜싱
def test_restored_older_copy_is_refused(local, local_path, target, settings, tmp_path):
    rep = _linked(local_path, target)
    _workload(settings, local)
    rep.drain()
    rep.close()
    local.close()
    copy = str(tmp_path / "older-copy.db")
    shutil.copy(local_path, copy)                                  # 백업 시점의 복사본
    reopened = Store(local_path)
    reopened.enable_replication_log()
    _workload(settings, reopened)                                  # 원본은 계속 전진
    rep2 = _replicator(local_path, target)
    rep2.drain()
    rep2.close()
    reopened.close()
    old = _replicator(copy, target)                                # 오래된 복사본으로 기동 → 복제본이 앞서 있다
    try:
        with pytest.raises(rp.NotLinked, match="ahead"):
            old.check_link()
        with pytest.raises(rp.NotLinked):
            rp.startup_check(old)
    finally:
        old.close()


def test_twin_ledger_is_fenced_and_halts(local, local_path, target, settings, tmp_path):
    rep = _linked(local_path, target)
    _workload(settings, local)
    rep.drain()
    rep.close()
    local.close()
    twin = str(tmp_path / "twin.db")
    shutil.copy(local_path, twin)
    a = Store(local_path)
    b = Store(twin)
    a.enable_replication_log()
    b.enable_replication_log()
    _workload(settings, a)
    _workload(settings, b)
    ra = _replicator(local_path, target)
    rb = _replicator(twin, target)
    halt = str(tmp_path / "HALT")
    rb.halt_file = halt
    try:
        ra.check_link()
        rb.check_link()
        last_before = int(ra.target().get_state("last_id"))
        ra.replicate_once()                                        # a 가 먼저 전진
        with pytest.raises(rp.Fenced):                             # b 는 배치 시작에 읽은 last_id 가 바뀌어 거부
            rb.replicate_once()
        assert rb.state["fenced"] is True and os.path.exists(halt)
        with pytest.raises(rp.Fenced):                             # 재시작 전까지 복제하지 않는다
            rb.replicate_once()
        assert int(ra.target().get_state("last_id")) > last_before
        assert b.repl_queue_len() > 0                              # b 의 변경은 복제본에 들어가지 않았다
    finally:
        ra.close()
        rb.close()
        a.close()
        b.close()


def test_run_forever_stops_on_fence_and_alerts(local, local_path, target, settings, alerts, tmp_path):
    rep = _linked(local_path, target)
    _workload(settings, local)
    rep.drain()
    rep.close()
    local.close()
    twin = str(tmp_path / "twin2.db")
    shutil.copy(local_path, twin)
    a, b = Store(local_path), Store(twin)
    a.enable_replication_log()
    b.enable_replication_log()
    _workload(settings, a)
    _workload(settings, b)
    ra, rb = _replicator(local_path, target), _replicator(twin, target, alerts=alerts)
    rb.halt_file = str(tmp_path / "HALT2")
    try:
        ra.check_link()
        rb.check_link()                                            # 둘 다 링크는 맞다
        ra.replicate_once()                                        # a 가 먼저 전진
        rb.startup_delay_s = 0.0
        stop = threading.Event()
        t = threading.Thread(target=rb.run_forever, args=(stop,), daemon=True)
        t.start()
        t.join(10)
        assert not t.is_alive()                                    # Fenced → 루프 종료 (재시작 전까지 복제 안 함)
        assert rb.state["fenced"] is True and os.path.exists(rb.halt_file)
        assert sum("FENCED" in m for m in alerts.messages) == 1
    finally:
        ra.close()
        rb.close()
        a.close()
        b.close()


# ---------------------------------------------------------------- 영구 오류 격리
def test_poison_row_is_quarantined_and_replication_continues(local, local_path, target, settings, alerts):
    rep = _linked(local_path, target)
    rep.alerts = alerts
    _workload(settings, local)
    tgt = rep.target().store
    # 복제본에 로컬엔 없는 제약을 걸어 특정 행만 거부되게 한다: meta.value 길이 제한 (sqlite CHECK 는 ALTER 로 못 넣으므로 트리거로)
    with tgt._lock:
        if tgt.backend == "sqlite":
            tgt._conn.execute("CREATE TRIGGER reject_poison BEFORE INSERT ON meta WHEN NEW.key='poison' "
                              "BEGIN SELECT RAISE(ABORT, 'poison rejected'); END")
        else:
            tgt._conn.execute("ALTER TABLE meta ADD CONSTRAINT no_poison CHECK (key <> 'poison')")
    local.set_meta("poison", "x")
    local.set_meta("fine", "y")
    rep.startup_delay_s = 0.0
    rep.interval_s = 0.05
    rep.stuck_retry_s = 0.1
    stop = threading.Event()
    th = threading.Thread(target=rep.run_forever, args=(stop,), daemon=True)
    th.start()
    slow = 12 if PG_URL else 1                                       # Postgres: 격리는 행마다 왕복 (수십 초)
    deadline = time.monotonic() + 20 * slow
    while time.monotonic() < deadline and local.repl_queue_len() > 0:
        time.sleep(0.05)
    stop.set()
    th.join(10 * slow)
    assert not th.is_alive()
    assert local.repl_queue_len() == 0
    assert local.repl_dead_count() == 1 and local.repl_dead_rows()[0]["row"]["key"] == "poison"
    assert "poison" in local.repl_dead_rows()[0]["error"] or "no_poison" in local.repl_dead_rows()[0]["error"]
    tgt = target()                                                  # 복제기가 오류 뒤 대상 연결을 닫았으므로 다시 연다
    try:
        assert tgt.get_meta("fine") == "y" and tgt.get_meta("poison") is None
    finally:
        tgt.close()
    assert rep.status()["dead"] == 1
    assert sum("quarantined" in m for m in alerts.messages) == 1
    # 격리된 행 뒤의 변경도 계속 흐른다
    local.set_meta("later", "z")
    rep2 = _replicator(local_path, target)
    try:
        assert rep2.drain() >= 1 and rep2.target().store.get_meta("later") == "z"
    finally:
        rep2.close()
        rep.close()


# ---------------------------------------------------------------- 스키마 진화 / 인코딩
def test_missing_replica_column_is_added_before_replication(local, local_path, target, settings):
    rep = _linked(local_path, target)
    rep.close()
    tgt = target()
    with tgt._lock:
        tgt._conn.execute("ALTER TABLE lots DROP COLUMN last_event_id")
    tgt.close()
    _workload(settings, local)
    rep2 = _replicator(local_path, target)
    try:
        assert rep2.drain() > 0
        assert "last_event_id" in rep2.target().columns("lots")
        _assert_same(local, rep2.target().store)
    finally:
        rep2.close()


def test_real_values_replicate_bit_exact_and_stray_blob_in_text_column(local, local_path, target):
    rep = _linked(local_path, target)
    try:
        vals = [0.1 + 0.2, 80000.123456789012, 1 / 3, 2.0, 1e-9, 123456789.98765432]
        for i, v in enumerate(vals):
            local.insert_equity("test", "acct", 1_700_000_000_000 + i, v, v * 3, -v, v / 7)
        with local._tx():                                           # TEXT 열에 bytes (코드가 하지 않는 일이지만 트리거가 죽으면 안 된다)
            local._conn.execute("INSERT INTO meta(key, value) VALUES('blobby', ?)", (b"\x00\xffraw",))
        rep.drain()
        t = rep.target().store
        got = [r["total_equity"] for r in t.account_equity_series("test", "acct")]
        assert got == vals                                          # 17자리 printf → 비트 단위로 같다
        assert [r["unrealised_pnl"] for r in t.account_equity_series("test", "acct")] == [-v for v in vals]
        with t._lock:
            raw = t._conn.execute("SELECT value FROM meta WHERE key='blobby'").fetchone()[0]
        if t.backend == "sqlite":
            assert bytes(raw) == b"\x00\xffraw"
        else:                                                       # Postgres TEXT 열은 bytes 를 담을 수 없다 → bytea 의 텍스트 표기
            assert isinstance(raw, str) and "00ff" in raw.lower()
    finally:
        rep.close()


def test_replace_eviction_emits_delete_for_the_evicted_row(local, local_path, target):
    rep = _linked(local_path, target)                               # 양쪽이 빈 동안 링크
    # signal_log UNIQUE(mode,event_id): REPLACE 가 다른 id 의 행을 밀어내면 delete 가 큐에 들어가야 한다
    from lake_executor.signal_log import signal_to_row
    from lake_executor.schemas import Signal
    from conftest import make_signal
    sig = Signal.model_validate(make_signal())
    row = signal_to_row(sig, "new", now_ms(), [])
    assert local.append_signal_log(row) is True
    cols_list = local.table_schema("signal_log")["cols"]
    cols = ", ".join(cols_list)
    sel = ", ".join("9999" if c == "id" else c for c in cols_list)
    with local._lock:
        old_id = local._conn.execute("SELECT id FROM signal_log").fetchone()[0]
    # 같은 (mode,event_id) 로 다른 id 를 REPLACE → 옛 행의 delete + 새 행의 upsert 가 큐에 들어가야 한다 (recursive_triggers)
    with local._tx():
        local._conn.execute(f"INSERT OR REPLACE INTO signal_log({cols}) SELECT {sel} FROM signal_log WHERE id=?", (old_id,))
    ops = [(r["op"], r["row"].get("id")) for r in local.repl_fetch(100) if r["tbl"] == "signal_log"]
    assert ("delete", old_id) in ops and ("upsert", 9999) in ops
    try:
        rep.drain()
        assert [r["id"] for r in rep.target().store.signal_log_rows()] == [9999]
    finally:
        rep.close()


# ---------------------------------------------------------------- 끄기 / 기동 검증 / 실행기 깨우기
def test_disabling_replication_clears_link_so_next_linked_start_refuses(local, local_path, target):
    rep = _linked(local_path, target)
    rep.close()
    assert local.get_meta(rp.LINK_META_KEY)
    local.disable_replication_log()                                 # DATABASE_URL 없이 한 번 돌았다
    assert local.get_meta(rp.LINK_META_KEY) is None
    local.set_meta("gap", "lost-if-silently-relinked")
    local.enable_replication_log()
    rep2 = _replicator(local_path, target)
    try:
        with pytest.raises(rp.NotLinked):                           # 복제본에는 링크가 남아 있고 로컬은 없다 → pull/init 필요
            rep2.check_link()
    finally:
        rep2.close()


def test_startup_check_defers_only_when_local_has_link(local, local_path, tmp_path):
    def unreachable():
        raise ConnectionError("no route")
    rep = rp.Replicator(lambda: Store(local_path), unreachable)
    try:
        with pytest.raises(rp.NotLinked, match="unverified"):      # 링크조차 없는 장부 + 복제본 불통 → 기동 거부
            rp.startup_check(rep)
        local.set_meta(rp.LINK_META_KEY, "abc")
        assert rp.startup_check(rep) == "deferred"                   # 링크가 있으면 로컬 장부로 기동, 뒤에서 재검증
    finally:
        rep.close()


def test_receiver_wakes_executor_without_poll_delay(client, services, settings, store, executor):
    from conftest import SIGNAL_PATH, TEST_SIGNAL_SECRET, make_signal, sign_body
    assert not executor.wake.is_set()
    body, headers = sign_body(make_signal(), TEST_SIGNAL_SECRET)
    r = client.post(SIGNAL_PATH, content=body, headers=headers)
    assert r.status_code == 202
    assert executor.wake.is_set()


@pytest.mark.skipif(not PG_URL, reason="needs LAKE_TEST_DATABASE_URL")
def test_postgres_sequences_follow_replicated_ids(local, local_path, target):
    rep = _linked(local_path, target)
    try:
        for i in range(3):
            local.log_ingress("401", f"ev-{i}", b"deadbeef", "unauthorized")
        rep.drain()
        t = rep.target().store
        t.log_ingress("401", "direct", b"cafebabe", "written directly on the replica")   # BIGSERIAL 이 복제된 id 뒤를 잇는다
        assert t.recent_ingress(limit=10)[0]["id"] == 4
    finally:
        rep.close()
