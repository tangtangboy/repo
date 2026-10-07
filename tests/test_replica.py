"""장부 복제 (replica.py): 로컬 SQLite 장부의 변경을 외부 DB 에 비동기로 복제한다.

- 트리거: 모든 테이블의 INSERT/UPDATE → upsert 행, DELETE/PK 변경 → delete 행, BLOB 은 hex, 같은 트랜잭션 안에서 적재
- 실행기 실워크로드(entry→full_exit) 가 복제본에 **동일하게** 재현된다 (SQLite→SQLite 항상, LAKE_TEST_DATABASE_URL 있으면 SQLite→Postgres)
- 주문 경로는 복제본을 한 번도 호출하지 않는다
- 정확히 한 번: 대상 COMMIT 과 로컬 ack 사이에서 죽어도 재기동 뒤 중복 적용 없음 (last_id)
- 링크: 양쪽이 비어 있으면 자동, 아니면 NotLinked (알림 5분 1회) — pull / init 가 링크를 만든다
- 대상 장애: 백오프·알림·회복, 큐는 로컬에 남는다
- 설정: database.ledger local|remote → ledger_target / replica_url; 대시보드 Overview 행; CLI status (Postgres 있을 때)
"""
from __future__ import annotations

import os
import threading
import time
import uuid

import pytest

from lake_executor import config, replica as rp
from lake_executor.exchange import PaperExchange
from lake_executor.executor import Executor
from lake_executor.reporter import Reporter
from lake_executor.store import Store

from conftest import PAPER_PRICE, AlertsStub, FakeClient, make_signal, run_signal

PG_URL = os.environ.get("LAKE_TEST_DATABASE_URL", "").strip()


# ---------------------------------------------------------------- helpers
@pytest.fixture
def local_path(tmp_path):
    return str(tmp_path / "local.db")


@pytest.fixture
def local(local_path):
    s = Store(local_path)
    s.enable_replication_log()
    yield s
    s.close()


@pytest.fixture
def target(tmp_path):
    """(factory, cleanup): SQLite 파일 또는 Postgres 임시 스키마 (LAKE_TEST_DATABASE_URL)."""
    if PG_URL:
        schema = "lake_test_" + uuid.uuid4().hex[:10]
        factory = lambda: Store(PG_URL, schema=schema)  # noqa: E731
        yield factory
        s = factory()
        try:
            s._conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            s.close()
    else:
        path = str(tmp_path / "target.db")
        yield lambda: Store(path)


def _workload(settings, local, n=1):
    paper = PaperExchange(settings.accounts[0], price=PAPER_PRICE, id_tag=uuid.uuid4().hex[:6])   # 호출마다 다른 ID (장부에 이전 행이 남아 있어도 충돌 없음)
    alerts = AlertsStub()
    reporter = Reporter(settings, local, alerts, client=FakeClient())
    ex = Executor(settings, local, {"test": {"bybit": paper}, "live": {}}, reporter, alerts)
    try:
        for _ in range(n):
            pid = f"rep-{uuid.uuid4().hex[:8]}"
            base = dict(position_id=pid, leg="short", position_idx=2, strategy="overheat")
            e1 = make_signal(**base, event_sequence=1, action="entry", qty_btc=0.002, expected_qty_btc_after=0.002,
                             reference_price=PAPER_PRICE)
            assert run_signal(ex, local, e1)["status"] == "done"
            paper.set_price(PAPER_PRICE - 500.0)
            e2 = make_signal(**base, event_sequence=2, action="full_exit", qty_btc=0.002, expected_qty_btc_after=0,
                             reference_price=None)
            assert run_signal(ex, local, e2)["status"] == "done"
            paper.set_price(PAPER_PRICE)
    finally:
        reporter.close()


def _rows(store, t, cols):
    where = " WHERE key NOT LIKE 'replica%'" if t == "meta" else ""          # 링크 메타는 복제 대상이 아니다
    with store._lock:
        rows = store._conn.execute(f"SELECT {', '.join(cols)} FROM {t}{where}").fetchall()
    out = [tuple(bytes(v) if isinstance(v, memoryview) else v for v in r) for r in rows]
    return sorted(out, key=lambda r: tuple((str(type(v)), v) for v in r))


def _assert_same(local, target):
    for t in local.replicated_tables():
        s = local.table_schema(t)
        a, b = _rows(local, t, s["cols"]), _rows(target, t, s["cols"])
        assert len(a) == len(b), (t, len(a), len(b))
        for ra, rb in zip(a, b):
            for c, x, y in zip(s["cols"], ra, rb):
                if isinstance(x, float) or isinstance(y, float):
                    assert float(x) == pytest.approx(float(y), rel=1e-12, abs=1e-9), (t, c, x, y)
                else:
                    assert x == y, (t, c, x, y)


def _replicator(local_path, target, alerts=None, **kw):
    return rp.Replicator(lambda: Store(local_path), target, alerts=alerts, **kw)


# ---------------------------------------------------------------- 트리거
def test_triggers_capture_insert_update_delete_pk_change_and_blob(local, settings):
    assert local.replication_enabled() and len(local._repl_triggers()) == 4 * len(local.replicated_tables())
    local.set_meta("k1", "v1")
    local.set_meta("k1", "v2")
    with local._tx():
        local._conn.execute("UPDATE meta SET key='k2' WHERE key='k1'")
        local._conn.execute("DELETE FROM meta WHERE key='k2'")
    rows = [r for r in local.repl_fetch(100) if r["tbl"] == "meta" and r["row"].get("key") in ("k1", "k2")]
    ops = [(r["op"], r["row"].get("key"), r["row"].get("value")) for r in rows]
    assert ops[:2] == [("upsert", "k1", "v1"), ("upsert", "k1", "v2")]
    assert ("delete", "k1", None) in ops and ("upsert", "k2", "v2") in ops and ops[-1] == ("delete", "k2", None)
    assert all(r["created_at_ms"] > 1_700_000_000_000 for r in rows)
    # BLOB (signals.raw_body) 은 hex 로 실린다
    _workload(settings, local)
    sig_rows = [r for r in local.repl_fetch(10000) if r["tbl"] == "signals"]
    assert sig_rows and all(r["op"] == "upsert" for r in sig_rows)
    raw = sig_rows[0]["row"]["raw_body"]
    assert isinstance(raw, str) and bytes.fromhex(raw).startswith(b"{")
    assert local.repl_queue_oldest_ms() > 0


def test_disable_replication_log_drops_triggers_and_queue(local):
    local.set_meta("x", "1")
    assert local.repl_queue_len() > 0
    local.disable_replication_log()
    assert not local.replication_enabled() and local.repl_queue_len() == 0
    local.set_meta("x", "2")
    assert local.repl_queue_len() == 0
    local.enable_replication_log()
    local.set_meta("x", "3")
    assert local.repl_queue_len() == 1


# ---------------------------------------------------------------- 복제 본체
def test_executor_workload_replicates_identically_and_trade_path_never_touches_target(local, local_path, target, settings):
    rep = _replicator(local_path, target)
    try:
        link = rep.check_link()                                   # 양쪽 비어 있음 → 자동 링크
        assert rep.state["linked"] is True and local.get_meta(rp.LINK_META_KEY) == link
        tgt_conn = rep.target().store._conn
        calls = {"n": 0}
        if hasattr(tgt_conn, "set_trace_callback"):               # sqlite3.Connection: 메서드를 바꿀 수 없다
            tgt_conn.set_trace_callback(lambda _sql: calls.__setitem__("n", calls["n"] + 1))
        else:                                                     # _PgConn
            orig = tgt_conn.execute

            def counting(sql, params=()):
                calls["n"] += 1
                return orig(sql, params)
            tgt_conn.execute = counting
        _workload(settings, local, n=2)
        assert calls["n"] == 0                                    # 주문 경로는 복제본을 모른다
        queued = local.repl_queue_len()
        assert queued > 50
        applied = rep.drain()
        assert applied == queued and local.repl_queue_len() == 0
        _assert_same(local, rep.target().store)
        st = rep.status()
        assert st["linked"] is True and st["queue"] == 0 and st["applied_total"] <= queued and st["last_ok_ms"] > 0
        assert st["last_error"] == "" and st["last_id"] >= queued
        assert rep.drain() == 0                                   # 멱등
        assert rep.target().get_state("last_id") == str(st["last_id"])
        # 이어서 생기는 변경도 따라간다 (중간 갱신은 마지막 값만)
        local.set_meta("note", "a")
        local.set_meta("note", "b")
        assert rep.drain() == 2
        assert rep.target().store.get_meta("note") == "b"
    finally:
        rep.close()


def test_exactly_once_across_crash_between_target_commit_and_local_ack(local, local_path, target, settings):
    rep = _replicator(local_path, target)
    rep.check_link()
    _workload(settings, local)
    queued = local.repl_queue_len()
    crash_local = rep.local()
    orig_ack = crash_local.repl_ack

    def boom(max_id):
        raise RuntimeError("crash after target commit")
    crash_local.repl_ack = boom
    with pytest.raises(RuntimeError):
        rep.replicate_once()
    rep.close()                                                   # 프로세스 재시작을 흉내낸다
    assert local.repl_queue_len() == queued                       # 로컬 큐는 그대로 (ack 못 함)
    local.set_meta("after_crash", "1")
    rep2 = _replicator(local_path, target)
    try:
        n = rep2.replicate_once()
        assert n == queued + 1                                    # 큐 행은 읽었지만
        assert rep2.state["applied_total"] == 1                   # last_id 이하는 건너뛰고 새 행만 적용
        assert local.repl_queue_len() == 0
        _assert_same(local, rep2.target().store)
    finally:
        rep2.close()
    del orig_ack


def test_not_linked_refuses_and_alerts_once(local, local_path, target, settings, alerts):
    _workload(settings, local)                                    # 로컬에 데이터가 있고 대상은 비어 있음 → 자동 링크 불가
    rep = _replicator(local_path, target, alerts=alerts)
    try:
        with pytest.raises(rp.NotLinked):
            rep.replicate_once()
        assert rep.state["linked"] is False
        # 링크 불일치도 거부
        local.set_meta(rp.LINK_META_KEY, "aaaa")
        rep.target().set_state("link", "bbbb")
        with pytest.raises(rp.NotLinked):
            rep.check_link()
        rep.startup_delay_s = 0.0
        stop = threading.Event()
        t = threading.Thread(target=rep.run_forever, args=(stop,), daemon=True)
        t.start()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not alerts.messages:
            time.sleep(0.02)
        stop.set()
        t.join(5)
        assert not t.is_alive()
        assert sum("[replica]" in m and "not linked" in m for m in alerts.messages) == 1
        assert local.repl_queue_len() > 0                         # 큐는 보존
    finally:
        rep.close()


def test_pull_and_init_create_links_and_copies(local, local_path, target, settings, tmp_path):
    rep = _replicator(local_path, target)
    rep.check_link()
    _workload(settings, local, n=2)
    rep.drain()
    rep.close()
    # pull: 복제본 → 새 로컬 (컷오버/재해복구)
    new_path = str(tmp_path / "new.db")
    new_local = Store(new_path)
    tgt = target()
    try:
        counts = rp.pull(tgt, new_local)
        link = counts.pop("_link")
        assert counts["signals"] == 4 and counts["lots"] == 2 and counts["fills"] == 4 and counts["reports"] >= 8
        assert new_local.get_meta(rp.LINK_META_KEY) == link and rp._Target(tgt).get_state("link") == link
        assert rp._Target(tgt).get_state("last_id") == "0" and new_local.repl_queue_len() == 0
        assert not new_local.replication_enabled()                 # pull 은 트리거 없이 (복사가 큐에 들어가지 않는다)
        new_local.enable_replication_log()
        _assert_same(new_local, tgt)
    finally:
        tgt.close()
    # 새 로컬에서 이어서 매매하면 그대로 복제된다
    _workload(settings, new_local)
    rep2 = _replicator(new_path, target)
    try:
        assert rep2.check_link() == link
        assert rep2.drain() > 0
        _assert_same(new_local, rep2.target().store)
        # init: 대상이 비어 있지 않으면 거부, --force 면 PK 로 덮어쓴다
        with pytest.raises(rp.ReplicaError):
            rp.init_push(new_local, rep2.target().store)
        counts = rp.init_push(new_local, rep2.target().store, force=True)
        assert counts["signals"] == 6 and counts["_dropped_queue_rows"] >= 0
        assert new_local.get_meta(rp.LINK_META_KEY) == counts["_link"] == rep2.target().get_state("link")
        assert rp.compare(new_local, rep2.target().store)["signals"] == (6, 6)
    finally:
        rep2.close()
        new_local.close()


def test_target_failure_backoff_alert_and_recovery(local, local_path, target, settings, alerts):
    rep0 = _replicator(local_path, target)
    rep0.check_link()
    rep0.close()
    _workload(settings, local)
    fails = {"n": 2}

    def flaky():
        if fails["n"] > 0:
            fails["n"] -= 1
            raise ConnectionError("target down")
        return target()
    rep = rp.Replicator(lambda: Store(local_path), flaky, alerts=alerts)
    rep.startup_delay_s = 0.0
    rep.interval_s = 0.05
    stop = threading.Event()
    t = threading.Thread(target=rep.run_forever, args=(stop,), daemon=True)
    t.start()
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline and local.repl_queue_len() > 0:
        time.sleep(0.05)
    stop.set()
    t.join(10)
    assert not t.is_alive()
    st = rep.status()
    assert local.repl_queue_len() == 0 and st["errors"] >= 1 and st["last_ok_ms"] > st["last_error_ms"]
    assert sum("[replica] ConnectionError" in m for m in alerts.messages) == 1 and alerts.contains("[replica] recovered")
    tgt = target()
    try:
        _assert_same(local, tgt)
    finally:
        tgt.close()


def test_coalescing_keeps_last_op_per_key(local, local_path, target):
    rep = _replicator(local_path, target)
    try:
        rep.check_link()
        local.set_meta("a", "1")
        local.set_meta("a", "2")
        local.set_meta("b", "1")
        with local._tx():
            local._conn.execute("DELETE FROM meta WHERE key='b'")
        queued = local.repl_queue_len()                           # 링크 메타(replica_link) 는 큐에 들어가지 않는다
        assert queued == 4 and rep.replicate_once() == queued
        assert rep.state["applied_total"] == 4 and rep.state["written_total"] == 2   # a(최종값) upsert, b delete
        t = rep.target().store
        assert t.get_meta("a") == "2" and t.get_meta("b") is None
    finally:
        rep.close()


# ---------------------------------------------------------------- 설정 / 대시보드 / CLI
def test_config_ledger_modes(settings_factory):
    s = settings_factory()
    assert s.db_ledger == "local" and s.ledger_target == s.db_path and s.replica_url == ""
    url = "postgresql://user:pw@db.example.com:5432/postgres"
    s2 = settings_factory(env_overrides={"DATABASE_URL": url})
    assert s2.ledger_target == s2.db_path and s2.replica_url == url            # 기본: 로컬 장부 + 복제
    s3 = settings_factory(config_overrides={"database": {"ledger": "remote"}}, env_overrides={"DATABASE_URL": url})
    assert s3.ledger_target == url and s3.replica_url == ""                    # 옛 동작
    with pytest.raises(config.ConfigError):
        settings_factory(config_overrides={"database": {"ledger": "cloud"}})


def test_dashboard_overview_replica_row(client, services, settings):
    from test_web import login
    login(client)
    r = client.get("/ui")
    assert r.status_code == 200 and "replica (DATABASE_URL, async)" in r.text and "none (no DATABASE_URL)" in r.text

    class FakeRep:
        def status(self):
            return {"linked": True, "queue": 3, "lag_ms": 4200, "applied_total": 10, "last_ok_ms": 1, "last_error": "",
                    "last_error_ms": 0, "target": "postgres h:5432/db"}
    services.replica = FakeRep()
    r = client.get("/ui")
    assert "catching up (3 queued, lag 4s)" in r.text

    class BadRep(FakeRep):
        def status(self):
            return dict(super().status(), linked=False)
    services.replica = BadRep()
    assert "NOT LINKED" in client.get("/ui").text
    del services.replica


@pytest.mark.skipif(not PG_URL, reason="needs LAKE_TEST_DATABASE_URL")
def test_cli_replica_status_pull_init_and_check(settings_factory, capsys, tmp_path):
    from lake_executor.main import main as cli_main
    schema = "lake_test_" + uuid.uuid4().hex[:10]
    s = settings_factory(config_overrides={"database": {"schema": schema}}, env_overrides={"DATABASE_URL": PG_URL})
    from pathlib import Path
    root = Path(s.state_dir).parent
    argv = ["--config", str(root / "config.json"), "--env", str(root / ".env")]
    try:
        local = Store(s.db_path)
        local.enable_replication_log()
        _workload(s, local)
        local.close()
        assert cli_main(argv + ["replica", "init"]) == 2                       # --yes 필요
        assert cli_main(argv + ["replica", "init", "--yes"]) == 0
        out = capsys.readouterr().out
        assert "pushed sqlite" in out and "signals" in out
        assert cli_main(argv + ["replica", "status"]) == 0
        out = capsys.readouterr().out
        assert "linked : yes" in out and "queued changes=0" in out
        assert cli_main(argv + ["check"]) in (0, 1)
        out = capsys.readouterr().out
        assert "ledger            : sqlite" in out and "replica           :" in out and "linked" in out
        assert cli_main(argv + ["replica", "pull", "--yes"]) == 0
        out = capsys.readouterr().out
        assert "pulled" in out and os.path.exists(s.db_path)
        pulled = Store(s.db_path)
        try:
            assert pulled.count_rows("signals") == 2 and pulled.get_meta(rp.LINK_META_KEY)
        finally:
            pulled.close()
    finally:
        t = Store(PG_URL, schema=schema)
        try:
            t._conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            t.close()
