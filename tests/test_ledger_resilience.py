"""원장 백엔드 전환·재연결 복구 관련 동작 (SQLite 로 돌아간다; 실제 Postgres 는 test_store_postgres.py).

- store.translate_pg: SQLite 문장 → Postgres 문장 변환 규칙
- config.validate_database: DATABASE_URL / database.schema / guards.expired_actions_execute 검증
- 수신기: 만료된 신호라도 정책에 든 action(full_exit 등) 은 202 로 접수, entry 는 410; 원장 불통이면 503 + Retry-After
- 실행기: 만료된 full_exit 는 정책에 따라 실행되고 run note 에 stale 표시, 만료된 entry/add 는 EXPIRED 거부;
  루프는 LedgerUnavailable 에 백오프하며 같은 오류 알림을 5분에 한 번만 보낸다
- 대시보드: DATABASE_URL 이 편집 가능한 키이고 Overview 에 원장 백엔드가 보인다 (값/비밀번호는 안 보인다)
"""
from __future__ import annotations

import json
import os
import threading
import time

import pytest

from lake_executor import config, store as st
from lake_executor.store import LedgerUnavailable, describe_target, is_postgres_url, translate_pg
from lake_executor.util import now_ms

from conftest import (
    ADMIN_TOKEN,
    SIGNAL_PATH,
    TEST_SIGNAL_SECRET,
    execution_statuses,
    ingest,
    load_reports,
    make_signal,
    run_signal,
    sign_body,
)

HERE = os.path.dirname(os.path.abspath(__file__))


def _example(name: str) -> dict:
    with open(os.path.join(HERE, "..", "tools", "signals", name), encoding="utf-8") as f:
        return json.load(f)


def _post(client, d: dict, secret: str = TEST_SIGNAL_SECRET):
    raw, headers = sign_body(d, secret)
    return client.post(SIGNAL_PATH, content=raw, headers=headers)


# ---------------------------------------------------------------- translate_pg
def test_translate_pg_rules():
    assert translate_pg("BEGIN IMMEDIATE", False) == "BEGIN"
    assert translate_pg("INSERT OR IGNORE INTO fills(a,b) VALUES(?,?)", True) == \
        "INSERT INTO fills(a,b) VALUES(%s,%s) ON CONFLICT DO NOTHING"
    assert translate_pg("SELECT * FROM lots WHERE protection_orders LIKE '%order_link_id%' AND mode=?", True) == \
        "SELECT * FROM lots WHERE protection_orders LIKE '%%order_link_id%%' AND mode=%s"
    assert translate_pg("SELECT '%' AS pct", False) == "SELECT '%' AS pct"          # 파라미터 없음 → % 그대로
    ddl = translate_pg(st.TABLES["ingress_log"], False)
    assert "BIGSERIAL PRIMARY KEY" in ddl and "AUTOINCREMENT" not in ddl and "BIGINT" in ddl
    sig_ddl = translate_pg(st.TABLES["signals"], False)
    assert "BYTEA" in sig_ddl and "DOUBLE PRECISION" in sig_ddl and "INTEGER" not in sig_ddl
    idx = translate_pg(st.INDEXES["signals"][0][1], False)
    assert idx.startswith("CREATE INDEX IF NOT EXISTS")
    assert translate_pg("UPDATE orders SET status=?, updated_at_ms=? WHERE account=? AND order_link_id=?;", True).endswith(
        "WHERE account=%s AND order_link_id=%s")


def test_url_helpers_hide_password():
    assert is_postgres_url("postgresql://u:s3cret@db.example.com:5432/postgres")
    assert is_postgres_url("postgres://u:s3cret@h/db") and not is_postgres_url("state/lake.db") and not is_postgres_url(None)
    d = describe_target("postgresql://postgres.ref:s3cret@aws-1-ap-south-1.pooler.supabase.com:5432/postgres")
    assert d == "postgres aws-1-ap-south-1.pooler.supabase.com:5432/postgres" and "s3cret" not in d
    assert describe_target("state/lake.db") == "sqlite state/lake.db"


# ---------------------------------------------------------------- config
def test_database_config_validation(settings_factory):
    s = settings_factory()
    assert s.db_url == "" and s.ledger_target.endswith("lake.db") and s.db_schema == "lake_executor"
    assert s.expired_actions_execute == ["partial_exit", "full_exit", "protection_update"]
    ok = settings_factory(env_overrides={"DATABASE_URL": "postgresql://u:p@aws-1-ap-south-1.pooler.supabase.com:5432/postgres"},
                          config_overrides={"database": {"schema": "lake_x"}, "guards": {"expired_actions_execute": ["full_exit"]}})
    # 기본(database.ledger=local): 장부는 SQLite, DATABASE_URL 은 비동기 복제본. remote 로 바꾸면 URL 이 장부가 된다.
    assert ok.ledger_target.endswith("lake.db") and ok.replica_url.startswith("postgresql://")
    assert ok.db_schema == "lake_x" and ok.expired_actions_execute == ["full_exit"]
    remote = settings_factory(config_overrides={"database": {"schema": "lake_x", "ledger": "remote"}},
                              env_overrides={"DATABASE_URL": "postgresql://u:p@aws-1-ap-south-1.pooler.supabase.com:5432/postgres"})
    assert remote.ledger_target.startswith("postgresql://") and remote.replica_url == ""
    with pytest.raises(config.ConfigError, match="transaction pooler"):
        settings_factory(env_overrides={"DATABASE_URL": "postgresql://u:p@aws-1-ap-south-1.pooler.supabase.com:6543/postgres"})
    with pytest.raises(config.ConfigError, match="query parameters"):
        settings_factory(env_overrides={"DATABASE_URL": "postgresql://u:p@h:5432/postgres?pgbouncer=true"})
    with pytest.raises(config.ConfigError, match="postgres://"):
        settings_factory(env_overrides={"DATABASE_URL": "mysql://u:p@h/db"})
    with pytest.raises(config.ConfigError, match="database.schema"):
        settings_factory(config_overrides={"database": {"schema": "Bad-Name"}})
    with pytest.raises(config.ConfigError, match="unknown actions"):
        settings_factory(config_overrides={"guards": {"expired_actions_execute": ["entry", "teleport"]}})
    with pytest.raises(config.ConfigError, match="list of action names"):
        settings_factory(config_overrides={"guards": {"expired_actions_execute": "full_exit"}})


# ---------------------------------------------------------------- receiver
def test_receiver_accepts_expired_exit_but_rejects_expired_entry(client, store):
    old = now_ms() - 5000
    entry = make_signal(ts=old, expires_at_ms=old + 1000)
    assert _post(client, entry).status_code == 410
    fx = dict(_example("5_full_exit_long.json"))
    fx.update(make_signal(action="full_exit", qty_btc=fx.get("qty_btc"), expected_qty_btc_after=fx.get("expected_qty_btc_after"),
                          leg=fx["leg"], position_idx=fx["position_idx"], ts=old, expires_at_ms=old + 1000))
    r = _post(client, fx)
    assert r.status_code == 202, r.text
    assert store.get_signal(fx["event_id"], "test")["status"] == "accepted"


def test_receiver_expired_policy_can_be_disabled(settings_factory, store):
    """guards.expired_actions_execute=[] 면 예전처럼 전부 410."""
    from fastapi.testclient import TestClient
    from lake_executor.receiver import create_app
    s = settings_factory(config_overrides={"guards": {"expired_actions_execute": []}})
    st_ = st.Store(s.db_path)
    try:
        with TestClient(create_app(s, st_, None)) as c:
            old = now_ms() - 5000
            fx = dict(_example("5_full_exit_long.json"))
            fx.update(make_signal(action="full_exit", qty_btc=fx.get("qty_btc"), expected_qty_btc_after=fx.get("expected_qty_btc_after"),
                                  leg=fx["leg"], position_idx=fx["position_idx"], ts=old, expires_at_ms=old + 1000))
            assert _post(c, fx).status_code == 410
    finally:
        st_.close()


def test_receiver_returns_503_when_ledger_unavailable(client, store, monkeypatch):
    def boom(*a, **k):
        raise LedgerUnavailable("postgres connect failed: OperationalError")
    monkeypatch.setattr(store, "insert_signal", boom)
    r = _post(client, make_signal())
    assert r.status_code == 503 and r.json()["code"] == "LEDGER_UNAVAILABLE" and r.headers.get("retry-after") == "5"
    assert client.get("/healthz").status_code in (200, 503)


# ---------------------------------------------------------------- executor
def _opened(executor, store, paper) -> dict:
    d = make_signal()                                        # entry short 0.002 (make_signal 기본)
    row = run_signal(executor, store, d)
    assert row["status"] == "done"
    assert paper.positions() != {}
    return d


def test_expired_full_exit_is_executed_as_stale(executor, store, paper):
    d = _opened(executor, store, paper)
    old = now_ms() - 5000
    fx = make_signal(position_id=d["position_id"], event_sequence=2, action="full_exit", qty_btc=d["qty_btc"],
                     expected_qty_btc_after=0.0, leg=d["leg"], position_idx=d["position_idx"], ts=old, expires_at_ms=old + 1000)
    row = run_signal(executor, store, fx)
    assert row["status"] == "done", row
    run = store.get_run("test", fx["event_id"], "bybit")
    assert run["status"] == "done" and run["note"].startswith("stale(executed after expires_at_ms)")
    assert paper.positions() == {}                            # 늦었지만 청산됐다
    assert "rejected" not in execution_statuses(load_reports(store, "test"))[-3:]


def test_expired_add_is_still_rejected(executor, store, paper):
    d = _opened(executor, store, paper)
    before = paper.positions()
    old = now_ms() - 5000
    add = make_signal(position_id=d["position_id"], event_sequence=2, action="add", qty_btc=0.001,
                      expected_qty_btc_after=0.003, leg=d["leg"], position_idx=d["position_idx"], ts=old, expires_at_ms=old + 1000)
    row = run_signal(executor, store, add)
    assert row["status"] == "rejected" and row["reason_code"] == "EXPIRED"
    assert paper.positions() == before


def test_executor_loop_backs_off_on_ledger_unavailable_and_alerts_once(executor, store, alerts, monkeypatch):
    calls = {"n": 0}
    real = store.claim_next_signal

    def flaky():
        calls["n"] += 1
        if calls["n"] <= 3:
            raise LedgerUnavailable("down")
        return real()

    monkeypatch.setattr(store, "claim_next_signal", flaky)
    stop = threading.Event()
    t = threading.Thread(target=executor.run_forever, args=(stop,), daemon=True)
    t.start()
    deadline = time.time() + 15
    while calls["n"] < 4 and time.time() < deadline:
        time.sleep(0.1)
    stop.set()
    t.join(timeout=35)
    assert calls["n"] >= 4
    errs = [m for m in alerts.messages if "loop error: LedgerUnavailable" in m]
    assert len(errs) == 1, alerts.messages                     # 3번 실패했지만 알림은 한 번
    assert any("loop recovered after LedgerUnavailable" in m for m in alerts.messages)


# ---------------------------------------------------------------- dashboard
def test_dashboard_exposes_database_url_key_and_ledger_row(client, settings, services, app):
    from types import SimpleNamespace
    from lake_executor.web import allowed_env_keys, in_process_env
    env_path = os.path.join(os.path.dirname(settings.state_dir), ".env")
    cfg_path = os.path.join(os.path.dirname(settings.state_dir), "config.json")
    services.paths = SimpleNamespace(env=env_path, config=cfg_path)
    specs = allowed_env_keys(settings)
    assert "DATABASE_URL" in specs and specs["DATABASE_URL"].secret is True
    assert in_process_env(settings)["DATABASE_URL"] == ""
    r = client.post("/ui/login", data={"token": ADMIN_TOKEN}, follow_redirects=False)
    assert r.status_code == 303
    page = client.get("/ui", follow_redirects=False).text
    assert "ledger" in page and "expired actions still executed" in page and "full_exit" in page
    assert ("sqlite" in page) or ("postgres" in page)       # LAKE_TEST_DATABASE_URL 로 돌리면 postgres
    assert "@" not in page.split("ledger", 1)[1][:300]      # 접속 문자열(비밀번호) 은 절대 안 보인다
    secrets_page = client.get("/ui/secrets", follow_redirects=False).text
    assert "DATABASE_URL" in secrets_page


# ---------------------------------------------------------------- paper exchange ids must not collide across restarts
def test_paper_exchange_ids_are_tagged_in_production_factory(settings):
    from lake_executor.exchange import PaperExchange, build_exchange
    acct = settings.accounts[0]
    plain = PaperExchange(acct)
    o = plain.place_market("Buy", 0.001, 1, False, "lk-plain")
    assert o["order_id"] == "porder-1" and plain.executions("porder-1")[0]["exec_id"] == "pexec-1"   # 테스트는 결정적
    tagged = PaperExchange(acct, id_tag="abc")
    o2 = tagged.place_market("Buy", 0.001, 1, False, "lk-tagged")
    assert o2["order_id"] == "porder-abc-1" and tagged.executions("porder-abc-1")[0]["exec_id"] == "pexec-abc-1"
    # 운영 팩토리(test 모드 simulate_fills) 는 프로세스마다 다른 태그를 붙인다 → 재시작 뒤 이전 체결 ID 와 충돌하지 않는다
    ex = build_exchange(acct, kind="paper")
    assert isinstance(ex, PaperExchange)
    o3 = ex.place_market("Buy", 0.001, 1, False, "lk-prod")
    assert o3["order_id"].startswith("porder-") and o3["order_id"] != "porder-1"
    assert ex.executions(o3["order_id"])[0]["exec_id"] != "pexec-1"
