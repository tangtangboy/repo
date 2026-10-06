"""web.py — 운영 대시보드 (/ui): 404 정책, 로그인/세션 쿠키/CSRF/속도 제한, 페이지 렌더링, .env/config 쓰기, 제어, 시크릿 비노출.

전부 오프라인. 거래소 생성자 드라이런과 연결 확인은 PaperExchange 팩토리로 대체하고, 재시작은 kill 기록기로 대체한다.
"""
from __future__ import annotations

import json
import os
import re
import signal
import stat
import threading
import time
import urllib.parse
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from lake_executor import config, receiver as rmod, util
from lake_executor.exchange import ExchangeError, PaperExchange
from lake_executor.executor import Executor
from lake_executor.receiver import create_app
from lake_executor.reporter import Reporter
from lake_executor.store import Store
from lake_executor.util import now_ms, parse_env_file, read_json, write_env_file
from lake_executor.web import SessionCodec

from conftest import (
    ADMIN_TOKEN,
    ALL_SECRETS,
    PAPER_PRICE,
    SIGNAL_PATH,
    TEST_SIGNAL_SECRET,
    AlertsStub,
    FakeClient,
    ingest,
    make_signal,
    run_signal,
    sign_body,
)

CSRF_RE = re.compile(r'name="_csrf" value="([0-9a-f]+)"')
PAGES = ("/ui", "/ui/signals", "/ui/orders", "/ui/reports", "/ui/ingress", "/ui/accounts", "/ui/secrets", "/ui/controls")


# ---------------------------------------------------------------- 도우미
def env_path(settings) -> str:
    return os.path.join(os.path.dirname(settings.state_dir), ".env")


def cfg_path(settings) -> str:
    return os.path.join(os.path.dirname(settings.state_dir), "config.json")


def paper_factory(acct):
    return PaperExchange(acct, price=PAPER_PRICE)


def csrf_of(client: TestClient) -> str:
    r = client.get("/ui/controls", follow_redirects=False)
    assert r.status_code == 200, r.status_code
    m = CSRF_RE.search(r.text)
    assert m, "csrf field missing"
    return m.group(1)


def login(client: TestClient) -> str:
    r = client.post("/ui/login", data={"token": ADMIN_TOKEN}, follow_redirects=False)
    assert r.status_code == 303, r.text
    assert client.cookies.get("lake_ui")
    return csrf_of(client)


class UI:
    """로그인된 TestClient + csrf. post() 는 _csrf 를 자동으로 넣고 리다이렉트를 따라가지 않는다."""

    def __init__(self, client: TestClient, csrf: str, app, settings, services, store, alerts):
        self.client, self.csrf, self.app = client, csrf, app
        self.settings, self.services, self.store, self.alerts = settings, services, store, alerts
        self.env = env_path(settings)
        self.config = cfg_path(settings)

    def get(self, path: str, **kw):
        return self.client.get(path, follow_redirects=False, **kw)

    def page(self, path: str) -> str:
        r = self.get(path)
        assert r.status_code == 200, (path, r.status_code)
        return r.text

    def post(self, path: str, data: dict | None = None, *, csrf: bool = True, **kw):
        d = dict(data or {})
        if csrf:
            d.setdefault("_csrf", self.csrf)
        return self.client.post(path, data=d, follow_redirects=False, **kw)


@pytest.fixture
def ui(client, app, settings, services, store, alerts):
    services.paths = SimpleNamespace(env=env_path(settings), config=cfg_path(settings))
    app.state.ui_exchange_factory = paper_factory
    app.state.ui_restart_guard.kill = lambda pid, sig: None
    return UI(client, login(client), app, settings, services, store, alerts)


@contextmanager
def rig(settings, *, paths: bool = True):
    """다른 Settings(멀티 계정 / paths 없음) 로 대시보드 전체를 띄운다."""
    store = Store(settings.db_path)
    alerts = AlertsStub()
    reporter = Reporter(settings, store, alerts, client=FakeClient())
    paper = {a.name: PaperExchange(a, price=PAPER_PRICE) for a in settings.accounts}
    executor = Executor(settings, store, {"test": dict(paper), "live": {}}, reporter, alerts)
    services = SimpleNamespace(executor=executor, reporter=reporter, alerts=alerts)
    if paths:
        services.paths = SimpleNamespace(env=env_path(settings), config=cfg_path(settings))
    app = create_app(settings, store, services)
    app.state.ui_exchange_factory = paper_factory
    app.state.ui_restart_guard.kill = lambda pid, sig: None
    try:
        with TestClient(app) as client:
            yield UI(client, login(client), app, settings, services, store, alerts)
    finally:
        reporter.close()
        store.close()


def assert_no_secret(text: str, *extra: str) -> None:
    for s in (*ALL_SECRETS, *extra):
        assert s not in text


# ---------------------------------------------------------------- 1. 404 정책
def test_ui_is_404_without_admin_token(settings_factory):
    s = settings_factory(drop_env=("ADMIN_TOKEN",))
    store = Store(s.db_path)
    try:
        with TestClient(create_app(s, store, SimpleNamespace())) as c:
            for path in ("/ui/login", *PAGES, "/ui/signals/event-1"):
                r = c.get(path, follow_redirects=False)
                assert r.status_code == 404 and r.json() == {"error": "NOT_FOUND"}, path
            for path in ("/ui/login", "/ui/logout", "/ui/controls/halt", "/ui/secrets", "/ui/accounts/bybit/keys"):
                r = c.post(path, data={"token": ADMIN_TOKEN, "_csrf": "x"}, follow_redirects=False)
                assert r.status_code == 404 and r.json() == {"error": "NOT_FOUND"}, path
            assert not os.path.exists(s.halt_file)
    finally:
        store.close()


# ---------------------------------------------------------------- 2-6. 로그인 / 세션
def test_login_page_renders_without_values(client):
    r = client.get("/ui/login")
    assert r.status_code == 200
    assert 'name="token"' in r.text and 'type="password"' in r.text
    assert "value=" not in r.text
    assert r.headers["cache-control"] == "no-store"
    assert "default-src 'none'" in r.headers["content-security-policy"]
    assert "frame-ancestors 'none'" in r.headers["content-security-policy"]
    assert r.headers["x-frame-options"] == "DENY"
    assert r.headers["x-content-type-options"] == "nosniff"
    assert r.headers["referrer-policy"] == "no-referrer"
    assert_no_secret(r.text)


def test_login_wrong_token_401_and_throttles(client):
    for _ in range(rmod.ADMIN_FAIL_LIMIT):
        r = client.post("/ui/login", data={"token": "guess"}, follow_redirects=False)
        assert r.status_code == 401 and "set-cookie" not in r.headers
    # 한도를 넘긴 IP 는 올바른 토큰도 비교 없이 401 (쿠키 없음), /state 도 같은 예산
    r = client.post("/ui/login", data={"token": ADMIN_TOKEN}, follow_redirects=False)
    assert r.status_code == 401 and "set-cookie" not in r.headers and "too many attempts" in r.text
    assert client.get("/state", headers={"X-Admin-Token": ADMIN_TOKEN}).status_code == 401
    assert_no_secret(r.text)


def test_admin_header_failures_block_ui_login(client):
    for _ in range(rmod.ADMIN_FAIL_LIMIT):
        assert client.get("/state", headers={"X-Admin-Token": "guess"}).status_code == 401
    assert client.post("/ui/login", data={"token": ADMIN_TOKEN}, follow_redirects=False).status_code == 401


def test_login_ok_sets_cookie_attributes(client):
    r = client.post("/ui/login", data={"token": ADMIN_TOKEN}, follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"] == "/ui"
    sc = r.headers["set-cookie"]
    for attr in ("lake_ui=", "HttpOnly", "SameSite=Strict", "Path=/ui", "Max-Age=43200"):
        assert attr in sc, sc
    assert "Secure" not in sc
    assert r.headers["cache-control"] == "no-store"
    # 세션이 있으면 로그인 페이지는 개요로 보낸다
    assert client.get("/ui/login", follow_redirects=False).status_code == 303
    # Caddy 뒤(https) 에서는 Secure 가 붙는다 (쿠키 저장소가 없는 클라이언트로: Secure 쿠키는 http 로 되돌아오지 않는다).
    # X-Forwarded-Proto 는 피어가 루프백(=같은 호스트의 Caddy) 일 때만 믿는다.
    fresh = TestClient(client.app, client=("127.0.0.1", 50000))
    r = fresh.post("/ui/login", data={"token": ADMIN_TOKEN}, headers={"X-Forwarded-Proto": "https"},
                   follow_redirects=False)
    assert r.status_code == 303 and "Secure" in r.headers["set-cookie"]
    spoof = TestClient(client.app)   # 피어 "testclient" (루프백 아님) → 헤더 무시
    r = spoof.post("/ui/login", data={"token": ADMIN_TOKEN}, headers={"X-Forwarded-Proto": "https"},
                   follow_redirects=False)
    assert r.status_code == 303 and "Secure" not in r.headers["set-cookie"]


def test_client_ip_trusts_forwarded_for_only_from_loopback():
    def req(host, xff=None):
        headers = {"x-forwarded-for": xff} if xff else {}
        return SimpleNamespace(headers=headers, client=SimpleNamespace(host=host) if host else None)
    assert rmod._client_ip(req("127.0.0.1", "9.9.9.9, 10.0.0.1")) == "9.9.9.9"
    assert rmod._client_ip(req("::1", "9.9.9.9")) == "9.9.9.9"
    assert rmod._client_ip(req("203.0.113.5", "9.9.9.9")) == "203.0.113.5"   # 루프백이 아닌 피어의 헤더는 무시
    assert rmod._client_ip(req("testclient", "9.9.9.9")) == "testclient"
    assert rmod._client_ip(req("127.0.0.1", " , 9.9.9.9")) == "127.0.0.1"    # 빈 첫 항목 → 피어
    assert rmod._client_ip(req("203.0.113.5")) == "203.0.113.5"
    assert rmod._client_ip(req(None, "9.9.9.9")) == "?"


def test_login_throttle_cannot_be_reset_by_forwarded_for(client):
    # 루프백이 아닌 피어가 X-Forwarded-For 를 매번 바꿔도 같은 예산을 쓴다
    for i in range(rmod.ADMIN_FAIL_LIMIT):
        r = client.post("/ui/login", data={"token": "guess"}, headers={"X-Forwarded-For": f"10.0.0.{i}"},
                        follow_redirects=False)
        assert r.status_code == 401
    r = client.post("/ui/login", data={"token": ADMIN_TOKEN}, headers={"X-Forwarded-For": "10.0.0.99"},
                    follow_redirects=False)
    assert r.status_code == 401 and "too many attempts" in r.text and "set-cookie" not in r.headers


def test_token_in_query_or_header_is_ignored(client, settings):
    h = {"X-Admin-Token": ADMIN_TOKEN}
    r = client.get(f"/ui?token={ADMIN_TOKEN}", headers=h, follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"] == "/ui/login"
    r = client.post("/ui/controls/halt", data={"token": ADMIN_TOKEN, "_csrf": "x"}, headers=h, follow_redirects=False)
    assert r.status_code == 401
    assert not os.path.exists(settings.halt_file)
    r = client.get(f"/ui/login?token={ADMIN_TOKEN}", headers=h, follow_redirects=False)
    assert r.status_code == 200 and "set-cookie" not in r.headers


def test_session_cookie_tamper_rejected(ui):
    cookie = ui.client.cookies.get("lake_ui")
    sid, exp, sig = cookie.split(".")
    codec: SessionCodec = ui.app.state.ui_session_codec
    key = codec._key()

    def flip(s: str, i: int = 0) -> str:
        return s[:i] + ("a" if s[i] != "a" else "b") + s[i + 1:]

    past = now_ms() - 1000
    expired_signed = f"{sid}.{past}.{SessionCodec._sig(key, sid, past)}"
    other = SessionCodec(lambda: "other-admin-token-0123456789abcdef-0123456789").issue(now_ms()).cookie
    bad_cookies = [f"{flip(sid)}.{exp}.{sig}", f"{sid}.{int(exp) + 1}.{sig}", f"{sid}.{exp}.{flip(sig)}",
                   f"{sid}.{exp}", "garbage", "", expired_signed, other, f"{sid}.notanumber.{sig}"]
    # '²' 는 str.isdigit() 이 True 지만 int() 가 ValueError 를 낸다 (원시 0xB2 바이트 쿠키가 latin-1 로 디코드되면 이 값) → None, 예외 없음
    assert codec.parse(f"{sid}.\u00b2.{sig}", now_ms()) is None
    assert codec.parse(f"{sid}.\u0661\u0662.{sig}", now_ms()) is None        # 아랍-인도 숫자
    assert codec.parse(f"{sid}.{'9' * 40}.{sig}", now_ms()) is None
    fresh = TestClient(ui.app)   # 쿠키 저장소 없는 클라이언트
    for bad in bad_cookies:
        r = fresh.get("/ui", headers={"Cookie": f"lake_ui={bad}"}, follow_redirects=False)
        assert r.status_code == 303 and r.headers["location"] == "/ui/login", bad
        r = fresh.post("/ui/controls/halt", data={"_csrf": ui.csrf}, headers={"Cookie": f"lake_ui={bad}"},
                       follow_redirects=False)
        assert r.status_code == 401, bad
    # 원본 쿠키는 통과
    assert fresh.get("/ui", headers={"Cookie": f"lake_ui={cookie}"}, follow_redirects=False).status_code == 200
    assert not os.path.exists(ui.settings.halt_file)


# ---------------------------------------------------------------- 7-9. POST 보호
def test_unauthenticated_post_401(client, settings):
    r = client.post("/ui/controls/halt", data={"_csrf": "x"}, follow_redirects=False)
    assert r.status_code == 401 and r.text == "UNAUTHORIZED"
    assert not os.path.exists(settings.halt_file)


def test_csrf_missing_or_wrong_403(ui):
    r = ui.post("/ui/controls/halt", {"other": "1"}, csrf=False)
    assert r.status_code == 403 and r.text == "CSRF"
    r = ui.post("/ui/controls/halt", {"_csrf": "0" * 64})
    assert r.status_code == 403
    r = ui.post("/ui/controls/halt", {"_csrf": ui.csrf[:-1]})
    assert r.status_code == 403
    assert not os.path.exists(ui.settings.halt_file)
    # CSRF 실패도 토큰 실패와 같은 IP 예산을 쓴다
    for _ in range(rmod.ADMIN_FAIL_LIMIT):
        ui.post("/ui/controls/halt", {"_csrf": "bad"})
    assert ui.client.post("/ui/login", data={"token": ADMIN_TOKEN}, follow_redirects=False).status_code == 401


def test_form_content_type_and_size(ui):
    r = ui.client.post("/ui/controls/halt", json={"_csrf": ui.csrf}, follow_redirects=False)
    assert r.status_code == 415
    big = b"_csrf=" + ui.csrf.encode() + b"&pad=" + b"a" * 17000
    r = ui.client.post("/ui/controls/halt", content=big, headers={"Content-Type": "application/x-www-form-urlencoded"},
                       follow_redirects=False)
    assert r.status_code == 413
    r = ui.client.post("/ui/controls/halt", content=b"_csrf=\xff\xfe", headers={"Content-Type": "application/x-www-form-urlencoded"},
                       follow_redirects=False)
    assert r.status_code == 400
    assert not os.path.exists(ui.settings.halt_file)


# ---------------------------------------------------------------- 10-11. 제어
def test_halt_resume_roundtrip(ui):
    r = ui.post("/ui/controls/halt")
    assert r.status_code == 303 and r.headers["location"].startswith("/ui/controls")
    assert os.path.exists(ui.settings.halt_file)
    assert ui.client.get("/healthz").json()["halted"] is True
    assert ui.alerts.contains("HALT set via dashboard")
    page = ui.page(r.headers["location"])
    assert "HALT is set" in page and "HALT set" in page
    r = ui.post("/ui/controls/resume")
    assert r.status_code == 303
    assert not os.path.exists(ui.settings.halt_file)
    assert ui.alerts.contains("HALT cleared via dashboard")
    page = ui.page("/ui/controls")
    assert "HALT is set" not in page
    assert "testclient" in page and "HALT set" in page   # 최근 동작 20건 (이름/IP 만)


def test_reconcile_paper_consistent(ui):
    r = ui.post("/ui/controls/reconcile", {"mode": "test"})
    assert r.status_code == 303 and "consistent" in r.headers["location"]
    page = ui.page("/ui/controls")
    assert "Last reconcile" in page and "bybit" in page and 'class="st st-true"' in page
    assert ui.alerts.contains("reconcile test/* via dashboard")
    assert ui.post("/ui/controls/reconcile", {"mode": "bogus"}).status_code == 400
    assert ui.post("/ui/controls/reconcile", {"mode": "test", "account": "bogus"}).status_code == 400
    # 5초 간격 제한
    r = ui.post("/ui/controls/reconcile", {"mode": "test", "account": "bybit"})
    assert r.status_code == 429 and "TOO_SOON" in r.text


# ---------------------------------------------------------------- 12-16. 읽기 페이지
def test_overview_renders_accounts_and_flags(ui):
    page = ui.page("/ui")
    for needle in ("bybit", "LIVE_DISABLED", "key: missing", "routing", "fanout", "pending restart", "Signals (mode", 'http-equiv="refresh"'):
        assert needle in page, needle
    assert_no_secret(page)
    assert 'http-equiv="refresh"' not in ui.page("/ui?refresh=0")


def test_signals_list_and_detail_after_run(ui, executor, store):
    d = make_signal()
    row = run_signal(executor, store, d)
    assert row["status"] == "done"
    page = ui.page("/ui/signals")
    assert d["event_id"] in page and 'class="st st-done"' in page
    detail = ui.page(f"/ui/signals/{d['event_id']}?mode=test")
    for needle in ("done", "bybit", "entry", 'class="st st-filled"', "acknowledged", "filled", "<pre>", "&quot;position_id&quot;",
                   "Runs (per account)", "Lot (mode, account, position_id)"):
        assert needle in detail, needle
    assert detail.count("<pre>") == 1
    assert ui.get(f"/ui/signals/{d['event_id']}").status_code == 200   # mode 생략 가능
    assert ui.get("/ui/signals/bad%20id!").status_code == 404
    assert ui.get("/ui/signals/unknown-event-id").status_code == 404
    assert ui.get(f"/ui/signals/{d['event_id']}?mode=bogus").status_code == 400
    assert_no_secret(detail)


def test_signals_filters_and_paging(ui, store):
    ids = []
    for _ in range(3):
        d = make_signal()
        assert ingest(store, d) == "new"
        ids.append(d["event_id"])
    page = ui.page("/ui/signals?limit=2")
    assert sum(1 for i in ids if i in page) == 2 and "next" in page
    page2 = ui.page("/ui/signals?limit=2&offset=2")
    assert sum(1 for i in ids if i in page2) == 1 and "prev" in page2
    page = ui.page("/ui/signals?status=rejected")
    assert not any(i in page for i in ids)
    page = ui.page("/ui/signals?mode=test&status=accepted")
    assert all(i in page for i in ids)
    assert ui.get("/ui/signals?status=bogus").status_code == 400
    assert ui.get("/ui/signals?mode=bogus").status_code == 400
    assert ui.get("/ui/signals?limit=abc").status_code == 400
    assert ui.get("/ui/signals?limit=999&offset=-5").status_code == 200   # 범위 밖은 클램프


def test_orders_and_reports_pages(ui, executor, store):
    d = make_signal()
    run_signal(executor, store, d)
    store.insert_order("lk-raw-test-order", "test", "pos-raw", "entry", "Buy", 0.001, False, event_id=None, status="new",
                       raw='{"orderId":"XYZ-RAW-VALUE"}', account="bybit")
    page = ui.page("/ui/orders")
    assert "lk-raw-test-order" in page and d["event_id"] in page
    assert "orderId" not in page and "XYZ-RAW-VALUE" not in page
    assert ui.page("/ui/orders?mode=test&account=bybit&status=new").count("lk-raw-test-order") == 1
    assert ui.get("/ui/orders?account=bogus").status_code == 400
    assert ui.get("/ui/orders?status=bad%20status").status_code == 400
    page = ui.page("/ui/reports")
    reports = store.recent_reports("test", "bybit", 5)
    assert reports and reports[0]["report_id"] in page
    assert "schema_version" not in page and "lake_dedicated" not in page and "account_scope" not in page
    assert ui.page("/ui/reports?state=pending&mode=test").count(reports[0]["report_id"]) == 1
    assert ui.get("/ui/reports?state=bogus").status_code == 400
    assert_no_secret(page)


def test_duplicate_signal_is_persisted_in_ingress(ui, store):
    d = make_signal()
    raw, headers = sign_body(d, TEST_SIGNAL_SECRET)
    assert ui.client.post(SIGNAL_PATH, content=raw, headers=headers).status_code == 202
    assert ui.client.post(SIGNAL_PATH, content=raw, headers=headers).status_code == 200
    row = store.recent_ingress(10)[0]
    assert row["code"] == "DUPLICATE" and row["note"] == "http=200" and row["event_id"] == d["event_id"]
    n = len(store.recent_ingress(100))
    raw2, headers2 = sign_body(make_signal(), "wrong-secret-0123456789abcdef-0123456789")
    assert ui.client.post(SIGNAL_PATH, content=raw2, headers=headers2).status_code == 401
    assert len(store.recent_ingress(100)) == n          # 서명 전 거부는 행을 만들지 않는다
    page = ui.page("/ui/ingress")
    assert "DUPLICATE" in page and d["event_id"] in page and "BAD_SIGNATURE" in page and "counters only" in page
    detail = ui.page(f"/ui/signals/{d['event_id']}?mode=test")
    assert "DUPLICATE" in detail and "http=200" in detail


# ---------------------------------------------------------------- 17-22. 시크릿 쓰기
def test_secrets_page_masks_everything(ui):
    for path in ("/ui/secrets", "/ui/accounts"):
        page = ui.page(path)
        assert_no_secret(page)
        assert "value=\"" + ADMIN_TOKEN[:4] not in page
    secrets_page = ui.page("/ui/secrets")
    assert "set (" in secrets_page and "LAKE_SIGNAL_SECRET_TEST" in secrets_page and "ROTATE" in secrets_page
    accounts_page = ui.page("/ui/accounts")
    assert "BYBIT_API_KEY" in accounts_page and "missing" in accounts_page
    # 비밀 입력란은 절대 미리 채우지 않는다 (hidden csrf 외 value 없음)
    assert re.findall(r'<input type="password"[^>]*value=', secrets_page) == []
    assert re.findall(r'<input type="password"[^>]*value=', accounts_page) == []
    with rig(_multi(ui)) as m:
        page = m.page("/ui/accounts")
        assert "OKX_API_KEY" in page and "OKX_API_PASSPHRASE" in page and "TOOBIT_API_KEY" in page
        assert_no_secret(page)


def _multi(ui):
    """ui 의 settings_factory 가 없으므로 같은 tmp 트리 옆에 3계정 설정을 만든다."""
    from conftest import MULTI_ACCOUNTS, build_settings
    import pathlib
    root = pathlib.Path(os.path.dirname(ui.settings.state_dir)).parent / "multi"
    return build_settings(root, config_overrides={"routing": "fanout", "accounts": [dict(a) for a in MULTI_ACCOUNTS]})


def test_save_account_keys_writes_env_and_marks_pending(ui):
    old_text = open(ui.env, encoding="utf-8").read()
    r = ui.post("/ui/accounts/bybit/keys", {"api_key": "k-test-123456", "api_secret": "s-test-123456"})
    assert r.status_code == 303, r.text
    assert "BYBIT_API_KEY%2CBYBIT_API_SECRET" in r.headers["location"] and "restart" in r.headers["location"]
    env = parse_env_file(ui.env)
    assert env["BYBIT_API_KEY"] == "k-test-123456" and env["BYBIT_API_SECRET"] == "s-test-123456"
    assert env["ADMIN_TOKEN"] == ADMIN_TOKEN                      # 다른 키는 그대로
    assert not os.path.exists(ui.env + ".bak")                    # 재검증 통과 뒤 교체 전 사본은 지운다 (이전 시크릿 비보존)
    assert old_text != open(ui.env, encoding="utf-8").read()
    if os.name != "nt":
        assert stat.S_IMODE(os.stat(ui.env).st_mode) == 0o600
    overview = ui.page("/ui")
    assert "restart required" in overview and "BYBIT_API_KEY,BYBIT_API_SECRET" in overview
    accounts = ui.page(r.headers["location"])
    assert "saved BYBIT_API_KEY,BYBIT_API_SECRET" in accounts and "restart pending" in accounts
    assert ui.settings.accounts[0].api_key == ""                  # 프로세스 내 값은 바뀌지 않는다 (핫스왑 없음)
    for path in PAGES:
        assert_no_secret(ui.page(path), "k-test-123456", "s-test-123456")
    assert ui.alerts.contains("BYBIT_API_KEY,BYBIT_API_SECRET")
    assert not any("k-test-123456" in m or "s-test-123456" in m for m in ui.alerts.messages)
    # 두 번째 저장은 pending 목록에 합쳐진다
    assert ui.post("/ui/secrets", {"TELEGRAM_CHAT_ID": "4242"}).status_code == 303
    assert "BYBIT_API_KEY,BYBIT_API_SECRET,TELEGRAM_CHAT_ID" in ui.page("/ui")


def test_save_rejects_invalid_candidate_and_leaves_file_untouched(ui):
    before = open(ui.env, "rb").read()
    r = ui.post("/ui/secrets", {"LAKE_SIGNAL_SECRET_TEST": "short-1234"})
    assert r.status_code == 400 and "32 bytes" in r.text and "short-1234" not in r.text
    assert open(ui.env, "rb").read() == before and not os.path.exists(ui.env + ".bak")
    with rig(_multi(ui)) as m:
        before_m = open(m.env, "rb").read()
        r = m.post("/ui/accounts/okx/keys", {"api_key": "okx-key-123456", "api_secret": "okx-secret-123456"})
        assert r.status_code == 400 and "PASSPHRASE" in r.text and "okx-key-123456" not in r.text
        assert open(m.env, "rb").read() == before_m and not os.path.exists(m.env + ".bak")
        # passphrase 를 같이 주면 통과한다 (Paper 팩토리 드라이런)
        r = m.post("/ui/accounts/okx/keys", {"api_key": "okx-key-123456", "api_secret": "okx-secret-123456",
                                             "api_passphrase": "pp-123456"})
        assert r.status_code == 303
        assert parse_env_file(m.env)["OKX_API_PASSPHRASE"] == "pp-123456"
        # bybit 계정에 passphrase 필드는 없다
        assert m.post("/ui/accounts/bybit/keys", {"api_passphrase": "x"}).status_code == 400
    # 거래소 생성자 드라이런 실패 → 400, 파일 그대로
    def boom(acct):
        raise RuntimeError("constructor says secret-ish stuff")
    ui.app.state.ui_exchange_factory = boom
    r = ui.post("/ui/accounts/bybit/keys", {"api_key": "k-test-123456", "api_secret": "s-test-123456"})
    assert r.status_code == 400 and "RuntimeError" in r.text and "secret-ish" not in r.text
    assert open(ui.env, "rb").read() == before


def test_save_rejects_unknown_key(ui):
    r = ui.post("/ui/secrets", {"FOO": "bar"})
    assert r.status_code == 400 and r.json()["error"] == "BAD_KEY"
    r = ui.post("/ui/secrets", {"BYBIT_API_KEY": "k"})       # 계정 키는 Accounts 폼의 검증기를 공유 → 허용 목록에 있음
    assert r.status_code == 303
    r = ui.post("/ui/accounts/bybit/keys", {"unknown_field": "x"})
    assert r.status_code == 400 and r.json()["error"] == "BAD_KEY"
    assert ui.post("/ui/accounts/nope/keys", {"api_key": "x"}).status_code == 404


def test_empty_fields_keep_current(ui):
    before = open(ui.env, "rb").read()
    r = ui.post("/ui/secrets", {"LAKE_SIGNAL_SECRET_TEST": "", "TELEGRAM_CHAT_ID": ""})
    assert r.status_code == 303 and "nothing+changed" in r.headers["location"]
    assert "nothing changed" in ui.page(r.headers["location"])
    r = ui.post("/ui/accounts/bybit/keys", {"api_key": "", "api_secret": ""})
    assert r.status_code == 303 and "nothing+changed" in r.headers["location"]
    assert open(ui.env, "rb").read() == before and not os.path.exists(ui.env + ".bak")
    # 같은 값을 다시 저장해도 파일/백업을 만들지 않는다
    r = ui.post("/ui/secrets", {"LAKE_SIGNAL_SECRET_TEST": TEST_SIGNAL_SECRET})
    assert r.status_code == 303 and "nothing+changed" in r.headers["location"]
    assert open(ui.env, "rb").read() == before


def test_admin_token_rotation_requires_confirm(ui):
    new = "rotated-admin-token-0123456789abcdef-0123456789"
    r = ui.post("/ui/secrets", {"ADMIN_TOKEN": new})
    assert r.status_code == 400 and "ROTATE" in r.text and new not in r.text
    assert parse_env_file(ui.env)["ADMIN_TOKEN"] == ADMIN_TOKEN
    r = ui.post("/ui/secrets", {"ADMIN_TOKEN": new, "confirm": "ROTATE"})
    assert r.status_code == 303
    assert parse_env_file(ui.env)["ADMIN_TOKEN"] == new
    page = ui.page(r.headers["location"])
    assert "log in again" in page and new not in page and "ADMIN_TOKEN" in page
    assert ui.get("/ui").status_code == 200           # 재시작 전까지 기존 세션 유효 (문서화된 동작)
    assert ui.settings.secrets.admin_token == ADMIN_TOKEN


def test_live_toggle(ui, monkeypatch):
    assert ui.post("/ui/controls/live", {"enabled": "1"}).status_code == 400
    assert ui.post("/ui/controls/live", {"enabled": "1", "confirm": "nope"}).status_code == 400
    assert ui.post("/ui/controls/live", {"enabled": "2", "confirm": "LIVE"}).status_code == 400
    assert read_json(ui.config)["live"]["enabled"] is False
    r = ui.post("/ui/controls/live", {"enabled": "1", "confirm": "LIVE"})
    assert r.status_code == 303
    assert read_json(ui.config)["live"]["enabled"] is True
    assert ui.settings.live_enabled is False                     # 켜기는 재시작으로만 적용
    assert "restart required" in ui.page("/ui") and "live.enabled" in ui.page("/ui")
    ui.settings.live_enabled = True
    r = ui.post("/ui/controls/live", {"enabled": "0"})
    assert r.status_code == 303
    assert read_json(ui.config)["live"]["enabled"] is False
    assert ui.settings.live_enabled is False                     # 끄기는 즉시 적용
    assert ui.app.state.ui_pending is None                       # 켜기가 남긴 live.enabled 보류 표시는 지워진다 (디스크=프로세스)
    assert "restart required" not in ui.page("/ui")
    # 다른 보류 키는 남는다
    assert ui.post("/ui/secrets", {"TELEGRAM_CHAT_ID": "777"}).status_code == 303
    assert ui.post("/ui/controls/live", {"enabled": "1", "confirm": "LIVE"}).status_code == 303
    assert ui.app.state.ui_pending["keys"] == ["TELEGRAM_CHAT_ID", "live.enabled"]
    assert ui.post("/ui/controls/live", {"enabled": "0"}).status_code == 303
    assert ui.app.state.ui_pending["keys"] == ["TELEGRAM_CHAT_ID"]
    before = open(ui.config, "rb").read()

    def bad_load(*a, **k):
        raise config.ConfigError("candidate rejected")
    monkeypatch.setattr(config, "load", bad_load)
    r = ui.post("/ui/controls/live", {"enabled": "1", "confirm": "LIVE"})
    assert r.status_code == 500 and "candidate rejected" in r.text
    assert open(ui.config, "rb").read() == before
    assert not [n for n in os.listdir(os.path.dirname(ui.config)) if ".tmp-" in n]


# ---------------------------------------------------------------- 24. 재시작 가드
def test_restart_guard(ui, store):
    guard = ui.app.state.ui_restart_guard
    now = now_ms()
    assert guard.allowed(now) == (False, "PROCESS_TOO_YOUNG")
    assert ui.post("/ui/controls/restart").status_code == 429
    guard.started_ms = now - 120_000
    calls: list[tuple[int, int]] = []

    class FakeTimer:
        def __init__(self, delay, fn):
            assert delay == guard.DELAY_S
            self.fn = fn

        def start(self):
            self.fn()

    guard.kill = lambda pid, sig: calls.append((pid, sig))
    guard.timer = FakeTimer
    r = ui.post("/ui/controls/restart")
    assert r.status_code == 303 and "restarting" in r.headers["location"]
    assert calls == [(os.getpid(), signal.SIGTERM)]
    assert ui.alerts.contains("restart requested via dashboard")
    r = ui.post("/ui/controls/restart")
    assert r.status_code == 429 and "TOO_SOON" in r.text and calls == [(os.getpid(), signal.SIGTERM)]
    hist = json.loads(store.get_meta("ui_restarts"))
    assert len(hist) == 1 and now - 5000 <= hist[0] <= now_ms()
    t = now_ms()
    store.set_meta("ui_restarts", json.dumps([t - 200_000, t - 130_000, t - 65_000]))
    assert guard.allowed(t) == (False, "RATE_LIMITED")
    store.set_meta("ui_restarts", json.dumps([t - 400_000, t - 200_000, t - 65_000]))   # 창 밖 항목은 버린다
    assert guard.allowed(t) == (True, "")
    assert "restart guard" in ui.page("/ui/controls")


# ---------------------------------------------------------------- 25. 계정 연결 확인
def test_account_check_uses_disk_keys_and_rate_limits(ui):
    seen: list[str] = []

    def factory(acct):
        seen.append(acct.api_key)
        return PaperExchange(acct, price=PAPER_PRICE)
    ui.app.state.ui_exchange_factory = factory
    r = ui.post("/ui/accounts/bybit/check")
    assert r.status_code == 303 and "skipped" in ui.page("/ui/accounts") and seen == []
    ui.app.state.ui_check_at.clear()
    write_env_file(ui.env, {"BYBIT_API_KEY": "disk-key-123456", "BYBIT_API_SECRET": "disk-secret-123456"})
    r = ui.post("/ui/accounts/bybit/check")
    assert r.status_code == 303
    page = ui.page("/ui/accounts")
    assert "instrument: ok" in page and "last/mark: ok" in page and "positions: ok" in page
    assert seen == ["disk-key-123456"] and ui.settings.accounts[0].api_key == ""
    assert "disk-key-123456" not in page and "disk-secret-123456" not in page
    r = ui.post("/ui/accounts/bybit/check")
    assert r.status_code == 429 and seen == ["disk-key-123456"]
    ui.app.state.ui_check_at.clear()

    def failing(acct):
        raise ExchangeError("AUTH", "message with secret-ish text")
    ui.app.state.ui_exchange_factory = failing
    assert ui.post("/ui/accounts/bybit/check").status_code == 303
    page = ui.page("/ui/accounts")
    assert "FAILED (AUTH)" in page and "secret-ish" not in page
    assert ui.post("/ui/accounts/nope/check").status_code == 404
    assert "account check bybit" in ui.page("/ui/controls")


# ---------------------------------------------------------------- 26-27. 로그아웃 / paths 없음
def test_logout_revokes(ui):
    old = ui.client.cookies.get("lake_ui")
    r = ui.post("/ui/logout")
    assert r.status_code == 303 and r.headers["location"] == "/ui/login"
    assert "lake_ui=;" in r.headers["set-cookie"] and "Max-Age=0" in r.headers["set-cookie"]
    fresh = TestClient(ui.app)
    r = fresh.get("/ui", headers={"Cookie": f"lake_ui={old}"}, follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"] == "/ui/login"
    assert ui.get("/ui").status_code == 303


def test_env_path_unknown_disables_writes(settings_factory):
    with rig(settings_factory(), paths=False) as u:
        for path in PAGES:
            assert u.get(path).status_code == 200, path
        assert "editing disabled" in u.page("/ui/accounts") and "editing disabled" in u.page("/ui/secrets")
        r = u.post("/ui/secrets", {"TELEGRAM_CHAT_ID": "1"})
        assert r.status_code == 409 and r.json()["error"] == "ENV_PATH_UNKNOWN"
        r = u.post("/ui/accounts/bybit/keys", {"api_key": "k", "api_secret": "s"})
        assert r.status_code == 409 and r.json()["error"] == "ENV_PATH_UNKNOWN"
        r = u.post("/ui/controls/live", {"enabled": "0"})
        assert r.status_code == 409
        assert u.post("/ui/controls/halt").status_code == 303       # 쓰기 아닌 제어는 그대로 동작
        assert os.path.exists(u.settings.halt_file)


# ---------------------------------------------------------------- 28. 시크릿 유출 전수 검사
def test_no_secret_leak_sweep(ui, executor, store):
    d = make_signal()
    run_signal(executor, store, d)
    raw, headers = sign_body(d, TEST_SIGNAL_SECRET)
    ui.client.post(SIGNAL_PATH, content=raw, headers=headers)
    assert ui.post("/ui/accounts/bybit/keys", {"api_key": "k-sweep-123456", "api_secret": "s-sweep-123456"}).status_code == 303
    assert ui.post("/ui/secrets", {"TELEGRAM_BOT_TOKEN": "123456:telegram-sweep-token"}).status_code == 303
    extra = ("k-sweep-123456", "s-sweep-123456", "123456:telegram-sweep-token")
    texts = [ui.page(p) for p in (*PAGES, f"/ui/signals/{d['event_id']}?mode=test", "/ui/signals?mode=test",
                                 "/ui/orders?mode=test", "/ui/reports?mode=test", "/ui?refresh=0")]
    errors = [
        ui.get("/ui/signals?status=bogus"), ui.get("/ui/signals/unknown-x"), ui.get("/ui/orders?account=bogus"),
        ui.post("/ui/secrets", {"FOO": "bar"}), ui.post("/ui/secrets", {"LAKE_SIGNAL_SECRET_LIVE": "short"}),
        ui.post("/ui/secrets", {"ADMIN_TOKEN": "x" * 40}), ui.post("/ui/controls/live", {"enabled": "1"}),
        ui.post("/ui/controls/reconcile", {"mode": "bogus"}), ui.post("/ui/controls/restart"),
        ui.post("/ui/controls/halt", {"_csrf": "bad"}), ui.post("/ui/accounts/nope/check"),
    ]
    assert all(r.status_code >= 400 for r in errors)
    texts += [r.text for r in errors]
    texts += [ui.client.get("/ui/login").text, ui.client.post("/ui/login", data={"token": "nope"}, follow_redirects=False).text]
    for t in texts:
        assert_no_secret(t, *extra)
    assert not any(any(s in m for s in (*ALL_SECRETS, *extra)) for m in ui.alerts.messages)


# ---------------------------------------------------------------- 29. /ui 아래 미등록 경로·메서드 (404 정책 + 보안 헤더)
FALLBACK_CASES = (("GET", "/ui/controls/halt"), ("GET", "/ui/logout"), ("POST", "/ui"), ("GET", "/ui/accounts/x/keys"),
                  ("GET", "/ui/nope"), ("PUT", "/ui/secrets"), ("DELETE", "/ui/signals/abc"), ("HEAD", "/ui"),
                  ("OPTIONS", "/ui/login"), ("PATCH", "/ui/nope/deeper"))


def _assert_sec_headers(r) -> None:
    assert r.headers["cache-control"] == "no-store"
    assert "default-src 'none'" in r.headers["content-security-policy"]
    assert r.headers["x-frame-options"] == "DENY"


def test_ui_fallback_404_policy_without_admin_token(settings_factory):
    s = settings_factory(drop_env=("ADMIN_TOKEN",))
    store = Store(s.db_path)
    try:
        with TestClient(create_app(s, store, SimpleNamespace())) as c:
            for method, path in FALLBACK_CASES:
                r = c.request(method, path, follow_redirects=False)
                assert r.status_code == 404, (method, path, r.status_code)
                if method != "HEAD":
                    assert r.json() == {"error": "NOT_FOUND"}, (method, path)
                    assert "Method Not Allowed" not in r.text and "detail" not in r.text
                _assert_sec_headers(r)
    finally:
        store.close()


def test_ui_fallback_with_admin_token(ui):
    fresh = TestClient(ui.app)
    # 로그인 전: GET 은 로그인으로, POST 는 401 (등록된 경로와 같은 게이트)
    r = fresh.get("/ui/nope", follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"] == "/ui/login"
    r = fresh.post("/ui/nope", data={"_csrf": "x"}, follow_redirects=False)
    assert r.status_code == 401 and r.text == "UNAUTHORIZED"
    _assert_sec_headers(r)
    # 로그인 후: 없는 경로 404, 있는 경로의 다른 메서드 405 — 둘 다 보안 헤더 포함, Starlette 기본 본문 아님
    r = ui.get("/ui/nope")
    assert r.status_code == 404 and "NOT_FOUND" in r.text and "detail" not in r.text
    _assert_sec_headers(r)
    r = ui.get("/ui/controls/halt")
    assert r.status_code == 405 and "METHOD_NOT_ALLOWED" in r.text and "Method Not Allowed" not in r.text
    _assert_sec_headers(r)
    assert not os.path.exists(ui.settings.halt_file)
    r = ui.client.request("PUT", "/ui/secrets", follow_redirects=False)
    assert r.status_code == 405
    _assert_sec_headers(r)
    assert_no_secret(r.text)
    # 등록된 라우트는 그대로
    assert ui.get("/ui/controls").status_code == 200


# ---------------------------------------------------------------- 30. 플래시 배너는 세션이 서명한 것만
def test_flash_banner_requires_session_signature(ui):
    forged = urllib.parse.urlencode({"flash": "forged banner HALT cleared by ops"})
    page = ui.page(f"/ui/controls?{forged}")
    assert "forged banner" not in page
    assert "forged banner" not in ui.page(f"/ui?{forged}")
    # 진짜 플래시는 보인다
    r = ui.post("/ui/controls/halt")
    assert r.status_code == 303
    loc = r.headers["location"]
    q = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(loc).query))
    assert q["flash"] == "HALT set" and q["fat"].isdigit() and len(q["fsig"]) == 32
    assert "HALT set" in ui.page(loc)
    ui.post("/ui/controls/resume")

    def with_q(**over):
        d = dict(q, **over)
        return "/ui/controls?" + urllib.parse.urlencode(d)
    # 문구 변조 / 서명 변조 / 시각 변조 / 서명 없음 → 배너 없음
    assert "tampered" not in ui.page(with_q(flash="tampered text"))
    assert "HALT set" not in ui.page(with_q(fsig="0" * 32)).split("<main>")[0]
    assert "HALT set" not in ui.page(with_q(fat=str(int(q["fat"]) + 1))).split("<main>")[0]
    assert "HALT set" not in ui.page("/ui/controls?" + urllib.parse.urlencode({"flash": q["flash"]})).split("<main>")[0]
    # 60초가 지나면 무시 (서명은 유효해도)
    codec = ui.app.state.ui_session_codec
    sid = ui.client.cookies.get("lake_ui").split(".")[0]
    old = now_ms() - 61_000
    stale = "/ui/controls?" + urllib.parse.urlencode({"flash": "stale banner", "fat": old,
                                                     "fsig": codec.flash_sig(sid, old, "stale banner")})
    assert "stale banner" not in ui.page(stale)
    # 다른 세션의 서명은 맞지 않는다
    other = TestClient(ui.app)
    other.post("/ui/login", data={"token": ADMIN_TOKEN}, follow_redirects=False)
    r = other.get(loc, follow_redirects=False)
    assert r.status_code == 200 and "HALT set" not in r.text.split("<main>")[0]


# ---------------------------------------------------------------- 31. Secrets 페이지: 계정별 회신 행의 거짓 'restart pending' 없음
def test_secrets_page_no_false_pending_for_account_rows(ui):
    page = ui.page("/ui/secrets")
    assert "LAKE_REPORT_URL_TEST_BYBIT" in page and "LAKE_REPORT_SECRET_LIVE_BYBIT" in page
    assert "restart pending" not in page                          # 디스크(기본값 상속) 와 프로세스(유효값) 가 같다
    # 모드 기본값을 바꾸면 그 값을 물려받는 계정 행도 pending 으로 바뀐다 (유효값이 달라졌으므로)
    r = ui.post("/ui/secrets", {"LAKE_REPORT_URL_LIVE": "https://lake.live.example/report"})
    assert r.status_code == 303
    page = ui.page("/ui/secrets")
    rows = {m.group(1): m.group(0) for m in re.finditer(r"<tr><td>(LAKE_[A-Z_]+)</td>.*?</tr>", page)}
    assert "restart pending" in rows["LAKE_REPORT_URL_LIVE"] and "restart pending" in rows["LAKE_REPORT_URL_LIVE_BYBIT"]
    assert "restart pending" not in rows["LAKE_REPORT_URL_TEST_BYBIT"]
    assert "restart pending" not in rows["LAKE_REPORT_SECRET_TEST_BYBIT"]
    assert "lake.live.example" in rows["LAKE_REPORT_URL_LIVE"] and "https://lake.live.example/report" not in page


# ---------------------------------------------------------------- 32. 회신 URL 은 절대 URL 만
def test_report_url_must_be_absolute(ui):
    before = open(ui.env, "rb").read()
    for bad in ("not-a-url", "/relative", "htps://lake.example/report", "https://", "ftp://lake.example/x", "lake.example/report"):
        r = ui.post("/ui/secrets", {"LAKE_REPORT_URL_TEST": bad})
        assert r.status_code == 400 and "absolute http(s) URL" in r.text and "LAKE_REPORT_URL_TEST" in r.text, bad
        assert open(ui.env, "rb").read() == before, bad
    assert not os.path.exists(ui.env + ".bak")
    r = ui.post("/ui/secrets", {"LAKE_REPORT_URL_LIVE_BYBIT": "https://lake.example/acct", "LAKE_REPORT_SECRET_LIVE": "s" * 32})
    assert r.status_code == 303
    assert parse_env_file(ui.env)["LAKE_REPORT_URL_LIVE_BYBIT"] == "https://lake.example/acct"
    # min_bytes 는 화이트리스트에서 바로 막는다 (값은 메시지에 없다)
    r = ui.post("/ui/secrets", {"LAKE_REPORT_SECRET_LIVE_BYBIT": "short-secret"})
    assert r.status_code == 400 and "at least 32 bytes" in r.text and "short-secret" not in r.text


# ---------------------------------------------------------------- 33. offset 상한
def test_paging_offset_overflow_is_clamped(ui):
    for path in ("/ui/signals", "/ui/orders", "/ui/reports", "/ui/ingress"):
        r = ui.get(f"{path}?offset=9999999999999999999999")
        assert r.status_code == 200, (path, r.status_code)
        assert "offset 1000000000" in r.text
    assert ui.get("/ui/signals?offset=abc").status_code == 400


# ---------------------------------------------------------------- 34. 겹친 저장은 직렬화된다 (임시 파일 충돌·갱신 유실 없음)
def test_concurrent_saves_are_serialized(ui, monkeypatch):
    real_render = util.render_env_update

    def slow_render(text, updates):
        time.sleep(0.3)
        return real_render(text, updates)
    monkeypatch.setattr(util, "render_env_update", slow_render)
    results: dict[str, int] = {}

    def save_keys():
        results["keys"] = ui.post("/ui/accounts/bybit/keys", {"api_key": "k-par-123456", "api_secret": "s-par-123456"}).status_code

    def save_chat():
        results["chat"] = ui.post("/ui/secrets", {"TELEGRAM_CHAT_ID": "31337"}).status_code
    threads = [threading.Thread(target=save_keys), threading.Thread(target=save_chat)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    assert results == {"keys": 303, "chat": 303}
    env = parse_env_file(ui.env)
    assert env["BYBIT_API_KEY"] == "k-par-123456" and env["BYBIT_API_SECRET"] == "s-par-123456" and env["TELEGRAM_CHAT_ID"] == "31337"
    assert env["ADMIN_TOKEN"] == ADMIN_TOKEN
    assert not [n for n in os.listdir(os.path.dirname(ui.env)) if ".tmp-" in n]
    assert sorted(ui.app.state.ui_pending["keys"]) == ["BYBIT_API_KEY", "BYBIT_API_SECRET", "TELEGRAM_CHAT_ID"]
