"""receiver.py — FastAPI TestClient + 임시 store (ARCHITECTURE.md §2, §9).

202 / 200 dup / 409 conflict / 409 seq / 410 expired / 400 schema / 401 / 413 / 415 / 503(키 없음) /
position_idx 모드 불일치 400 / 응답에 시크릿 노출 없음 / healthz / admin 토큰.
"""
from __future__ import annotations

import json
import os
from types import SimpleNamespace

from fastapi.testclient import TestClient

from lake_executor.receiver import create_app
from lake_executor.store import Store
from lake_executor.util import now_ms

from conftest import (
    ADMIN_TOKEN,
    ALL_SECRETS,
    LIVE_SIGNAL_SECRET,
    SIGNAL_PATH,
    TEST_SIGNAL_SECRET,
    execution_statuses,
    load_reports,
    make_signal,
    sign_body,
)


def _post(client, d: dict, secret: str = TEST_SIGNAL_SECRET, extra_headers: dict | None = None):
    raw, headers = sign_body(d, secret)
    headers.update(extra_headers or {})
    return client.post(SIGNAL_PATH, content=raw, headers=headers)


def _assert_no_secret(resp):
    text = resp.text
    for s in ALL_SECRETS:
        assert s not in text


# ---------------------------------------------------------------- 접수 결과 코드
def test_new_signal_is_202_and_persisted(client, store):
    d = make_signal()
    r = _post(client, d)
    assert r.status_code == 202, r.text
    assert r.json() == {"accepted": True, "event_id": d["event_id"]}
    row = store.get_signal(d["event_id"])
    assert row is not None
    assert row["status"] == "accepted"
    assert row["mode"] == "test"
    assert row["event_sequence"] == 1
    _assert_no_secret(r)


def test_exact_duplicate_is_200_and_not_executed_twice(client, store, executor):
    d = make_signal()
    raw, headers = sign_body(d, TEST_SIGNAL_SECRET)
    r1 = client.post(SIGNAL_PATH, content=raw, headers=headers)
    r2 = client.post(SIGNAL_PATH, content=raw, headers=headers)
    assert r1.status_code == 202
    assert r2.status_code == 200
    assert r2.json() == {"accepted": True, "duplicate": True}
    # 접수 레코드는 하나, 실행기는 한 번만 처리한다
    assert executor.run_once() is True
    assert executor.run_once() is False
    assert store.get_signal(d["event_id"])["status"] == "done"
    acked = [r for r in load_reports(store, "test")
             if r["kind"] == "execution" and r["execution"]["status"] == "acknowledged"]
    assert len(acked) == 1
    assert acked[0]["execution"]["event_id"] == d["event_id"]


def test_same_event_id_different_body_is_409_conflict(client, store):
    d = make_signal()
    assert _post(client, d).status_code == 202
    d2 = dict(d, qty_btc=0.003, expected_qty_btc_after=0.003)
    r = _post(client, d2)
    assert r.status_code == 409
    body = r.json()
    assert body["error"] == "CONFLICT"
    assert body["code"]
    # 원래 접수가 덮어써지지 않는다
    assert store.get_signal(d["event_id"])["qty_btc"] == 0.002


def test_sequence_reversal_is_409(client, store):
    pid = "position-seq-test"
    d2 = make_signal(position_id=pid, event_sequence=2)
    d1 = make_signal(position_id=pid, event_sequence=1)
    assert _post(client, d2).status_code == 202
    r = _post(client, d1)
    assert r.status_code == 409
    assert r.json()["error"] == "CONFLICT"
    assert store.get_signal(d1["event_id"]) is None
    # 같은 번호 재사용도 거부, 더 큰 번호는 허용
    assert _post(client, make_signal(position_id=pid, event_sequence=2)).status_code == 409
    assert _post(client, make_signal(position_id=pid, event_sequence=3, action="add",
                                     expected_qty_btc_after=0.004)).status_code == 202


def test_expired_signal_is_410(client, store):
    ts = now_ms() - 5000
    d = make_signal(ts=ts, expires_at_ms=ts + 1000)
    r = _post(client, d)
    assert r.status_code == 410
    assert r.json()["error"] == "EXPIRED"
    assert store.get_signal(d["event_id"]) is None


def test_wrong_secret_is_401(client, store):
    d = make_signal()
    r = _post(client, d, secret="not-the-right-secret-0123456789abcdef-0123")
    assert r.status_code == 401
    assert r.json()["error"] == "UNAUTHORIZED"
    assert store.get_signal(d["event_id"]) is None
    _assert_no_secret(r)


def test_live_secret_does_not_sign_test_signal(client):
    r = _post(client, make_signal(mode="test"), secret=LIVE_SIGNAL_SECRET)
    assert r.status_code == 401


def test_timestamp_header_mismatch_and_skew_are_401(client):
    d = make_signal()
    raw, headers = sign_body(d, TEST_SIGNAL_SECRET)
    headers["X-Timestamp"] = str(d["ts"] + 1)
    assert client.post(SIGNAL_PATH, content=raw, headers=headers).status_code == 401
    old = now_ms() - 120000
    d_old = make_signal(ts=old, expires_at_ms=old + 15000)
    assert _post(client, d_old).status_code == 401
    raw, headers = sign_body(d, TEST_SIGNAL_SECRET)
    del headers["X-Signature"]
    assert client.post(SIGNAL_PATH, content=raw, headers=headers).status_code == 401


def test_unknown_field_is_400(client, store):
    d = make_signal(extra_field="nope")
    r = _post(client, d)
    assert r.status_code == 400
    body = r.json()
    assert body["error"] == "INVALID_SIGNAL"
    assert "nope" not in r.text  # 입력 값은 응답에 노출하지 않는다
    assert store.get_signal(d["event_id"]) is None


def test_schema_violations_are_400(client):
    assert _post(client, make_signal(action="protection_update")).status_code == 400  # qty_btc must be null
    assert _post(client, make_signal(qty_btc=-1)).status_code == 400
    assert _post(client, make_signal(leg="long", position_idx=2)).status_code == 400
    assert _post(client, make_signal(symbol="ETHUSDT")).status_code == 400
    assert _post(client, make_signal(mode="paper")).status_code == 400


def test_position_idx_mode_mismatch_is_400(client, store):
    # hedge 모드에서는 position_idx 0 (단방향) 을 받지 않는다
    d = make_signal(position_idx=0, leg="short")
    r = _post(client, d)
    assert r.status_code == 400
    assert r.json()["code"] == "POSITION_MODE_MISMATCH"
    assert store.get_signal(d["event_id"]) is None


def test_bad_json_is_400(client):
    headers = sign_body({"ts": now_ms()}, TEST_SIGNAL_SECRET)[1]
    r = client.post(SIGNAL_PATH, content=b"{not json", headers=headers)
    assert r.status_code == 400
    r = client.post(SIGNAL_PATH, content=b"[1,2]", headers=headers)
    assert r.status_code == 400


def test_body_over_limit_is_413(client, store, settings):
    limit = settings.max_body_bytes
    assert limit == 65536
    payload = b'{"pad":"' + b"x" * (limit + 1 - len(b'{"pad":""}')) + b'"}'
    assert len(payload) == limit + 1 == 65537
    headers = {"Content-Type": "application/json", "X-Timestamp": str(now_ms()), "X-Signature": "0" * 64}
    r = client.post(SIGNAL_PATH, content=payload, headers=headers)
    assert r.status_code == 413
    assert r.json()["error"] == "PAYLOAD_TOO_LARGE"


def test_body_at_limit_is_not_413(client):
    """정확히 65536 바이트는 크기 때문에 거부되지 않는다 (서명 없음 → 401 로 떨어진다)."""
    payload = b'{"pad":"' + b"x" * (65536 - len(b'{"pad":""}')) + b'"}'
    assert len(payload) == 65536
    r = client.post(SIGNAL_PATH, content=payload, headers={"Content-Type": "application/json"})
    assert r.status_code != 413


def test_content_encoding_gzip_is_415(client, store):
    d = make_signal()
    r = _post(client, d, extra_headers={"Content-Encoding": "gzip"})
    assert r.status_code == 415
    assert r.json()["error"] == "UNSUPPORTED_MEDIA_TYPE"
    assert store.get_signal(d["event_id"]) is None


def test_non_json_content_type_is_415(client):
    d = make_signal()
    r = _post(client, d, extra_headers={"Content-Type": "text/plain"})
    assert r.status_code == 415


def test_live_signal_without_live_secret_is_503(settings_factory):
    s = settings_factory(drop_env=("LAKE_SIGNAL_SECRET_LIVE",))
    store = Store(s.db_path)
    try:
        app = create_app(s, store, SimpleNamespace())
        with TestClient(app) as c:
            d = make_signal(mode="live")
            r = _post(c, d, secret=LIVE_SIGNAL_SECRET)
            assert r.status_code == 503
            assert r.json()["error"] == "SECRET_NOT_CONFIGURED"
            _assert_no_secret(r)
            assert store.get_signal(d["event_id"]) is None
            # TEST 모드는 그대로 접수된다
            assert _post(c, make_signal(mode="test")).status_code == 202
    finally:
        store.close()


def test_live_signal_with_live_secret_is_accepted_even_when_live_disabled(client, store):
    """live 실행이 막혀 있어도 접수(202)는 된다 — 실행 결과는 회신(LIVE_DISABLED)으로만 전달."""
    d = make_signal(mode="live")
    r = _post(client, d, secret=LIVE_SIGNAL_SECRET)
    assert r.status_code == 202
    assert store.get_signal(d["event_id"])["mode"] == "live"


def test_rejections_are_logged_without_raw_body(client, store):
    d = make_signal()
    _post(client, d, secret="wrong-secret-0123456789abcdef-0123456789")
    rows = store._q("SELECT * FROM ingress_log ORDER BY id DESC LIMIT 1")
    assert rows and rows[0]["code"] == "BAD_SIGNATURE"
    assert rows[0]["body_sha256"] and len(rows[0]["body_sha256"]) == 64
    assert "raw_body" not in rows[0]


def test_responses_never_expose_secrets(client):
    for resp in (
        _post(client, make_signal(), secret="wrong-secret-0123456789abcdef-0123456789"),
        _post(client, make_signal(extra="x")),
        _post(client, make_signal(), extra_headers={"Content-Encoding": "gzip"}),
        client.get("/healthz"),
        client.get("/state"),
        client.get("/state", headers={"X-Admin-Token": "nope"}),
    ):
        _assert_no_secret(resp)


# ---------------------------------------------------------------- healthz
def test_healthz(client, settings, store):
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.json() == {"ok": True, "halted": False, "inconsistent": {"test": False, "live": False}}
    store.set_inconsistent("live", True, "x")
    os.makedirs(os.path.dirname(settings.halt_file), exist_ok=True)
    with open(settings.halt_file, "w") as f:
        f.write("halt\n")
    r = client.get("/healthz")
    assert r.json() == {"ok": True, "halted": True, "inconsistent": {"test": False, "live": True}}


# ---------------------------------------------------------------- admin
def test_admin_endpoints_are_404_without_admin_token_configured(settings_factory):
    s = settings_factory(drop_env=("ADMIN_TOKEN",))
    store = Store(s.db_path)
    try:
        with TestClient(create_app(s, store, SimpleNamespace())) as c:
            h = {"X-Admin-Token": ADMIN_TOKEN}
            assert c.get("/state", headers=h).status_code == 404
            assert c.post("/admin/halt", headers=h).status_code == 404
            assert c.post("/admin/resume", headers=h).status_code == 404
            assert c.post("/admin/reconcile?mode=test", headers=h).status_code == 404
            assert not os.path.exists(s.halt_file)
    finally:
        store.close()


def test_admin_endpoints_require_correct_token(client, settings):
    for h in ({}, {"X-Admin-Token": "wrong"}, {"X-Admin-Token": ADMIN_TOKEN + "x"}):
        assert client.get("/state", headers=h).status_code == 401
        assert client.post("/admin/halt", headers=h).status_code == 401
        assert client.post("/admin/resume", headers=h).status_code == 401
        assert client.post("/admin/reconcile?mode=test", headers=h).status_code == 401
    assert not os.path.exists(settings.halt_file)


def test_admin_halt_resume_state_reconcile(client, settings, store, alerts):
    h = {"X-Admin-Token": ADMIN_TOKEN}
    r = client.post("/admin/halt", headers=h)
    assert r.status_code == 200 and r.json()["halted"] is True
    assert os.path.exists(settings.halt_file)
    assert client.get("/healthz").json()["halted"] is True

    r = client.get("/state", headers=h)
    assert r.status_code == 200
    st = r.json()
    assert st["halted"] is True
    assert st["live_execution_possible"] is False
    assert "signals" in st and "reports" in st and "open_lots" in st
    assert all(s not in r.text for s in ALL_SECRETS)

    r = client.post("/admin/resume", headers=h)
    assert r.status_code == 200 and r.json()["halted"] is False
    assert not os.path.exists(settings.halt_file)

    r = client.post("/admin/reconcile?mode=test", headers=h)
    assert r.status_code == 200
    assert r.json()["consistent"] is True
    assert client.post("/admin/reconcile?mode=bogus", headers=h).status_code == 400


def test_receiver_only_persists_and_does_not_execute(client, store, executor, paper):
    """수신 경로는 DB 에만 쓴다: 실행은 run_once 가 돌 때까지 일어나지 않는다."""
    d = make_signal()
    assert _post(client, d).status_code == 202
    assert store.get_signal(d["event_id"])["status"] == "accepted"
    assert paper.positions() == {}
    assert load_reports(store, "test") == []
    assert executor.run_once() is True
    assert paper.positions()[2]["size"] == 0.002
    assert execution_statuses(load_reports(store, "test")) == ["acknowledged", "submitted", "filled", "snapshot"]
