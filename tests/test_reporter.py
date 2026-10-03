"""reporter.py (ARCHITECTURE.md §5, §9).

본문이 계약 스키마와 일치(jsonschema 없이 필드/타입 직접 검사), sequence 연속·재시작 유지, observed 단조,
체결 외 null 규칙, 전송 상태 전이(가짜 client: 202/200/409/500→재시도/타임아웃/창 초과).
"""
from __future__ import annotations

import json
import re
import threading
import time

import httpx
import pytest

from lake_executor import auth, reporter as reporter_mod, store as st
from lake_executor.reporter import Reporter, ReportError
from lake_executor.store import Store
from lake_executor.util import now_ms

from conftest import REPORT_URL_TEST, TEST_REPORT_SECRET, FakeClient

ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
ORDER_ID_RE = re.compile(r"^o-[0-9a-f]{24}$")
FILL_ID_RE = re.compile(r"^f-[0-9a-f]{24}$")
HEADER_KEYS = {"schema_version", "report_id", "mode", "sequence", "ts", "observed_at_ms", "exchange", "category",
               "symbol", "kind"}
EXECUTION_KEYS = {"position_id", "strategy", "leg", "position_idx", "event_id", "action", "status", "qty",
                  "fill_price", "order_id", "fill_id", "reason_code"}
POSITION_KEYS = {"position_id", "strategy", "leg", "position_idx", "qty", "entry_price", "mark_price", "stop_loss",
                 "take_profit", "updated_at_ms"}

IDENT = dict(event_id="event-demo-partial-1", position_id="position-demo-1", strategy="overheat", leg="short",
             position_idx=2)


def _body(alloc: dict) -> dict:
    return json.loads(alloc["body"].decode("utf-8"))


def _row(store: Store, report_id: str) -> dict:
    rows = store._q("SELECT * FROM reports WHERE report_id=?", (report_id,))
    assert rows
    return rows[0]


def _ack(reporter: Reporter, mode: str = "test", **over) -> dict:
    kw = dict(IDENT, action="partial_exit", status="acknowledged")
    kw.update(over)
    return reporter.execution(mode, **kw)


def _header_ok(b: dict, mode: str, kind: str) -> None:
    assert HEADER_KEYS <= set(b)
    assert b["schema_version"] == 1
    assert ID_RE.match(b["report_id"])
    assert b["mode"] == mode
    assert isinstance(b["sequence"], int) and b["sequence"] >= 1
    assert isinstance(b["ts"], int) and isinstance(b["observed_at_ms"], int)
    assert 1 <= b["observed_at_ms"] <= b["ts"]
    assert b["exchange"] == "Bybit" and b["category"] == "linear" and b["symbol"] == "BTCUSDT"
    assert b["kind"] == kind


# --------------------------------------------------------------------------- #
# 본문: execution
# --------------------------------------------------------------------------- #
def test_execution_non_fill_body_matches_contract(reporter, store):
    before = now_ms()
    alloc = reporter.execution("test", **IDENT, action="partial_exit", status="acknowledged")
    b = _body(alloc)
    _header_ok(b, "test", "execution")
    assert set(b) == HEADER_KEYS | {"execution"}
    e = b["execution"]
    assert set(e) == EXECUTION_KEYS
    assert e["event_id"] == IDENT["event_id"] and e["position_id"] == IDENT["position_id"]
    assert e["strategy"] == "overheat" and e["leg"] == "short" and e["position_idx"] == 2
    assert e["action"] == "partial_exit" and e["status"] == "acknowledged"
    assert e["qty"] is None and e["fill_price"] is None and e["fill_id"] is None
    assert e["order_id"] is None and e["reason_code"] is None
    assert b["sequence"] == 1 and before <= b["ts"] <= now_ms()
    # allocate 결과와 저장 행이 일치, 저장 바이트 = 반환 바이트
    assert alloc["sequence"] == 1 and alloc["mode"] == "test" and alloc["kind"] == "execution"
    assert alloc["payload"] == b
    row = _row(store, alloc["report_id"])
    assert row["state"] == st.REPORT_PENDING and row["attempts"] == 0 and row["sequence"] == 1
    assert bytes(row["body"]) == alloc["body"]
    # 직렬화는 compact JSON (재직렬화 없이 서명 가능)
    assert alloc["body"] == json.dumps(b, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def test_execution_fill_body_matches_contract_example(reporter):
    alloc = reporter.execution("test", **IDENT, action="partial_exit", status="partially_filled",
                               qty=0.0005, fill_price=85500, order_id="order-example-only",
                               fill_id="fill-example-only", observed_at_ms=now_ms() - 10)
    b = _body(alloc)
    _header_ok(b, "test", "execution")
    e = b["execution"]
    assert set(e) == EXECUTION_KEYS
    assert e["status"] == "partially_filled"
    assert e["qty"] == 0.0005 and e["fill_price"] == 85500.0
    assert ORDER_ID_RE.match(e["order_id"]) and FILL_ID_RE.match(e["fill_id"])
    assert e["order_id"] == Reporter.pseudonym("o", "order-example-only")
    assert e["fill_id"] == Reporter.pseudonym("f", "fill-example-only")
    assert "order-example-only" not in alloc["body"].decode() and "fill-example-only" not in alloc["body"].decode()
    assert e["reason_code"] is None


def test_non_fill_statuses_force_nulls(reporter):
    """체결 외 상태는 qty/fill_price/fill_id 가 전달되더라도 null (order_id 는 가명으로 유지 가능)."""
    for status in ("acknowledged", "submitted", "rejected", "cancelled", "error"):
        alloc = reporter.execution("test", **IDENT, action="entry", status=status, qty=0.1, fill_price=1.0,
                                   fill_id="x", order_id="o1", reason_code="EXCHANGE_TIMEOUT" if status == "error" else None)
        e = _body(alloc)["execution"]
        assert e["qty"] is None and e["fill_price"] is None and e["fill_id"] is None, status
        assert e["order_id"] == Reporter.pseudonym("o", "o1")
    alloc = reporter.execution("test", **IDENT, action="protection_update", status="protection_updated")
    e = _body(alloc)["execution"]
    assert e["qty"] is None and e["fill_price"] is None and e["fill_id"] is None and e["order_id"] is None


def test_fill_status_requires_qty_price_fill_id(reporter, store):
    for kw in (dict(fill_price=1.0, fill_id="f"), dict(qty=0.1, fill_id="f"), dict(qty=0.1, fill_price=1.0)):
        with pytest.raises(ReportError):
            reporter.execution("test", **IDENT, action="entry", status="filled", order_id="o", **kw)
    with pytest.raises(ReportError):
        reporter.execution("test", **IDENT, action="entry", status="filled", qty=0, fill_price=1.0, fill_id="f")
    assert store.pending_reports("test") == []      # 실패한 호출은 sequence 를 소비하지 않는다
    assert reporter.execution("test", **IDENT, action="entry", status="filled", qty=0.001, fill_price=1.0,
                              fill_id="f")["sequence"] == 1


def test_protection_updated_requires_protection_update_action(reporter):
    with pytest.raises(ReportError):
        reporter.execution("test", **IDENT, action="entry", status="protection_updated")
    b = _body(reporter.execution("test", **IDENT, action="protection_update", status="protection_updated"))
    assert b["execution"]["action"] == "protection_update"


def test_identity_validation(reporter):
    with pytest.raises(ReportError):
        reporter.execution("test", **dict(IDENT, leg="long"), action="entry", status="acknowledged")       # idx2→short
    with pytest.raises(ReportError):
        reporter.execution("test", **dict(IDENT, position_idx=1), action="entry", status="acknowledged")   # idx1→long
    with pytest.raises(ReportError):
        reporter.execution("paper", **IDENT, action="entry", status="acknowledged")
    with pytest.raises(ReportError):
        reporter.execution("test", **dict(IDENT, event_id="bad id!"), action="entry", status="acknowledged")
    with pytest.raises(ReportError):
        reporter.execution("test", **IDENT, action="close", status="acknowledged")
    with pytest.raises(ReportError):
        reporter.execution("test", **IDENT, action="entry", status="done")
    with pytest.raises(ReportError):
        reporter.execution("test", **IDENT, action="entry", status="rejected", reason_code="free text!")
    # idx 0 은 어느 레그든 허용
    b = _body(reporter.execution("test", **dict(IDENT, position_idx=0, leg="long"), action="entry", status="acknowledged"))
    assert b["execution"]["position_idx"] == 0


def test_pseudonym_is_deterministic_and_prefixed():
    assert Reporter.pseudonym("o", "porder-1") == Reporter.pseudonym("order", "porder-1")
    assert Reporter.pseudonym("f", "pexec-1") == Reporter.pseudonym("fill", "pexec-1")
    assert ORDER_ID_RE.match(Reporter.pseudonym("o", "porder-1"))
    assert FILL_ID_RE.match(Reporter.pseudonym("f", "pexec-1"))
    assert Reporter.pseudonym("o", "a") != Reporter.pseudonym("o", "b")
    assert Reporter.pseudonym("o", "a")[2:] == Reporter.pseudonym("f", "a")[2:]
    assert Reporter.pseudonym("o", None) is None
    with pytest.raises(ReportError):
        Reporter.pseudonym("x", "a")


# --------------------------------------------------------------------------- #
# 본문: snapshot
# --------------------------------------------------------------------------- #
def test_snapshot_body_matches_contract(reporter):
    obs = now_ms()
    positions = [
        {"position_id": "position-demo-1", "strategy": "overheat", "leg": "short", "position_idx": 2,
         "qty": 0.002, "entry_price": 86000, "mark_price": 85950, "stop_loss": None, "take_profit": None,
         "updated_at_ms": obs - 5},
        {"position_id": "position-demo-2", "strategy": "basic", "leg": "long", "position_idx": 1,
         "qty": 0.001, "entry_price": 85000, "mark_price": None, "stop_loss": 84000, "take_profit": [],
         "updated_at_ms": obs + 100_000},   # 미래 → observed 로 클램프
        {"position_id": "position-demo-3", "strategy": "range", "leg": "long", "position_idx": 1,
         "qty": 0.003, "entry_price": 85000, "mark_price": 85950, "stop_loss": 84000, "take_profit": 90000,
         "updated_at_ms": obs - 1},
    ]
    alloc = reporter.snapshot("test", positions, observed_at_ms=obs)
    b = _body(alloc)
    _header_ok(b, "test", "snapshot")
    assert set(b) == HEADER_KEYS | {"complete", "account_scope", "positions"}
    assert b["complete"] is True and b["account_scope"] == "lake_dedicated_BTCUSDT"
    assert b["observed_at_ms"] == obs
    ps = {p["position_id"]: p for p in b["positions"]}
    assert len(ps) == 3
    for p in ps.values():
        assert set(p) == POSITION_KEYS
        assert p["updated_at_ms"] <= b["observed_at_ms"]
        assert p["qty"] > 0 and p["entry_price"] > 0
    assert ps["position-demo-1"]["stop_loss"] is None and ps["position-demo-1"]["take_profit"] is None
    assert ps["position-demo-1"]["mark_price"] == 85950.0
    assert ps["position-demo-2"]["take_profit"] == [] and ps["position-demo-2"]["stop_loss"] == 84000.0
    assert ps["position-demo-2"]["mark_price"] is None
    assert ps["position-demo-2"]["updated_at_ms"] == obs
    assert ps["position-demo-3"]["take_profit"] == [90000.0]
    assert alloc["kind"] == "snapshot"


def test_empty_snapshot_is_confirmed_flat(reporter):
    b = _body(reporter.snapshot("test", [], observed_at_ms=now_ms()))
    assert b["positions"] == [] and b["complete"] is True


def test_snapshot_rejects_contract_violations(reporter, store):
    good = {"position_id": "p1", "strategy": "basic", "leg": "long", "position_idx": 1, "qty": 0.001,
            "entry_price": 85000, "mark_price": None, "stop_loss": None, "take_profit": None, "updated_at_ms": now_ms()}
    obs = now_ms()
    with pytest.raises(ReportError):
        reporter.snapshot("test", [dict(good, qty=0)], observed_at_ms=obs)
    with pytest.raises(ReportError):
        reporter.snapshot("test", [dict(good, entry_price=None)], observed_at_ms=obs)
    with pytest.raises(ReportError):
        reporter.snapshot("test", [good, dict(good)], observed_at_ms=obs)            # position_id 중복
    with pytest.raises(ReportError):
        reporter.snapshot("test", [good, dict(good, position_id="p2", position_idx=0)], observed_at_ms=obs)  # 0 과 1 혼용
    with pytest.raises(ReportError):
        reporter.snapshot("test", [dict(good, leg="short")], observed_at_ms=obs)    # idx1 → long
    with pytest.raises(ReportError):
        reporter.snapshot("test", [dict(good, take_profit=[1.0] * 21)], observed_at_ms=obs)
    with pytest.raises(ReportError):
        reporter.snapshot("test", [dict(good, stop_loss=-1)], observed_at_ms=obs)
    with pytest.raises(ReportError):   # 단방향(idx 0) 에 롱/숏 동시 보유
        reporter.snapshot("test", [dict(good, position_idx=0), dict(good, position_id="p2", position_idx=0, leg="short")],
                          observed_at_ms=obs)
    assert store.pending_reports("test") == []


# --------------------------------------------------------------------------- #
# sequence / observed
# --------------------------------------------------------------------------- #
def test_sequence_is_contiguous_and_per_mode(reporter, store):
    seqs = [_ack(reporter)["sequence"] for _ in range(3)]
    seqs.append(reporter.snapshot("test", [], observed_at_ms=now_ms())["sequence"])
    assert seqs == [1, 2, 3, 4]
    assert _ack(reporter, mode="live")["sequence"] == 1
    assert _ack(reporter, mode="live")["sequence"] == 2
    assert _ack(reporter)["sequence"] == 5
    ids = [r["report_id"] for r in store.pending_reports("test")]
    assert len(ids) == len(set(ids)) == 5


def test_sequence_persists_across_restart(settings, alerts, fake_client):
    store1 = Store(settings.db_path)
    r1 = Reporter(settings, store1, alerts, client=fake_client)
    assert _ack(r1)["sequence"] == 1
    assert _ack(r1)["sequence"] == 2
    obs_last = _body(_ack(r1, observed_at_ms=now_ms()))["observed_at_ms"]
    store1.close()

    store2 = Store(settings.db_path)
    try:
        r2 = Reporter(settings, store2, alerts, client=fake_client)
        alloc = _ack(r2)
        assert alloc["sequence"] == 4
        assert _body(alloc)["observed_at_ms"] >= obs_last
        assert len(store2.pending_reports("test")) == 4
    finally:
        store2.close()


def test_observed_at_ms_is_monotonic_and_not_after_ts(reporter):
    t0 = now_ms() - 1000
    b1 = _body(_ack(reporter, observed_at_ms=t0))
    assert b1["observed_at_ms"] == t0
    b2 = _body(_ack(reporter, observed_at_ms=t0 - 500))           # 과거로 역행 → 이전 값으로 클램프
    assert b2["observed_at_ms"] == t0
    b3 = _body(_ack(reporter, observed_at_ms=now_ms() + 60_000))  # 미래 → ts 로 클램프
    assert b3["observed_at_ms"] == b3["ts"]
    b4 = _body(reporter.snapshot("test", [], observed_at_ms=t0))  # 스냅샷도 같은 스트림
    assert b4["observed_at_ms"] >= b3["observed_at_ms"]
    with pytest.raises(ReportError):
        _ack(reporter, observed_at_ms=0)


# --------------------------------------------------------------------------- #
# 전송 상태 전이
# --------------------------------------------------------------------------- #
def test_delivery_202_is_sent_with_signed_stored_bytes(reporter, store, fake_client, settings):
    alloc = _ack(reporter)
    fake_client.queue(202)
    assert reporter.deliver_pending("test") == 1
    row = _row(store, alloc["report_id"])
    assert row["state"] == st.REPORT_SENT and row["http_status"] == 202 and row["attempts"] == 1
    assert row["sent_at_ms"] is not None
    assert len(fake_client.posts) == 1
    p = fake_client.posts[0]
    assert p["url"] == REPORT_URL_TEST
    assert p["content"] == alloc["body"] == bytes(row["body"])     # 저장된 바이트 그대로 전송
    assert p["timeout"] == settings.report_http_timeout_s
    h = p["headers"]
    assert h["Content-Type"] == "application/json"
    assert h["X-Timestamp"] == str(_body(alloc)["ts"])
    assert h["X-Signature"] == auth.sign(alloc["body"], TEST_REPORT_SECRET)
    assert auth.verify(p["content"], h["X-Signature"], h["X-Timestamp"], _body(alloc)["ts"], TEST_REPORT_SECRET,
                       now_ms(), 60000) == _body(alloc)["ts"]
    assert TEST_REPORT_SECRET not in str(h)
    # 보낼 게 없으면 0, 재전송 없음
    assert reporter.deliver_pending("test") == 0
    assert len(fake_client.posts) == 1


def test_delivery_200_is_duplicate(reporter, store, fake_client, alerts):
    alloc = _ack(reporter)
    fake_client.queue(200)
    assert reporter.deliver_pending("test") == 1
    row = _row(store, alloc["report_id"])
    assert row["state"] == st.REPORT_DUPLICATE and row["http_status"] == 200 and row["note"] == "duplicate"
    assert row["sent_at_ms"] is not None
    assert alerts.messages == []


def test_delivery_409_is_conflict_with_alert(reporter, store, fake_client, alerts):
    alloc = _ack(reporter)
    fake_client.queue(409)
    assert reporter.deliver_pending("test") == 0
    row = _row(store, alloc["report_id"])
    assert row["state"] == st.REPORT_CONFLICT and row["http_status"] == 409
    assert alerts.contains("conflict") and alerts.contains("409")
    assert reporter.deliver_pending("test") == 0 and len(fake_client.posts) == 1   # 재시도하지 않는다


def test_delivery_other_4xx_is_failed_and_does_not_block_next(reporter, store, fake_client, alerts):
    a1 = _ack(reporter)
    a2 = _ack(reporter)
    fake_client.queue(400, 202)
    assert reporter.deliver_pending("test") == 1
    assert _row(store, a1["report_id"])["state"] == st.REPORT_FAILED
    assert _row(store, a2["report_id"])["state"] == st.REPORT_SENT
    assert alerts.contains("400")


def test_delivery_500_then_202_retry_succeeds(reporter, store, fake_client, alerts):
    alloc = _ack(reporter)
    fake_client.queue(500)
    assert reporter.deliver_pending("test") == 0
    row = _row(store, alloc["report_id"])
    assert row["state"] == st.REPORT_PENDING and row["attempts"] == 1 and row["http_status"] == 500
    assert alerts.messages == []
    fake_client.queue(202)
    assert reporter.deliver_pending("test") == 1
    row = _row(store, alloc["report_id"])
    assert row["state"] == st.REPORT_SENT and row["attempts"] == 2 and row["http_status"] == 202
    assert len(fake_client.posts) == 2
    assert fake_client.posts[0]["content"] == fake_client.posts[1]["content"] == alloc["body"]   # 같은 바이트 재전송
    assert fake_client.posts[0]["headers"] == fake_client.posts[1]["headers"]


def test_delivery_5xx_blocks_later_sequences_until_resolved(reporter, store, fake_client):
    a1 = _ack(reporter)
    a2 = _ack(reporter)
    fake_client.queue(503)
    assert reporter.deliver_pending("test") == 0
    assert len(fake_client.posts) == 1                      # 뒤 번호는 시도하지 않았다
    assert _row(store, a2["report_id"])["state"] == st.REPORT_PENDING and _row(store, a2["report_id"])["attempts"] == 0
    fake_client.queue(202, 202)
    assert reporter.deliver_pending("test") == 2
    assert [json.loads(p["content"])["sequence"] for p in fake_client.posts] == [1, 1, 2]
    assert _row(store, a1["report_id"])["state"] == st.REPORT_SENT
    assert _row(store, a2["report_id"])["state"] == st.REPORT_SENT


def test_delivery_timeout_retries_then_fails_after_max_attempts(reporter, store, fake_client, alerts, settings):
    assert settings.report_max_attempts == 3
    alloc = _ack(reporter)
    fake_client.queue(httpx.TimeoutException("timed out"), httpx.ConnectError("refused"), httpx.TimeoutException("t"))
    assert reporter.deliver_pending("test") == 0
    assert _row(store, alloc["report_id"])["state"] == st.REPORT_PENDING
    assert reporter.deliver_pending("test") == 0
    row = _row(store, alloc["report_id"])
    assert row["state"] == st.REPORT_PENDING and row["attempts"] == 2
    assert alerts.messages == []
    assert reporter.deliver_pending("test") == 0
    row = _row(store, alloc["report_id"])
    assert row["state"] == st.REPORT_FAILED and row["attempts"] == 3
    assert alerts.contains("failed")
    assert reporter.deliver_pending("test") == 0 and len(fake_client.posts) == 3


def test_delivery_timeout_then_success(reporter, store, fake_client):
    alloc = _ack(reporter)
    fake_client.queue(httpx.TimeoutException("timed out"), 202)
    assert reporter.deliver_pending("test") == 0
    assert reporter.deliver_pending("test") == 1
    row = _row(store, alloc["report_id"])
    assert row["state"] == st.REPORT_SENT and row["attempts"] == 2


def test_delivery_fails_when_attempt_window_exceeded(reporter, store, fake_client, alerts, settings, monkeypatch):
    alloc = _ack(reporter)
    fake_client.queue(500)
    assert reporter.deliver_pending("test") == 0
    assert _row(store, alloc["report_id"])["state"] == st.REPORT_PENDING
    # 창(report_attempt_window_ms) 이 지난 뒤에는 서명 시각이 상대의 60초 창을 벗어나므로 재시도하지 않고 failed
    real_now = now_ms()
    monkeypatch.setattr(reporter_mod, "now_ms", lambda: real_now + settings.report_attempt_window_ms + 1)
    fake_client.queue(202)
    assert reporter.deliver_pending("test") == 0
    row = _row(store, alloc["report_id"])
    assert row["state"] == st.REPORT_FAILED and row["attempts"] == 1
    assert "window" in (row["note"] or "")
    assert len(fake_client.posts) == 1
    assert alerts.contains("window")


def test_unconfigured_mode_is_marked_unsent(reporter, store, fake_client, settings):
    assert "live" not in settings.secrets.report_url
    alloc = _ack(reporter, mode="live")
    assert reporter.deliver_pending("live") == 0
    row = _row(store, alloc["report_id"])
    assert row["state"] == st.REPORT_UNSENT and row["attempts"] == 0
    assert fake_client.posts == []


def test_unconfigured_secret_is_unsent(settings_factory, alerts):
    s = settings_factory(drop_env=("LAKE_REPORT_SECRET_TEST",))
    store = Store(s.db_path)
    try:
        fc = FakeClient()
        r = Reporter(s, store, alerts, client=fc)
        alloc = _ack(r)
        assert r.deliver_pending("test") == 0
        assert _row(store, alloc["report_id"])["state"] == st.REPORT_UNSENT
        assert fc.posts == []
    finally:
        store.close()


def test_run_forever_drains_and_stops(reporter, store, fake_client):
    a1 = _ack(reporter)
    fake_client.queue(202)
    stop = threading.Event()
    t = threading.Thread(target=reporter.run_forever, args=(stop,), daemon=True)
    t.start()
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and _row(store, a1["report_id"])["state"] != st.REPORT_SENT:
        time.sleep(0.05)
    stop.set()
    t.join(timeout=3)
    assert not t.is_alive()
    assert _row(store, a1["report_id"])["state"] == st.REPORT_SENT
