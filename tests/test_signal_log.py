"""라이브 신호 로그 (signal_log.py) — 주문 경로 밖 비동기 기록, JSONL + DB, 내보내기(조인), 대시보드 페이지.

- store: append(중복 제거)/rows/count/export 조인
- SignalLog: record() 는 블로킹하지 않는다(가격 조회가 느려도), writer 가 가격·JSONL·DB 를 채운다, DB 장애 시 JSONL 은 남고 통계에 집계
- 수신기 훅: 202(new)/200(duplicate) 뒤에 큐에 들어가고 DB 에는 첫 기록만 남는다
- export: 계정별 run + 체결 집계 + 계정 사이징 문맥이 한 행에, CSV 헤더 = EXPORT_COLUMNS
- 대시보드 /ui/signal-log, /ui/signal-log.csv (로그인 필요, 필터 검증)
"""
from __future__ import annotations

import json
import threading
import time

import pytest

from lake_executor import signal_log as sl
from lake_executor.main import parse_when
from lake_executor.schemas import Signal
from lake_executor.store import Store
from lake_executor.util import now_ms

from conftest import ADMIN_TOKEN, SIGNAL_PATH, TEST_SIGNAL_SECRET, make_signal, run_signal, sign_body


def _stub_price(symbol: str) -> dict:
    return {"mark_price": 81000.5, "last_price": 81001.0, "source": "stub"}


def _factory(store):
    """테스트의 store 와 같은 백엔드/경로(/스키마) 로 두 번째 연결을 연다 (SQLite 파일 또는 Postgres 임시 스키마)."""
    return lambda: Store(store.path, schema=getattr(store, "schema", "") or "lake_executor")


def _slog(store, settings, tmp_path, fetcher=_stub_price) -> sl.SignalLog:
    return sl.SignalLog(store_factory=_factory(store), state_dir=str(tmp_path / "st"), settings=settings, price_fetcher=fetcher)


# ---------------------------------------------------------------- store
def test_store_append_dedupes_and_filters(store, settings):
    sig = Signal.model_validate(make_signal())
    row = sl.signal_to_row(sig, "new", now_ms(), sl.accounts_context(settings))
    assert store.append_signal_log(row) is True
    assert store.append_signal_log(row) is False                     # 같은 (mode, event_id) → 첫 기록만
    rows = store.signal_log_rows(mode="test")
    assert len(rows) == 1 and rows[0]["event_id"] == sig.event_id
    assert rows[0]["accounts"][0]["name"] == "bybit" and rows[0]["accounts"][0]["leverage"] == settings.accounts[0].leverage
    assert rows[0]["take_profit"] is None and rows[0]["ingest_result"] == "new"
    assert store.signal_log_count() == 1 and store.signal_log_count("live") == 0
    assert store.signal_log_rows(mode="live") == []
    assert store.signal_log_rows(since_ms=now_ms() + 60_000) == []
    assert store.signal_log_rows(until_ms=rows[0]["received_at_ms"]) == []


# ---------------------------------------------------------------- writer
def test_writer_thread_adds_price_jsonl_and_db(store, settings, tmp_path):
    slog = _slog(store, settings, tmp_path)
    sigs = [Signal.model_validate(make_signal()) for _ in range(3)]
    for s in sigs:
        assert slog.record(s, "new", now_ms()) is True
    assert slog.snapshot()["queued"] == 3
    stop = threading.Event()
    t = threading.Thread(target=slog.run_forever, args=(stop,), daemon=True)
    t.start()
    deadline = time.time() + 20
    while slog.snapshot()["written"] < 3 and time.time() < deadline:
        time.sleep(0.05)
    stop.set()
    t.join(15)
    st = slog.snapshot()
    assert st["written"] == 3 and st["db_errors"] == 0 and st["dropped"] == 0 and st["pending"] == 0
    lines = (tmp_path / "st" / "signal_log.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3 and json.loads(lines[0])["mark_price"] == 81000.5 and json.loads(lines[0])["price_source"] == "stub"
    rows = store.signal_log_rows(mode="test", ascending=True)
    assert [r["event_id"] for r in rows] == [s.event_id for s in sigs]
    assert rows[0]["last_price"] == 81001.0 and rows[0]["price_at_ms"] >= rows[0]["received_at_ms"]


def test_record_never_blocks_even_if_price_fetch_is_slow(store, settings, tmp_path):
    def slow(symbol):
        time.sleep(1.5)
        return None
    slog = _slog(store, settings, tmp_path, fetcher=slow)
    t0 = time.monotonic()
    assert slog.record(Signal.model_validate(make_signal()), "new") is True
    assert time.monotonic() - t0 < 0.2
    assert slog.snapshot()["pending"] == 1
    assert slog.drain() == 1
    st = slog.snapshot()
    assert st["written"] == 1 and st["price_missing"] == 1
    assert store.signal_log_rows()[0]["mark_price"] is None


def test_db_outage_keeps_jsonl_and_counts_errors(settings, tmp_path, monkeypatch):
    class Broken:
        def append_signal_log(self, row):
            raise RuntimeError("db down")

        def close(self):
            pass
    monkeypatch.setattr(sl, "DB_RETRY_MAX", 1)
    slog = sl.SignalLog(store_factory=lambda: Broken(), state_dir=str(tmp_path / "st"), settings=settings, price_fetcher=_stub_price)
    assert slog.record(Signal.model_validate(make_signal()), "new") is True
    assert slog.drain() == 1
    st = slog.snapshot()
    assert st["written"] == 0 and st["db_errors"] >= 1 and st["last_error"].startswith("db:")
    assert len((tmp_path / "st" / "signal_log.jsonl").read_text(encoding="utf-8").splitlines()) == 1


def test_queue_full_drops_and_counts(store, settings, tmp_path):
    slog = sl.SignalLog(store_factory=_factory(store), state_dir=str(tmp_path / "st"), settings=settings,
                        price_fetcher=_stub_price, max_queue=2)
    for _ in range(3):
        slog.record(Signal.model_validate(make_signal()), "new")
    st = slog.snapshot()
    assert st["queued"] == 2 and st["dropped"] == 1


# ---------------------------------------------------------------- receiver hook
def test_receiver_records_new_and_duplicate_but_db_keeps_first(client, store, settings, services, tmp_path):
    slog = _slog(store, settings, tmp_path)
    services.signal_log = slog
    d = make_signal()
    raw, headers = sign_body(d, TEST_SIGNAL_SECRET)
    assert client.post(SIGNAL_PATH, content=raw, headers=headers).status_code == 202
    assert client.post(SIGNAL_PATH, content=raw, headers=headers).status_code == 200    # duplicate
    assert slog.snapshot()["queued"] == 2
    assert slog.drain() == 2
    st = slog.snapshot()
    assert st["written"] == 1 and st["duplicates"] == 1
    rows = store.signal_log_rows()
    assert len(rows) == 1 and rows[0]["ingest_result"] == "new" and rows[0]["event_id"] == d["event_id"]
    # 거부(401) 는 기록하지 않는다
    raw2, headers2 = sign_body(make_signal(), "wrong-secret-wrong-secret-wrong-secret-xx")
    assert client.post(SIGNAL_PATH, content=raw2, headers=headers2).status_code == 401
    assert slog.snapshot()["queued"] == 2


# ---------------------------------------------------------------- export
def test_export_joins_runs_fills_and_account_context(executor, store, settings, paper):
    d = make_signal()
    sig = Signal.model_validate(d)
    store.append_signal_log(sl.signal_to_row(sig, "new", now_ms(), sl.accounts_context(settings)))
    assert run_signal(executor, store, d)["status"] == "done"
    rows = store.export_signal_rows(mode="test")
    assert len(rows) == 1
    r = rows[0]
    assert r["event_id"] == sig.event_id and r["account"] == "bybit" and r["run_status"] == "done"
    assert r["fill_qty"] == pytest.approx(0.002) and r["fill_avg_price"] > 0 and r["fill_count"] >= 1
    assert r["signal_status"] == "done" and r["processing_ms"] is not None and r["fill_latency_ms"] is not None
    assert r["account_leverage"] == settings.accounts[0].leverage and r["account_exchange"] == "bybit"
    assert r["latency_signal_to_receipt_ms"] is not None
    csv_text = sl.rows_to_csv(rows)
    header = csv_text.splitlines()[0].split(",")
    assert header == list(sl.EXPORT_COLUMNS) and sig.event_id in csv_text
    first = json.loads(sl.rows_to_jsonl(rows).splitlines()[0])
    assert first["event_id"] == sig.event_id and first["received_at"].endswith("Z")
    # run 이 없는(미처리) 신호도 한 행
    sig2 = Signal.model_validate(make_signal())
    store.append_signal_log(sl.signal_to_row(sig2, "new", now_ms(), sl.accounts_context(settings)))
    rows = store.export_signal_rows(mode="test")
    assert len(rows) == 2 and rows[1]["account"] == "" and rows[1]["run_status"] is None and rows[1]["fill_qty"] is None
    assert sl.rows_to_csv([]) .splitlines() == [",".join(sl.EXPORT_COLUMNS)] and sl.rows_to_jsonl([]) == ""


def test_parse_when_formats():
    assert parse_when(None) is None and parse_when("") is None
    assert parse_when("1700000000000") == 1700000000000
    assert parse_when("2026-10-01") == 1790812800000
    assert parse_when("2026-10-01T12:30") == 1790857800000
    assert parse_when("2026-10-01T12:30:05") == 1790857805000
    with pytest.raises(ValueError):
        parse_when("yesterday")


# ---------------------------------------------------------------- dashboard
def test_dashboard_signal_log_page_and_csv(client, store, settings):
    sig = Signal.model_validate(make_signal())
    store.append_signal_log(sl.signal_to_row(sig, "new", now_ms(), sl.accounts_context(settings)))
    assert client.get("/ui/signal-log", follow_redirects=False).status_code == 303          # 로그인 필요
    assert client.get("/ui/signal-log.csv", follow_redirects=False).status_code == 303
    r = client.post("/ui/login", data={"token": ADMIN_TOKEN}, follow_redirects=False)
    assert r.status_code == 303
    page = client.get("/ui/signal-log", follow_redirects=False)
    assert page.status_code == 200 and sig.event_id in page.text and "Download CSV" in page.text and "total rows: 1" in page.text
    csv_r = client.get("/ui/signal-log.csv?mode=test", follow_redirects=False)
    assert csv_r.status_code == 200 and csv_r.headers["content-type"].startswith("text/csv")
    assert "attachment" in csv_r.headers["content-disposition"]
    assert csv_r.text.splitlines()[0].startswith("received_at_ms,received_at,mode,event_id") and sig.event_id in csv_r.text
    assert client.get("/ui/signal-log?since=notadate", follow_redirects=False).status_code == 400
    assert client.get("/ui/signal-log?mode=bogus", follow_redirects=False).status_code == 400
    assert client.get("/ui/signal-log?mode=live", follow_redirects=False).text.count(sig.event_id) == 0
