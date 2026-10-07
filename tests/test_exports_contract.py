"""lake 인수인계(2026-10-07) 후속: 응답 본문(duplicate/mode/event_id), 정수 필드 strict, 사후 대조용 내보내기(receipts/fills/ingress)."""
from __future__ import annotations

import json
from pathlib import Path

from lake_executor.main import main as cli_main
from lake_executor.signal_log import FILL_COLUMNS, INGRESS_COLUMNS, RECEIPT_COLUMNS, dicts_to_csv, export_rows

from conftest import SIGNAL_PATH, TEST_SIGNAL_SECRET, make_signal, sign_body


def _post(client, d):
    body, headers = sign_body(d, TEST_SIGNAL_SECRET)
    return client.post(SIGNAL_PATH, content=body, headers=headers)


def test_responses_carry_duplicate_mode_and_event_id(client, store):
    d = make_signal()
    r = _post(client, d)
    assert r.status_code == 202 and r.json() == {"accepted": True, "duplicate": False, "mode": "test", "event_id": d["event_id"]}
    r2 = _post(client, d)
    assert r2.status_code == 200 and r2.json() == {"accepted": True, "duplicate": True, "mode": "test", "event_id": d["event_id"]}
    d2 = dict(d, qty_btc=0.003)                                        # 같은 ID, 다른 본문
    r3 = _post(client, d2)
    assert r3.status_code == 409 and r3.json()["code"] == "EVENT_ID_CONFLICT"
    d3 = make_signal(position_id=d["position_id"], event_sequence=1, action="full_exit", qty_btc=0.002,
                     expected_qty_btc_after=0, reference_price=None)   # 순서 역전/동일 → 409
    r4 = _post(client, d3)
    assert r4.status_code == 409 and r4.json()["code"] == "SEQUENCE_CONFLICT"


def test_integer_fields_reject_strings_and_booleans(client):
    for over in ({"ts": "str"}, {"event_sequence": True}, {"expires_at_ms": "x"}, {"protection_revision": 1.5}):
        d = make_signal()
        if over == {"ts": "str"}:
            d["ts"] = str(d["ts"])                                      # 본문 ts 만 문자열 (헤더 X-Timestamp 는 같은 값의 정수)
        else:
            d.update(over)
        r = _post(client, d)
        # 본문 ts 가 문자열이면 서명 단계의 본문/헤더 시각 대조에서 먼저 걸린다 (401 TIMESTAMP_MISMATCH); 나머지는 스키마 400
        expected = 401 if over == {"ts": "str"} else 400
        assert r.status_code == expected, (over, r.status_code, r.text)
        if expected == 401:
            assert r.json()["code"] == "TIMESTAMP_MISMATCH"


def test_receipt_fill_and_ingress_exports(client, store, executor, settings, capsys):
    pid = make_signal()["position_id"]
    base = dict(position_id=pid, leg="short", position_idx=2, strategy="overheat")
    e1 = make_signal(**base, event_sequence=1, action="entry", qty_btc=0.002, expected_qty_btc_after=0.002, reference_price=86000)
    assert _post(client, e1).status_code == 202
    assert _post(client, e1).status_code == 200                       # 중복 재전달 1회
    assert executor.run_once() is True
    e2 = make_signal(**base, event_sequence=2, action="full_exit", qty_btc=0.002, expected_qty_btc_after=0, reference_price=None)
    assert _post(client, e2).status_code == 202
    assert executor.run_once() is True
    e3 = make_signal(**base, event_sequence=1, action="add", qty_btc=0.001, expected_qty_btc_after=0.003)   # 역전 → 409
    assert _post(client, e3).status_code == 409

    rows, cols = export_rows(store, "receipts", "test", None, None)
    assert cols == RECEIPT_COLUMNS and [r["event_id"] for r in rows] == [e1["event_id"], e2["event_id"]]
    r1 = rows[0]
    assert r1["signal_status"] == "done" and r1["duplicate_count"] == 1 and r1["conflict_count"] == 0
    assert len(r1["body_sha256"]) == 64 and r1["action"] == "entry" and r1["event_sequence"] == 1 and r1["processed_at_ms"]
    assert export_rows(store, "receipts", "live", None, None)[0] == []

    fills, fcols = export_rows(store, "fills", "test", None, None)
    assert fcols == FILL_COLUMNS and len(fills) == 2
    f1 = fills[0]
    assert f1["event_id"] == e1["event_id"] and f1["order_link_id"] and f1["exec_id"] and f1["exec_qty"] == 0.002
    assert f1["strategy"] == "overheat" and f1["leg"] == "short" and f1["action"] == "entry" and f1["purpose"]
    assert fills[1]["action"] == "full_exit" and fills[1]["reduce_only"] == 1 and fills[1]["fee"] is None   # 수수료는 거래소 히스토리에서

    ing, icols = export_rows(store, "ingress", None, None, None)
    assert icols == INGRESS_COLUMNS
    codes = [(r["code"], r["http"], r["event_id"]) for r in ing]
    assert ("DUPLICATE", 200, e1["event_id"]) in codes and ("SEQUENCE_CONFLICT", 409, e3["event_id"]) in codes
    assert all(len(r["body_sha256"]) == 64 for r in ing)

    csv_text = dicts_to_csv(rows, cols)
    assert csv_text.splitlines()[0] == ",".join(RECEIPT_COLUMNS) and e1["event_id"] in csv_text

    # CLI + 대시보드
    root = Path(settings.state_dir).parent
    argv = ["--config", str(root / "config.json"), "--env", str(root / ".env")]
    assert cli_main(argv + ["export", "--kind", "receipts", "--mode", "test", "--format", "jsonl"]) == 0
    out = capsys.readouterr().out
    first = json.loads(out.splitlines()[0])
    assert first["event_id"] == e1["event_id"] and first["duplicate_count"] == 1 and first["received_at"].endswith("Z")
    assert cli_main(argv + ["export", "--kind", "fills", "--format", "csv"]) == 0
    assert capsys.readouterr().out.splitlines()[0] == ",".join(FILL_COLUMNS)
    from test_web import login
    login(client)
    r = client.get("/ui/signal-log.csv?kind=fills&mode=test")
    assert r.status_code == 200 and r.text.splitlines()[0] == ",".join(FILL_COLUMNS) and "attachment; filename=\"fills_test_" in r.headers["content-disposition"]
    assert client.get("/ui/signal-log.csv?kind=nope").status_code == 400
    page = client.get("/ui/signal-log")
    assert "receipts CSV" in page.text and "ingress CSV" in page.text
