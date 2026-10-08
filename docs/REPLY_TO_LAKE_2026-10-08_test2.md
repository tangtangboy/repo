# 2026-10-08 서명 TEST 재전송(event lake-test-db35307e-…) 수신측 대조 결과 — 정상

확인 시각: 2026-10-08 22:05 KST (13:05 UTC)
결론: **접수·본문 해시·가상 체결 모두 정상.** 실주문 0건. 같은 position_id 로 seq 2 `full_exit` 를 보내 주면 진입→청산 한 쌍이 완성된다.

## 1) 접수 (직접 확인)

| 항목 | 수신측 기록 |
|---|---|
| Caddy 접속 로그 | 1791464408.994 = 2026-10-08 13:00:08.994 UTC (22:00:08.994 KST), POST /lake/signal, **202**, 응답 109 bytes, 처리 23 ms |
| 본문 ts → 수신 저장 | ts 1791464408713 → received 1791464408988 (13:00:08.988 UTC): 신호 생성 → 접수 275 ms |
| event_id / position_id / seq | lake-test-db35307e-69f0-49f5-b81a-382c8b4dc647 / lake-test-position-db35307e-69f0-49f5-b81a-382c8b4dc647 / 1 |
| mode / strategy / action / leg / idx | test / overheat / entry / short / 2 |
| 수량·가격 | 0.001 BTC, reference 82275.9, SL 83921.42, TP [80630.38] |
| **본문 SHA-256(수신 원본 바이트)** | **116eec33bc05e1c5d5ed0b84d922944a45dfd5cfef00d85b59668cc9b231cfc5** — 송신 원장의 body_sha256 과 대조 바람 |
| 수신 시점 Bybit 공개 시세 | mark 82277.99 / last 82275.8 |
| 재전달 | duplicate_count 0, conflict_count 0, ingress 기록 없음 |

## 2) 실행 — 가상 체결 완료 (실주문 없음)

| 항목 | 값 |
|---|---|
| signal_status / reason | **done** / 없음 (processed 13:00:09.043 UTC, 접수 후 55 ms) |
| 가상 주문 | order_link_id lk4e5c47e67e3afe0afc39ea357c24369d, order_id **porder-179cc674-3**, Sell 0.001, Filled |
| 가상 체결 | exec_id **pexec-179cc674-3**, 0.001 BTC @ **82275.9** (= reference_price; test 모드는 reference_price 를 모의 시세로 씀), 13:00:09.012 UTC, 접수 후 24 ms |
| lot | position_id 위와 같음, short, idx 2, qty 0.001, avg_entry 82275.9, SL 83921.42, TP [80630.38], protection_revision 1, **open** |
| 가상거래소 포지션 | idx 2 Sell 0.001 @ 82275.9 (lot 과 일치, 대사 정상) |
| 실주문 | 0건 — 거래소 키 없음(LIVE_DISABLED), test 모드는 가상 체결기만 사용 |

receipts 원문:
```
{"received_at_ms": 1791464408988, "received_at": "2026-10-08T13:00:08.988Z", "mode": "test", "event_id": "lake-test-db35307e-69f0-49f5-b81a-382c8b4dc647", "position_id": "lake-test-position-db35307e-69f0-49f5-b81a-382c8b4dc647", "event_sequence": 1, "strategy": "overheat", "action": "entry", "leg": "short", "position_idx": 2, "qty_btc": 0.001, "body_sha256": "116eec33bc05e1c5d5ed0b84d922944a45dfd5cfef00d85b59668cc9b231cfc5", "signal_status": "done", "signal_reason": null, "processed_at_ms": 1791464409043, "signal_note": "", "duplicate_count": 0, "conflict_count": 0}
```
fills 원문:
```
{"mode": "test", "account": "bybit", "event_id": "lake-test-db35307e-69f0-49f5-b81a-382c8b4dc647", "position_id": "lake-test-position-db35307e-69f0-49f5-b81a-382c8b4dc647", "strategy": "overheat", "leg": "short", "position_idx": 2, "action": "entry", "purpose": "entry", "side": "Sell", "reduce_only": 0, "order_link_id": "lk4e5c47e67e3afe0afc39ea357c24369d", "order_id": "porder-179cc674-3", "order_status": "Filled", "order_qty": 0.001, "exec_id": "pexec-179cc674-3", "exec_qty": 0.001, "exec_price": 82275.9, "exec_time_ms": 1791464409012, "exec_time": "2026-10-08T13:00:09.012Z", "fee": 0.0, "fee_currency": "USDT", "exec_type": "Trade"}
```
(fee 0.0 은 가상 체결기의 값이며 실제 거래소 수수료가 아니다. ingress: 해당 event 기록 없음 = 정상.)

## 3) 다음

- 같은 position_id 로 **seq 2 `full_exit`**(qty_btc 0.001, expected_qty_btc_after 0, reference_price 는 현재가 또는 null) 를 보내 주면 lot 이 닫히고 fills 에 청산 체결 1행이 추가된다. 보내면 시각·event_id 만 알려 달라.
- 그 뒤 add / partial_exit / protection_update 표본도 같은 방식으로 대조 가능하다.
- 정책 합의 항목(만료 경계, 순번 건너뜀, 보호값 null, TP 배분·트리거 기준) 은 별도로.
