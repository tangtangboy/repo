# 2026-10-09 청산 TEST(event lake-test-exit-19aa5919-…) 대조 결과와 자동 연결 전 규격 답변

확인 시각: 2026-10-09 01:30 KST (2026-10-08 16:30 UTC)
결론: **청산 TEST 정상.** 접수·본문 해시 일치, reduceOnly 가상 청산 체결, lot 닫힘(qty 0), 남은 보호주문 없음, 실주문 0건. 진입→청산 한 쌍이 양측 원장에서 맞춰졌다.

## 1) 청산 event 대조 (직접 확인)

| 항목 | 수신측 기록 |
|---|---|
| Caddy 접속 로그 | 1791474801.694 = 2026-10-08 15:53:21.694 UTC (00:53:21.694 KST), POST /lake/signal, **202**, 응답 114 bytes, 처리 4 ms |
| 본문 ts → 접수 저장 | 1791474801488 → 1791474801691 (15:53:21.691 UTC): 신호 생성 → 접수 203 ms |
| event_id / position_id / seq | lake-test-exit-19aa5919-dcda-55bd-ab8e-3a7f4a390cb0 / lake-test-position-db35307e-69f0-49f5-b81a-382c8b4dc647 / 2 |
| mode / strategy / action / leg / idx | test / overheat / full_exit / short / 2 |
| qty / reference | 0.001 / 81270.4 |
| **본문 SHA-256** | **41d72f371982ea8797016f51f049864a80f56eee14cada1897cbda7d27dc7550** — 송신측 값과 동일 |
| signal_status / reason | **done** / 없음 (processed 15:53:21.721 UTC, 접수 후 30 ms) |
| 수신 시점 공개 시세 | mark 81275.38 / last 81286.8 |
| 재전달 | duplicate_count 0, conflict_count 0, ingress 기록 없음 |

## 2) 청산 fills (가상)

| 항목 | 값 |
|---|---|
| 가상 주문 | order_link_id lk6e7f1c3bfffd4678f9e4b1bd2bf17f3f, order_id **porder-179cc674-6**, **Buy, reduceOnly=1**, 0.001, Filled |
| 가상 체결 | exec_id **pexec-179cc674-4**, 0.001 BTC @ **81270.4** (= reference_price), 15:53:21.699 UTC, 접수 후 8 ms |
| 실현손익(가상) | 숏 82275.9 → 81270.4 청산: +1.0055 USDT (0.001 × 1005.5), 수수료 0(가상) |

fills 원문:
```
{"mode": "test", "account": "bybit", "event_id": "lake-test-exit-19aa5919-dcda-55bd-ab8e-3a7f4a390cb0", "position_id": "lake-test-position-db35307e-69f0-49f5-b81a-382c8b4dc647", "strategy": "overheat", "leg": "short", "position_idx": 2, "action": "full_exit", "purpose": "exit", "side": "Buy", "reduce_only": 1, "order_link_id": "lk6e7f1c3bfffd4678f9e4b1bd2bf17f3f", "order_id": "porder-179cc674-6", "order_status": "Filled", "order_qty": 0.001, "exec_id": "pexec-179cc674-4", "exec_qty": 0.001, "exec_price": 81270.4, "exec_time_ms": 1791474801699, "exec_time": "2026-10-08T15:53:21.699Z", "fee": 0.0, "fee_currency": "USDT", "exec_type": "Trade"}
```
receipts 원문:
```
{"received_at_ms": 1791474801691, "received_at": "2026-10-08T15:53:21.691Z", "mode": "test", "event_id": "lake-test-exit-19aa5919-dcda-55bd-ab8e-3a7f4a390cb0", "position_id": "lake-test-position-db35307e-69f0-49f5-b81a-382c8b4dc647", "event_sequence": 2, "strategy": "overheat", "action": "full_exit", "leg": "short", "position_idx": 2, "qty_btc": 0.001, "body_sha256": "41d72f371982ea8797016f51f049864a80f56eee14cada1897cbda7d27dc7550", "signal_status": "done", "signal_reason": null, "processed_at_ms": 1791474801721, "signal_note": "", "duplicate_count": 0, "conflict_count": 0}
```

## 3) 포지션 최종 상태

- lot lake-test-position-db35307e-…: **status closed, qty 0.0**, avg_entry 82275.9, closed_at 15:53:21.710 UTC, last_event_id = 위 청산 event.
- 그 position 의 주문 4건: entry Sell 0.001 Filled / **sl Buy 0.001 Cancelled (trigger 83921.42) / tp Buy 0.001 Cancelled (trigger 80630.38)** / exit Buy 0.001 reduceOnly Filled. 남은 보호주문 없음(`protection_missing` 비어 있음).
- 가상거래소 포지션: 없음. test 계정 open lot 0, 불일치 플래그 없음.
- 실주문: 0건(거래소 키 없음, test 는 가상 체결기만).

## 4) 이전 건(lake-test-d92cdc33-a707-4757-8c4c-f1bf9101819b) 상태

접수는 됐지만 당시 우리 쪽 불일치 상태로 실행이 `RECONCILE_REQUIRED` 거부됐으므로 **lot 자체가 만들어지지 않았다**(주문·체결 0건, 보호주문 0건). 원장에는 `signals` 행만 `rejected / RECONCILE_REQUIRED` 로 남아 있다. 정리할 포지션이 없다.

## 5) 자동 연결 전 규격 답변

**(a) 수량 상한 0.05 BTC 와 0.057632222728401426 BTC 전량 종료**
- 상한(`guards.max_order_qty_btc` 0.05, `max_leg_qty_btc` 0.2) 은 **entry/add 에만** 적용된다(`QTY_LIMIT` / `LEG_LIMIT` 거부). **partial_exit/full_exit 에는 적용하지 않는다** — 청산을 막아 포지션을 열어 두는 쪽이 더 위험하기 때문.
- `full_exit` 은 본문 `qty_btc` 를 쓰지 않고 **우리 lot 잔량 전부**를 reduceOnly 로 닫는다. 0.057632… 를 보내도 lot 에 0.057 이 있으면 0.057 을 닫고 lot 은 0 이 된다.
- 수량 단위: entry/add/partial_exit 의 `qty_btc` 는 계정 배수(현재 1.0) 를 곱한 뒤 거래소 단위(0.001) 로 **내림**한다. 0.057632… 로 entry 하면 0.057 로 주문된다(단, 0.05 상한에 걸려 `QTY_LIMIT` — 아래 참조). 소수 잔량(0.000632…) 은 거래소에서 거래할 수 없으므로 우리 lot 에는 애초에 생기지 않는다.
- `expected_qty_btc_after` 는 실행 뒤 lot 수량과 단위 허용오차로 대조한다. 어긋나면 **거부가 아니라 실행 기록 note 에 `QTY_MISMATCH` + 알림**으로 남긴다(주문은 그대로 실행). 송신측 전략 잔량이 0.057632… 이고 우리 lot 이 0.057 이면 mismatch 로 기록되므로, 송신측에서 `expected_qty_btc_after` 를 **거래소 단위로 내린 값**으로 보내면 깔끔하다(권장). 전략 내부 잔량을 바꾸라는 뜻은 아니다.
- entry/add 가 0.05 를 넘는 전략이면 상한을 올려야 한다. 상한은 운영자(수신측 소유자) 가 설정으로 정한다 — 전략의 최대 1회 진입량과 포지션당 최대 보유량을 알려주면 그에 맞춰 올린다. 송신측에서 임의 분할 전송할 필요는 없다.

**(b) 수신측 보호주문(SL/TP) 이 먼저 체결된 뒤 늦게 온 청산 신호**
- 보호주문이 체결되면 대사가 그 체결을 수집해 lot 을 닫고 회신 기록(auto:sl / auto:tp) 을 남긴다. 그 뒤 도착한 `full_exit`/`partial_exit` 는 lot 이 닫혀 있으므로 **`POSITION_NOT_FOUND` 로 거부**되고 주문을 내지 않는다(이중 청산 없음). 청산 신호 처리 도중 보호주문이 체결되는 경합은 `POSITION_CLOSED` 로 거부한다.
- TP 가 일부만 체결된 경우(분할 TP) lot 잔량이 줄어 있으므로 `partial_exit` 의 `qty_btc` 가 잔량보다 크면 `QTY_EXCEEDS_LOT` 거부, `full_exit` 은 잔량만 닫는다. `expected_qty_btc_after` 가 송신측 모델과 다르면 (a) 와 같이 `QTY_MISMATCH` 로 기록만 한다.
- 모든 판단은 position_id 의 lot 단위다. 같은 심볼의 다른 전략 lot 은 건드리지 않는다.
- 사후 대조에는 fills 내보내기에 보호주문 체결(purpose sl/tp, 신호 event_id 없음, position_id 로 귀속) 이 함께 나온다.

**(c) 합의 전 정책** — 이번 시험으로 바꾸지 않았다. 현재 동작: 만료된 청산/보호갱신은 실행(`stale` 기록), 순번 건너뜀 허용, 보호값 null = 해당 보호 없음, TP 배분은 수신측 규칙, 트리거 MarkPrice. 송신측 결정이 오면 설정/코드로 맞춘다.

**(d) null TP/SL 과 protection_update**
- 송신측 화면의 null 이 "숫자 하나로 표현 불가" 라면 **그 상태에서는 protection_update 를 보내지 않는 것이 맞다**(현재 의미로는 null 이 취소가 되므로). 숫자 두 개(SL, TP 배열) 가 모두 확정된 경우에만 보내면 지금 규격으로 안전하다.
- 더 편하게 하려면 다음 의미로 바꿀 수 있다(수신측 구현 가능, 송신측이 택하면 반영): **null = 유지(변경 없음)**, 명시 취소는 `stop_loss: 0` / `take_profit: []`. 이렇게 하면 한쪽만 바꾸는 갱신이 가능하다. 어느 쪽을 쓸지 알려 달라.

## 6) 다음

- TEST 자동 송신을 켜기 전에 (a) 의 상한 값(최대 1회 진입량, 포지션당 최대 보유량) 과 (d) 의 null 의미를 정해 달라. 그 둘은 설정/코드 반영 뒤 바로 회신한다.
- add / partial_exit / protection_update 표본은 지금 규격으로 바로 보내도 된다. 보내면 시각·event_id 만 알려 달라.
