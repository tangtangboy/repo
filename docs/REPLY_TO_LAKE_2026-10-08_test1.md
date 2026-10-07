# 2026-10-08 서명 TEST 1건(event lake-test-d92cdc33-…) 수신측 대조 결과

확인 시각: 2026-10-08 03:10 KST (2026-10-07 18:10 UTC)
결론 먼저: **접수(202)·본문 해시·시각은 모두 일치**했지만, **실행(가상 체결) 은 우리 쪽 시험 환경 상태 때문에 `RECONCILE_REQUIRED` 로 거부**됐다. 송신 신호에는 문제가 없었다. 원인을 고치고 배포했으니 **새 event_id 로 TEST 1건을 다시 보내 달라.** 실주문은 0건이다(구조적으로 불가능).

## 1) 접수 대조 — 일치

| 항목 | 송신측 기록 | 수신측 기록(직접 확인) |
|---|---|---|
| 요청 | POST /lake/signal | Caddy 접속 로그 1791394051.251 = 2026-10-07 17:27:31.251 UTC, POST /lake/signal, **202**, 응답 109 bytes, 처리 5 ms, 발신 115.138.36.x |
| 발송 시각 | 17:27:30.680 UTC | 수신(접수 저장) 17:27:31.247 UTC — 네트워크·TLS 포함 약 570 ms(송신측 응답 소요 565 ms 와 일치) |
| event_id | lake-test-d92cdc33-a707-4757-8c4c-f1bf9101819b | 동일 |
| position_id / seq | lake-test-position-d92cdc33-… / 1 | 동일 / 1 |
| 본문 SHA-256 | 56939e6c991b1461661d55cbe3e8575001c269ac974240aea9390158f63b7247 | **56939e6c991b1461661d55cbe3e8575001c269ac974240aea9390158f63b7247** (수신 원본 바이트 기준, 동일) |
| mode/strategy/action/leg/idx | test / overheat / entry / short / 2 | 동일 |
| 수량·가격 | 0.001 BTC, ref 83192.0, SL 84855.84, TP [81528.16] | 동일. 수신 시점 Bybit 공개 시세: mark 83192.0 / last 83188.5 |
| 응답 | 202 accepted=true duplicate=false mode=test | 동일 |
| 재전달 | 1건, 재시도 0회 | duplicate_count 0, conflict_count 0 (ingress 기록 없음 = 거부·중복 재전달 없음) |

receipts 내보내기 원문(서버):
```
{"received_at_ms": 1791394051247, "received_at": "2026-10-07T17:27:31.247Z", "mode": "test", "event_id": "lake-test-d92cdc33-a707-4757-8c4c-f1bf9101819b", "position_id": "lake-test-position-d92cdc33-a707-4757-8c4c-f1bf9101819b", "event_sequence": 1, "strategy": "overheat", "action": "entry", "leg": "short", "position_idx": 2, "qty_btc": 0.001, "body_sha256": "56939e6c991b1461661d55cbe3e8575001c269ac974240aea9390158f63b7247", "signal_status": "rejected", "signal_reason": "RECONCILE_REQUIRED", "processed_at_ms": 1791394051259, "signal_note": "", "duplicate_count": 0, "conflict_count": 0}
```
ingress: 해당 event_id 기록 없음(정상). fills: 없음(아래 2) 참조).

## 2) 실행 결과 — 거부 (우리 쪽 원인)

- 실행기 로그 17:27:31.256 UTC: `test/bybit/entry … rejected RECONCILE_REQUIRED`. 접수 12 ms 뒤에 판정됐고 가상 주문은 내지 않았다.
- 원인: 그 시각 test 계정에 **장부 lot 합계 0.002 BTC vs 가상거래소 포지션 0.001 BTC** 불일치 플래그가 서 있었다. 수신측이 2026-10-07 15:01 UTC 와 15:11 UTC 에 보낸 자체 진단 entry 두 건을 청산하지 않은 채 15:04 UTC 에 서버를 재배포(재시작) 해서, 메모리에만 있던 가상 포지션 하나가 사라진 것이다. 불일치 상태에서는 안전장치가 모든 새 신호를 `RECONCILE_REQUIRED` 로 거부한다(실계좌라면 맞는 동작).
- 송신 신호와 무관하다. 서명·형식·시각·순번 모두 통과했고 접수 원장에 저장됐다.
- 수정(2026-10-07 18:02 UTC 배포): 서버 기동 때 장부의 열린 lot 으로 가상 포지션을 다시 만든다. 이제 재시작해도 어긋나지 않는다. 배포 뒤 대사 `consistent again` 확인, 남아 있던 진단 lot 두 건은 full_exit 로 정리해 test 계정은 **현재 포지션 0, 불일치 없음**이다.
- 이 event_id 는 장부에 `rejected / RECONCILE_REQUIRED` 로 남는다. 거부된 신호는 자동 재실행하지 않는다(접수 응답은 바뀌지 않는다).

## 3) 실주문 여부

0건. test 모드는 가상 체결기만 쓰고, 서버에 거래소 키가 없어 실거래소 객체 자체가 만들어지지 않는다(기동 로그 `live execution disabled … LIVE_DISABLED`). 이번 event 는 가상 주문조차 내기 전에 거부됐다.

## 4) 요청

- **새 event_id·position_id·현재 ts 로 TEST entry 1건 재전송.** 전송 시각(초) 과 event_id 를 알려주면 receipts/fills 를 같은 형식으로 보낸다. 이번에는 `signal_status: done` 과 fills 1행(가상 주문 `porder-…`, 체결 `pexec-…`, 수량 0.001, 체결가 = reference_price) 이 나와야 정상이다.
- 가능하면 같은 position_id 로 seq 2 `full_exit`(qty 0.001, expected 0) 도 이어서 보내 달라. 진입→청산 한 쌍이 대조 표본으로 더 낫다.
- 5절 정책(만료 경계 `>`/`>=`, 순번 건너뜀, 보호값 null, TP 배분·트리거 기준) 은 이번 TEST 와 분리해 합의하는 데 동의한다. 송신측 결정이 오면 수신측 설정으로 맞춘다.

## 5) 수신측 교훈(송신측 조치 불필요)

진단 신호는 반드시 full_exit 로 닫고, 열린 test lot 이 있는 상태에서 재시작하지 않는다. 재시작 시 가상 포지션 복원은 코드로 고정했다.
