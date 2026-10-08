# 2026-10-09 동적 청산 방식 4단계 TEST(position lake-test-lifecycle-position-84615ed5-…) 수신 대조 결과 — 정상

확인 시각: 2026-10-09 02:00 KST (2026-10-08 17:00 UTC)
결론: **4건 모두 접수(202)·본문 해시 일치·가상 체결 완료.** lot 잔량은 0.002 → 0.003 → 0.002 → 0 으로 진행해 최종 **closed, qty 0**. 고정 SL/TP 보호주문 **0건**(entry 의 stop_loss/take_profit null 이므로 만들지 않음). 실주문 0건.

## 1) event 별 대조 (직접 확인)

| # | action | seq | qty | expected_after | 접수(UTC) | Caddy 202 | body_sha256 (수신 원본) | signal_status |
|---|---|---|---|---|---|---|---|---|
| 1 | entry | 1 | 0.002 | 0.002 | 16:24:54.719 | 16:24:54.723, 5 ms | 1ad2c1387439a0e460701389056dbb0aedc351bda77e9fba50b1bbc76671b7d8 | done |
| 2 | add | 2 | 0.001 | 0.003 | 16:24:54.934 | 16:24:54.938, 4 ms | 818b4f68aa3b925a0e1728626acb26fafb0ec9611e5f61c3c4aeea06d1a9f824 | done |
| 3 | partial_exit | 3 | 0.001 | 0.002 | 16:24:55.145 | 16:24:55.148, 4 ms | 749219e403e04df14e06bfb3ae6115de7acd4e600f493c3b87c28f3bd59fbdc8 | done |
| 4 | full_exit | 4 | 0.002 | 0 | 16:24:55.316 | 16:24:55.319, 4 ms | 1fa486894feee237cf3dc32c4f22c08c01f6b39559d95e6798ccb956932f2090 | done |

- 네 해시 모두 송신측 값과 동일. signal_reason 없음, note 없음(→ `QTY_MISMATCH` 없음: 각 단계 실행 뒤 lot 잔량이 expected_qty_btc_after 0.002 / 0.003 / 0.002 / 0 과 일치했다는 뜻).
- 재전달 없음(duplicate_count 0, conflict_count 0, ingress 기록 없음). 처리 시간: 접수 → 완료 25~28 ms.
- 수신 시점 공개 시세 mark 81250.8 (4건 모두 같은 초 안).

## 2) fills (가상, 시간순)

| # | 주문 | 방향 / reduceOnly | 수량 @ 가격 | 체결 | 시각(UTC) |
|---|---|---|---|---|---|
| 1 | porder-179cc674-7 (entry) | Sell / 0 | 0.002 @ 81246.5 | pexec-179cc674-5 | 16:24:54.727 |
| 2 | porder-179cc674-8 (add) | Sell / 0 | 0.001 @ 81294.9 | pexec-179cc674-6 | 16:24:54.942 |
| 3 | porder-179cc674-9 (exit) | **Buy / 1** | 0.001 @ 81294.9 | pexec-179cc674-7 | 16:24:55.153 |
| 4 | porder-179cc674-10 (exit) | **Buy / 1** | 0.002 @ 81294.9 | pexec-179cc674-8 | 16:24:55.327 |

order_link_id: lk2fd5e4af1339d9dfb53ecc273ed3257f / lk8a223a46db4e3263acc6243c798a281d / lk3b8ad82525365f84fd8b7ffa54ef8aaf / lk463c3fb60b7834652eaa45d0805c2f9c (event 순).
체결가 = 각 신호의 reference_price (test 모드 모의 시세). 수수료 0(가상).

## 3) lot 최종 상태와 보호주문

- lot: **status closed, qty 0.0**, avg_entry 81262.633… (= (0.002×81246.5 + 0.001×81294.9)/0.003), stop_loss null, take_profit null, protection_revision 1, closed_at 16:24:55.337 UTC, last_event_id = 4번 full_exit.
- 이 position 의 주문은 위 4건뿐이다. **sl/tp 주문 0건** — entry 가 null 로 왔으므로 보호주문을 만들지 않았고, 취소할 것도 없었다.
- 가상거래소 포지션 없음, test 계정 open lot 0, 불일치 플래그 없음, protection_missing 없음.
- 실주문: 0건(거래소 키 없음, test 는 가상 체결기만).

## 4) 송신측 규칙에 대한 확인

- `expected_qty_btc_after` 를 "전송 수량 누적(각 단계 내림 후 합)" 으로 보내는 방식은 수신측 lot 계산과 정확히 같다(lot 도 단계별 내림 수량의 합). 그대로 쓰면 `QTY_MISMATCH` 가 나지 않는다. 총량 0.057632… 를 한 번에 내린 0.057 과 달라지는 것도 맞다.
- 상한은 지금대로 유지한다: entry/add 1회 0.05 BTC, leg 합계 0.2 BTC (관측 최대 1회 0.028, 포지션 최대 0.0576 이면 여유 있음). 초과는 송신측에서 차단한다고 했으니 수신측에서는 `QTY_LIMIT`/`LEG_LIMIT` 거부가 안전망으로만 남는다.
- 동적 청산 방식(고정 SL/TP 없이 partial_exit/full_exit 로 청산) 은 이번 4단계로 검증됐다. null 보호값의 의미 변경은 보류로 알겠다. 현재 규격에서 entry/add 의 null = 보호주문 없음, protection_update 는 보내지 않으면 아무 일도 없다.
- 실전 연결 범위 entry/add/partial_exit/full_exit 만으로 충분하며, 익절·손절·기타 청산의 구분(송신 측 원본 사유) 은 우리 쪽에 전달되지 않아도 동작에 영향이 없다. 사후 대조 때 event_id 로 붙이면 된다.

## 5) 다음

- 송신측 검증 항목(만료·역순·소수 잔량·재시작·실시간 동등성) 중 수신측이 받아 줄 표본이 있으면 지금 규격으로 바로 보내도 된다: 만료 entry(410 EXPIRED 기대), 만료 full_exit(202 + 실행, note stale 기대), 역순 seq(409 SEQUENCE_CONFLICT 기대), 같은 바이트 재전달(200 duplicate 기대). 보내면 시각·event_id 만 알려 달라.
- TEST 자동 송신 활성화 시점을 알려 주면 그 시각부터 receipts/fills 를 주기적으로 내보내 대조한다.
