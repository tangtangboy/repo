# 2026-10-09 TEST 자동 송신 활성화에 대한 수신측 회신

확인 시각: 2026-10-09 02:35 KST (2026-10-08 17:35 UTC)

## 1) 수신 상태

- 수신 중, HALT 아님, test 계정 open lot 0, 가상 포지션 없음, 불일치 플래그 없음, 복제본 동기화 정상.
- 활성화 기준 시각 2026-10-08 17:18:00 UTC 이후 17:32 UTC 까지 `/lake/signal` 요청 **0건**(Caddy 접속 로그 기준). 송신측의 "활성화 확인 당시 0건" 과 일치. 전략 신호가 나오면 그때부터 쌓인다.
- 자동 event_id 접두 `lake-test-event-` 는 id 규칙(`^[A-Za-z0-9_.:-]{1,128}$`) 에 맞는다. 본문·서명 방식 변경 없음 확인.

## 2) 대조 방법 (활성화 시각 이후, mode=test)

CLI(서버):
```
python -m lake_executor export --kind receipts --mode test --since 2026-10-08T17:18 --format jsonl
python -m lake_executor export --kind fills    --mode test --since 2026-10-08T17:18 --format jsonl
python -m lake_executor export --kind ingress  --since 2026-10-08T17:18 --format jsonl
```
대시보드(로그인 뒤, 같은 데이터의 CSV):
```
https://3-39-172-119.sslip.io/ui/signal-log.csv?kind=receipts&mode=test&since=2026-10-08T17:18
https://3-39-172-119.sslip.io/ui/signal-log.csv?kind=fills&mode=test&since=2026-10-08T17:18
https://3-39-172-119.sslip.io/ui/signal-log.csv?kind=ingress&since=2026-10-08T17:18
```
- 붙이는 키: 접수 원장 = event_id + body_sha256, 포지션 변화 = position_id + event_sequence, 체결 = event_id → order_link_id → order_id → exec_id.
- 각 행의 `signal_status`(accepted|processing|done|rejected|error) 와 `signal_reason`, fills 의 side/reduce_only/exec_qty/exec_price/exec_time 으로 수량·방향·시각을 확인한다. `signal_note` 에 `QTY_MISMATCH` 가 있으면 그 단계의 lot 잔량이 expected_qty_btc_after 와 달랐다는 뜻(주문은 실행됨).
- ingress 에 `EXPIRED`(entry/add 가 15초 TTL 을 넘어 도착) 나 `SEQUENCE_CONFLICT`(순번 역전) 가 보이면 그 사건은 실행되지 않은 것이다. 만료된 partial_exit/full_exit 는 실행되고 run note 에 `stale` 이 남는다.
- 기간은 UTC. `--since` 포함 / `--until` 제외. 한 신호의 체결이 기간 밖이어도 event_id 로 붙는다.

## 3) 수신측 운영 안내

- 처리 시간: 접수 → 가상 체결 약 10 ms, 완료 약 30 ms. 신호가 몰려도 순서대로 처리한다(단일 실행기).
- 송신측 PC 절전/종료로 끊겼다 재개돼도 수신측은 그대로다. 재개 뒤 첫 신호가 그 포지션의 다음 순번이면 정상 처리되고, 과거 순번이면 409 로 거부된다. 포지션이 열린 채로 송신이 멈추면 수신측 가상 포지션도 열린 채 남는다(실주문 아님). 정리가 필요하면 알려 달라.
- 주기적 내보내기는 요청 시 즉시, 또는 위 링크로 송신측이 직접 받을 수 있다(대시보드 토큰은 수신측 운영자가 관리).

## 4) 변경 없음 확인

null 의미·TP 배분·트리거 기준·상한(0.05 / 0.2)·만료 정책 모두 현재 상태 유지. LIVE 비활성 유지.
