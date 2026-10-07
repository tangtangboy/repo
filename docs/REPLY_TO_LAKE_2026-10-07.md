# lake 인수인계(2026-10-07) 에 대한 수신측 회신 — 12절 양식

확인 시각: 2026-10-08 00:05 KST (2026-10-07 15:05 UTC)
작성: 수신측(주문 실행 서버) 개발 환경 AI. 아래 "직접 확인" 은 서버 로그·코드·테스트·실제 요청으로 확인한 사실이고, "보고" 는 송신측 문서 내용이다.
비밀키·서명값·거래소 키는 포함하지 않는다.

## 1) 실행 중인 수신 주소/경로

- `POST https://3-39-172-119.sslip.io/lake/signal` (lake 전용 경로), `GET https://3-39-172-119.sslip.io/healthz`
- 서버: AWS 서울 리전 EC2, Caddy HTTPS(Let's Encrypt 공인 인증서). 서비스 최초 기동 2026-10-06 16:03 UTC (2026-10-07 01:03 KST) — 서버 로그로 직접 확인(송신측 보고와 일치).
- 2026-10-08 00:05 KST 기준 `healthz` = `{"ok":true,"halted":false,"inconsistent":{"test":false,"live":false},"protection_missing":{"test":0,"live":0}}`.
- 오늘(2026-10-07) 장부 구조 변경과 재배포가 있었다. 변경 중 두 차례 짧은 재시작(각 10초 안팎) 외에는 계속 수신 상태였다.

## 2) 2026-10-07 17:52:47 KST 의 401 요청 로그

직접 확인 — Caddy 접속 로그(`/var/log/caddy/access.log`):

| 항목 | 값 |
|---|---|
| 수신 시각 | 1791363168.013 = 2026-10-07 08:52:48.013 UTC = 17:52:48.013 KST |
| 요청 | `POST /lake/signal` |
| HTTP 상태 | 401 |
| 응답 크기 | 121 bytes |
| 발신 IP | 115.138.36.x (마지막 옥텟은 생략; 필요하면 담당자 간 직접 전달) |
| User-Agent | Python/3.10 aiohttp/3.13.5 |

직접 확인 — 애플리케이션 로그(`state/lake-executor.log`): `2026-10-07 08:52:48,011 INFO signal rejected (pre-auth) http=401 code=MISSING_SIGNATURE`.

대조 결과: 송신 원장의 attempt 08:52:47.844 UTC → 수신 08:52:48.011 UTC (약 170 ms), 경로·상태·응답 본문 형식 모두 송신측 기록과 일치한다. 이 시각 전후로 `/lake/signal` 에 다른 요청은 없다(송신측 "1건" 과 일치).

event_id 기록 여부: **서명 전 거부(401 등) 는 event_id 를 DB·앱 로그에 남기지 않는다.** 인증 없는 요청이 디스크·락을 소모하지 못하게 메모리 카운터로만 세고, 응답 본문에 본문의 event_id 를 되돌려 주기만 한다(송신측이 받은 `"event_id":"lake-unsigned-probe-…"` 가 그것). 따라서 이 401 은 Caddy 접속 로그 + 앱 로그의 시각/경로/상태로 대조했고, 원장(수신 DB) 에는 당연히 행이 없다. 서명을 통과한 뒤의 거부/중복(409/410/400/200-duplicate) 은 event_id·본문 sha256 과 함께 DB 에 남고 9) 의 ingress 내보내기로 받을 수 있다.

## 3) TEST 주문 차단과 TEST/LIVE 분리

직접 확인(코드):
- 실행기는 `mode` 별로 거래소 객체를 따로 둔다(`main._exchanges_for`). `test` 모드는 `test.simulate_fills=true` 일 때 **가상 체결기(PaperExchange)** 만 쓰고, 이 객체는 거래소 API 를 한 번도 호출하지 않는다(pybit 세션 자체가 없다). 실거래소 객체는 `live` 모드에서만, 그것도 `live.enabled=true` + 실키 + LIVE 시크릿이 모두 있을 때만 만들어진다.
- 현재 서버: `live.enabled=false`, Bybit 실키 미입력 → 기동 로그 `live execution disabled for account bybit (LIVE_DISABLED): live signals will be rejected`. 즉 live 거래소 객체가 존재하지 않아 어떤 신호도 실주문으로 갈 수 없다.
- 원장은 모든 테이블이 `(mode, …)` 키로 분리(신호·실행·lot·주문·체결·회신·위치 순번). 회신 sequence 도 mode 별. TEST 와 LIVE 시크릿도 별개(`LAKE_SIGNAL_SECRET_TEST` / `_LIVE`).

직접 확인(테스트, 2026-10-07): 전체 414 passed. 특히 `tests/test_executor_paper.py`(entry→add→partial_exit→protection_update→full_exit 전 과정이 가상 체결기에서만 진행), `test_live_signal_with_live_disabled_is_rejected_live_disabled`, `tests/test_receiver.py`(mode 별 시크릿·응답에 비밀값 없음).

직접 확인(실요청): 2026-10-07 14:30 UTC 와 15:01 UTC 에 서명된 TEST 신호(entry→full_exit, entry 중복) 를 실제 주소로 보냈다. 가상 체결만 기록됐고 거래소 주문·계좌 변경은 0건이다(거래소 키가 없으므로 구조적으로 불가능).

## 4) TEST 공유키

- 설정됨: `LAKE_SIGNAL_SECRET_TEST` — 43 bytes, 공백 없음, 따옴표 없음, hex 형태 아님. **문자열의 UTF-8 바이트를 그대로 HMAC 키로 쓴다**(hex 디코딩 없음). 우리 쪽 최소 길이 규칙도 32 bytes 이상이라 송신측 요구(32 bytes 이상·공백 없음) 와 맞는다.
- 전달 방법: 운영자(서버 소유자) 가 비공개 채널로 송신측 담당자에게 직접 전달한다(대면/비밀번호 관리자 공유/종단간 암호화 메신저). 채팅 로그·문서·소스·대시보드 화면에는 넣지 않는다. 수신측에서 키를 바꿔야 하면 대시보드 Secrets 페이지에서 교체 후 재시작한다(값은 항상 마스킹).
- 키 값은 이 회신에 없다.

## 5) 21개 필드 수용 여부와 차이점

21개 필드 전부 수용한다. 스키마는 `extra=forbid`(정의되지 않은 필드 거부), NaN/Infinity 거부. 차이·주의점:

| 항목 | 수신측 동작 |
|---|---|
| 정수 필드 `event_sequence`, `ts`, `expires_at_ms`, `protection_revision` | strict: 문자열("123")·boolean 거부 → 400 (2026-10-07 반영). 본문 `ts` 가 문자열이면 서명 단계의 본문/헤더 시각 대조에서 먼저 401 `TIMESTAMP_MISMATCH` |
| 실수 필드 `qty_btc`, `expected_qty_btc_after`, `reference_price`, `stop_loss`, `take_profit[]` | 숫자만(정수도 허용), 문자열·boolean·NaN·Infinity 거부 |
| `event_id`, `position_id` | `^[A-Za-z0-9_.:-]{1,128}$` — 송신측 규칙과 동일 |
| `exchange` | `"Bybit"` (대소문자 정확히; `"bybit"` 는 400). `category` `"linear"`. `symbol` 은 1~32자 문자열이며 계정 심볼(BTCUSDT) 과 대조 |
| `strategy_name` | 1~128자 자유 문자열. `"lake(가명) strategt"` 철자 그대로 수용 |
| `strategy` | `basic` \| `overheat` \| `range` |
| `position_idx` | 0 \| 1 \| 2. 헤지 계정에서 1↔long, 2↔short 불일치는 거부. 우리 계정은 **헤지 모드**로 운영하므로 1/2 를 쓴다(0 은 계정이 단방향일 때만) |
| `take_profit` | 가격 배열 또는 null. 단일 숫자도 받아 1단계 배열로 해석. **빈 배열 `[]` 은 null 과 같이 "TP 없음"** 으로 처리 |
| `qty_btc` | BTC 수량 × 계정 배수(`qty_multiplier`, 현재 1.0) 를 거래소 수량 단위(0.001) 로 **내림**. 최소 수량 미만이면 `QTY_BELOW_MIN` 거부. 가드: 주문당 0.05 BTC, leg 합계 0.2 BTC 초과 거부, entry 는 `reference_price` 대비 1.5% 초과 슬리피지면 거부 |

## 6) 서명 원문·UTF-8 키·밀리초·15초 만료

- 서명 대상은 **수신한 본문 바이트 그대로**. 재직렬화·정렬·들여쓰기 없이 원본 바이트로 HMAC-SHA256 을 계산해 `X-Signature`(소문자 hex 64자) 와 상수시간 비교한다.
- 키는 UTF-8 원문 바이트(4 참조).
- `X-Timestamp` 는 Unix ms 정수이고 **본문 `ts` 와 정확히 같아야** 하며 수신 시각과 ±60,000 ms(설정 `max_clock_skew_ms`) 안이어야 한다. 아니면 401 `TIMESTAMP_SKEW` / `TIMESTAMP_MISMATCH`.
- 만료: `수신 시각(ms) > expires_at_ms` 이면 만료(같으면 유효). 15초 TTL 그대로 수용한다. 단 **행동에 따라 처리가 다르다**: `entry`/`add` 는 410 `EXPIRED` 로 거부하고 실행하지 않는다. `partial_exit`/`full_exit`/`protection_update` 는 만료됐어도 **202 로 접수하고 실행**한다(끊김 뒤 늦게 도착한 청산·보호가격 변경을 버리는 것이 더 위험하다고 봐서; 실행 기록에 `stale(executed after expires_at_ms)` 로 남는다). 이 정책은 설정 `guards.expired_actions_execute` 로 바꿀 수 있다(빈 리스트면 전부 410). 송신측이 "만료면 전부 거부" 를 원하면 알려 달라.

## 7) 정상/중복/오류 응답 예제 (실제 서버 응답, 2026-10-07 15:01 UTC)

송신측 제안 형식을 그대로 반영했다(2026-10-07 배포).

```
202  {"accepted":true,"duplicate":false,"mode":"test","event_id":"dup-1791385263879-entry-1"}
200  {"accepted":true,"duplicate":true,"mode":"test","event_id":"dup-1791385263879-entry-1"}      (같은 ID·같은 바이트 재전달)
409  {"error":"CONFLICT","code":"EVENT_ID_CONFLICT"}                                           (같은 ID·다른 바이트)
409  {"error":"CONFLICT","code":"SEQUENCE_CONFLICT"}                                            (같은 position_id 에서 순번이 마지막 값 이하)
410  {"error":"EXPIRED","code":"EXPIRED","event_id":"..."}                                        (entry/add 만료)
401  {"error":"UNAUTHORIZED","code":"MISSING_SIGNATURE","event_id":"..."}                        (서명 전 거부; code 는 MISSING_SIGNATURE | MISSING_TIMESTAMP | BAD_TIMESTAMP | TIMESTAMP_SKEW | TIMESTAMP_MISMATCH | BAD_SIGNATURE)
400  {"error":"BAD_REQUEST","code":"BAD_JSON" | "BAD_MODE" | "SCHEMA" | ...}                     (형식·필드)
413 PAYLOAD_TOO_LARGE(65,536 bytes 초과), 415 CONTENT_TYPE_NOT_JSON / CONTENT_ENCODING_NOT_ALLOWED
503  {"error":...,"code":"SECRET_NOT_CONFIGURED"}  또는 "LEDGER_UNAVAILABLE" (+ Retry-After; 재전송 대상)
```

- 2xx 는 **영속 저장(DB 커밋) 이 끝난 뒤** 반환한다. 2xx 는 접수 확인이고 체결 완료가 아니다.
- 응답에 공유키·서명·거래소 키를 넣지 않는다(테스트 `test_responses_never_expose_secrets`).
- 재시작 뒤에도 중복 방지는 유지된다(event_id·본문 sha256 이 장부에 영속). 타임아웃으로 접수 여부가 불명하면 **같은 바이트를 다시 보내면 된다**: 접수됐으면 200 duplicate, 안 됐으면 202 — 어느 쪽도 이중 실행은 없다.

## 8) 추가·부분 청산·전량 종료·보호 갱신·헤지 지원과 미확정 정책

지원(전부 구현·테스트됨):
- `entry`: 새 lot(position_id 별). `expected_qty_btc_after` 는 실제 체결 뒤 lot 수량과 대조해 기록.
- `add`: 같은 position_id·leg 에 증분. 다른 leg/idx 면 거부.
- `partial_exit`: reduceOnly 로 qty_btc 만 감소. lot 보다 크면 `QTY_EXCEEDS_LOT` 거부(잔량은 0 보다 커야 함).
- `full_exit`: 그 position_id 에 귀속된 lot 잔량만 reduceOnly 청산. 같은 심볼의 다른 전략/반대 방향은 건드리지 않는다(전략별 lot 원장 분리).
- `protection_update`: `qty_btc` null 필수. **`protection_revision` 이 lot 의 현재 값보다 커야** 적용(같거나 작으면 `STALE_PROTECTION_REVISION` 거부; 같은 revision 재전송은 직전 보호주문 생성이 실패해 비어 있을 때만 재시도로 수용). 적용되면 기존 보호주문을 취소하고 본문의 `stop_loss`/`take_profit` 으로 **전체를 새로 만든다** — 즉 null 은 "해당 보호 없음"(기존 SL 또는 TP 취소) 이다. 유지하려면 기존 값을 다시 보내야 한다.
- 헤지: position_idx 1/2 와 leg 일치 검증, long·short 동시 보유 가능. 전략(basic/overheat/range) 이 같은 BTCUSDT 를 써도 position_id 별로 따로 귀속·청산한다.

미확정(송신측 확인/합의 필요):
- 순번 **건너뜀**(1 → 3): 현재는 거부하지 않고 접수한다(역전·동일만 409). 건너뜀을 거부하길 원하면 알려 달라.
- `stop_loss`/`take_profit` null 의 의미: 위처럼 "없음" 으로 처리 중. "미제공=유지" 로 바꾸려면 별도 필드가 필요하다.
- `take_profit` 가격별 **수량 배분 규칙**은 우리 쪽 구현 규칙을 따른다(배열만으로 비율을 추정하지 않음). 규칙 상세는 LIVE 전에 따로 적어 보낸다.
- 보호주문 트리거 가격 기준은 MarkPrice(설정) 이다.

## 9) 수신 원장 내보내기와 실제 체결 ID 연결

구현됨(2026-10-07). CLI 와 대시보드 두 경로, JSON Lines/CSV. 기간은 UTC, `--since`(포함)/`--until`(제외) 는 `YYYY-MM-DD`, `YYYY-MM-DDTHH:MM`, Unix ms.

```
python -m lake_executor export --kind receipts --mode test --since 2026-10-07 --format jsonl
python -m lake_executor export --kind ingress  --since 2026-10-07 --format jsonl
python -m lake_executor export --kind fills    --mode test --since 2026-10-07 --format csv
python -m lake_executor export --kind signals  --mode test --format csv        # 라이브 신호 로그 + 우리 실행/체결 집계(백테스트 입력)
```
대시보드: `/ui/signal-log` 의 receipts CSV / fills CSV / ingress CSV 링크(로그인 필요).

receipts(수신 원장: 서명 통과 후 접수된 event_id 1행):
`received_at_ms, received_at, mode, event_id, position_id, event_sequence, strategy, action, leg, position_idx, qty_btc, body_sha256, signal_status(accepted|processing|done|rejected|error), signal_reason, processed_at_ms, signal_note, duplicate_count, conflict_count`
- `body_sha256` 은 수신한 원본 바이트의 SHA-256 → 송신 원장의 `body_sha256` 과 직접 비교.
- `duplicate_count`/`conflict_count` = 같은 event_id 로 다시 온 횟수(같은 바이트 / 다른 바이트).

ingress(서명 통과 뒤 거부·중복 기록; 서명 전 거부는 Caddy 접속 로그로만):
`id, received_at_ms, received_at, code(DUPLICATE|EVENT_ID_CONFLICT|SEQUENCE_CONFLICT|EXPIRED|SCHEMA…), http, event_id, body_sha256, note`

fills(주문·체결 원장, 체결 1행; 한 신호에 여러 체결 가능):
`mode, account, event_id, position_id, strategy, leg, position_idx, action, purpose, side, reduce_only, order_link_id, order_id, order_status, order_qty, exec_id, exec_qty, exec_price, exec_time_ms, exec_time, fee, fee_currency, exec_type`
- 연결: `event_id` → `order_link_id`(event_id 에서 결정적으로 만든 주문 식별자) → `order_id`(거래소) → `exec_id`(거래소 체결). 보호주문(SL/TP) 체결은 신호 event_id 가 아니라 lot(position_id) 에 귀속된다.
- `fee`/`fee_currency`/`exec_type` 은 LIVE 에서 거래소 체결 히스토리 API 를 적재한 뒤 채워진다(수수료·실현손익·펀딩은 별도 테이블로도 보관). TEST 가상 체결은 `exec_id` 가 `pexec-…` 이고 fee 는 null — 실제 체결과 섞이지 않는다.

실제 샘플(서버, TEST):
```
{"received_at_ms": 1791385264044, "received_at": "2026-10-07T15:01:04.044Z", "mode": "test", "event_id": "dup-1791385263879-entry-1", "position_id": "dup-1791385263879-pos", "event_sequence": 1, "strategy": "overheat", "action": "entry", "leg": "short", "position_idx": 2, "qty_btc": 0.001, "body_sha256": "a7bf8487570c294a5badd3b8c14b96028a0ae846ad328fb8a8ead29fa2839e87", "signal_status": "done", "signal_reason": null, "processed_at_ms": 1791385264152, "signal_note": "", "duplicate_count": 1, "conflict_count": 1}
{"id": 1, "received_at_ms": 1791385264074, "received_at": "2026-10-07T15:01:04.074Z", "code": "DUPLICATE", "http": 200, "event_id": "dup-1791385263879-entry-1", "body_sha256": "a7bf8487570c294a5badd3b8c14b96028a0ae846ad328fb8a8ead29fa2839e87", "note": "http=200"}
{"id": 2, "received_at_ms": 1791385264113, "received_at": "2026-10-07T15:01:04.113Z", "code": "EVENT_ID_CONFLICT", "http": 409, "event_id": "dup-1791385263879-entry-1", "body_sha256": "265c3240a5ea2068190de869913dd5267cd18cdd5be7c03e3f8324e342d811ab", "note": "http=409"}
```

사후 대조 절차(9절) 는 그대로 따를 수 있다: 대상 event_id 집합을 송신 ts 로 고르고, receipts 의 body_sha256·판정·시각, ingress 의 재전달 기록, fills 의 체결을 event_id 로 붙인다. 실시간 체결 회신 URL 은 요구하지 않는다(우리 회신기는 URL 이 없으면 `unsent` 로만 기록).

## 10) 정상 서명 TEST 준비 완료 여부와 시각 협의

준비 완료. 서버는 24시간 수신 중이고 TEST 키가 설정돼 있다. 절차: 운영자가 키를 전달 → 송신측이 새 event_id/position_id·현재 ts·TTL 15초로 TEST 1건 전송 → 송신측이 전송 시각(초 단위) 과 event_id 를 알려주면 수신측이 Caddy 로그·receipts 내보내기로 202/200·body_sha256 일치·가상 체결·실주문 0건을 즉시 회신한다. 가능한 시각: 언제든. 수신측 처리 시간은 수신 → 가상 체결 약 30 ms, 완료까지 약 100 ms(2026-10-07 실측).

## 11) 남은 작업 / 미검증 항목

- 정상 서명 외부 TEST 202 확인: 송신측에서 아직 0건(키 전달 대기). 수신측 자체 서명 TEST 는 통과.
- 8) 의 미확정 정책 3가지(순번 건너뜀, 보호값 null 의미, TP 수량 배분) 합의.
- LIVE: 거래소 키 미입력, `live.enabled=false` 유지. LIVE 전환은 별도 승인·절차(키·사이징·계좌 모드·보호주문·중지/복구) 뒤에만.
- 체결 회신 URL(`LAKE_REPORT_URL`) 미설정 — 일방 송신 결정에 따라 불필요. 필요해지면 설정만으로 켤 수 있다.
- 서명 전 거부 요청의 event_id 는 기록하지 않는 설계 유지(접속 로그로 대조). 바꾸길 원하면 협의.
- 외부 DB 리전 이전 예정 — 매매 경로와 무관(장부는 서버 안, 외부 DB 는 비동기 복제본).

"200/202 확인" 과 "실제 체결 확인" 은 별개로 적었다. 실행하지 않은 시험을 통과로 적지 않았다.
