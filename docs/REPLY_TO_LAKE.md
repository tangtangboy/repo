# lake 측 회신 — 시그널 수신 · 체결 회신 연동 답변

> 받은 자료(`01_먼저읽어주세요` / `웹훅 수신 안내` / `시그널 연동 설명서` / `lake_execution_contract.json`, 2026-10-03)에서
> 요청하신 항목에 순서대로 답합니다. 실제 시크릿·주소의 실값은 이 문서에 넣지 않습니다. `<host>` 는 대역 외로 전달합니다.

## 1. 신호 수신 주소 (lake 전용 경로)

- **`POST https://<host>/lake/signal`** — lake 전용 경로입니다(기존 ZigZag 경로와 재사용하지 않음). 전략 등록 절차는 없습니다:
  본문의 `strategy`(basic/overheat/range) 와 `position_id` 로 몫을 구분합니다.
- 발신 IP 허용 목록은 **필요 없습니다**(서명·시각으로 인증). 원하시면 lake 발신 IP 를 받아 방화벽에 추가할 수 있습니다.
- `GET https://<host>/healthz` → `{"ok":true,"halted":false,"inconsistent":{"test":false,"live":false}}` (인증 없음, 연결 확인용).
- 서버는 AWS 서울 리전, HTTPS 는 공인 인증서(Let's Encrypt). 주소는 고정 IP 기반이며 도메인으로 바뀌면 미리 알립니다.

## 2. 헤더와 검증 규칙 (수신 안내·설명서 §4 와 동일 규격)

| 항목 | 요구 |
|---|---|
| `Content-Type` | `application/json`. `Content-Encoding`(압축) 불허. 본문 ≤ 65,536 바이트 |
| `X-Signature` | `hex(HMAC-SHA256(secret, raw_body))` 소문자 64자. **전송하는 바이트 그대로** 서명(우리도 재직렬화 없이 원본 바이트로 검증) |
| `X-Timestamp` | Unix ms. **본문 `ts` 와 정확히 일치**, 수신 시각 ±60초 |
| 시크릿 선택 | 본문 `mode` 로 TEST/LIVE 키 선택. 해당 모드 키 미설정이면 503 |
| 본문 | `lake_execution_contract.json` 의 `outgoing_signal_proposal` 스키마 그대로. **정의되지 않은 필드·JSON 중복 키·NaN/Infinity 거부** |
| 의미 검증 | `exchange:"Bybit"`, `category:"linear"`, `symbol:"BTCUSDT"`; `position_idx` 1→`leg:long`, 2→`leg:short`; `protection_update` 는 `qty_btc:null`, 그 외는 양수; `expires_at_ms ≥ ts` |
| 만료 | 수신 시각 > `expires_at_ms` → 410 |
| 중복·순서 | `event_id` 영속. 같은 `event_id` 재전송은 200 duplicate(재실행 없음). `position_id` 별 `event_sequence` 가 마지막 값 이하이면 409 |
| 응답 시간 | 영속 접수 후 5초 안에 응답. 2xx 는 **접수 확인**이며 실행·체결은 회신(§5)으로만 전달 |

검증 순서: 크기/인코딩 → JSON 파싱 → mode 로 키 선택 → 서명·시각 → 스키마 → 의미 → 만료 → 영속 접수.

## 3. 응답 코드

| 코드 | 본문 | 의미 / lake 측 처리 |
|---|---|---|
| **202** | `{"accepted":true,"event_id":"…"}` | 최초 영속 접수. 실행 결과는 회신으로 |
| **200** | `{"accepted":true,"duplicate":true}` | 같은 `event_id` + 같은 본문 재수신. 재실행하지 않음 |
| **409** | `{"error":"CONFLICT","code":"EVENT_ID_CONFLICT"}` | 같은 `event_id` 다른 본문. 자동 우회 없음 → 대조 필요 |
| **409** | `{"error":"CONFLICT","code":"SEQUENCE_CONFLICT"}` | `event_sequence` 가 그 `position_id` 의 마지막 값 이하(역전·중복). 접수하지 않음 |
| **410** | `{"error":"EXPIRED","code":"EXPIRED"}` | `expires_at_ms` 경과. 재전송하려면 **새 event_id·sequence** 로 새 신호 |
| **400** | `{"error":"BAD_JSON"\|"INVALID_SIGNAL","code":…,"fields":[…]}` | 파싱/스키마/의미 오류. `code`: `PARSE_ERROR`, `DUPLICATE_KEY`, `BAD_MODE`, `SCHEMA`(필드 경로만), `SYMBOL_MISMATCH`, `POSITION_MODE_MISMATCH` |
| **401** | `{"error":"UNAUTHORIZED","code":…}` | `MISSING_SIGNATURE` / `MISSING_TIMESTAMP` / `BAD_TIMESTAMP` / `TIMESTAMP_SKEW` / `TIMESTAMP_MISMATCH` / `BAD_SIGNATURE` |
| **413** | `{"error":"PAYLOAD_TOO_LARGE","max_bytes":65536}` | 본문 크기 초과 |
| **415** | `{"error":"UNSUPPORTED_MEDIA_TYPE","code":…}` | `Content-Type: application/json` 이 없거나 다름, 또는 `Content-Encoding` 사용 |
| **503** | `{"error":"SECRET_NOT_CONFIGURED","mode":"…"}` | 그 모드의 수신 키가 우리 쪽에 미설정. 설정 후 재전송 가능 |
| 500 / 502 | — | 우리 저장 장애 / 서비스 재시작 중. 재전송 정책(§7) 적용 |

응답 본문에 시크릿·내부 예외 원문은 포함하지 않습니다. 거부 건은 코드와 본문 sha256 만 기록합니다(서명 전 거부는 횟수만).
`event_id` 의 유일성은 **mode 별**입니다(TEST 와 LIVE 가 같은 event_id 를 써도 서로 충돌하지 않습니다).

## 4. 지원하는 동작 — 모두 지원합니다

| action | 처리 |
|---|---|
| `entry` | 해당 `position_id` 에 열린 lot 이 없을 때 시장가 진입(positionIdx = 신호값). 신호의 `stop_loss`/`take_profit` 로 보호주문 생성. 이미 열려 있으면 `rejected/POSITION_EXISTS` |
| `add` | 열린 lot 에 `qty_btc` 만큼 같은 방향 시장가 추가 → lot 수량·평균가 갱신 → 보호주문 수량 재설정 |
| `partial_exit` | 열린 lot 에서 **명시 BTC 수량만** reduceOnly 시장가로 감소. lot 잔량 초과는 `rejected/QTY_EXCEEDS_LOT`. **전량 청산으로 변환하지 않음** |
| `full_exit` | **그 lot 의 잔량만** reduceOnly 시장가 청산 → 보호주문 취소 → lot 종료. 다른 전략 몫·반대 레그는 건드리지 않음 |
| `protection_update` | `protection_revision` 이 lot 의 현재 값보다 클 때만 적용(아니면 `rejected/STALE_PROTECTION_REVISION`). 기존 보호주문 전부 취소 → 새 `stop_loss`/`take_profit` 로 재생성 → `protection_updated` 회신 + 스냅샷. 재생성이 거래소 사정으로 실패하면 `error/PROTECTION_FAILED` 를 보내고 우리 쪽 대사가 자동으로 다시 만듭니다 — 이때는 **같은 revision 재전송을 멱등 재시도로 받습니다** |

- **전략별 수량 귀속(lot 원장)**: Bybit 는 같은 레그를 합산하지만, 우리는 `(mode, position_id)` 단위 lot 원장을 따로 가지고
  각 신호를 그 lot 에만 적용합니다. 30초마다 positionIdx 별 거래소 수량과 lot 합계를 대사합니다.
- **헤지**: 계정을 Bybit 헤지 모드로 고정 운용, `position_idx` 1(롱)/2(숏) 동시 보유 지원. `position_idx:0` 신호는 400.
- **분할 체결**: 체결 건마다 별도 회신(§5). **퍼센트 수량 해석은 사용하지 않습니다** — `qty_btc` 절대 수량만.
- **보호주문 구현**: 우리가 내는 **조건부 reduceOnly 시장가 주문**(트리거 기준 MarkPrice, 기본값). `take_profit` 배열이면 각
  가격에 lot 수량을 균등 분할(최소 주문 수량 미만 레벨은 건너뜀, 나머지는 마지막 레벨). `add`/`partial_exit` 뒤에는 같은
  revision 으로 수량만 재설정합니다.
- 보호주문은 전부 **reduceOnly** 이며, 우리 봇은 lake 신호 또는 보호주문 트리거 외에는 포지션을 열거나 닫지 않습니다.
  예외 하나: 보호가격이 **이미 지나 있어** 거래소가 조건부 주문을 받지 않으면(예: 롱 진입 직후 현재가가 SL 아래) 그 보호를
  즉시 reduceOnly 시장가로 실행하고 §6 의 `auto:sl`/`auto:tp` 이벤트로 회신합니다(포지션을 보호 없이 두지 않기 위해).
- 체결된 신호의 회신 상태는 체결이 결정합니다. 체결 뒤 보호주문 생성에 실패해도 같은 `event_id` 로 `rejected`/`error` 를
  보내지 않고(이미 `filled` 를 보냈으므로), 직후 스냅샷에 `stop_loss:null` 로 드러낸 뒤 30초 대사가 다시 만듭니다.
- 접수 뒤 실행이 늦어져 `expires_at_ms` 가 지난 신호(재시작·백로그)는 실행하지 않고 `rejected/EXPIRED` 로 회신합니다.

## 5. 회신 방식 (설명서 §4 수신 규격 그대로 구현)

- 전송: `POST <LAKE_REPORT_URL_{TEST|LIVE}>`, `Content-Type: application/json`, `X-Signature`/`X-Timestamp`(본문 `ts`),
  **TEST/LIVE 별도 키**(`LAKE_REPORT_SECRET_TEST/LIVE`, 각 32바이트 이상)로 서명. 본문은 계약 스키마 그대로(추가 필드 없음).
- **execution 회신 순서(신호 1건당)**: `acknowledged`(처리 시작) → `submitted`(주문 ID 확보) → 체결마다
  `partially_filled`(마지막 체결만 `filled`, `qty` = 그 체결 수량, `fill_price`, `fill_id` 필수) → 실패 시
  `rejected`/`cancelled`/`error`(`reason_code` 포함, `qty/fill_price/fill_id` null) → **스냅샷**.
  `protection_update` 는 `acknowledged` → `protection_updated`(action `protection_update`) → 스냅샷.
- **TEST 기록 전용 단계**에서는 신호당 `acknowledged` **하나만** 보냅니다(주문·체결 없음). TEST 시뮬레이션 단계(§8 2단계)에서는
  내부 모의 거래소로 체결·스냅샷까지 TEST 모드로 보냅니다. 실거래소는 TEST 에서 절대 호출하지 않습니다.
- **snapshot**: 30초마다 + 체결·보호가격 변경 직후. `complete:true`, `account_scope:"lake_dedicated_BTCUSDT"`(lake 전용 서브계정,
  BTCUSDT 전체). 전송 전 거래소 실수량과 lot 합계를 대사하고, **불일치면 스냅샷을 보내지 않습니다**(불완전 스냅샷 금지).
  `stop_loss` = 거래소에서 확인된 가격 또는 null, `take_profit` = `[]`(익절 없음 확인) / 가격 배열 / `null`(미확인),
  `mark_price` 는 Bybit 마크가격 또는 null, 각 `updated_at_ms ≤ observed_at_ms`.
- **sequence**: mode 별 1부터 증가, 재시작 뒤 유지, SQLite 에 영속. 같은 mode 는 번호 순서대로 직렬 전송하며 앞 번호가 재시도
  중이면 뒤 번호를 먼저 보내지 않습니다. `observed_at_ms` 는 같은 mode 안에서 단조 증가, `ts` 이하.
- **가명 ID**: `order_id` = `"o-" + sha256(실제 주문ID)[:24]`, `fill_id` = `"f-" + sha256(실제 체결ID)[:24]`. 같은 체결은 같은
  `fill_id` 로만 보고하며 중복 보고하지 않습니다. 계정 식별자는 보내지 않습니다.
- **reason_code**(정리된 코드만, 거래소 원문 없음): `LIVE_DISABLED` `OPERATOR_HALT` `POSITION_MODE_MISMATCH` `POSITION_EXISTS`
  `POSITION_NOT_FOUND` `POSITION_CLOSED` `OPPOSING_LEG` `QTY_EXCEEDS_LOT` `QTY_BELOW_MIN` `QTY_LIMIT` `LEG_LIMIT` `SLIPPAGE_GUARD`
  `STALE_PROTECTION_REVISION` `RECONCILE_REQUIRED` `EXPIRED` `EXCHANGE_REJECTED` `EXCHANGE_ERROR` `EXCHANGE_TIMEOUT`
  `PROTECTION_FAILED` `UNKNOWN_STATE` `STOP_LOSS_TRIGGERED` `TAKE_PROFIT_TRIGGERED`.
- `EXCHANGE_TIMEOUT`(`error`) 뒤에 거래소에서 체결이 확인되면 같은 `event_id` 로 체결(`filled`) 회신이 **늦게** 나갈 수 있습니다
  (주문을 잃어버리지 않기 위해). 그 뒤 스냅샷이 실제 잔량을 보여 줍니다.
- lake 응답 처리: 202 → 전송 완료, 200 → duplicate 로 기록, 409 → `conflict` 로 두고 운영자 알림(자동 번호 덮어쓰기 없음),
  그 외 4xx → `failed` + 알림, 5xx/타임아웃 → 재시도(최대 3회, 2초×n 백오프, **같은 바이트·ID·sequence·ts 유지**, 생성 후 50초 안에서만).
  창을 넘기면 그 번호는 결번(`failed`)으로 남기고 알림 → 다음 완전 스냅샷으로 잔량 재확인(설명서 §5 "불일치" 처리와 일치).

## 6. 거래소 트리거 체결(SL/TP) 회신 — 합의 요청

보호주문이 거래소에서 자체 트리거되면 lake 의 `event_id` 가 없습니다. 아래 **합성 event_id** 로 execution 회신을 보내겠습니다
(ID 패턴 `^[A-Za-z0-9_.:-]{1,128}$` 만족):

- 손절: **`auto:sl:<position_id>:<protection_revision>:L<lot_opened_at_ms>`**
- 익절(i 번째 레벨, 0부터): **`auto:tp:<position_id>:<protection_revision>:<i>:L<lot_opened_at_ms>`**
- `L<lot_opened_at_ms>` 는 우리 쪽 lot 인스턴스(해당 position_id 가 열린 Unix ms) 입니다. 같은 `position_id` 를 닫았다가 다시 열어
  같은 revision 으로 다시 손절되더라도 이전 이벤트와 ID 가 겹치지 않게 하기 위한 것입니다(파싱하지 않고 식별자로만 쓰시면 됩니다).
- `action` = 잔량이 남으면 `partial_exit`, 0 이 되면 `full_exit` — **첫 보고에서 정해진 값을 그 event_id 의 후속 보고에서도 유지**합니다;
  `reason_code` = `STOP_LOSS_TRIGGERED` / `TAKE_PROFIT_TRIGGERED`; 체결마다 `partially_filled`/`filled` 회신 → 스냅샷. 같은 revision 안에서
  같은 보호주문이 여러 번 부분 체결되면 같은 event_id 로 후속 보고합니다(설명서 "같은 event_id 의 후속 보고는 같은 포지션·전략·방향·동작 유지" 준수).
- `position_id` 가 매우 길어 128자를 넘으면 `sha256(position_id)[:24]` 로 축약합니다.

lake 대시보드가 이 형식을 자기 신호와 구분해 표시할 수 있는지 확인 부탁드립니다.

## 7. 합의가 필요한 항목

1. **시크릿 교환**: 대역 외(1회성 비밀 공유 링크 또는 통화로 분할 전달)로 네 값(`LAKE_SIGNAL_SECRET_TEST/LIVE` = 신호 서명,
   `LAKE_REPORT_SECRET_TEST/LIVE` = 회신 서명)을 교환. 각 32바이트 이상, 서로 다른 값. 메신저 본문·문서·소스·로그에 넣지 않음.
   교체(rotation) 시각을 양쪽이 맞춰 동시에 적용.
2. **회신 URL**: 공개 HTTPS `LAKE_REPORT_URL_TEST` / `LAKE_REPORT_URL_LIVE` 두 개(같아도 됨). 우리 발신 IP(고정)를 알려드릴 수 있습니다.
3. **유효시간 정책**: 예시의 `expires_at_ms = ts + 15s` 는 수용 가능. 단 네트워크 재전송을 고려해 **15~30초** 권장.
   만료(410)는 재전송하지 말고 **새 event_id·sequence** 로 새 판단 신호를 보내주세요. 만료는 접수 시(410)와 **실행 시점**
   (접수 뒤 지연되면 `rejected/EXPIRED` 회신) 두 번 검사하므로, 오래된 신호가 현재가로 실행되는 일은 없습니다.
   접수된 신호는 FIFO 로 실행하므로 lake 측 발신 큐가 밀리지 않게 해주세요.
4. **재시도 정책(lake → 우리)**: 비2xx/타임아웃 시 **같은 바이트·같은 event_id·같은 ts** 로 60초 서명 창 안에서 최대 2~3회
   재전송(간격 0.5~2초) 제안. 같은 바이트면 200 duplicate 로 안전합니다. ts 만 바꾸면 409 `EVENT_ID_CONFLICT` 가 납니다.
   60초가 지나면 재전송 대신 우리 회신(`acknowledged` 유무)으로 접수 여부를 판단해 주세요.
   (우리 → lake 재시도는 §5: 최대 3회, 50초 창.)
5. **`take_profit` 의 `null` 과 `[]`**: 신호에서는 **둘 다 "익절 없음"** 으로 처리합니다(기존 익절 주문이 있으면 취소). 숫자 하나는
   `[가격]` 으로, 배열은 각 가격에 균등 분할. 회신 스냅샷에서는 설명서 §4 대로 `null`=미확인 / `[]`=없음 확인으로 구분합니다.
   `stop_loss:null` 은 "손절 없음"(기존 손절 취소) 입니다. 이 해석에 동의하는지 확인 부탁드립니다.
6. **수량 반올림**: `qty_btc` 는 Bybit `qtyStep`(현재 BTCUSDT **0.001**) 배수로 **내림**하고, `minOrderQty`(0.001) 미만이면
   `rejected/QTY_BELOW_MIN`. 수량은 0.001 단위로 보내주세요. `expected_qty_btc_after` 비교도 qtyStep/2 오차로 합니다.
   추가로 우리 쪽 리스크 가드(주문 1건 상한 `max_order_qty_btc`, 레그 합계 상한 `max_leg_qty_btc`, 진입 슬리피지
   `max_entry_slippage_pct`=1.5% 대비 `reference_price`)에 걸리면 `QTY_LIMIT`/`LEG_LIMIT`/`SLIPPAGE_GUARD` 로 거부합니다.
   상한 값은 운영 시작 전 공유합니다. `reference_price:null` 이면 슬리피지 가드는 생략합니다.
7. **`expected_qty_btc_after` 불일치**: 회신 status 는 **실제 체결 그대로** 보내고(거부하지 않음), 우리 쪽에서는 운영자 알림 +
   신호 메모 `QTY_MISMATCH` 를 남깁니다. 직후 스냅샷의 `qty` 가 실제 잔량이니 lake 는 그 값을 기준으로 다음 신호의
   `qty_btc` 를 잡아 주세요. 예: 부분 체결(IOC)로 적게 들어간 경우 `cancelled` + `QTY_MISMATCH`.
8. **중단·복구**: 우리 측 HALT(운영자 중지) 중에는 신호를 202 로 접수한 뒤 `rejected/OPERATOR_HALT` 로 회신하며 기존 보호주문은
   유지합니다. 거래소 수량과 원장이 어긋나면 `entry/add` 만 `rejected/RECONCILE_REQUIRED`, 청산류는 계속 처리, 스냅샷은
   대사가 맞을 때까지 중단(lake 화면은 90초 뒤 "갱신 지연"). 서비스 재시작 중(수 초)은 502 → §7-4 재전송.
   HALT·재개·수동 대사는 사전에 알리겠습니다. lake 측 발신 중지 신호 방식(예: 전량 `full_exit` 후 중지)도 알려주세요.
9. **운영 시작 시각·발송 승인**: LIVE 발송은 §8 테스트 대조와 소액 가드 확인 후 **미래 신호부터**, 시작 시각을 문서로 합의.

## 8. 기록 전용 테스트 계획 (설명서 §6 대응)

양쪽 TEST 모드, 우리 쪽 `live.enabled=false`(LIVE 신호는 접수만 하고 `rejected/LIVE_DISABLED` 회신). 단계별로 우리 `/state` 와
lake 대시보드를 대조합니다.

| 단계 | lake → 우리 | 기대 응답 / 우리 → lake 회신 |
|---|---|---|
| 1 | 공개 주소·키·회신 URL 확정, `/healthz` 확인 | `{"ok":true,…}` |
| 2 | 정상 서명 / 잘못된 서명 / 만료 시각 / 서명 후 본문 1바이트 변경 / `X-Timestamp`≠`ts` | 202 / 401 `BAD_SIGNATURE` / 410 / 401 `BAD_SIGNATURE` / 401 `TIMESTAMP_MISMATCH` |
| 3 | (우리 쪽 자동) 빈 전체 스냅샷 | `kind:snapshot, positions:[]` — TEST 시뮬레이션 단계에서 30초마다 |
| 4 | `entry` → `add` → `partial_exit` → `protection_update` → `full_exit` (합의 예시 값, 같은 `position_id`, sequence 1..5) | 기록 전용: 각 `acknowledged`. 시뮬레이션 단계: `acknowledged`→`submitted`→`filled`→스냅샷(보유) … `protection_updated`→스냅샷 … `full_exit` 체결→빈 스냅샷 |
| 5 | 같은 신호 재전송(같은 바이트) | 200 duplicate, 회신 없음 |
| 6 | 같은 `event_id` 다른 본문 | 409 `EVENT_ID_CONFLICT` |
| 7 | `event_sequence` 역전(이전 번호) / 누락(번호 건너뜀) | 역전 409 `SEQUENCE_CONFLICT` / 건너뜀은 202 접수(순서 상승이면 허용 — 누락 감지는 lake 발신 측 책임, 필요하면 "연속 번호만 허용" 으로 바꿀 수 있음 → 합의) |
| 8 | 잘못된 수량: `qty_btc` 0 또는 음수 / `protection_update` 에 `qty_btc` 값 / lot 잔량 초과 `partial_exit` / 0.0005 | 400 `SCHEMA` / 400 `SCHEMA` / 202 후 `rejected/QTY_EXCEEDS_LOT` / 202 후 `rejected/QTY_BELOW_MIN`(시뮬레이션 단계) |
| 9 | 헤지: `position_idx:1 leg:long` 과 `position_idx:2 leg:short` 동시 보유, `position_idx:0` / idx1+short | 양 레그 lot 분리 보유 스냅샷 / 400 `POSITION_MODE_MISMATCH` / 400 `SCHEMA` |
| 10 | 미지 필드 추가 / 중복 키 / NaN | 400 `SCHEMA` / 400 `DUPLICATE_KEY` / 400 `PARSE_ERROR` |
| 11 | 우리 회신 검증(lake 측): 재전송 duplicate 200, sequence 연속, observed 단조, 체결 외 null, fill_id 중복 없음, 90초 지연 표시 | lake 가 409 를 주면 우리 쪽 `conflict` 알림으로 확인 |
| 12 | 우리 서비스 재시작 후 5 단계 재전송 / 회신 sequence 이어짐 확인 | 200 duplicate / 번호 연속 |
| 13 | TEST 회신이 lake LIVE 화면에 들어오지 않는지 | lake 측 확인 |

통과 기준: 중복이 새 매매로 처리되지 않음, 거부가 전부 코드로 구분됨, 시뮬레이션 단계에서 스냅샷 잔량 = lake 기대 잔량.
그 다음 우리 쪽 Bybit 테스트넷 → LIVE 소액 가드 순으로 진행하며, 각 단계 전환은 별도 승인으로 합니다.

## 9. 참고: 신호 서명 예시

`tools/send_signal_example.sh`(curl + openssl) 를 함께 보냅니다. 핵심은 **전송할 바이트를 먼저 확정하고 그 바이트로 서명·전송**
하는 것입니다(`--data-binary`). `python -m lake_executor sign --file signal.json --mode test` 로 우리 쪽에서도 같은 헤더를 만들어
대조할 수 있습니다.
