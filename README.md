# lake-executor — lake 웹훅 시그널 → 거래소 자동 실행기 (Bybit · OKX · Toobit)

lake(전략 측)가 **HTTPS POST 웹훅**으로 보내는 매매 신호(entry / add / partial_exit / full_exit / protection_update)를
받아 **서명·시각·만료·중복·순서 검증 → 영속 접수** 한 뒤, 단일 워커가 설정된 **계정 목록**(Bybit USDT 무기한 BTCUSDT,
OKX BTC-USDT-SWAP, Toobit BTC-SWAP-USDT, 모두 헤지 모드)에 시장가 주문과 보호주문(SL/TP)을 넣고, **체결 회신(execution)** 과
**30초 전체 포지션 스냅샷(snapshot)** 을 lake 회신 URL로 HMAC 서명해 돌려보낸다.

고정 계약은 `docs/ARCHITECTURE.md`(v0.1, Bybit 단일) + `docs/ARCHITECTURE_MULTI_EXCHANGE.md`(v0.2, 계정 목록),
거래소 API 매핑 근거는 `docs/exchange_api_notes.md`, 상대 측 규격은 `docs/lake_handoff/`. 운영 절차는 `docs/RUNBOOK.md`.

## 흐름

```
lake ──POST /lake/signal (X-Signature / X-Timestamp)──▶ receiver.py ─persist─▶ store.signals(accepted)
       ◀── 202 accepted | 200 duplicate | 409/410/400/401/413/415/503                   │
                                                                                         │ claim (FIFO)
                                          executor.py (단일 워커) ◀───────────────────────┘
                                             │  게이트: mode / live.enabled / HALT / RECONCILE_REQUIRED / 수량·슬리피지 가드
                                             │  라우팅: routing(fanout|by_exchange) → enabled 계정 목록 → 계정마다 실행
                                             │  exchange.py (Bybit | Paper) · exchange_okx.py · exchange_toobit.py
                                             │  (mode, account, position_id) lot 원장, fills, 보호주문 생성·재설정·취소
                                             ▼
                                          reporter.py ─allocate(seq per mode·account)─▶ store.reports(pending) ─deliver─▶ lake 회신 URL
                                          snapshot loop(30s, (mode, account) 마다): executor.reconcile → reporter.snapshot
```

- 2xx 는 **접수 확인**일 뿐이다. 실행 결과(submitted / filled / rejected …)는 회신(execution report)으로만 전달된다.
- 과거·재전송 신호는 새 매매로 처리하지 않는다 (`event_id` 영속, `position_id` 별 `event_sequence` 검증).
- `full_exit` 은 **그 lot 잔량만** 청산한다. 심볼 전량 청산으로 확대하지 않는다.
- 실행기는 수량을 **항상 BTC** 로 다룬다. 계약 수(OKX `sz`, Toobit `quantity`) 변환은 거래소 래퍼 안에서만 한다.

## 모듈

| 파일 | 역할 |
|---|---|
| `lake_executor/receiver.py` | FastAPI 수신기: `POST /lake/signal`, `GET /healthz`, 관리 엔드포인트(`/state`, `/admin/*`) |
| `lake_executor/auth.py` | HMAC-SHA256 서명 검증/생성 (원본 바이트 그대로, 재직렬화 금지) |
| `lake_executor/schemas.py` | 신호 스키마(pydantic, 미정의 필드 거부; `exchange` ∈ Bybit/OKX/Toobit), 상태·reason_code 열거형 |
| `lake_executor/store.py` | SQLite 원장(schema v2): signals / signal_runs / lots / orders / fills / reports / ingress_log / meta — lots·orders·fills·reports 는 계정 단위. v0.1 DB 는 열 때 자동 마이그레이션(account='bybit') |
| `lake_executor/executor.py` | 단일 워커: 게이트 → 라우팅 → 계정별 액션 처리 → 체결 수집 → 보호주문 → 대사(reconcile) |
| `lake_executor/exchange.py` | 공통 인터페이스 `ExchangeBase`, `BybitExchange`(pybit v5), `PaperExchange`(메모리 시뮬레이터), `build_exchange(account)` |
| `lake_executor/exchange_okx.py` | `OkxExchange` — python-okx(Trade/Account/PublicData/MarketData), 계약 수 `sz` ↔ BTC 변환 |
| `lake_executor/exchange_toobit.py` | `ToobitExchange` — 자체 httpx 클라이언트(ccxt `toobit.sign` 과 동일 서명), 계약 수 ↔ BTC 변환, 포지션 단위 TP/SL |
| `lake_executor/reporter.py` | execution / snapshot 회신 생성(계약 스키마), (mode, account) 별 sequence 직렬 전송, 재시도 |
| `lake_executor/ops.py` | 로깅(시크릿 마스킹), 알림(텔레그램), HALT 파일 |
| `lake_executor/config.py` | `config.json`(비밀 아님) + `.env`(비밀) 로더/검증, `accounts[]`/`routing` |
| `lake_executor/main.py` | CLI: `serve` / `check` / `simulate` / `sign` |
| `deploy/` | AWS EC2 + Caddy(HTTPS) + systemd 배포 스크립트 (`deploy/README.md`) |
| `tools/send_signal_example.sh` | 상대 측 참고용 서명·전송 예시 |

## 빠른 시작 (로컬)

```bash
python3 -m venv .venv && . .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt

cp config.example.json config.json                   # 비밀 아님. live.enabled=false, bybit 만 enabled 로 시작
cp .env.example .env                                 # 비밀. 시크릿은 각 32바이트 이상
# .env 에 최소 LAKE_SIGNAL_SECRET_TEST 를 채운다 (거래소 키 없이도 TEST 는 동작)

python -m lake_executor check                        # 설정·키 유무·거래소 읽기 전용 연결 확인 (주문 없음)
python -m lake_executor serve                        # 127.0.0.1:8787 수신 + 실행기 + 회신기
python -m lake_executor simulate                     # 다른 터미널: TEST 신호 entry→add→partial_exit→protection_update→full_exit
python -m lake_executor sign --file signal.json --mode test   # 임의 신호 파일에 서명 헤더 + curl 예시
python -m pytest -q                                  # 네트워크 없이 전부 통과해야 함
```

`config.json` 의 `"test": {"simulate_fills": true}` 로 두면 TEST 신호가 계정마다 하나씩 만든 `PaperExchange` 로 체결되어
회신·스냅샷 흐름까지 확인할 수 있다 (실거래소 호출 없음). `false` 면 TEST 는 **기록 전용**(`acknowledged` 회신 하나만).

## 실매매 테스트 (내 거래소 키로 신호 한 건 넣어보기)

목표: 내 서버(또는 PC)에서 `serve` 를 띄우고, 예시 신호 파일을 **서명해서 바로 쏴서** 실제 주문이 들어가는지 본다.

```bash
cp config.example.json config.json       # live.enabled 를 true 로, 쓸 계정만 enabled: true
cp .env.example .env                     # BYBIT_API_KEY/SECRET (+ OKX_*, TOOBIT_*) 와
                                         # LAKE_SIGNAL_SECRET_LIVE (아무 32자 이상 문자열, 발신기와 같은 값) 입력
python -m lake_executor check            # 잔고·계약 정보 읽기. 여기서 실패하면 키/IP 화이트리스트 문제
python -m lake_executor serve            # 터미널 1

# 터미널 2 — tools/signals/ 의 예시를 순서대로 (ts/expires_at_ms/서명은 fire 가 채움)
python -m lake_executor fire --mode live --file tools/signals/1_entry_long.json          # 롱 진입 0.001 BTC
python -m lake_executor fire --mode live --file tools/signals/3_protect_long.json        # 손절/익절 가격 설정·변경
python -m lake_executor fire --mode live --file tools/signals/4_partial_exit_long.json   # 롱 일부 익절
python -m lake_executor fire --mode live --file tools/signals/5_full_exit_long.json      # 롱 전량 종료
python -m lake_executor fire --mode live --file tools/signals/6_entry_short.json         # 숏 진입
python -m lake_executor fire --mode live --file tools/signals/7_full_exit_short.json     # 숏 종료
```

- 응답 `202` 는 접수. 실제 체결은 서버 로그, `GET /state`(헤더 `X-Admin-Token`), 거래소 앱에서 확인한다.
- 같은 `event_id` 를 다시 보내면 200 duplicate 로 **재실행되지 않는다**. 다시 돌리려면 `--event-id auto --position-id pos-L2` 처럼 새 ID 를 주거나 파일의 `position_id`/`event_sequence` 를 바꾼다.
- `--qty 0.002` 로 수량만 바꿔 보낼 수 있다. `--mode test` 로 보내면 기록만 하고 주문하지 않는다(`test.simulate_fills=true` 면 모의 체결).
- 거래소 키는 서버 공인 IP(EC2 면 Elastic IP, PC 면 집 IP)로 화이트리스트해야 한다.
- 발신기(내 AWS 신호 서버)는 `tools/send_signal_example.sh` 또는 `lake_executor/auth.py` 의 서명 방식 그대로 보내면 된다.

## 계정 목록과 라우팅 (v0.2)

`config.json` 의 `accounts[]` 가 실행 대상이다. **`accounts` 키가 없으면 v0.1 과 동일**하게 최상위
`symbol/position_mode/leverage/margin_mode/testnet` + `.env` 의 `BYBIT_API_KEY/SECRET` 로 `bybit` 계정 하나를 만든다.

| 필드 | 뜻 |
|---|---|
| `name` | 계정 이름 `[a-z0-9_-]{1,32}`, 유일. 원장(lots/orders/fills/reports)·회신 sequence·메타 키의 구분자 |
| `exchange` | `bybit` \| `okx` \| `toobit` |
| `enabled` | `false` 면 라우팅 대상에서 제외(신호는 그 계정에 대해 `ACCOUNT_DISABLED`) |
| `symbol` | 거래소 네이티브 심볼. 기본 `BTCUSDT` / `BTC-USDT-SWAP` / `BTC-SWAP-USDT`. 회신 `symbol` 은 항상 lake 표준 `BTCUSDT` |
| `position_mode` | `hedge` \| `one_way`. **Toobit 은 `hedge` 만**(LONG/SHORT 양방향 포지션, 설정 검증에서 거부). 수신 검증의 허용 `position_idx` 는 enabled 계정들의 합집합 |
| `leverage` / `margin_mode` / `testnet` | 기동 시 `ensure_account_setup` 이 맞춘다. OKX `testnet=true` = 데모 트레이딩(`x-simulated-trading: 1`), **Toobit 은 테스트넷 없음 — `testnet:true` 는 설정 오류**(항상 메인넷 실주문) |
| `env_prefix` | `.env` 키 접두사: `{PREFIX}_API_KEY` / `_API_SECRET` / `_API_PASSPHRASE`(OKX 필수). 기본 = 거래소 이름 대문자. **계정마다 달라야 한다**(같은 접두사 = 같은 키 = 같은 거래소 계정에 같은 주문을 두 번 보내는 것 → 설정 오류). 이름이 `okx-sub`/`okx_sub` 처럼 같은 env 접미사로 겹치는 것도 거부 |
| `qty_multiplier` | 신호 `qty_btc` × 배수 = 그 계정의 주문 수량 (시드 비율). `expected_qty_btc_after` 비교도 같은 배수. (0, 100] |
| `report` | `false` 면 그 계정의 회신을 `unsent` 로 저장만 한다. **기본 예시는 bybit 만 `true`** — lake 와 거래소별 스트림 합의 전까지 OKX/Toobit 회신은 보내지 않는다 |

**라우팅** `routing`:
- `fanout`(기본): 모든 enabled 계정에 같은 신호를 실행한다(계정마다 `qty_multiplier` 적용). 한 계정의 실패가 다른 계정 실행을 막지 않으며,
  계정별 결과는 `signal_runs` 에, 신호 전체 상태는 모든 계정 처리 뒤 `done`(하나라도 error 면 `error`, 전부 rejected 면 `rejected`) 로 닫힌다.
- `by_exchange`: 신호의 `exchange`(Bybit/OKX/Toobit, 대소문자 무시)와 거래소가 같은 enabled 계정만. 일치하는 계정이 없으면 `rejected/NO_TARGET_ACCOUNT`.

**회신 스트림**은 (mode, account) 마다 독립이다: sequence·`observed_at_ms` 클램프·`inconsistent` 플래그가 계정별로 돌고,
본문 `exchange` 는 계정 거래소의 표시 이름(`Bybit`/`OKX`/`Toobit`), snapshot `account_scope` 는 bybit `lake_dedicated_BTCUSDT`,
그 외 `lake_dedicated_OKX_BTCUSDT` / `lake_dedicated_TOOBIT_BTCUSDT`. 회신 URL/시크릿은 `LAKE_REPORT_URL_{MODE}`/`LAKE_REPORT_SECRET_{MODE}` 가
기본값이고 `LAKE_REPORT_URL_{MODE}_{NAME}` / `LAKE_REPORT_SECRET_{MODE}_{NAME}`(NAME = 계정 이름 대문자, 영숫자 외 `_`) 로 계정별 덮어쓸 수 있다.
lake 쪽 수용 여부는 `docs/REPLY_TO_LAKE.md` §10 참고.

### 수량 단위와 최소 수량

| 거래소 | 심볼 | 네이티브 수량 | 변환 (래퍼 내부) | 최소 주문 (BTC) |
|---|---|---|---|---|
| Bybit | BTCUSDT (linear) | BTC | 그대로 | qtyStep **0.001** |
| OKX | BTC-USDT-SWAP | 계약 수 `sz` | `sz = qty_btc / ctVal` (ctVal 0.01 BTC, lotSz 1) | **0.01** — 더 작은 신호는 그 계정만 `rejected/QTY_BELOW_MIN` |
| Toobit | BTC-SWAP-USDT | 계약 수 `quantity` | `quantity = qty_btc / contractMultiplier` (0.001) | `exchangeInfo` LOT_SIZE × contractMultiplier (현재 0.001) |

`instrument()` 는 BTC 로 환산한 `qty_step/min_qty/max_qty` 를 돌려주고, 체결 수량·포지션 `size` 도 BTC 로 환산된다.
`qty_multiplier` 를 쓰면 **곱한 뒤** 거래소 step 으로 내림하므로, 작은 신호가 OKX 최소 0.01 BTC 에 못 미치면 OKX 계정만 거부된다.

### 보호주문 방식 (거래소별)

| 거래소 | `supports_lot_protection` | 방식 |
|---|---|---|
| Bybit | True | lot 단위 조건부 reduceOnly 시장가(v0.1 그대로) |
| OKX | True | lot 단위 알고 주문(`place_algo_order` conditional, `slTriggerPx`/`tpTriggerPx` + `-1` 시장가) |
| Toobit | **False (실계정 검증 전)** | **포지션 단위** TP/SL `POST api/v1/futures/position/trading-stop` — 한 레그(LONG/SHORT)의 포지션 전체에 적용 |

Toobit 제약: lot 단위 조건부 주문(`type=STOP`, `stopPrice`)은 공식 문서를 확인하지 못해 **VERIFY** 상태이며 실행기가 호출하지 않는다.
포지션 단위 보호의 한계 — 레그에는 SL 하나 + TP 하나뿐이다. 같은 레그에 lot 이 둘 이상이면 레그에 거는 값은 **lot 들의 의도를 합친 것**
(설정하는 lot 의 SL/TP 가 우선, 그 lot 이 정하지 않은 부분은 최근 형제 lot 의 값)이고, 한 lot 을 닫거나 재설정해도 **형제의 보호를 비우지 않는다**
(형제 값을 다시 적용; 형제가 없을 때만 해제). 레그에 걸린 값은 계정+레그 단위로 원장(`leg_protection` meta)에 한 곳에 기록되며 스냅샷의
`stop_loss/take_profit` 과 `protection_missing` 은 이 값을 본다(해제된 레그를 묵은 lot 기록으로 "보호됨" 이라 보고하지 않음). `take_profit` 배열은
첫 레벨 하나만 쓰고, 트리거되면 레그 전량이 닫힌다. 파라미터 이름(`stopLoss/takeProfit/slTriggerBy/tpTriggerBy`, 해제 값 `"0"`)은 ccxt 부착형
TP/SL 에서 유추한 값이므로 **소액 실계정 검증 뒤에 enabled 로 켠다** (`docs/RUNBOOK.md` §6.3).

## 모드와 안전 게이트

| 게이트 | 효과 |
|---|---|
| 신호 `mode: test` | 실거래소를 절대 건드리지 않음. 기록 전용 또는 계정별 PaperExchange 시뮬레이션 |
| 신호 `mode: live` | `live.enabled=true` **그리고** `LAKE_SIGNAL_SECRET_LIVE` **그리고** 그 계정의 실제 키(OKX 는 passphrase 포함)가 있어야 그 계정에서 실행. 아니면 접수(202)는 하되 `rejected/LIVE_DISABLED`(계정이 `enabled:false` 면 `ACCOUNT_DISABLED`) |
| `state/HALT` 파일 | 새 신호를 전부 `rejected/OPERATOR_HALT`. 거래소의 기존 보호주문(SL/TP)은 그대로 유지. 스냅샷·회신은 계속 |
| 실행 시점 만료 | 접수 뒤 지연돼 `expires_at_ms` 가 지난 신호는 `rejected/EXPIRED`. 판정은 팬아웃 전에 한 번 — 모든 대상 계정에 같은 결과(앞 계정의 느린 체결로 뒤 계정만 EXPIRED 가 되지 않음) |
| 보호주문 자가 복구 | 체결 뒤 SL/TP 생성이 실패해도 신호는 체결대로 종결(note `PROTECTION_FAILED` + 알림)하고, 30초 대사가 다시 만든다. 트리거가 이미 지난 보호가격은 즉시 reduceOnly 시장가로 실행(`auto:sl`/`auto:tp`). `/healthz` 의 `protection_missing` 로 확인 |
| 불명 주문 재확인 | `EXCHANGE_TIMEOUT`/조회 실패로 끝난 시장가 주문은 대사가 거래소에서 종결될 때까지 재확인 |
| 불일치 플래그 (`RECONCILE_REQUIRED`) | (mode, account) 별. 거래소 포지션 ≠ 그 계정 lot 합계이면 그 계정의 `entry/add` 거부, 청산류는 허용, **그 계정 스냅샷 전송 중단** |
| 수량 가드 | 거래소 `qty_step` 내림 → `min_qty` 미만 `QTY_BELOW_MIN`, 주문 1건 > `guards.max_order_qty_btc` → `QTY_LIMIT`, 레그 합계 > `guards.max_leg_qty_btc` → `LEG_LIMIT` (전부 BTC 단위, `qty_multiplier` 적용 후) |
| 슬리피지 가드 (entry/add) | `|last − reference_price| / reference_price` > `guards.max_entry_slippage_pct` % → `SLIPPAGE_GUARD` (reference_price 가 null 이면 생략) |
| 플레이스홀더 키 | `.env` 의 키가 비어 있거나 `PUT_` 플레이스홀더이면 그 계정은 live 실행 불가 |
| 멱등 주문 | 모든 주문은 거래소 클라이언트 주문 ID(`orderLinkId` / OKX `clOrdId` 영숫자 32자 / Toobit `newClientOrderId`)로 멱등. 재시작 시 `processing` 신호는 재실행하지 않고 거래소 조회로 마무리 |

시크릿은 방향·모드별로 분리한다: 수신 검증 `LAKE_SIGNAL_SECRET_{TEST,LIVE}`, 회신 서명 `LAKE_REPORT_SECRET_{TEST,LIVE}`,
회신 URL `LAKE_REPORT_URL_{TEST,LIVE}`(+ 계정별 `_{NAME}` 덮어쓰기). 해당 모드의 수신 키가 없으면 그 모드 신호는 **503**.
회신 URL/키가 없거나 계정이 `report:false` 면 회신은 `unsent` 로 저장만 된다. `event_id` 는 mode 별로 유일하다.
`ADMIN_TOKEN` 은 시크릿과 같이 32바이트 이상(`python -c "import secrets;print(secrets.token_urlsafe(32))"`)이어야 하고,
배포에서는 `/state`·`/admin/*` 가 Caddy 에서 운영자 CIDR(기본 서버 로컬)로 제한된다.

## 배포

AWS EC2(서울) + Elastic IP + Caddy(자동 HTTPS 443) → `127.0.0.1:8787` lake-executor(systemd).
절차·명령은 **`deploy/README.md`** 참고 (`provision.py` → `push.py` → `.env`/`config.json` 작성 → `finalize.py`).
모든 거래소 API 키는 Elastic IP 로 IP 제한하므로 로컬에서 `check` 가 거래소 단계에서 실패하는 것이 정상이다.
계정 추가 절차(키 권한, OKX passphrase·데모, Toobit 양방향 포지션)는 `docs/RUNBOOK.md` §6.

## 절대 커밋 금지 (`.gitignore` 처리됨)

`.env`(API 키·passphrase·시크릿·회신 URL), `config.json`, `state/`(SQLite 원장·로그·HALT), `*.db*`, `*.log`, `*.pem`(EC2 SSH 키),
`deploy/aws_state.json`(인스턴스 ID·IP·키 경로). 시크릿은 문서·대화·소스·로그 어디에도 넣지 않는다 (로그 포맷터가
알려진 시크릿 값을 `***` 로 마스킹하지만 안전망일 뿐이다).
