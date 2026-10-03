# 다거래소 확장 설계 (Bybit + OKX + Toobit) — 2단계 계약

> 1단계(`docs/ARCHITECTURE.md`, Bybit 단일)를 **깨지 않고** 계정 목록으로 일반화한다. 1단계 설정 파일(최상위 `symbol/position_mode/...` + `BYBIT_*` env)은 그대로 동작해야 한다(= 단일 bybit 계정으로 해석).
> API 근거(오프라인): ccxt 4.5.85 `toobit`/`okx` 소스, python-okx. 거래소 문서 사이트는 이 환경에서 접근 불가하므로 **Toobit 조건부 주문 파라미터는 실계정/소액 검증 필요**로 표시한다.

## 1. 설정
```json
"routing": "fanout",            // fanout: 모든 enabled 계정에 같은 신호 실행 | by_exchange: 신호의 exchange 필드와 일치하는 계정만
"accounts": [
  {"name": "bybit",  "exchange": "bybit",  "enabled": true,  "symbol": "BTCUSDT",       "position_mode": "hedge", "leverage": 5, "margin_mode": "isolated", "testnet": false, "env_prefix": "BYBIT",  "qty_multiplier": 1.0, "report": true},
  {"name": "okx",    "exchange": "okx",    "enabled": false, "symbol": "BTC-USDT-SWAP", "position_mode": "hedge", "leverage": 5, "margin_mode": "isolated", "testnet": false, "env_prefix": "OKX",    "qty_multiplier": 1.0, "report": false},
  {"name": "toobit", "exchange": "toobit", "enabled": false, "symbol": "BTC-SWAP-USDT", "position_mode": "hedge", "leverage": 5, "margin_mode": "isolated", "testnet": false, "env_prefix": "TOOBIT", "qty_multiplier": 1.0, "report": false}
]
```
- env: `{PREFIX}_API_KEY`, `{PREFIX}_API_SECRET`, `{PREFIX}_API_PASSPHRASE`(OKX 필수). 회신 URL/시크릿은 기본 `LAKE_REPORT_URL_{MODE}` / `LAKE_REPORT_SECRET_{MODE}`, 계정별 덮어쓰기 `LAKE_REPORT_URL_{MODE}_{NAME_UPPER}` / `LAKE_REPORT_SECRET_{MODE}_{NAME_UPPER}`.
- `accounts` 가 없으면 최상위 값 + `BYBIT_*` 로 `bybit` 계정 하나를 만든다(1단계 호환). `config.Settings.accounts: list[AccountSettings]`, `Settings.account(name)`.
- `qty_multiplier`: 신호 `qty_btc` 에 곱해 계정별 수량을 만든다(시드 비율). `expected_qty_btc_after` 비교도 같은 배수로.
- `live_execution_possible(account)`: `live.enabled` + 그 계정의 실키 + `LAKE_SIGNAL_SECRET_LIVE`.

## 2. 수량 단위 — 실행기는 항상 BTC, 변환은 거래소 래퍼 안에서
| 거래소 | 심볼 | 네이티브 수량 | 변환 | 최소 |
|---|---|---|---|---|
| Bybit | BTCUSDT (linear) | BTC | 그대로 | qtyStep 0.001 |
| OKX | BTC-USDT-SWAP | 계약 수 `sz` | `sz = qty_btc / ctVal` (ctVal=0.01 BTC, lotSz=1) | **0.01 BTC** → 더 작은 신호는 `QTY_BELOW_MIN` |
| Toobit | BTC-SWAP-USDT | 계약 수 `quantity` | `quantity = qty_btc / contractMultiplier` (0.001) | exchangeInfo LOT_SIZE |
`instrument()` 는 BTC 단위로 환산된 `qty_step/min_qty/max_qty` 를 돌려준다. 체결 수량(`executions()`)과 포지션 `size` 도 BTC 로 환산해 반환한다.

## 3. 거래소 래퍼 (`exchange.py` 확장, 인터페이스는 1단계 §4 유지)
공통 추가: `display_name` ("Bybit"|"OKX"|"Toobit"), `supports_lot_protection: bool`, `set_position_protection(position_idx, stop_loss, take_profit) -> dict` (lot 단위 조건부 주문을 못 쓰는 거래소용 포지션 단위 TP/SL), `build_exchange(account: AccountSettings, kind=None, price=None)` (키는 `account.api_key/...` 에 이미 병합되어 있으므로 별도 secrets 인자 없음; `kind` 생략 시 `account.exchange`, `okx`/`toobit` 래퍼는 지연 import).

### OKX (`OkxExchange`, python-okx `okx.Trade/Account/PublicData/MarketData`, `flag="1"` = 데모 트레이딩 when testnet)
- 주문: `TradeAPI.place_order(instId, tdMode=isolated|cross, side=buy|sell, ordType="market", sz=계약수, posSide=long|short(헤지)|net(단방향), reduceOnly="true"|"false", clOrdId=영숫자≤32)`. clOrdId 는 `util.order_link_id(...)` 를 **영숫자만 남겨 32자로 절단**.
- 조건부(SL/TP, lot 단위): `TradeAPI.place_algo_order(instId, tdMode, side, ordType="conditional", sz, posSide, reduceOnly="true", slTriggerPx=p, slOrdPx="-1", slTriggerPxType="mark"|"last", algoClOrdId=...)` (TP 는 `tpTriggerPx/tpOrdPx="-1"`). 취소 `cancel_algo_order([{"instId","algoId"}])`, 조회 `get_algo_order_details(algoClOrdId=)` / `order_algos_list(ordType="conditional", instId)`. 트리거되면 일반 주문이 생성되므로 체결은 `get_fills(instType="SWAP", instId, ordId=생성된 ordId)` 로 수집(알고 상세의 `ordId`).
- 조회: `get_order(instId, clOrdId=)` → `state` live|partially_filled|filled|canceled, `accFillSz`, `avgPx`; `get_fills(instType="SWAP", instId, ordId)` → `tradeId, fillSz, fillPx, ts`; `AccountAPI.get_positions(instType="SWAP", instId)` → `pos`(계약수), `avgPx`, `markPx`, `posSide`, `uTime`; 설정 `set_position_mode("long_short_mode"|"net_mode")`, `set_leverage(lever, mgnMode, instId, posSide=long/short)`; `PublicAPI.get_instruments(instType="SWAP", instId)` → `ctVal, lotSz, minSz, tickSz`; 시세 `MarketAPI.get_ticker(instId)` → `last`, 마크가는 `PublicAPI.get_mark_price(instType="SWAP", instId)`.
- 응답 `code != "0"` 이면 `ExchangeRejected(ret_code=int(code))` (데이터 안 sCode 도 확인). 오류 원문은 로그에만.
  단 **결과를 알 수 없는 코드는 거부가 아니다**: 쓰기에서 `50004`(요청 타임아웃, 성공/실패 불명) → `ExchangeError("EXCHANGE_TIMEOUT")`,
  `50001/50013/50026`(점검/바쁨/시스템 오류) → `ExchangeError("EXCHANGE_ERROR")` 로 전달해 실행기의 "get_order 확인 → unknown → reconcile 재확인" 경로를 탄다.
- clOrdId 는 32자로 잘리므로 재기동 뒤 `open_conditional_orders()` 가 모르는 id 는 `link_resolver(cid)`(실행기가 원장의
  `order_link_id` 접두사 검색으로 꽂아 줌)로 원래 34자 link 를 되찾는다. 실행기의 고아 스탑 정리도 같은 접두사 검색을 한다.

### Toobit (`ToobitExchange`, 자체 httpx 클라이언트, ccxt `toobit.sign` 과 동일 규격)
- 서명: 파라미터(POST/DELETE 는 urlencoded 본문, GET 은 쿼리)에 `timestamp`(ms)·`recvWindow=5000` 추가 → `signature = HMAC-SHA256(secret, body + query)` hex → `?signature=` 쿼리에 부착. 헤더 `X-BB-APIKEY`. 베이스 `https://api.toobit.com`.
- 엔드포인트: `POST api/v1/futures/order` {symbol, side=BUY_OPEN|SELL_OPEN|BUY_CLOSE|SELL_CLOSE, type="LIMIT", priceType="MARKET"(시장가), quantity(계약수), newClientOrderId, timeInForce?, stopPrice?(조건부), stopLoss/takeProfit(부착형, slTriggerBy=MARK_PRICE|CONTRACT_PRICE)}; `GET api/v1/futures/order?clientOrderId=` ; `DELETE api/v1/futures/order` {orderId|clientOrderId}; `GET api/v1/futures/openOrders`; `GET api/v1/futures/userTrades` {symbol, startTime, limit} → `id, orderId, qty, price, time, side`; `GET api/v1/futures/positions` {symbol} → `side=LONG|SHORT, position, avgPrice, markPrice`; `POST api/v1/futures/leverage` {symbol, leverage}; `POST api/v1/futures/marginType` {symbol, marginType=ISOLATED|CROSS}; `POST api/v1/futures/position/trading-stop` (포지션 단위 TP/SL); `GET api/v1/exchangeInfo` → `contractMultiplier`, `filters[LOT_SIZE|PRICE_FILTER]`; `GET quote/v1/contract/ticker/price` / `quote/v1/markPrice`.
- 헤지: Toobit 은 LONG/SHORT 양방향 포지션 → `position_idx` 1→LONG(BUY_OPEN/SELL_CLOSE), 2→SHORT(SELL_OPEN/BUY_CLOSE). 단방향(0) 은 지원하지 않음(설정 검증에서 거부).
- **lot 단위 조건부 주문(stopPrice + 반대 side *_CLOSE)은 실계정 검증 전까지 `supports_lot_protection=False`** 로 두고 `set_position_protection` (`position/trading-stop`) 을 쓴다. 검증되면 플래그만 바꾼다. 오류 코드: -1141 중복 clientOrderId → 멱등 성공으로 처리(기존 주문 조회), -1021/-1022 시각/서명.
  **결과 불명 코드는 거부가 아니다**: 쓰기에서 `-1006/-1007/-1146/-1147`(execution status unknown, 주문 생성/취소 타임아웃) → `ExchangeError("EXCHANGE_TIMEOUT")`,
  `-1000/-1001` 과 코드 없는 HTTP ≥ 500 → `ExchangeError("EXCHANGE_ERROR")` (실행기가 조회·재확인). `testnet:true` 는 설정 검증에서 거부(Toobit 은 테스트넷 없음 — 항상 메인넷).
- `executions(order_id)`: `userTrades` 에 orderId 필터가 없으므로 이 래퍼가 낸 주문은 생성 시각으로 `startTime` 을 좁히고(−60s),
  한 페이지(limit 500)가 가득 차면 마지막 체결 시각+1 부터 이어 읽는다(최대 20페이지). 모르는 주문은 24시간 창으로 같은 방식.
- `check` 명령은 Toobit/OKX 계정에 대해 exchangeInfo·포지션·잔고 읽기만으로 연결을 검증하고 변환 계수를 출력한다.

### Paper
`PaperExchange(account, price)` 는 계정마다 하나씩, `display_name` 은 계정의 거래소 이름을 따른다(테스트에서 OKX/Toobit 경로를 BTC 단위 그대로 시뮬레이션).
`build_exchange(account, "paper")` 는 계정 거래소의 기본 instrument(`exchange.PAPER_INSTRUMENTS`: okx 0.01 BTC step/min, bybit·toobit 0.001)를 쓰므로
TEST(simulate_fills) 결과가 live 의 `QTY_BELOW_MIN` 판정을 그대로 예측한다.

## 4. 원장 (`store.py` 변경 — 마이그레이션 포함)
- `lots`, `orders`, `fills`: `account TEXT NOT NULL DEFAULT 'bybit'` 추가. `lots` PK → `(mode, account, position_id)`. 기존 DB 는 `ALTER TABLE ... ADD COLUMN` + 새 테이블 재생성으로 마이그레이션(스키마 버전 `meta:schema_version`).
  1단계 행이 귀속될 계정은 `Store(path, legacy_account=settings.legacy_account_name())`(첫 bybit 계정, 없으면 첫 계정; 기본 `'bybit'`) — 운영자가 v0.1 계정을
  다른 이름으로 올려도 lot/주문/회신 sequence 가 고아가 되지 않는다. 쓴 이름은 meta `legacy_account` 에 남는다. `serve` 는 기동 시 `main.verify_ledger_accounts` 로
  원장의 계정 이름이 설정에 전부 있는지 검사하고, 설정에 없는 계정에 open lot 또는 pending 회신이 있으면 `ConfigError`(exit 2) 로 멈춘다(닫힌 행만이면 경고).
- `lots_with_pending_cancel`: lot 단위 보호주문 항목이 남은 닫힌 lot **과 포지션 단위 보호 기록(`protection_orders.kind=="position"`, `position` 이 아직 값)이 남은 닫힌 lot** —
  해제 실패분을 reconcile ④ 가 재시도한다.
- meta `leg_protection:{mode}:{account}:{position_idx}` (JSON `{stop_loss, take_profit, tp_i, position_id, sl_from, tp_from, set_at_ms[, cleared]}`): 포지션 단위 보호를 쓰는
  거래소에서 **그 레그에 지금 걸려 있다고 아는 값** (계정+레그 단위 진실). `Store.get_leg_protection/set_leg_protection`; 기록이 없으면(이 키 이전 DB) 실행기가 open lot 의 `po.position` 으로 한 번 시드한다.
- `signal_runs(mode, event_id, account, status, reason_code, processed_at_ms, note)` PK `(mode, event_id, account)`: 계정별 실행 결과 (`event_id` 는 mode 안에서만 유일하므로 `mode` 포함 — `signals` PK 와 동일). `signals.status` 는 모든 계정 처리 후 `Store.summarize_runs` 로 닫는다: 하나라도 error 면 `error`, 전부 rejected 면 `rejected`(첫 reason_code), 그 외 `done`; note 는 `acct=status[/reason][ note];…` (대상 계정이 하나면 그 run 의 reason_code/note 그대로 = 1단계 호환).
- `reports`: `account TEXT NOT NULL DEFAULT 'bybit'`, UNIQUE `(mode, account, sequence)`; meta 키 `seq:{mode}:{account}`, `observed:{mode}:{account}`, `inconsistent:{mode}:{account}`.
- API 시그니처에 `account` 인자 추가: `get_lot(mode, account, position_id)`, `open_lots(mode, account)`, `insert_order(..., account)`, `insert_fill(..., account)`, `allocate_report(mode, account, ...)`, `pending_reports(mode, account)`, `is_inconsistent(mode, account)`, `set_inconsistent(mode, account, ...)`, `set_run_result(mode, event_id, account, status, reason_code, note)`, `get_runs(mode, event_id)`, `recent_runs(mode, account=None, limit)`, `finalize_signal(mode, event_id)`. 실행기 전용 meta 키도 계정 범위로: `account_setup:live:{account}`, `auto_action:{mode}:{account}:{event_id}` (마이그레이션 대상 아님 — 첫 기동 시 계정당 멱등 setup 1회 추가).

## 5. 실행기/회신
- `Executor(settings, store, exchanges: dict[str, dict[str, ExchangeBase]], reporter, alerts)` — `exchanges = {mode: {account_name: ExchangeBase}}` (한 프로세스가 같은 계정의 test(Paper)·live(실거래소)를 함께 서비스하므로 mode 가 바깥 키; 1단계의 `{mode: ExchangeBase|None}` 모양은 `accounts[0]` 로 정규화). `process(row)`: 대상 계정 = routing 규칙 + enabled + (live 면 그 계정의 live 가능 여부) → 계정별로 1단계 로직 실행(lot 키에 account), 계정마다 `set_run_result`. 한 계정 실패(ExchangeError/예상 밖 예외)가 다른 계정 실행을 막지 않는다. 대상 계정이 없으면 `rejected/NO_TARGET_ACCOUNT`(알림만, 회신 스트림 없음). `reconcile(mode, account=None=전체 AND)`, `snapshot_now(mode, account)`, `snapshot_all(mode)`, `build_snapshot_positions(mode, account)`, `protection_missing(mode, account=None)`.
- `position_idx` 매핑: 계정이 `one_way` 면 신호 1/2 → 0 으로 처리(방향은 leg), `hedge` 면 그대로.
- 보호주문: `exchange.supports_lot_protection` 이면 1단계 방식(조건부 주문), 아니면 `set_position_protection(position_idx, sl, tp[0])` 로 포지션 단위 설정.
  레그에는 SL 하나 + TP 하나뿐이므로 걸 값은 **lot 들의 의도를 합친 것**(`_leg_intent`): 설정하는 lot 의 SL/TP 가 우선, 그 lot 이 정하지 않은 부분은 같은 레그의 최근 형제 lot 의 의도로 채운다
  (경고 로그). 한 lot 을 닫거나 재설정할 때 같은 레그에 다른 open lot 이 있으면 **레그를 비우지 않고 형제의 의도를 다시 적용**한다(형제가 보호 없이 남지 않음); 형제가 없을 때만 `(idx, None, None)` 으로 해제.
  set/clear 때마다 `leg_protection` meta 를 갱신하고, 스냅샷의 `stop_loss/take_profit`(읽기 API 가 없을 때)과 `protection_missing` 은 lot 별 기록이 아니라 **이 레그 상태**를 본다
  (SL 의도가 있는데 레그에 SL 이 없거나 TP 의도가 있는데 레그에 TP 가 없으면 빠짐 → reconcile 이 다시 건다). 해제가 실패한 닫힌 lot 은 `lots_with_pending_cancel` 로 reconcile ④ 가 재시도한다.
- 만료(`expires_at_ms`)는 **팬아웃 전에 한 번** 판정해 모든 대상 계정에 같은 결과를 준다(앞 계정의 체결 대기로 뒤 계정만 `EXPIRED` 가 되어 lot 이 갈라지지 않게).
- `Reporter.execution(mode, account, ...)`, `snapshot(mode, account, positions, observed_at_ms)`, `deliver_pending(mode, account)`. 본문 `exchange` = `display_name`, `symbol` = `"BTCUSDT"`(lake 표준, 네이티브 심볼 아님), `account_scope` = bybit 는 `lake_dedicated_BTCUSDT`, 그 외 `lake_dedicated_{EXCHANGE}_BTCUSDT`(상대 계약 확장 필요 — REPLY_TO_LAKE 에 명시). `report:false` 계정은 `unsent` 로 저장만.
- 스냅샷 루프는 (mode, account) 조합마다 돈다.

## 6. 수신/관리/CLI
- `schemas.Signal.exchange`: `Literal["Bybit","OKX","Toobit"]` (lake 는 현재 Bybit 만 보냄). `by_exchange` 라우팅에서 대소문자 무시 비교.
- `GET /state`: `routing` + `accounts{name: {exchange, display_name, enabled, report, has_real_keys, live_execution_possible, …, modes{mode: {exchange_ready, inconsistent, open_lots, reports, runs, protection_missing, snapshot_positions, exchange_positions}}}}` 계정별 섹션. `snapshot_positions`/`exchange_positions` 최상위 키는 `{mode: {account: …}}` 로 바뀜(1단계의 `{mode: list}` 와 비호환 — 이를 읽는 도구가 있으면 수정). `inconsistent{mode}` 는 계정 중 하나라도 true. `POST /admin/reconcile?mode=&account=` — `account` 생략 시 그 mode 에 거래소가 있는 모든 계정, 모르는 이름이면 400 `BAD_ACCOUNT`; 응답에 `accounts{name: {consistent, positions, exchange_positions}}`.
- `check`: 계정별 설정·키 유무·회신 설정 출력 + 실키가 있는 계정만 읽기 전용 연결 검증(instrument/변환 계수·최소수량, last/mark, 포지션, Bybit 은 지갑 잔고) + 원장 계정 이름 대조(마이그레이션 없이 읽기 전용; 설정에 없는 계정의 open lot 은 문제로 집계).
  `simulate` 는 변경 없음(신호 `exchange:"Bybit"`, fanout 이면 모든 enabled 계정에 실행).
- `serve`: 실키가 있는 enabled 계정의 실거래소를 만들지 못하면(모듈 없음 등) `ConfigError` 로 **기동을 멈춘다**(exit 2) — 계정이 조용히 빠진 채 떠서 그 계정의 live 신호가 전부 거부되는 일을 막는다. `requirements.txt` 에 `python-okx` 포함.
- 설정 검증 추가: 계정 간 `env_prefix` 중복(같은 API 키 = 같은 거래소 계정 → 같은 order_link_id 이중 전송) 과 `env_suffix(name)` 충돌(`okx-sub`/`okx_sub`) 거부, Toobit `testnet:true` 거부, `leverage/qty_multiplier/testnet/enabled/report` 의 잘못된 형식은 계정·필드 이름을 담은 `ConfigError`.

## 7. 테스트 추가
- 설정 호환성(accounts 없음 → bybit 하나), 팬아웃 3계정(Paper×3) 전체 수명주기와 계정별 회신 sequence 독립, `qty_multiplier`, OKX 최소수량 `QTY_BELOW_MIN`(instrument min 0.01 모킹), `by_exchange` 라우팅, 한 계정 거래소 오류가 다른 계정을 막지 않음, 포지션 단위 보호(`supports_lot_protection=False` Paper 변형) 경로, 스키마 마이그레이션(1단계 DB 파일 열기).
- OKX/Toobit 래퍼는 httpx/python-okx 호출을 **모킹**해 파라미터 매핑(side/posSide/sz 변환/서명 헤더)을 단위 테스트한다. 실 API 호출 테스트는 없음.

## 8. 문서
- README/RUNBOOK: 계정 추가 절차, 거래소별 키 권한, 최소 수량 표, Toobit 조건부 주문 미검증 경고, 회신은 기본 bybit 만.
- `docs/exchange_api_notes.md`: 위 §3 의 엔드포인트·파라미터 매핑을 근거(ccxt/python-okx 소스 경로)와 함께 정리.
- `docs/REPLY_TO_LAKE.md`: 다거래소 회신을 위해 `exchange` enum 확장·`account_scope` 계정별 값·스트림 분리(URL 또는 account_scope) 합의 요청 항목 추가.
