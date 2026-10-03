# 거래소 API 매핑 노트 (Bybit · OKX · Toobit)

`docs/ARCHITECTURE_MULTI_EXCHANGE.md` §3 의 엔드포인트·파라미터 매핑을 **근거와 함께** 정리한다. 거래소 문서 사이트에는
이 환경에서 접근할 수 없었으므로 근거는 전부 오프라인 소스다:

| 약어 | 근거 |
|---|---|
| `ccxt/toobit` | ccxt **4.5.85** `toobit.py` — `describe()`(엔드포인트 목록·오류 코드), `sign()`, `create_contract_order_request()`, `parse_order()`, `parse_order_status()`, `fetch_positions()`, `fetch_my_trades()`, `set_leverage()`, `set_margin_mode()`, `handle_errors()`. 엔드포인트 목록 사본: `docs/api_reference_offline/ccxt_toobit_describe.json` |
| `ccxt/okx` | ccxt 4.5.85 `okx.py` — `create_order()`(reduceOnly·posSide 처리), `describe()` 오류 코드표(`51xxx`) |
| `python-okx` | 설치된 python-okx: `okx/Trade.py`(`place_order`, `place_algo_order`, `cancel_order`, `cancel_algo_order`, `get_order`, `get_fills`, `get_algo_order_details`, `order_algos_list`), `okx/Account.py`(`get_positions`, `set_position_mode`, `set_leverage`, `get_instruments`), `okx/PublicData.py`, `okx/MarketData.py`, `okx/consts.py`(REST 경로), `okx/client.py`·`okx/utils.py`(헤더·서명·`x-simulated-trading`) |
| `pybit` | Bybit v5 `unified_trading.HTTP` (v0.1, `docs/ARCHITECTURE.md` §4) |

구현 파일: `lake_executor/exchange.py`(Bybit, Paper), `lake_executor/exchange_okx.py`, `lake_executor/exchange_toobit.py`.
단위 테스트(`tests/test_exchange_okx.py`, `tests/test_exchange_toobit.py`)는 클라이언트를 가짜로 바꿔 아래 매핑을 고정한다
(Toobit 서명은 설치된 ccxt 의 `toobit.sign` 과 직접 대조). **실 API 호출 테스트는 없다.**

표기: `★ VERIFY` = 소스에서 확인하지 못해 유추한 값. 실계정(소액)에서 확인 전까지 운영 금지(RUNBOOK §6.3).

## 0. 공통 인터페이스 ↔ 거래소

실행기는 `ExchangeBase` 만 본다(`docs/ARCHITECTURE.md` §4 + v0.2 추가 `display_name`, `supports_lot_protection`, `set_position_protection`).
수량은 **항상 BTC**, 변환은 래퍼 안에서.

| 인터페이스 | Bybit (pybit) | OKX (python-okx) | Toobit (httpx 자체) |
|---|---|---|---|
| `instrument()` | `get_instruments_info` → lotSizeFilter.qtyStep/minOrderQty/maxOrderQty, priceFilter.tickSize | `PublicAPI.get_instruments(instType="SWAP", instId)` → `ctVal, lotSz, minSz, maxMktSz, tickSz` → `qty_step=lotSz×ctVal`, `min_qty=minSz×ctVal`, `max_qty=maxMktSz×ctVal` | `GET api/v1/exchangeInfo` → `contracts[symbol].contractMultiplier`, `filters[LOT_SIZE].stepSize/minQty/maxQty`, `filters[PRICE_FILTER].tickSize` → `× contractMultiplier` |
| `last_price()` / `mark_price()` | `get_tickers` lastPrice / markPrice | `MarketAPI.get_ticker(instId).last` / `PublicAPI.get_mark_price(instType, instId).markPx` | `GET quote/v1/contract/ticker/price` (`p`; 폴백 `quote/v1/ticker/price`, `quote/v1/contract/ticker/24hr`) / `GET quote/v1/markPrice` (`p`\|`markPrice`\|`price`, 실패 시 None) |
| `ensure_account_setup` | `switch_position_mode(mode=3\|0)`, `set_leverage`, `set_margin_mode` | `AccountAPI.set_position_mode(posMode="long_short_mode"\|"net_mode")`, `set_leverage(lever, mgnMode, instId, posSide=long/short)`(isolated+hedge 는 두 번) | `POST api/v1/futures/leverage {symbol, leverage}`, `POST api/v1/futures/marginType {symbol, marginType=ISOLATED\|CROSS}`. 모드 전환 API 없음(hedge 전용) |
| `positions()` | `get_positions(category, symbol)` positionIdx/size/avgPrice/markPrice/updatedTime | `AccountAPI.get_positions(instType="SWAP", instId)` → `pos`(계약수)×ctVal, `posSide`→idx(long 1/short 2/net 0, net 은 pos 부호로 side), `avgPx`, `markPx`, `uTime` | `GET api/v1/futures/positions {symbol}` → `side=LONG\|SHORT`→idx 1/2, `position`(계약수)×mult, `avgPrice`, `markPrice`, `updateTime` |
| `place_market` | `place_order(orderType=Market, qty, positionIdx, reduceOnly, orderLinkId, timeInForce=IOC)` | `TradeAPI.place_order(instId, tdMode, side, ordType="market", sz, posSide, reduceOnly, clOrdId)` | `POST api/v1/futures/order {symbol, side=*_OPEN\|*_CLOSE, type=LIMIT, priceType=MARKET, quantity, newClientOrderId, timeInForce=IOC}` |
| `place_conditional` (lot 단위 SL/TP) | `place_order(orderType=Market, triggerPrice, triggerDirection, triggerBy, reduceOnly, closeOnTrigger, positionIdx, orderLinkId)` | `TradeAPI.place_algo_order(ordType="conditional", slTriggerPx/slOrdPx="-1" 또는 tpTriggerPx/tpOrdPx="-1", *TriggerPxType, sz, posSide, algoClOrdId)` | `POST api/v1/futures/order {type=STOP, priceType=MARKET, stopPrice, side=*_CLOSE, quantity, newClientOrderId}` ★ VERIFY — `supports_lot_protection=False` 라 실행기가 호출하지 않음 |
| `set_position_protection` (포지션 단위) | 미구현(NotImplementedError; lot 단위만) | 미구현(lot 단위만) | `POST api/v1/futures/position/trading-stop {symbol, side=LONG\|SHORT, stopLoss, takeProfit, slTriggerBy, tpTriggerBy}` ★ VERIFY |
| `cancel_order(link)` | `cancel_order(orderLinkId)` | `TradeAPI.cancel_order(instId, clOrdId)` → 없으면 `cancel_algo_order([{instId, algoId}])` | `DELETE api/v1/futures/order {symbol, clientOrderId}` |
| `get_order(link)` | `get_open_orders(orderLinkId)` → `get_order_history` | `TradeAPI.get_order(instId, clOrdId)` → 없으면 `get_algo_order_details(algoClOrdId)` | `GET api/v1/futures/order {symbol, clientOrderId}` |
| `executions(order_id)` | `get_executions(orderId)` | `TradeAPI.get_fills(instType="SWAP", instId, ordId)`; algoId 면 알고 상세의 `ordId` 로 | `GET api/v1/futures/userTrades {symbol, startTime, limit=500}` 를 `orderId` 로 필터. orderId 파라미터가 없으므로(ccxt `fetch_my_trades`: symbol/startTime/endTime/limit) 이 래퍼가 낸 주문은 `startTime = 생성 시각 − 60s`, 모르는 주문은 now−24h; 페이지가 가득 차면 마지막 체결 시각+1 부터 이어 읽음(≤20 페이지) |
| `open_conditional_orders(idx)` | `get_open_orders(orderFilter=StopOrder)` | `TradeAPI.order_algos_list(ordType="conditional", instType, instId, limit=100[, after])` state live/pause/partially_effective | `GET api/v1/futures/openOrders {symbol}` 중 `stopPrice>0` |
| 클라이언트 주문 ID | `orderLinkId` = `util.order_link_id(...)` (`lk`+sha1 32자 = 34자) | `clOrdId`/`algoClOrdId` = `util.alnum_only(link, 32)` (영숫자만, 32자 절단; 래퍼가 역매핑을 메모리에 보관) | `newClientOrderId`/`clientOrderId` = link 그대로 |

## 1. 수량·가격 변환

| 거래소 | 심볼 | 네이티브 수량 | BTC → 네이티브 | 네이티브 → BTC | 최소 (BTC) | 근거 |
|---|---|---|---|---|---|---|
| Bybit | BTCUSDT (linear) | BTC 문자열 | `floor_step(qty, qtyStep)` | 그대로 | qtyStep 0.001 | pybit `get_instruments_info` |
| OKX | BTC-USDT-SWAP | 계약 수 `sz` | `sz = floor(qty / ctVal / lotSz) × lotSz` (Decimal), 0 이면 거부 51020 | `× ctVal` | `minSz × ctVal` = **0.01** | `python-okx Account.get_instruments` 응답 `ctVal="0.01"`, `lotSz="1"`, `minSz="1"` (테스트 픽스처) |
| Toobit | BTC-SWAP-USDT | 계약 수 `quantity` | `floor(qty / contractMultiplier / stepSize) × stepSize` (Decimal) | `× contractMultiplier` | `LOT_SIZE.minQty × contractMultiplier` (0.001) | `ccxt/toobit fetch_markets()` 가 `contractMultiplier` 를 `contractSize` 로, `LOT_SIZE` 를 amount limits 로 읽음 |

가격은 모두 `round_tick(price, tick)` 뒤 `fmt_step`. OKX 트리거 가격 종류: `guards.protection_trigger_by` MarkPrice→`mark`, LastPrice→`last`, IndexPrice→`index`.

## 2. 포지션 방향 매핑

| `position_idx` (신호) | Bybit | OKX hedge (`posSide`) | OKX one_way | Toobit |
|---|---|---|---|---|
| 1 (long) | positionIdx 1 | `long` — 진입 `side=buy`, 청산 `side=sell` | — | 진입 `BUY_OPEN`, 청산 `SELL_CLOSE`, 포지션 `side=LONG` |
| 2 (short) | positionIdx 2 | `short` — 진입 `sell`, 청산 `buy` | — | 진입 `SELL_OPEN`, 청산 `BUY_CLOSE`, 포지션 `side=SHORT` |
| 0 (one_way) | positionIdx 0 | — | `net`, `reduceOnly="true"/"false"` | **미지원**(설정 검증에서 거부) |

- OKX `reduceOnly` 는 **net 모드에서만** 보낸다. long/short 모드에서는 `side+posSide` 가 청산을 결정하므로 비운다
  (근거: `ccxt/okx create_order()` 도 hedge 에서 reduceOnly 를 생략).
- Toobit `(Buy|Sell, position_idx, reduce_only)` → `*_OPEN/*_CLOSE` 조합이 맞지 않으면 ValueError (근거: `ccxt/toobit create_contract_order_request()`
  `request['side'] = 'BUY_CLOSE' if reduceOnly else 'BUY_OPEN'` 등).

## 3. 주문 상태 매핑 (→ v0.1 어휘 `New|PartiallyFilled|Filled|Cancelled|Rejected|Untriggered|Triggered|Deactivated|PartiallyFilledCanceled`)

| 거래소 | 원 상태 | 우리 상태 | 비고 |
|---|---|---|---|
| OKX 일반 (`state`) | live / partially_filled / filled / canceled(mmp_canceled) | New / PartiallyFilled / Filled / Cancelled | `canceled` 인데 `accFillSz>0` 이면 PartiallyFilledCanceled |
| OKX 알고 (`state`) | live, pause / partially_effective / effective / canceled / order_failed | Untriggered / Triggered / (생성된 `ordId` 의 일반 주문 상태를 따라감) / Cancelled / Rejected | `order_id` 는 항상 algoId, 생성 주문은 `exec_order_id` |
| Toobit (`status`) | NEW, PENDING_NEW / PARTIALLY_FILLED / FILLED / CANCELED, PENDING_CANCEL / REJECTED | New / PartiallyFilled / Filled / Cancelled / Rejected | `type=STOP` + `stopPrice>0` + NEW → Untriggered ★ VERIFY(발동 전/후 status 구분). 근거: `ccxt/toobit parse_order_status()` |

체결: OKX `get_fills` → `tradeId, fillSz(×ctVal), fillPx, ts`; Toobit `userTrades` → `id, orderId, qty(×mult), price, time`
(근거: `ccxt/toobit fetch_my_trades()` 의 응답 주석). 둘 다 `(exec_time_ms, exec_id)` 로 정렬.

## 4. 인증·서명

### OKX (python-okx 가 처리)
- `TradeAPI/AccountAPI(api_key, api_secret_key, passphrase, flag)`; `flag="1"` 이면 모든 요청에 헤더 `x-simulated-trading: 1`
  (데모 트레이딩, `account.testnet=true`), 아니면 `"0"`. 근거: `okx/utils.py get_header()`, `okx/client.py`.
- 공개 API(`PublicAPI`, `MarketAPI`)는 키 없이 생성. 베이스 `https://www.okx.com` (`okx/consts.py API_URL`).
- REST 경로(`okx/consts.py`): `/api/v5/trade/order`(place/get), `/api/v5/trade/order-algo`(place algo/details), `/api/v5/trade/cancel-order`,
  `/api/v5/trade/cancel-algos`, `/api/v5/trade/fills`, `/api/v5/account/positions`, `/api/v5/account/set-position-mode`,
  `/api/v5/account/set-leverage`, `/api/v5/public/instruments`, `/api/v5/public/mark-price`, `/api/v5/market/ticker`.

### Toobit (`exchange_toobit.sign_request`, ccxt `toobit.sign()` 재현)
- 베이스 `https://api.toobit.com` (`describe().urls.api.common/private`).
- 비공개 요청: 파라미터 뒤에 `recvWindow=5000`, `timestamp=<ms>` 를 **그 순서로** 붙인다.
  - POST/DELETE: 본문 = `urlencode(params + recvWindow + timestamp)`; `signature = HMAC-SHA256(secret, 본문)` hex 를 **본문 끝에** `&signature=` 로.
  - GET: 쿼리 = 같은 urlencode; signature 는 쿼리로 계산해 쿼리 끝에 `&signature=`.
  - 헤더 `X-BB-APIKEY: <key>`, `Content-Type: application/x-www-form-urlencoded`.
  - urlencode 는 `urllib.parse.urlencode(..., quote_via=quote)`, bool 은 `true`/`false` (ccxt `Exchange.urlencode`).
- 공개(`quote/*`, `api/v1/exchangeInfo`)는 서명 없는 GET.
- 테스트는 설치된 ccxt 의 `toobit.sign` 출력과 바이트 단위로 대조한다 (`tests/test_exchange_toobit.py`).

## 5. 오류 코드 처리

| 거래소 | 코드 | 처리 | 근거 |
|---|---|---|---|
| OKX | 응답 `code != "0"` 또는 `data[i].sCode != "0"` | 쓰기 → `ExchangeRejected(ret_code)`, 읽기 → `ExchangeError(EXCHANGE_ERROR)` | python-okx 응답 형식 |
| OKX | 50004 (endpoint request timeout — 성공/실패 불명) | 쓰기에서도 거부가 아니라 `ExchangeError(EXCHANGE_TIMEOUT)` → 실행기가 get_order 로 확인, 없으면 unknown 으로 남겨 reconcile 재확인 | `ccxt/okx exceptions.exact['50004']: RequestTimeout` |
| OKX | 50001 / 50013 / 50026 (매칭엔진 점검 / 시스템 바쁨 / 시스템 오류) | 쓰기에서도 `ExchangeError(EXCHANGE_ERROR)` (위와 같은 확인 경로) | 〃 `OnMaintenance` / `ExchangeNotAvailable` |
| OKX | 51277~51280 (TP/SL 트리거 가격이 현재가를 이미 지남) | 메시지에 "crossed" 를 붙여 실행기의 "트리거 이미 지남 → 즉시 reduceOnly 시장가" 경로로 | `ccxt/okx describe()` 오류표 |
| OKX | 51400/51401/51402/51405/51410/51603, "not exist/already/completed" | 취소 → 이미 없음으로 보고 True; 조회 → None | 〃 |
| OKX | 51020 (최소 수량 미만) | `_sz_str` 가 0 계약/minSz 미만이면 같은 코드로 거부 | 〃 |
| OKX | 51016 (중복 clOrdId) | 거부로 전달(실행기가 `get_order(link)` 로 멱등 확인) | 〃 |
| Toobit | JSON `code` ∉ {0, 200} 또는 HTTP ≥ 400 | 쓰기 → `ExchangeRejected(ret_code)`, 읽기 → `ExchangeError` | `ccxt/toobit handle_errors()` |
| Toobit | `-1006` / `-1007` (execution status unknown) / `-1146` / `-1147` (주문 생성·취소 타임아웃) | 쓰기에서도 거부가 아니라 `ExchangeError(EXCHANGE_TIMEOUT)` (주문이 체결됐을 수 있음 → 실행기 확인/재확인) | `exceptions.exact`: 전부 `OperationFailed` |
| Toobit | `-1000` / `-1001` (unknown / internal error), 코드 없는 HTTP ≥ 500 | 쓰기에서도 `ExchangeError(EXCHANGE_ERROR)` | 〃 |
| Toobit | `-1141` 중복 clientOrderId | **멱등 성공**: `get_order(link)` 로 기존 주문을 돌려줌 | `describe().exceptions.exact['-1141']: InvalidOrder  # Duplicate clientOrderId` |
| Toobit | `-1139/-1142/-1143/-2013` (체결됨/취소됨/오더북에 없음/존재하지 않음) | 취소 → True; `-1143/-2013` 조회 → None | `describe().exceptions.exact` |
| Toobit | `-1021` recvWindow 밖 / `-1022` 서명 오류 | 거부로 전달 → 서버 시계·시크릿 확인(RUNBOOK §6.3) | 〃 |
| 공통 | httpx `TimeoutException`, HTTP 408/504 | `ExchangeError("EXCHANGE_TIMEOUT")`; 읽기는 1회 재시도 | — |

거래소 오류 원문은 WARNING 로그에 200자 절단으로만 남기고 회신에는 코드만 넣는다. 키·passphrase 는 로그에 남기지 않는다.

## 6. ★ VERIFY 목록 (실계정 소액 검증 항목)

| # | 항목 | 현재 구현 | 확인 방법 |
|---|---|---|---|
| T1 | Toobit `position/trading-stop` 파라미터 `stopLoss`/`takeProfit`/`slTriggerBy`/`tpTriggerBy`/`side` | ccxt `create_contract_order_request()` 의 부착형 TP/SL 파라미터(`stopLoss`, `takeProfit`, `slTriggerBy`)에서 유추. `side` 는 포지션 side `LONG\|SHORT` 로 가정 | 소액 진입 후 호출 → 거래소 화면에 TP/SL 표시 확인 |
| T2 | Toobit trading-stop **해제** 값 | `protection_clear_value="0"` | `stop_loss:null` 로 `protection_update` → TP/SL 이 사라지는지 |
| T3 | Toobit trading-stop 트리거 기준 값 | `MARK_PRICE` (ccxt `triggerPriceTypes`: mark→`MARK_PRICE`, last→`CONTRACT_PRICE`) | 설정 화면 표기와 대조 |
| T4 | Toobit lot 단위 조건부 주문 `type=STOP` + `priceType=MARKET` + `stopPrice` | 구현만, `supports_lot_protection=False` | 동작하면 플래그를 True 로 바꾸고 T1~T3 경로 대신 사용 |
| T5 | Toobit 조건부 주문의 발동 전/후 `status` 구분 | `STOP`+`stopPrice`+`NEW` → Untriggered | T4 와 함께 |
| T6 | Toobit `exchangeInfo` 의 `contracts[]`/`contractMultiplier` 키 이름 | ccxt `fetch_markets()` 기준 (`contracts`, 없으면 `symbols`) | `check`/기동 로그 `toobit instrument …` |
| T7 | Toobit `userTrades` 응답 필드(`id, orderId, qty, price, time`) 와 `positions` 필드(`side, position, avgPrice, markPrice`) | ccxt `fetch_my_trades()`/`fetch_positions()` 주석 | 소액 체결 뒤 `/state` 의 fills·exchange_positions |
| T8 | Toobit `userTrades` 의 정렬 방향과 `startTime` 준수 | 오름차순 가정(가득 찬 페이지 뒤 `startTime = 최신 time + 1`); 내림차순이어도 첫 페이지에 최신 체결이 있으므로 창을 좁힌 조회는 동작 | 500건 이상 체결 뒤 `executions` 로그 "window not exhausted" 유무 |
| T9 | Toobit `positions` 응답에 레그 TP/SL(`stopLoss`/`takeProfit` 류) 필드가 있는지 | 없다고 가정(ccxt `parse_position` 에 없음) → 스냅샷은 원장의 `leg_protection` 상태를 보고, 거래소 읽기 API 는 미구현 | 있으면 `ToobitExchange.get_position_protection(idx)` 를 구현해 스냅샷이 거래소 값을 쓰게 한다 |
| O1 | OKX 데모(`flag="1"`)에서 알고 주문·fills 응답이 실계정과 같은지 | 가정 | 데모 트레이딩 왕복 |
| O2 | OKX clOrdId 32자 절단으로 인한 재기동 뒤 알고 주문 역매핑 | `link_resolver`(실행기의 원장 `order_link_id` 접두사 검색)로 34자 link 복원 → 고아 스탑도 취소된다. 접두사가 모호(여러 주문)하면 절단 ID 그대로 → "not ours" 알림 | 재기동 후 `open_conditional_orders` 로그에 34자 link 가 보이는지 |

검증이 끝나면 이 표와 `RUNBOOK.md` §6.3, 래퍼 소스의 `VERIFY` 주석을 함께 갱신한다.
