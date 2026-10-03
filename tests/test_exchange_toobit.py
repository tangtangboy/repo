"""ToobitExchange 단위 테스트 — 네트워크 없음. 가짜 httpx 클라이언트가 요청을 기록하고 준비한 응답을 돌려준다.

검증 항목
  - 서명이 ccxt `toobit.sign` 과 정확히 같다 (독립 HMAC 계산 + 설치된 ccxt 와 직접 대조)
  - 헤더(X-BB-APIKEY, Content-Type), POST 본문/GET 쿼리 배치
  - side/quantity 매핑 (0.004 BTC × contractMultiplier 0.001 → '4', BUY_OPEN/SELL_CLOSE/SELL_OPEN/BUY_CLOSE)
  - 주문 status / 포지션 / 체결 매핑(계약 수 → BTC)
  - -1141 중복 clientOrderId → 기존 주문 조회로 멱등 성공
  - supports_lot_protection=False 와 set_position_protection(trading-stop) 페이로드
  - 오류 변환: 읽기 → ExchangeError, 쓰기 → ExchangeRejected, 타임아웃 → EXCHANGE_TIMEOUT
"""
from __future__ import annotations

import hashlib
import hmac
import json
from urllib.parse import parse_qsl, urlsplit

import pytest

from lake_executor.config import AccountSettings
from lake_executor.exchange import ExchangeError, ExchangeRejected, build_exchange
from lake_executor.exchange_toobit import (
    BASE_URL,
    CODE_DUPLICATE_CLIENT_ORDER_ID,
    ToobitExchange,
    ccxt_urlencode,
    sign_request,
)

API_KEY = "test-toobit-key-not-real"
API_SECRET = "test-toobit-secret-not-real-0123456789"
SYMBOL = "BTC-SWAP-USDT"
CLOCK_MS = 1_700_000_000_000

EXCHANGE_INFO = {
    "timezone": "UTC",
    "serverTime": "1755583099926",
    "symbols": [{"symbol": "ETHUSDT", "filters": []}],
    "contracts": [
        {
            "symbol": SYMBOL,
            "status": "TRADING",
            "contractMultiplier": "0.001",
            "inverse": False,
            "marginToken": "USDT",
            "filters": [
                {"minPrice": "0.1", "maxPrice": "10000000", "tickSize": "0.1", "filterType": "PRICE_FILTER"},
                {"minQty": "1", "maxQty": "100000", "stepSize": "1", "filterType": "LOT_SIZE"},
                {"minNotional": "5", "filterType": "MIN_NOTIONAL"},
            ],
        }
    ],
}


# --------------------------------------------------------------------------- #
# 가짜 클라이언트
# --------------------------------------------------------------------------- #
class FakeResp:
    def __init__(self, data, status_code: int = 200, text: str | None = None):
        self._data = data
        self.status_code = status_code
        self.text = text if text is not None else (json.dumps(data) if data is not None else "")

    def json(self):
        if self._data is None:
            raise ValueError("no json")
        return self._data


class FakeHttpClient:
    """httpx.Client 호환 최소 구현. (METHOD, path) 별 응답(또는 응답 리스트/예외/콜러블)을 돌려주고 모든 요청을 기록한다."""

    def __init__(self):
        self.calls: list[dict] = []
        self.routes: dict[tuple[str, str], list] = {}
        self.closed = False

    def route(self, method: str, path: str, *responses):
        self.routes.setdefault((method.upper(), path), []).extend(responses)
        return self

    def request(self, method: str, url: str, content=None, headers=None, timeout=None, **kw):
        parts = urlsplit(url)
        path = parts.path.lstrip("/")
        call = {
            "method": method.upper(),
            "url": url,
            "path": path,
            "query": dict(parse_qsl(parts.query, keep_blank_values=True)),
            "raw_query": parts.query,
            "content": content,
            "body": dict(parse_qsl(content.decode("utf-8"), keep_blank_values=True)) if content else {},
            "headers": dict(headers or {}),
            "timeout": timeout,
        }
        self.calls.append(call)
        q = self.routes.get((method.upper(), path))
        if not q:
            raise AssertionError(f"unexpected request {method} {path}")
        item = q.pop(0) if len(q) > 1 else q[0]
        if callable(item) and not isinstance(item, FakeResp):
            item = item(call)
        if isinstance(item, BaseException):
            raise item
        if isinstance(item, type) and issubclass(item, BaseException):
            raise item("fake transport error")
        if isinstance(item, FakeResp):
            return item
        return FakeResp(item)

    def close(self):
        self.closed = True

    # 편의
    def last(self, method: str | None = None, path: str | None = None) -> dict:
        for c in reversed(self.calls):
            if (method is None or c["method"] == method.upper()) and (path is None or c["path"] == path):
                return c
        raise AssertionError(f"no call {method} {path}")


def make_account(**over) -> AccountSettings:
    base = dict(name="toobit", exchange="toobit", enabled=True, symbol=SYMBOL, position_mode="hedge", leverage=5,
                margin_mode="isolated", testnet=False, env_prefix="TOOBIT", qty_multiplier=1.0, report=False,
                api_key=API_KEY, api_secret=API_SECRET)
    base.update(over)
    return AccountSettings(**base)


@pytest.fixture
def fake_http():
    c = FakeHttpClient()
    c.route("GET", "api/v1/exchangeInfo", EXCHANGE_INFO)
    return c


@pytest.fixture
def ex(fake_http):
    return ToobitExchange(make_account(), client=fake_http, clock=lambda: CLOCK_MS)


def expected_signature(payload: str) -> str:
    return hmac.new(API_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()


# --------------------------------------------------------------------------- #
# 서명
# --------------------------------------------------------------------------- #
def test_sign_post_matches_independent_hmac():
    params = {"symbol": SYMBOL, "side": "BUY_OPEN", "type": "LIMIT", "priceType": "MARKET", "quantity": "4",
              "newClientOrderId": "lkabc", "timeInForce": "IOC"}
    s = sign_request(API_SECRET, "POST", params, CLOCK_MS, 5000)
    payload = ccxt_urlencode({**params, "recvWindow": "5000", "timestamp": str(CLOCK_MS)})
    assert s["payload"] == payload
    assert s["signature"] == expected_signature(payload)
    assert s["query"] == ""                                         # POST: 쿼리 없음
    assert s["body"] == payload + "&signature=" + s["signature"]    # 서명은 본문 끝 (ccxt 와 동일)


def test_sign_get_matches_independent_hmac():
    params = {"symbol": SYMBOL, "clientOrderId": "lkabc"}
    s = sign_request(API_SECRET, "GET", params, CLOCK_MS, 5000)
    payload = ccxt_urlencode({**params, "recvWindow": "5000", "timestamp": str(CLOCK_MS)})
    assert s["payload"] == payload
    assert s["body"] is None
    assert s["query"] == payload + "&signature=" + expected_signature(payload)


@pytest.mark.parametrize("method,path,params", [
    ("POST", "api/v1/futures/order", {"symbol": SYMBOL, "side": "SELL_CLOSE", "type": "LIMIT", "priceType": "MARKET",
                                       "quantity": "4", "newClientOrderId": "lk0123456789abcdef", "timeInForce": "IOC"}),
    ("GET", "api/v1/futures/order", {"symbol": SYMBOL, "clientOrderId": "lk0123456789abcdef"}),
    ("DELETE", "api/v1/futures/order", {"symbol": SYMBOL, "clientOrderId": "lk0123456789abcdef"}),
    ("GET", "api/v1/futures/userTrades", {"symbol": SYMBOL, "startTime": "1699999000000", "limit": "500"}),
    ("POST", "api/v1/futures/position/trading-stop", {"symbol": SYMBOL, "side": "LONG", "stopLoss": "84000.0",
                                                       "takeProfit": "0", "slTriggerBy": "MARK_PRICE", "tpTriggerBy": "MARK_PRICE"}),
])
def test_sign_matches_installed_ccxt(method, path, params):
    """설치된 ccxt 4.5.85 `toobit.sign` 과 URL/본문/헤더를 직접 대조 (milliseconds 를 고정)."""
    ccxt = pytest.importorskip("ccxt")
    t = ccxt.toobit({"apiKey": API_KEY, "secret": API_SECRET})
    t.milliseconds = lambda: CLOCK_MS
    ref = t.sign(path, "private", method, dict(params))
    s = sign_request(API_SECRET, method, params, CLOCK_MS, 5000)
    url = BASE_URL + "/" + path + ("?" + s["query"] if s["query"] else "")
    assert url == ref["url"]
    assert s["body"] == ref["body"]
    assert ref["headers"]["X-BB-APIKEY"] == API_KEY
    assert ref["headers"]["Content-Type"] == "application/x-www-form-urlencoded"


def test_private_post_request_shape(ex, fake_http):
    fake_http.route("POST", "api/v1/futures/order", {"orderId": "9001", "clientOrderId": "lkabc", "status": "PENDING_NEW"})
    ex.place_market("Buy", 0.004, 1, False, "lkabc")
    c = fake_http.last("POST", "api/v1/futures/order")
    assert c["url"] == BASE_URL + "/api/v1/futures/order"           # POST: 쿼리 없음
    assert c["headers"]["X-BB-APIKEY"] == API_KEY
    assert c["headers"]["Content-Type"] == "application/x-www-form-urlencoded"
    assert c["timeout"] == ex.http_timeout_s
    body = c["content"].decode()
    assert body.endswith("&signature=" + c["body"]["signature"])
    unsigned = body.rsplit("&signature=", 1)[0]
    assert c["body"]["signature"] == expected_signature(unsigned)
    # 키 순서: 파라미터 → recvWindow → timestamp → signature
    keys = [kv.split("=")[0] for kv in body.split("&")]
    assert keys[-3:] == ["recvWindow", "timestamp", "signature"]
    assert c["body"]["timestamp"] == str(CLOCK_MS) and c["body"]["recvWindow"] == "5000"
    # 비밀값이 본문/URL/헤더 어디에도 평문으로 없다
    assert API_SECRET not in body and API_SECRET not in c["url"] and API_SECRET not in json.dumps(c["headers"])


def test_private_get_puts_signature_in_query(ex, fake_http):
    fake_http.route("GET", "api/v1/futures/order", {"orderId": "9001", "clientOrderId": "lkabc", "status": "NEW",
                                                     "origQty": "4", "executedQty": "0", "avgPrice": "0", "side": "BUY_OPEN", "type": "LIMIT"})
    ex.get_order("lkabc")
    c = fake_http.last("GET", "api/v1/futures/order")
    assert c["content"] is None
    assert c["headers"]["X-BB-APIKEY"] == API_KEY
    q = c["raw_query"]
    unsigned = q.rsplit("&signature=", 1)[0]
    assert c["query"]["signature"] == expected_signature(unsigned)
    assert c["query"]["clientOrderId"] == "lkabc" and c["query"]["symbol"] == SYMBOL


def test_public_get_is_unsigned(ex, fake_http):
    ex.instrument()
    c = fake_http.last("GET", "api/v1/exchangeInfo")
    assert "signature" not in c["query"] and "X-BB-APIKEY" not in c["headers"] and c["content"] is None


def test_missing_keys_rejects_private_calls(fake_http):
    ex = ToobitExchange(make_account(api_key="", api_secret=""), client=fake_http, clock=lambda: CLOCK_MS)
    with pytest.raises(ExchangeError) as ei:
        ex.positions()
    assert ei.value.code == "EXCHANGE_ERROR"
    assert ex.instrument()["qty_step"] == pytest.approx(0.001)   # 공개 엔드포인트는 키 없이 동작


# --------------------------------------------------------------------------- #
# 계약 정보 / 단위 변환
# --------------------------------------------------------------------------- #
def test_instrument_converts_contracts_to_btc(ex, fake_http):
    info = ex.instrument()
    assert info["qty_step"] == pytest.approx(0.001)
    assert info["min_qty"] == pytest.approx(0.001)
    assert info["max_qty"] == pytest.approx(100.0)
    assert info["tick"] == pytest.approx(0.1)
    assert info["contract_multiplier"] == pytest.approx(0.001)
    assert info["contract_step"] == 1.0
    ex.instrument()
    assert len([c for c in fake_http.calls if c["path"] == "api/v1/exchangeInfo"]) == 1   # 캐시


def test_instrument_symbol_missing_is_error(fake_http):
    fake_http.routes.clear()
    fake_http.route("GET", "api/v1/exchangeInfo", {"contracts": [{"symbol": "ETH-SWAP-USDT", "contractMultiplier": "0.01"}]})
    ex = ToobitExchange(make_account(), client=fake_http, clock=lambda: CLOCK_MS)
    with pytest.raises(ExchangeError):
        ex.instrument()


def test_build_exchange_returns_toobit():
    ex = build_exchange(make_account(), "toobit")
    assert isinstance(ex, ToobitExchange)
    assert ex.display_name == "Toobit" and ex.name == "toobit"
    ex.close()


# --------------------------------------------------------------------------- #
# 주문 매핑
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("side,idx,reduce_only,native", [
    ("Buy", 1, False, "BUY_OPEN"),
    ("Sell", 1, True, "SELL_CLOSE"),
    ("Sell", 2, False, "SELL_OPEN"),
    ("Buy", 2, True, "BUY_CLOSE"),
])
def test_place_market_side_and_quantity(ex, fake_http, side, idx, reduce_only, native):
    fake_http.route("POST", "api/v1/futures/order", {"orderId": "777", "clientOrderId": "lkm", "status": "PENDING_NEW"})
    r = ex.place_market(side, 0.004, idx, reduce_only, "lkm")
    assert r == {"order_id": "777", "order_link_id": "lkm"}
    b = fake_http.last("POST", "api/v1/futures/order")["body"]
    assert b["symbol"] == SYMBOL
    assert b["side"] == native
    assert b["quantity"] == "4"                 # 0.004 BTC / 0.001 → 4 계약 (float 잡음 없이)
    assert b["type"] == "LIMIT" and b["priceType"] == "MARKET"
    assert b["timeInForce"] == "IOC"
    assert b["newClientOrderId"] == "lkm"
    assert "reduceOnly" not in b and "positionIdx" not in b


def test_place_market_quantity_floors_to_step(ex, fake_http):
    fake_http.route("POST", "api/v1/futures/order", {"orderId": "1", "clientOrderId": "x"})
    ex.place_market("Buy", 0.0049, 1, False, "x")
    assert fake_http.last("POST")["body"]["quantity"] == "4"
    with pytest.raises(ExchangeRejected):
        ex.place_market("Buy", 0.0004, 1, False, "y")     # 1 계약 미만 → 거부, 전송 안 함
    assert len([c for c in fake_http.calls if c["method"] == "POST"]) == 1


@pytest.mark.parametrize("side,idx,reduce_only", [("Sell", 1, False), ("Buy", 1, True), ("Buy", 2, False), ("Sell", 2, True)])
def test_place_market_side_mismatch_is_value_error(ex, fake_http, side, idx, reduce_only):
    with pytest.raises(ValueError):
        ex.place_market(side, 0.004, idx, reduce_only, "lk")
    assert not [c for c in fake_http.calls if c["method"] == "POST"]


def test_place_market_one_way_idx_rejected(ex):
    with pytest.raises(ValueError):
        ex.place_market("Buy", 0.004, 0, False, "lk")


def test_duplicate_client_order_id_is_idempotent_success(ex, fake_http):
    fake_http.route("POST", "api/v1/futures/order",
                    FakeResp({"code": CODE_DUPLICATE_CLIENT_ORDER_ID, "msg": "Duplicate clientOrderId"}, status_code=400))
    fake_http.route("GET", "api/v1/futures/order",
                    {"orderId": "555", "clientOrderId": "lkdup", "status": "FILLED", "origQty": "4", "executedQty": "4",
                     "avgPrice": "85000.5", "side": "BUY_OPEN", "type": "LIMIT"})
    r = ex.place_market("Buy", 0.004, 1, False, "lkdup")
    assert r == {"order_id": "555", "order_link_id": "lkdup"}
    assert fake_http.last("GET", "api/v1/futures/order")["query"]["clientOrderId"] == "lkdup"


def test_duplicate_without_existing_order_reraises(ex, fake_http):
    fake_http.route("POST", "api/v1/futures/order",
                    FakeResp({"code": -1141, "msg": "Duplicate clientOrderId"}, status_code=400))
    fake_http.route("GET", "api/v1/futures/order", FakeResp({"code": -2013, "msg": "Order does not exist."}, status_code=400))
    with pytest.raises(ExchangeRejected) as ei:
        ex.place_market("Buy", 0.004, 1, False, "lkdup2")
    assert ei.value.ret_code == -1141


def test_write_rejection_maps_to_exchange_rejected(ex, fake_http):
    fake_http.route("POST", "api/v1/futures/order", FakeResp({"code": -1131, "msg": "Balance insufficient"}, status_code=400))
    with pytest.raises(ExchangeRejected) as ei:
        ex.place_market("Buy", 0.004, 1, False, "lk1")
    assert ei.value.ret_code == -1131 and ei.value.code == "EXCHANGE_REJECTED"


def test_place_conditional_payload_marked_verify(ex, fake_http):
    fake_http.route("POST", "api/v1/futures/order", {"orderId": "31", "clientOrderId": "lkc"})
    r = ex.place_conditional("Sell", 0.004, 1, 84000.04, 2, "lkc", "MarkPrice")
    assert r["order_id"] == "31"
    b = fake_http.last("POST")["body"]
    assert b["side"] == "SELL_CLOSE" and b["type"] == "STOP" and b["priceType"] == "MARKET"
    assert b["stopPrice"] == "84000.0" and b["quantity"] == "4" and b["newClientOrderId"] == "lkc"
    with pytest.raises(ValueError):
        ex.place_conditional("Sell", 0.004, 1, 84000, 3, "lkc2", "MarkPrice")


# --------------------------------------------------------------------------- #
# 취소 / 조회
# --------------------------------------------------------------------------- #
def test_cancel_order_sends_delete_with_client_order_id(ex, fake_http):
    fake_http.route("DELETE", "api/v1/futures/order", {"orderId": "1", "clientOrderId": "lkx", "status": "CANCELED"})
    assert ex.cancel_order("lkx") is True
    c = fake_http.last("DELETE", "api/v1/futures/order")
    assert c["body"]["clientOrderId"] == "lkx" and c["body"]["symbol"] == SYMBOL
    assert c["url"] == BASE_URL + "/api/v1/futures/order"
    assert c["headers"]["Content-Type"] == "application/x-www-form-urlencoded"


@pytest.mark.parametrize("code", [-1139, -1142, -1143, -2013])
def test_cancel_order_gone_codes_are_true(ex, fake_http, code):
    fake_http.route("DELETE", "api/v1/futures/order", FakeResp({"code": code, "msg": "gone"}, status_code=400))
    assert ex.cancel_order("lkx") is True


def test_cancel_order_other_rejection_raises(ex, fake_http):
    fake_http.route("DELETE", "api/v1/futures/order", FakeResp({"code": -1022, "msg": "Signature for this request is not valid."}, status_code=400))
    with pytest.raises(ExchangeRejected) as ei:
        ex.cancel_order("lkx")
    assert ei.value.ret_code == -1022


@pytest.mark.parametrize("raw,mapped", [
    ("NEW", "New"), ("PENDING_NEW", "New"), ("PARTIALLY_FILLED", "PartiallyFilled"), ("FILLED", "Filled"),
    ("CANCELED", "Cancelled"), ("PENDING_CANCEL", "Cancelled"), ("REJECTED", "Rejected"),
])
def test_get_order_status_mapping(ex, fake_http, raw, mapped):
    fake_http.route("GET", "api/v1/futures/order",
                    {"orderId": "42", "clientOrderId": "lkq", "status": raw, "origQty": "4", "executedQty": "3",
                     "avgPrice": "85010.5", "side": "SELL_CLOSE", "type": "LIMIT", "stopPrice": "0"})
    o = ex.get_order("lkq")
    assert o is not None
    assert o["status"] == mapped
    assert o["order_id"] == "42" and o["order_link_id"] == "lkq"
    assert o["qty"] == pytest.approx(0.004) and o["cum_qty"] == pytest.approx(0.003)
    assert o["avg_price"] == pytest.approx(85010.5)
    assert o["trigger_price"] is None
    assert o["side"] == "Sell" and o["position_idx"] == 1 and o["reduce_only"] is True


def test_get_order_stop_order_is_untriggered(ex, fake_http):
    fake_http.route("GET", "api/v1/futures/order",
                    {"orderId": "43", "clientOrderId": "lks", "status": "NEW", "origQty": "4", "executedQty": "0",
                     "avgPrice": "0", "side": "BUY_CLOSE", "type": "STOP", "stopPrice": "90000"})
    o = ex.get_order("lks")
    assert o["status"] == "Untriggered" and o["trigger_price"] == 90000.0
    assert o["avg_price"] is None and o["position_idx"] == 2


@pytest.mark.parametrize("code", [-2013, -1143])
def test_get_order_not_found_is_none(ex, fake_http, code):
    fake_http.route("GET", "api/v1/futures/order", FakeResp({"code": code, "msg": "Order does not exist."}, status_code=400))
    assert ex.get_order("lknone") is None


def test_get_order_other_read_error_raises_exchange_error(ex, fake_http):
    fake_http.route("GET", "api/v1/futures/order", FakeResp({"code": -1021, "msg": "Timestamp outside recvWindow"}, status_code=400))
    with pytest.raises(ExchangeError) as ei:
        ex.get_order("lk")
    assert ei.value.code == "EXCHANGE_ERROR" and ei.value.ret_code == -1021
    assert not isinstance(ei.value, ExchangeRejected)


def test_executions_filters_by_order_id_and_converts_qty(ex, fake_http):
    fake_http.route("GET", "api/v1/futures/userTrades", [
        {"time": "1756758426899", "id": "t2", "orderId": "77", "symbol": SYMBOL, "price": "85001", "qty": "3", "side": "BUY_OPEN"},
        {"time": "1756758426800", "id": "t1", "orderId": "77", "symbol": SYMBOL, "price": "85000", "qty": "1", "side": "BUY_OPEN"},
        {"time": "1756758426700", "id": "t0", "orderId": "99", "symbol": SYMBOL, "price": "84000", "qty": "5", "side": "SELL_OPEN"},
    ])
    xs = ex.executions("77")
    assert [x["exec_id"] for x in xs] == ["t1", "t2"]                 # 시간순
    assert xs[0] == {"exec_id": "t1", "qty": pytest.approx(0.001), "price": 85000.0, "exec_time_ms": 1756758426800, "order_id": "77"}
    assert xs[1]["qty"] == pytest.approx(0.003)
    q = fake_http.last("GET", "api/v1/futures/userTrades")["query"]
    assert q["symbol"] == SYMBOL and q["limit"] == str(ex.trades_limit)
    assert int(q["startTime"]) == CLOCK_MS - ex.trades_lookback_ms


def test_positions_mapping(ex, fake_http):
    fake_http.route("GET", "api/v1/futures/positions", [
        {"symbol": SYMBOL, "side": "LONG", "avgPrice": "85000.5", "position": "4", "leverage": "5", "markPrice": "85100.1"},
        {"symbol": SYMBOL, "side": "SHORT", "avgPrice": "0", "position": "0", "leverage": "5", "markPrice": "85100.1"},
        {"symbol": "ETH-SWAP-USDT", "side": "SHORT", "avgPrice": "3000", "position": "10", "markPrice": "3001"},
    ])
    ps = ex.positions()
    assert set(ps) == {1}
    p = ps[1]
    assert p["position_idx"] == 1 and p["side"] == "Buy"
    assert p["size"] == pytest.approx(0.004)
    assert p["avg_price"] == 85000.5 and p["mark_price"] == 85100.1
    assert p["updated_time_ms"] == 0
    assert fake_http.last("GET", "api/v1/futures/positions")["query"]["symbol"] == SYMBOL


def test_positions_short_is_idx2(ex, fake_http):
    fake_http.route("GET", "api/v1/futures/positions", [
        {"symbol": SYMBOL, "side": "SHORT", "avgPrice": "86000", "position": "12", "markPrice": "85900"},
    ])
    ps = ex.positions()
    assert list(ps) == [2] and ps[2]["side"] == "Sell" and ps[2]["size"] == pytest.approx(0.012)


def test_open_conditional_orders_filters_stop_orders_by_idx(ex, fake_http):
    fake_http.route("GET", "api/v1/futures/openOrders", [
        {"orderId": "1", "clientOrderId": "lkA", "status": "NEW", "origQty": "4", "executedQty": "0", "side": "SELL_CLOSE",
         "type": "STOP", "stopPrice": "84000"},
        {"orderId": "2", "clientOrderId": "lkB", "status": "NEW", "origQty": "2", "executedQty": "0", "side": "BUY_CLOSE",
         "type": "STOP", "stopPrice": "90000"},
        {"orderId": "3", "clientOrderId": "lkC", "status": "NEW", "origQty": "2", "executedQty": "0", "side": "BUY_OPEN",
         "type": "LIMIT", "stopPrice": "0", "price": "80000"},
    ])
    xs = ex.open_conditional_orders(1)
    assert xs == [{"order_link_id": "lkA", "order_id": "1", "trigger_price": 84000.0, "qty": pytest.approx(0.004), "side": "Sell"}]
    assert [x["order_link_id"] for x in ex.open_conditional_orders(2)] == ["lkB"]


# --------------------------------------------------------------------------- #
# 시세 / 계정 설정
# --------------------------------------------------------------------------- #
def test_last_price_and_mark_price(ex, fake_http):
    fake_http.route("GET", "quote/v1/contract/ticker/price", [{"s": SYMBOL, "p": "85123.4"}])
    fake_http.route("GET", "quote/v1/markPrice", {"symbol": SYMBOL, "markPrice": "85120.0", "time": 1})
    assert ex.last_price() == 85123.4
    assert ex.mark_price() == 85120.0
    assert fake_http.last("GET", "quote/v1/contract/ticker/price")["query"] == {"symbol": SYMBOL}


def test_last_price_falls_back_to_24hr_ticker(ex, fake_http):
    fake_http.route("GET", "quote/v1/contract/ticker/price", FakeResp({"code": -1121, "msg": "Invalid symbol."}, status_code=400))
    fake_http.route("GET", "quote/v1/ticker/price", FakeResp(None, status_code=404, text="not found"))
    fake_http.route("GET", "quote/v1/contract/ticker/24hr", [{"s": SYMBOL, "c": "85000.0", "h": "1", "l": "1"}])
    assert ex.last_price() == 85000.0


def test_mark_price_unknown_shape_is_none(ex, fake_http):
    fake_http.route("GET", "quote/v1/markPrice", {"something": "else"})
    assert ex.mark_price() is None


def test_ensure_account_setup_payloads(ex, fake_http):
    fake_http.route("POST", "api/v1/futures/leverage", {"code": 200, "symbolId": SYMBOL, "leverage": "5"})
    fake_http.route("POST", "api/v1/futures/marginType", {"code": 200, "symbolId": SYMBOL, "marginType": "ISOLATED"})
    ex.ensure_account_setup("hedge", 5, "isolated")
    assert fake_http.last("POST", "api/v1/futures/leverage")["body"]["leverage"] == "5"
    assert fake_http.last("POST", "api/v1/futures/marginType")["body"]["marginType"] == "ISOLATED"
    ex.ensure_account_setup("hedge", 5, "cross")
    assert fake_http.last("POST", "api/v1/futures/marginType")["body"]["marginType"] == "CROSS"
    with pytest.raises(ValueError):
        ex.ensure_account_setup("one_way", 5, "isolated")


def test_ensure_account_setup_already_set_is_ok(ex, fake_http):
    fake_http.route("POST", "api/v1/futures/leverage", FakeResp({"code": -1020, "msg": "No need to change leverage"}, status_code=400))
    fake_http.route("POST", "api/v1/futures/marginType", FakeResp({"code": -1020, "msg": "margin type already ISOLATED"}, status_code=400))
    ex.ensure_account_setup("hedge", 5, "isolated")


# --------------------------------------------------------------------------- #
# 포지션 단위 보호 (trading-stop)
# --------------------------------------------------------------------------- #
def test_supports_lot_protection_is_false_by_default():
    assert ToobitExchange.supports_lot_protection is False
    ex = ToobitExchange(make_account(), client=FakeHttpClient(), clock=lambda: CLOCK_MS)
    assert ex.supports_lot_protection is False
    ex.supports_lot_protection = True      # 실계정 검증 후 인스턴스/클래스에서 켤 수 있다
    assert ex.supports_lot_protection is True and ToobitExchange.supports_lot_protection is False


def test_set_position_protection_payload(ex, fake_http):
    fake_http.route("POST", "api/v1/futures/position/trading-stop", {"code": 200})
    r = ex.set_position_protection(1, 84000.04, 90000.0)
    assert r == {"position_idx": 1, "stop_loss": 84000.0, "take_profit": 90000.0}
    c = fake_http.last("POST", "api/v1/futures/position/trading-stop")
    b = c["body"]
    assert b["symbol"] == SYMBOL and b["side"] == "LONG"
    assert b["stopLoss"] == "84000.0" and b["takeProfit"] == "90000.0"
    assert b["slTriggerBy"] == "MARK_PRICE" and b["tpTriggerBy"] == "MARK_PRICE"
    assert c["headers"]["X-BB-APIKEY"] == API_KEY
    unsigned = c["content"].decode().rsplit("&signature=", 1)[0]
    assert b["signature"] == expected_signature(unsigned)


def test_set_position_protection_none_clears_and_short_side(ex, fake_http):
    fake_http.route("POST", "api/v1/futures/position/trading-stop", {"code": 200})
    r = ex.set_position_protection(2, None, 80000.0)
    assert r == {"position_idx": 2, "stop_loss": None, "take_profit": 80000.0}
    b = fake_http.last("POST")["body"]
    assert b["side"] == "SHORT" and b["stopLoss"] == ex.protection_clear_value and b["takeProfit"] == "80000.0"
    ex.set_position_protection(2, None, None)
    b = fake_http.last("POST")["body"]
    assert b["stopLoss"] == "0" and b["takeProfit"] == "0"


def test_set_position_protection_rejection_and_bad_idx(ex, fake_http):
    fake_http.route("POST", "api/v1/futures/position/trading-stop", FakeResp({"code": -1004, "msg": "Bad request"}, status_code=400))
    with pytest.raises(ExchangeRejected) as ei:
        ex.set_position_protection(1, 84000.0, None)
    assert ei.value.ret_code == -1004
    with pytest.raises(ValueError):
        ex.set_position_protection(0, 84000.0, None)


# --------------------------------------------------------------------------- #
# 네트워크 오류
# --------------------------------------------------------------------------- #
def test_timeout_maps_to_exchange_timeout(fake_http):
    httpx = pytest.importorskip("httpx")
    ex = ToobitExchange(make_account(), client=fake_http, clock=lambda: CLOCK_MS)
    ex.read_retry_sleep_s = 0.0
    fake_http.route("POST", "api/v1/futures/order", httpx.ReadTimeout("timed out"))
    with pytest.raises(ExchangeError) as ei:
        ex.place_market("Buy", 0.004, 1, False, "lkto")
    assert ei.value.code == "EXCHANGE_TIMEOUT"
    assert len([c for c in fake_http.calls if c["method"] == "POST"]) == 1        # 쓰기는 재시도 없음
    fake_http.route("GET", "api/v1/futures/positions", httpx.ConnectTimeout("t"), httpx.ConnectTimeout("t"), httpx.ConnectTimeout("t"))
    with pytest.raises(ExchangeError) as ei:
        ex.positions()
    assert ei.value.code == "EXCHANGE_TIMEOUT"
    assert len([c for c in fake_http.calls if c["path"] == "api/v1/futures/positions"]) == ex.read_attempts   # 읽기는 재시도


def test_read_retries_then_succeeds(fake_http):
    ex = ToobitExchange(make_account(), client=fake_http, clock=lambda: CLOCK_MS)
    ex.read_retry_sleep_s = 0.0
    fake_http.route("GET", "api/v1/futures/positions", ConnectionError("reset"), [])
    assert ex.positions() == {}
    assert len([c for c in fake_http.calls if c["path"] == "api/v1/futures/positions"]) == 2


def test_http_504_without_json_is_timeout(fake_http):
    ex = ToobitExchange(make_account(), client=fake_http, clock=lambda: CLOCK_MS)
    ex.read_retry_sleep_s = 0.0
    fake_http.route("POST", "api/v1/futures/order", FakeResp(None, status_code=504, text="gateway timeout"))
    with pytest.raises(ExchangeError) as ei:
        ex.place_market("Buy", 0.004, 1, False, "lk504")
    assert ei.value.code == "EXCHANGE_TIMEOUT"


def test_signature_timestamp_is_fresh_per_attempt(fake_http):
    ticks = iter([CLOCK_MS, CLOCK_MS + 1000, CLOCK_MS + 2000])
    ex = ToobitExchange(make_account(), client=fake_http, clock=lambda: next(ticks))
    ex.read_retry_sleep_s = 0.0
    fake_http.route("GET", "api/v1/futures/positions", ConnectionError("reset"), [])
    ex.positions()
    ts = [c["query"]["timestamp"] for c in fake_http.calls if c["path"] == "api/v1/futures/positions"]
    assert ts == [str(CLOCK_MS), str(CLOCK_MS + 1000)]


# --------------------------------------------------------------------------- #
# 결과를 알 수 없는 쓰기 응답 → 거부가 아니라 오류 (실행기가 조회/재확인)
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("code,expected", [
    (-1007, "EXCHANGE_TIMEOUT"),   # Timeout waiting for response from backend server; execution status unknown
    (-1006, "EXCHANGE_TIMEOUT"),   # unexpected response from the message bus; execution status unknown
    (-1146, "EXCHANGE_TIMEOUT"),   # Order creation timeout
    (-1147, "EXCHANGE_TIMEOUT"),   # Order cancellation timeout
    (-1000, "EXCHANGE_ERROR"),     # unknown error
    (-1001, "EXCHANGE_ERROR"),     # internal error
])
def test_uncertain_write_codes_are_errors_not_rejections(ex, fake_http, code, expected):
    fake_http.route("POST", "api/v1/futures/order", FakeResp({"code": code, "msg": "execution status unknown"}, status_code=500))
    with pytest.raises(ExchangeError) as ei:
        ex.place_market("Buy", 0.004, 1, False, "lkunk")
    assert not isinstance(ei.value, ExchangeRejected)
    assert ei.value.code == expected and ei.value.ret_code == code
    assert len([c for c in fake_http.calls if c["method"] == "POST"]) == 1    # 쓰기는 재시도 없음


def test_http_5xx_json_without_code_on_write_is_error_not_rejection(ex, fake_http):
    fake_http.route("POST", "api/v1/futures/order", FakeResp({"msg": "bad gateway"}, status_code=502),
                    FakeResp({"msg": "forbidden"}, status_code=403))
    with pytest.raises(ExchangeError) as ei:
        ex.place_market("Buy", 0.004, 1, False, "lk502")
    assert not isinstance(ei.value, ExchangeRejected) and ei.value.code == "EXCHANGE_ERROR" and ei.value.ret_code is None
    # 코드 없는 4xx 는 여전히 거부
    with pytest.raises(ExchangeRejected):
        ex.place_market("Buy", 0.004, 1, False, "lk403")


def test_uncertain_code_on_read_is_plain_exchange_error(ex, fake_http):
    ex.read_retry_sleep_s = 0.0
    fake_http.route("GET", "api/v1/futures/positions", FakeResp({"code": -1007, "msg": "timeout"}, status_code=500))
    with pytest.raises(ExchangeError) as ei:
        ex.positions()
    assert ei.value.code == "EXCHANGE_ERROR" and ei.value.ret_code == -1007


def test_testnet_account_is_refused_by_wrapper():
    with pytest.raises(ValueError, match="no testnet"):
        ToobitExchange(make_account(testnet=True), client=FakeHttpClient(), clock=lambda: CLOCK_MS)


# --------------------------------------------------------------------------- #
# executions(): 주문 생성 시각으로 창을 좁히고 가득 찬 페이지는 이어 읽는다
# --------------------------------------------------------------------------- #
def test_executions_narrows_window_to_order_creation_and_paginates(fake_http):
    ticks = iter([CLOCK_MS])          # 첫 호출(주문 생성 시각) 만 CLOCK_MS, 이후는 5초 뒤
    ex = ToobitExchange(make_account(), client=fake_http, clock=lambda: next(ticks, CLOCK_MS + 5000))
    ex.trades_limit = 2
    fake_http.route("POST", "api/v1/futures/order", {"orderId": "77", "clientOrderId": "lkpg"})
    ex.place_market("Buy", 0.004, 1, False, "lkpg")                       # 생성 시각 = CLOCK_MS
    page1 = [
        {"time": str(CLOCK_MS + 10), "id": "t1", "orderId": "77", "symbol": SYMBOL, "price": "85000", "qty": "1"},
        {"time": str(CLOCK_MS + 20), "id": "x1", "orderId": "99", "symbol": SYMBOL, "price": "85000", "qty": "9"},
    ]
    page2 = [
        {"time": str(CLOCK_MS + 30), "id": "t2", "orderId": "77", "symbol": SYMBOL, "price": "85001", "qty": "3"},
    ]
    fake_http.route("GET", "api/v1/futures/userTrades", page1, page2)
    xs = ex.executions("77")
    assert [x["exec_id"] for x in xs] == ["t1", "t2"] and xs[1]["qty"] == pytest.approx(0.003)
    calls = [c for c in fake_http.calls if c["path"] == "api/v1/futures/userTrades"]
    assert len(calls) == 2
    assert int(calls[0]["query"]["startTime"]) == CLOCK_MS - ex.trades_start_margin_ms   # 24h 창이 아니라 주문 생성 시각
    assert int(calls[1]["query"]["startTime"]) == CLOCK_MS + 21                          # 마지막 체결 시각 + 1
    assert calls[0]["query"]["limit"] == "2"


def test_executions_unknown_order_uses_lookback_and_stops_on_short_page(ex, fake_http):
    fake_http.route("GET", "api/v1/futures/userTrades", [
        {"time": "1756758426800", "id": "t1", "orderId": "55", "symbol": SYMBOL, "price": "85000", "qty": "1"},
    ])
    xs = ex.executions("55")
    assert [x["exec_id"] for x in xs] == ["t1"]
    calls = [c for c in fake_http.calls if c["path"] == "api/v1/futures/userTrades"]
    assert len(calls) == 1 and int(calls[0]["query"]["startTime"]) == CLOCK_MS - ex.trades_lookback_ms


def test_executions_page_cap_stops_runaway_pagination(ex, fake_http):
    ex.trades_limit = 1
    ex.trades_max_pages = 3
    rows = [{"time": str(CLOCK_MS + i), "id": f"t{i}", "orderId": "1", "symbol": SYMBOL, "price": "1", "qty": "1"}
            for i in range(10)]
    fake_http.route("GET", "api/v1/futures/userTrades", *[[r] for r in rows])
    xs = ex.executions("1")
    assert len(xs) == 3
    assert len([c for c in fake_http.calls if c["path"] == "api/v1/futures/userTrades"]) == 3
