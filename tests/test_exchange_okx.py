"""OkxExchange (lake_executor/exchange_okx.py) — python-okx 클라이언트를 가짜 객체로 바꿔 파라미터 매핑을 검증한다.

실 API 호출 없음 (ARCHITECTURE_MULTI_EXCHANGE.md §7). 검증 항목:
  - instrument(): ctVal/lotSz/minSz 로 BTC 단위 환산 (min_qty 0.01 BTC 노출), 캐시
  - place_market: instId/tdMode/side/posSide/ordType/sz(0.02 BTC → '2')/reduceOnly(net 모드만)/clOrdId(영숫자 32자)
  - place_conditional: SL/TP 분기(side × trigger_direction), 트리거 가격 종류, algoClOrdId
  - get_order: 일반 주문 state 매핑, 알고 주문 폴백(live→Untriggered, effective→생성 주문 추적), 없음→None
  - executions: tradeId/fillSz×ctVal/fillPx/ts, algoId 로 호출해도 생성된 ordId 의 체결을 읽음
  - cancel_order: 일반 → 알고 폴백, '이미 없음' 코드는 True
  - open_conditional_orders / positions / ensure_account_setup 매핑
  - 오류: code/sCode != '0' → 쓰기 ExchangeRejected(ret_code) / 읽기 ExchangeError, 타임아웃 → EXCHANGE_TIMEOUT, 읽기 재시도
"""
from __future__ import annotations

import httpx
import pytest

from lake_executor.config import AccountSettings
from lake_executor.exchange import ExchangeError, ExchangeRejected, build_exchange
from lake_executor.exchange_okx import OkxExchange, clordid_of
from lake_executor.util import alnum_only, order_link_id

SYMBOL = "BTC-USDT-SWAP"
INSTRUMENT_RESP = {"code": "0", "msg": "", "data": [{
    "instId": SYMBOL, "instType": "SWAP", "ctVal": "0.01", "ctValCcy": "BTC", "lotSz": "1", "minSz": "1",
    "tickSz": "0.1", "maxMktSz": "10000", "maxLmtSz": "100000",
}]}


def ok(*items: dict) -> dict:
    return {"code": "0", "msg": "", "data": list(items)}


def err(code: str, msg: str = "", *, scode: str | None = None, with_data: bool = True) -> dict:
    """OKX 오류 응답. scode 를 주면 data[0].sCode 형태(주문 API), 아니면 최상위 code 만."""
    if scode is not None:
        return {"code": "1", "msg": "Operation failed.", "data": [{"sCode": scode, "sMsg": msg, "clOrdId": "", "ordId": ""}]}
    return {"code": code, "msg": msg, "data": [] if with_data else None}


class FakeAPI:
    """python-okx *API 대체. 호출을 (메서드, args, kwargs) 로 기록하고, 메서드별 응답 큐(또는 고정 응답)를 돌려준다.
    큐 항목이 예외(인스턴스/클래스)면 raise."""

    def __init__(self, **responses):
        self.calls: list[tuple[str, tuple, dict]] = []
        self._fixed: dict[str, object] = {}
        self._queue: dict[str, list] = {}
        for k, v in responses.items():
            self.set(k, v)

    def set(self, name: str, resp) -> "FakeAPI":
        self._fixed[name] = resp
        return self

    def queue(self, name: str, *items) -> "FakeAPI":
        self._queue.setdefault(name, []).extend(items)
        return self

    def kwargs(self, name: str, n: int = -1) -> dict:
        found = [c for c in self.calls if c[0] == name]
        assert found, f"{name} was not called"
        return found[n][2]

    def args(self, name: str, n: int = -1) -> tuple:
        found = [c for c in self.calls if c[0] == name]
        assert found, f"{name} was not called"
        return found[n][1]

    def count(self, name: str) -> int:
        return sum(1 for c in self.calls if c[0] == name)

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)

        def method(*args, **kwargs):
            self.calls.append((name, args, dict(kwargs)))
            q = self._queue.get(name)
            if q:
                item = q.pop(0)
            elif name in self._fixed:
                item = self._fixed[name]
            else:
                raise AssertionError(f"no fake response configured for {name}")
            if isinstance(item, BaseException):
                raise item
            if isinstance(item, type) and issubclass(item, BaseException):
                raise item("fake transport error")
            return item

        return method


def account(**over) -> AccountSettings:
    base = dict(name="okx", exchange="okx", enabled=True, symbol=SYMBOL, position_mode="hedge", leverage=5,
                margin_mode="isolated", testnet=False, env_prefix="OKX", qty_multiplier=1.0, report=False)
    base.update(over)
    return AccountSettings(**base)


def make(acct: AccountSettings | None = None, **clients) -> tuple[OkxExchange, FakeAPI, FakeAPI, FakeAPI, FakeAPI]:
    trade = clients.get("trade") or FakeAPI()
    acc = clients.get("account") or FakeAPI()
    public = clients.get("public") or FakeAPI(get_instruments=INSTRUMENT_RESP)
    market = clients.get("market") or FakeAPI()
    ex = OkxExchange(acct or account(), clients={"trade": trade, "account": acc, "public": public, "market": market})
    ex.read_retry_sleep_s = 0.0
    return ex, trade, acc, public, market


LINK = order_link_id("test", "event-1")          # "lk" + 32 hex = 34자 → clOrdId 는 32자로 절단
CID = alnum_only(LINK, 32)


# --------------------------------------------------------------------------- #
# 기본 속성 / 생성
# --------------------------------------------------------------------------- #
def test_identity_and_flags():
    ex, *_ = make()
    assert ex.name == "okx" and ex.display_name == "OKX" and ex.supports_lot_protection is True
    assert ex.account.name == "okx" and ex.settings is ex.account and ex.symbol == SYMBOL
    assert ex.flag == "0" and ex.td_mode == "isolated"
    with pytest.raises(NotImplementedError):
        ex.set_position_protection(1, 80000.0, None)


def test_testnet_flag_and_cross_default():
    ex, *_ = make(account(testnet=True, margin_mode=""))
    assert ex.flag == "1" and ex.td_mode == "cross"


def test_constructor_accepts_settings_and_build_exchange(multi_settings, monkeypatch):
    acct = multi_settings.account("okx")
    ex = OkxExchange(multi_settings.__class__(accounts=[acct]), clients={"trade": FakeAPI(), "account": FakeAPI(),
                                                                         "public": FakeAPI(), "market": FakeAPI()})
    assert ex.account is acct
    # build_exchange 는 clients 없이 OkxExchange(acct) 를 호출 → python-okx 클라이언트 생성(네트워크 없음)
    built = build_exchange(acct, "okx")
    assert isinstance(built, OkxExchange) and built.display_name == "OKX"
    assert built.trade.flag == "0" and built.public.flag == "0"
    assert built.trade.API_KEY == "-1"      # 키 없음 → 서명 없는 클라이언트
    with_keys = AccountSettings(name="okx", exchange="okx", symbol=SYMBOL, api_key="k" * 24, api_secret="s" * 40,
                                api_passphrase="pp", testnet=True)
    built2 = OkxExchange(with_keys)
    assert built2.trade.API_KEY == "k" * 24 and built2.trade.PASSPHRASE == "pp" and built2.trade.flag == "1"
    assert built2.acct.API_SECRET_KEY == "s" * 40


def test_clordid_sanitised_and_deterministic():
    assert clordid_of(LINK) == CID and len(CID) == 32 and CID.isalnum() and LINK.startswith(CID)
    assert clordid_of("ab-c_d:e.f") == "abcdef"
    with pytest.raises(ExchangeRejected):
        clordid_of("---")


# --------------------------------------------------------------------------- #
# instrument
# --------------------------------------------------------------------------- #
def test_instrument_converts_contracts_to_btc_and_caches():
    ex, _, _, public, _ = make()
    info = ex.instrument()
    assert info == {"qty_step": 0.01, "min_qty": 0.01, "max_qty": 100.0, "tick": 0.1}
    assert ex.instrument()["min_qty"] == 0.01
    assert public.count("get_instruments") == 1
    assert public.kwargs("get_instruments") == {"instType": "SWAP", "instId": SYMBOL}


def test_instrument_missing_is_exchange_error():
    ex, *_ = make(public=FakeAPI(get_instruments=ok()))
    with pytest.raises(ExchangeError) as ei:
        ex.instrument()
    assert ei.value.code == "EXCHANGE_ERROR" and not isinstance(ei.value, ExchangeRejected)


# --------------------------------------------------------------------------- #
# place_market
# --------------------------------------------------------------------------- #
def test_place_market_hedge_long_maps_params_and_converts_sz():
    trade = FakeAPI(place_order=ok({"ordId": "o-1", "clOrdId": CID, "sCode": "0", "sMsg": ""}))
    ex, *_ = make(trade=trade)
    r = ex.place_market("Buy", 0.02, 1, False, LINK)
    assert r == {"order_id": "o-1", "order_link_id": LINK}
    kw = trade.kwargs("place_order")
    assert kw["instId"] == SYMBOL and kw["tdMode"] == "isolated" and kw["side"] == "buy"
    assert kw["posSide"] == "long" and kw["ordType"] == "market" and kw["sz"] == "2"
    assert kw["clOrdId"] == CID and len(kw["clOrdId"]) <= 32 and kw["clOrdId"].isalnum()
    assert kw["reduceOnly"] == ""     # long/short 모드: OKX 는 reduceOnly 를 받지 않는다 (side+posSide 로 청산)


def test_place_market_hedge_short_close_uses_buy_short():
    trade = FakeAPI(place_order=ok({"ordId": "o-2", "sCode": "0"}))
    ex, *_ = make(trade=trade)
    ex.place_market("Buy", 0.035, 2, True, LINK)
    kw = trade.kwargs("place_order")
    assert kw["side"] == "buy" and kw["posSide"] == "short" and kw["sz"] == "3"   # 0.035/0.01 = 3.5 → 3 계약 (내림)


def test_place_market_one_way_sends_net_and_reduce_only():
    trade = FakeAPI(place_order=ok({"ordId": "o-3", "sCode": "0"}))
    ex, *_ = make(account(position_mode="one_way", margin_mode="cross"), trade=trade)
    ex.place_market("Sell", 0.01, 0, True, LINK)
    kw = trade.kwargs("place_order")
    assert kw["posSide"] == "net" and kw["reduceOnly"] == "true" and kw["tdMode"] == "cross" and kw["sz"] == "1"
    ex.place_market("Sell", 0.01, 0, False, order_link_id("test", "event-2"))
    assert trade.kwargs("place_order")["reduceOnly"] == "false"


def test_place_market_rejects_qty_below_one_contract_without_calling_exchange():
    trade = FakeAPI(place_order=ok({"ordId": "o-x", "sCode": "0"}))
    ex, *_ = make(trade=trade)
    with pytest.raises(ExchangeRejected) as ei:
        ex.place_market("Buy", 0.005, 1, False, LINK)
    assert ei.value.ret_code == 51020 and trade.count("place_order") == 0


def test_place_market_wrong_idx_for_mode_is_rejected():
    ex, trade, *_ = make()
    with pytest.raises(ExchangeRejected):
        ex.place_market("Buy", 0.02, 0, False, LINK)   # hedge 계정에 idx 0
    assert trade.count("place_order") == 0


def test_place_market_scode_error_is_rejected_with_ret_code():
    trade = FakeAPI(place_order=err("1", "Duplicated client order ID", scode="51016"))
    ex, *_ = make(trade=trade)
    with pytest.raises(ExchangeRejected) as ei:
        ex.place_market("Buy", 0.02, 1, False, LINK)
    assert ei.value.ret_code == 51016 and ei.value.code == "EXCHANGE_REJECTED"
    assert "duplicate" in ei.value.message.lower()   # 실행기의 중복 link 재시도 판정 단어


def test_place_market_top_level_code_error_without_scode():
    trade = FakeAPI(place_order={"code": "51008", "msg": "Order placement failed due to insufficient balance", "data": []})
    ex, *_ = make(trade=trade)
    with pytest.raises(ExchangeRejected) as ei:
        ex.place_market("Buy", 0.02, 1, False, LINK)
    assert ei.value.ret_code == 51008


def test_place_market_missing_ord_id_is_error():
    trade = FakeAPI(place_order=ok({"sCode": "0"}))
    ex, *_ = make(trade=trade)
    with pytest.raises(ExchangeError) as ei:
        ex.place_market("Buy", 0.02, 1, False, LINK)
    assert ei.value.code == "EXCHANGE_ERROR"


def test_network_errors_map_to_timeout_or_error_and_writes_do_not_retry():
    trade = FakeAPI().queue("place_order", httpx.ReadTimeout("slow"))
    ex, *_ = make(trade=trade)
    with pytest.raises(ExchangeError) as ei:
        ex.place_market("Buy", 0.02, 1, False, LINK)
    assert ei.value.code == "EXCHANGE_TIMEOUT" and trade.count("place_order") == 1
    trade.queue("place_order", httpx.ConnectError("refused"))
    with pytest.raises(ExchangeError) as ei:
        ex.place_market("Buy", 0.02, 1, False, order_link_id("test", "event-9"))
    assert ei.value.code == "EXCHANGE_ERROR" and not isinstance(ei.value, ExchangeRejected)


# --------------------------------------------------------------------------- #
# place_conditional
# --------------------------------------------------------------------------- #
def test_conditional_stop_loss_for_long_uses_sl_fields():
    trade = FakeAPI(place_algo_order=ok({"algoId": "a-1", "algoClOrdId": CID, "sCode": "0"}))
    ex, *_ = make(trade=trade)
    r = ex.place_conditional("Sell", 0.02, 1, 79999.96, 2, LINK, "MarkPrice")
    assert r == {"order_id": "a-1", "order_link_id": LINK}
    kw = trade.kwargs("place_algo_order")
    assert kw["instId"] == SYMBOL and kw["tdMode"] == "isolated" and kw["side"] == "sell" and kw["posSide"] == "long"
    assert kw["ordType"] == "conditional" and kw["sz"] == "2" and kw["algoClOrdId"] == CID
    assert kw["slTriggerPx"] == "80000.0" and kw["slOrdPx"] == "-1" and kw["slTriggerPxType"] == "mark"
    assert "tpTriggerPx" not in kw and kw["reduceOnly"] == ""


def test_conditional_take_profit_for_long_uses_tp_fields_last_price():
    trade = FakeAPI(place_algo_order=ok({"algoId": "a-2", "sCode": "0"}))
    ex, *_ = make(trade=trade)
    ex.place_conditional("Sell", 0.02, 1, 90000, 1, LINK, "LastPrice")
    kw = trade.kwargs("place_algo_order")
    assert kw["tpTriggerPx"] == "90000.0" and kw["tpOrdPx"] == "-1" and kw["tpTriggerPxType"] == "last"
    assert "slTriggerPx" not in kw


def test_conditional_for_short_leg_sl_rising_tp_falling_index():
    trade = FakeAPI(place_algo_order=ok({"algoId": "a-3", "sCode": "0"}))
    ex, *_ = make(trade=trade)
    ex.place_conditional("Buy", 0.02, 2, 90000, 1, LINK, "IndexPrice")       # 숏 손절: 상승 돌파
    kw = trade.kwargs("place_algo_order")
    assert kw["side"] == "buy" and kw["posSide"] == "short" and kw["slTriggerPx"] == "90000.0" and kw["slTriggerPxType"] == "index"
    ex.place_conditional("Buy", 0.02, 2, 80000, 2, order_link_id("x", "tp"), "MarkPrice")   # 숏 익절: 하락 돌파
    kw = trade.kwargs("place_algo_order")
    assert kw["tpTriggerPx"] == "80000.0" and "slTriggerPx" not in kw


def test_conditional_one_way_sends_reduce_only_true_and_net():
    trade = FakeAPI(place_algo_order=ok({"algoId": "a-4", "sCode": "0"}))
    ex, *_ = make(account(position_mode="one_way"), trade=trade)
    ex.place_conditional("Sell", 0.02, 0, 80000, 2, LINK, "MarkPrice")
    kw = trade.kwargs("place_algo_order")
    assert kw["posSide"] == "net" and kw["reduceOnly"] == "true"


def test_conditional_bad_direction_and_crossed_trigger():
    trade = FakeAPI(place_algo_order=err("1", "SL trigger price can not be higher than the last price", scode="51280"))
    ex, *_ = make(trade=trade)
    with pytest.raises(ValueError):
        ex.place_conditional("Sell", 0.02, 1, 80000, 3, LINK, "MarkPrice")
    with pytest.raises(ExchangeRejected) as ei:
        ex.place_conditional("Sell", 0.02, 1, 80000, 2, LINK, "MarkPrice")
    assert ei.value.ret_code == 51280 and "crossed" in ei.value.message.lower()


# --------------------------------------------------------------------------- #
# get_order
# --------------------------------------------------------------------------- #
def _order(state: str, acc: str = "0", avg: str = "", **over) -> dict:
    d = {"ordId": "o-1", "clOrdId": CID, "state": state, "sz": "2", "accFillSz": acc, "avgPx": avg,
         "side": "buy", "posSide": "long", "reduceOnly": "false", "ordType": "market"}
    d.update(over)
    return d


def test_get_order_maps_regular_order_states():
    trade = FakeAPI().queue("get_order",
                            ok(_order("live")), ok(_order("partially_filled", "1", "85000")),
                            ok(_order("filled", "2", "85010.5")), ok(_order("canceled")),
                            ok(_order("canceled", "1", "85000")), ok(_order("mmp_canceled")))
    ex, *_ = make(trade=trade)
    o = ex.get_order(LINK)
    assert o["status"] == "New" and o["order_id"] == "o-1" and o["order_link_id"] == LINK
    assert o["qty"] == 0.02 and o["cum_qty"] == 0.0 and o["avg_price"] is None and o["trigger_price"] is None
    assert o["side"] == "Buy" and o["position_idx"] == 1 and o["reduce_only"] is False
    assert trade.kwargs("get_order") == {"instId": SYMBOL, "clOrdId": CID}
    o = ex.get_order(LINK)
    assert o["status"] == "PartiallyFilled" and o["cum_qty"] == 0.01 and o["avg_price"] == 85000.0
    o = ex.get_order(LINK)
    assert o["status"] == "Filled" and o["cum_qty"] == 0.02 and o["avg_price"] == 85010.5
    assert ex.get_order(LINK)["status"] == "Cancelled"
    assert ex.get_order(LINK)["status"] == "PartiallyFilledCanceled"      # IOC 부분 체결 후 취소
    assert ex.get_order(LINK)["status"] == "Cancelled"
    assert trade.count("get_algo_order_details") == 0     # 일반 주문으로 찾았으니 알고 조회 없음


def test_get_order_falls_back_to_algo_and_none_when_absent():
    trade = FakeAPI(get_order=err("51603", "Order does not exist"),
                    get_algo_order_details=ok({"algoId": "a-1", "algoClOrdId": CID, "state": "live", "sz": "2",
                                               "slTriggerPx": "80000", "slOrdPx": "-1", "side": "sell", "posSide": "long",
                                               "ordId": ""}))
    ex, *_ = make(trade=trade)
    o = ex.get_order(LINK)
    assert o["status"] == "Untriggered" and o["order_id"] == "a-1" and o["trigger_price"] == 80000.0
    assert o["qty"] == 0.02 and o["side"] == "Sell" and o["position_idx"] == 1 and o["reduce_only"] is True
    assert o["exec_order_id"] is None
    assert trade.kwargs("get_algo_order_details") == {"algoClOrdId": CID}
    # 둘 다 없음 → None (예외 아님)
    trade.set("get_algo_order_details", err("51603", "Order does not exist"))
    assert ex.get_order(order_link_id("test", "other")) is None
    # 알고 응답이 code 0 + 빈 data 여도 None
    trade.set("get_algo_order_details", ok())
    assert ex.get_order(order_link_id("test", "other2")) is None


def test_get_order_read_error_other_than_not_found_raises_exchange_error():
    trade = FakeAPI(get_order=err("50113", "Invalid signature"))
    ex, *_ = make(trade=trade)
    with pytest.raises(ExchangeError) as ei:
        ex.get_order(LINK)
    assert ei.value.code == "EXCHANGE_ERROR" and ei.value.ret_code == 50113 and not isinstance(ei.value, ExchangeRejected)


def test_get_order_read_retries_once_on_transport_error():
    trade = FakeAPI().queue("get_order", httpx.ConnectError("boom"), ok(_order("filled", "2", "85000")))
    ex, *_ = make(trade=trade)
    assert ex.get_order(LINK)["status"] == "Filled" and trade.count("get_order") == 2
    trade.queue("get_order", httpx.ReadTimeout("t1"), httpx.ReadTimeout("t2"))
    with pytest.raises(ExchangeError) as ei:
        ex.get_order(LINK)
    assert ei.value.code == "EXCHANGE_TIMEOUT" and trade.count("get_order") == 4


def test_algo_known_link_skips_regular_lookup_and_effective_follows_generated_order():
    trade = FakeAPI(place_algo_order=ok({"algoId": "a-7", "sCode": "0"}))
    ex, *_ = make(trade=trade)
    ex.place_conditional("Sell", 0.02, 1, 80000, 2, LINK, "MarkPrice")
    algo_eff = {"algoId": "a-7", "algoClOrdId": CID, "state": "effective", "sz": "2", "slTriggerPx": "80000",
                "side": "sell", "posSide": "long", "ordId": "gen-77", "actualSz": "2", "actualPx": "79990"}
    trade.set("get_algo_order_details", ok(algo_eff))
    trade.set("get_order", ok(_order("filled", "2", "79990.5", ordId="gen-77", clOrdId="", side="sell")))
    o = ex.get_order(LINK)
    assert o["status"] == "Filled" and o["order_id"] == "a-7" and o["exec_order_id"] == "gen-77"
    assert o["cum_qty"] == 0.02 and o["avg_price"] == 79990.5 and o["trigger_price"] == 80000.0
    # 알고로 기억된 link → 일반 주문 조회는 clOrdId 가 아니라 생성된 ordId 로만 (algo 상세가 먼저)
    assert [c[0] for c in trade.calls][1:] == ["get_algo_order_details", "get_order"]
    assert trade.kwargs("get_order") == {"instId": SYMBOL, "ordId": "gen-77"}
    # 생성 주문이 아직 live → Triggered (대기), 없음 → Triggered
    trade.set("get_order", ok(_order("live", ordId="gen-77")))
    assert ex.get_order(LINK)["status"] == "Triggered"
    trade.set("get_order", err("51603", "Order does not exist"))
    assert ex.get_order(LINK)["status"] == "Triggered"
    # 그 외 알고 상태
    for state, want in (("canceled", "Cancelled"), ("order_failed", "Rejected"), ("pause", "Untriggered"),
                        ("partially_effective", "Triggered")):
        trade.set("get_algo_order_details", ok(dict(algo_eff, state=state, ordId="")))
        assert ex.get_order(LINK)["status"] == want, state


# --------------------------------------------------------------------------- #
# executions
# --------------------------------------------------------------------------- #
FILLS = ok(
    {"tradeId": "t-2", "ordId": "o-1", "fillSz": "1", "fillPx": "85001", "ts": "1700000000200", "side": "buy"},
    {"tradeId": "t-1", "ordId": "o-1", "fillSz": "1", "fillPx": "85000", "ts": "1700000000100", "side": "buy"},
    {"tradeId": "t-x", "ordId": "o-other", "fillSz": "1", "fillPx": "1", "ts": "1700000000300"},
    {"tradeId": "t-0", "ordId": "o-1", "fillSz": "0", "fillPx": "85000", "ts": "1700000000050"},
)


def test_executions_map_fills_and_sort():
    trade = FakeAPI(get_fills=FILLS)
    ex, *_ = make(trade=trade)
    xs = ex.executions("o-1")
    assert trade.kwargs("get_fills") == {"instType": "SWAP", "instId": SYMBOL, "ordId": "o-1"}
    assert [x["exec_id"] for x in xs] == ["t-1", "t-2"]
    assert xs[0] == {"exec_id": "t-1", "qty": 0.01, "price": 85000.0, "exec_time_ms": 1700000000100, "order_id": "o-1"}


def test_executions_for_algo_id_resolve_generated_order():
    trade = FakeAPI(get_algo_order_details=ok({"algoId": "a-9", "state": "effective", "ordId": "gen-9", "sz": "2"}))
    trade.queue("get_fills", ok(), ok({"tradeId": "t-9", "ordId": "gen-9", "fillSz": "2", "fillPx": "80000", "ts": "1700000001000"}))
    ex, *_ = make(trade=trade)
    xs = ex.executions("a-9")
    assert [x["exec_id"] for x in xs] == ["t-9"] and xs[0]["qty"] == 0.02 and xs[0]["order_id"] == "gen-9"
    assert trade.kwargs("get_fills", 0)["ordId"] == "a-9" and trade.kwargs("get_fills", 1)["ordId"] == "gen-9"
    assert trade.kwargs("get_algo_order_details") == {"algoId": "a-9"}
    # 매핑 캐시 → 다음 호출은 바로 생성 주문 id 로
    trade.queue("get_fills", ok())
    ex.executions("a-9")
    assert trade.kwargs("get_fills")["ordId"] == "gen-9" and trade.count("get_algo_order_details") == 1
    # 알고도 없음 → []
    trade.set("get_algo_order_details", err("51603", "Order does not exist"))
    trade.queue("get_fills", ok())
    assert ex.executions("a-none") == []


# --------------------------------------------------------------------------- #
# cancel_order
# --------------------------------------------------------------------------- #
def test_cancel_known_regular_order():
    trade = FakeAPI(place_order=ok({"ordId": "o-1", "sCode": "0"}), cancel_order=ok({"ordId": "o-1", "sCode": "0"}))
    ex, *_ = make(trade=trade)
    ex.place_market("Buy", 0.02, 1, False, LINK)
    assert ex.cancel_order(LINK) is True
    assert trade.kwargs("cancel_order") == {"instId": SYMBOL, "clOrdId": CID}
    assert trade.count("cancel_algo_order") == 0 and trade.count("get_algo_order_details") == 0
    # 이미 체결/취소됨 → True
    for sc in ("51400", "51401", "51402", "51410"):
        trade.set("cancel_order", err("1", "Cancellation failed", scode=sc))
        assert ex.cancel_order(LINK) is True
    # 그 외 거부 → ExchangeRejected
    trade.set("cancel_order", err("1", "Invalid IP", scode="50110"))
    with pytest.raises(ExchangeRejected):
        ex.cancel_order(LINK)


def test_cancel_known_algo_uses_algo_id():
    trade = FakeAPI(place_algo_order=ok({"algoId": "a-5", "sCode": "0"}),
                    cancel_algo_order=ok({"algoId": "a-5", "sCode": "0"}))
    ex, *_ = make(trade=trade)
    ex.place_conditional("Sell", 0.02, 1, 80000, 2, LINK, "MarkPrice")
    assert ex.cancel_order(LINK) is True
    assert trade.args("cancel_algo_order") == ([{"instId": SYMBOL, "algoId": "a-5"}],)
    assert trade.count("cancel_order") == 0
    trade.set("cancel_algo_order", err("1", "Cancellation failed as the order is already canceled", scode="51401"))
    assert ex.cancel_order(LINK) is True


def test_cancel_unknown_link_falls_back_to_algo_lookup():
    trade = FakeAPI(cancel_order=err("1", "Cancellation failed as the order does not exist", scode="51400"),
                    get_algo_order_details=ok({"algoId": "a-6", "algoClOrdId": CID, "state": "live", "sz": "2"}),
                    cancel_algo_order=ok({"algoId": "a-6", "sCode": "0"}))
    ex, *_ = make(trade=trade)     # 재기동 후: link 종류를 모름
    assert ex.cancel_order(LINK) is True
    assert [c[0] for c in trade.calls] == ["cancel_order", "get_algo_order_details", "cancel_algo_order"]
    assert trade.args("cancel_algo_order") == ([{"instId": SYMBOL, "algoId": "a-6"}],)
    # 알고가 이미 effective(트리거됨) → 취소 호출 없이 True
    trade.calls.clear()
    trade.set("get_algo_order_details", ok({"algoId": "a-6", "state": "effective", "ordId": "gen-6"}))
    link2 = order_link_id("test", "other")
    assert ex.cancel_order(link2) is True and trade.count("cancel_algo_order") == 0
    # 둘 다 없음 → True (이미 없음)
    trade.set("get_algo_order_details", err("51603", "Order does not exist"))
    assert ex.cancel_order(order_link_id("test", "none")) is True


# --------------------------------------------------------------------------- #
# open_conditional_orders / positions
# --------------------------------------------------------------------------- #
def test_open_conditional_orders_filtered_by_pos_side_and_link_restored():
    trade = FakeAPI(place_algo_order=ok({"algoId": "a-1", "sCode": "0"}))
    ex, *_ = make(trade=trade)
    ex.place_conditional("Sell", 0.02, 1, 80000, 2, LINK, "MarkPrice")
    other_cid = alnum_only(order_link_id("unknown", "after-restart"), 32)
    trade.set("order_algos_list", ok(
        {"algoId": "a-1", "algoClOrdId": CID, "state": "live", "sz": "2", "slTriggerPx": "80000", "side": "sell", "posSide": "long"},
        {"algoId": "a-2", "algoClOrdId": other_cid, "state": "live", "sz": "3", "tpTriggerPx": "90000", "side": "sell", "posSide": "long"},
        {"algoId": "a-3", "algoClOrdId": "zzz", "state": "live", "sz": "1", "slTriggerPx": "90000", "side": "buy", "posSide": "short"},
    ))
    xs = ex.open_conditional_orders(1)
    assert trade.kwargs("order_algos_list") == {"ordType": "conditional", "instType": "SWAP", "instId": SYMBOL, "limit": "100"}
    assert xs == [
        {"order_link_id": LINK, "order_id": "a-1", "trigger_price": 80000.0, "qty": 0.02, "side": "Sell"},
        {"order_link_id": other_cid, "order_id": "a-2", "trigger_price": 90000.0, "qty": 0.03, "side": "Sell"},
    ]
    ys = ex.open_conditional_orders(2)
    assert [y["order_id"] for y in ys] == ["a-3"] and ys[0]["side"] == "Buy" and ys[0]["qty"] == 0.01


def test_positions_hedge_mapping():
    acc = FakeAPI(get_positions=ok(
        {"instId": SYMBOL, "posSide": "long", "pos": "3", "avgPx": "85000", "markPx": "85100", "uTime": "1700000000000"},
        {"instId": SYMBOL, "posSide": "short", "pos": "0", "avgPx": "", "markPx": "85100", "uTime": "1700000000001"},
        {"instId": SYMBOL, "posSide": "short", "pos": "1", "avgPx": "86000", "markPx": "85100", "uTime": "1700000000002"},
        {"instId": "ETH-USDT-SWAP", "posSide": "long", "pos": "5", "avgPx": "1", "markPx": "1", "uTime": "1"},
    ))
    ex, *_ = make(account=acc)
    ps = ex.positions()
    assert acc.kwargs("get_positions") == {"instType": "SWAP", "instId": SYMBOL}
    assert set(ps) == {1, 2}
    assert ps[1] == {"position_idx": 1, "size": 0.03, "side": "Buy", "avg_price": 85000.0, "mark_price": 85100.0,
                     "updated_time_ms": 1700000000000}
    assert ps[2]["size"] == 0.01 and ps[2]["side"] == "Sell" and ps[2]["avg_price"] == 86000.0


def test_positions_net_mode_sign_gives_side():
    acc = FakeAPI(get_positions=ok({"instId": SYMBOL, "posSide": "net", "pos": "-2", "avgPx": "85000", "markPx": "84000", "uTime": "5"}))
    ex, *_ = make(account(position_mode="one_way"), account=acc)
    ps = ex.positions()
    assert ps == {0: {"position_idx": 0, "size": 0.02, "side": "Sell", "avg_price": 85000.0, "mark_price": 84000.0, "updated_time_ms": 5}}


def test_last_and_mark_price():
    market = FakeAPI(get_ticker=ok({"instId": SYMBOL, "last": "85123.4"}))
    public = FakeAPI(get_instruments=INSTRUMENT_RESP, get_mark_price=ok({"instId": SYMBOL, "markPx": "85120.0"}))
    ex, *_ = make(market=market, public=public)
    assert ex.last_price() == 85123.4 and market.kwargs("get_ticker") == {"instId": SYMBOL}
    assert ex.mark_price() == 85120.0 and public.kwargs("get_mark_price") == {"instType": "SWAP", "instId": SYMBOL}
    market.set("get_ticker", ok())
    with pytest.raises(ExchangeError):
        ex.last_price()


# --------------------------------------------------------------------------- #
# ensure_account_setup
# --------------------------------------------------------------------------- #
def test_ensure_account_setup_hedge_isolated_sets_both_sides():
    acc = FakeAPI(set_position_mode=ok({"posMode": "long_short_mode"}), set_leverage=ok({"lever": "5"}))
    ex, *_ = make(account=acc)
    ex.ensure_account_setup("hedge", 5, "isolated")
    assert acc.kwargs("set_position_mode") == {"posMode": "long_short_mode"}
    assert acc.count("set_leverage") == 2
    assert acc.kwargs("set_leverage", 0) == {"lever": "5", "mgnMode": "isolated", "instId": SYMBOL, "posSide": "long"}
    assert acc.kwargs("set_leverage", 1) == {"lever": "5", "mgnMode": "isolated", "instId": SYMBOL, "posSide": "short"}


def test_ensure_account_setup_one_way_cross_and_already_set_is_ignored():
    acc = FakeAPI(set_position_mode=err("59000", "Position mode is already net_mode"), set_leverage=ok({"lever": "3"}))
    ex, *_ = make(account(position_mode="one_way", margin_mode="cross"), account=acc)
    ex.ensure_account_setup("one_way", 3, "cross")
    assert acc.kwargs("set_position_mode") == {"posMode": "net_mode"}
    assert acc.kwargs("set_leverage") == {"lever": "3", "mgnMode": "cross", "instId": SYMBOL}
    # 진짜 실패(포지션 있어 변경 불가) 는 전달
    acc.set("set_position_mode", err("59000", "Your settings failed as you have positions or open orders"))
    with pytest.raises(ExchangeRejected) as ei:
        ex.ensure_account_setup("one_way", 3, "cross")
    assert ei.value.ret_code == 59000
    with pytest.raises(ValueError):
        ex.ensure_account_setup("both", 3, "cross")


def test_ensure_account_setup_isolated_one_way_uses_net_pos_side():
    acc = FakeAPI(set_position_mode=ok(), set_leverage=ok())
    ex, *_ = make(account(position_mode="one_way"), account=acc)
    ex.ensure_account_setup("one_way", 10, "isolated")
    assert acc.kwargs("set_leverage") == {"lever": "10", "mgnMode": "isolated", "instId": SYMBOL, "posSide": "net"}


# --------------------------------------------------------------------------- #
# 결과를 알 수 없는 쓰기 응답 (50004 등) → 거부가 아니라 오류
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("code,expected", [
    ("50004", "EXCHANGE_TIMEOUT"),   # Endpoint request timeout (does not indicate success or failure)
    ("50001", "EXCHANGE_ERROR"),     # Matching engine upgrading
    ("50013", "EXCHANGE_ERROR"),     # System is busy
    ("50026", "EXCHANGE_ERROR"),     # System error
])
def test_uncertain_write_codes_map_to_exchange_error_not_rejected(code, expected):
    trade = FakeAPI(place_order=err(code, "API endpoint request timeout (does not mean success or failure)"))
    ex, *_ = make(trade=trade)
    with pytest.raises(ExchangeError) as ei:
        ex.place_market("Buy", 0.02, 1, False, LINK)
    assert not isinstance(ei.value, ExchangeRejected)
    assert ei.value.code == expected and ei.value.ret_code == int(code)
    # data[i].sCode 형태로 와도 같다
    trade.set("place_order", err("1", "timeout", scode=code))
    with pytest.raises(ExchangeError) as ei2:
        ex.place_market("Buy", 0.02, 1, False, order_link_id("test", "event-2"))
    assert not isinstance(ei2.value, ExchangeRejected) and ei2.value.code == expected
    # 보통의 거부 코드는 그대로 거부
    trade.set("place_order", err("51008", "insufficient balance"))
    with pytest.raises(ExchangeRejected):
        ex.place_market("Buy", 0.02, 1, False, order_link_id("test", "event-3"))


# --------------------------------------------------------------------------- #
# 재기동 뒤 절단된 algoClOrdId → link_resolver 로 원래 link 복원
# --------------------------------------------------------------------------- #
def test_link_resolver_restores_truncated_cid_after_restart():
    trade = FakeAPI()
    ex, *_ = make(trade=trade)                        # 메모리 역매핑 없음 (재기동 직후)
    asked: list[str] = []

    def resolver(cid: str):
        asked.append(cid)
        return LINK if cid == CID else None

    ex.link_resolver = resolver
    other = alnum_only(order_link_id("x", "y"), 32)
    trade.set("order_algos_list", ok(
        {"algoId": "a-1", "algoClOrdId": CID, "state": "live", "sz": "2", "slTriggerPx": "80000", "side": "sell", "posSide": "long"},
        {"algoId": "a-2", "algoClOrdId": other, "state": "live", "sz": "1", "tpTriggerPx": "90000", "side": "sell", "posSide": "long"},
    ))
    xs = ex.open_conditional_orders(1)
    assert [x["order_link_id"] for x in xs] == [LINK, other]       # 복원 / 모르면 절단 id 그대로
    assert asked == [CID, other]
    # 복원된 매핑은 캐시되고 취소에도 쓰인다 (algoId 기억)
    trade.set("cancel_algo_order", ok({"algoId": "a-1", "sCode": "0"}))
    assert ex.cancel_order(LINK) is True
    assert trade.args("cancel_algo_order")[0] == [{"instId": SYMBOL, "algoId": "a-1"}]
    assert trade.count("order_algos_list") == 1


def test_link_resolver_result_must_match_cid_and_errors_are_ignored():
    trade = FakeAPI()
    ex, *_ = make(trade=trade)
    ex.link_resolver = lambda cid: "lkwrong"                      # cid 와 맞지 않는 link → 무시
    trade.set("order_algos_list", ok(
        {"algoId": "a-1", "algoClOrdId": CID, "state": "live", "sz": "2", "slTriggerPx": "80000", "side": "sell", "posSide": "long"}))
    assert ex.open_conditional_orders(1)[0]["order_link_id"] == CID

    def broken(cid):
        raise RuntimeError("db down")

    ex.link_resolver = broken
    assert ex.open_conditional_orders(1)[0]["order_link_id"] == CID
