"""config.py 계정 목록 (ARCHITECTURE_MULTI_EXCHANGE.md §1) + exchange.py 2단계 공통부 (§3).

  - accounts 없음 → 최상위 값 + BYBIT_* 로 bybit 계정 하나 (1단계 호환)
  - 계정별 키/회신 URL·시크릿 env 매핑과 계정별 덮어쓰기
  - 검증: exchange 집합, toobit hedge 강제, okx passphrase, 이름 중복/패턴, routing
  - live_execution_possible(account) / has_real_keys / route_accounts
  - PaperExchange(account) display_name, lot_protection=False 변형의 포지션 단위 보호, build_exchange 분기
"""
from __future__ import annotations

import pytest

from lake_executor import config
from lake_executor.config import AccountSettings, ConfigError, Settings
from lake_executor.exchange import BybitExchange, ExchangeRejected, PaperExchange, build_exchange
from lake_executor.schemas import ReasonCode, Signal
from lake_executor.util import alnum_only, env_suffix, order_link_id
from tests.conftest import (LIVE_REPORT_SECRET, MULTI_ACCOUNTS, PAPER_PRICE, REPORT_URL_TEST, TEST_REPORT_SECRET,
                            make_signal)

REAL_KEY = "k" * 24
REAL_SECRET = "s" * 40
OKX_REPORT_SECRET = "okx-report-secret-0123456789abcdef-0123456789"


def _accounts(**over):
    out = []
    for a in MULTI_ACCOUNTS:
        d = dict(a)
        d.update(over.get(d["name"], {}))
        out.append(d)
    return out


# --------------------------------------------------------------------------- #
# 1단계 호환
# --------------------------------------------------------------------------- #
def test_no_accounts_key_yields_single_bybit_account(settings_factory):
    s = settings_factory(config_overrides={"position_mode": "one_way", "leverage": 7, "testnet": True},
                         env_overrides={"BYBIT_API_KEY": REAL_KEY, "BYBIT_API_SECRET": REAL_SECRET})
    assert [a.name for a in s.accounts] == ["bybit"]
    a = s.accounts[0]
    assert a.exchange == "bybit" and a.enabled and a.symbol == "BTCUSDT" and a.position_mode == "one_way"
    assert a.leverage == 7 and a.margin_mode == "isolated" and a.testnet is True and a.env_prefix == "BYBIT"
    assert a.qty_multiplier == 1.0 and a.report is True and a.category == "linear"
    assert a.api_key == REAL_KEY and a.api_secret == REAL_SECRET and a.api_passphrase == ""
    assert a.has_real_keys() and s.has_real_keys("bybit") and s.has_real_keys()
    assert s.secrets.bybit_api_key == REAL_KEY and s.secrets.has_real_bybit_keys()   # 옛 속성도 유지
    assert a.report_url == {"test": REPORT_URL_TEST}
    assert a.report_secret == {"test": TEST_REPORT_SECRET, "live": LIVE_REPORT_SECRET}
    assert a.display_name == "Bybit" and a.account_scope == "lake_dedicated_BTCUSDT"
    assert s.routing == "fanout" and s.account("bybit") is a and s.enabled_accounts() == [a]
    assert s.allowed_position_idx() == {0}
    with pytest.raises(KeyError):
        s.account("okx")


def test_placeholder_keys_are_not_real(settings_factory):
    s = settings_factory(env_overrides={"BYBIT_API_KEY": "PUT_KEY_HERE", "BYBIT_API_SECRET": REAL_SECRET})
    assert s.accounts[0].has_real_keys() is False
    assert s.live_execution_possible() == (False, "LIVE_DISABLED")


def test_live_execution_possible_compat_and_per_account(settings_factory):
    s = settings_factory(config_overrides={"live": {"enabled": True}},
                         env_overrides={"BYBIT_API_KEY": REAL_KEY, "BYBIT_API_SECRET": REAL_SECRET})
    assert s.live_execution_possible() == (True, "")
    assert s.live_execution_possible("bybit") == (True, "")
    assert s.live_execution_possible(s.accounts[0]) == (True, "")
    assert s.live_execution_possible("nope") == (False, "NO_TARGET_ACCOUNT")
    # live 수신 키가 없으면 불가
    s2 = settings_factory(config_overrides={"live": {"enabled": True}},
                          env_overrides={"BYBIT_API_KEY": REAL_KEY, "BYBIT_API_SECRET": REAL_SECRET},
                          drop_env=("LAKE_SIGNAL_SECRET_LIVE",))
    assert s2.live_execution_possible("bybit") == (False, "LIVE_DISABLED")
    # 키 없음
    s3 = settings_factory(config_overrides={"live": {"enabled": True}})
    assert s3.live_execution_possible() == (False, "LIVE_DISABLED")
    assert s3.live_execution_possible("bybit") == (False, "LIVE_DISABLED")


# --------------------------------------------------------------------------- #
# 계정 목록
# --------------------------------------------------------------------------- #
def test_multi_accounts_load_env_and_report_overrides(settings_factory):
    env = {
        "BYBIT_API_KEY": REAL_KEY, "BYBIT_API_SECRET": REAL_SECRET,
        "OKX_API_KEY": REAL_KEY, "OKX_API_SECRET": REAL_SECRET, "OKX_API_PASSPHRASE": "pp",
        "LAKE_REPORT_URL_TEST_OKX": "http://lake.test/okx", "LAKE_REPORT_SECRET_TEST_OKX": OKX_REPORT_SECRET,
        "LAKE_REPORT_URL_LIVE": "http://lake.live/report",
    }
    s = settings_factory(config_overrides={"routing": "by_exchange", "accounts": _accounts(okx={"enabled": True},
                                                                                           toobit={"enabled": False})},
                         env_overrides=env)
    assert [a.name for a in s.accounts] == ["bybit", "okx", "toobit"]
    assert [a.name for a in s.enabled_accounts()] == ["bybit", "okx"]
    okx, toobit = s.account("okx"), s.account("toobit")
    assert okx.api_passphrase == "pp" and okx.has_real_keys() and not toobit.has_real_keys()
    assert okx.symbol == "BTC-USDT-SWAP" and toobit.symbol == "BTC-SWAP-USDT"
    assert okx.display_name == "OKX" and toobit.display_name == "Toobit"
    assert okx.account_scope == "lake_dedicated_OKX_BTCUSDT" and toobit.account_scope == "lake_dedicated_TOOBIT_BTCUSDT"
    # 회신: 계정별 덮어쓰기 > 기본
    assert okx.report_url == {"test": "http://lake.test/okx", "live": "http://lake.live/report"}
    assert okx.report_secret["test"] == OKX_REPORT_SECRET and okx.report_secret["live"] == LIVE_REPORT_SECRET
    assert s.account("bybit").report_url == {"test": REPORT_URL_TEST, "live": "http://lake.live/report"}
    assert s.account("bybit").report_secret["test"] == TEST_REPORT_SECRET
    assert okx.report is False and s.account("bybit").report is True
    # 최상위 1단계 필드는 첫 계정을 따른다
    assert s.symbol == "BTCUSDT" and s.position_mode == "hedge"
    # 라우팅
    assert [a.name for a in s.route_accounts("OKX")] == ["okx"]
    assert [a.name for a in s.route_accounts("bybit")] == ["bybit"]
    assert s.route_accounts("Toobit") == [] and s.route_accounts(None) == []
    s.routing = "fanout"
    assert [a.name for a in s.route_accounts("Bybit")] == ["bybit", "okx"]


def test_account_defaults_inherit_top_level(settings_factory):
    s = settings_factory(config_overrides={"position_mode": "one_way", "leverage": 3, "margin_mode": "cross",
                                           "accounts": [{"exchange": "bybit"}, {"name": "b2", "exchange": "bybit",
                                                                                 "env_prefix": "BYBIT2", "qty_multiplier": 0.5}]})
    a, b = s.accounts
    assert a.name == "bybit" and a.position_mode == "one_way" and a.leverage == 3 and a.margin_mode == "cross"
    assert b.env_prefix == "BYBIT2" and b.qty_multiplier == 0.5 and b.position_mode == "one_way"
    assert s.allowed_position_idx() == {0}


def test_allowed_position_idx_is_union_of_enabled_accounts(settings_factory):
    s = settings_factory(config_overrides={"accounts": _accounts(bybit={"position_mode": "one_way"})})
    assert s.allowed_position_idx() == {0, 1, 2}
    s = settings_factory(config_overrides={"accounts": _accounts(bybit={"position_mode": "one_way"},
                                                                 okx={"enabled": False}, toobit={"enabled": False})})
    assert s.allowed_position_idx() == {0}


def test_live_execution_possible_per_account_multi(settings_factory):
    env = {"OKX_API_KEY": REAL_KEY, "OKX_API_SECRET": REAL_SECRET, "OKX_API_PASSPHRASE": "pp"}
    s = settings_factory(config_overrides={"live": {"enabled": True},
                                           "accounts": _accounts(toobit={"enabled": False})}, env_overrides=env)
    assert s.live_execution_possible() == (True, "")                       # 하나라도 가능
    assert s.live_execution_possible("okx") == (True, "")
    assert s.live_execution_possible("bybit") == (False, "LIVE_DISABLED")  # 키 없음
    assert s.live_execution_possible("toobit") == (False, "ACCOUNT_DISABLED")
    assert s.has_real_keys("okx") and not s.has_real_keys("bybit")


@pytest.mark.parametrize("over,msg", [
    ({"bybit": {"exchange": "binance"}}, "exchange must be one of"),
    ({"toobit": {"position_mode": "one_way"}}, "toobit supports position_mode hedge"),
    ({"okx": {"name": "bybit"}}, "duplicate account name"),
    ({"okx": {"name": "OKX"}}, "must match"),
    ({"okx": {"name": "a" * 33}}, "must match"),
    ({"okx": {"name": ""}}, "must match"),
    ({"okx": {"qty_multiplier": 0}}, "qty_multiplier"),
    ({"okx": {"leverage": 0}}, "leverage out of range"),
    ({"okx": {"margin_mode": "portfolio"}}, "margin_mode"),
    ({"okx": {"env_prefix": "okx"}}, "env_prefix"),
])
def test_account_validation_errors(settings_factory, over, msg):
    with pytest.raises(ConfigError, match=msg):
        settings_factory(config_overrides={"accounts": _accounts(**over)})


def test_okx_requires_passphrase_only_when_keys_set(settings_factory):
    settings_factory(config_overrides={"accounts": _accounts()})   # 키 없음 → OK
    with pytest.raises(ConfigError, match="PASSPHRASE"):
        settings_factory(config_overrides={"accounts": _accounts()},
                         env_overrides={"OKX_API_KEY": REAL_KEY, "OKX_API_SECRET": REAL_SECRET})
    s = settings_factory(config_overrides={"accounts": _accounts()},
                         env_overrides={"OKX_API_KEY": REAL_KEY, "OKX_API_SECRET": REAL_SECRET, "OKX_API_PASSPHRASE": "pp"})
    assert s.account("okx").has_real_keys()


def test_routing_and_accounts_shape_validation(settings_factory):
    with pytest.raises(ConfigError, match="routing"):
        settings_factory(config_overrides={"routing": "broadcast"})
    with pytest.raises(ConfigError, match="accounts"):
        settings_factory(config_overrides={"accounts": []})
    with pytest.raises(ConfigError, match="accounts"):
        settings_factory(config_overrides={"accounts": "bybit"})
    with pytest.raises(ConfigError, match="report secret"):
        settings_factory(config_overrides={"accounts": _accounts()}, env_overrides={"LAKE_REPORT_SECRET_TEST_OKX": "short"})


def test_validate_accepts_programmatic_settings():
    s = Settings(accounts=[AccountSettings(name="okx", exchange="okx", env_prefix="OKX", symbol="BTC-USDT-SWAP")])
    config.validate(s)
    s.accounts.append(AccountSettings(name="okx", exchange="toobit", env_prefix="TOOBIT", symbol="BTC-SWAP-USDT"))
    with pytest.raises(ConfigError, match="duplicate"):
        config.validate(s)


def test_util_helpers():
    assert env_suffix("okx-sub.1") == "OKX_SUB_1" and env_suffix("bybit") == "BYBIT"
    link = order_link_id("live", "ev-1")
    assert alnum_only(link, 32) == link[:32] and len(alnum_only("a-b_c:d" * 10, 32)) == 32
    assert alnum_only("") == ""


# --------------------------------------------------------------------------- #
# schemas
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("ex,key", [("Bybit", "bybit"), ("OKX", "okx"), ("Toobit", "toobit")])
def test_signal_exchange_enum(ex, key):
    sig = Signal.model_validate(make_signal(exchange=ex))
    assert sig.exchange == ex and sig.exchange_key() == key


def test_signal_rejects_unknown_exchange():
    with pytest.raises(ValueError):
        Signal.model_validate(make_signal(exchange="Binance"))
    assert ReasonCode.ACCOUNT_DISABLED.value == "ACCOUNT_DISABLED" and ReasonCode.NO_TARGET_ACCOUNT.value == "NO_TARGET_ACCOUNT"


# --------------------------------------------------------------------------- #
# exchange.py 2단계 공통부
# --------------------------------------------------------------------------- #
def test_paper_display_name_follows_account(multi_settings, multi_paper_exchanges):
    assert {n: e.display_name for n, e in multi_paper_exchanges.items()} == {"bybit": "Bybit", "okx": "OKX", "toobit": "Toobit"}
    for a in multi_settings.accounts:
        e = multi_paper_exchanges[a.name]
        assert e.account is a and e.symbol == a.symbol and e.supports_lot_protection is True and e.name == "paper"
    # 계정별로 독립된 상태
    multi_paper_exchanges["okx"].place_market("Buy", 0.01, 1, False, "l1")
    assert multi_paper_exchanges["okx"].positions()[1]["size"] == pytest.approx(0.01)
    assert multi_paper_exchanges["bybit"].positions() == {}
    with pytest.raises(NotImplementedError):
        multi_paper_exchanges["okx"].set_position_protection(1, 80000, None)


def test_paper_accepts_settings_for_compat(settings):
    p = PaperExchange(settings, price=PAPER_PRICE)
    assert p.account is settings.accounts[0] and p.display_name == "Bybit" and p.last_price() == PAPER_PRICE
    with pytest.raises(ExchangeRejected):
        p.place_market("Buy", 0.002, 0, False, "x")   # hedge 계정 → idx 0 거부


def test_paper_without_lot_protection_uses_position_protection(settings):
    acct = settings.accounts[0]
    p = PaperExchange(acct, price=86000.0, lot_protection=False)
    assert p.supports_lot_protection is False
    with pytest.raises(NotImplementedError):
        p.place_conditional("Buy", 0.002, 2, 90000.0, 1, "sl1", "MarkPrice")
    with pytest.raises(ExchangeRejected) as ei:
        p.set_position_protection(2, 90000.0, 80000.0)      # 포지션 없음
    assert ei.value.ret_code == 110017
    p.place_market("Sell", 0.002, 2, False, "e1")           # short lot A
    p.place_market("Sell", 0.003, 2, False, "e2")           # short lot B (같은 레그)
    with pytest.raises(ExchangeRejected, match="crossed"):
        p.set_position_protection(2, 85000.0, None)        # 숏 SL 85000 < 현재가 86000 → 이미 지남
    r = p.set_position_protection(2, 90000.0, 80000.0)
    assert r == {"position_idx": 2, "stop_loss": 90000.0, "take_profit": 80000.0}
    assert p.get_position_protection(2) == {"stop_loss": 90000.0, "take_profit": 80000.0}
    assert p.open_conditional_orders(2) == []
    assert p.set_price(88000.0) == []                       # 미발동
    fired = p.set_price(90000.0)                            # SL 발동 → 레그 전량(0.005) 청산
    assert len(fired) == 1 and fired[0]["status"] == "Filled" and fired[0]["qty"] == pytest.approx(0.005)
    assert fired[0]["side"] == "Buy" and fired[0]["reduce_only"] is True
    assert p.positions() == {} and p.get_position_protection(2) is None
    ev = p.protection_events[-1]
    assert ev["kind"] == "sl" and ev["position_idx"] == 2 and ev["price"] == 90000.0
    assert p.get_order(ev["order_link_id"])["order_id"] == ev["order_id"]
    assert sum(x["qty"] for x in p.executions(ev["order_id"])) == pytest.approx(0.005)
    # TP 경로 (롱) + 해제
    p.place_market("Buy", 0.002, 1, False, "e3")
    p.set_position_protection(1, 80000.0, 95000.0)
    assert p.set_position_protection(1, None, None) == {"position_idx": 1, "stop_loss": None, "take_profit": None}
    assert p.set_price(95000.0) == [] and p.positions()[1]["size"] == pytest.approx(0.002)
    p.set_position_protection(1, 80000.0, 97000.0)
    fired = p.set_price(97000.0)
    assert len(fired) == 1 and p.protection_events[-1]["kind"] == "tp" and p.positions() == {}
    p.reset()
    assert p.protection_events == [] and p.position_protections == {}


def test_build_exchange_dispatch(settings, multi_settings, monkeypatch):
    import sys
    import types

    e = build_exchange(settings, "paper", price=12345.0)
    assert isinstance(e, PaperExchange) and e.last_price() == 12345.0 and e.account is settings.accounts[0]
    # kind None → account.exchange ('okx'/'toobit') → 지연 import. 모듈이 아직 없을 수 있으므로 가짜 모듈로 분기만 확인
    created = {}
    fake_okx = types.ModuleType("lake_executor.exchange_okx")
    fake_okx.OkxExchange = lambda acct: created.setdefault("okx", acct)   # type: ignore[attr-defined]
    fake_toobit = types.ModuleType("lake_executor.exchange_toobit")
    fake_toobit.ToobitExchange = lambda acct: created.setdefault("toobit", acct)   # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "lake_executor.exchange_okx", fake_okx)
    monkeypatch.setitem(sys.modules, "lake_executor.exchange_toobit", fake_toobit)
    build_exchange(multi_settings.account("okx"))
    build_exchange(multi_settings.account("toobit"), "toobit")
    assert created == {"okx": multi_settings.account("okx"), "toobit": multi_settings.account("toobit")}
    with pytest.raises(ValueError):
        build_exchange(settings, "binance")


def test_build_exchange_okx_without_module_raises_import_error(multi_settings, monkeypatch):
    import sys
    monkeypatch.setitem(sys.modules, "lake_executor.exchange_okx", None)   # import 를 강제로 실패시킨다
    with pytest.raises(ImportError):
        build_exchange(multi_settings.account("okx"))


def test_bybit_exchange_reads_keys_from_account(settings_factory):
    s = settings_factory(config_overrides={"testnet": True},
                         env_overrides={"BYBIT_API_KEY": REAL_KEY, "BYBIT_API_SECRET": REAL_SECRET})

    class FakeHTTP:
        def __init__(self, **kw):
            self.kw = kw

    import pybit.unified_trading as ut
    orig = ut.HTTP
    ut.HTTP = FakeHTTP
    try:
        ex = BybitExchange(s.accounts[0])
        ex2 = BybitExchange(s)           # Settings 호환
    finally:
        ut.HTTP = orig
    assert ex.http.kw["api_key"] == REAL_KEY and ex.http.kw["api_secret"] == REAL_SECRET and ex.http.kw["testnet"] is True
    assert ex.display_name == "Bybit" and ex.supports_lot_protection is True and ex.symbol == "BTCUSDT"
    assert ex2.account is s.accounts[0]
    with pytest.raises(NotImplementedError):
        ex.set_position_protection(1, 1.0, None)
    injected = BybitExchange(s.accounts[0], http=object())
    assert injected.account.name == "bybit"
