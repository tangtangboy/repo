"""다계정 팬아웃 (ARCHITECTURE_MULTI_EXCHANGE.md §5–§7) — executor / reporter / receiver / main 의 계정 범위 동작.

  - fanout: 3 Paper 계정(bybit/okx/toobit) 에 같은 신호 → 계정별 독립 lot, 계정별 회신 스트림(sequence/exchange/account_scope)
  - qty_multiplier: 계정 수량 배수 + expected_qty_btc_after 비교도 같은 배수
  - by_exchange 라우팅: 신호 exchange 'OKX' 는 okx 계정만; 대상 없음 → rejected/NO_TARGET_ACCOUNT (실행기) / 400 (수신기)
  - 한 계정의 ExchangeError 가 다른 계정을 막지 않고 그 계정 run 만 error
  - 포지션 단위 보호 경로 (Paper lot_protection=False): set_position_protection, 스냅샷 sl/tp 표시, 갱신/해제, crossed 즉시 실행
  - one_way 계정의 position_idx 매핑, hedge 계정에 idx 0 → POSITION_MODE_MISMATCH
  - reporter: report=false 계정은 unsent, 스트림별 독립 전송
  - recover_processing 이 run 결과가 없는 계정만 복구
  - receiver /state 계정 섹션, /admin/reconcile?account=, by_exchange 검증
  - main._exchanges_for / cmd_check (오프라인, 키 없음)
  - 1단계 호환: {"test": paper, "live": None} 모양의 exchanges, 단일 계정 기본값
전부 오프라인 (PaperExchange). OKX/Toobit 실 래퍼는 import 하지 않는다.
"""
from __future__ import annotations

import json
import os
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from lake_executor import main as main_mod, store as st
from lake_executor.exchange import ExchangeError, PaperExchange
from lake_executor.executor import Executor
from lake_executor.receiver import create_app
from lake_executor.reporter import Reporter
from lake_executor.store import Store

from conftest import (
    ADMIN_TOKEN,
    MULTI_ACCOUNTS,
    PAPER_PRICE,
    REPORT_URL_TEST,
    SIGNAL_PATH,
    TEST_SIGNAL_SECRET,
    FakeClient,
    build_settings,
    execution_statuses,
    ingest,
    load_reports,
    make_signal,
    run_signal,
    sign_body,
)

NAMES = ("bybit", "okx", "toobit")
DISPLAY = {"bybit": "Bybit", "okx": "OKX", "toobit": "Toobit"}
SCOPE = {"bybit": "lake_dedicated_BTCUSDT", "okx": "lake_dedicated_OKX_BTCUSDT", "toobit": "lake_dedicated_TOOBIT_BTCUSDT"}


# --------------------------------------------------------------------------- #
# 픽스처 / 헬퍼
# --------------------------------------------------------------------------- #
def _accounts(**over) -> list[dict]:
    out = []
    for a in MULTI_ACCOUNTS:
        d = dict(a)
        d.update(over.get(d["name"], {}))
        out.append(d)
    return out


class Rig:
    """settings + store + 계정별 Paper + reporter + executor 를 한 번에."""

    def __init__(self, settings, exchanges: dict[str, PaperExchange] | None = None, live: dict | None = None):
        self.settings = settings
        self.store = Store(settings.db_path)
        self.alerts = _Alerts()
        self.client = FakeClient()
        self.reporter = Reporter(settings, self.store, self.alerts, client=self.client)
        self.paper = exchanges if exchanges is not None else {a.name: PaperExchange(a, price=PAPER_PRICE)
                                                              for a in settings.accounts}
        self.executor = Executor(settings, self.store, {"test": dict(self.paper), "live": dict(live or {})},
                                 self.reporter, self.alerts)

    def run(self, d: dict) -> dict:
        return run_signal(self.executor, self.store, d)

    def close(self) -> None:
        self.store.close()


class _Alerts:
    def __init__(self):
        self.messages: list[str] = []

    def send(self, text: str) -> bool:
        self.messages.append(str(text))
        return True

    def contains(self, needle: str) -> bool:
        return any(needle in m for m in self.messages)


@pytest.fixture
def rig(multi_settings):
    r = Rig(multi_settings)
    yield r
    r.close()


@pytest.fixture
def rig_factory(tmp_path):
    rigs: list[Rig] = []
    n = [0]

    def make(config_overrides: dict | None = None, env_overrides: dict | None = None, drop_env=(), **kw) -> Rig:
        n[0] += 1
        s = build_settings(tmp_path / f"m{n[0]}", config_overrides, env_overrides, drop_env)
        r = Rig(s, **kw)
        rigs.append(r)
        return r

    yield make
    for r in rigs:
        r.close()


def _short_entry(pid: str, **over) -> dict:
    d = dict(position_id=pid, leg="short", position_idx=2, qty_btc=0.002, expected_qty_btc_after=0.002)
    d.update(over)
    return make_signal(**d)


# --------------------------------------------------------------------------- #
# fanout
# --------------------------------------------------------------------------- #
def test_fanout_runs_every_account_with_independent_lots_and_streams(rig):
    pid = "fan-1"
    row = rig.run(_short_entry(pid, stop_loss=88000, take_profit=[84000]))
    assert row["status"] == "done" and row["reason_code"] is None
    assert row["note"] == "bybit=done;okx=done;toobit=done"
    runs = {r["account"]: r for r in rig.store.get_runs("test", row["event_id"])}
    assert set(runs) == set(NAMES) and all(r["status"] == "done" for r in runs.values())

    for name in NAMES:
        lot = rig.store.get_lot("test", name, pid)
        assert lot and lot["account"] == name and lot["status"] == "open" and lot["qty"] == pytest.approx(0.002)
        assert rig.paper[name].positions()[2]["size"] == pytest.approx(0.002)
        assert sorted(c["trigger_price"] for c in rig.paper[name].open_conditional_orders(2)) == [84000.0, 88000.0]
        rs = load_reports(rig.store, "test", name)
        assert execution_statuses(rs) == ["acknowledged", "submitted", "filled", "snapshot"]
        assert [r["sequence"] for r in rs] == [1, 2, 3, 4]                     # 스트림별 독립 sequence
        assert all(r["exchange"] == DISPLAY[name] and r["symbol"] == "BTCUSDT" for r in rs)
        assert rs[-1]["account_scope"] == SCOPE[name]
        assert rs[-1]["positions"][0]["stop_loss"] == 88000.0 and rs[-1]["positions"][0]["take_profit"] == [84000.0]
        assert all(r["report_id"].startswith(f"r-test-{name}-") for r in rs)
        assert rig.store.is_inconsistent("test", name) is False
    # 계정 간 원장 격리: 한 계정의 lot 만 닫혀도 다른 계정은 그대로
    assert len(rig.store.open_lots("test")) == 3
    assert len(rig.store.open_lots("test", "okx")) == 1

    # 두 번째 신호 (partial_exit) 도 전 계정
    row = rig.run(_short_entry(pid, event_sequence=2, action="partial_exit", qty_btc=0.001, expected_qty_btc_after=0.001,
                               reference_price=None))
    assert row["status"] == "done"
    for name in NAMES:
        assert rig.store.get_lot("test", name, pid)["qty"] == pytest.approx(0.001)
        assert rig.store.fills_for_order(rig.store.get_order(
            __import__("lake_executor.util", fromlist=["order_link_id"]).order_link_id("test", row["event_id"]), name)["order_link_id"], name)
    # fills PK 는 (account, exec_id): 세 Paper 가 같은 'pexec-N' 을 내도 충돌 없음
    assert rig.store._q("SELECT COUNT(*) AS n FROM fills")[0]["n"] == 6


def test_reconcile_and_snapshot_are_per_account(rig):
    pid = "fan-recon"
    rig.run(_short_entry(pid, stop_loss=88000))
    # okx 거래소에만 수동 포지션 → okx 만 불일치
    rig.paper["okx"].place_market("Sell", 0.001, 2, False, "manual")
    assert rig.executor.reconcile("test", "okx") is False
    assert rig.executor.reconcile("test", "bybit") is True
    assert rig.executor.reconcile("test") is False                               # 전 계정 AND
    assert rig.store.is_inconsistent("test", "okx") is True and rig.store.is_inconsistent("test", "bybit") is False
    assert rig.executor.snapshot_now("test", "okx") is None
    assert rig.executor.snapshot_now("test", "bybit")["kind"] == "snapshot"
    res = rig.executor.snapshot_all("test")
    assert set(res) == set(NAMES) and res["okx"] is None and res["bybit"]["account"] == "bybit"
    with pytest.raises(ValueError):
        rig.executor.snapshot_now("test")                                        # 여러 계정이면 account 필수
    # 불일치 계정만 entry 거부, 다른 계정은 정상
    row = rig.run(_short_entry("fan-recon-2"))
    assert row["status"] == "done"
    runs = {r["account"]: r for r in rig.store.get_runs("test", row["event_id"])}
    assert runs["okx"]["status"] == "rejected" and runs["okx"]["reason_code"] == "RECONCILE_REQUIRED"
    assert runs["bybit"]["status"] == "done" and runs["toobit"]["status"] == "done"
    assert "okx=rejected/RECONCILE_REQUIRED" in row["note"]
    # SL 체결은 그 계정에서만 처리
    rig.paper["okx"].place_market("Buy", 0.001, 2, True, "manual-fix")
    rig.paper["toobit"].set_price(88000)
    assert rig.executor.reconcile("test") is True
    assert rig.store.get_lot("test", "toobit", pid)["status"] == "closed"
    assert rig.store.get_lot("test", "bybit", pid)["status"] == "open"
    auto = [r for r in load_reports(rig.store, "test", "toobit") if r["kind"] == "execution"
            and r["execution"]["event_id"].startswith("auto:sl:")]
    assert len(auto) == 1 and auto[0]["exchange"] == "Toobit"
    assert not any(r["kind"] == "execution" and r["execution"]["event_id"].startswith("auto:")
                   for r in load_reports(rig.store, "test", "bybit"))
    assert rig.executor.protection_missing("test") == []


# --------------------------------------------------------------------------- #
# qty_multiplier
# --------------------------------------------------------------------------- #
def test_qty_multiplier_scales_order_and_expected_qty(rig_factory):
    r = rig_factory(config_overrides={"routing": "fanout", "accounts": _accounts(okx={"qty_multiplier": 2.0},
                                                                                  toobit={"qty_multiplier": 0.5})})
    pid = "mult"
    row = r.run(_short_entry(pid, qty_btc=0.002, expected_qty_btc_after=0.002))
    assert row["status"] == "done" and "QTY_MISMATCH" not in row["note"]
    assert r.store.get_lot("test", "bybit", pid)["qty"] == pytest.approx(0.002)
    assert r.store.get_lot("test", "okx", pid)["qty"] == pytest.approx(0.004)
    assert r.store.get_lot("test", "toobit", pid)["qty"] == pytest.approx(0.001)
    assert r.paper["okx"].positions()[2]["size"] == pytest.approx(0.004)
    assert not r.alerts.contains("QTY_MISMATCH")
    fill = [x for x in load_reports(r.store, "test", "okx") if x["kind"] == "execution" and x["execution"]["status"] == "filled"]
    assert fill[0]["execution"]["qty"] == pytest.approx(0.004)
    # partial_exit 도 배수 적용, full_exit 는 lot 잔량
    row = r.run(_short_entry(pid, event_sequence=2, action="partial_exit", qty_btc=0.001, expected_qty_btc_after=0.001,
                             reference_price=None))
    assert row["status"] == "done" and "QTY_MISMATCH" not in row["note"]
    assert r.store.get_lot("test", "okx", pid)["qty"] == pytest.approx(0.002)
    assert r.store.get_lot("test", "toobit", pid)["qty"] == pytest.approx(0.0005) or \
        r.store.get_lot("test", "toobit", pid)["qty"] == pytest.approx(0.001)   # 0.0005 는 step 미만 → QTY_BELOW_MIN
    runs = {x["account"]: x for x in r.store.get_runs("test", row["event_id"])}
    assert runs["toobit"]["reason_code"] == "QTY_BELOW_MIN"
    row = r.run(_short_entry(pid, event_sequence=3, action="full_exit", qty_btc=0.001, expected_qty_btc_after=0,
                             reference_price=None))
    assert row["status"] == "done"
    assert all(r.store.get_lot("test", n, pid)["status"] == "closed" for n in NAMES)


# --------------------------------------------------------------------------- #
# by_exchange 라우팅
# --------------------------------------------------------------------------- #
def test_by_exchange_routes_only_matching_account(rig_factory):
    r = rig_factory(config_overrides={"routing": "by_exchange", "accounts": _accounts()})
    row = r.run(_short_entry("okx-only", exchange="OKX"))
    assert row["status"] == "done" and row["note"] == ""     # run 하나 → 1단계 note 그대로
    assert [x["account"] for x in r.store.get_runs("test", row["event_id"])] == ["okx"]
    assert r.store.get_lot("test", "okx", "okx-only")["qty"] == pytest.approx(0.002)
    assert r.store.get_lot("test", "bybit", "okx-only") is None and r.store.get_lot("test", "toobit", "okx-only") is None
    assert r.paper["bybit"].positions() == {} and r.paper["toobit"].positions() == {}
    assert load_reports(r.store, "test", "bybit") == [] and load_reports(r.store, "test", "toobit") == []
    assert execution_statuses(load_reports(r.store, "test", "okx")) == ["acknowledged", "submitted", "filled", "snapshot"]
    row = r.run(_short_entry("bybit-only", exchange="bybit".capitalize()))
    assert [x["account"] for x in r.store.get_runs("test", row["event_id"])] == ["bybit"]
    row = r.run(_short_entry("toobit-only", exchange="Toobit"))
    assert [x["account"] for x in r.store.get_runs("test", row["event_id"])] == ["toobit"]


def test_no_target_account_is_rejected_without_reports(rig_factory):
    r = rig_factory(config_overrides={"routing": "by_exchange", "accounts": _accounts(okx={"enabled": False})})
    row = r.run(_short_entry("no-target", exchange="OKX"))
    assert row["status"] == "rejected" and row["reason_code"] == "NO_TARGET_ACCOUNT"
    assert "by_exchange" in row["note"] and r.store.get_runs("test", row["event_id"]) == []
    assert all(load_reports(r.store, "test", n) == [] for n in NAMES)
    assert r.alerts.contains("NO_TARGET_ACCOUNT")
    # disabled 계정은 fanout 에서도 제외
    r2 = rig_factory(config_overrides={"routing": "fanout", "accounts": _accounts(okx={"enabled": False})})
    row = r2.run(_short_entry("fan-disabled"))
    assert row["status"] == "done"
    assert [x["account"] for x in r2.store.get_runs("test", row["event_id"])] == ["bybit", "toobit"]
    assert r2.store.get_lot("test", "okx", "fan-disabled") is None


# --------------------------------------------------------------------------- #
# 한 계정의 오류가 다른 계정을 막지 않는다
# --------------------------------------------------------------------------- #
class BrokenPaper(PaperExchange):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.fail = True

    def place_market(self, *a, **kw):
        if self.fail:
            raise ExchangeError("EXCHANGE_ERROR", "simulated outage")
        return super().place_market(*a, **kw)


class ExplodingPaper(PaperExchange):
    def place_market(self, *a, **kw):
        raise RuntimeError("boom")


def test_exchange_error_on_one_account_does_not_block_others(multi_settings):
    papers = {a.name: (BrokenPaper if a.name == "okx" else PaperExchange)(a, price=PAPER_PRICE) for a in multi_settings.accounts}
    r = Rig(multi_settings, exchanges=papers)
    try:
        pid = "one-broken"
        row = r.run(_short_entry(pid, stop_loss=88000))
        assert row["status"] == "error" and row["reason_code"] == "EXCHANGE_ERROR"
        assert row["note"] == "bybit=done;okx=error/EXCHANGE_ERROR;toobit=done"
        runs = {x["account"]: x for x in r.store.get_runs("test", row["event_id"])}
        assert runs["okx"]["status"] == "error" and runs["okx"]["reason_code"] == "EXCHANGE_ERROR"
        assert runs["bybit"]["status"] == "done" and runs["toobit"]["status"] == "done"
        assert r.store.get_lot("test", "okx", pid) is None
        assert r.store.get_lot("test", "bybit", pid)["qty"] == pytest.approx(0.002)
        assert r.store.get_lot("test", "toobit", pid)["qty"] == pytest.approx(0.002)
        assert execution_statuses(load_reports(r.store, "test", "okx")) == ["acknowledged", "error", "snapshot"]
        assert load_reports(r.store, "test", "okx")[1]["execution"]["reason_code"] == "EXCHANGE_ERROR"
        assert execution_statuses(load_reports(r.store, "test", "bybit")) == ["acknowledged", "submitted", "filled", "snapshot"]
        assert execution_statuses(load_reports(r.store, "test", "toobit")) == ["acknowledged", "submitted", "filled", "snapshot"]
        assert r.alerts.contains("[executor:test/okx]")
        # 복구 뒤 okx 만 다시 진입 가능 (다른 계정은 POSITION_EXISTS)
        papers["okx"].fail = False
        row = r.run(_short_entry(pid, event_sequence=2))
        runs = {x["account"]: x for x in r.store.get_runs("test", row["event_id"])}
        assert runs["okx"]["status"] == "done"
        assert runs["bybit"]["reason_code"] == "POSITION_EXISTS" and runs["toobit"]["reason_code"] == "POSITION_EXISTS"
        assert row["status"] == "done"       # 하나라도 done 이면 done
    finally:
        r.close()


def test_unexpected_exception_on_one_account_is_unknown_state_for_that_account_only(multi_settings):
    papers = {a.name: (ExplodingPaper if a.name == "toobit" else PaperExchange)(a, price=PAPER_PRICE)
              for a in multi_settings.accounts}
    r = Rig(multi_settings, exchanges=papers)
    try:
        row = r.run(_short_entry("explode"))
        assert row["status"] == "error" and row["reason_code"] == "UNKNOWN_STATE"
        runs = {x["account"]: x for x in r.store.get_runs("test", row["event_id"])}
        assert runs["toobit"]["status"] == "error" and runs["toobit"]["reason_code"] == "UNKNOWN_STATE"
        assert runs["bybit"]["status"] == "done" and runs["okx"]["status"] == "done"
        assert r.executor.run_once() is False
    finally:
        r.close()


# --------------------------------------------------------------------------- #
# 포지션 단위 보호 (lot_protection=False)
# --------------------------------------------------------------------------- #
def _position_rig(multi_settings) -> Rig:
    papers = {a.name: PaperExchange(a, price=PAPER_PRICE, lot_protection=(a.name != "toobit"))
              for a in multi_settings.accounts}
    return Rig(multi_settings, exchanges=papers)


def test_position_level_protection_path(multi_settings):
    r = _position_rig(multi_settings)
    tb = r.paper["toobit"]
    try:
        assert tb.supports_lot_protection is False
        pid = "pos-prot"
        row = r.run(_short_entry(pid, stop_loss=88000, take_profit=[84000, 83000]))
        assert row["status"] == "done" and row["note"] == "bybit=done;okx=done;toobit=done"
        lot = r.store.get_lot("test", "toobit", pid)
        po = lot["protection_orders"]
        assert po["kind"] == "position" and po["sl"] is None and po["tp"] == []
        assert po["position"]["stop_loss"] == 88000.0 and po["position"]["take_profit"] == 84000.0 and po["position"]["tp_i"] == 0
        assert po["skipped"] == ["tp1"]                                   # 포지션 단위 TP 는 첫 레벨만
        assert tb.get_position_protection(2) == {"stop_loss": 88000.0, "take_profit": 84000.0}
        assert tb.open_conditional_orders(2) == []
        assert r.executor.protection_missing("test", "toobit") == []
        # 다른 계정(lot 보호) 은 조건부 주문 2+1 개
        assert len(r.paper["bybit"].open_conditional_orders(2)) == 3
        # 스냅샷: 포지션 단위 값 표시
        snap = load_reports(r.store, "test", "toobit")[-1]
        assert snap["kind"] == "snapshot" and snap["exchange"] == "Toobit"
        assert snap["positions"][0]["stop_loss"] == 88000.0 and snap["positions"][0]["take_profit"] == [84000.0]
        assert r.executor.reconcile("test", "toobit") is True

        # protection_update → 레그 값 갱신
        pu = make_signal(position_id=pid, leg="short", position_idx=2, event_sequence=2, action="protection_update",
                         qty_btc=None, expected_qty_btc_after=None, reference_price=None, protection_revision=2,
                         stop_loss=89000, take_profit=[85000])
        assert r.run(pu)["status"] == "done"
        assert tb.get_position_protection(2) == {"stop_loss": 89000.0, "take_profit": 85000.0}
        lot = r.store.get_lot("test", "toobit", pid)
        assert lot["protection_revision"] == 2 and lot["protection_orders"]["position"]["stop_loss"] == 89000.0
        rs = load_reports(r.store, "test", "toobit")
        assert execution_statuses(rs)[-3:] == ["acknowledged", "protection_updated", "snapshot"]
        assert rs[-1]["positions"][0]["stop_loss"] == 89000.0 and rs[-1]["positions"][0]["take_profit"] == [85000.0]

        # partial_exit → 해제 후 재설정 (값 유지)
        assert r.run(_short_entry(pid, event_sequence=3, action="partial_exit", qty_btc=0.001, expected_qty_btc_after=0.001,
                                  reference_price=None))["status"] == "done"
        assert tb.get_position_protection(2) == {"stop_loss": 89000.0, "take_profit": 85000.0}
        assert tb.positions()[2]["size"] == pytest.approx(0.001)

        # full_exit → 해제, lot closed, flat 스냅샷
        assert r.run(_short_entry(pid, event_sequence=4, action="full_exit", qty_btc=0.001, expected_qty_btc_after=0,
                                  reference_price=None))["status"] == "done"
        lot = r.store.get_lot("test", "toobit", pid)
        assert lot["status"] == "closed" and lot["protection_orders"]["position"] is None
        assert tb.get_position_protection(2) is None and tb.positions() == {}
        assert load_reports(r.store, "test", "toobit")[-1]["positions"] == []
        assert r.store.lots_with_pending_cancel("test", "toobit") == []
    finally:
        r.close()


def test_position_level_protection_crossed_executes_market_close(multi_settings):
    r = _position_rig(multi_settings)
    tb = r.paper["toobit"]
    try:
        pid = "pos-crossed"
        row = r.run(_short_entry(pid, stop_loss=85000))      # 숏인데 SL 85000 < 현재가 86000 → 이미 지남
        assert row["status"] == "done"
        runs = {x["account"]: x for x in r.store.get_runs("test", row["event_id"])}
        assert runs["toobit"]["status"] == "done" and runs["bybit"]["status"] == "done"
        lot = r.store.get_lot("test", "toobit", pid)
        assert lot["status"] == "closed" and tb.positions() == {} and tb.get_position_protection(2) is None
        auto = [x["execution"] for x in load_reports(r.store, "test", "toobit")
                if x["kind"] == "execution" and x["execution"]["event_id"].startswith("auto:sl:")]
        assert auto and auto[-1]["status"] == "filled" and auto[-1]["action"] == "full_exit"
        assert auto[-1]["reason_code"] == "STOP_LOSS_TRIGGERED"
        assert r.alerts.contains("already crossed")
        # lot 보호 계정도 같은 결과 (1단계 경로)
        assert r.store.get_lot("test", "bybit", pid)["status"] == "closed"
    finally:
        r.close()


def test_position_level_protection_fire_surfaces_as_inconsistency(multi_settings):
    """포지션 단위 보호가 발동하면 거래소는 flat 인데 lot 은 open — 체결 경합 감지가 불가능하므로 reconcile ⑤ 가
    RECONCILE_REQUIRED 로 드러낸다 (운영자 정리). 다른 계정은 영향 없음."""
    r = _position_rig(multi_settings)
    tb = r.paper["toobit"]
    try:
        pid = "pos-fire"
        r.run(_short_entry(pid, stop_loss=88000))
        fired = tb.set_price(88000)
        assert len(fired) == 1 and tb.positions() == {} and tb.protection_events[-1]["kind"] == "sl"
        assert r.executor.reconcile("test", "toobit") is False
        assert r.store.is_inconsistent("test", "toobit") is True
        assert r.alerts.contains("[executor:test/toobit] RECONCILE_REQUIRED")
        assert r.executor.reconcile("test", "bybit") is True
    finally:
        r.close()


def test_position_level_protection_failure_is_self_healed_by_reconcile(multi_settings):
    class FlakyPositionPaper(PaperExchange):
        def __init__(self, *a, **kw):
            super().__init__(*a, **kw)
            self.errors: list[Exception] = []

        def set_position_protection(self, idx, sl, tp):
            if self.errors:
                raise self.errors.pop(0)
            return super().set_position_protection(idx, sl, tp)

    papers = {a.name: PaperExchange(a, price=PAPER_PRICE) for a in multi_settings.accounts}
    papers["toobit"] = FlakyPositionPaper(multi_settings.account("toobit"), price=PAPER_PRICE, lot_protection=False)
    r = Rig(multi_settings, exchanges=papers)
    try:
        # 신호 처리 + 직후 스냅샷의 reconcile 자가 복구까지 두 번 실패시킨다
        papers["toobit"].errors = [ExchangeError("EXCHANGE_TIMEOUT", "t"), ExchangeError("EXCHANGE_TIMEOUT", "t")]
        pid = "pos-flaky"
        row = r.run(_short_entry(pid, stop_loss=88000))
        assert row["status"] == "done" and "toobit=done PROTECTION_FAILED" in row["note"]
        assert r.executor.protection_missing("test", "toobit") == [pid]
        assert papers["toobit"].get_position_protection(2) is None
        snap = load_reports(r.store, "test", "toobit")[-1]
        assert snap["kind"] == "snapshot" and snap["positions"][0]["stop_loss"] is None   # 정직한 스냅샷
        assert r.executor.reconcile("test", "toobit") is True
        assert r.executor.protection_missing("test", "toobit") == []
        assert papers["toobit"].get_position_protection(2) == {"stop_loss": 88000.0, "take_profit": None}
    finally:
        r.close()


# --------------------------------------------------------------------------- #
# position_idx 매핑 (one_way 계정)
# --------------------------------------------------------------------------- #
def test_one_way_account_maps_hedge_signal_to_idx0(rig_factory):
    r = rig_factory(config_overrides={"routing": "fanout", "accounts": _accounts(okx={"position_mode": "one_way"})})
    pid = "ow-map"
    row = r.run(_short_entry(pid, stop_loss=88000))
    assert row["status"] == "done"
    assert r.store.get_lot("test", "okx", pid)["position_idx"] == 0
    assert r.store.get_lot("test", "bybit", pid)["position_idx"] == 2
    assert r.paper["okx"].positions()[0]["side"] == "Sell" and r.paper["okx"].positions()[0]["size"] == pytest.approx(0.002)
    rs = load_reports(r.store, "test", "okx")
    assert all(x["execution"]["position_idx"] == 0 for x in rs if x["kind"] == "execution")
    assert rs[-1]["positions"][0]["position_idx"] == 0 and rs[-1]["positions"][0]["stop_loss"] == 88000.0
    assert r.executor.reconcile("test") is True
    # hedge 계정에 idx 0 신호 → 그 계정만 POSITION_MODE_MISMATCH, one_way 계정은 실행
    row = r.run(_short_entry("ow-direct", position_idx=0))
    runs = {x["account"]: x for x in r.store.get_runs("test", row["event_id"])}
    assert runs["okx"]["status"] == "done"
    assert runs["bybit"]["reason_code"] == "POSITION_MODE_MISMATCH" and runs["toobit"]["reason_code"] == "POSITION_MODE_MISMATCH"
    # one_way 계정에서 반대 레그는 OPPOSING_LEG
    row = r.run(make_signal(position_id="ow-long", leg="long", position_idx=1))
    runs = {x["account"]: x for x in r.store.get_runs("test", row["event_id"])}
    assert runs["okx"]["reason_code"] == "OPPOSING_LEG" and runs["bybit"]["status"] == "done"


# --------------------------------------------------------------------------- #
# reporter: 스트림별 전송
# --------------------------------------------------------------------------- #
def test_reporter_delivers_only_report_true_accounts(rig):
    rig.run(_short_entry("rep-1"))
    rig.client.queue(202, 202, 202, 202)
    assert rig.reporter.deliver_pending("test") == 4            # bybit 4건만 전송
    assert all(json.loads(p["content"])["exchange"] == "Bybit" for p in rig.client.posts)
    assert [json.loads(p["content"])["sequence"] for p in rig.client.posts] == [1, 2, 3, 4]
    assert all(p["url"] == REPORT_URL_TEST for p in rig.client.posts)
    for name in ("okx", "toobit"):
        rows = rig.store._q("SELECT state, note FROM reports WHERE account=? AND mode='test'", (name,))
        assert rows and all(x["state"] == st.REPORT_UNSENT and "report=false" in x["note"] or "disabled" in x["note"]
                            for x in rows)
    assert rig.store.pending_reports("test") == []
    assert rig.reporter.deliver_pending("test", "bybit") == 0


def test_reporter_per_account_url_override_and_independent_retry(rig_factory):
    r = rig_factory(config_overrides={"routing": "fanout", "accounts": _accounts(okx={"report": True})},
                    env_overrides={"LAKE_REPORT_URL_TEST_OKX": "http://okx.test/report",
                                   "LAKE_REPORT_SECRET_TEST_OKX": "okx-report-secret-0123456789abcdef-0123456789"})
    r.run(_short_entry("rep-2"))
    r.client.queue(503)                                         # bybit seq1 → 재시도 대기
    assert r.reporter.deliver_pending("test", "bybit") == 0
    r.client.queue(202, 202, 202, 202)
    assert r.reporter.deliver_pending("test", "okx") == 4       # okx 스트림은 독립적으로 진행
    okx_posts = [p for p in r.client.posts if p["url"] == "http://okx.test/report"]
    assert len(okx_posts) == 4 and all(json.loads(p["content"])["exchange"] == "OKX" for p in okx_posts)
    assert r.reporter._retry_after[("test", "bybit")] > 0 and ("test", "okx") not in r.reporter._retry_after
    r.client.queue(202, 202, 202, 202)
    assert r.reporter.deliver_pending("test") == 4              # bybit 재시도 성공, toobit 은 unsent
    with pytest.raises(Exception):
        r.reporter.execution("test", "nope", event_id="e", position_id="p", strategy="basic", leg="long",
                             position_idx=1, action="entry", status="acknowledged")


# --------------------------------------------------------------------------- #
# recover_processing (계정별)
# --------------------------------------------------------------------------- #
def test_recover_processing_only_touches_accounts_without_run_result(rig):
    d = _short_entry("recover-multi")
    assert ingest(rig.store, d) == "new"
    row = rig.store.claim_next_signal()
    assert row["event_id"] == d["event_id"]
    # bybit 는 이미 끝난 것으로 기록, okx/toobit 은 run 없음 (크래시 상황)
    rig.store.set_run_result("test", d["event_id"], "bybit", "done", None, "")
    rig.executor.recover_processing()
    row = rig.store.get_signal(d["event_id"], "test")
    assert row["status"] == "error" and row["reason_code"] == "UNKNOWN_STATE"
    runs = {x["account"]: x for x in rig.store.get_runs("test", d["event_id"])}
    assert runs["bybit"]["status"] == "done"
    assert runs["okx"]["reason_code"] == "UNKNOWN_STATE" and runs["toobit"]["reason_code"] == "UNKNOWN_STATE"
    assert load_reports(rig.store, "test", "bybit") == []                       # 끝난 계정은 건드리지 않는다
    assert execution_statuses(load_reports(rig.store, "test", "okx")) == ["error"]
    assert rig.executor.run_once() is False


def test_ensure_account_setup_is_per_account_and_idempotent(rig_factory):
    real = {"BYBIT_API_KEY": "k" * 24, "BYBIT_API_SECRET": "s" * 40,
            "TOOBIT_API_KEY": "k" * 24, "TOOBIT_API_SECRET": "s" * 40}
    s_over = {"routing": "fanout", "live": {"enabled": True}, "accounts": _accounts(bybit={"leverage": 7})}
    r0 = rig_factory(config_overrides=s_over, env_overrides=real)
    live = {a.name: PaperExchange(a, price=PAPER_PRICE) for a in r0.settings.accounts}
    r = Rig(r0.settings, live=live)
    try:
        r.executor.ensure_account_setup()
        assert live["bybit"].account_setup == {"position_mode": "hedge", "leverage": 7, "margin_mode": "isolated"}
        assert live["toobit"].account_setup == {"position_mode": "hedge", "leverage": 5, "margin_mode": "isolated"}
        assert live["okx"].account_setup is None                                  # 실키 없음 → live 불가 → 건너뜀
        assert r.store.get_meta("account_setup:live:bybit") == "hedge|7|isolated|False|BTCUSDT"
        live["bybit"].account_setup = None
        r.executor.ensure_account_setup()
        assert live["bybit"].account_setup is None                                # 같은 서명 → 거래소 쓰기 없음
        # live 신호: 키 있는 계정만 실행, okx 는 LIVE_DISABLED
        row = r.run(make_signal(mode="live", position_id="live-fan", leg="short", position_idx=2))
        runs = {x["account"]: x for x in r.store.get_runs("live", row["event_id"])}
        assert runs["bybit"]["status"] == "done" and runs["toobit"]["status"] == "done"
        assert runs["okx"]["status"] == "rejected" and runs["okx"]["reason_code"] == "LIVE_DISABLED"
        assert row["status"] == "done"
    finally:
        r.close()


# --------------------------------------------------------------------------- #
# receiver: /state 계정 섹션, /admin/reconcile?account=, by_exchange 검증
# --------------------------------------------------------------------------- #
@pytest.fixture
def multi_client(rig):
    app = create_app(rig.settings, rig.store, SimpleNamespace(executor=rig.executor, reporter=rig.reporter, alerts=rig.alerts))
    with TestClient(app) as c:
        yield c


def test_state_has_per_account_sections(rig, multi_client):
    rig.run(_short_entry("state-1", stop_loss=88000))
    rig.paper["okx"].place_market("Sell", 0.001, 2, False, "manual")
    assert rig.executor.reconcile("test") is False
    h = {"X-Admin-Token": ADMIN_TOKEN}
    r = multi_client.get("/state", headers=h)
    assert r.status_code == 200
    body = r.json()
    assert body["routing"] == "fanout" and set(body["accounts"]) == set(NAMES)
    assert body["inconsistent"] == {"test": True, "live": False}
    assert "okx:" in body["inconsistent_note"]["test"] and "idx2" in body["inconsistent_note"]["test"]
    okx = body["accounts"]["okx"]
    assert okx["exchange"] == "okx" and okx["display_name"] == "OKX" and okx["report"] is False
    assert okx["live_execution_possible"] is False and okx["live_block_reason"] == "LIVE_DISABLED"
    assert okx["modes"]["test"]["inconsistent"] is True and okx["modes"]["test"]["exchange_ready"] is True
    assert okx["modes"]["live"]["exchange_ready"] is False
    assert len(okx["modes"]["test"]["open_lots"]) == 1 and okx["modes"]["test"]["open_lots"][0]["account"] == "okx"
    assert okx["modes"]["test"]["runs"][0]["status"] == "done"
    assert okx["modes"]["test"]["exchange_positions"]["2"]["size"] == pytest.approx(0.003)
    assert [x["account"] for x in body["accounts"]["bybit"]["modes"]["test"]["reports"]] == ["bybit"] * 4
    assert body["snapshot_positions"]["test"]["bybit"][0]["stop_loss"] == 88000.0
    assert len(body["open_lots"]["test"]) == 3 and body["protection_missing"] == {"test": [], "live": []}
    assert multi_client.get("/healthz").json()["inconsistent"] == {"test": True, "live": False}
    for s in (TEST_SIGNAL_SECRET, ADMIN_TOKEN):
        assert s not in r.text


def test_admin_reconcile_per_account(rig, multi_client):
    rig.run(_short_entry("recon-api"))
    rig.paper["toobit"].place_market("Sell", 0.001, 2, False, "manual")
    h = {"X-Admin-Token": ADMIN_TOKEN}
    r = multi_client.post("/admin/reconcile?mode=test&account=toobit", headers=h)
    assert r.status_code == 200
    body = r.json()
    assert body["consistent"] is False and body["account"] == "toobit" and list(body["accounts"]) == ["toobit"]
    assert body["accounts"]["toobit"]["inconsistent"] is True and "idx2" in body["inconsistent_note"]
    r = multi_client.post("/admin/reconcile?mode=test&account=bybit", headers=h)
    assert r.json()["consistent"] is True and r.json()["positions"]["bybit"][0]["position_id"] == "recon-api"
    r = multi_client.post("/admin/reconcile?mode=test", headers=h)
    assert r.json()["consistent"] is False and set(r.json()["accounts"]) == set(NAMES)
    assert r.json()["accounts"]["bybit"]["consistent"] is True
    assert multi_client.post("/admin/reconcile?mode=test&account=nope", headers=h).status_code == 400
    assert multi_client.post("/admin/reconcile?mode=live", headers=h).json()["ok"] is False   # live 거래소 없음


def test_receiver_by_exchange_validation(rig_factory):
    r = rig_factory(config_overrides={"routing": "by_exchange", "accounts": _accounts(okx={"enabled": False},
                                                                                      toobit={"position_mode": "hedge"})})
    app = create_app(r.settings, r.store, SimpleNamespace(executor=r.executor))
    with TestClient(app) as c:
        def post(d):
            raw, headers = sign_body(d, TEST_SIGNAL_SECRET)
            return c.post(SIGNAL_PATH, content=raw, headers=headers)
        d = _short_entry("rx-okx", exchange="OKX")
        resp = post(d)
        assert resp.status_code == 400 and resp.json()["code"] == "NO_TARGET_ACCOUNT"
        assert r.store.get_signal(d["event_id"]) is None
        rows = r.store._q("SELECT code FROM ingress_log ORDER BY id DESC LIMIT 1")
        assert rows and rows[0]["code"] == "NO_TARGET_ACCOUNT"
        assert post(_short_entry("rx-bybit", exchange="Bybit")).status_code == 202
        assert post(_short_entry("rx-toobit", exchange="Toobit")).status_code == 202
        # position_idx 검증은 대상 계정 기준: bybit(hedge) 에 idx 0 → 400
        assert post(_short_entry("rx-idx0", exchange="Bybit", position_idx=0)).status_code == 400
    # fanout 에서는 exchange 필드를 라우팅에 쓰지 않는다 (lake 는 Bybit 만 보냄)
    r2 = rig_factory(config_overrides={"routing": "fanout", "accounts": _accounts(okx={"position_mode": "one_way"})})
    app2 = create_app(r2.settings, r2.store, SimpleNamespace(executor=r2.executor))
    with TestClient(app2) as c:
        raw, headers = sign_body(_short_entry("rx-fan", exchange="OKX"), TEST_SIGNAL_SECRET)
        assert c.post(SIGNAL_PATH, content=raw, headers=headers).status_code == 202
        raw, headers = sign_body(_short_entry("rx-fan-0", position_idx=0), TEST_SIGNAL_SECRET)
        assert c.post(SIGNAL_PATH, content=raw, headers=headers).status_code == 202   # one_way 계정이 있으면 idx 0 허용


# --------------------------------------------------------------------------- #
# main: 거래소 준비 / check
# --------------------------------------------------------------------------- #
def test_main_exchanges_for_builds_paper_per_enabled_account(settings_factory):
    s = settings_factory(config_overrides={"routing": "fanout", "accounts": _accounts(okx={"enabled": False})})
    ex = main_mod._exchanges_for(s)
    assert set(ex) == {"live", "test"} and ex["live"] == {}
    assert set(ex["test"]) == {"bybit", "toobit"}
    assert ex["test"]["toobit"].display_name == "Toobit" and ex["test"]["toobit"].name == "paper"
    s2 = settings_factory(config_overrides={"test": {"simulate_fills": False}})
    assert main_mod._exchanges_for(s2) == {"live": {}, "test": {}}


def test_main_check_reports_each_account_offline(tmp_path, capsys):
    root = tmp_path / "chk"
    build_settings(root, {"routing": "fanout", "accounts": _accounts()})
    rc = main_mod.main(["--config", str(root / "config.json"), "--env", str(root / ".env"), "check"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "routing           : fanout  accounts=3" in out
    for name in NAMES:
        assert f"[{name}] account" in out and f"[{name}] connect        : skipped (no real API keys)" in out
    assert "[okx] api key        : missing (OKX_API_KEY/_API_SECRET/_API_PASSPHRASE)" in out
    assert "[okx] report test    : url set  secret set  (report=false → unsent)" in out
    assert "result            : OK" in out
    for secret in (TEST_SIGNAL_SECRET, ADMIN_TOKEN):
        assert secret not in out
    # live.enabled 인데 어느 계정도 실키가 없으면 문제로 표시
    root2 = tmp_path / "chk2"
    build_settings(root2, {"live": {"enabled": True}})
    assert main_mod.main(["--config", str(root2 / "config.json"), "--env", str(root2 / ".env"), "check"]) == 1


# --------------------------------------------------------------------------- #
# 1단계 호환
# --------------------------------------------------------------------------- #
def test_single_account_config_and_old_exchanges_shape_still_work(settings, alerts, fake_client):
    store = Store(settings.db_path)
    try:
        paper = PaperExchange(settings, price=PAPER_PRICE)
        reporter = Reporter(settings, store, alerts, client=fake_client)
        ex = Executor(settings, store, {"test": paper, "live": None}, reporter, alerts)   # 1단계 모양
        assert ex.exchanges == {"test": {"bybit": paper}, "live": {}}
        d = _short_entry("compat", stop_loss=88000)
        row = run_signal(ex, store, d)
        assert row["status"] == "done" and row["reason_code"] is None and row["note"] == ""
        assert store.get_lot("test", "bybit", "compat")["account"] == "bybit"
        assert [r["account"] for r in store.get_runs("test", d["event_id"])] == ["bybit"]
        rs = load_reports(store, "test")                      # 기본 계정 'bybit'
        assert execution_statuses(rs) == ["acknowledged", "submitted", "filled", "snapshot"]
        assert rs[0]["exchange"] == "Bybit" and rs[-1]["account_scope"] == "lake_dedicated_BTCUSDT"
        # account 생략 호출 (계정 하나) 도 된다
        assert ex.reconcile("test") is True and ex.snapshot_now("test")["account"] == "bybit"
        assert ex.build_snapshot_positions("test")[0]["position_id"] == "compat"
        assert ex.protection_missing("test") == []
        alloc = reporter.execution("test", event_id="x-1", position_id="compat", strategy="overheat", leg="short",
                                   position_idx=2, action="entry", status="acknowledged")
        assert alloc["account"] == "bybit"
        assert os.path.exists(settings.db_path)
    finally:
        store.close()
