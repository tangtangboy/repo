"""v0.2 리뷰 지적 사항 회귀 테스트 (다계정/다거래소 강화).

  - 포지션 단위 보호(레그 단위 상태): 한 lot 을 닫을 때 형제 lot 의 SL/TP 를 다시 적용(레그를 비우지 않음),
    스냅샷·protection_missing 은 lot 별 po.position 이 아니라 레그 상태를 본다, 해제 실패는 reconcile 이 재시도
  - 만료 판정은 팬아웃 전에 한 번 (앞 계정의 느린 체결로 뒤 계정만 EXPIRED 가 되지 않는다)
  - Paper 거래소의 거래소별 최소 수량 (test 가 live 의 QTY_BELOW_MIN 을 예측)
  - OKX 식 절단 clOrdId 를 원장 접두사로 되찾아 고아 보호주문을 취소
  - 1단계 DB 마이그레이션 대상 계정 이름 설정 / 원장-설정 계정 불일치 기동 거부 / live 거래소 생성 실패 fail-fast
  - 설정 검증: env_prefix/env_suffix 중복, Toobit testnet, 비숫자 필드 → ConfigError
전부 오프라인 (PaperExchange).
"""
from __future__ import annotations

import time

import pytest

from lake_executor import main as main_mod
from lake_executor.config import ConfigError
from lake_executor.exchange import ExchangeError, PaperExchange, build_exchange
from lake_executor.executor import Executor
from lake_executor.reporter import Reporter
from lake_executor.store import Store
from lake_executor.util import now_ms, order_link_id

from conftest import MULTI_ACCOUNTS, PAPER_PRICE, load_reports, make_signal, run_signal
from test_multi_account import Rig, _position_rig, _short_entry
from test_store_migration import LINK as V1_LINK, PID as V1_PID, make_v1_db


def _accounts(**over) -> list[dict]:
    out = []
    for a in MULTI_ACCOUNTS:
        d = dict(a)
        d.update(over.get(d["name"], {}))
        out.append(d)
    return out


def _full_exit(pid: str, seq: int, qty: float = 0.002) -> dict:
    return _short_entry(pid, event_sequence=seq, action="full_exit", qty_btc=qty, expected_qty_btc_after=0,
                        reference_price=None)


class FlakyPositionPaper(PaperExchange):
    """set_position_protection 이 errors 큐의 예외를 앞에서부터 던진다 (해제/설정 실패 주입)."""

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.errors: list[Exception] = []

    def set_position_protection(self, idx, sl, tp):
        if self.errors:
            raise self.errors.pop(0)
        return super().set_position_protection(idx, sl, tp)


class NoReaderPaper(PaperExchange):
    """실제 ToobitExchange 처럼 포지션 단위 보호 읽기 API 가 없는 거래소."""

    get_position_protection = None   # type: ignore[assignment]

    def peek_protection(self, idx):
        return PaperExchange.get_position_protection(self, idx)


# --------------------------------------------------------------------------- #
# 포지션 단위 보호 — 레그 단위 상태
# --------------------------------------------------------------------------- #
def test_closing_one_lot_re_applies_sibling_leg_protection(multi_settings):
    """같은 레그(idx2)에 lot A/B. A 를 닫아도 B 의 SL/TP 가 레그에 그대로 걸려 있어야 한다 (레그를 비우지 않음)."""
    r = _position_rig(multi_settings)
    tb = r.paper["toobit"]
    try:
        assert r.run(_short_entry("A", stop_loss=90000, take_profit=[82000]))["status"] == "done"
        assert r.run(_short_entry("B", stop_loss=91000, take_profit=[81000]))["status"] == "done"
        assert tb.get_position_protection(2) == {"stop_loss": 91000.0, "take_profit": 81000.0}   # 마지막 갱신이 레그 전체
        assert r.store.get_leg_protection("test", "toobit", 2)["position_id"] == "B"

        row = r.run(_full_exit("A", 2))
        assert row["status"] == "done" and "PROTECTION_FAILED" not in (row["note"] or "")
        lot_a = r.store.get_lot("test", "toobit", "A")
        assert lot_a["status"] == "closed" and lot_a["protection_orders"]["position"] is None
        # 거래소의 레그 보호는 B 의 값으로 유지된다
        assert tb.get_position_protection(2) == {"stop_loss": 91000.0, "take_profit": 81000.0}
        assert tb.positions()[2]["size"] == pytest.approx(0.002)
        assert r.executor.protection_missing("test", "toobit") == []
        leg = r.store.get_leg_protection("test", "toobit", 2)
        assert leg["position_id"] == "B" and leg["stop_loss"] == 91000.0 and leg["take_profit"] == 81000.0
        assert r.store.lots_with_pending_cancel("test", "toobit") == []
        snap = load_reports(r.store, "test", "toobit")[-1]
        assert snap["kind"] == "snapshot" and [p["position_id"] for p in snap["positions"]] == ["B"]
        assert snap["positions"][0]["stop_loss"] == 91000.0 and snap["positions"][0]["take_profit"] == [81000.0]
        assert r.alerts.messages == [] or not r.alerts.contains("PROTECTION_FAILED")

        # B 의 손절이 여전히 발동한다 (레그가 비어 있었다면 아무것도 닫히지 않았을 것)
        fired = tb.set_price(95000)
        assert len(fired) == 1 and tb.positions() == {} and tb.protection_events[-1]["kind"] == "sl"
    finally:
        r.close()


def test_partial_exit_on_one_lot_keeps_leg_protected_and_last_update_wins(multi_settings):
    r = _position_rig(multi_settings)
    tb = r.paper["toobit"]
    try:
        r.run(_short_entry("A", stop_loss=90000))
        r.run(_short_entry("B", stop_loss=91000, take_profit=[81000]))
        row = r.run(_short_entry("A", event_sequence=2, action="partial_exit", qty_btc=0.001, expected_qty_btc_after=0.001,
                                 reference_price=None))
        assert row["status"] == "done"
        # A 의 재설정이 마지막 → SL 은 A 의 값, A 가 정하지 않은 TP 는 형제 B 의 의도로 채워진다 (레그는 SL 하나 + TP 하나).
        # 어느 시점에도 레그가 비지 않았고, 두 lot 모두 '빠짐' 이 아니다 → reconcile 이 lot 사이를 오가며 되감지 않는다
        assert tb.get_position_protection(2) == {"stop_loss": 90000.0, "take_profit": 81000.0}
        leg = r.store.get_leg_protection("test", "toobit", 2)
        assert leg["position_id"] == "A" and leg["sl_from"] == "A" and leg["tp_from"] == "B"
        assert r.executor.protection_missing("test", "toobit") == []
        n_before = len([m for m in r.alerts.messages if "re-placing" in m])
        assert r.executor.reconcile("test", "toobit") is True
        assert tb.get_position_protection(2) == {"stop_loss": 90000.0, "take_profit": 81000.0}   # 안정 (되감기 없음)
        assert len([m for m in r.alerts.messages if "re-placing" in m]) == n_before
        assert r.executor.protection_missing("test", "toobit") == []
    finally:
        r.close()


def test_snapshot_and_missing_follow_leg_state_without_exchange_reader(multi_settings):
    """읽기 API 가 없는 거래소(실 Toobit): 스냅샷/protection_missing 은 lot 의 묵은 po.position 이 아니라 레그 상태를 본다."""
    papers = {a.name: PaperExchange(a, price=PAPER_PRICE) for a in multi_settings.accounts}
    tb = NoReaderPaper(multi_settings.account("toobit"), price=PAPER_PRICE, lot_protection=False)
    papers["toobit"] = tb
    r = Rig(multi_settings, exchanges=papers)
    try:
        r.run(_short_entry("B", stop_loss=91000, take_profit=[81000]))
        snap = load_reports(r.store, "test", "toobit")[-1]
        assert snap["positions"][0]["stop_loss"] == 91000.0 and snap["positions"][0]["take_profit"] == [81000.0]
        lot = r.store.get_lot("test", "toobit", "B")
        assert lot["protection_orders"]["position"]["stop_loss"] == 91000.0

        # 레그 보호가 해제됐다고 기록된 상황 (lot 의 po.position 은 그대로 묵은 값)
        tb.set_position_protection(2, None, None)
        r.store.set_leg_protection("test", "toobit", 2, None)
        positions = r.executor.build_snapshot_positions("test", "toobit")
        assert positions[0]["stop_loss"] is None and positions[0]["take_profit"] == []      # 묵은 91000 을 보고하지 않는다
        assert r.executor.protection_missing("test", "toobit") == ["B"]

        # reconcile 자가 복구 → 레그 상태/스냅샷이 다시 B 의 값
        assert r.executor.reconcile("test", "toobit") is True
        assert r.executor.protection_missing("test", "toobit") == []
        assert tb.peek_protection(2) == {"stop_loss": 91000.0, "take_profit": 81000.0}
        positions = r.executor.build_snapshot_positions("test", "toobit")
        assert positions[0]["stop_loss"] == 91000.0 and positions[0]["take_profit"] == [81000.0]
    finally:
        r.close()


def test_leg_state_is_seeded_from_open_lots_for_pre_upgrade_db(multi_settings):
    papers = {a.name: PaperExchange(a, price=PAPER_PRICE) for a in multi_settings.accounts}
    papers["toobit"] = NoReaderPaper(multi_settings.account("toobit"), price=PAPER_PRICE, lot_protection=False)
    r = Rig(multi_settings, exchanges=papers)
    try:
        r.run(_short_entry("B", stop_loss=91000))
        # 이 코드 이전 DB 를 흉내: 레그 상태 키 삭제 → open lot 의 po.position 으로 한 번 시드된다
        with r.store._tx():
            r.store._conn.execute("DELETE FROM meta WHERE key LIKE 'leg_protection:%'")
        assert r.store.get_leg_protection("test", "toobit", 2) is None
        assert r.executor.protection_missing("test", "toobit") == []
        leg = r.store.get_leg_protection("test", "toobit", 2)
        assert leg is not None and leg["stop_loss"] == 91000.0 and leg["position_id"] == "B"
    finally:
        r.close()


def test_failed_leg_clear_on_close_is_retried_by_reconcile(multi_settings):
    """lot 종료 시 포지션 단위 보호 해제가 실패하면 닫힌 lot 이 pending-cancel 로 남고 reconcile 이 해제를 재시도한다."""
    papers = {a.name: PaperExchange(a, price=PAPER_PRICE) for a in multi_settings.accounts}
    tb = FlakyPositionPaper(multi_settings.account("toobit"), price=PAPER_PRICE, lot_protection=False)
    papers["toobit"] = tb
    r = Rig(multi_settings, exchanges=papers)
    try:
        r.run(_short_entry("A", stop_loss=90000))
        assert tb.get_position_protection(2) == {"stop_loss": 90000.0, "take_profit": None}
        # 종료 시 해제 + 직후 스냅샷의 reconcile 재시도까지 두 번 실패시킨다
        tb.errors = [ExchangeError("EXCHANGE_TIMEOUT", "t"), ExchangeError("EXCHANGE_TIMEOUT", "t")]
        row = r.run(_full_exit("A", 2))
        assert row["status"] == "done" and r.alerts.contains("PROTECTION_FAILED")   # 체결 결과와 보호 실패는 분리 (v0.1 규칙)
        lot = r.store.get_lot("test", "toobit", "A")
        assert lot["status"] == "closed" and lot["protection_orders"]["position"] is not None
        assert tb.get_position_protection(2) == {"stop_loss": 90000.0, "take_profit": None}   # 거래소에 묵은 트리거가 남았다
        assert [l["position_id"] for l in r.store.lots_with_pending_cancel("test", "toobit")] == ["A"]
        assert r.alerts.contains("still has live protection orders")

        assert r.executor.reconcile("test", "toobit") is True
        assert tb.get_position_protection(2) is None
        lot = r.store.get_lot("test", "toobit", "A")
        assert lot["protection_orders"]["position"] is None
        assert r.store.lots_with_pending_cancel("test", "toobit") == []
        leg = r.store.get_leg_protection("test", "toobit", 2)
        assert leg["stop_loss"] is None and leg["take_profit"] is None
        # 다음 lot 은 묵은 90000 이 아니라 자기 값으로 보호된다
        r.run(_short_entry("C", stop_loss=92000))
        assert tb.get_position_protection(2) == {"stop_loss": 92000.0, "take_profit": None}
    finally:
        r.close()


def test_failed_sibling_reapply_marks_sibling_failed_and_heals(multi_settings):
    papers = {a.name: PaperExchange(a, price=PAPER_PRICE) for a in multi_settings.accounts}
    tb = FlakyPositionPaper(multi_settings.account("toobit"), price=PAPER_PRICE, lot_protection=False)
    papers["toobit"] = tb
    r = Rig(multi_settings, exchanges=papers)
    try:
        r.run(_short_entry("A", stop_loss=90000))
        r.run(_short_entry("B", stop_loss=91000))
        tb.errors = [ExchangeError("EXCHANGE_TIMEOUT", "t"), ExchangeError("EXCHANGE_TIMEOUT", "t"),
                     ExchangeError("EXCHANGE_TIMEOUT", "t")]
        row = r.run(_full_exit("A", 2))
        assert row["status"] == "done" and r.alerts.contains("re-applying sibling ['B'] leg protection failed")
        assert "B" in r.executor.protection_missing("test", "toobit")          # 형제가 failed 로 표시됨
        tb.errors = []
        assert r.executor.reconcile("test", "toobit") is True
        assert r.executor.protection_missing("test", "toobit") == []
        assert tb.get_position_protection(2) == {"stop_loss": 91000.0, "take_profit": None}
        assert r.store.get_lot("test", "toobit", "A")["protection_orders"]["position"] is None
        assert r.store.lots_with_pending_cancel("test", "toobit") == []
    finally:
        r.close()


# --------------------------------------------------------------------------- #
# 만료는 팬아웃 전에 한 번
# --------------------------------------------------------------------------- #
class SlowPaper(PaperExchange):
    delay_s = 0.0

    def place_market(self, *a, **kw):
        if self.delay_s:
            time.sleep(self.delay_s)
        return super().place_market(*a, **kw)


def test_expiry_is_decided_once_for_the_whole_fanout(multi_settings):
    papers = {a.name: SlowPaper(a, price=PAPER_PRICE) for a in multi_settings.accounts}
    papers["bybit"].delay_s = 0.4          # 첫 계정의 체결이 느리다
    r = Rig(multi_settings, exchanges=papers)
    try:
        d = _short_entry("exp")
        d["expires_at_ms"] = d["ts"] + 200   # 첫 계정 처리 중에 만료 시각이 지난다
        row = r.run(d)
        runs = {x["account"]: x for x in r.store.get_runs("test", d["event_id"])}
        assert row["status"] == "done"
        assert {x["status"] for x in runs.values()} == {"done"}, runs
        assert all(r.store.get_lot("test", n, "exp")["status"] == "open" for n in ("bybit", "okx", "toobit"))

        # 이미 만료된 신호는 모든 계정이 일관되게 EXPIRED
        d2 = _short_entry("exp2")
        d2["ts"] = now_ms() - 5000
        d2["expires_at_ms"] = d2["ts"] + 1000
        row2 = r.run(d2)
        assert row2["status"] == "rejected" and row2["reason_code"] == "EXPIRED"
        assert {x["reason_code"] for x in r.store.get_runs("test", d2["event_id"])} == {"EXPIRED"}
    finally:
        r.close()


# --------------------------------------------------------------------------- #
# Paper 거래소의 거래소별 최소 수량
# --------------------------------------------------------------------------- #
def test_paper_exchange_uses_exchange_specific_instrument(multi_settings):
    okx = build_exchange(multi_settings.account("okx"), "paper", price=PAPER_PRICE)
    assert okx.instrument()["min_qty"] == 0.01 and okx.instrument()["qty_step"] == 0.01
    assert build_exchange(multi_settings.account("toobit"), "paper").instrument()["min_qty"] == 0.001
    assert build_exchange(multi_settings.account("bybit"), "paper").instrument()["min_qty"] == 0.001
    assert main_mod._exchanges_for(multi_settings)["test"]["okx"].instrument()["min_qty"] == 0.01

    papers = {a.name: build_exchange(a, "paper", price=PAPER_PRICE) for a in multi_settings.accounts}
    r = Rig(multi_settings, exchanges=papers)
    try:
        row = r.run(_short_entry("minq"))          # 0.002 BTC
        runs = {x["account"]: x for x in r.store.get_runs("test", row["event_id"])}
        assert runs["okx"]["status"] == "rejected" and runs["okx"]["reason_code"] == "QTY_BELOW_MIN"
        assert runs["bybit"]["status"] == "done" and runs["toobit"]["status"] == "done"
        assert "okx=rejected/QTY_BELOW_MIN" in row["note"]
        assert r.store.get_lot("test", "okx", "minq") is None
        row2 = r.run(_short_entry("okq", qty_btc=0.02, expected_qty_btc_after=0.02))
        assert {x["status"] for x in r.store.get_runs("test", row2["event_id"])} == {"done"}
    finally:
        r.close()


# --------------------------------------------------------------------------- #
# 절단된 clOrdId 역매핑 (OKX 재기동 뒤 고아 스탑)
# --------------------------------------------------------------------------- #
class TruncatingPaper(PaperExchange):
    """OKX 처럼 재기동 뒤 조건부 주문의 클라이언트 ID 를 32자로 잘라 돌려주는 거래소."""

    def open_conditional_orders(self, position_idx):
        return [dict(o, order_link_id=o["order_link_id"][:32]) for o in super().open_conditional_orders(position_idx)]


def test_orphan_sweep_resolves_truncated_links_via_ledger_prefix(settings, store, reporter, alerts):
    paper = TruncatingPaper(settings.accounts[0], price=PAPER_PRICE)
    ex = Executor(settings, store, {"test": {"bybit": paper}, "live": {}}, reporter, alerts)
    pid = "orphan-trunc"
    run_signal(ex, store, _short_entry(pid, stop_loss=90000))
    assert len(paper.open_conditional_orders(2)) == 1
    # 어떤 lot 도 참조하지 않는 우리 보호주문(orders 에 sl 로 기록) 을 거래소에 하나 더 둔다 (재기동 전 취소 실패분 등)
    orphan = order_link_id("test", pid, "sl", "9", "0")
    store.insert_order(orphan, "test", pid, "sl", "Buy", 0.002, True, status="Untriggered", account="bybit")
    paper.place_conditional("Buy", 0.002, 2, 95000, 1, orphan, "MarkPrice")
    assert len(paper.open_conditional_orders(2)) == 2
    assert paper.open_conditional_orders(2)[1]["order_link_id"] == orphan[:32]      # 거래소는 절단된 id 를 돌려준다
    assert ex.reconcile("test", "bybit") is True
    links = [o["order_link_id"] for o in paper.open_conditional_orders(2)]
    assert len(links) == 1 and links[0] != orphan[:32]                               # 고아만 취소, lot 의 SL 은 유지
    assert store.get_order(orphan, "bybit")["status"] == "Cancelled"
    assert alerts.contains("cancelled orphan protection order")
    assert not alerts.contains("not ours")


def test_resolve_link_prefix_requires_unique_match(settings, store, reporter, alerts):
    ex = Executor(settings, store, {"test": {}, "live": {}}, reporter, alerts)
    a = order_link_id("test", "e1")
    b = order_link_id("test", "e9")
    store.insert_order(a, "test", "p", "sl", "Sell", 0.002, True, status="Untriggered", account="bybit")
    store.insert_order(b, "test", "p", "tp", "Sell", 0.002, True, status="Untriggered", account="bybit")
    assert ex._resolve_link_prefix("bybit", a[:32]) == a
    assert ex._resolve_link_prefix("bybit", "lk") is None            # 모호(여러 개)는 None
    assert ex._resolve_link_prefix("okx", a[:32]) is None            # 계정 범위
    assert store.orders_by_link_prefix("bybit", "lk-bad") == []      # 영숫자 외 접두사는 검색하지 않는다


def test_okx_exchange_gets_link_resolver_from_executor(settings, store, reporter, alerts):
    class WithResolver(PaperExchange):
        link_resolver = None

    paper = WithResolver(settings.accounts[0], price=PAPER_PRICE)
    Executor(settings, store, {"test": {"bybit": paper}, "live": {}}, reporter, alerts)
    assert callable(paper.link_resolver)
    a = order_link_id("test", "e2")
    store.insert_order(a, "test", "p", "sl", "Sell", 0.002, True, status="Untriggered", account="bybit")
    assert paper.link_resolver(a[:32]) == a


# --------------------------------------------------------------------------- #
# 1단계 DB 마이그레이션 대상 계정 / 원장-설정 대조 / fail-fast
# --------------------------------------------------------------------------- #
def test_v1_migration_targets_configured_legacy_account(tmp_path):
    path = str(tmp_path / "lake.db")
    make_v1_db(path)
    s = Store(path, legacy_account="main")
    try:
        assert s.get_meta("legacy_account") == "main"
        lot = s.get_lot("test", "main", V1_PID)
        assert lot is not None and lot["status"] == "open" and s.get_lot("test", "bybit", V1_PID) is None
        assert s.get_order(V1_LINK, "main") is not None and s.get_order(V1_LINK, "bybit") is None
        assert s.get_meta("seq:test:main") == "7" and s.get_meta("seq:test:bybit") is None
        assert s.is_inconsistent("test", "main") is True
        acc = s.ledger_accounts()
        assert set(acc) == {"main"} and acc["main"]["open_lots"] == 1 and acc["main"]["rows"] >= 4
    finally:
        s.close()


def test_legacy_account_name_prefers_first_bybit_account(settings_factory):
    assert settings_factory().legacy_account_name() == "bybit"                       # 1단계 형식
    s = settings_factory(config_overrides={"accounts": [
        {"name": "okx", "exchange": "okx"}, {"name": "main", "exchange": "bybit"}, {"name": "b2", "exchange": "bybit",
                                                                                   "env_prefix": "BYBIT2"}]})
    assert s.legacy_account_name() == "main"
    assert settings_factory(config_overrides={"accounts": [{"name": "okx", "exchange": "okx"}]}).legacy_account_name() == "okx"


def test_serve_refuses_ledger_accounts_missing_from_config(settings_factory):
    s = settings_factory(config_overrides={"accounts": [{"name": "main", "exchange": "bybit"}]})
    store = Store(s.db_path)
    try:
        t = now_ms()
        lot = {"mode": "live", "account": "bybit", "position_id": "p1", "strategy": "overheat", "leg": "short",
               "position_idx": 2, "qty": 0.002, "avg_entry": 86000.0, "status": "open", "opened_at_ms": t, "updated_at_ms": t}
        store.upsert_lot(lot)
        with pytest.raises(ConfigError, match="not configured.*'bybit'.*open_lots=1"):
            main_mod.verify_ledger_accounts(s, store)
        # 닫힌 행만 남은 계정은 경고만
        lot["status"] = "closed"
        lot["qty"] = 0.0
        store.upsert_lot(lot)
        assert main_mod.verify_ledger_accounts(s, store) == ["'bybit' (open_lots=0 pending_reports=0 rows=1)"]
        # 설정에 있는 계정이면 문제 없음
        with store._tx():
            store._conn.execute("DELETE FROM lots WHERE account='bybit'")
        lot["account"] = "main"
        lot["status"] = "open"
        store.upsert_lot(lot)
        s2 = settings_factory(config_overrides={"accounts": [{"name": "main", "exchange": "bybit"},
                                                             {"name": "okx", "exchange": "okx"}]})
        assert main_mod.verify_ledger_accounts(s2, store) == []
    finally:
        store.close()


def test_check_reports_ledger_accounts_without_migrating(settings_factory, capsys):
    import os
    s = settings_factory(config_overrides={"accounts": [{"name": "main", "exchange": "bybit"}]})
    os.makedirs(os.path.dirname(s.db_path), exist_ok=True)
    make_v1_db(s.db_path)
    assert main_mod._check_ledger(s) == 0
    out = capsys.readouterr().out
    assert "v0.1 schema → will be migrated to account 'main'" in out
    # 마이그레이션은 하지 않았다
    import sqlite3
    conn = sqlite3.connect(s.db_path)
    assert "account" not in [r[1] for r in conn.execute("PRAGMA table_info(lots)").fetchall()]
    conn.close()
    # v2 DB 에 설정에 없는 계정의 open lot → 문제로 집계
    Store(s.db_path, legacy_account="bybit").close()
    assert main_mod._check_ledger(s) == 1
    assert "NOT IN config accounts (open lots!)" in capsys.readouterr().out


def test_serve_fails_fast_when_live_exchange_cannot_be_built(settings_factory, monkeypatch):
    from lake_executor import exchange as exm
    s = settings_factory(config_overrides={"live": {"enabled": True}, "accounts": [{"name": "okx", "exchange": "okx"}]},
                         env_overrides={"OKX_API_KEY": "k" * 24, "OKX_API_SECRET": "s" * 40, "OKX_API_PASSPHRASE": "pp"})
    assert s.live_execution_possible("okx") == (True, "")

    def boom(acct, kind=None, price=None):
        if kind == "paper":
            return PaperExchange(acct, price=PAPER_PRICE)
        raise ModuleNotFoundError("No module named 'okx'")

    monkeypatch.setattr(exm, "build_exchange", boom)
    assert main_mod._exchanges_for(s)["live"] == {}                       # 관대한 모드: 그 계정만 빠진다
    with pytest.raises(ConfigError, match="okx \\(okx\\): ModuleNotFoundError.*python-okx"):
        main_mod._exchanges_for(s, strict=True)                           # serve: 기동 거부 (exit 2)


# --------------------------------------------------------------------------- #
# 설정 검증
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("accounts,msg", [
    ([{"name": "toobit", "exchange": "toobit"}, {"name": "toobit2", "exchange": "toobit"}], "share env_prefix TOOBIT"),
    ([{"name": "okx-sub", "exchange": "okx", "env_prefix": "OKX_A"}, {"name": "okx_sub", "exchange": "okx", "env_prefix": "OKX_B"}],
     "same env suffix OKX_SUB"),
    ([{"name": "toobit", "exchange": "toobit", "testnet": True}], "toobit has no testnet"),
    ([{"name": "okx", "exchange": "okx", "leverage": "5x"}], "account okx: leverage='5x' is not a valid int"),
    ([{"name": "okx", "exchange": "okx", "qty_multiplier": "half"}], "qty_multiplier='half' is not a valid float"),
    ([{"name": "okx", "exchange": "okx", "testnet": "yes"}], "testnet='yes' is not a valid bool"),
    ([{"name": "okx", "exchange": "okx", "enabled": "no"}], "enabled='no' is not a valid bool"),
])
def test_account_config_errors(settings_factory, accounts, msg):
    with pytest.raises(ConfigError, match=msg):
        settings_factory(config_overrides={"accounts": accounts})


def test_distinct_env_prefix_allows_two_accounts_on_one_exchange(settings_factory):
    s = settings_factory(config_overrides={"accounts": [{"name": "toobit", "exchange": "toobit"},
                                                        {"name": "toobit2", "exchange": "toobit", "env_prefix": "TOOBIT2"}]},
                         env_overrides={"TOOBIT_API_KEY": "a" * 24, "TOOBIT_API_SECRET": "b" * 40,
                                        "TOOBIT2_API_KEY": "c" * 24, "TOOBIT2_API_SECRET": "d" * 40})
    assert s.account("toobit").api_key != s.account("toobit2").api_key
    # 숫자 1/0 은 bool 로 받는다 (JSON 의 true/false 외 호환)
    s2 = settings_factory(config_overrides={"accounts": [{"name": "okx", "exchange": "okx", "enabled": 0, "report": 1}]})
    assert s2.account("okx").enabled is False and s2.account("okx").report is True


def test_check_prints_config_error_for_bad_field(tmp_path, capsys):
    import json
    root = tmp_path / "bad"
    root.mkdir()
    (root / "config.json").write_text(json.dumps({"accounts": [{"name": "okx", "exchange": "okx", "leverage": "5x"}]}),
                                      encoding="utf-8")
    (root / ".env").write_text("", encoding="utf-8")
    rc = main_mod.main(["--config", str(root / "config.json"), "--env", str(root / ".env"), "check"])
    assert rc == 2
    assert "CONFIG ERROR: account okx: leverage='5x' is not a valid int" in capsys.readouterr().out
