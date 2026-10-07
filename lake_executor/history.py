"""계정(사용자) 트레이드 히스토리 적재 — 거래소에서 체결·청산손익·자산·입출금을 가져와 장부(DB) 에 쌓는다.

왜: 신호 로그(signal_log) 가 "무엇을 받았나" 라면, 여기는 "각 계정에서 실제로 무엇이 체결되고 자산이 어떻게 변했나" 다.
지표(metrics.py) 가 이 테이블로 시드 대비 ROI·실현/미실현 손익·수수료·낙폭·승률을 계산하고, 나중 대시보드는 JSON API 로 읽는다.

구조
  HistorySource  거래소별 어댑터. fetch_executions / fetch_closed_pnl / fetch_cashflows (시간 창 + 커서 페이지) / fetch_equity (현재 자산)
    BybitHistory  Bybit v5 (히스토리 전용 pybit 세션): execution/list, position/closed-pnl, account/wallet-balance,
                  account/transaction-log. 창은 최대 7일(거래소 제한) 로 잘라 돌고, 심볼을 제한하지 않는다(사용자의 전체 히스토리).
    PaperHistory  test 모드 가상거래소(PaperExchange): 체결·청산손익·시드 기준 자산.
  HistorySync    (mode, account) 마다 증분 동기화(sync_state 커서) + 백필(backfill_since). 전용 DB 연결(원장 락과 무관).
                 run_forever 는 history.sync_interval_s 마다 돌고, 자산 스냅샷은 history.equity_interval_s 마다 한 줄.
                 거래소 오류는 알림을 5분에 한 번으로 줄이고 다음 주기에 다시 시도한다 (주문 경로와 완전히 분리).

멱등: 모든 적재는 INSERT OR IGNORE (PK: (mode, account, exec_id|pnl_id|flow_id)) 라 백필을 몇 번 돌려도 중복이 없다.
증분: 다음 동기화는 last_ts - OVERLAP 부터 다시 읽는다 (거래소의 지연 반영 레코드 보호; 중복은 PK 가 거른다).

CLI: python -m lake_executor backfill --mode live --account bybit --since 2026-06-01
     python -m lake_executor performance --mode live [--account bybit] [--since …] [--json]
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable

from .util import now_ms

log = logging.getLogger("lake_executor.history")

DAY_MS = 24 * 3600 * 1000
BYBIT_WINDOW_MS = 7 * DAY_MS - 1000          # Bybit: startTime~endTime 최대 7일
OVERLAP_MS = 5 * 60 * 1000                    # 증분 동기화 시 되돌아가 읽는 폭
KINDS = ("executions", "closed_pnl", "cashflow")
ALERT_EVERY_MS = 5 * 60 * 1000


def _f(v, default=None):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def _i(v, default=0):
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


# --------------------------------------------------------------------------- #
# 소스
# --------------------------------------------------------------------------- #
class BybitHistory:
    """Bybit v5 히스토리. exchange: BybitExchange (http/account 재사용) 또는 (account, http) 를 직접."""
    name = "bybit"
    max_window_ms = BYBIT_WINDOW_MS

    def __init__(self, exchange: Any = None, *, account: Any = None, http: Any = None):
        if exchange is not None:
            self.account = exchange.account
            # 주문 경로(실행기 스레드) 의 세션을 같이 쓰지 않도록 히스토리 전용 pybit 세션을 하나 더 연다 (실패 시 공유)
            self.http = self._own_http(exchange) or exchange.http
        else:
            self.account = account
            self.http = http
        self.category = getattr(self.account, "category", "linear") or "linear"
        self.symbol = getattr(self.account, "symbol", "BTCUSDT")

    @staticmethod
    def _own_http(exchange: Any) -> Any:
        try:
            from pybit.unified_trading import HTTP
        except Exception:  # noqa: BLE001
            return None
        a = exchange.account
        try:
            return HTTP(testnet=bool(getattr(a, "testnet", False)), api_key=a.api_key, api_secret=a.api_secret,
                        timeout=int(getattr(exchange, "http_timeout_s", 10) or 10))
        except Exception:  # noqa: BLE001
            return None

    def _page(self, fn: Callable[..., Any], **kw) -> tuple[list[dict], str | None]:
        r = fn(**kw)
        res = (r or {}).get("result") or {}
        items = res.get("list") or []
        nxt = res.get("nextPageCursor") or None
        return items, nxt

    def fetch_executions(self, since_ms: int, until_ms: int, cursor: str | None) -> tuple[list[dict], str | None]:
        kw: dict[str, Any] = {"category": self.category, "startTime": int(since_ms), "endTime": int(until_ms), "limit": 100}
        if cursor:
            kw["cursor"] = cursor
        items, nxt = self._page(self.http.get_executions, **kw)
        rows = []
        for x in items:
            exec_id = str(x.get("execId") or "")
            if not exec_id:
                continue
            rows.append({
                "exec_id": exec_id, "exchange": "bybit", "symbol": x.get("symbol") or self.symbol,
                "order_id": x.get("orderId"), "order_link_id": x.get("orderLinkId") or None, "side": x.get("side"),
                "qty": _f(x.get("execQty"), 0.0), "price": _f(x.get("execPrice"), 0.0), "fee": _f(x.get("execFee")),
                "fee_currency": x.get("feeCurrency") or "USDT", "exec_type": x.get("execType"),
                "closed_size": _f(x.get("closedSize")), "position_idx": None, "exec_time_ms": _i(x.get("execTime")),
                "raw": {k: x.get(k) for k in ("isMaker", "feeRate", "orderType", "stopOrderType", "seq") if k in x},
            })
        return rows, nxt

    def fetch_closed_pnl(self, since_ms: int, until_ms: int, cursor: str | None) -> tuple[list[dict], str | None]:
        kw: dict[str, Any] = {"category": self.category, "startTime": int(since_ms), "endTime": int(until_ms), "limit": 100}
        if cursor:
            kw["cursor"] = cursor
        items, nxt = self._page(self.http.get_closed_pnl, **kw)
        rows = []
        for x in items:
            oid = str(x.get("orderId") or "")
            created = _i(x.get("createdTime"))
            if not oid and not created:
                continue
            rows.append({
                "pnl_id": f"{oid}:{created}", "exchange": "bybit", "symbol": x.get("symbol") or self.symbol,
                "order_id": oid, "side": x.get("side"), "qty": _f(x.get("qty"), 0.0),
                "avg_entry_price": _f(x.get("avgEntryPrice")), "avg_exit_price": _f(x.get("avgExitPrice")),
                "closed_pnl": _f(x.get("closedPnl"), 0.0), "leverage": _f(x.get("leverage")), "position_idx": None,
                "created_at_ms": created, "updated_at_ms": _i(x.get("updatedTime")) or None,
                "raw": {k: x.get(k) for k in ("orderType", "execType", "fillCount", "cumEntryValue", "cumExitValue") if k in x},
            })
        return rows, nxt

    _FLOW_TYPES = {"TRANSFER_IN": "deposit", "TRANSFER_OUT": "withdraw", "SETTLEMENT": "funding"}

    def fetch_cashflows(self, since_ms: int, until_ms: int, cursor: str | None) -> tuple[list[dict], str | None]:
        fn = getattr(self.http, "get_transaction_log", None)
        if fn is None:
            return [], None
        kw: dict[str, Any] = {"accountType": "UNIFIED", "currency": "USDT", "startTime": int(since_ms),
                              "endTime": int(until_ms), "limit": 50}
        if cursor:
            kw["cursor"] = cursor
        items, nxt = self._page(fn, **kw)
        rows = []
        for x in items:
            typ = self._FLOW_TYPES.get(str(x.get("type") or ""))
            if typ is None:
                continue
            ts = _i(x.get("transactionTime"))
            amount = _f(x.get("cashFlow"), None)
            if amount is None:
                amount = _f(x.get("change"), 0.0)
            rows.append({"flow_id": str(x.get("id") or f"{x.get('type')}:{ts}:{amount}"), "ts_ms": ts, "type": typ,
                         "amount": amount, "currency": x.get("currency") or "USDT",
                         "raw": {k: x.get(k) for k in ("type", "symbol", "fee", "funding", "tradeId") if k in x}})
        return rows, nxt

    def fetch_equity(self) -> dict | None:
        r = self.http.get_wallet_balance(accountType="UNIFIED")
        lst = ((r or {}).get("result") or {}).get("list") or []
        if not lst:
            return None
        a = lst[0]
        return {"total_equity": _f(a.get("totalEquity"), 0.0), "wallet_balance": _f(a.get("totalWalletBalance")),
                "unrealised_pnl": _f(a.get("totalPerpUPL")), "available": _f(a.get("totalAvailableBalance"))}


class PaperHistory:
    """test 모드 가상거래소. 체결은 paper 의 실행 기록, 청산손익은 paper.closed_pnls, 자산은 seed + 실현 + 미실현."""
    name = "paper"
    max_window_ms = 10 * 365 * DAY_MS

    def __init__(self, paper: Any):
        self.paper = paper
        self.account = paper.account

    def fetch_executions(self, since_ms: int, until_ms: int, cursor: str | None) -> tuple[list[dict], str | None]:
        rows = []
        for o in self.paper.all_orders():
            for ex in self.paper.executions(o["order_id"]):
                t = int(ex["exec_time_ms"])
                if since_ms <= t < until_ms:
                    rows.append({"exec_id": ex["exec_id"], "exchange": "paper", "symbol": self.paper.symbol,
                                 "order_id": o["order_id"], "order_link_id": o.get("order_link_id"), "side": o.get("side"),
                                 "qty": float(ex["qty"]), "price": float(ex["price"]), "fee": 0.0, "fee_currency": "USDT",
                                 "exec_type": "Trade", "closed_size": None, "position_idx": o.get("position_idx"),
                                 "exec_time_ms": t, "raw": None})
        return rows, None

    def fetch_closed_pnl(self, since_ms: int, until_ms: int, cursor: str | None) -> tuple[list[dict], str | None]:
        rows = []
        for p in list(getattr(self.paper, "closed_pnls", []) or []):
            t = int(p["created_at_ms"])
            if since_ms <= t < until_ms:
                rows.append({"pnl_id": p["pnl_id"], "exchange": "paper", "symbol": self.paper.symbol, "order_id": p.get("order_id"),
                             "side": p["side"], "qty": float(p["qty"]), "avg_entry_price": p["avg_entry_price"],
                             "avg_exit_price": p["avg_exit_price"], "closed_pnl": float(p["closed_pnl"]),
                             "leverage": p.get("leverage"), "position_idx": p.get("position_idx"), "created_at_ms": t,
                             "updated_at_ms": t, "raw": None})
        return rows, None

    def fetch_cashflows(self, since_ms: int, until_ms: int, cursor: str | None) -> tuple[list[dict], str | None]:
        return [], None

    def fetch_equity(self) -> dict | None:
        snap = getattr(self.paper, "equity_snapshot", None)
        return snap() if callable(snap) else None


def build_sources(settings: Any, exchanges: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """{mode: {account: HistorySource}}. live: BybitExchange → BybitHistory (OKX/Toobit 은 아직 없음 → 건너뜀, 로그).
    test: PaperExchange → PaperHistory."""
    out: dict[str, dict[str, Any]] = {"live": {}, "test": {}}
    for mode, per in (exchanges or {}).items():
        for name, ex in (per or {}).items():
            kind = str(getattr(ex, "name", "") or type(ex).__name__).lower()
            if mode == "test" and kind == "paper":
                out["test"][name] = PaperHistory(ex)
            elif mode == "live" and kind == "bybit":
                out["live"][name] = BybitHistory(ex)
            elif mode == "live":
                log.info("history: no adapter for %s (%s) yet; trade history not collected", name, kind)
    return out


# --------------------------------------------------------------------------- #
# 동기화 엔진
# --------------------------------------------------------------------------- #
class HistorySync:
    def __init__(self, settings: Any, store_factory: Callable[[], Any], sources: dict[str, dict[str, Any]],
                 alerts: Any = None, clock: Callable[[], int] = now_ms):
        self.settings = settings
        self._store_factory = store_factory
        self._store: Any = None
        self.sources = sources
        self.alerts = alerts
        self.clock = clock
        self.interval_s = float(getattr(settings, "history_sync_interval_s", 60) or 60)
        self.equity_interval_ms = int(float(getattr(settings, "history_equity_interval_s", 300) or 300) * 1000)
        self.backfill_days = int(getattr(settings, "history_backfill_days", 30) or 30)
        self._last_equity: dict[tuple[str, str], int] = {}
        self._last_alert: dict[str, int] = {}
        self._lock = threading.Lock()
        self.stats: dict[str, dict] = {}        # f"{mode}/{account}" -> {"last_sync_ms", "inserted": {...}, "errors", "last_error"}
        self._backfill_threads: list[threading.Thread] = []
        self.startup_delay_s = 5.0                # run_forever: 기동 직후 거래소 설정/복구와 겹치지 않게

    # ---- 저장소 ----
    def store(self) -> Any:
        if self._store is None:
            self._store = self._store_factory()
        return self._store

    def close(self) -> None:
        if self._store is not None:
            try:
                self._store.close()
            except Exception:  # noqa: BLE001
                pass
            self._store = None

    def _alert(self, key: str, text: str) -> None:
        now = self.clock()
        if now - self._last_alert.get(key, 0) < ALERT_EVERY_MS:
            return
        self._last_alert[key] = now
        if self.alerts is not None:
            try:
                self.alerts.send(text)
            except Exception:  # noqa: BLE001
                pass

    # ---- 한 종류 적재 (창 + 커서) ----
    def _pull_kind(self, mode: str, account: str, src: Any, kind: str, since_ms: int, until_ms: int) -> int:
        st = self.store()
        fetch = {"executions": src.fetch_executions, "closed_pnl": src.fetch_closed_pnl, "cashflow": src.fetch_cashflows}[kind]
        upsert = {"executions": st.upsert_executions, "closed_pnl": st.upsert_closed_pnl, "cashflow": st.upsert_cashflows}[kind]
        tcol = {"executions": "exec_time_ms", "closed_pnl": "created_at_ms", "cashflow": "ts_ms"}[kind]
        window = int(getattr(src, "max_window_ms", BYBIT_WINDOW_MS) or BYBIT_WINDOW_MS)
        inserted = 0
        max_ts = since_ms
        start = since_ms
        while start < until_ms:
            end = min(start + window, until_ms)
            cursor = None
            pages = 0
            while True:
                rows, cursor = fetch(start, end, cursor)
                for r in rows:
                    r["mode"], r["account"] = mode, account
                if rows:
                    inserted += upsert(rows)
                    max_ts = max(max_ts, max(int(r.get(tcol) or 0) for r in rows))
                pages += 1
                if not cursor or not rows or pages >= 200:
                    break
            start = end            # 창은 [start, end) 로 이어 붙인다 (경계 중복은 PK 가 거른다)
        st.set_sync_state(mode, account, kind, last_ts_ms=max(max_ts, since_ms), note=f"until={until_ms}")
        return inserted

    def sync_account(self, mode: str, account: str, src: Any, backfill_since_ms: int | None = None,
                     until_ms: int | None = None, kinds: tuple[str, ...] = KINDS, equity: bool = True) -> dict:
        """증분(기본) 또는 백필(backfill_since_ms). 반환: {"inserted": {kind: n}, "equity": bool}."""
        st = self.store()
        now = self.clock()
        until = int(until_ms or now)
        out: dict[str, Any] = {"inserted": {}, "equity": False}
        for kind in kinds:
            if backfill_since_ms is not None:
                since = int(backfill_since_ms)
            else:
                state = st.get_sync_state(mode, account, kind)
                if state and int(state.get("last_ts_ms") or 0) > 0:
                    since = int(state["last_ts_ms"]) - OVERLAP_MS
                else:
                    since = now - self.backfill_days * DAY_MS
            since = max(0, since)
            out["inserted"][kind] = self._pull_kind(mode, account, src, kind, since, until) if since < until else 0
        if equity:
            out["equity"] = self._equity_snapshot(mode, account, src, force=backfill_since_ms is not None)
        key = f"{mode}/{account}"
        with self._lock:
            s = self.stats.setdefault(key, {"inserted": {k: 0 for k in KINDS}, "errors": 0, "last_error": "", "last_sync_ms": 0})
            for k, n in out["inserted"].items():
                s["inserted"][k] = s["inserted"].get(k, 0) + int(n)
            s["last_sync_ms"] = self.clock()
        return out

    def _equity_snapshot(self, mode: str, account: str, src: Any, force: bool = False) -> bool:
        now = self.clock()
        last = self._last_equity.get((mode, account), 0)
        if not force and now - last < self.equity_interval_ms:
            return False
        eq = src.fetch_equity()
        if not eq or eq.get("total_equity") is None:
            return False
        self.store().insert_equity(mode, account, now, float(eq["total_equity"]), eq.get("wallet_balance"),
                                   eq.get("unrealised_pnl"), eq.get("available"), source="sync")
        self._last_equity[(mode, account)] = now
        return True

    def sync_all(self) -> None:
        for mode, per in self.sources.items():
            for name, src in per.items():
                try:
                    self.sync_account(mode, name, src)
                except Exception as e:  # noqa: BLE001 - 한 계정 실패가 다른 계정/루프를 막지 않는다
                    key = f"{mode}/{name}"
                    with self._lock:
                        s = self.stats.setdefault(key, {"inserted": {k: 0 for k in KINDS}, "errors": 0, "last_error": "", "last_sync_ms": 0})
                        s["errors"] += 1
                        s["last_error"] = type(e).__name__
                    log.warning("history sync %s failed: %s", key, type(e).__name__)
                    self._alert(key, f"[history] {key} sync failed: {type(e).__name__}")

    def run_forever(self, stop_event: threading.Event) -> None:
        log.info("history sync started (interval=%ss, equity every %ss, sources=%s)", self.interval_s,
                 self.equity_interval_ms // 1000, {m: list(p) for m, p in self.sources.items()})
        stop_event.wait(self.startup_delay_s)
        while not stop_event.is_set():
            try:
                self.sync_all()
            except Exception as e:  # noqa: BLE001
                log.exception("history loop error: %s", type(e).__name__)
            stop_event.wait(self.interval_s)
        self.close()
        log.info("history sync stopped")

    # ---- 백필 (대시보드 버튼/CLI) ----
    def backfill(self, mode: str, account: str, since_ms: int, until_ms: int | None = None) -> dict:
        src = (self.sources.get(mode) or {}).get(account)
        if src is None:
            raise KeyError(f"no history source for {mode}/{account}")
        return self.sync_account(mode, account, src, backfill_since_ms=int(since_ms), until_ms=until_ms)

    def backfill_async(self, mode: str, account: str, since_ms: int) -> threading.Thread:
        def _run():
            try:
                res = self.backfill(mode, account, since_ms)
                log.info("backfill %s/%s done: %s", mode, account, res)
            except Exception as e:  # noqa: BLE001
                log.warning("backfill %s/%s failed: %s", mode, account, type(e).__name__)
                self._alert(f"backfill/{mode}/{account}", f"[history] backfill {mode}/{account} failed: {type(e).__name__}")
        t = threading.Thread(target=_run, name=f"backfill-{mode}-{account}", daemon=True)
        t.start()
        self._backfill_threads.append(t)
        return t

    def snapshot(self) -> dict:
        with self._lock:
            return {k: dict(v, inserted=dict(v["inserted"])) for k, v in self.stats.items()}
