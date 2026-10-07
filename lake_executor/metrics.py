"""성과 지표 — 적재된 트레이드 히스토리(history.py) 와 우리 체결 원장으로 계정별 PnL / ROI 를 계산한다.

정의 (대시보드가 그대로 쓰도록 고정):
  seed                 시드. config accounts[].seed_usdt 가 있으면 그 값, 없으면 그 계정의 **첫 자산 스냅샷** total_equity.
  equity_now           최신 자산 스냅샷 total_equity (미실현 포함).
  net_deposits         seed + 입금 - 출금 (account_cashflow deposit/withdraw). 시드가 첫 스냅샷이면 그 뒤의 입출금만 반영된다.
  pnl_total            equity_now - net_deposits (실현 + 미실현 + 수수료/펀딩 반영된 "진짜" 손익)
  roi_vs_seed          (equity_now - seed) / seed
  roi_vs_net_deposits  (equity_now - net_deposits) / net_deposits   ← 입출금이 있으면 이쪽이 맞다
  realized_closed_pnl  거래소 청산손익(account_closed_pnl.closed_pnl) 합 — Bybit 는 수수료 차감 전/후 여부가 문서마다 달라
                       `fees` 를 따로 노출한다 (실제 잔고 변화는 pnl_total 로 본다).
  fees, funding        체결 수수료 합(account_executions.fee), 펀딩 정산 합(account_cashflow funding)
  max_drawdown         자산 스냅샷 시계열의 (고점 - 저점) / 고점 최대값
  win_rate / profit_factor / avg_win / avg_loss / largest_win / largest_loss   청산손익 레코드 기준
  ledger               우리 체결(fills) 평균단가 방식 실현손익: 전략별·포지션별 귀속 (test/paper 포함). 거래소 집계와 별개.
  daily                UTC 일별: 청산손익·수수료·펀딩·거래수·종가 자산
"""
from __future__ import annotations

import time
from typing import Any

from .util import now_ms

DAY_MS = 24 * 3600 * 1000


def _day(ms: int) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(int(ms) / 1000.0))


def _safe_div(a: float | None, b: float | None) -> float | None:
    try:
        if a is None or b in (None, 0):
            return None
        return float(a) / float(b)
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def ledger_pnl(store: Any, mode: str, account: str, since_ms: int | None = None, until_ms: int | None = None) -> dict:
    """우리 체결(fills) 로 포지션별 실현손익 (평균단가). reduce_only 주문의 체결 = 청산, 그 외 = 진입/추가.
    방향은 lot.leg (long/short). lot 정보가 없으면 side 로 추정(Buy 진입 = long)."""
    rows = store.ledger_fills_joined(mode, account, since_ms, until_ms)
    pos: dict[str, dict] = {}
    realized_by_pos: dict[str, float] = {}
    strategy_of: dict[str, str] = {}
    for r in rows:
        pid = r["position_id"]
        p = pos.setdefault(pid, {"qty": 0.0, "cost": 0.0, "leg": None})
        leg = r.get("leg")
        if leg is None:
            leg = "long" if (r.get("side") == "Buy" and not r.get("reduce_only")) else ("short" if r.get("side") == "Sell" and not r.get("reduce_only") else p["leg"])
        if p["leg"] is None and leg:
            p["leg"] = leg
        strategy_of[pid] = r.get("strategy") or strategy_of.get(pid) or "?"
        qty, price = float(r["qty"]), float(r["price"])
        if r.get("reduce_only"):
            if p["qty"] <= 0:
                continue
            closed = min(qty, p["qty"])
            avg = p["cost"] / p["qty"]
            direction = 1.0 if (p["leg"] or "long") == "long" else -1.0
            realized_by_pos[pid] = realized_by_pos.get(pid, 0.0) + (price - avg) * closed * direction
            p["qty"] -= closed
            p["cost"] -= avg * closed
            if p["qty"] <= 1e-12:
                p["qty"], p["cost"] = 0.0, 0.0
        else:
            p["qty"] += qty
            p["cost"] += qty * price
    by_strategy: dict[str, float] = {}
    for pid, v in realized_by_pos.items():
        s = strategy_of.get(pid, "?")
        by_strategy[s] = by_strategy.get(s, 0.0) + v
    open_positions = [{"position_id": pid, "strategy": strategy_of.get(pid, "?"), "leg": p["leg"], "qty": round(p["qty"], 10),
                       "avg_entry": (p["cost"] / p["qty"]) if p["qty"] > 0 else None}
                      for pid, p in pos.items() if p["qty"] > 1e-12]
    return {"realized": round(sum(realized_by_pos.values()), 10), "by_strategy": {k: round(v, 10) for k, v in by_strategy.items()},
            "by_position": {k: round(v, 10) for k, v in realized_by_pos.items()}, "fills": len(rows),
            "positions_closed": sum(1 for pid, p in pos.items() if p["qty"] <= 1e-12 and pid in realized_by_pos),
            "open_positions": open_positions}


def equity_stats(series: list[dict]) -> dict:
    """스냅샷 시계열 → 첫/최신/고점/최대낙폭."""
    if not series:
        return {"first": None, "latest": None, "peak": None, "max_drawdown": None, "max_drawdown_pct": None, "points": 0}
    first = float(series[0]["total_equity"])
    latest = float(series[-1]["total_equity"])
    peak = first
    mdd = 0.0
    mdd_pct = 0.0
    for s in series:
        v = float(s["total_equity"])
        if v > peak:
            peak = v
        dd = peak - v
        if dd > mdd:
            mdd = dd
            mdd_pct = dd / peak if peak else 0.0
    return {"first": first, "latest": latest, "peak": peak, "max_drawdown": round(mdd, 10),
            "max_drawdown_pct": round(mdd_pct, 10), "points": len(series),
            "first_ts_ms": int(series[0]["ts_ms"]), "latest_ts_ms": int(series[-1]["ts_ms"])}


def closed_pnl_stats(closed: list[dict]) -> dict:
    pnls = [float(c.get("closed_pnl") or 0.0) for c in closed]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    gross_profit = sum(wins)
    gross_loss = sum(losses)
    return {"trades": len(pnls), "wins": len(wins), "losses": len(losses), "flat": len(pnls) - len(wins) - len(losses),
            "win_rate": _safe_div(len(wins), len(pnls)), "realized_closed_pnl": round(sum(pnls), 10),
            "gross_profit": round(gross_profit, 10), "gross_loss": round(gross_loss, 10),
            "profit_factor": _safe_div(gross_profit, abs(gross_loss)) if losses else None,
            "avg_win": _safe_div(gross_profit, len(wins)), "avg_loss": _safe_div(gross_loss, len(losses)),
            "largest_win": max(wins) if wins else None, "largest_loss": min(losses) if losses else None,
            "volume_qty": round(sum(float(c.get("qty") or 0.0) for c in closed), 10)}


def daily_series(closed: list[dict], execs: list[dict], cash: list[dict], equity: list[dict]) -> list[dict]:
    days: dict[str, dict] = {}

    def row(d: str) -> dict:
        return days.setdefault(d, {"date": d, "closed_pnl": 0.0, "trades": 0, "fees": 0.0, "funding": 0.0, "deposits": 0.0,
                                   "withdrawals": 0.0, "executions": 0, "equity_close": None})
    for c in closed:
        r = row(_day(c["created_at_ms"]))
        r["closed_pnl"] += float(c.get("closed_pnl") or 0.0)
        r["trades"] += 1
    for e in execs:
        r = row(_day(e["exec_time_ms"]))
        r["fees"] += float(e.get("fee") or 0.0)
        r["executions"] += 1
    for f in cash:
        r = row(_day(f["ts_ms"]))
        t = f.get("type")
        amt = float(f.get("amount") or 0.0)
        if t == "funding":
            r["funding"] += amt
        elif t == "deposit":
            r["deposits"] += abs(amt)
        elif t == "withdraw":
            r["withdrawals"] += abs(amt)
    for s in equity:
        row(_day(s["ts_ms"]))["equity_close"] = float(s["total_equity"])     # 시계열은 시간순이므로 마지막 값이 남는다
    out = [days[d] for d in sorted(days)]
    for r in out:
        for k in ("closed_pnl", "fees", "funding", "deposits", "withdrawals"):
            r[k] = round(r[k], 10)
    return out


def account_performance(store: Any, settings: Any, mode: str, account: str, since_ms: int | None = None,
                        until_ms: int | None = None) -> dict:
    acct = None
    for a in getattr(settings, "accounts", None) or []:
        if a.name == account:
            acct = a
            break
    closed = store.account_closed_pnl(mode, account, since_ms, until_ms)
    execs = store.account_executions(mode, account, since_ms, until_ms)
    cash = store.account_cashflows(mode, account, since_ms, until_ms)
    series = store.account_equity_series(mode, account, since_ms, until_ms)
    latest = store.account_equity_latest(mode, account)
    first_ever = store.account_equity_first(mode, account)

    seed_cfg = getattr(acct, "seed_usdt", None) if acct is not None else None
    seed = float(seed_cfg) if seed_cfg else (float(first_ever["total_equity"]) if first_ever else None)
    seed_source = "config" if seed_cfg else ("first_equity_snapshot" if first_ever else None)
    equity_now = float(latest["total_equity"]) if latest else None
    deposits = sum(abs(float(f["amount"])) for f in cash if f.get("type") == "deposit")
    withdrawals = sum(abs(float(f["amount"])) for f in cash if f.get("type") == "withdraw")
    funding = sum(float(f["amount"]) for f in cash if f.get("type") == "funding")
    fees = sum(float(e.get("fee") or 0.0) for e in execs)
    net_deposits = (seed or 0.0) + deposits - withdrawals if seed is not None else None
    pnl_total = (equity_now - net_deposits) if (equity_now is not None and net_deposits is not None) else None

    cps = closed_pnl_stats(closed)
    eqs = equity_stats(series)
    ledger = ledger_pnl(store, mode, account, since_ms, until_ms)
    return {
        "mode": mode, "account": account, "exchange": getattr(acct, "exchange", None), "symbol": getattr(acct, "symbol", None),
        "leverage": getattr(acct, "leverage", None), "qty_multiplier": getattr(acct, "qty_multiplier", None),
        "since_ms": since_ms, "until_ms": until_ms, "computed_at_ms": now_ms(),
        "seed": seed, "seed_source": seed_source,
        "equity_now": equity_now, "equity_at_ms": int(latest["ts_ms"]) if latest else None,
        "unrealised_pnl": (float(latest["unrealised_pnl"]) if latest and latest.get("unrealised_pnl") is not None else None),
        "wallet_balance": (float(latest["wallet_balance"]) if latest and latest.get("wallet_balance") is not None else None),
        "deposits": round(deposits, 10), "withdrawals": round(withdrawals, 10), "net_deposits": net_deposits,
        "pnl_total": round(pnl_total, 10) if pnl_total is not None else None,
        "roi_vs_seed": _safe_div((equity_now - seed) if (equity_now is not None and seed) else None, seed),
        "roi_vs_net_deposits": _safe_div(pnl_total, net_deposits),
        "fees": round(fees, 10), "funding": round(funding, 10), "executions": len(execs),
        "closed": cps, "equity": eqs, "ledger": ledger,
        "daily": daily_series(closed, execs, cash, series),
        "sync": {s["kind"]: {"last_ts_ms": s["last_ts_ms"], "updated_at_ms": s["updated_at_ms"]} for s in store.sync_states(mode, account)},
    }


def performance_all(store: Any, settings: Any, mode: str, since_ms: int | None = None, until_ms: int | None = None,
                    accounts: list[str] | None = None) -> list[dict]:
    names = accounts or [a.name for a in (getattr(settings, "accounts", None) or [])]
    return [account_performance(store, settings, mode, n, since_ms, until_ms) for n in names]
