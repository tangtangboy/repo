"""CLI (ARCHITECTURE.md §7, ARCHITECTURE_MULTI_EXCHANGE.md §6) — `python -m lake_executor <serve|check|simulate|sign>`.

  serve     store 열기 → enabled 계정마다 거래소 준비(live 가능하면 그 계정의 실거래소, test_simulate_fills 면 Paper)
            → executor 복구 → 스레드(executor / reporter / snapshot loop: (mode, account) 조합마다) → uvicorn.
            SIGTERM/SIGINT 시 정상 종료.
  check     설정·계정별 키 유무·계정별 읽기 전용 연결(instrument/변환 계수/시세/positions, Bybit 은 잔고도)·
            회신 URL 설정 여부 출력. 주문 없음.
  simulate  TEST 시크릿으로 서명한 합성 신호를 entry→add→partial_exit→protection_update→full_exit 순으로 전송.
  sign      파일 본문에 현재 ts 를 넣고 서명 헤더 + curl 예시 출력 (상대 테스트용).

시크릿은 출력하지 않는다(존재 여부만). 실거래소 호출은 check 에서도 읽기 전용뿐이다.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
from types import SimpleNamespace
from typing import Any

from . import auth, config
from .util import canonical_json, now_ms

log = logging.getLogger("lake_executor.main")

MODES = ("test", "live")
DEFAULT_BASE_URL = "http://127.0.0.1:8787"
DEFAULT_CONFIG_PATH = "config.json"
DEFAULT_ENV_PATH = ".env"
SIM_EXPIRES_MS = 15_000          # 계약 예시: 신호 만료 15초
SIM_DEFAULT_QTY = 0.002
THREAD_JOIN_TIMEOUT_S = 15.0
SNAPSHOT_TICK_S = 0.5


# --------------------------------------------------------------------------- #
# 공통
# --------------------------------------------------------------------------- #
def _load_settings(args: argparse.Namespace):
    return config.load(args.config, args.env)


def _present(v: Any) -> str:
    return "set" if v else "missing"


def _exchanges_for(settings, strict: bool = False) -> dict[str, dict[str, Any]]:
    """serve: {mode: {account: ExchangeBase}}. enabled 계정마다 live 가능하면 그 계정의 실거래소(account.exchange),
    test_simulate_fills 면 Paper. 실거래소 생성 실패(모듈 없음·잘못된 키 형식 등 결정적 오류)는
    strict=True(serve) 면 ConfigError 로 올려 **기동을 멈춘다** — 실키가 있는 계정이 조용히 빠진 채 서비스가 떠서
    운영자는 켜져 있다고 믿는데 그 계정의 live 신호가 전부 거부되는 일을 막는다. strict=False 면 그 계정만 빠진다."""
    from .exchange import build_exchange
    exchanges: dict[str, dict[str, Any]] = {"live": {}, "test": {}}
    failed: list[str] = []
    for acct in settings.enabled_accounts():
        ok, reason = settings.live_execution_possible(acct)
        if ok:
            try:
                exchanges["live"][acct.name] = build_exchange(acct)
                log.info("live exchange %s: %s %s (testnet=%s)", acct.name, acct.exchange, acct.symbol, acct.testnet)
            except Exception as e:  # noqa: BLE001 - 원문(키 포함 가능) 은 남기지 않는다
                log.error("live exchange %s (%s) could not be created: %s", acct.name, acct.exchange, type(e).__name__)
                failed.append(f"{acct.name} ({acct.exchange}): {type(e).__name__}")
        else:
            log.warning("live execution disabled for account %s (%s): live signals will be rejected/%s",
                        acct.name, reason, reason or "LIVE_DISABLED")
        if settings.test_simulate_fills:
            exchanges["test"][acct.name] = build_exchange(acct, "paper")
            log.info("test exchange %s: paper (%s, simulate_fills=true)", acct.name, acct.display_name)
    if not settings.test_simulate_fills:
        log.info("test mode: record only (simulate_fills=false)")
    if not settings.live_enabled:
        log.warning("live.enabled=false: all live signals will be rejected/LIVE_DISABLED")
    if failed and strict:
        raise config.ConfigError("live exchange could not be created for enabled account(s) with real keys: "
                                 + "; ".join(failed) + " — install the exchange SDK (requirements.txt: python-okx/pybit) "
                                 "or disable the account")
    return exchanges


def verify_ledger_accounts(settings, store) -> list[str]:
    """원장(lots/orders/fills/reports)에 있는 계정 이름이 설정 accounts 에 전부 있는지 검사한다.
    설정에 없는 계정에 **open lot 또는 pending 회신**이 있으면 ConfigError (기동 거부) — 1단계 계정을 다른 이름으로
    올리는 등 이름이 어긋나면 그 lot 의 보호주문·회신 sequence 가 영영 고아가 되기 때문이다. 닫힌 행만 있으면 경고 목록만 돌려준다."""
    known = {a.name for a in getattr(settings, "accounts", None) or []}
    warnings: list[str] = []
    blocking: list[str] = []
    for name, info in sorted(store.ledger_accounts().items()):
        if name in known:
            continue
        desc = f"{name!r} (open_lots={info['open_lots']} pending_reports={info['pending_reports']} rows={info['rows']})"
        if info["open_lots"] or info["pending_reports"]:
            blocking.append(desc)
        else:
            warnings.append(desc)
    if blocking:
        legacy = store.get_meta("legacy_account")
        hint = (f" (the v0.1 ledger was migrated to account {legacy!r}; keep that account name in config.json or "
                f"rename the rows by hand)" if legacy else "")
        raise config.ConfigError("ledger has accounts that are not configured: " + "; ".join(blocking) + hint)
    for w in warnings:
        log.warning("ledger has rows for unconfigured account %s (closed/sent only; ignored)", w)
    return warnings


# --------------------------------------------------------------------------- #
# serve
# --------------------------------------------------------------------------- #
def _snapshot_loop(settings, executor, exchanges: dict[str, dict[str, Any]], stop_event: threading.Event) -> None:
    """(mode, account) 조합마다 snapshot_interval_ms 마다 reconcile → reporter.snapshot (executor.snapshot_now).
    다음 예정 시각은 호출이 **끝난 뒤** 잡는다: 실행기 락 대기/거래소 지연으로 호출이 길어져도 다음 스냅샷이
    바로 이어져 lake 의 신선도 창(90초) 안에서 간격이 두 배로 벌어지지 않는다."""
    interval_ms = max(1000, int(getattr(settings, "snapshot_interval_ms", 30000)))
    streams = [(m, a) for m in MODES for a in sorted((exchanges.get(m) or {}).keys())]
    next_at = {key: now_ms() + 2000 for key in streams}  # 기동 직후 2초 뒤 첫 스냅샷
    log.info("snapshot loop started (interval=%sms, streams=%s)", interval_ms, [f"{m}/{a}" for m, a in streams])
    while not stop_event.is_set():
        for key in streams:
            mode, account = key
            if now_ms() < next_at[key]:
                continue
            started = now_ms()
            try:
                res = executor.snapshot_now(mode, account)
                if res is None:
                    log.warning("snapshot %s/%s skipped (reconcile not consistent)", mode, account)
            except Exception as e:  # noqa: BLE001 - 루프는 죽지 않는다
                log.exception("snapshot %s/%s failed: %s", mode, account, type(e).__name__)
            finished = now_ms()
            if finished - started > interval_ms // 2:
                log.warning("snapshot %s/%s took %dms (lock/exchange contention)", mode, account, finished - started)
            # 호출에 걸린 시간만큼 다음 슬롯을 당긴다 (최소 1초 뒤)
            next_at[key] = max(finished + 1000, started + interval_ms)
        stop_event.wait(SNAPSHOT_TICK_S)
    log.info("snapshot loop stopped")


def cmd_serve(args: argparse.Namespace) -> int:
    import uvicorn

    from .executor import Executor
    from .ops import Alerts, setup_logging
    from .receiver import create_app
    from .reporter import Reporter
    from .store import Store

    settings = _load_settings(args)
    setup_logging(settings)
    log.info("lake-executor serve: routing=%s accounts=%s listen=%s:%s path=%s",
             settings.routing, [f"{a.name}({a.exchange}{'' if a.enabled else ',disabled'})" for a in settings.accounts],
             settings.listen_host, settings.listen_port, settings.signal_path)

    # 1단계 DB 라면 기존 행은 settings.legacy_account_name()(첫 bybit 계정) 으로 귀속된다.
    # DATABASE_URL 이 있으면 Postgres(Supabase) 원장, 없으면 state/lake.db
    try:
        store = Store(settings.ledger_target, legacy_account=settings.legacy_account_name(), schema=settings.db_schema)
    except Exception as e:  # noqa: BLE001 - 원장 불통/설정 오류는 기동 거부 (systemd 가 5초 뒤 재시도)
        log.error("ledger open failed (%s): %s", "DATABASE_URL" if settings.db_url else settings.ledger_target,
                  type(e).__name__)
        raise config.ConfigError(f"ledger open failed: {type(e).__name__}") from e
    log.info("ledger: %s", store.describe())
    try:
        verify_ledger_accounts(settings, store)   # 설정에 없는 계정의 open lot/pending 회신 → ConfigError (exit 2)
    except config.ConfigError:
        store.close()
        raise
    alerts = Alerts(settings)
    reporter = Reporter(settings, store, alerts)
    try:
        exchanges = _exchanges_for(settings, strict=True)
    except config.ConfigError as e:
        alerts.send(f"[serve] {e}")
        store.close()
        raise
    executor = Executor(settings, store, exchanges, reporter, alerts)

    # 시작 시 계정 설정(live 에서만) — 실패해도 서비스는 올린다 (주문은 거래소가 거부하고 회신으로 드러남)
    try:
        executor.ensure_account_setup()
    except Exception as e:  # noqa: BLE001
        log.error("ensure_account_setup failed: %s", type(e).__name__)
        alerts.send(f"[serve] ensure_account_setup failed: {type(e).__name__}")
    try:
        executor.recover_processing()
    except Exception as e:  # noqa: BLE001
        log.exception("recover_processing failed: %s", type(e).__name__)
        alerts.send(f"[serve] recover_processing failed: {type(e).__name__}")

    # paths/started_ms 는 대시보드(web.py) 가 쓴다: .env/config.json 편집 대상 경로와 재시작 가드의 기준 시각
    services = SimpleNamespace(executor=executor, reporter=reporter, alerts=alerts, exchanges=exchanges,
                               paths=SimpleNamespace(env=args.env, config=args.config), started_ms=now_ms())
    app = create_app(settings, store, services)

    stop_event = threading.Event()
    server = uvicorn.Server(uvicorn.Config(app, host=settings.listen_host, port=int(settings.listen_port),
                                           log_level="info", access_log=False))

    def _on_signal(signum, _frame):
        # uvicorn 은 자기 핸들러로 should_exit 를 세우고, 종료 후 원래 핸들러(이 함수)로 신호를 다시 올린다.
        log.warning("signal %s received: shutting down", signum)
        stop_event.set()
        server.should_exit = True

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _on_signal)
        except (ValueError, OSError):  # 메인 스레드가 아닐 때
            pass

    threads = [
        threading.Thread(target=executor.run_forever, args=(stop_event,), name="executor", daemon=True),
        threading.Thread(target=reporter.run_forever, args=(stop_event,), name="reporter", daemon=True),
        threading.Thread(target=_snapshot_loop, args=(settings, executor, exchanges, stop_event), name="snapshot",
                         daemon=True),
    ]
    for t in threads:
        t.start()

    rc = 0
    try:
        server.run()  # SIGTERM/SIGINT → should_exit → 반환
    except Exception as e:  # noqa: BLE001
        log.exception("uvicorn stopped with error: %s", type(e).__name__)
        rc = 1
    finally:
        stop_event.set()
        for t in threads:
            t.join(THREAD_JOIN_TIMEOUT_S)
            if t.is_alive():
                log.warning("thread %s did not stop in time", t.name)
        try:
            reporter.close()
        except Exception:  # noqa: BLE001
            pass
        try:
            alerts.close()
        except Exception:  # noqa: BLE001
            pass
        try:
            store.close()
        except Exception:  # noqa: BLE001
            pass
        log.info("lake-executor stopped")
    return rc


# --------------------------------------------------------------------------- #
# check
# --------------------------------------------------------------------------- #
def _usdt_equity(wallet_resp: Any) -> tuple[float | None, float | None]:
    """get_wallet_balance 응답에서 (totalEquity, USDT walletBalance) 추출. 없으면 None."""
    try:
        lst = (wallet_resp or {}).get("result", {}).get("list") or []
        if not lst:
            return None, None
        acct = lst[0]
        total = acct.get("totalEquity")
        usdt = None
        for c in acct.get("coin") or []:
            if c.get("coin") == "USDT":
                usdt = c.get("walletBalance")
                break
        return (float(total) if total not in (None, "") else None,
                float(usdt) if usdt not in (None, "") else None)
    except Exception:  # noqa: BLE001
        return None, None


def _check_account(acct, ex_factory) -> int:
    """계정 하나의 읽기 전용 연결 검증 + 변환 계수 출력. 반환: 문제 수."""
    from .exchange import ExchangeError
    tag = f"[{acct.name}]"
    problems = 0
    try:
        ex = ex_factory(acct)
    except ExchangeError as e:
        print(f"{tag} connect        : FAILED ({e.code})")
        return 1
    except Exception as e:  # noqa: BLE001 - 원문은 출력하지 않는다
        print(f"{tag} connect        : FAILED ({type(e).__name__})")
        return 1
    try:
        instr = ex.instrument()
        print(f"{tag} instrument     : qty_step={instr.get('qty_step')} min_qty={instr.get('min_qty')} "
              f"max_qty={instr.get('max_qty')} tick={instr.get('tick')} (BTC units)")
        extras = {k: v for k, v in instr.items() if k not in ("qty_step", "min_qty", "max_qty", "tick")}
        if extras:
            print(f"{tag} conversion     : " + " ".join(f"{k}={v}" for k, v in sorted(extras.items())))
    except ExchangeError as e:
        print(f"{tag} instrument     : FAILED ({e.code})")
        return 1
    except Exception as e:  # noqa: BLE001
        print(f"{tag} instrument     : FAILED ({type(e).__name__})")
        return 1
    try:
        print(f"{tag} last/mark      : {ex.last_price()} / {ex.mark_price()}")
    except ExchangeError as e:
        print(f"{tag} ticker         : FAILED ({e.code})")
        problems += 1
    except Exception as e:  # noqa: BLE001
        print(f"{tag} ticker         : FAILED ({type(e).__name__})")
        problems += 1
    if acct.exchange == "bybit" and getattr(ex, "http", None) is not None:
        try:
            resp = ex.http.get_wallet_balance(accountType="UNIFIED")
            total, usdt = _usdt_equity(resp)
            print(f"{tag} balance        : totalEquity={total} USDT walletBalance={usdt}")
        except Exception as e:  # noqa: BLE001 - 원문은 출력하지 않는다
            print(f"{tag} balance        : FAILED ({type(e).__name__})")
            problems += 1
    try:
        pos = ex.positions()
        if pos:
            for idx, p in sorted(pos.items()):
                print(f"{tag} position       : idx={idx} side={p.get('side')} size={p.get('size')} avg={p.get('avg_price')}")
        else:
            print(f"{tag} position       : none")
    except ExchangeError as e:
        print(f"{tag} positions      : FAILED ({e.code})")
        problems += 1
    except Exception as e:  # noqa: BLE001
        print(f"{tag} positions      : FAILED ({type(e).__name__})")
        problems += 1
    print(f"{tag} lot protection : {'conditional orders' if getattr(ex, 'supports_lot_protection', True) else 'position-level (set_position_protection) — VERIFY on a small live account'}")
    close = getattr(ex, "close", None)
    if callable(close):
        try:
            close()
        except Exception:  # noqa: BLE001
            pass
    return problems


def _check_ledger(settings) -> int:
    """원장 점검. DATABASE_URL 이 있으면 Postgres 에 실제로 접속해 왕복 시간과 원장 계정을 출력하고, 없으면 SQLite 파일을 읽는다."""
    if not settings.db_url:
        return _check_ledger_sqlite(settings)
    from .store import Store, describe_target
    try:
        store = Store(settings.ledger_target, legacy_account=settings.legacy_account_name(), schema=settings.db_schema)
    except Exception as e:  # noqa: BLE001
        print(f"ledger            : {describe_target(settings.db_url)} schema={settings.db_schema}  CONNECT FAILED ({type(e).__name__})")
        return 1
    try:
        rtt = store.ping_ms()
        print(f"ledger            : {store.describe()}  round-trip {rtt:.0f} ms  schema_version={store.get_meta('schema_version')}")
        if rtt > 500:
            print("ledger            : WARNING round-trip > 500 ms — use a Postgres in the same region as the server")
        known = {a.name for a in settings.accounts}
        problems = 0
        for account, info in store.ledger_accounts().items():
            flag = "" if account in known else ("  ← NOT IN config accounts" + (" (open lots!)" if info["open_lots"] else ""))
            print(f"ledger            : account {account!r} open_lots={info['open_lots']} "
                  f"pending_reports={info['pending_reports']} rows={info['rows']}{flag}")
            if account not in known and info["open_lots"]:
                problems += 1
        return problems
    except Exception as e:  # noqa: BLE001
        print(f"ledger            : query failed ({type(e).__name__})")
        return 1
    finally:
        store.close()


def _check_ledger_sqlite(settings) -> int:
    """DB 가 있으면 (마이그레이션 없이, 읽기 전용 sqlite 로) 원장의 계정 이름을 설정과 대조해 출력. 반환: 문제 수.
    1단계 스키마(account 컬럼 없음) 면 어느 계정으로 이전될지(legacy_account_name) 만 알린다 — 마이그레이션은 serve 가 한다."""
    import sqlite3
    path = settings.db_path
    if not os.path.exists(path):
        return 0
    known = {a.name for a in settings.accounts}
    problems = 0
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            cols = [r[1] for r in conn.execute("PRAGMA table_info(lots)").fetchall()]
            if not cols:
                return 0
            if "account" not in cols:
                print(f"ledger            : v0.1 schema → will be migrated to account {settings.legacy_account_name()!r} on serve")
                return 0
            rows = conn.execute("SELECT account, SUM(CASE WHEN status='open' THEN 1 ELSE 0 END), COUNT(*) FROM lots "
                                "GROUP BY account").fetchall()
        finally:
            conn.close()
    except sqlite3.Error as e:
        print(f"ledger            : could not read ({type(e).__name__})")
        return 1
    for account, open_n, total in rows:
        flag = "" if account in known else ("  ← NOT IN config accounts" + (" (open lots!)" if open_n else ""))
        print(f"ledger            : account {account!r} lots open={int(open_n or 0)} total={int(total or 0)}{flag}")
        if account not in known and open_n:
            problems += 1
    return problems


def cmd_check(args: argparse.Namespace) -> int:
    try:
        settings = _load_settings(args)
    except config.ConfigError as e:
        print(f"CONFIG ERROR: {e}")
        return 2
    sec = settings.secrets
    ok_live, reason = settings.live_execution_possible()
    halted = os.path.exists(settings.halt_file)

    print("== lake-executor check (read-only, no orders) ==")
    print(f"config            : {args.config}  env: {args.env}")
    print(f"routing           : {settings.routing}  accounts={len(settings.accounts)} "
          f"(enabled: {[a.name for a in settings.enabled_accounts()]})")
    print(f"listen            : {settings.listen_host}:{settings.listen_port}{settings.signal_path}")
    db_desc = ("DATABASE_URL (postgres)" if settings.db_url
               else f"sqlite {'exists' if os.path.exists(settings.db_path) else 'new'}")
    print(f"state_dir         : {settings.state_dir}  db={db_desc}  HALT={'YES' if halted else 'no'}")
    print(f"expired policy    : execute after expiry = {settings.expired_actions_execute} (others rejected EXPIRED)")
    print(f"live.enabled      : {settings.live_enabled}  -> live execution possible (any account): {ok_live}{'' if ok_live else ' (' + reason + ')'}")
    print(f"test.simulate     : {settings.test_simulate_fills}")
    for m in MODES:
        print(f"[{m}] signal secret : {_present(sec.signal_secret.get(m))}   "
              f"report secret: {_present(sec.report_secret.get(m))}   report url: {_present(sec.report_url.get(m))}")
    print(f"admin token       : {_present(sec.admin_token)}")
    print(f"telegram          : {_present(sec.telegram_bot_token and sec.telegram_chat_id)}")

    rc = 0
    rc += _check_ledger(settings)

    from .exchange import build_exchange
    any_keys = False
    for acct in settings.accounts:
        tag = f"[{acct.name}]"
        ok_acct, why = settings.live_execution_possible(acct)
        print(f"{tag} account        : exchange={acct.exchange} symbol={acct.symbol} enabled={acct.enabled} "
              f"position_mode={acct.position_mode} leverage={acct.leverage} margin={acct.margin_mode or '(unchanged)'} "
              f"testnet={acct.testnet} qty_multiplier={acct.qty_multiplier} report={acct.report}")
        print(f"{tag} api key        : {_present(acct.has_real_keys())} ({acct.env_prefix}_API_KEY/_API_SECRET"
              f"{'/_API_PASSPHRASE' if acct.exchange == 'okx' else ''})  live possible: {ok_acct}{'' if ok_acct else ' (' + why + ')'}")
        for m in MODES:
            print(f"{tag} report {m:<5}   : url {_present(acct.report_url.get(m))}  secret {_present(acct.report_secret.get(m))}"
                  f"{'  (report=false → unsent)' if not acct.report else ''}")
        if not acct.has_real_keys():
            print(f"{tag} connect        : skipped (no real API keys)")
            continue
        any_keys = True
        rc += _check_account(acct, lambda a: build_exchange(a))
    if not any_keys and settings.live_enabled:
        rc += 1
    print("result            :", "OK" if rc == 0 else "PROBLEMS FOUND")
    return 0 if rc == 0 else 1


# --------------------------------------------------------------------------- #
# simulate
# --------------------------------------------------------------------------- #
def _sim_signal(settings, *, position_id: str, seq: int, action: str, leg: str, position_idx: int,
                qty: float | None, expected_after: float | None, revision: int, stop_loss: float | None,
                take_profit: Any, reference_price: float | None, strategy: str) -> dict:
    ts = now_ms()
    return {
        "schema_version": 1,
        "strategy_name": "lake-simulate",
        "strategy": strategy,
        "mode": "test",
        "event_id": f"{position_id}:{seq}",
        "event_sequence": seq,
        "ts": ts,
        "expires_at_ms": ts + SIM_EXPIRES_MS,
        "exchange": "Bybit",
        "category": settings.category,
        "symbol": settings.symbol,
        "position_id": position_id,
        "leg": leg,
        "position_idx": position_idx,
        "action": action,
        "qty_btc": qty,
        "expected_qty_btc_after": expected_after,
        "reference_price": reference_price,
        "protection_revision": revision,
        "stop_loss": stop_loss,
        "take_profit": take_profit,
    }


def cmd_simulate(args: argparse.Namespace) -> int:
    import httpx

    settings = _load_settings(args)
    secret = settings.secrets.signal_secret.get("test")
    if not secret:
        print("LAKE_SIGNAL_SECRET_TEST is not set; cannot sign test signals")
        return 2
    url = args.base_url.rstrip("/") + settings.signal_path
    leg = args.leg
    position_idx = (1 if leg == "long" else 2) if settings.position_mode == "hedge" else 0
    qty = float(args.qty)
    position_id = args.position_id or f"sim-{now_ms()}"
    ref = float(args.reference_price) if args.reference_price is not None else None
    # 보호가격: 기준가가 있으면 그 기준 ±2%/±4%, 없으면 null
    if ref:
        sl1 = ref * (0.98 if leg == "long" else 1.02)
        tp1 = [ref * (1.02 if leg == "long" else 0.98), ref * (1.04 if leg == "long" else 0.96)]
        sl2 = ref * (0.99 if leg == "long" else 1.01)
    else:
        sl1, tp1, sl2 = None, None, None

    steps = [
        ("entry", dict(qty=qty, expected_after=qty, revision=1, stop_loss=sl1, take_profit=tp1)),
        ("add", dict(qty=qty, expected_after=qty * 2, revision=1, stop_loss=sl1, take_profit=tp1)),
        ("partial_exit", dict(qty=qty, expected_after=qty, revision=1, stop_loss=sl1, take_profit=tp1)),
        ("protection_update", dict(qty=None, expected_after=qty, revision=2, stop_loss=sl2, take_profit=tp1)),
        ("full_exit", dict(qty=qty, expected_after=0.0, revision=2, stop_loss=sl2, take_profit=tp1)),
    ]
    print(f"simulate -> {url}  position_id={position_id} leg={leg} idx={position_idx} qty={qty} (mode=test)")
    rc = 0
    with httpx.Client(timeout=10.0) as client:
        for i, (action, kw) in enumerate(steps, start=1):
            body = _sim_signal(settings, position_id=position_id, seq=i, action=action, leg=leg,
                               position_idx=position_idx, reference_price=ref, strategy=args.strategy, **kw)
            raw = canonical_json(body)
            headers = auth.headers_for(raw, secret, body["ts"])
            try:
                r = client.post(url, content=raw, headers=headers)
            except httpx.HTTPError as e:
                print(f"  {i}. {action:<18} -> ERROR {type(e).__name__}")
                rc = 1
                break
            text = r.text.strip().replace("\n", " ")
            print(f"  {i}. {action:<18} -> {r.status_code} {text[:200]}")
            if r.status_code >= 300:
                rc = 1
            if i < len(steps) and args.delay > 0:
                time.sleep(args.delay)
    print("simulate done" if rc == 0 else "simulate finished with non-2xx responses")
    return rc


# --------------------------------------------------------------------------- #
# sign
# --------------------------------------------------------------------------- #
def _stamp_signal(settings, body: dict, mode: str, ttl_s: float | None = None, *,
                  event_id: str | None = None, position_id: str | None = None,
                  seq: int | None = None, qty: float | None = None) -> tuple[bytes, dict, dict]:
    """신호 dict 에 ts/expires_at_ms/mode(+선택 덮어쓰기)를 채우고 (raw_bytes, headers, body) 를 돌려준다.

    event_id 가 'auto' 면 '<position_id>-<action>-<ts>' 로 생성한다(재전송 충돌 방지).
    """
    secret = settings.secrets.signal_secret.get(mode)
    if not secret:
        raise SystemExit(f"LAKE_SIGNAL_SECRET_{mode.upper()} is not set; cannot sign")
    body = dict(body)
    ts = now_ms()
    body["ts"] = ts
    body["mode"] = mode
    if position_id:
        body["position_id"] = position_id
    if seq is not None:
        body["event_sequence"] = int(seq)
    if qty is not None and body.get("action") != "protection_update":
        body["qty_btc"] = float(qty)
    if event_id:
        body["event_id"] = (f"{body.get('position_id','pos')}-{body.get('action','sig')}-{ts}"
                            if event_id == "auto" else event_id)
    ttl_ms = int((ttl_s if ttl_s is not None else SIM_EXPIRES_MS / 1000) * 1000)
    exp = body.get("expires_at_ms")
    if ttl_s is not None or not isinstance(exp, int) or isinstance(exp, bool) or exp < ts:
        body["expires_at_ms"] = ts + ttl_ms
    raw = canonical_json(body)
    headers = auth.headers_for(raw, secret, ts)
    return raw, headers, body


def cmd_fire(args: argparse.Namespace) -> int:
    """신호 파일에 서명해서 서버로 바로 전송한다 (수동 실매매 테스트용)."""
    import httpx

    settings = _load_settings(args)
    with open(args.file, "r", encoding="utf-8") as f:
        body = json.load(f)
    if not isinstance(body, dict):
        print("signal file must contain a JSON object")
        return 2
    raw, headers, body = _stamp_signal(settings, body, args.mode, args.ttl, event_id=args.event_id,
                                       position_id=args.position_id, seq=args.seq, qty=args.qty)
    url = args.url or (args.base_url.rstrip("/") + settings.signal_path)
    print(f"fire -> {url}")
    print(f"  mode={body['mode']} action={body.get('action')} position_id={body.get('position_id')} "
          f"event_id={body.get('event_id')} seq={body.get('event_sequence')} leg={body.get('leg')} "
          f"idx={body.get('position_idx')} qty_btc={body.get('qty_btc')} sl={body.get('stop_loss')} tp={body.get('take_profit')}")
    if body["mode"] == "live" and not args.yes:
        ans = input("  LIVE 신호입니다. 실제 주문이 나갑니다. 계속할까요? [y/N] ").strip().lower()
        if ans != "y":
            print("  취소")
            return 1
    try:
        with httpx.Client(timeout=10.0) as client:
            r = client.post(url, content=raw, headers=headers)
    except httpx.HTTPError as e:
        print(f"  ERROR {type(e).__name__}: {str(e)[:200]}")
        return 1
    print(f"  -> {r.status_code} {r.text.strip()[:300]}")
    if r.status_code in (200, 202):
        print("  접수됨. 실행 결과는 서버 로그 / GET /state (X-Admin-Token) / 거래소 앱에서 확인하세요.")
    return 0 if r.status_code < 300 else 1


def cmd_sign(args: argparse.Namespace) -> int:
    settings = _load_settings(args)
    mode = args.mode
    with open(args.file, "r", encoding="utf-8") as f:
        body = json.load(f)
    if not isinstance(body, dict):
        print("signal file must contain a JSON object")
        return 2
    if body.get("mode") != mode:
        print(f"note: body.mode set to '{mode}' (was {body.get('mode')!r})")
    raw, headers, body = _stamp_signal(settings, body, mode, args.ttl, event_id=args.event_id,
                                       position_id=args.position_id, seq=args.seq, qty=args.qty)

    out_path = args.out or (os.path.splitext(args.file)[0] + ".signed.json")
    with open(out_path, "wb") as f:
        f.write(raw)
    url = args.url or (DEFAULT_BASE_URL + settings.signal_path)

    print(f"signed body written: {out_path}  ({len(raw)} bytes; send these exact bytes)")
    print(f"X-Timestamp: {headers['X-Timestamp']}")
    print(f"X-Signature: {headers['X-Signature']}")
    print(f"(valid for +/-{settings.max_clock_skew_ms // 1000}s around ts; expires_at_ms={body['expires_at_ms']})")
    print()
    print("curl example:")
    print(f"  curl -sS -i -X POST '{url}' \\")
    print("    -H 'Content-Type: application/json' \\")
    print(f"    -H 'X-Timestamp: {headers['X-Timestamp']}' \\")
    print(f"    -H 'X-Signature: {headers['X-Signature']}' \\")
    print(f"    --data-binary @'{out_path}'")
    return 0


# --------------------------------------------------------------------------- #
# argparse
# --------------------------------------------------------------------------- #
def _add_config_args(p: argparse.ArgumentParser, *, suppress: bool) -> None:
    """--config/--env 는 서브커맨드 앞뒤 어디에 와도 된다 (`check --config x` / `--config x check`).
    서브파서 쪽은 SUPPRESS 기본값이라 주어졌을 때만 전역 값을 덮어쓴다."""
    p.add_argument("--config", default=argparse.SUPPRESS if suppress else DEFAULT_CONFIG_PATH,
                   help=f"config.json path (default: {DEFAULT_CONFIG_PATH})")
    p.add_argument("--env", default=argparse.SUPPRESS if suppress else DEFAULT_ENV_PATH,
                   help=f".env path with secrets (default: {DEFAULT_ENV_PATH})")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m lake_executor",
                                description="lake-executor: lake webhook -> Bybit executor")
    _add_config_args(p, suppress=False)
    sub = p.add_subparsers(dest="command", required=True)

    sp = sub.add_parser("serve", help="run receiver + executor + reporter")
    _add_config_args(sp, suppress=True)
    sp.set_defaults(func=cmd_serve)

    cp = sub.add_parser("check", help="verify config, keys and read-only exchange connectivity per account (no orders)")
    _add_config_args(cp, suppress=True)
    cp.set_defaults(func=cmd_check)

    mp = sub.add_parser("simulate", help="send signed TEST-mode synthetic signals to a running server")
    _add_config_args(mp, suppress=True)
    mp.add_argument("--base-url", default=DEFAULT_BASE_URL)
    mp.add_argument("--qty", type=float, default=SIM_DEFAULT_QTY, help="qty_btc per step (default 0.002)")
    mp.add_argument("--leg", choices=("long", "short"), default="long")
    mp.add_argument("--strategy", choices=("basic", "overheat", "range"), default="basic")
    mp.add_argument("--position-id", default=None, help="default: sim-<now_ms>")
    mp.add_argument("--reference-price", type=float, default=None,
                    help="reference_price for entry/add (also derives SL/TP); omit for null")
    mp.add_argument("--delay", type=float, default=1.0, help="seconds between signals (default 1.0)")
    mp.set_defaults(func=cmd_simulate)

    gp = sub.add_parser("sign", help="stamp ts into a signal file and print signature headers + curl example")
    _add_config_args(gp, suppress=True)
    gp.add_argument("--file", required=True, help="signal JSON file")
    gp.add_argument("--mode", choices=MODES, required=True)
    gp.add_argument("--out", default=None, help="where to write the signed body (default: <file>.signed.json)")
    gp.add_argument("--url", default=None, help="URL for the curl example (default: http://127.0.0.1:8787<signal_path>)")
    gp.set_defaults(func=cmd_sign)
    for _p in (gp,):
        _p.add_argument("--ttl", type=float, default=None, help="expires_at_ms = now + ttl seconds (default 15)")
        _p.add_argument("--event-id", default=None, help="override event_id ('auto' = <position_id>-<action>-<ts>)")
        _p.add_argument("--position-id", default=None)
        _p.add_argument("--seq", type=int, default=None, help="override event_sequence")
        _p.add_argument("--qty", type=float, default=None, help="override qty_btc")

    fp = sub.add_parser("fire", help="sign a signal file and POST it to a running server (manual live/test firing)")
    _add_config_args(fp, suppress=True)
    fp.add_argument("--file", required=True, help="signal JSON file (see tools/signals/)")
    fp.add_argument("--mode", choices=MODES, required=True)
    fp.add_argument("--base-url", default=DEFAULT_BASE_URL)
    fp.add_argument("--url", default=None, help="full URL (overrides --base-url + signal_path)")
    fp.add_argument("--ttl", type=float, default=None, help="expires_at_ms = now + ttl seconds (default 15)")
    fp.add_argument("--event-id", default=None, help="override event_id ('auto' = <position_id>-<action>-<ts>)")
    fp.add_argument("--position-id", default=None)
    fp.add_argument("--seq", type=int, default=None, help="override event_sequence")
    fp.add_argument("--qty", type=float, default=None, help="override qty_btc")
    fp.add_argument("-y", "--yes", action="store_true", help="skip the LIVE confirmation prompt")
    fp.set_defaults(func=cmd_fire)
    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args) or 0)
    except config.ConfigError as e:
        print(f"CONFIG ERROR: {e}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
