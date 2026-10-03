"""CLI (ARCHITECTURE.md §7) — `python -m lake_executor <serve|check|simulate|sign>`.

  serve     store 열기 → 거래소 준비(live 가능하면 Bybit, test_simulate_fills 면 Paper) → executor 복구
            → 스레드(executor / reporter / snapshot loop) → uvicorn. SIGTERM/SIGINT 시 정상 종료.
  check     설정·키 유무·Bybit 읽기 전용 연결(잔고/instrument/positions)·회신 URL 설정 여부 출력. 주문 없음.
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


def _exchanges_for(settings) -> dict[str, Any]:
    """§7 serve: live 가능하면 Bybit, test_simulate_fills 면 Paper. 아니면 None."""
    from .exchange import build_exchange
    exchanges: dict[str, Any] = {"live": None, "test": None}
    ok, reason = settings.live_execution_possible()
    if ok:
        exchanges["live"] = build_exchange(settings, "bybit")
        log.info("live exchange: bybit (testnet=%s)", settings.testnet)
    else:
        log.warning("live execution disabled (%s): live signals will be rejected/LIVE_DISABLED", reason)
    if settings.test_simulate_fills:
        exchanges["test"] = build_exchange(settings, "paper")
        log.info("test exchange: paper (simulate_fills=true)")
    else:
        log.info("test mode: record only (simulate_fills=false)")
    return exchanges


# --------------------------------------------------------------------------- #
# serve
# --------------------------------------------------------------------------- #
def _snapshot_loop(settings, executor, exchanges: dict[str, Any], stop_event: threading.Event) -> None:
    """모드별 snapshot_interval_ms 마다 reconcile → reporter.snapshot (executor.snapshot_now).
    다음 예정 시각은 호출이 **끝난 뒤** 잡는다: 실행기 락 대기/거래소 지연으로 호출이 길어져도 다음 스냅샷이
    바로 이어져 lake 의 신선도 창(90초) 안에서 간격이 두 배로 벌어지지 않는다."""
    interval_ms = max(1000, int(getattr(settings, "snapshot_interval_ms", 30000)))
    next_at = {m: now_ms() + 2000 for m in MODES}  # 기동 직후 2초 뒤 첫 스냅샷
    log.info("snapshot loop started (interval=%sms)", interval_ms)
    while not stop_event.is_set():
        for mode in MODES:
            if exchanges.get(mode) is None or now_ms() < next_at[mode]:
                continue
            started = now_ms()
            try:
                res = executor.snapshot_now(mode)
                if res is None:
                    log.warning("snapshot %s skipped (reconcile not consistent)", mode)
            except Exception as e:  # noqa: BLE001 - 루프는 죽지 않는다
                log.exception("snapshot %s failed: %s", mode, type(e).__name__)
            finished = now_ms()
            if finished - started > interval_ms // 2:
                log.warning("snapshot %s took %dms (lock/exchange contention)", mode, finished - started)
            # 호출에 걸린 시간만큼 다음 슬롯을 당긴다 (최소 1초 뒤)
            next_at[mode] = max(finished + 1000, started + interval_ms)
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
    log.info("lake-executor serve: symbol=%s position_mode=%s listen=%s:%s path=%s",
             settings.symbol, settings.position_mode, settings.listen_host, settings.listen_port, settings.signal_path)

    store = Store(settings.db_path)
    alerts = Alerts(settings)
    reporter = Reporter(settings, store, alerts)
    exchanges = _exchanges_for(settings)
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

    services = SimpleNamespace(executor=executor, reporter=reporter, alerts=alerts, exchanges=exchanges)
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
    print(f"symbol/category   : {settings.symbol} / {settings.category}")
    print(f"position_mode     : {settings.position_mode}  leverage={settings.leverage}  margin={settings.margin_mode or '(unchanged)'}")
    print(f"testnet           : {settings.testnet}")
    print(f"listen            : {settings.listen_host}:{settings.listen_port}{settings.signal_path}")
    print(f"state_dir         : {settings.state_dir}  db={'exists' if os.path.exists(settings.db_path) else 'new'}  HALT={'YES' if halted else 'no'}")
    print(f"live.enabled      : {settings.live_enabled}  -> live execution possible: {ok_live}{'' if ok_live else ' (' + reason + ')'}")
    print(f"test.simulate     : {settings.test_simulate_fills}")
    print(f"bybit api key     : {_present(sec.has_real_bybit_keys())}")
    for m in MODES:
        print(f"[{m}] signal secret : {_present(sec.signal_secret.get(m))}   "
              f"report secret: {_present(sec.report_secret.get(m))}   report url: {_present(sec.report_url.get(m))}")
    print(f"admin token       : {_present(sec.admin_token)}")
    print(f"telegram          : {_present(sec.telegram_bot_token and sec.telegram_chat_id)}")

    if not sec.has_real_bybit_keys():
        print("bybit             : skipped (no real API keys)")
        return 0 if not settings.live_enabled else 1

    from .exchange import ExchangeError, build_exchange
    rc = 0
    try:
        ex = build_exchange(settings, "bybit")
        instr = ex.instrument()
        print(f"bybit instrument  : qty_step={instr.get('qty_step')} min_qty={instr.get('min_qty')} "
              f"max_qty={instr.get('max_qty')} tick={instr.get('tick')}")
        try:
            print(f"bybit last/mark   : {ex.last_price()} / {ex.mark_price()}")
        except ExchangeError as e:
            print(f"bybit ticker      : FAILED ({e.code})")
            rc = 1
        try:
            resp = ex.http.get_wallet_balance(accountType="UNIFIED")
            total, usdt = _usdt_equity(resp)
            print(f"bybit balance     : totalEquity={total} USDT walletBalance={usdt}")
        except Exception as e:  # noqa: BLE001 - 원문은 출력하지 않는다
            print(f"bybit balance     : FAILED ({type(e).__name__})")
            rc = 1
        try:
            pos = ex.positions()
            if pos:
                for idx, p in sorted(pos.items()):
                    print(f"bybit position    : idx={idx} side={p.get('side')} size={p.get('size')} avg={p.get('avg_price')}")
            else:
                print("bybit position    : none")
        except ExchangeError as e:
            print(f"bybit positions   : FAILED ({e.code})")
            rc = 1
    except ExchangeError as e:
        print(f"bybit connect     : FAILED ({e.code})")
        rc = 1
    except Exception as e:  # noqa: BLE001
        print(f"bybit connect     : FAILED ({type(e).__name__})")
        rc = 1
    print("result            :", "OK" if rc == 0 else "PROBLEMS FOUND")
    return rc


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
def cmd_sign(args: argparse.Namespace) -> int:
    settings = _load_settings(args)
    mode = args.mode
    secret = settings.secrets.signal_secret.get(mode)
    if not secret:
        print(f"LAKE_SIGNAL_SECRET_{mode.upper()} is not set; cannot sign")
        return 2
    with open(args.file, "r", encoding="utf-8") as f:
        body = json.load(f)
    if not isinstance(body, dict):
        print("signal file must contain a JSON object")
        return 2
    ts = now_ms()
    body["ts"] = ts
    if body.get("mode") != mode:
        print(f"note: body.mode set to '{mode}' (was {body.get('mode')!r})")
        body["mode"] = mode
    exp = body.get("expires_at_ms")
    if not isinstance(exp, int) or isinstance(exp, bool) or exp < ts:
        body["expires_at_ms"] = ts + SIM_EXPIRES_MS
    raw = canonical_json(body)
    headers = auth.headers_for(raw, secret, ts)

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

    cp = sub.add_parser("check", help="verify config, keys and read-only Bybit connectivity (no orders)")
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
