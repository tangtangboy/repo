"""라이브 신호 로그 — 백테스트 검증용 데이터셋을 **주문 경로 밖에서** 비동기로 남긴다.

왜: 차트 백테스트는 실제로 받은 신호와 다르다. 실시간으로 받은 신호(시각·신호가 말한 가격·수신 순간의 시장가·수량·계정 사이징
문맥) 를 그대로 적어 두면, 나중에 시드·레버리지·비중을 바꿔 가며 재계산할 수 있는 "진짜 라이브 데이터" 가 된다. 체결은 거래소
트레이드 히스토리 API 로도 맞출 수 있지만, 우리 체결 기록(fills) 도 event_id 로 붙여 내보낸다 (`export_signal_rows`).

어떻게: 수신기는 접수(202/200 duplicate) 직후 `SignalLog.record()` 로 큐에 넣기만 한다 (마이크로초, 절대 블로킹/예외 없음).
백그라운드 스레드가 (1) 수신 순간의 시장가를 공개 API 로 가져오고(실패하면 null), (2) `state/signal_log.jsonl` 에 한 줄 append,
(3) **자기 전용 DB 연결**(Store 별도 인스턴스) 로 signal_log 테이블에 INSERT 한다. 원장 연결의 락을 쓰지 않으므로 DB 가 느려도
실행기·수신기는 영향이 없고, DB 가 끊겨 있으면 JSONL 에는 남고 DB 쪽은 재시도 뒤 포기(통계에 집계) 한다.

내보내기: `python -m lake_executor export --mode live --since 2026-10-01 --format csv --out signals.csv`
          대시보드 /ui/signal-log 와 /ui/signal-log.csv
"""
from __future__ import annotations

import csv
import io
import json
import logging
import os
import queue
import threading
import time
from typing import Any, Callable

from .util import now_ms

log = logging.getLogger("lake_executor.signal_log")

PUBLIC_TICKER_URL = "https://api.bybit.com/v5/market/tickers"
PRICE_CACHE_MS = 1000
DB_RETRY_MAX = 5

# export 열 (순서 고정 — 백테스트 입력으로 그대로 쓴다)
EXPORT_COLUMNS = (
    "received_at_ms", "received_at", "mode", "event_id", "position_id", "event_sequence", "strategy", "strategy_name",
    "action", "leg", "position_idx", "exchange", "symbol", "qty_btc", "expected_qty_btc_after", "reference_price",
    "mark_price", "last_price", "price_at_ms", "price_source", "stop_loss", "take_profit", "protection_revision",
    "signal_ts", "expires_at_ms", "latency_signal_to_receipt_ms", "ingest_result",
    "signal_status", "signal_reason", "processed_at_ms", "processing_ms",
    "account", "run_status", "run_reason", "run_note",
    "fill_qty", "fill_avg_price", "fill_count", "first_fill_ms", "last_fill_ms", "fill_latency_ms",
    "account_exchange", "account_leverage", "account_margin_mode", "account_position_mode", "account_qty_multiplier",
    "account_live_possible",
)


def iso_ms(ms: Any) -> str:
    try:
        t = int(ms) / 1000.0
    except (TypeError, ValueError):
        return ""
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t)) + f".{int(ms) % 1000:03d}Z"


def public_bybit_price(symbol: str, timeout_s: float = 3.0) -> dict | None:
    """공개 티커 (키 불필요). 실패하면 None — 로그 행은 가격 없이도 남긴다."""
    try:
        import httpx
        r = httpx.get(PUBLIC_TICKER_URL, params={"category": "linear", "symbol": symbol}, timeout=timeout_s)
        d = r.json()
        item = ((d.get("result") or {}).get("list") or [{}])[0]
        mark, last = item.get("markPrice"), item.get("lastPrice")
        if mark is None and last is None:
            return None
        return {"mark_price": float(mark) if mark not in (None, "") else None,
                "last_price": float(last) if last not in (None, "") else None,
                "source": "bybit-public"}
    except Exception as e:  # noqa: BLE001
        log.debug("public price fetch failed: %s", type(e).__name__)
        return None


def accounts_context(settings: Any) -> list[dict]:
    """계정 사이징 문맥 (다른 시드/레버리지/비중으로 재계산할 때 기준값). I/O 없음."""
    out = []
    for a in getattr(settings, "accounts", None) or []:
        try:
            live_ok = bool(settings.live_execution_possible(a)[0])
        except Exception:  # noqa: BLE001
            live_ok = None
        out.append({"name": a.name, "exchange": a.exchange, "enabled": bool(a.enabled), "symbol": a.symbol,
                    "leverage": a.leverage, "margin_mode": a.margin_mode, "position_mode": a.position_mode,
                    "qty_multiplier": a.qty_multiplier, "live_possible": live_ok})
    return out


def signal_to_row(sig: Any, ingest_result: str, received_at_ms: int, accounts: list[dict]) -> dict:
    tp = sig.take_profit
    if isinstance(tp, (int, float)):
        tp = [float(tp)]
    return {
        "received_at_ms": int(received_at_ms), "mode": sig.mode.value, "event_id": sig.event_id,
        "position_id": sig.position_id, "event_sequence": int(sig.event_sequence), "strategy": sig.strategy.value,
        "strategy_name": sig.strategy_name, "action": sig.action.value, "leg": sig.leg.value,
        "position_idx": int(sig.position_idx), "exchange": sig.exchange, "symbol": sig.symbol,
        "qty_btc": sig.qty_btc, "expected_qty_btc_after": sig.expected_qty_btc_after,
        "reference_price": sig.reference_price, "stop_loss": sig.stop_loss, "take_profit": tp,
        "protection_revision": int(sig.protection_revision), "signal_ts": int(sig.ts),
        "expires_at_ms": int(sig.expires_at_ms), "ingest_result": ingest_result,
        "mark_price": None, "last_price": None, "price_at_ms": None, "price_source": None,
        "accounts": accounts,
    }


def rows_to_csv(rows: list[dict]) -> str:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=list(EXPORT_COLUMNS), extrasaction="ignore", lineterminator="\n")
    w.writeheader()
    for r in rows:
        r = dict(r)
        r["received_at"] = iso_ms(r.get("received_at_ms"))
        if isinstance(r.get("take_profit"), (list, dict)):
            r["take_profit"] = json.dumps(r["take_profit"])
        w.writerow({k: ("" if r.get(k) is None else r.get(k)) for k in EXPORT_COLUMNS})
    return buf.getvalue()


def rows_to_jsonl(rows: list[dict]) -> str:
    out = []
    for r in rows:
        r = dict(r)
        r["received_at"] = iso_ms(r.get("received_at_ms"))
        out.append(json.dumps({k: r.get(k) for k in EXPORT_COLUMNS}, ensure_ascii=False))
    return "\n".join(out) + ("\n" if out else "")


class SignalLog:
    """큐 + 백그라운드 writer. record() 는 절대 블로킹하지 않는다."""

    def __init__(self, store_factory: Callable[[], Any], state_dir: str, settings: Any,
                 price_fetcher: Callable[[str], dict | None] | None = public_bybit_price,
                 jsonl: bool = True, max_queue: int = 10000):
        self._store_factory = store_factory
        self._settings = settings
        self._price_fetcher = price_fetcher
        self._jsonl_path = os.path.join(state_dir, "signal_log.jsonl") if jsonl else None
        self._q: queue.Queue = queue.Queue(maxsize=max_queue)
        self._store: Any = None
        self._price_cache: dict[str, tuple[int, dict | None]] = {}
        self.stats = {"queued": 0, "written": 0, "duplicates": 0, "dropped": 0, "db_errors": 0, "jsonl_errors": 0,
                      "price_missing": 0, "last_error": "", "last_written_at_ms": 0, "pending": 0}
        self._lock = threading.Lock()

    # ---- 호출 측 (수신기 스레드풀) ----
    def record(self, sig: Any, ingest_result: str, received_at_ms: int | None = None) -> bool:
        try:
            row = signal_to_row(sig, ingest_result, received_at_ms or now_ms(), accounts_context(self._settings))
            self._q.put_nowait(row)
            with self._lock:
                self.stats["queued"] += 1
                self.stats["pending"] = self._q.qsize()
            return True
        except queue.Full:
            with self._lock:
                self.stats["dropped"] += 1
            log.warning("signal_log queue full; dropped %s", getattr(sig, "event_id", "?"))
            return False
        except Exception as e:  # noqa: BLE001 - 로그는 절대 수신기를 깨뜨리지 않는다
            with self._lock:
                self.stats["last_error"] = type(e).__name__
            log.warning("signal_log.record failed: %s", type(e).__name__)
            return False

    # ---- writer 스레드 ----
    def _price(self, symbol: str) -> dict | None:
        if self._price_fetcher is None:
            return None
        now = now_ms()
        cached = self._price_cache.get(symbol)
        if cached and now - cached[0] < PRICE_CACHE_MS:
            return cached[1]
        try:
            p = self._price_fetcher(symbol)
        except Exception as e:  # noqa: BLE001
            log.debug("price fetcher error: %s", type(e).__name__)
            p = None
        self._price_cache[symbol] = (now, p)
        return p

    def _append_jsonl(self, row: dict) -> None:
        if not self._jsonl_path:
            return
        try:
            os.makedirs(os.path.dirname(self._jsonl_path), exist_ok=True)
            with open(self._jsonl_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        except OSError as e:
            with self._lock:
                self.stats["jsonl_errors"] += 1
                self.stats["last_error"] = f"jsonl:{type(e).__name__}"

    def _store_or_none(self) -> Any:
        if self._store is None:
            try:
                self._store = self._store_factory()
            except Exception as e:  # noqa: BLE001
                with self._lock:
                    self.stats["db_errors"] += 1
                    self.stats["last_error"] = f"db-open:{type(e).__name__}"
                return None
        return self._store

    def _write_db(self, row: dict) -> bool:
        """True = 끝남(기록 또는 중복). False = DB 불통 → 호출자가 재시도."""
        store = self._store_or_none()
        if store is None:
            return False
        try:
            inserted = store.append_signal_log(row)
            with self._lock:
                if inserted:
                    self.stats["written"] += 1
                    self.stats["last_written_at_ms"] = now_ms()
                else:
                    self.stats["duplicates"] += 1
            return True
        except Exception as e:  # noqa: BLE001
            with self._lock:
                self.stats["db_errors"] += 1
                self.stats["last_error"] = f"db:{type(e).__name__}"
            log.warning("signal_log db write failed: %s", type(e).__name__)
            try:
                store.close()
            except Exception:  # noqa: BLE001
                pass
            self._store = None
            return False

    def process_one(self, row: dict) -> None:
        """한 행 처리: 가격 → JSONL → DB(재시도). 테스트에서 직접 호출한다."""
        if row.get("price_at_ms") is None:
            p = self._price(str(row.get("symbol") or "BTCUSDT"))
            if p:
                row["mark_price"], row["last_price"] = p.get("mark_price"), p.get("last_price")
                row["price_source"] = p.get("source")
            else:
                with self._lock:
                    self.stats["price_missing"] += 1
            row["price_at_ms"] = now_ms()
        if not row.get("_jsonl_done"):
            self._append_jsonl({k: v for k, v in row.items() if not k.startswith("_")})
            row["_jsonl_done"] = True
        attempt = 0
        while not self._write_db(row):
            attempt += 1
            if attempt >= DB_RETRY_MAX:
                log.error("signal_log: giving up DB write for %s after %d attempts (kept in jsonl)", row.get("event_id"), attempt)
                return
            time.sleep(min(2.0 * attempt, 10.0))

    def drain(self, max_items: int = 1000) -> int:
        """큐에 있는 것을 지금 처리 (테스트/종료용). 처리한 수 반환."""
        n = 0
        while n < max_items:
            try:
                row = self._q.get_nowait()
            except queue.Empty:
                break
            self.process_one(row)
            n += 1
        with self._lock:
            self.stats["pending"] = self._q.qsize()
        return n

    def run_forever(self, stop_event: threading.Event) -> None:
        log.info("signal_log writer started (jsonl=%s)", self._jsonl_path or "off")
        while not stop_event.is_set():
            try:
                row = self._q.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                self.process_one(row)
            except Exception as e:  # noqa: BLE001 - writer 는 절대 죽지 않는다
                log.exception("signal_log writer error: %s", type(e).__name__)
            with self._lock:
                self.stats["pending"] = self._q.qsize()
        try:
            self.drain(max_items=500)        # 종료 시 남은 것은 빨리 비운다 (재시도 없이)
        except Exception:  # noqa: BLE001
            pass
        self.close()
        log.info("signal_log writer stopped")

    def close(self) -> None:
        if self._store is not None:
            try:
                self._store.close()
            except Exception:  # noqa: BLE001
                pass
            self._store = None

    def snapshot(self) -> dict:
        with self._lock:
            d = dict(self.stats)
        d["pending"] = self._q.qsize()
        d["jsonl"] = self._jsonl_path
        return d
