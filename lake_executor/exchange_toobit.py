"""Toobit USDT-M 무기한 선물 래퍼 (ARCHITECTURE_MULTI_EXCHANGE.md §3 Toobit).

자체 httpx 클라이언트로 ccxt 4.5.85 `toobit.sign` 과 **동일한 규격**으로 서명한다
(근거: ccxt/toobit.py `sign`, `create_contract_order_request`, `parse_order`, `fetch_positions`, `handle_errors`).

서명 규격 (ccxt `toobit.sign` 그대로)
  - 비공개 엔드포인트: 파라미터에 `recvWindow`(5000) 와 `timestamp`(ms) 를 **그 순서로 뒤에** 붙인다.
  - POST/DELETE: 본문 = urlencode(params + recvWindow + timestamp), 쿼리 없음.
    signature = HMAC-SHA256(secret, 본문) hex → **본문 끝에** `&signature=` 로 붙는다 (ccxt 가 그렇게 한다).
  - GET: 쿼리 = urlencode(params + recvWindow + timestamp). signature 는 쿼리 위에서 계산해 `&signature=` 로 쿼리 끝에 붙는다.
  - 헤더: `X-BB-APIKEY`, `Content-Type: application/x-www-form-urlencoded` (ccxt 는 private 요청 전부에 넣는다).
  - urlencode 는 `urllib.parse.urlencode(..., quote_via=quote)` 이며 bool 은 'true'/'false' (ccxt `Exchange.urlencode`).
  - 공개(quote/exchangeInfo) 엔드포인트는 서명 없이 GET 쿼리만.

체결 조회 (`executions(order_id)`)
  - `userTrades` 는 orderId 필터가 없다(ccxt `fetch_my_trades`: symbol/startTime/endTime/limit 뿐). 24시간 창에 limit 500 으로
    한 페이지만 읽으면 활발한 계정에서는 방금 낸 주문의 체결이 페이지 밖에 남을 수 있으므로, 이 래퍼가 낸 주문은 생성 시각을
    기억해 `startTime = 생성 시각 − trades_start_margin_ms` 로 창을 좁히고, 한 페이지가 가득 차면 마지막 체결 시각 + 1 부터
    다음 페이지를 읽는다(최대 trades_max_pages). 재기동 뒤 모르는 주문은 24시간 창으로 같은 방식으로 페이지를 넘긴다.

수량 단위
  - 실행기와는 항상 BTC 로 주고받는다. Toobit 네이티브 수량은 **계약 수** (`quantity = qty_btc / contractMultiplier`,
    BTC-SWAP-USDT 의 contractMultiplier = 0.001). `instrument()` 는 BTC 로 환산한 step/min/max 를 돌려준다.
  - 변환은 Decimal 로 한다 (0.004 / 0.001 이 float 로는 4.000000000000001 이 되는 문제 방지).

포지션/사이드
  - Toobit 은 LONG/SHORT 양방향(헤지) 포지션만 쓴다. position_idx 1 → LONG (BUY_OPEN / SELL_CLOSE),
    2 → SHORT (SELL_OPEN / BUY_CLOSE). 단방향(0) 은 지원하지 않는다 (설정 검증에서 이미 거부, 여기서는 ValueError).

보호주문 (★ 실계정 검증 전)
  - `supports_lot_protection = False` (클래스 속성). lot 단위 조건부 주문(type=STOP) 파라미터는 공식 문서를 확인하지 못해
    `place_conditional` 은 구현만 해 두고 VERIFY 로 표시한다. 실계정(소액)에서 검증되면
    `ToobitExchange.supports_lot_protection = True` 로 바꾸거나 인스턴스에서 `ex.supports_lot_protection = True` 로 켠다.
  - 기본 경로는 `set_position_protection` → `POST api/v1/futures/position/trading-stop` (포지션 단위 TP/SL).
    파라미터 이름(stopLoss/takeProfit/slTriggerBy/tpTriggerBy/side)은 ccxt 의 부착형 TP/SL 파라미터에서 유추한 것 — VERIFY.

오류 처리
  - 응답 JSON `{code, msg}` 에서 code 가 0/200 이 아니면: 쓰기 → `ExchangeRejected(ret_code=code)`, 읽기 → `ExchangeError(EXCHANGE_ERROR)`.
  - 단, **실행 결과를 알 수 없는** 응답은 쓰기에서도 거부가 아니라 `ExchangeError` 다 (주문이 들어갔을 수 있으므로 실행기가
    get_order 로 확인하고 미확정으로 남긴다): `-1006/-1007/-1146/-1147`(execution status unknown / 주문 생성·취소 타임아웃) →
    `EXCHANGE_TIMEOUT`, `-1000/-1001`(unknown/internal error) 과 코드 없는 HTTP ≥ 500 → `EXCHANGE_ERROR`
    (ccxt toobit: 전부 OperationFailed).
  - `-1141`(중복 clientOrderId) 은 멱등 성공으로 보고 기존 주문을 조회해 돌려준다.
  - 타임아웃(httpx.TimeoutException 또는 HTTP 408/504) → `ExchangeError("EXCHANGE_TIMEOUT")`. 읽기는 네트워크 오류에 한해 재시도.
  - 거래소 응답 원문은 WARNING 로그에 200자 절단으로만 남긴다. 키/시크릿은 절대 로그에 남기지 않는다.
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import threading
import time
from decimal import Decimal, ROUND_DOWN
from typing import Any, Callable
from urllib.parse import quote, urlencode

from .config import AccountSettings
from .exchange import ExchangeBase, ExchangeError, ExchangeRejected, _as_account, _f, _i, _trunc
from .util import fmt_step, now_ms, round_tick

log = logging.getLogger("lake_executor.exchange_toobit")

BASE_URL = "https://api.toobit.com"

# 엔드포인트 (ccxt toobit describe: common.get / private.get|post|delete)
EP_EXCHANGE_INFO = "api/v1/exchangeInfo"
EP_TICKER_PRICE = "quote/v1/contract/ticker/price"
EP_TICKER_PRICE_FALLBACK = "quote/v1/ticker/price"
EP_TICKER_24H = "quote/v1/contract/ticker/24hr"
EP_MARK_PRICE = "quote/v1/markPrice"
EP_ORDER = "api/v1/futures/order"
EP_OPEN_ORDERS = "api/v1/futures/openOrders"
EP_USER_TRADES = "api/v1/futures/userTrades"
EP_POSITIONS = "api/v1/futures/positions"
EP_LEVERAGE = "api/v1/futures/leverage"
EP_MARGIN_TYPE = "api/v1/futures/marginType"
EP_TRADING_STOP = "api/v1/futures/position/trading-stop"

# Toobit 오류 코드 (ccxt toobit exceptions.exact)
CODE_DUPLICATE_CLIENT_ORDER_ID = -1141
_ORDER_GONE_CODES = {-1139, -1142, -1143, -2013}      # 체결됨 / 취소됨 / 오더북에 없음 / 존재하지 않음
_ORDER_NOT_FOUND_CODES = {-1143, -2013}
_OK_CODES = {"", "0", "200"}
_TIMEOUT_HTTP = {408, 504}
# 쓰기 응답의 결과를 알 수 없는 코드 (ccxt toobit exceptions.exact: OperationFailed) → 거부 대신 오류로 전달
_UNCERTAIN_TIMEOUT_CODES = {-1006, -1007, -1146, -1147}   # execution status unknown / order creation|cancellation timeout
_UNCERTAIN_ERROR_CODES = {-1000, -1001}                   # unknown error / internal error
_ALREADY_SET_WORDS = ("no need to change", "not modified", "already", "same as")

_STATUS_MAP = {
    "NEW": "New",
    "PENDING_NEW": "New",
    "PARTIALLY_FILLED": "PartiallyFilled",
    "FILLED": "Filled",
    "CANCELED": "Cancelled",
    "PENDING_CANCEL": "Cancelled",
    "REJECTED": "Rejected",
}
# position_idx → (열기 side, 닫기 side, 포지션 side)
_SIDE_BY_IDX = {1: ("BUY_OPEN", "SELL_CLOSE", "LONG"), 2: ("SELL_OPEN", "BUY_CLOSE", "SHORT")}
_IDX_BY_NATIVE_SIDE = {"BUY_OPEN": 1, "SELL_CLOSE": 1, "SELL_OPEN": 2, "BUY_CLOSE": 2}
_IDX_BY_POSITION_SIDE = {"LONG": 1, "SHORT": 2}
_TRIGGER_BY_MAP = {"markprice": "MARK_PRICE", "lastprice": "CONTRACT_PRICE", "mark": "MARK_PRICE", "last": "CONTRACT_PRICE"}


# --------------------------------------------------------------------------- #
# 서명 (ccxt toobit.sign 재현) — 테스트에서 같은 함수로 기대값을 만들 수 있도록 모듈 함수로 둔다
# --------------------------------------------------------------------------- #
def ccxt_urlencode(params: dict) -> str:
    """ccxt `Exchange.urlencode`: bool → 'true'/'false', `urllib.parse.urlencode(..., quote_via=quote)`. 키 순서는 삽입 순서."""
    out = {}
    for k, v in params.items():
        if isinstance(v, bool):
            out[k] = "true" if v else "false"
        else:
            out[k] = v
    return urlencode(out, doseq=False, quote_via=quote)


def sign_request(secret: str, method: str, params: dict, timestamp_ms: int, recv_window: int = 5000) -> dict:
    """ccxt `toobit.sign` (private, 비-배치) 와 동일한 서명 결과.
    → {"query": str, "body": str|None, "payload": str, "signature": str}
       POST/DELETE: body = urlencode(params+recvWindow+timestamp) + '&signature=..', query = ''
       GET        : query = urlencode(params+recvWindow+timestamp) + '&signature=..', body = None"""
    extended = dict(params)
    extended["recvWindow"] = str(recv_window)
    extended["timestamp"] = str(int(timestamp_ms))
    m = method.upper()
    if m in ("POST", "DELETE"):
        body = ccxt_urlencode(extended)
        query = ""
    else:
        body = None
        query = ccxt_urlencode(extended)
    payload = (body or "") + query          # ccxt: payload = payloadBody + queryString
    sig = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    if query:
        query = query + "&signature=" + sig
    else:
        body = (body or "") + "&signature=" + sig
    return {"query": query, "body": body, "payload": payload, "signature": sig}


# --------------------------------------------------------------------------- #
# 헬퍼
# --------------------------------------------------------------------------- #
def _dec(v: Any) -> Decimal:
    return Decimal(str(v))


def _code_of(data: Any) -> str:
    """응답 JSON 의 code 를 문자열로 (없으면 '')."""
    if not isinstance(data, dict):
        return ""
    c = data.get("code")
    if c is None:
        return ""
    return str(c).strip()


def _is_timeout_exc(e: BaseException) -> bool:
    try:
        import httpx  # 지연 import: 테스트 더블만 쓰는 환경에서도 모듈이 import 되게
        if isinstance(e, httpx.TimeoutException):
            return True
    except Exception:  # pragma: no cover
        pass
    name = type(e).__name__.lower()
    return "timeout" in name or "timed out" in str(e).lower()


# --------------------------------------------------------------------------- #
# Toobit
# --------------------------------------------------------------------------- #
class ToobitExchange(ExchangeBase):
    """Toobit USDT-M 선물 래퍼. 단일 심볼(account.symbol, 예: BTC-SWAP-USDT), 헤지 모드 전용."""

    name = "toobit"
    display_name = "Toobit"
    # ★ lot 단위 조건부 주문(STOP) 은 실계정 검증 전 → 실행기는 set_position_protection 을 쓴다.
    #   검증 후 `ToobitExchange.supports_lot_protection = True` (또는 인스턴스 속성) 로 전환한다.
    supports_lot_protection = False

    base_url: str = BASE_URL
    recv_window: int = 5000
    http_timeout_s: float = 10.0
    read_attempts: int = 2
    read_retry_sleep_s: float = 0.3
    trades_lookback_ms: int = 24 * 3600 * 1000     # executions(): 모르는 주문의 userTrades startTime = now - lookback
    trades_start_margin_ms: int = 60_000           # executions(): 아는 주문은 startTime = 생성 시각 - margin
    trades_limit: int = 500
    trades_max_pages: int = 20                     # executions(): 가득 찬 페이지 뒤 이어 읽는 상한
    protection_clear_value: str = "0"              # trading-stop 에서 '해제' 로 보내는 값 (VERIFY)
    protection_trigger_by: str = "MARK_PRICE"      # slTriggerBy / tpTriggerBy (VERIFY)

    def __init__(self, account, client=None, clock: Callable[[], int] | None = None):
        """account: config.AccountSettings (키 account.api_key/api_secret). 1단계 호환으로 Settings 도 받는다.
        client: httpx.Client 호환 객체 (`request(method, url, content=, headers=, timeout=)` → `.status_code/.json()/.text`).
        clock: ms 를 돌려주는 함수 (timestamp 서명용, 테스트 주입)."""
        self.account: AccountSettings = _as_account(account)
        if self.account.testnet:
            # 설정 검증(config.validate_account) 이 먼저 막지만, 프로그램적으로 만든 계정도 샌드박스로 오인하지 못하게 한다.
            raise ValueError("toobit has no testnet: account.testnet must be False (orders would go to the mainnet)")
        self.settings = self.account     # 하위 호환 속성명
        self.symbol = self.account.symbol
        self._clock: Callable[[], int] = clock or now_ms
        self._instr: dict | None = None
        self._lock = threading.Lock()
        self._order_created_ms: dict[str, int] = {}   # order_id -> 이 래퍼가 주문을 낸 시각 (executions 창 축소용)
        self._owns_client = client is None
        if client is not None:
            self.client = client
        else:
            import httpx  # 지연 import
            self.client = httpx.Client(base_url="", timeout=self.http_timeout_s)

    def close(self) -> None:
        if self._owns_client:
            try:
                self.client.close()
            except Exception:  # noqa: BLE001
                pass

    # ----- HTTP ------------------------------------------------------------
    def _url(self, path: str) -> str:
        return self.base_url.rstrip("/") + "/" + path.lstrip("/")

    def _send(self, method: str, url: str, body: str | None, headers: dict) -> Any:
        """1회 전송 + 응답 파싱. 네트워크/타임아웃 → ExchangeError. 응답 JSON 의 오류 코드는 (code, msg, data) 로 돌려준다."""
        try:
            resp = self.client.request(method, url, content=(body.encode("utf-8") if body is not None else None),
                                       headers=headers, timeout=self.http_timeout_s)
        except ExchangeError:
            raise
        except Exception as e:  # noqa: BLE001
            if _is_timeout_exc(e):
                raise ExchangeError("EXCHANGE_TIMEOUT", type(e).__name__) from None
            raise ExchangeError("EXCHANGE_ERROR", f"{type(e).__name__}: {str(e)[:80]}") from None
        status = _i(getattr(resp, "status_code", 0), 0)
        try:
            data = resp.json()
        except Exception:  # noqa: BLE001
            data = None
        if data is None:
            if status in _TIMEOUT_HTTP:
                raise ExchangeError("EXCHANGE_TIMEOUT", f"http {status}")
            text = str(getattr(resp, "text", "") or "")
            raise ExchangeError("EXCHANGE_ERROR", f"http {status} non-json: {text[:80]}")
        return status, data

    def _request(self, method: str, path: str, params: dict | None = None, *, private: bool, read: bool,
                 quiet_codes: set[int] | None = None) -> Any:
        """서명/전송/오류 변환. read=True 면 네트워크·타임아웃 오류에 한해 재시도.
        응답 code != 0/200 → 쓰기: ExchangeRejected(ret_code) / 읽기: ExchangeError(EXCHANGE_ERROR, ret_code)."""
        params = dict(params or {})
        method = method.upper()
        attempts = self.read_attempts if read else 1
        last: ExchangeError | None = None
        for i in range(attempts):
            # 서명은 매 시도마다 새 timestamp 로 (recvWindow 초과 방지)
            url = self._url(path)
            body: str | None = None
            headers: dict[str, str] = {}
            if private:
                if not (self.account.api_key and self.account.api_secret):
                    raise ExchangeError("EXCHANGE_ERROR", "toobit api key/secret missing")
                signed = sign_request(self.account.api_secret, method, params, self._clock(), self.recv_window)
                if signed["query"]:
                    url = url + "?" + signed["query"]
                body = signed["body"]
                headers = {
                    "X-BB-APIKEY": self.account.api_key,
                    "Content-Type": "application/x-www-form-urlencoded",
                }
            elif params:
                url = url + "?" + ccxt_urlencode(params)
            try:
                status, data = self._send(method, url, body, headers)
            except ExchangeError as e:
                log.warning("toobit %s %s failed (%s): %s", method, path, e.code, _trunc(e))
                last = e
                if read and e.code in ("EXCHANGE_TIMEOUT", "EXCHANGE_ERROR") and i + 1 < attempts:
                    time.sleep(self.read_retry_sleep_s)
                    continue
                raise
            code = _code_of(data)
            if code in _OK_CODES and status < 400:
                return data
            # 거래소 오류 (JSON {code, msg}) 또는 코드 없는 HTTP 오류
            ret_code = _i(code, 0) if code not in _OK_CODES else None
            msg = str((data.get("msg") if isinstance(data, dict) else "") or "")
            if ret_code is None and status in _TIMEOUT_HTTP:
                raise ExchangeError("EXCHANGE_TIMEOUT", f"http {status}")
            lvl = logging.DEBUG if (quiet_codes and ret_code in quiet_codes) else logging.WARNING
            log.log(lvl, "toobit %s %s rejected http=%s code=%s: %s", method, path, status, ret_code, msg[:200])
            if read:
                raise ExchangeError("EXCHANGE_ERROR", msg or f"http {status}", ret_code=ret_code)
            # 쓰기: 결과를 알 수 없는 응답은 '거부' 가 아니다 (주문이 체결됐을 수 있다 → 실행기가 조회/재확인)
            if ret_code in _UNCERTAIN_TIMEOUT_CODES:
                raise ExchangeError("EXCHANGE_TIMEOUT", msg or f"http {status}", ret_code=ret_code)
            if ret_code in _UNCERTAIN_ERROR_CODES or (ret_code is None and status >= 500):
                raise ExchangeError("EXCHANGE_ERROR", msg or f"http {status}", ret_code=ret_code)
            raise ExchangeRejected(ret_code=ret_code, message=msg or f"http {status}")
        assert last is not None  # pragma: no cover
        raise last

    def _public_get(self, path: str, params: dict | None = None) -> Any:
        return self._request("GET", path, params, private=False, read=True)

    def _private(self, method: str, path: str, params: dict | None = None, *, read: bool,
                 quiet_codes: set[int] | None = None) -> Any:
        return self._request(method, path, params, private=True, read=read, quiet_codes=quiet_codes)

    # ----- 계약 정보 / 단위 변환 ---------------------------------------------
    def instrument(self) -> dict:
        """BTC 단위 {"qty_step","min_qty","max_qty","tick"} + 참고용 {"contract_multiplier","contract_step","contract_min_qty"} (캐시)."""
        with self._lock:
            if self._instr is not None:
                return dict(self._instr)
        data = self._public_get(EP_EXCHANGE_INFO)
        entry = None
        if isinstance(data, dict):
            for key in ("contracts", "symbols"):
                for it in data.get(key) or []:
                    if isinstance(it, dict) and str(it.get("symbol") or "") == self.symbol:
                        entry = it
                        break
                if entry is not None:
                    break
        if entry is None:
            raise ExchangeError("EXCHANGE_ERROR", f"instrument {self.symbol} not found in exchangeInfo")
        mult = _f(entry.get("contractMultiplier"))
        if not mult or mult <= 0:
            log.warning("toobit exchangeInfo %s: contractMultiplier missing → 1.0 (VERIFY)", self.symbol)
            mult = 1.0
        filters = {str(f.get("filterType")): f for f in (entry.get("filters") or []) if isinstance(f, dict)}
        lot = filters.get("LOT_SIZE") or {}
        pf = filters.get("PRICE_FILTER") or {}
        c_step = _f(lot.get("stepSize"), 1.0) or 1.0
        c_min = _f(lot.get("minQty"), c_step) or c_step
        c_max = _f(lot.get("maxQty"), 1e9) or 1e9
        m = _dec(mult)
        info = {
            "qty_step": float(_dec(c_step) * m),
            "min_qty": float(_dec(c_min) * m),
            "max_qty": float(_dec(c_max) * m),
            "tick": _f(pf.get("tickSize"), 0.1) or 0.1,
            "contract_multiplier": float(mult),
            "contract_step": float(c_step),
            "contract_min_qty": float(c_min),
        }
        with self._lock:
            self._instr = info
        log.info("toobit instrument %s: %s", self.symbol, info)
        return dict(info)

    def _contracts_str(self, qty_btc: float) -> str:
        """BTC 수량 → 계약 수 문자열 (stepSize 배수로 내림, stepSize 자릿수로 포맷). 0 이 되면 거부."""
        info = self.instrument()
        mult = _dec(info["contract_multiplier"])
        step = _dec(info["contract_step"])
        contracts = _dec(qty_btc) / mult
        contracts = (contracts / step).to_integral_value(rounding=ROUND_DOWN) * step
        if contracts <= 0:
            raise ExchangeRejected(ret_code=10001, message="qty rounds to zero contracts")
        return fmt_step(float(contracts), float(step))

    def _btc(self, contracts: Any) -> float:
        """계약 수 → BTC (Decimal 곱)."""
        info = self.instrument()
        c = _f(contracts, 0.0) or 0.0
        return float(_dec(c) * _dec(info["contract_multiplier"]))

    def _price_str(self, price: float) -> str:
        info = self.instrument()
        return fmt_step(round_tick(float(price), info["tick"]), info["tick"])

    # ----- 시세 ------------------------------------------------------------
    @staticmethod
    def _pick_entry(data: Any, symbol: str) -> dict | None:
        """[{s,...}] 또는 {s,...} 응답에서 심볼 항목 하나."""
        if isinstance(data, list):
            for it in data:
                if isinstance(it, dict) and str(it.get("s") or it.get("symbol") or "") in ("", symbol):
                    return it
            return None
        if isinstance(data, dict):
            return data
        return None

    def last_price(self) -> float:
        for path, keys in ((EP_TICKER_PRICE, ("p", "price")), (EP_TICKER_PRICE_FALLBACK, ("p", "price")),
                           (EP_TICKER_24H, ("c", "lastPrice", "close"))):
            try:
                data = self._public_get(path, {"symbol": self.symbol})
            except ExchangeError as e:
                log.warning("toobit %s failed, trying next: %s", path, e.code)
                continue
            ent = self._pick_entry(data, self.symbol)
            if ent:
                for k in keys:
                    p = _f(ent.get(k))
                    if p and p > 0:
                        return p
        raise ExchangeError("EXCHANGE_ERROR", "lastPrice missing")

    def mark_price(self) -> float | None:
        """quote/v1/markPrice → 'p'|'markPrice'|'price'. 모양이 다르거나 실패하면 None (실행기는 None 허용)."""
        try:
            data = self._public_get(EP_MARK_PRICE, {"symbol": self.symbol})
        except ExchangeError as e:
            log.warning("toobit markPrice failed: %s", e.code)
            return None
        ent = self._pick_entry(data, self.symbol)
        if not ent:
            return None
        for k in ("p", "markPrice", "price", "mp"):
            p = _f(ent.get(k))
            if p and p > 0:
                return p
        return None

    # ----- 포지션 ------------------------------------------------------------
    def positions(self) -> dict[int, dict]:
        data = self._private("GET", EP_POSITIONS, {"symbol": self.symbol}, read=True)
        rows = data if isinstance(data, list) else (data.get("data") if isinstance(data, dict) else None) or []
        out: dict[int, dict] = {}
        for p in rows:
            if not isinstance(p, dict):
                continue
            if str(p.get("symbol") or self.symbol) != self.symbol:
                continue
            idx = _IDX_BY_POSITION_SIDE.get(str(p.get("side") or "").upper())
            if idx is None:
                continue
            size = self._btc(p.get("position"))
            if size <= 0:
                continue
            out[idx] = {
                "position_idx": idx,
                "size": size,
                "side": "Buy" if idx == 1 else "Sell",
                "avg_price": _f(p.get("avgPrice")),
                "mark_price": _f(p.get("markPrice")),
                "updated_time_ms": _i(p.get("updateTime") or p.get("time"), 0),
                "leverage": _i(p.get("leverage"), 0),
            }
        return out

    # ----- 계정 설정 -------------------------------------------------------
    def _setup_call(self, what: str, path: str, params: dict) -> None:
        try:
            self._private("POST", path, params, read=False)
            log.info("toobit %s applied: %s", what, params)
        except ExchangeRejected as e:
            msg = (e.message or "").lower()
            if any(w in msg for w in _ALREADY_SET_WORDS):
                log.info("toobit %s: already set (ret_code=%s)", what, e.ret_code)
                return
            raise

    def ensure_account_setup(self, position_mode: str, leverage: int, margin_mode: str) -> None:
        """Toobit 은 헤지(LONG/SHORT) 전용이라 position_mode 는 'hedge' 만 받는다 (전환 API 없음)."""
        if position_mode != "hedge":
            raise ValueError("toobit supports position_mode=hedge only")
        self._setup_call("set_leverage", EP_LEVERAGE, {"symbol": self.symbol, "leverage": str(int(leverage))})
        if margin_mode:
            if margin_mode not in ("isolated", "cross"):
                raise ValueError(f"unknown margin_mode {margin_mode}")
            self._setup_call("set_margin_type", EP_MARGIN_TYPE,
                             {"symbol": self.symbol, "marginType": "ISOLATED" if margin_mode == "isolated" else "CROSS"})

    # ----- 주문 ------------------------------------------------------------
    @staticmethod
    def _native_side(side: str, position_idx: int, reduce_only: bool) -> str:
        """(Buy|Sell, position_idx, reduce_only) → BUY_OPEN|SELL_OPEN|BUY_CLOSE|SELL_CLOSE. 조합이 맞지 않으면 ValueError."""
        idx = int(position_idx)
        if idx not in _SIDE_BY_IDX:
            raise ValueError(f"toobit supports hedge position_idx 1|2 only (got {position_idx})")
        open_side, close_side, _ = _SIDE_BY_IDX[idx]
        native = close_side if reduce_only else open_side
        want = "BUY" if str(side).lower() == "buy" else "SELL" if str(side).lower() == "sell" else None
        if want is None or not native.startswith(want):
            raise ValueError(f"side {side} does not match position_idx={idx} reduce_only={reduce_only} ({native})")
        return native

    @staticmethod
    def _order_id_of(r: Any) -> str:
        oid = str((r or {}).get("orderId") or "") if isinstance(r, dict) else ""
        if not oid:
            raise ExchangeError("EXCHANGE_ERROR", "orderId missing in response")
        return oid

    def _place(self, what: str, params: dict, order_link_id: str) -> dict:
        """주문 전송. -1141(중복 clientOrderId) 은 멱등 성공 → 기존 주문 조회."""
        t0 = int(self._clock())   # 생성 시각은 전송 전에 찍는다 (체결은 이 뒤에만 있다 → executions 창의 하한)
        try:
            r = self._private("POST", EP_ORDER, params, read=False, quiet_codes={CODE_DUPLICATE_CLIENT_ORDER_ID})
        except ExchangeRejected as e:
            if e.ret_code == CODE_DUPLICATE_CLIENT_ORDER_ID:
                existing = self.get_order(order_link_id)
                if existing and existing.get("order_id"):
                    log.info("toobit %s link=%s: duplicate clientOrderId → existing order %s", what, order_link_id,
                             existing["order_id"])
                    return {"order_id": existing["order_id"], "order_link_id": order_link_id}
            raise
        oid = self._order_id_of(r)
        with self._lock:
            self._order_created_ms[oid] = t0
            if len(self._order_created_ms) > 2000:   # 메모리 상한 (오래된 것부터 버림)
                for k in list(self._order_created_ms)[:1000]:
                    self._order_created_ms.pop(k, None)
        return {"order_id": oid, "order_link_id": order_link_id}

    def place_market(self, side: str, qty: float, position_idx: int, reduce_only: bool, order_link_id: str) -> dict:
        """시장가 = type LIMIT + priceType MARKET (ccxt create_contract_order_request 와 동일), IOC."""
        native = self._native_side(side, position_idx, reduce_only)
        params = {
            "symbol": self.symbol,
            "side": native,
            "type": "LIMIT",
            "priceType": "MARKET",
            "quantity": self._contracts_str(qty),
            "newClientOrderId": order_link_id,
            "timeInForce": "IOC",
        }
        out = self._place("market", params, order_link_id)
        log.info("toobit market %s qty=%s idx=%s ro=%s link=%s -> %s", native, qty, position_idx, reduce_only,
                 order_link_id, out["order_id"])
        return out

    def place_conditional(self, side: str, qty: float, position_idx: int, trigger_price: float,
                          trigger_direction: int, order_link_id: str, trigger_by: str) -> dict:
        """lot 단위 조건부 reduceOnly 시장가 — ★ VERIFY: Toobit STOP 주문 파라미터는 실계정 검증 전.
        supports_lot_protection=False 인 동안 실행기는 이 메서드를 호출하지 않는다.
        trigger_by / trigger_direction 은 요청에 싣지 않는다(파라미터 이름 미확인; stopPrice 와 현재가 관계로 거래소가 방향을 정한다고 가정)."""
        if int(trigger_direction) not in (1, 2):
            raise ValueError("trigger_direction must be 1 or 2")
        native = self._native_side(side, position_idx, True)
        params = {
            "symbol": self.symbol,
            "side": native,                                  # *_CLOSE (reduceOnly)
            "type": "STOP",                                  # VERIFY
            "priceType": "MARKET",                           # VERIFY
            "stopPrice": self._price_str(trigger_price),     # VERIFY
            "quantity": self._contracts_str(qty),
            "newClientOrderId": order_link_id,
        }
        out = self._place("conditional", params, order_link_id)
        log.info("toobit conditional %s qty=%s idx=%s trig=%s dir=%s by=%s link=%s -> %s (VERIFY)", native, qty,
                 position_idx, trigger_price, trigger_direction, trigger_by, order_link_id, out["order_id"])
        return out

    def cancel_order(self, order_link_id: str) -> bool:
        try:
            self._private("DELETE", EP_ORDER, {"symbol": self.symbol, "clientOrderId": order_link_id}, read=False,
                          quiet_codes=_ORDER_GONE_CODES)
            log.info("toobit cancelled link=%s", order_link_id)
            return True
        except ExchangeRejected as e:
            msg = (e.message or "").lower()
            if e.ret_code in _ORDER_GONE_CODES or any(w in msg for w in ("not exist", "not found", "already", "has been")):
                log.info("toobit cancel link=%s: already gone (ret_code=%s)", order_link_id, e.ret_code)
                return True
            raise

    # ----- 조회 ------------------------------------------------------------
    def _map_order(self, o: dict) -> dict:
        raw_side = str(o.get("side") or "").upper()
        raw_type = str(o.get("type") or "").upper()
        raw_status = str(o.get("status") or "").upper()
        status = _STATUS_MAP.get(raw_status, raw_status)
        trigger = _f(o.get("stopPrice"))
        trigger = trigger if trigger and trigger > 0 else None
        if trigger is not None and raw_type == "STOP" and status == "New":
            status = "Untriggered"   # 조건부 주문의 대기 상태를 Bybit 어휘로 (VERIFY: Toobit 의 발동 전/후 status 구분)
        avg = _f(o.get("avgPrice"))
        return {
            "order_id": str(o.get("orderId") or ""),
            "order_link_id": str(o.get("clientOrderId") or ""),
            "status": status,
            "qty": self._btc(o.get("origQty")),
            "cum_qty": self._btc(o.get("executedQty")),
            "avg_price": avg if avg and avg > 0 else None,
            "trigger_price": trigger,
            "side": "Buy" if raw_side.startswith("BUY") else "Sell" if raw_side.startswith("SELL") else "",
            "position_idx": _IDX_BY_NATIVE_SIDE.get(raw_side, 0),
            "reduce_only": raw_side.endswith("_CLOSE"),
            "native_side": raw_side,
            "native_status": raw_status,
        }

    def get_order(self, order_link_id: str) -> dict | None:
        try:
            data = self._private("GET", EP_ORDER, {"symbol": self.symbol, "clientOrderId": order_link_id}, read=True,
                                 quiet_codes=_ORDER_NOT_FOUND_CODES)
        except ExchangeError as e:
            if e.ret_code in _ORDER_NOT_FOUND_CODES:
                return None
            raise
        if isinstance(data, list):
            data = next((o for o in data if isinstance(o, dict) and str(o.get("clientOrderId")) == order_link_id), None)
        if not isinstance(data, dict) or not (data.get("orderId") or data.get("clientOrderId")):
            return None
        if data.get("clientOrderId") and str(data.get("clientOrderId")) != order_link_id:
            return None
        return self._map_order(data)

    def executions(self, order_id: str) -> list[dict]:
        """userTrades {symbol, startTime, limit} 를 orderId 로 걸러 BTC 단위 체결 목록.
        userTrades 에는 orderId 필터가 없으므로(ccxt fetch_my_trades) 창을 주문 생성 시각으로 좁히고, 한 페이지가 가득 차면
        마지막 체결 시각 + 1 부터 이어 읽는다 (최대 trades_max_pages 페이지)."""
        with self._lock:
            created = self._order_created_ms.get(str(order_id))
        now = int(self._clock())
        start = (int(created) - self.trades_start_margin_ms) if created else (now - self.trades_lookback_ms)
        out: dict[str, dict] = {}
        seen_rows = 0
        for _ in range(max(1, self.trades_max_pages)):
            params = {"symbol": self.symbol, "startTime": str(max(0, start)), "limit": str(self.trades_limit)}
            data = self._private("GET", EP_USER_TRADES, params, read=True)
            rows = data if isinstance(data, list) else (data.get("data") if isinstance(data, dict) else None) or []
            rows = [x for x in rows if isinstance(x, dict)]
            seen_rows += len(rows)
            for x in rows:
                if str(x.get("orderId") or "") != str(order_id):
                    continue
                q = self._btc(x.get("qty"))
                if q <= 0:
                    continue
                exec_id = str(x.get("id") or x.get("ticketId") or "")
                out[exec_id] = {
                    "exec_id": exec_id,
                    "qty": q,
                    "price": _f(x.get("price"), 0.0) or 0.0,
                    "exec_time_ms": _i(x.get("time"), 0),
                    "order_id": str(x.get("orderId") or order_id),
                }
            if len(rows) < self.trades_limit:
                break   # 창의 끝까지 읽었다
            newest = max((_i(x.get("time"), 0) for x in rows), default=0)
            if newest < start:
                break   # 서버가 startTime 을 무시하면 무한 루프 방지
            start = newest + 1
        else:
            log.warning("toobit executions(%s): %d rows over %d pages, window not exhausted", order_id, seen_rows,
                        self.trades_max_pages)
        lst = list(out.values())
        lst.sort(key=lambda d: (d["exec_time_ms"], d["exec_id"]))
        return lst

    def open_conditional_orders(self, position_idx: int) -> list[dict]:
        data = self._private("GET", EP_OPEN_ORDERS, {"symbol": self.symbol}, read=True)
        rows = data if isinstance(data, list) else (data.get("data") if isinstance(data, dict) else None) or []
        out: list[dict] = []
        for o in rows:
            if not isinstance(o, dict):
                continue
            trig = _f(o.get("stopPrice"))
            if not trig or trig <= 0:
                continue
            m = self._map_order(o)
            if m["position_idx"] != int(position_idx):
                continue
            out.append({
                "order_link_id": m["order_link_id"],
                "order_id": m["order_id"],
                "trigger_price": m["trigger_price"],
                "qty": m["qty"],
                "side": m["side"],
            })
        return out

    # ----- 포지션 단위 보호 (trading-stop) -----------------------------------
    def set_position_protection(self, position_idx: int, stop_loss: float | None, take_profit: float | None) -> dict:
        """POST api/v1/futures/position/trading-stop {symbol, side LONG|SHORT, stopLoss, takeProfit, slTriggerBy, tpTriggerBy}.
        None 은 해제(protection_clear_value, 기본 "0"). 한 레그(LONG/SHORT)의 포지션 전체에 적용된다.
        ★ VERIFY: 파라미터 이름/해제 값/트리거 기준은 ccxt 부착형 TP/SL 파라미터에서 유추 — 실계정 소액 검증 필요."""
        idx = int(position_idx)
        if idx not in _SIDE_BY_IDX:
            raise ValueError(f"toobit supports hedge position_idx 1|2 only (got {position_idx})")
        info = self.instrument()
        sl = round_tick(float(stop_loss), info["tick"]) if stop_loss is not None else None
        tp = round_tick(float(take_profit), info["tick"]) if take_profit is not None else None
        params = {
            "symbol": self.symbol,
            "side": _SIDE_BY_IDX[idx][2],
            "stopLoss": fmt_step(sl, info["tick"]) if sl is not None else self.protection_clear_value,       # VERIFY
            "takeProfit": fmt_step(tp, info["tick"]) if tp is not None else self.protection_clear_value,     # VERIFY
            "slTriggerBy": self.protection_trigger_by,     # VERIFY (MARK_PRICE | CONTRACT_PRICE)
            "tpTriggerBy": self.protection_trigger_by,     # VERIFY
        }
        self._private("POST", EP_TRADING_STOP, params, read=False)
        log.info("toobit position protection idx=%s sl=%s tp=%s applied", idx, sl, tp)
        return {"position_idx": idx, "stop_loss": sl, "take_profit": tp}
