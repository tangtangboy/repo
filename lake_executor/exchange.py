"""거래소 래퍼 (ARCHITECTURE.md §4, ARCHITECTURE_MULTI_EXCHANGE.md §3).

모든 구현이 같은 인터페이스(`ExchangeBase`)를 가진다. 생성자는 `config.AccountSettings` 하나를 받는다.
  - BybitExchange  : pybit `unified_trading.HTTP` 를 감싼 실거래소 래퍼 (linear, 단일 심볼).
  - PaperExchange  : 메모리 시뮬레이터 (테스트 / `test_simulate_fills`). 계정마다 하나씩, display_name 은 계정 거래소.
  - OkxExchange / ToobitExchange : 별도 모듈(exchange_okx / exchange_toobit). `build_exchange` 가 지연 import 한다.

공통 추가(2단계)
  - `display_name` ("Bybit"|"OKX"|"Toobit"), `supports_lot_protection` (lot 단위 조건부 주문 가능 여부),
    `set_position_protection(position_idx, stop_loss, take_profit)` (포지션 단위 TP/SL; lot 보호를 못 쓰는 거래소용),
    `account` (AccountSettings).
  - 수량은 항상 BTC 단위로 주고받는다. 계약 수 변환은 각 래퍼 안에서만 한다.

규칙
  - 수량·가격은 float 로 받고, 문자열 변환은 내부에서 `util.fmt_step` 으로만 한다.
  - 거래소 예외 원문은 WARNING 로그에 200자 절단으로만 남기고, 호출자에게는
    `ExchangeError.code` / `ExchangeRejected.ret_code` 만 전달한다.
  - import 시 부작용 없음 (HTTP 세션은 실거래소 래퍼 생성 시에만 만든다).
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable

from .config import EXCHANGE_DISPLAY, AccountSettings, Settings
from .util import floor_step, fmt_step, now_ms, round_tick

log = logging.getLogger("lake_executor.exchange")


def _as_account(obj) -> AccountSettings:
    """AccountSettings 또는 1단계 호출 호환용 Settings(→ accounts[0]) 를 AccountSettings 로."""
    if isinstance(obj, AccountSettings):
        return obj
    if isinstance(obj, Settings):
        if not obj.accounts:
            raise ValueError("settings has no accounts")
        return obj.accounts[0]
    accts = getattr(obj, "accounts", None)   # Settings 호환 덕타이핑 (테스트 더블)
    if accts:
        return _as_account(accts[0])
    raise TypeError(f"expected AccountSettings or Settings, got {type(obj).__name__}")

_LOG_TRUNC = 200

# Bybit 가 "이미 그 상태" 라는 뜻으로 돌려주는 코드들 (설정 변경 시 무시)
_NOT_MODIFIED_CODES = {110043, 110026, 110025}
# 취소 요청 시 "이미 없음/체결됨/취소됨" 으로 보는 코드들 → cancel_order 는 True
_CANCEL_GONE_CODES = {110001}
_CANCEL_GONE_WORDS = ("not exist", "too late", "already", "has been filled", "has been cancel")


# --------------------------------------------------------------------------- #
# 예외
# --------------------------------------------------------------------------- #
class ExchangeError(Exception):
    """거래소 호출 실패. code ∈ EXCHANGE_ERROR | EXCHANGE_TIMEOUT (| EXCHANGE_REJECTED, 하위 클래스)."""

    code: str = "EXCHANGE_ERROR"

    def __init__(self, code: str = "EXCHANGE_ERROR", message: str = "", ret_code: int | None = None):
        self.code = code
        self.ret_code = ret_code
        self.message = message
        super().__init__(f"{code}" + (f" ret_code={ret_code}" if ret_code is not None else "") + (f": {message}" if message else ""))


class ExchangeRejected(ExchangeError):
    """거래소가 요청을 거부(retCode != 0). ret_code 에 Bybit retCode."""

    code = "EXCHANGE_REJECTED"

    def __init__(self, ret_code: int | None = None, message: str = ""):
        super().__init__("EXCHANGE_REJECTED", message, ret_code)


# --------------------------------------------------------------------------- #
# 공통 인터페이스
# --------------------------------------------------------------------------- #
class ExchangeBase:
    name: str = "base"                      # 구현 종류: bybit | okx | toobit | paper
    display_name: str = "Bybit"             # 회신 본문 exchange 값: Bybit | OKX | Toobit
    supports_lot_protection: bool = True    # False 면 실행기는 place_conditional 대신 set_position_protection 을 쓴다
    account: AccountSettings | None = None

    def set_position_protection(self, position_idx: int, stop_loss: float | None, take_profit: float | None) -> dict:
        """포지션(레그) 단위 TP/SL 설정. None 은 해제. 한 레그의 모든 lot 에 같은 값이 적용된다.
        → {"position_idx","stop_loss","take_profit"}. lot 보호를 지원하는 거래소는 구현하지 않는다."""
        raise NotImplementedError(f"{self.name} does not implement position-level protection")

    def instrument(self) -> dict:
        """{"qty_step","min_qty","max_qty","tick"} (캐시)."""
        raise NotImplementedError

    def last_price(self) -> float:
        raise NotImplementedError

    def mark_price(self) -> float | None:
        raise NotImplementedError

    def ensure_account_setup(self, position_mode: str, leverage: int, margin_mode: str) -> None:
        raise NotImplementedError

    def positions(self) -> dict[int, dict]:
        """{position_idx: {"size","side","avg_price","mark_price","updated_time_ms","position_idx"}} — size>0 만."""
        raise NotImplementedError

    def place_market(self, side: str, qty: float, position_idx: int, reduce_only: bool, order_link_id: str) -> dict:
        """시장가(IOC). → {"order_id","order_link_id"}"""
        raise NotImplementedError

    def place_conditional(self, side: str, qty: float, position_idx: int, trigger_price: float,
                          trigger_direction: int, order_link_id: str, trigger_by: str) -> dict:
        """조건부 reduceOnly 시장가(보호주문). trigger_direction 1=상승 돌파, 2=하락 돌파. → {"order_id","order_link_id"}"""
        raise NotImplementedError

    def cancel_order(self, order_link_id: str) -> bool:
        """취소됨 / 이미 없음(체결·취소 완료) → True."""
        raise NotImplementedError

    def get_order(self, order_link_id: str) -> dict | None:
        """{"order_id","order_link_id","status","qty","cum_qty","avg_price","trigger_price"} | None.
        status ∈ New|PartiallyFilled|Filled|Cancelled|Rejected|Untriggered|Triggered|Deactivated"""
        raise NotImplementedError

    def executions(self, order_id: str) -> list[dict]:
        """[{"exec_id","qty","price","exec_time_ms","order_id"}]"""
        raise NotImplementedError

    def open_conditional_orders(self, position_idx: int) -> list[dict]:
        """[{"order_link_id","order_id","trigger_price","qty","side"}]"""
        raise NotImplementedError


# --------------------------------------------------------------------------- #
# 헬퍼
# --------------------------------------------------------------------------- #
def _f(v: Any, default: float | None = None) -> float | None:
    """Bybit 숫자 문자열 → float. ""/None/비정상 → default."""
    if v is None or v == "":
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _i(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return default


def _trunc(e: BaseException) -> str:
    s = str(e).replace("\n", " ")
    return s[:_LOG_TRUNC]


def _result_list(resp: Any) -> list[dict]:
    if not isinstance(resp, dict):
        return []
    res = resp.get("result") or {}
    lst = res.get("list") if isinstance(res, dict) else None
    return list(lst) if isinstance(lst, list) else []


_MARGIN_MODE_MAP = {"isolated": "ISOLATED_MARGIN", "cross": "REGULAR_MARGIN"}


# --------------------------------------------------------------------------- #
# Bybit
# --------------------------------------------------------------------------- #
class BybitExchange(ExchangeBase):
    """Bybit v5 (pybit) 래퍼. 단일 category/symbol."""

    http_timeout_s: int = 10
    read_attempts: int = 2
    read_retry_sleep_s: float = 0.3

    name = "bybit"
    display_name = "Bybit"
    supports_lot_protection = True

    def __init__(self, account, http=None):
        """account: config.AccountSettings (키는 account.api_key/api_secret). 1단계 호환으로 Settings 도 받는다."""
        self.account = _as_account(account)
        self.settings = self.account     # 하위 호환 속성명 (allowed_position_idx 등 같은 메서드를 가진다)
        self.category = self.account.category
        self.symbol = self.account.symbol
        self._instr: dict | None = None
        self._lock = threading.Lock()
        if http is not None:
            self.http = http  # 테스트 주입용 (pybit HTTP 호환 객체)
        else:
            from pybit.unified_trading import HTTP  # 지연 import: 모듈 import 부작용 최소화
            kw: dict[str, Any] = {"testnet": bool(self.account.testnet), "timeout": self.http_timeout_s}
            if self.account.api_key and self.account.api_secret:
                kw["api_key"] = self.account.api_key
                kw["api_secret"] = self.account.api_secret
            self.http = HTTP(**kw)

    # ----- 예외 매핑 -------------------------------------------------------
    @staticmethod
    def _classify(e: BaseException) -> ExchangeError:
        """pybit / requests 예외 → ExchangeError 계열. 원문은 message 에만(로그는 호출부에서 절단)."""
        try:
            from pybit.exceptions import FailedRequestError, InvalidRequestError
        except Exception:  # pragma: no cover - pybit 미설치 환경
            InvalidRequestError = FailedRequestError = ()  # type: ignore[assignment]
        if InvalidRequestError and isinstance(e, InvalidRequestError):
            # retCode != 0 (주문 거부, 파라미터 오류, 권한 등)
            return ExchangeRejected(ret_code=_i(getattr(e, "status_code", None), 0) or None,
                                    message=str(getattr(e, "message", "") or ""))
        if FailedRequestError and isinstance(e, FailedRequestError):
            sc = _i(getattr(e, "status_code", None), 0)
            msg = str(getattr(e, "message", "") or "")
            if sc in (408, 504) or "timeout" in msg.lower() or "timed out" in msg.lower():
                return ExchangeError("EXCHANGE_TIMEOUT", msg)
            return ExchangeError("EXCHANGE_ERROR", msg)
        name = type(e).__name__.lower()
        if "timeout" in name or "timed out" in str(e).lower():
            return ExchangeError("EXCHANGE_TIMEOUT", type(e).__name__)
        return ExchangeError("EXCHANGE_ERROR", f"{type(e).__name__}: {str(e)[:80]}")

    def _call(self, name: str, fn: Callable[..., Any], *, read: bool, quiet_codes: set[int] | None = None, **kw) -> Any:
        """pybit 호출 래퍼. read=True 면 네트워크 오류에 한해 재시도(총 read_attempts 회).
        retCode 거부: 쓰기 → ExchangeRejected / 읽기 → ExchangeError(EXCHANGE_ERROR, ret_code).
        quiet_codes 에 든 retCode 는 호출부가 무해하게 처리하므로 DEBUG 로만 남긴다."""
        attempts = self.read_attempts if read else 1
        last: ExchangeError | None = None
        for i in range(attempts):
            try:
                return fn(**kw)
            except ExchangeError:
                raise
            except Exception as e:  # noqa: BLE001 - 모든 거래소/네트워크 예외를 코드로 변환
                err = self._classify(e)
                lvl = logging.DEBUG if (quiet_codes and err.ret_code in quiet_codes) else logging.WARNING
                log.log(lvl, "bybit %s failed (%s ret_code=%s): %s", name, err.code, err.ret_code, _trunc(e))
                if isinstance(err, ExchangeRejected):
                    if read:
                        # 읽기 호출의 retCode 오류는 '주문 거부' 가 아니므로 일반 오류로 전달
                        raise ExchangeError("EXCHANGE_ERROR", err.message, ret_code=err.ret_code) from None
                    raise err from None
                last = err
                if read and i + 1 < attempts:
                    time.sleep(self.read_retry_sleep_s)
                    continue
                raise err from None
        assert last is not None  # pragma: no cover
        raise last

    # ----- 읽기 ------------------------------------------------------------
    def instrument(self) -> dict:
        with self._lock:
            if self._instr is not None:
                return dict(self._instr)
        r = self._call("get_instruments_info", self.http.get_instruments_info, read=True,
                       category=self.category, symbol=self.symbol)
        lst = _result_list(r)
        if not lst:
            raise ExchangeError("EXCHANGE_ERROR", f"instrument {self.symbol} not found")
        item = lst[0]
        lot = item.get("lotSizeFilter") or {}
        pf = item.get("priceFilter") or {}
        info = {
            "qty_step": _f(lot.get("qtyStep"), 0.001),
            "min_qty": _f(lot.get("minOrderQty"), 0.001),
            "max_qty": _f(lot.get("maxOrderQty"), 100.0),
            "tick": _f(pf.get("tickSize"), 0.1),
        }
        with self._lock:
            self._instr = info
        log.info("bybit instrument %s: %s", self.symbol, info)
        return dict(info)

    def _ticker(self) -> dict:
        r = self._call("get_tickers", self.http.get_tickers, read=True, category=self.category, symbol=self.symbol)
        lst = _result_list(r)
        if not lst:
            raise ExchangeError("EXCHANGE_ERROR", f"ticker {self.symbol} not found")
        return lst[0]

    def last_price(self) -> float:
        p = _f(self._ticker().get("lastPrice"))
        if p is None or p <= 0:
            raise ExchangeError("EXCHANGE_ERROR", "lastPrice missing")
        return p

    def mark_price(self) -> float | None:
        p = _f(self._ticker().get("markPrice"))
        return p if p and p > 0 else None

    def positions(self) -> dict[int, dict]:
        r = self._call("get_positions", self.http.get_positions, read=True, category=self.category, symbol=self.symbol)
        out: dict[int, dict] = {}
        for p in _result_list(r):
            size = _f(p.get("size"), 0.0) or 0.0
            if size <= 0:
                continue
            idx = _i(p.get("positionIdx"), 0)
            out[idx] = {
                "position_idx": idx,
                "size": size,
                "side": p.get("side") or "",
                "avg_price": _f(p.get("avgPrice")),
                "mark_price": _f(p.get("markPrice")),
                "updated_time_ms": _i(p.get("updatedTime"), 0),
            }
        return out

    # ----- 계정 설정 -------------------------------------------------------
    def _setup_call(self, name: str, fn: Callable[..., Any], **kw) -> None:
        try:
            self._call(name, fn, read=False, quiet_codes=_NOT_MODIFIED_CODES, **kw)
            log.info("bybit %s applied", name)
        except ExchangeRejected as e:
            msg = (e.message or "").lower()
            if (e.ret_code in _NOT_MODIFIED_CODES) or ("not modified" in msg) or ("not been modified" in msg):
                log.info("bybit %s: already set (ret_code=%s)", name, e.ret_code)
                return
            raise

    def ensure_account_setup(self, position_mode: str, leverage: int, margin_mode: str) -> None:
        if position_mode not in ("hedge", "one_way"):
            raise ValueError("position_mode must be hedge | one_way")
        self._setup_call("switch_position_mode", self.http.switch_position_mode,
                         category=self.category, symbol=self.symbol, mode=3 if position_mode == "hedge" else 0)
        lev = str(int(leverage))
        self._setup_call("set_leverage", self.http.set_leverage,
                         category=self.category, symbol=self.symbol, buyLeverage=lev, sellLeverage=lev)
        if margin_mode:
            m = _MARGIN_MODE_MAP.get(margin_mode)
            if not m:
                raise ValueError(f"unknown margin_mode {margin_mode}")
            self._setup_call("set_margin_mode", self.http.set_margin_mode, setMarginMode=m)

    # ----- 주문 ------------------------------------------------------------
    def _qty_str(self, qty: float) -> str:
        info = self.instrument()
        q = floor_step(float(qty), info["qty_step"])
        if q <= 0:
            raise ExchangeRejected(ret_code=10001, message="qty rounds to zero")
        return fmt_step(q, info["qty_step"])

    def _price_str(self, price: float) -> str:
        info = self.instrument()
        return fmt_step(round_tick(float(price), info["tick"]), info["tick"])

    @staticmethod
    def _order_id_of(r: Any) -> str:
        res = (r or {}).get("result") or {}
        oid = str(res.get("orderId") or "")
        if not oid:
            raise ExchangeError("EXCHANGE_ERROR", "orderId missing in response")
        return oid

    def place_market(self, side: str, qty: float, position_idx: int, reduce_only: bool, order_link_id: str) -> dict:
        r = self._call("place_order", self.http.place_order, read=False,
                       category=self.category, symbol=self.symbol, side=side, orderType="Market",
                       qty=self._qty_str(qty), positionIdx=int(position_idx), reduceOnly=bool(reduce_only),
                       orderLinkId=order_link_id, timeInForce="IOC")
        oid = self._order_id_of(r)
        log.info("bybit market %s qty=%s idx=%s ro=%s link=%s -> %s", side, qty, position_idx, reduce_only, order_link_id, oid)
        return {"order_id": oid, "order_link_id": order_link_id}

    def place_conditional(self, side: str, qty: float, position_idx: int, trigger_price: float,
                          trigger_direction: int, order_link_id: str, trigger_by: str) -> dict:
        if int(trigger_direction) not in (1, 2):
            raise ValueError("trigger_direction must be 1 or 2")
        r = self._call("place_order", self.http.place_order, read=False,
                       category=self.category, symbol=self.symbol, side=side, orderType="Market",
                       qty=self._qty_str(qty), positionIdx=int(position_idx),
                       triggerPrice=self._price_str(trigger_price), triggerDirection=int(trigger_direction),
                       triggerBy=trigger_by, reduceOnly=True, closeOnTrigger=True, orderLinkId=order_link_id)
        oid = self._order_id_of(r)
        log.info("bybit conditional %s qty=%s idx=%s trig=%s dir=%s link=%s -> %s",
                 side, qty, position_idx, trigger_price, trigger_direction, order_link_id, oid)
        return {"order_id": oid, "order_link_id": order_link_id}

    def cancel_order(self, order_link_id: str) -> bool:
        try:
            self._call("cancel_order", self.http.cancel_order, read=False, quiet_codes=_CANCEL_GONE_CODES,
                       category=self.category, symbol=self.symbol, orderLinkId=order_link_id)
            log.info("bybit cancelled link=%s", order_link_id)
            return True
        except ExchangeRejected as e:
            msg = (e.message or "").lower()
            if e.ret_code in _CANCEL_GONE_CODES or any(w in msg for w in _CANCEL_GONE_WORDS):
                log.info("bybit cancel link=%s: already gone (ret_code=%s)", order_link_id, e.ret_code)
                return True
            raise

    # ----- 조회 ------------------------------------------------------------
    @staticmethod
    def _map_order(o: dict) -> dict:
        return {
            "order_id": str(o.get("orderId") or ""),
            "order_link_id": str(o.get("orderLinkId") or ""),
            "status": str(o.get("orderStatus") or ""),
            "qty": _f(o.get("qty"), 0.0) or 0.0,
            "cum_qty": _f(o.get("cumExecQty"), 0.0) or 0.0,
            "avg_price": _f(o.get("avgPrice")) or None,
            "trigger_price": _f(o.get("triggerPrice")) or None,
            "side": str(o.get("side") or ""),
            "position_idx": _i(o.get("positionIdx"), 0),
            "reduce_only": bool(o.get("reduceOnly", False)),
        }

    def get_order(self, order_link_id: str) -> dict | None:
        """열린 주문(일반 → 조건부 StopOrder) → 주문 이력 순으로 조회. 셋 다 없을 때만 None.
        (v5 realtime 은 orderFilter 기본값이 일반 주문이라 조건부 보호주문은 StopOrder 로 한 번 더 본다.)"""
        r = self._call("get_open_orders", self.http.get_open_orders, read=True,
                       category=self.category, symbol=self.symbol, orderLinkId=order_link_id)
        lst = [o for o in _result_list(r) if o.get("orderLinkId") == order_link_id]
        if not lst:
            r = self._call("get_open_orders", self.http.get_open_orders, read=True,
                           category=self.category, symbol=self.symbol, orderLinkId=order_link_id,
                           orderFilter="StopOrder")
            lst = [o for o in _result_list(r) if o.get("orderLinkId") == order_link_id]
        if not lst:
            r = self._call("get_order_history", self.http.get_order_history, read=True,
                           category=self.category, symbol=self.symbol, orderLinkId=order_link_id)
            lst = [o for o in _result_list(r) if o.get("orderLinkId") == order_link_id]
        if not lst:
            return None
        return self._map_order(lst[0])

    def executions(self, order_id: str) -> list[dict]:
        r = self._call("get_executions", self.http.get_executions, read=True,
                       category=self.category, symbol=self.symbol, orderId=order_id, limit=100)
        out: list[dict] = []
        for x in _result_list(r):
            if x.get("orderId") and str(x.get("orderId")) != str(order_id):
                continue
            et = x.get("execType")
            if et and et != "Trade":
                continue  # Funding/ADL/BustTrade 등은 체결이 아님
            q = _f(x.get("execQty"), 0.0) or 0.0
            if q <= 0:
                continue
            out.append({
                "exec_id": str(x.get("execId") or ""),
                "qty": q,
                "price": _f(x.get("execPrice"), 0.0) or 0.0,
                "exec_time_ms": _i(x.get("execTime"), 0),
                "order_id": str(x.get("orderId") or order_id),
            })
        out.sort(key=lambda d: (d["exec_time_ms"], d["exec_id"]))
        return out

    def open_conditional_orders(self, position_idx: int) -> list[dict]:
        out: list[dict] = []
        cursor = ""
        for _ in range(10):  # 페이지 안전 상한
            kw: dict[str, Any] = dict(category=self.category, symbol=self.symbol, orderFilter="StopOrder", limit=50)
            if cursor:
                kw["cursor"] = cursor
            r = self._call("get_open_orders", self.http.get_open_orders, read=True, **kw)
            for o in _result_list(r):
                if _i(o.get("positionIdx"), 0) != int(position_idx):
                    continue
                out.append({
                    "order_link_id": str(o.get("orderLinkId") or ""),
                    "order_id": str(o.get("orderId") or ""),
                    "trigger_price": _f(o.get("triggerPrice")),
                    "qty": _f(o.get("qty"), 0.0) or 0.0,
                    "side": str(o.get("side") or ""),
                })
            cursor = str(((r or {}).get("result") or {}).get("nextPageCursor") or "")
            if not cursor:
                break
        return out


# --------------------------------------------------------------------------- #
# Paper (메모리 시뮬레이터)
# --------------------------------------------------------------------------- #
_DEFAULT_PAPER_INSTRUMENT = {"qty_step": 0.001, "min_qty": 0.001, "max_qty": 100.0, "tick": 0.1}
# 거래소별 Paper 기본 instrument (BTC 단위) — test(simulate_fills) 가 live 의 최소 수량 규칙을 그대로 흉내 내도록
# (OKX: ctVal 0.01 × lotSz 1 = 0.01 BTC / Toobit: contractMultiplier 0.001 × stepSize 1 = 0.001 BTC / Bybit: qtyStep 0.001).
PAPER_INSTRUMENTS: dict[str, dict] = {
    "bybit": dict(_DEFAULT_PAPER_INSTRUMENT),
    "okx": {"qty_step": 0.01, "min_qty": 0.01, "max_qty": 100.0, "tick": 0.1},
    "toobit": {"qty_step": 0.001, "min_qty": 0.001, "max_qty": 100.0, "tick": 0.1},
}


class PaperExchange(ExchangeBase):
    """결정적 메모리 시뮬레이터.

    - 시장가: 현재가에 즉시 전량 체결 (order id "porder-N", exec id "pexec-N").
    - 조건부: `set_price(p)` 에서 trigger_direction 1 → p >= trigger, 2 → p <= trigger 이면 체결.
    - reduceOnly 는 포지션 크기만큼만 체결(포지션 없으면 110017 거부). 추가 시 평균가 가중 갱신.
    - `lot_protection=False` 면 lot 보호(조건부 주문)를 지원하지 않는 거래소(Toobit 등)를 흉내 낸다:
      `place_conditional` 은 NotImplementedError, `set_position_protection(idx, sl, tp)` 가 포지션 단위 SL/TP 를 저장하고
      `set_price` 에서 조건을 만족하면 그 idx 포지션 **전량**을 reduceOnly 시장가(주문 "pprot-N", link "pp:<idx>:<n>")
      로 닫는다. 발동 기록은 `protection_events` 에 쌓인다.
    - 스레드 안전(RLock). 상태는 `reset()` 으로 초기화.
    """

    name = "paper"

    def __init__(self, account, price: float = 85000.0, instrument: dict | None = None, clock=None, *,
                 lot_protection: bool = True, id_tag: str = ""):
        """account: config.AccountSettings (display_name 은 account.exchange 를 따른다). 1단계 호환으로 Settings 도 받는다.
        id_tag: 주문/체결 ID 접두어 ("porder-<tag>-N", "pexec-<tag>-N"). 원장이 영구(Postgres/SQLite 파일) 이므로 운영(serve) 에서는
        프로세스마다 다른 태그를 줘야 재시작 뒤 'pexec-1' 이 이전 프로세스의 체결과 (account, exec_id) 로 충돌해 무시되지 않는다
        (build_exchange 가 넣는다). 테스트는 빈 태그(결정적 ID) 를 쓴다."""
        self.account = _as_account(account)
        self._id_tag = f"{id_tag}-" if id_tag else ""
        self.settings = self.account     # 하위 호환 속성명
        self.display_name = EXCHANGE_DISPLAY.get(self.account.exchange, "Bybit")
        self.supports_lot_protection = bool(lot_protection)
        self.category = self.account.category
        self.symbol = self.account.symbol
        self._instr = dict(instrument or _DEFAULT_PAPER_INSTRUMENT)
        self._clock: Callable[[], int] = clock or now_ms
        self._lock = threading.RLock()
        self._price = float(price)
        self._mark: float | None = None
        self._positions: dict[int, dict] = {}
        self._orders: dict[str, dict] = {}          # order_link_id -> order
        self._by_id: dict[str, str] = {}            # order_id -> order_link_id
        self._execs: dict[str, list[dict]] = {}     # order_id -> executions
        self._order_seq = 0
        self._exec_seq = 0
        self._prot_seq = 0
        # 성과 추적 (test 모드 성과 지표용): 시드 + 실현손익(감소 체결마다 평균단가 기준) + 미실현
        self.seed_usdt = float(getattr(self.account, "seed_usdt", None) or 10000.0)
        self.realized_pnl = 0.0
        self.closed_pnls: list[dict] = []
        self._pnl_seq = 0
        self.account_setup: dict | None = None
        self.position_protections: dict[int, dict] = {}   # position_idx -> {"stop_loss","take_profit"}
        self.protection_events: list[dict] = []           # 포지션 단위 보호 발동 기록

    # ----- 테스트/운영 편의 ---------------------------------------------------
    def reset(self, price: float | None = None) -> None:
        with self._lock:
            if price is not None:
                self._price = float(price)
            self._mark = None
            self._positions.clear()
            self._orders.clear()
            self._by_id.clear()
            self._execs.clear()
            self._order_seq = 0
            self._exec_seq = 0
            self._prot_seq = 0
            self.realized_pnl = 0.0
            self.closed_pnls = []
            self._pnl_seq = 0
            self.position_protections.clear()
            self.protection_events.clear()

    def set_price(self, price: float, mark: float | None = None) -> list[dict]:
        """시세 갱신. 트리거된 조건부 주문을 체결하고 그 주문(get_order 형식) 목록을 돌려준다."""
        p = float(price)
        if p <= 0:
            raise ValueError("price must be > 0")
        with self._lock:
            self._price = p
            self._mark = float(mark) if mark is not None else None
            fired = self._trigger_conditionals(p)
            fired.extend(self._trigger_position_protections(p))
            return fired

    def all_orders(self) -> list[dict]:
        with self._lock:
            return [self._out_order(o) for o in self._orders.values()]

    # ----- 인터페이스 ---------------------------------------------------------
    def instrument(self) -> dict:
        return dict(self._instr)

    def last_price(self) -> float:
        with self._lock:
            return self._price

    def mark_price(self) -> float | None:
        with self._lock:
            return self._mark if self._mark is not None else self._price

    def ensure_account_setup(self, position_mode: str, leverage: int, margin_mode: str) -> None:
        if position_mode not in ("hedge", "one_way"):
            raise ValueError("position_mode must be hedge | one_way")
        if margin_mode and margin_mode not in _MARGIN_MODE_MAP:
            raise ValueError(f"unknown margin_mode {margin_mode}")
        with self._lock:
            self.account_setup = {"position_mode": position_mode, "leverage": int(leverage), "margin_mode": margin_mode}

    def positions(self) -> dict[int, dict]:
        with self._lock:
            out: dict[int, dict] = {}
            for idx, p in self._positions.items():
                if p["size"] <= 0:
                    continue
                out[idx] = {
                    "position_idx": idx,
                    "size": p["size"],
                    "side": p["side"],
                    "avg_price": p["avg_price"],
                    "mark_price": self._mark if self._mark is not None else self._price,
                    "updated_time_ms": p["updated_time_ms"],
                }
            return out

    def place_market(self, side: str, qty: float, position_idx: int, reduce_only: bool, order_link_id: str) -> dict:
        with self._lock:
            q = self._validate_new_order(side, qty, position_idx, order_link_id)
            o = self._new_order(order_link_id, side, q, int(position_idx), bool(reduce_only), kind="market")
            self._execute(o, self._price)
            return {"order_id": o["order_id"], "order_link_id": order_link_id}

    def place_conditional(self, side: str, qty: float, position_idx: int, trigger_price: float,
                          trigger_direction: int, order_link_id: str, trigger_by: str) -> dict:
        if not self.supports_lot_protection:
            raise NotImplementedError("this paper exchange simulates an exchange without lot-level conditional orders")
        if int(trigger_direction) not in (1, 2):
            raise ValueError("trigger_direction must be 1 or 2")
        tp = round_tick(float(trigger_price), self._instr["tick"])
        if tp <= 0:
            raise ExchangeRejected(ret_code=10001, message="trigger price must be > 0")
        with self._lock:
            q = self._validate_new_order(side, qty, position_idx, order_link_id)
            # Bybit: 현재가 기준으로 이미 트리거 조건을 만족하는 가격은 거부 (expected rising/falling)
            if (int(trigger_direction) == 1 and self._price >= tp) or (int(trigger_direction) == 2 and self._price <= tp):
                raise ExchangeRejected(ret_code=10001, message="trigger price already crossed by current price")
            o = self._new_order(order_link_id, side, q, int(position_idx), True, kind="conditional",
                                trigger_price=tp, trigger_direction=int(trigger_direction), trigger_by=trigger_by)
            o["status"] = "Untriggered"
            return {"order_id": o["order_id"], "order_link_id": order_link_id}

    def cancel_order(self, order_link_id: str) -> bool:
        with self._lock:
            o = self._orders.get(order_link_id)
            if o is None:
                return True
            if o["status"] in ("Untriggered", "New", "PartiallyFilled"):
                o["status"] = "Cancelled"
                o["updated_time_ms"] = self._clock()
            return True

    def get_order(self, order_link_id: str) -> dict | None:
        with self._lock:
            o = self._orders.get(order_link_id)
            return self._out_order(o) if o else None

    def executions(self, order_id: str) -> list[dict]:
        with self._lock:
            return [dict(x) for x in self._execs.get(order_id, [])]

    def open_conditional_orders(self, position_idx: int) -> list[dict]:
        with self._lock:
            return [
                {"order_link_id": o["order_link_id"], "order_id": o["order_id"],
                 "trigger_price": o["trigger_price"], "qty": o["qty"], "side": o["side"]}
                for o in self._orders.values()
                if o["kind"] == "conditional" and o["status"] == "Untriggered" and o["position_idx"] == int(position_idx)
            ]

    # ----- 포지션 단위 보호 (lot_protection=False 시뮬레이션) --------------------
    def set_position_protection(self, position_idx: int, stop_loss: float | None, take_profit: float | None) -> dict:
        """idx 포지션의 SL/TP 를 설정(None = 해제). 둘 다 None 이면 포지션이 없어도 해제만 한다.
        Bybit 조건부와 같은 규칙으로, 현재가가 이미 지난 가격은 10001 'already crossed' 로 거부한다."""
        if self.supports_lot_protection:
            # lot 보호를 지원하는 거래소(Bybit 시뮬레이션)에서는 호출 자체가 실행기 버그 → Bybit 래퍼와 같은 예외
            raise NotImplementedError("paper exchange with lot protection does not implement position-level protection")
        idx = int(position_idx)
        if idx not in self._allowed_idx():
            raise ExchangeRejected(ret_code=10001, message="position idx not match position mode")
        tick = self._instr["tick"]
        sl = round_tick(float(stop_loss), tick) if stop_loss is not None else None
        tp = round_tick(float(take_profit), tick) if take_profit is not None else None
        with self._lock:
            if sl is None and tp is None:
                self.position_protections.pop(idx, None)
                return {"position_idx": idx, "stop_loss": None, "take_profit": None}
            pos = self._positions.get(idx)
            if pos is None or pos["size"] <= 0:
                raise ExchangeRejected(ret_code=110017, message="no position to protect")
            long = pos["side"] == "Buy"
            if (sl is not None and sl <= 0) or (tp is not None and tp <= 0):
                raise ExchangeRejected(ret_code=10001, message="protection price must be > 0")
            p = self._price
            if sl is not None and ((long and p <= sl) or (not long and p >= sl)):
                raise ExchangeRejected(ret_code=10001, message="stop loss already crossed by current price")
            if tp is not None and ((long and p >= tp) or (not long and p <= tp)):
                raise ExchangeRejected(ret_code=10001, message="take profit already crossed by current price")
            self.position_protections[idx] = {"stop_loss": sl, "take_profit": tp}
            return {"position_idx": idx, "stop_loss": sl, "take_profit": tp}

    def get_position_protection(self, position_idx: int) -> dict | None:
        with self._lock:
            v = self.position_protections.get(int(position_idx))
            return dict(v) if v else None

    def _trigger_position_protections(self, p: float) -> list[dict]:
        fired: list[dict] = []
        for idx in sorted(list(self.position_protections)):
            prot = self.position_protections[idx]
            pos = self._positions.get(idx)
            if pos is None or pos["size"] <= 0:
                del self.position_protections[idx]
                continue
            long = pos["side"] == "Buy"
            sl, tp = prot.get("stop_loss"), prot.get("take_profit")
            kind = None
            if sl is not None and ((long and p <= sl) or (not long and p >= sl)):
                kind = "sl"
            elif tp is not None and ((long and p >= tp) or (not long and p <= tp)):
                kind = "tp"
            if kind is None:
                continue
            self._prot_seq += 1
            link = f"pp:{idx}:{self._prot_seq}"
            o = self._new_order(link, "Sell" if long else "Buy", pos["size"], idx, True, kind="position_protection",
                                trigger_price=sl if kind == "sl" else tp)
            self._execute(o, p)   # 전량 reduceOnly 체결 → 포지션 소멸
            del self.position_protections[idx]
            out = self._out_order(o)
            self.protection_events.append({"position_idx": idx, "kind": kind, "price": p, "qty": o["qty"],
                                           "order_id": o["order_id"], "order_link_id": link, "exec_time_ms": o["updated_time_ms"]})
            fired.append(out)
        return fired

    # ----- 내부 --------------------------------------------------------------
    def _allowed_idx(self) -> set[int]:
        try:
            return set(self.account.allowed_position_idx())
        except Exception:  # noqa: BLE001
            return {0, 1, 2}

    def _validate_new_order(self, side: str, qty: float, position_idx: int, order_link_id: str) -> float:
        if side not in ("Buy", "Sell"):
            raise ExchangeRejected(ret_code=10001, message="side must be Buy|Sell")
        if int(position_idx) not in self._allowed_idx():
            raise ExchangeRejected(ret_code=10001, message="position idx not match position mode")
        if not order_link_id:
            raise ExchangeRejected(ret_code=10001, message="orderLinkId required")
        if order_link_id in self._orders:
            raise ExchangeRejected(ret_code=110072, message="OrderLinkedID is duplicate")
        q = floor_step(float(qty), self._instr["qty_step"])
        if q < self._instr["min_qty"] or q <= 0:
            raise ExchangeRejected(ret_code=10001, message="qty below minimum")
        if q > self._instr["max_qty"]:
            raise ExchangeRejected(ret_code=10001, message="qty above maximum")
        return q

    def _new_order(self, order_link_id: str, side: str, qty: float, position_idx: int, reduce_only: bool, *,
                   kind: str, trigger_price: float | None = None, trigger_direction: int | None = None,
                   trigger_by: str | None = None) -> dict:
        self._order_seq += 1
        oid = f"porder-{self._id_tag}{self._order_seq}"
        t = self._clock()
        o = {
            "order_id": oid, "order_link_id": order_link_id, "side": side, "qty": qty, "cum_qty": 0.0,
            "avg_price": None, "status": "New", "position_idx": position_idx, "reduce_only": reduce_only,
            "kind": kind, "trigger_price": trigger_price, "trigger_direction": trigger_direction,
            "trigger_by": trigger_by, "created_time_ms": t, "updated_time_ms": t,
        }
        self._orders[order_link_id] = o
        self._by_id[oid] = order_link_id
        self._execs[oid] = []
        return o

    def _out_order(self, o: dict) -> dict:
        return {
            "order_id": o["order_id"], "order_link_id": o["order_link_id"], "status": o["status"],
            "qty": o["qty"], "cum_qty": o["cum_qty"], "avg_price": o["avg_price"],
            "trigger_price": o["trigger_price"], "side": o["side"], "position_idx": o["position_idx"],
            "reduce_only": o["reduce_only"],
        }

    def _execute(self, o: dict, price: float) -> None:
        """주문을 price 에 즉시 체결. reduceOnly 는 포지션 크기로 클램프."""
        step = self._instr["qty_step"]
        idx = o["position_idx"]
        pos = self._positions.get(idx)
        side = o["side"]
        qty = o["qty"]
        t = self._clock()

        if o["reduce_only"]:
            if pos is None or pos["size"] <= 0 or pos["side"] == side:
                # Bybit 110017: reduce-only rule not satisfied (닫을 포지션 없음)
                o["status"] = "Cancelled" if o["kind"] == "conditional" else "Rejected"
                o["updated_time_ms"] = t
                if o["kind"] != "conditional":
                    raise ExchangeRejected(ret_code=110017, message="reduce-only rule not satisfied")
                return
            if qty > pos["size"] + step / 2:
                qty = pos["size"]  # Bybit 는 reduceOnly 수량을 포지션 크기로 자동 조정
                o["qty"] = qty

        self._exec_seq += 1
        ex = {"exec_id": f"pexec-{self._id_tag}{self._exec_seq}", "qty": qty, "price": price, "exec_time_ms": t,
              "order_id": o["order_id"]}
        self._execs[o["order_id"]].append(ex)
        o["cum_qty"] = qty
        o["avg_price"] = price
        o["status"] = "Filled"
        o["updated_time_ms"] = t
        self._apply_fill(idx, side, qty, price, t)

    def _apply_fill(self, idx: int, side: str, qty: float, price: float, t: int) -> None:
        step = self._instr["qty_step"]
        pos = self._positions.get(idx)
        if pos is None or pos["size"] <= 0:
            self._positions[idx] = {"size": qty, "side": side, "avg_price": price, "updated_time_ms": t}
            return
        if pos["side"] == side:
            new_size = pos["size"] + qty
            pos["avg_price"] = (pos["avg_price"] * pos["size"] + price * qty) / new_size
            pos["size"] = floor_step(new_size + step / 4, step)
            pos["updated_time_ms"] = t
            return
        # 반대 방향: 감소. 초과분은 one_way(idx 0) 에서만 반전, 헤지 idx 는 0 에서 멈춤
        remain = qty - pos["size"]
        self._record_closed(idx, pos, min(qty, pos["size"]), price, t)
        if remain <= step / 2:
            new_size = floor_step(pos["size"] - qty + step / 4, step)
            if new_size <= step / 2:
                del self._positions[idx]
            else:
                pos["size"] = new_size
                pos["updated_time_ms"] = t
            return
        if idx == 0:
            self._positions[idx] = {"size": floor_step(remain + step / 4, step), "side": side,
                                    "avg_price": price, "updated_time_ms": t}
        else:
            del self._positions[idx]

    def _record_closed(self, idx: int, pos: dict, closed_qty: float, price: float, t: int) -> None:
        """감소 체결의 실현손익 (평균단가 기준). history.PaperHistory 가 청산손익 레코드로 읽는다."""
        if closed_qty <= 0:
            return
        direction = 1.0 if pos["side"] == "Buy" else -1.0
        pnl = (price - pos["avg_price"]) * closed_qty * direction
        self.realized_pnl += pnl
        self._pnl_seq += 1
        self.closed_pnls.append({
            "pnl_id": f"ppnl-{self._id_tag}{self._pnl_seq}", "order_id": None, "side": pos["side"], "qty": closed_qty,
            "avg_entry_price": pos["avg_price"], "avg_exit_price": price, "closed_pnl": pnl,
            "leverage": getattr(self.account, "leverage", None), "position_idx": idx, "created_at_ms": t,
        })

    def unrealised_pnl(self) -> float:
        p = self._mark if self._mark is not None else self._price
        total = 0.0
        for pos in self._positions.values():
            direction = 1.0 if pos["side"] == "Buy" else -1.0
            total += (p - pos["avg_price"]) * pos["size"] * direction
        return total

    def equity_snapshot(self) -> dict:
        """시드 + 실현 + 미실현 (history.PaperHistory.fetch_equity)."""
        with self._lock:
            wallet = self.seed_usdt + self.realized_pnl
            upl = self.unrealised_pnl()
            return {"total_equity": wallet + upl, "wallet_balance": wallet, "unrealised_pnl": upl, "available": wallet}

    def _trigger_conditionals(self, p: float) -> list[dict]:
        fired: list[dict] = []
        for o in sorted(self._orders.values(), key=lambda x: x["created_time_ms"]):
            if o["kind"] != "conditional" or o["status"] != "Untriggered":
                continue
            d = o["trigger_direction"]
            if (d == 1 and p >= o["trigger_price"]) or (d == 2 and p <= o["trigger_price"]):
                o["status"] = "Triggered"
                self._execute(o, p)  # reduceOnly 조건부는 포지션 없으면 Cancelled 로 끝남 (예외 없음)
                fired.append(self._out_order(o))
        return fired


# --------------------------------------------------------------------------- #
# 팩토리
# --------------------------------------------------------------------------- #
def build_exchange(account, kind: str | None = None, price: float | None = None) -> ExchangeBase:
    """account: AccountSettings (1단계 호환: Settings 를 주면 accounts[0]).
    kind: None → account.exchange | "paper" | "bybit" | "okx" | "toobit". paper 의 초기가는 price (기본 85000.0).
    okx/toobit 래퍼는 별도 모듈에서 지연 import 한다 (이 모듈은 그 모듈들 없이도 import 된다)."""
    acct = _as_account(account)
    k = (kind or acct.exchange or "").lower()
    if k == "paper":
        # 계정 거래소의 최소 수량/step 을 그대로 쓴다 (test 결과가 live 의 QTY_BELOW_MIN 판정을 예측하도록)
        instr = PAPER_INSTRUMENTS.get(str(acct.exchange or "").lower())
        # 프로세스마다 다른 ID 태그: 영구 원장에서 이전 프로세스의 paper 체결 ID 와 충돌하지 않도록 (test 모드 재시작 뒤 QTY_MISMATCH 방지)
        return PaperExchange(acct, price=85000.0 if price is None else float(price), instrument=instr,
                             id_tag=f"{now_ms() % 10**9:x}")
    if k == "bybit":
        return BybitExchange(acct)
    if k == "okx":
        from .exchange_okx import OkxExchange  # 지연 import (병렬 구현 모듈)
        return OkxExchange(acct)
    if k == "toobit":
        from .exchange_toobit import ToobitExchange  # 지연 import (병렬 구현 모듈)
        return ToobitExchange(acct)
    raise ValueError(f"unknown exchange kind: {kind}")
