"""OKX 거래소 래퍼 (ARCHITECTURE_MULTI_EXCHANGE.md §3 OKX) — python-okx 기반.

`OkxExchange(account, clients=None)` 는 `exchange.ExchangeBase` 인터페이스를 그대로 구현한다.
  - 클라이언트: okx.Trade.TradeAPI / okx.Account.AccountAPI / okx.PublicData.PublicAPI / okx.MarketData.MarketAPI.
    `flag="1"` (데모 트레이딩) when account.testnet, 아니면 "0". `clients={"trade","account","public","market"}` 로
    테스트 더블을 주입할 수 있다 (생성 시 실네트워크 접속 없음).
  - 수량 단위: 공개 인터페이스는 **전부 BTC**. 계약 수(sz) 변환은 이 모듈 안에서만 한다
    (`sz = qty_btc / ctVal`, lotSz 배수로 내림; 체결·포지션은 `× ctVal` 로 환산).
    `instrument()` = {qty_step: lotSz×ctVal, min_qty: minSz×ctVal, max_qty: maxMktSz×ctVal, tick: tickSz}.
  - position_idx ↔ posSide: hedge 1→long, 2→short / one_way 0→net. 포지션 조회는 그 역방향(net 은 pos 부호로 side).
  - clOrdId / algoClOrdId: `util.alnum_only(order_link_id, 32)` (영숫자만, 32자 절단). 같은 link → 같은 id (멱등).
    주의: `util.order_link_id()` 는 34자라 뒤 2자가 잘린다. 이 모듈은 본 적 있는 link 의 역매핑을 메모리에 두고,
    재기동 뒤 처음 보는 id 는 `link_resolver(cid) -> link|None` (실행기가 원장의 order_link_id 접두사 검색으로 꽂아 준다)
    로 되돌린다. 둘 다 없으면 잘린 id(32자) 를 그대로 돌려준다.
  - 시장가: place_order(ordType="market", tdMode=margin_mode|cross, side, posSide, sz, clOrdId[, reduceOnly]).
    reduceOnly 는 OKX 가 **net 모드에서만** 받는다(long/short 모드는 side+posSide 가 청산을 결정; ccxt 도 생략) →
    one_way 에서만 "true"/"false" 를 보내고 hedge 에서는 비운다.
  - 조건부(보호주문): place_algo_order(ordType="conditional", ...). side 와 trigger_direction 으로 SL/TP 를 가른다:
    포지션에 불리한 방향(Sell+하락, Buy+상승) → slTriggerPx/slOrdPx="-1", 유리한 방향 → tpTriggerPx/tpOrdPx="-1".
    트리거 가격 종류는 settings.protection_trigger_by (MarkPrice→mark, LastPrice→last, IndexPrice→index).
  - 조회: get_order(clOrdId) → state live→New, partially_filled→PartiallyFilled, filled→Filled,
    canceled→Cancelled (체결이 있으면 PartiallyFilledCanceled). 없으면 get_algo_order_details(algoClOrdId) →
    live/pause→Untriggered, partially_effective→Triggered, effective→**생성된 ordId 의 일반 주문을 따라가** Filled 등으로,
    canceled→Cancelled, order_failed→Rejected. 알고 주문의 `order_id` 는 항상 algoId (place_conditional 반환값과 같게),
    생성된 일반 주문 id 는 `exec_order_id` 로 노출하고 `executions(algoId)` 는 그것으로 체결을 읽는다.
  - 오류: 응답 code != "0" 또는 data[i].sCode != "0" → 쓰기 ExchangeRejected(ret_code) / 읽기 ExchangeError(EXCHANGE_ERROR).
    단, **결과를 알 수 없는** 코드는 쓰기에서도 거부가 아니라 ExchangeError 다 — 50004(요청 타임아웃, 성공/실패 불명) →
    EXCHANGE_TIMEOUT, 50001/50013/50026(매칭엔진 점검/시스템 바쁨/시스템 오류) → EXCHANGE_ERROR. 실행기는 이 경우
    get_order 로 확인한 뒤 미확정(unknown) 으로 남겨 reconcile 이 재확인한다 (ccxt okx: RequestTimeout / ExchangeNotAvailable).
    네트워크 타임아웃 → EXCHANGE_TIMEOUT, 그 외 전송 오류 → EXCHANGE_ERROR. 원문은 WARNING 로그에 200자 절단.
    트리거가 이미 지난 가격(51277~51280) 은 메시지에 "crossed" 를 덧붙여 실행기의 공통 판정(_is_crossed_trigger) 이 잡게 한다.
"""
from __future__ import annotations

import logging
import threading
import time
from decimal import Decimal, ROUND_DOWN
from typing import Any, Callable

from .config import AccountSettings
from .exchange import ExchangeBase, ExchangeError, ExchangeRejected, _as_account
from .util import alnum_only, fmt_step, round_tick

log = logging.getLogger("lake_executor.exchange_okx")

_LOG_TRUNC = 200
CLORDID_MAX = 32

# 취소 요청에 대해 "이미 없음/취소됨/완료됨" 으로 보는 OKX 코드 → cancel_order 는 True
_CANCEL_GONE_CODES = {51400, 51401, 51402, 51405, 51410, 51603}
_CANCEL_GONE_WORDS = ("not exist", "already", "completed", "canceled", "cancelled", "no pending")
# 조회 "없음" 코드 (get_order / get_algo_order_details)
_NOT_FOUND_CODES = {51603}
_NOT_FOUND_WORDS = ("not exist", "does not exist")
# 트리거 가격이 이미 지남 (TP/SL trigger price can not be higher/lower than the last price)
_CROSSED_CODES = {51277, 51278, 51279, 51280}
# 계정 설정이 이미 그 상태 → 무시
_ALREADY_WORDS = ("already", "not modified", "no change", "same as")
# 쓰기 호출의 결과를 알 수 없는 코드 (주문이 들어갔을 수 있다 → 거부가 아니라 오류로 전달해 실행기가 확인/재확인하게)
_UNCERTAIN_TIMEOUT_CODES = {50004}            # Endpoint request timeout (does not indicate success or failure)
_UNCERTAIN_ERROR_CODES = {50001, 50013, 50026}  # Matching engine upgrading / System busy / System error

_ORDER_STATE = {
    "live": "New",
    "partially_filled": "PartiallyFilled",
    "filled": "Filled",
    "canceled": "Cancelled",
    "mmp_canceled": "Cancelled",
}
_ALGO_STATE = {
    "live": "Untriggered",
    "pause": "Untriggered",
    "partially_effective": "Triggered",
    "effective": "Triggered",
    "canceled": "Cancelled",
    "order_failed": "Rejected",
}
_TRIGGER_PX_TYPE = {"MarkPrice": "mark", "LastPrice": "last", "IndexPrice": "index",
                    "mark": "mark", "last": "last", "index": "index"}
_POS_SIDE_HEDGE = {1: "long", 2: "short"}
_IDX_OF_POS_SIDE = {"long": 1, "short": 2, "net": 0, "": 0}


def _f(v: Any, default: float | None = None) -> float | None:
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


def _code_int(v: Any) -> int | None:
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return None


def _trunc(s: Any) -> str:
    return str(s).replace("\n", " ")[:_LOG_TRUNC]


def _data_list(resp: Any) -> list[dict]:
    if not isinstance(resp, dict):
        return []
    d = resp.get("data")
    if isinstance(d, list):
        return [x for x in d if isinstance(x, dict)]
    if isinstance(d, dict):
        return [d]
    return []


def _response_error(resp: Any) -> tuple[int | None, str] | None:
    """응답의 오류 (ret_code, msg). code != "0" 또는 data[i].sCode != "0" 이면 오류. 정상이면 None.
    data 안의 sCode 가 있으면 그 쪽(더 구체적)을 우선한다."""
    if not isinstance(resp, dict):
        return None, "non-dict response"
    code = str(resp.get("code", "0") or "0")
    for item in _data_list(resp):
        sc = item.get("sCode")
        if sc is not None and str(sc) not in ("", "0"):
            return _code_int(sc), str(item.get("sMsg") or resp.get("msg") or "")
    if code != "0":
        return _code_int(code), str(resp.get("msg") or "")
    return None


def _is_not_found(err: ExchangeError) -> bool:
    if err.ret_code in _NOT_FOUND_CODES:
        return True
    msg = (err.message or "").lower()
    return any(w in msg for w in _NOT_FOUND_WORDS)


def _is_gone(err: ExchangeError) -> bool:
    if err.ret_code in _CANCEL_GONE_CODES:
        return True
    msg = (err.message or "").lower()
    return any(w in msg for w in _CANCEL_GONE_WORDS)


def _is_already(err: ExchangeError) -> bool:
    msg = (err.message or "").lower()
    return any(w in msg for w in _ALREADY_WORDS)


def clordid_of(order_link_id: str) -> str:
    """order_link_id → OKX clOrdId/algoClOrdId (영숫자만, 32자)."""
    cid = alnum_only(order_link_id or "", CLORDID_MAX)
    if not cid:
        raise ExchangeRejected(ret_code=51000, message="order_link_id has no alphanumeric characters")
    return cid


class OkxExchange(ExchangeBase):
    """OKX v5 (python-okx) 래퍼. 단일 SWAP 심볼(account.symbol, 예: BTC-USDT-SWAP)."""

    http_timeout_s: float = 10.0
    read_attempts: int = 2
    read_retry_sleep_s: float = 0.3

    name = "okx"
    display_name = "OKX"
    supports_lot_protection = True

    def __init__(self, account, clients: dict | None = None):
        """account: config.AccountSettings (1단계 호환으로 Settings 도 받는다 → accounts[0]).
        clients: {"trade","account","public","market"} 테스트 주입 (없으면 python-okx 클라이언트 생성)."""
        self.account: AccountSettings = _as_account(account)
        self.settings = self.account          # 하위 호환 속성명 (BybitExchange 와 동일)
        self.symbol = self.account.symbol
        self.inst_type = "SWAP"
        self.flag = "1" if self.account.testnet else "0"
        self.td_mode = self.account.margin_mode or "cross"
        self._instr: dict | None = None
        self._lock = threading.RLock()
        # link ↔ clOrdId 역매핑, link 종류(order|algo), algoId → 생성된 ordId
        self._link_of_cid: dict[str, str] = {}
        self._kind_of_link: dict[str, str] = {}
        self._algo_id_of_link: dict[str, str] = {}
        self._exec_order_of_algo: dict[str, str] = {}
        # 재기동 뒤 처음 보는 clOrdId(32자 절단) → 원래 link. 실행기가 원장 검색 함수를 꽂는다 (없으면 절단 id 그대로).
        self.link_resolver: Callable[[str], str | None] | None = None
        if clients is not None:
            self.trade = clients["trade"]
            self.acct = clients["account"]
            self.public = clients["public"]
            self.market = clients["market"]
        else:
            self.trade, self.acct, self.public, self.market = self._make_clients()

    # ----- 클라이언트 생성 --------------------------------------------------------
    def _make_clients(self) -> tuple[Any, Any, Any, Any]:
        from okx.Account import AccountAPI       # 지연 import: 모듈 import 부작용 최소화
        from okx.MarketData import MarketAPI
        from okx.PublicData import PublicAPI
        from okx.Trade import TradeAPI
        a = self.account
        auth: dict[str, Any] = {"flag": self.flag}
        if a.api_key and a.api_secret:
            auth.update(api_key=a.api_key, api_secret_key=a.api_secret, passphrase=a.api_passphrase or "")
        trade = TradeAPI(**auth)
        acct = AccountAPI(**auth)
        public = PublicAPI(flag=self.flag)
        market = MarketAPI(flag=self.flag)
        for c in (trade, acct, public, market):
            try:
                import httpx
                c.timeout = httpx.Timeout(self.http_timeout_s)
            except Exception:  # noqa: BLE001 - 타임아웃 설정 실패는 치명적이지 않다
                pass
        return trade, acct, public, market

    # ----- 예외 매핑 ------------------------------------------------------------
    @staticmethod
    def _classify_exception(e: BaseException) -> ExchangeError:
        """전송 계층 예외(httpx / python-okx) → ExchangeError. 원문은 message 에만."""
        try:
            import httpx
            if isinstance(e, httpx.TimeoutException):
                return ExchangeError("EXCHANGE_TIMEOUT", f"{type(e).__name__}: {str(e)[:80]}")
        except Exception:  # pragma: no cover - httpx 미설치 환경
            pass
        name = type(e).__name__.lower()
        if "timeout" in name or "timed out" in str(e).lower():
            return ExchangeError("EXCHANGE_TIMEOUT", f"{type(e).__name__}: {str(e)[:80]}")
        return ExchangeError("EXCHANGE_ERROR", f"{type(e).__name__}: {str(e)[:80]}")

    @staticmethod
    def _rejected(ret_code: int | None, msg: str) -> ExchangeRejected:
        if ret_code in _CROSSED_CODES:
            msg = f"{msg} (trigger already crossed)"   # 실행기 _is_crossed_trigger 의 'crossed' 단어 판정
        return ExchangeRejected(ret_code=ret_code, message=msg)

    def _call(self, name: str, fn: Callable[..., Any], *args: Any, read: bool, quiet_codes: set[int] | None = None,
              **kw: Any) -> Any:
        """python-okx 호출 래퍼. read=True 면 전송 오류에 한해 재시도(총 read_attempts 회).
        응답 code/sCode 오류: 쓰기 → ExchangeRejected / 읽기 → ExchangeError(EXCHANGE_ERROR, ret_code).
        quiet_codes 의 코드는 호출부가 무해하게 처리하므로 DEBUG 로만 남긴다."""
        attempts = self.read_attempts if read else 1
        for i in range(attempts):
            try:
                resp = fn(*args, **kw)
            except ExchangeError:
                raise
            except Exception as e:  # noqa: BLE001 - 모든 전송 예외를 코드로 변환
                err = self._classify_exception(e)
                log.warning("okx %s failed (%s): %s", name, err.code, _trunc(e))
                if read and i + 1 < attempts:
                    time.sleep(self.read_retry_sleep_s)
                    continue
                raise err from None
            bad = _response_error(resp)
            if bad is None:
                return resp
            ret_code, msg = bad
            lvl = logging.DEBUG if (quiet_codes and ret_code in quiet_codes) else logging.WARNING
            log.log(lvl, "okx %s rejected (ret_code=%s): %s", name, ret_code, _trunc(msg))
            if read:
                raise ExchangeError("EXCHANGE_ERROR", msg, ret_code=ret_code)
            if ret_code in _UNCERTAIN_TIMEOUT_CODES:
                raise ExchangeError("EXCHANGE_TIMEOUT", msg, ret_code=ret_code)
            if ret_code in _UNCERTAIN_ERROR_CODES:
                raise ExchangeError("EXCHANGE_ERROR", msg, ret_code=ret_code)
            raise self._rejected(ret_code, msg)
        raise ExchangeError("EXCHANGE_ERROR", f"{name}: no attempts")  # pragma: no cover

    # ----- 단위 변환 ------------------------------------------------------------
    def instrument(self) -> dict:
        with self._lock:
            if self._instr is not None:
                return self._public_instr(self._instr)
        r = self._call("get_instruments", self.public.get_instruments, read=True,
                       instType=self.inst_type, instId=self.symbol)
        lst = [x for x in _data_list(r) if not x.get("instId") or x.get("instId") == self.symbol]
        if not lst:
            raise ExchangeError("EXCHANGE_ERROR", f"instrument {self.symbol} not found")
        item = lst[0]
        ct_val = _f(item.get("ctVal"), 0.0) or 0.0
        if ct_val <= 0:
            raise ExchangeError("EXCHANGE_ERROR", f"instrument {self.symbol}: ctVal missing")
        lot_sz = _f(item.get("lotSz"), 1.0) or 1.0
        min_sz = _f(item.get("minSz"), lot_sz) or lot_sz
        max_sz = _f(item.get("maxMktSz")) or _f(item.get("maxLmtSz")) or 0.0
        info = {
            "ct_val": ct_val, "lot_sz": lot_sz, "min_sz": min_sz, "max_sz": max_sz,
            "tick": _f(item.get("tickSz"), 0.1) or 0.1,
            "ct_val_ccy": str(item.get("ctValCcy") or ""),
        }
        with self._lock:
            self._instr = info
        log.info("okx instrument %s: %s -> %s", self.symbol, info, self._public_instr(info))
        return self._public_instr(info)

    @staticmethod
    def _public_instr(info: dict) -> dict:
        """내부 캐시(계약 단위) → 인터페이스(BTC 단위)."""
        ct = Decimal(str(info["ct_val"]))
        step = float(Decimal(str(info["lot_sz"])) * ct)
        min_qty = float(Decimal(str(info["min_sz"])) * ct)
        max_qty = float(Decimal(str(info["max_sz"])) * ct) if info.get("max_sz") else 100.0
        return {"qty_step": step, "min_qty": min_qty, "max_qty": max_qty, "tick": float(info["tick"])}

    def _raw_instr(self) -> dict:
        self.instrument()
        with self._lock:
            assert self._instr is not None
            return dict(self._instr)

    def _sz_str(self, qty_btc: float) -> str:
        """BTC 수량 → 계약 수 문자열 (lotSz 배수 내림). 0 계약이면 거부(51020 과 같은 의미)."""
        info = self._raw_instr()
        ct, lot = Decimal(str(info["ct_val"])), Decimal(str(info["lot_sz"]))
        contracts = (Decimal(str(float(qty_btc))) / ct / lot).to_integral_value(rounding=ROUND_DOWN) * lot
        if contracts <= 0:
            raise ExchangeRejected(ret_code=51020, message="qty rounds to zero contracts")
        if Decimal(str(info["min_sz"])) > contracts:
            raise ExchangeRejected(ret_code=51020, message="qty below minimum contract size")
        return fmt_step(float(contracts), float(lot))

    def _btc(self, contracts: Any) -> float:
        info = self._raw_instr()
        c = _f(contracts, 0.0) or 0.0
        return float(Decimal(str(c)) * Decimal(str(info["ct_val"])))

    def _price_str(self, price: float) -> str:
        tick = float(self._raw_instr()["tick"])
        p = round_tick(float(price), tick)
        if p <= 0:
            raise ExchangeRejected(ret_code=51000, message="price must be > 0")
        return fmt_step(p, tick)

    # ----- 매핑 ----------------------------------------------------------------
    def _pos_side(self, position_idx: int) -> str:
        idx = int(position_idx)
        if self.account.position_mode == "hedge":
            ps = _POS_SIDE_HEDGE.get(idx)
            if ps is None:
                raise ExchangeRejected(ret_code=51000, message="position_idx must be 1|2 in hedge mode")
            return ps
        if idx != 0:
            raise ExchangeRejected(ret_code=51000, message="position_idx must be 0 in one_way mode")
        return "net"

    @staticmethod
    def _idx_of(pos_side: Any) -> int:
        return _IDX_OF_POS_SIDE.get(str(pos_side or "").lower(), 0)

    @staticmethod
    def _side_out(side: Any) -> str:
        s = str(side or "").lower()
        return "Buy" if s == "buy" else ("Sell" if s == "sell" else "")

    @staticmethod
    def _side_in(side: str) -> str:
        if side not in ("Buy", "Sell"):
            raise ExchangeRejected(ret_code=51000, message="side must be Buy|Sell")
        return side.lower()

    def _remember(self, order_link_id: str, cid: str, kind: str, algo_id: str | None = None) -> None:
        with self._lock:
            self._link_of_cid[cid] = order_link_id
            self._kind_of_link[order_link_id] = kind
            if algo_id:
                self._algo_id_of_link[order_link_id] = algo_id

    def _link_for_cid(self, cid: str) -> str:
        with self._lock:
            link = self._link_of_cid.get(cid)
        if link:
            return link
        resolver = self.link_resolver
        if resolver is not None:
            try:
                found = resolver(cid)
            except Exception as e:  # noqa: BLE001 - 역매핑 실패는 치명적이지 않다 (절단 id 로 진행)
                log.warning("okx link_resolver failed for %s: %s", cid, type(e).__name__)
                found = None
            if found and clordid_of(found) == cid:
                with self._lock:
                    self._link_of_cid[cid] = found
                return found
        return cid

    # ----- 시세 ------------------------------------------------------------------
    def last_price(self) -> float:
        r = self._call("get_ticker", self.market.get_ticker, read=True, instId=self.symbol)
        lst = _data_list(r)
        p = _f(lst[0].get("last")) if lst else None
        if p is None or p <= 0:
            raise ExchangeError("EXCHANGE_ERROR", "last price missing")
        return p

    def mark_price(self) -> float | None:
        r = self._call("get_mark_price", self.public.get_mark_price, read=True, instType=self.inst_type, instId=self.symbol)
        lst = _data_list(r)
        p = _f(lst[0].get("markPx")) if lst else None
        return p if p and p > 0 else None

    # ----- 포지션 ----------------------------------------------------------------
    def positions(self) -> dict[int, dict]:
        r = self._call("get_positions", self.acct.get_positions, read=True, instType=self.inst_type, instId=self.symbol)
        out: dict[int, dict] = {}
        for p in _data_list(r):
            if p.get("instId") and p.get("instId") != self.symbol:
                continue
            raw = _f(p.get("pos"), 0.0) or 0.0
            if raw == 0:
                continue
            ps = str(p.get("posSide") or "net").lower()
            idx = self._idx_of(ps)
            if ps == "long":
                side = "Buy"
            elif ps == "short":
                side = "Sell"
            else:
                side = "Buy" if raw > 0 else "Sell"
            size = self._btc(abs(raw))
            if size <= 0:
                continue
            out[idx] = {
                "position_idx": idx,
                "size": size,
                "side": side,
                "avg_price": _f(p.get("avgPx")),
                "mark_price": _f(p.get("markPx")),
                "updated_time_ms": _i(p.get("uTime"), 0),
            }
        return out

    # ----- 계정 설정 ---------------------------------------------------------------
    def _setup_call(self, name: str, fn: Callable[..., Any], **kw: Any) -> None:
        try:
            self._call(name, fn, read=False, **kw)
            log.info("okx %s applied", name)
        except ExchangeRejected as e:
            if _is_already(e):
                log.info("okx %s: already set (ret_code=%s)", name, e.ret_code)
                return
            raise

    def ensure_account_setup(self, position_mode: str, leverage: int, margin_mode: str) -> None:
        if position_mode not in ("hedge", "one_way"):
            raise ValueError("position_mode must be hedge | one_way")
        if margin_mode and margin_mode not in ("isolated", "cross"):
            raise ValueError(f"unknown margin_mode {margin_mode}")
        self._setup_call("set_position_mode", self.acct.set_position_mode,
                         posMode="long_short_mode" if position_mode == "hedge" else "net_mode")
        mgn = margin_mode or self.td_mode
        lev = str(int(leverage))
        if mgn == "isolated" and position_mode == "hedge":
            for ps in ("long", "short"):
                self._setup_call("set_leverage", self.acct.set_leverage, lever=lev, mgnMode=mgn, instId=self.symbol, posSide=ps)
        elif mgn == "isolated":
            self._setup_call("set_leverage", self.acct.set_leverage, lever=lev, mgnMode=mgn, instId=self.symbol, posSide="net")
        else:
            self._setup_call("set_leverage", self.acct.set_leverage, lever=lev, mgnMode=mgn, instId=self.symbol)

    # ----- 주문 ---------------------------------------------------------------------
    def _reduce_only_param(self, reduce_only: bool) -> str:
        """OKX 는 reduceOnly 를 net 모드에서만 받는다 (long/short 모드는 side+posSide 로 청산 결정)."""
        if self.account.position_mode == "hedge":
            return ""
        return "true" if reduce_only else "false"

    def place_market(self, side: str, qty: float, position_idx: int, reduce_only: bool, order_link_id: str) -> dict:
        cid = clordid_of(order_link_id)
        kw: dict[str, Any] = dict(
            instId=self.symbol, tdMode=self.td_mode, side=self._side_in(side), ordType="market",
            sz=self._sz_str(qty), posSide=self._pos_side(position_idx),
            reduceOnly=self._reduce_only_param(reduce_only), clOrdId=cid,
        )
        self._remember(order_link_id, cid, "order")
        r = self._call("place_order", self.trade.place_order, read=False, **kw)
        lst = _data_list(r)
        oid = str(lst[0].get("ordId") or "") if lst else ""
        if not oid:
            raise ExchangeError("EXCHANGE_ERROR", "ordId missing in response")
        log.info("okx market %s qty=%s(sz=%s) idx=%s ro=%s link=%s -> %s",
                 side, qty, kw["sz"], position_idx, reduce_only, order_link_id, oid)
        return {"order_id": oid, "order_link_id": order_link_id}

    def place_conditional(self, side: str, qty: float, position_idx: int, trigger_price: float,
                          trigger_direction: int, order_link_id: str, trigger_by: str) -> dict:
        d = int(trigger_direction)
        if d not in (1, 2):
            raise ValueError("trigger_direction must be 1 or 2")
        s = self._side_in(side)
        px_type = _TRIGGER_PX_TYPE.get(str(trigger_by or ""), "mark")
        px = self._price_str(trigger_price)
        # 포지션에 불리한 방향(롱 청산 Sell + 하락 / 숏 청산 Buy + 상승) = 손절, 반대 = 익절
        adverse = (s == "sell" and d == 2) or (s == "buy" and d == 1)
        cid = clordid_of(order_link_id)
        kw: dict[str, Any] = dict(
            instId=self.symbol, tdMode=self.td_mode, side=s, ordType="conditional", sz=self._sz_str(qty),
            posSide=self._pos_side(position_idx), reduceOnly=self._reduce_only_param(True), algoClOrdId=cid,
        )
        if adverse:
            kw.update(slTriggerPx=px, slOrdPx="-1", slTriggerPxType=px_type)
        else:
            kw.update(tpTriggerPx=px, tpOrdPx="-1", tpTriggerPxType=px_type)
        self._remember(order_link_id, cid, "algo")
        r = self._call("place_algo_order", self.trade.place_algo_order, read=False, **kw)
        lst = _data_list(r)
        aid = str(lst[0].get("algoId") or "") if lst else ""
        if not aid:
            raise ExchangeError("EXCHANGE_ERROR", "algoId missing in response")
        self._remember(order_link_id, cid, "algo", aid)
        log.info("okx conditional %s %s qty=%s(sz=%s) idx=%s trig=%s dir=%s link=%s -> %s",
                 "sl" if adverse else "tp", side, qty, kw["sz"], position_idx, px, d, order_link_id, aid)
        return {"order_id": aid, "order_link_id": order_link_id}

    # ----- 취소 ---------------------------------------------------------------------
    def _cancel_regular(self, cid: str) -> bool | None:
        """일반 주문 취소. 이미 없음/체결/취소 → True. 주문이 존재하지 않으면(51400/51603) None (알고 쪽 확인 필요)."""
        try:
            self._call("cancel_order", self.trade.cancel_order, read=False, quiet_codes=_CANCEL_GONE_CODES,
                       instId=self.symbol, clOrdId=cid)
            return True
        except ExchangeRejected as e:
            if e.ret_code in (51400, 51603) or _is_not_found(e):
                return None
            if _is_gone(e):
                return True
            raise

    def _cancel_algo(self, algo_id: str) -> bool:
        try:
            self._call("cancel_algo_order", self.trade.cancel_algo_order, [{"instId": self.symbol, "algoId": algo_id}],
                       read=False, quiet_codes=_CANCEL_GONE_CODES)
            return True
        except ExchangeRejected as e:
            if _is_gone(e) or _is_not_found(e):
                return True
            raise

    def cancel_order(self, order_link_id: str) -> bool:
        cid = clordid_of(order_link_id)
        with self._lock:
            kind = self._kind_of_link.get(order_link_id)
            algo_id = self._algo_id_of_link.get(order_link_id)
        self._remember(order_link_id, cid, kind or "unknown")
        if kind != "algo":
            res = self._cancel_regular(cid)
            if res is not None:
                log.info("okx cancelled link=%s (regular)", order_link_id)
                return True
            if kind == "order":
                log.info("okx cancel link=%s: order not found (gone)", order_link_id)
                return True
        if not algo_id:
            algo = self._algo_details(cid)
            if algo is None:
                log.info("okx cancel link=%s: neither order nor algo found (gone)", order_link_id)
                return True
            algo_id = str(algo.get("algoId") or "")
            state = str(algo.get("state") or "")
            self._remember(order_link_id, cid, "algo", algo_id)
            if state not in ("live", "pause", "partially_effective"):
                log.info("okx cancel link=%s: algo already %s", order_link_id, state)
                return True
        ok = self._cancel_algo(algo_id)
        log.info("okx cancelled link=%s (algo %s)", order_link_id, algo_id)
        return ok

    # ----- 조회 ---------------------------------------------------------------------
    def _map_order(self, o: dict, *, link: str | None = None) -> dict:
        state = str(o.get("state") or "").lower()
        status = _ORDER_STATE.get(state, state)
        cum = self._btc(o.get("accFillSz"))
        if state == "canceled" and cum > 0:
            status = "PartiallyFilledCanceled"
        cid = str(o.get("clOrdId") or "")
        return {
            "order_id": str(o.get("ordId") or ""),
            "order_link_id": link or self._link_for_cid(cid),
            "status": status,
            "qty": self._btc(o.get("sz")),
            "cum_qty": cum,
            "avg_price": _f(o.get("avgPx")) or None,
            "trigger_price": None,
            "side": self._side_out(o.get("side")),
            "position_idx": self._idx_of(o.get("posSide")),
            "reduce_only": str(o.get("reduceOnly") or "").lower() == "true",
        }

    def _regular_order(self, *, cid: str = "", ord_id: str = "") -> dict | None:
        kw: dict[str, Any] = {"instId": self.symbol}
        if ord_id:
            kw["ordId"] = ord_id
        else:
            kw["clOrdId"] = cid
        try:
            r = self._call("get_order", self.trade.get_order, read=True, quiet_codes=_NOT_FOUND_CODES, **kw)
        except ExchangeError as e:
            if _is_not_found(e):
                return None
            raise
        lst = _data_list(r)
        if cid:
            lst = [o for o in lst if not o.get("clOrdId") or o.get("clOrdId") == cid]
        return lst[0] if lst else None

    def _algo_details(self, cid: str = "", algo_id: str = "") -> dict | None:
        kw = {"algoId": algo_id} if algo_id else {"algoClOrdId": cid}
        try:
            r = self._call("get_algo_order_details", self.trade.get_algo_order_details, read=True,
                           quiet_codes=_NOT_FOUND_CODES, **kw)
        except ExchangeError as e:
            if _is_not_found(e):
                return None
            raise
        lst = _data_list(r)
        return lst[0] if lst else None

    def _map_algo(self, a: dict, *, link: str) -> dict:
        state = str(a.get("state") or "").lower()
        status = _ALGO_STATE.get(state, "Deactivated" if state else "")
        aid = str(a.get("algoId") or "")
        exec_oid = str(a.get("ordId") or "")
        trig = _f(a.get("slTriggerPx")) or _f(a.get("tpTriggerPx")) or _f(a.get("triggerPx")) or None
        out = {
            "order_id": aid,
            "order_link_id": link,
            "status": status,
            "qty": self._btc(a.get("sz")),
            "cum_qty": 0.0,
            "avg_price": None,
            "trigger_price": trig,
            "side": self._side_out(a.get("side")),
            "position_idx": self._idx_of(a.get("posSide")),
            "reduce_only": True,
            "algo_id": aid,
            "exec_order_id": exec_oid or None,
        }
        if aid and exec_oid:
            with self._lock:
                self._exec_order_of_algo[aid] = exec_oid
        if state in ("effective", "partially_effective") and exec_oid:
            # 트리거되어 생성된 일반 주문을 따라가 체결 상태를 그대로 보여준다 (order_id 는 algoId 유지)
            try:
                o = self._regular_order(ord_id=exec_oid)
            except ExchangeError as e:
                log.warning("okx algo %s effective but linked order %s unreadable (%s)", aid, exec_oid, e.code)
                o = None
            if o is not None:
                m = self._map_order(o, link=link)
                st = m["status"]
                out.update(cum_qty=m["cum_qty"], avg_price=m["avg_price"])
                if st in ("Filled", "PartiallyFilled", "PartiallyFilledCanceled"):
                    out["status"] = st
                elif st == "Cancelled":
                    out["status"] = "Cancelled"
                elif st == "Rejected":
                    out["status"] = "Rejected"
                else:
                    out["status"] = "Triggered"
        return out

    def get_order(self, order_link_id: str) -> dict | None:
        cid = clordid_of(order_link_id)
        with self._lock:
            kind = self._kind_of_link.get(order_link_id)
        self._remember(order_link_id, cid, kind or "unknown")
        if kind != "algo":
            o = self._regular_order(cid=cid)
            if o is not None:
                self._remember(order_link_id, cid, "order")
                return self._map_order(o, link=order_link_id)
            if kind == "order":
                return None
        a = self._algo_details(cid)
        if a is None:
            return None
        self._remember(order_link_id, cid, "algo", str(a.get("algoId") or "") or None)
        return self._map_algo(a, link=order_link_id)

    def _fills(self, ord_id: str) -> list[dict]:
        r = self._call("get_fills", self.trade.get_fills, read=True, instType=self.inst_type, instId=self.symbol, ordId=ord_id)
        out: list[dict] = []
        for x in _data_list(r):
            if x.get("ordId") and str(x.get("ordId")) != str(ord_id):
                continue
            q = self._btc(x.get("fillSz"))
            if q <= 0:
                continue
            out.append({
                "exec_id": str(x.get("tradeId") or x.get("billId") or ""),
                "qty": q,
                "price": _f(x.get("fillPx"), 0.0) or 0.0,
                "exec_time_ms": _i(x.get("ts") or x.get("fillTime"), 0),
                "order_id": str(x.get("ordId") or ord_id),
            })
        out.sort(key=lambda d: (d["exec_time_ms"], d["exec_id"]))
        return out

    def executions(self, order_id: str) -> list[dict]:
        """order_id 가 algoId(보호주문) 면 트리거로 생성된 ordId 의 체결을 읽는다 (매핑은 get_order 에서 캐시, 없으면 조회)."""
        oid = str(order_id)
        with self._lock:
            linked = self._exec_order_of_algo.get(oid)
        if linked:
            return self._fills(linked)
        fills = self._fills(oid)
        if fills:
            return fills
        a = self._algo_details(algo_id=oid)
        exec_oid = str((a or {}).get("ordId") or "")
        if not exec_oid:
            return []
        with self._lock:
            self._exec_order_of_algo[oid] = exec_oid
        return self._fills(exec_oid)

    def open_conditional_orders(self, position_idx: int) -> list[dict]:
        want = self._pos_side(position_idx)
        out: list[dict] = []
        after = ""
        for _ in range(10):  # 페이지 안전 상한
            kw: dict[str, Any] = dict(ordType="conditional", instType=self.inst_type, instId=self.symbol, limit="100")
            if after:
                kw["after"] = after
            r = self._call("order_algos_list", self.trade.order_algos_list, read=True, **kw)
            lst = _data_list(r)
            for a in lst:
                ps = str(a.get("posSide") or "net").lower()
                if ps != want:
                    continue
                state = str(a.get("state") or "live").lower()
                if state not in ("live", "pause", "partially_effective"):
                    continue
                cid = str(a.get("algoClOrdId") or "")
                link = self._link_for_cid(cid) if cid else ""
                aid = str(a.get("algoId") or "")
                if link and aid:
                    self._remember(link, cid, "algo", aid)
                out.append({
                    "order_link_id": link,
                    "order_id": aid,
                    "trigger_price": _f(a.get("slTriggerPx")) or _f(a.get("tpTriggerPx")) or _f(a.get("triggerPx")),
                    "qty": self._btc(a.get("sz")),
                    "side": self._side_out(a.get("side")),
                })
            if len(lst) < 100:
                break
            after = str(lst[-1].get("algoId") or "")
            if not after:
                break
        return out
