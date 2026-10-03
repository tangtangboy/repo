"""수신 신호 스키마(협의안 v1) 와 공용 열거형.

근거: docs/lake_handoff/lake_execution_contract.json  outgoing_signal_proposal
  - 알 수 없는 필드 거부(extra=forbid), NaN/Inf 거부
  - qty_btc: protection_update 면 null, 그 외 양수
  - leg/position_idx 일관성: idx1→long, idx2→short, idx0 은 단방향(leg 는 방향)
  - take_profit: null | 숫자 | 숫자 배열 → 내부에서는 list[float] | None 으로 정규화
"""
from __future__ import annotations

import math
from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

ID_PATTERN = r"^[A-Za-z0-9_.:-]{1,128}$"


class Action(str, Enum):
    entry = "entry"
    add = "add"
    partial_exit = "partial_exit"
    full_exit = "full_exit"
    protection_update = "protection_update"


class Strategy(str, Enum):
    basic = "basic"
    overheat = "overheat"
    range = "range"


class Leg(str, Enum):
    long = "long"
    short = "short"


class Mode(str, Enum):
    test = "test"
    live = "live"


class ReportStatus(str, Enum):
    acknowledged = "acknowledged"
    submitted = "submitted"
    partially_filled = "partially_filled"
    filled = "filled"
    rejected = "rejected"
    cancelled = "cancelled"
    protection_updated = "protection_updated"
    error = "error"


class ReasonCode(str, Enum):
    """회신 reason_code (정리된 코드만, 거래소 오류 원문 금지)."""
    LIVE_DISABLED = "LIVE_DISABLED"
    OPERATOR_HALT = "OPERATOR_HALT"
    POSITION_MODE_MISMATCH = "POSITION_MODE_MISMATCH"
    POSITION_EXISTS = "POSITION_EXISTS"
    POSITION_NOT_FOUND = "POSITION_NOT_FOUND"
    POSITION_CLOSED = "POSITION_CLOSED"
    QTY_EXCEEDS_LOT = "QTY_EXCEEDS_LOT"
    QTY_BELOW_MIN = "QTY_BELOW_MIN"
    QTY_LIMIT = "QTY_LIMIT"
    LEG_LIMIT = "LEG_LIMIT"
    SLIPPAGE_GUARD = "SLIPPAGE_GUARD"
    STALE_PROTECTION_REVISION = "STALE_PROTECTION_REVISION"
    RECONCILE_REQUIRED = "RECONCILE_REQUIRED"
    EXCHANGE_REJECTED = "EXCHANGE_REJECTED"
    EXCHANGE_ERROR = "EXCHANGE_ERROR"
    EXCHANGE_TIMEOUT = "EXCHANGE_TIMEOUT"
    UNKNOWN_STATE = "UNKNOWN_STATE"
    STOP_LOSS_TRIGGERED = "STOP_LOSS_TRIGGERED"
    TAKE_PROFIT_TRIGGERED = "TAKE_PROFIT_TRIGGERED"
    TEST_RECORD_ONLY = "TEST_RECORD_ONLY"
    QTY_MISMATCH = "QTY_MISMATCH"
    EXPIRED = "EXPIRED"                       # 실행 시점에 expires_at_ms 경과 (접수 후 지연)
    OPPOSING_LEG = "OPPOSING_LEG"             # 단방향(idx 0) 에서 반대 방향 lot 이 이미 열려 있음
    PROTECTION_FAILED = "PROTECTION_FAILED"   # 체결은 됐으나 보호주문 생성/취소 실패 (reconcile 이 재시도)
    ACCOUNT_DISABLED = "ACCOUNT_DISABLED"     # 대상 계정이 enabled=false (계정별 run 결과)
    NO_TARGET_ACCOUNT = "NO_TARGET_ACCOUNT"   # 라우팅 결과 실행할 계정이 하나도 없음 (by_exchange 불일치 등)
    EXCHANGE_MISMATCH = "EXCHANGE_MISMATCH"   # by_exchange 라우팅에서 신호 exchange 와 계정 거래소 불일치 (계정별 run 결과)


def _finite_pos(v, name: str):
    if v is None:
        return None
    if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or v <= 0:
        raise ValueError(f"{name} must be a finite positive number or null")
    return float(v)


class Signal(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=False)

    schema_version: Literal[1]
    strategy_name: str = Field(min_length=1, max_length=128)
    strategy: Strategy
    mode: Mode
    event_id: str = Field(pattern=ID_PATTERN)
    event_sequence: int = Field(ge=1, le=9007199254740991)
    ts: int = Field(ge=1, le=9007199254740991)
    expires_at_ms: int = Field(ge=1, le=9007199254740991)
    exchange: Literal["Bybit", "OKX", "Toobit"]      # lake 는 현재 Bybit 만 보냄; by_exchange 라우팅은 대소문자 무시
    category: Literal["linear"]
    symbol: str = Field(min_length=1, max_length=32)
    position_id: str = Field(pattern=ID_PATTERN)
    leg: Leg
    position_idx: Literal[0, 1, 2]
    action: Action
    qty_btc: float | None = None
    expected_qty_btc_after: float | None = None
    reference_price: float | None = None
    protection_revision: int = Field(ge=0, le=9007199254740991)
    stop_loss: float | None = None
    take_profit: list[float] | float | None = None

    @field_validator("qty_btc", "reference_price", "stop_loss", mode="before")
    @classmethod
    def _pos_or_null(cls, v, info):
        return _finite_pos(v, info.field_name)

    @field_validator("expected_qty_btc_after", mode="before")
    @classmethod
    def _nonneg_or_null(cls, v):
        if v is None:
            return None
        if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or v < 0:
            raise ValueError("expected_qty_btc_after must be a finite number >= 0 or null")
        return float(v)

    @field_validator("take_profit", mode="before")
    @classmethod
    def _tp(cls, v):
        if v is None:
            return None
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return [_finite_pos(v, "take_profit")]
        if isinstance(v, list):
            if len(v) > 20:
                raise ValueError("take_profit: at most 20 prices")
            return [_finite_pos(x, "take_profit[]") for x in v]
        raise ValueError("take_profit must be null, a number, or an array of numbers")

    @model_validator(mode="after")
    def _consistency(self):
        if self.position_idx == 1 and self.leg != Leg.long:
            raise ValueError("position_idx 1 requires leg long")
        if self.position_idx == 2 and self.leg != Leg.short:
            raise ValueError("position_idx 2 requires leg short")
        if self.action == Action.protection_update:
            if self.qty_btc is not None:
                raise ValueError("protection_update requires qty_btc null")
        else:
            if self.qty_btc is None:
                raise ValueError(f"{self.action.value} requires positive qty_btc")
        if self.expires_at_ms < self.ts:
            raise ValueError("expires_at_ms must be >= ts")
        if self.expected_qty_btc_after is not None and self.expected_qty_btc_after < 0:
            raise ValueError("expected_qty_btc_after must be >= 0")
        return self

    # ---- 편의 ----
    @property
    def tp_list(self) -> list[float] | None:
        if self.take_profit is None:
            return None
        return list(self.take_profit)  # type: ignore[arg-type]

    def exchange_key(self) -> str:
        """config.AccountSettings.exchange 와 비교할 소문자 키 ("bybit" | "okx" | "toobit")."""
        return str(self.exchange).lower()

    def side(self) -> str:
        """진입/추가 방향의 Bybit side."""
        return "Buy" if self.leg == Leg.long else "Sell"

    def close_side(self) -> str:
        return "Sell" if self.leg == Leg.long else "Buy"

