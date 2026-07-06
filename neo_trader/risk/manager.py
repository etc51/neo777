"""Risk gate and sizing utilities.

The risk manager is a pure decision component. It does not place, modify, or
cancel orders.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time
from decimal import ROUND_FLOOR, Decimal
from enum import StrEnum
from typing import TypeAlias

NumericInput: TypeAlias = Decimal | float | int | str


class RiskAction(StrEnum):
    """Actions understood by the risk gate."""

    BUY = "BUY"
    SELL = "SELL"
    EXIT = "EXIT"
    HOLD = "HOLD"


class RiskPositionSide(StrEnum):
    """Normalized current position side."""

    FLAT = "FLAT"
    LONG = "LONG"
    SHORT = "SHORT"


class RiskReasonCode(StrEnum):
    """Machine-readable risk decision reasons."""

    APPROVED = "APPROVED"
    HOLD_PASSTHROUGH = "HOLD_PASSTHROUGH"
    EXIT_ALLOWED = "EXIT_ALLOWED"
    KILL_SWITCH = "KILL_SWITCH"
    DAILY_LOSS_LIMIT = "DAILY_LOSS_LIMIT"
    TRADE_COUNT_LIMIT = "TRADE_COUNT_LIMIT"
    NO_NEW_ENTRIES_TIME = "NO_NEW_ENTRIES_TIME"
    FORCE_FLATTEN_TIME = "FORCE_FLATTEN_TIME"
    POSITION_FLATTEN_REQUIRED = "POSITION_FLATTEN_REQUIRED"
    POSITION_ALREADY_OPEN = "POSITION_ALREADY_OPEN"
    STALE_MARKET_DATA = "STALE_MARKET_DATA"
    SPREAD_LIMIT = "SPREAD_LIMIT"
    SLIPPAGE_LIMIT = "SLIPPAGE_LIMIT"
    MISSING_ENTRY_PRICE = "MISSING_ENTRY_PRICE"
    MISSING_STOP_PRICE = "MISSING_STOP_PRICE"
    INVALID_POSITION_SIZE = "INVALID_POSITION_SIZE"


@dataclass(frozen=True)
class RiskPosition:
    """Current position snapshot."""

    side: RiskPositionSide = RiskPositionSide.FLAT
    quantity: Decimal = Decimal("0")

    @property
    def is_flat(self) -> bool:
        return self.side is RiskPositionSide.FLAT or self.quantity == 0


@dataclass(frozen=True)
class RiskConfig:
    """Configuration for risk checks and position sizing."""

    max_daily_loss: Decimal = Decimal("1000")
    max_trades_per_day: int = 10
    no_new_entries_after: time = time(18, 30)
    force_flatten_at: time = time(18, 40)
    max_market_data_stale_seconds: Decimal = Decimal("5")
    max_spread_bps: Decimal = Decimal("15")
    max_slippage_bps: Decimal = Decimal("20")
    risk_per_trade_fraction: Decimal = Decimal("0.01")
    max_position_notional_fraction: Decimal = Decimal("1")
    quantity_step: Decimal = Decimal("1")
    min_quantity: Decimal = Decimal("0")
    kill_switch: bool = False
    flatten_on_daily_loss: bool = True

    def __post_init__(self) -> None:
        if self.max_daily_loss <= 0:
            raise ValueError("max_daily_loss must be positive.")
        if self.max_trades_per_day < 0:
            raise ValueError("max_trades_per_day must be non-negative.")
        if self.max_market_data_stale_seconds <= 0:
            raise ValueError("max_market_data_stale_seconds must be positive.")
        if self.max_spread_bps <= 0:
            raise ValueError("max_spread_bps must be positive.")
        if self.max_slippage_bps <= 0:
            raise ValueError("max_slippage_bps must be positive.")
        if not Decimal("0") < self.risk_per_trade_fraction <= Decimal("1"):
            raise ValueError("risk_per_trade_fraction must be in (0, 1].")
        if self.max_position_notional_fraction <= 0:
            raise ValueError("max_position_notional_fraction must be positive.")
        if self.quantity_step <= 0:
            raise ValueError("quantity_step must be positive.")
        if self.min_quantity < 0:
            raise ValueError("min_quantity must be non-negative.")


@dataclass(frozen=True)
class RiskState:
    """Runtime risk context for one decision."""

    account_equity: Decimal
    daily_realized_pnl: Decimal
    trades_today: int
    market_data_last_seen_at: datetime | None
    spread_bps: Decimal
    expected_slippage_bps: Decimal | None = None
    position: RiskPosition = field(default_factory=RiskPosition)


@dataclass(frozen=True)
class RiskDecision:
    """Risk decision output."""

    action: RiskAction
    approved: bool
    reason_codes: tuple[RiskReasonCode, ...]
    position_size: Decimal = Decimal("0")
    max_notional: Decimal = Decimal("0")


class RiskManager:
    """Apply risk checks and calculate entry sizes."""

    def __init__(self, config: RiskConfig | None = None) -> None:
        self.config = config or RiskConfig()

    def evaluate(
        self,
        *,
        desired_action: RiskAction | str | object,
        state: RiskState,
        current_time: datetime,
        entry_price: NumericInput | None = None,
        stop_price: NumericInput | None = None,
        config: RiskConfig | None = None,
    ) -> RiskDecision:
        """Return the risk-approved action and optional entry quantity."""

        resolved_config = config or self.config
        action = _normalize_action(desired_action)

        if resolved_config.kill_switch:
            return _flatten_or_block(
                state,
                force_reason=RiskReasonCode.KILL_SWITCH,
            )

        if _force_flatten_due(current_time, resolved_config):
            if not state.position.is_flat:
                return RiskDecision(
                    action=RiskAction.EXIT,
                    approved=True,
                    reason_codes=(
                        RiskReasonCode.FORCE_FLATTEN_TIME,
                        RiskReasonCode.POSITION_FLATTEN_REQUIRED,
                    ),
                    position_size=state.position.quantity,
                    max_notional=state.account_equity
                    * resolved_config.max_position_notional_fraction,
                )
            if _is_entry(action):
                return RiskDecision(
                    action=RiskAction.HOLD,
                    approved=False,
                    reason_codes=(RiskReasonCode.FORCE_FLATTEN_TIME,),
                )

        if _daily_loss_exceeded(state, resolved_config):
            if not state.position.is_flat and resolved_config.flatten_on_daily_loss:
                return RiskDecision(
                    action=RiskAction.EXIT,
                    approved=True,
                    reason_codes=(
                        RiskReasonCode.DAILY_LOSS_LIMIT,
                        RiskReasonCode.POSITION_FLATTEN_REQUIRED,
                    ),
                    position_size=state.position.quantity,
                    max_notional=state.account_equity
                    * resolved_config.max_position_notional_fraction,
                )
            if _is_entry(action):
                return RiskDecision(
                    action=RiskAction.HOLD,
                    approved=False,
                    reason_codes=(RiskReasonCode.DAILY_LOSS_LIMIT,),
                )

        if action is RiskAction.EXIT:
            return RiskDecision(
                action=RiskAction.EXIT,
                approved=True,
                reason_codes=(RiskReasonCode.EXIT_ALLOWED,),
                position_size=state.position.quantity,
                max_notional=state.account_equity * resolved_config.max_position_notional_fraction,
            )

        if action is RiskAction.HOLD:
            return RiskDecision(
                action=RiskAction.HOLD,
                approved=True,
                reason_codes=(RiskReasonCode.HOLD_PASSTHROUGH,),
            )

        if _is_entry(action) and not state.position.is_flat:
            return RiskDecision(
                action=RiskAction.HOLD,
                approved=False,
                reason_codes=(RiskReasonCode.POSITION_ALREADY_OPEN,),
            )

        block_reason = _entry_block_reason(
            state=state,
            current_time=current_time,
            config=resolved_config,
        )
        if block_reason is not None:
            return RiskDecision(
                action=RiskAction.HOLD,
                approved=False,
                reason_codes=(block_reason,),
            )

        if entry_price is None:
            return RiskDecision(
                action=RiskAction.HOLD,
                approved=False,
                reason_codes=(RiskReasonCode.MISSING_ENTRY_PRICE,),
            )
        if stop_price is None:
            return RiskDecision(
                action=RiskAction.HOLD,
                approved=False,
                reason_codes=(RiskReasonCode.MISSING_STOP_PRICE,),
            )

        position_size = self.calculate_position_size(
            account_equity=state.account_equity,
            entry_price=entry_price,
            stop_price=stop_price,
            config=resolved_config,
        )
        max_notional = state.account_equity * resolved_config.max_position_notional_fraction
        if position_size <= 0:
            return RiskDecision(
                action=RiskAction.HOLD,
                approved=False,
                reason_codes=(RiskReasonCode.INVALID_POSITION_SIZE,),
                max_notional=max_notional,
            )

        return RiskDecision(
            action=action,
            approved=True,
            reason_codes=(RiskReasonCode.APPROVED,),
            position_size=position_size,
            max_notional=max_notional,
        )

    def calculate_position_size(
        self,
        *,
        account_equity: NumericInput,
        entry_price: NumericInput,
        stop_price: NumericInput,
        config: RiskConfig | None = None,
    ) -> Decimal:
        """Calculate position size from risk amount and stop distance."""

        resolved_config = config or self.config
        equity = _positive_decimal(account_equity, "account_equity")
        entry = _positive_decimal(entry_price, "entry_price")
        stop = _positive_decimal(stop_price, "stop_price")
        risk_per_unit = abs(entry - stop)
        if risk_per_unit == 0:
            return Decimal("0")

        risk_amount = equity * resolved_config.risk_per_trade_fraction
        risk_limited_quantity = risk_amount / risk_per_unit
        max_notional = equity * resolved_config.max_position_notional_fraction
        notional_limited_quantity = max_notional / entry
        raw_quantity = min(risk_limited_quantity, notional_limited_quantity)
        quantity = _floor_to_step(raw_quantity, resolved_config.quantity_step)

        if quantity < resolved_config.min_quantity:
            return Decimal("0")
        return quantity


def _flatten_or_block(
    state: RiskState,
    *,
    force_reason: RiskReasonCode,
) -> RiskDecision:
    if not state.position.is_flat:
        return RiskDecision(
            action=RiskAction.EXIT,
            approved=True,
            reason_codes=(force_reason, RiskReasonCode.POSITION_FLATTEN_REQUIRED),
            position_size=state.position.quantity,
            max_notional=state.account_equity,
        )
    return RiskDecision(
        action=RiskAction.HOLD,
        approved=False,
        reason_codes=(force_reason,),
    )


def _entry_block_reason(
    *,
    state: RiskState,
    current_time: datetime,
    config: RiskConfig,
) -> RiskReasonCode | None:
    if state.trades_today >= config.max_trades_per_day:
        return RiskReasonCode.TRADE_COUNT_LIMIT
    if current_time.time() >= config.no_new_entries_after:
        return RiskReasonCode.NO_NEW_ENTRIES_TIME
    if _market_data_is_stale(state.market_data_last_seen_at, current_time, config):
        return RiskReasonCode.STALE_MARKET_DATA
    if state.spread_bps > config.max_spread_bps:
        return RiskReasonCode.SPREAD_LIMIT
    if (
        state.expected_slippage_bps is not None
        and state.expected_slippage_bps > config.max_slippage_bps
    ):
        return RiskReasonCode.SLIPPAGE_LIMIT
    return None


def _daily_loss_exceeded(state: RiskState, config: RiskConfig) -> bool:
    return state.daily_realized_pnl <= -config.max_daily_loss


def _force_flatten_due(current_time: datetime, config: RiskConfig) -> bool:
    return current_time.time() >= config.force_flatten_at


def _market_data_is_stale(
    last_seen_at: datetime | None,
    current_time: datetime,
    config: RiskConfig,
) -> bool:
    if last_seen_at is None:
        return True
    stale_seconds = Decimal(str((current_time - last_seen_at).total_seconds()))
    return stale_seconds > config.max_market_data_stale_seconds


def _is_entry(action: RiskAction) -> bool:
    return action in {RiskAction.BUY, RiskAction.SELL}


def _normalize_action(value: RiskAction | str | object) -> RiskAction:
    if isinstance(value, RiskAction):
        return value
    raw_value = getattr(value, "value", value)
    try:
        return RiskAction(str(raw_value).upper())
    except ValueError as exc:
        allowed = ", ".join(action.value for action in RiskAction)
        raise ValueError(f"unsupported risk action: {value!r}. Allowed: {allowed}.") from exc


def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    steps = (value / step).to_integral_value(rounding=ROUND_FLOOR)
    return steps * step


def _positive_decimal(value: object, field_name: str) -> Decimal:
    decimal_value = _to_decimal(value)
    if decimal_value <= 0:
        raise ValueError(f"{field_name} must be positive.")
    return decimal_value


def _to_decimal(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        raise TypeError("boolean values are not valid numeric risk values.")
    if isinstance(value, int | str):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))
    raise TypeError(f"unsupported numeric value: {value!r}.")


__all__ = [
    "RiskAction",
    "RiskConfig",
    "RiskDecision",
    "RiskManager",
    "RiskPosition",
    "RiskPositionSide",
    "RiskReasonCode",
    "RiskState",
]
