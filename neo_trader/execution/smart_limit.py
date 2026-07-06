"""Smart limit execution primitives.

This module contains execution orchestration only. It does not implement a live
broker adapter; callers must provide a gateway implementation explicitly.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Protocol, TypeAlias
from uuid import uuid4

from neo_trader.config import Settings, get_settings
from neo_trader.risk.manager import (
    RiskAction,
    RiskDecision,
    RiskManager,
    RiskPosition,
    RiskPositionSide,
    RiskState,
)
from neo_trader.runtime import get_runtime_commit_hash

NumericInput: TypeAlias = Decimal | float | int | str
UuidFactory: TypeAlias = Callable[[], str]

BPS_FACTOR = Decimal("10000")


class ExecutionSide(StrEnum):
    """Order side."""

    BUY = "BUY"
    SELL = "SELL"


class ExecutionOrderType(StrEnum):
    """Supported order types."""

    LIMIT = "LIMIT"
    MARKET = "MARKET"


class ExecutionOrderStatus(StrEnum):
    """Tracked order status."""

    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"


class TradingStatus(StrEnum):
    """Minimal trading status categories used before order submission."""

    TRADING = "TRADING"
    CLOSED = "CLOSED"
    HALTED = "HALTED"
    UNKNOWN = "UNKNOWN"


class ExecutionReasonCode(StrEnum):
    """Machine-readable execution reasons."""

    SUBMITTED = "SUBMITTED"
    CANCEL_REPLACE_SUBMITTED = "CANCEL_REPLACE_SUBMITTED"
    CANCEL_REMAINING_SUBMITTED = "CANCEL_REMAINING_SUBMITTED"
    PARTIAL_FILL = "PARTIAL_FILL"
    FILLED = "FILLED"
    LIVE_TRADING_DISABLED = "LIVE_TRADING_DISABLED"
    MARKET_ORDER_FORBIDDEN = "MARKET_ORDER_FORBIDDEN"
    TRADING_STATUS_BLOCKED = "TRADING_STATUS_BLOCKED"
    RISK_REJECTED = "RISK_REJECTED"
    QUANTITY_EXCEEDS_RISK_APPROVAL = "QUANTITY_EXCEEDS_RISK_APPROVAL"
    NO_POSITION_TO_EXIT = "NO_POSITION_TO_EXIT"
    EMERGENCY_EXIT_SIDE_DERIVED = "EMERGENCY_EXIT_SIDE_DERIVED"
    INVALID_POSITION_SIDE = "INVALID_POSITION_SIDE"
    ORDER_NOT_FOUND = "ORDER_NOT_FOUND"
    ORDER_ALREADY_TERMINAL = "ORDER_ALREADY_TERMINAL"
    INVALID_FILL = "INVALID_FILL"


@dataclass(frozen=True)
class MarketQuote:
    """Top-of-book quote used to create marketable limits."""

    best_bid: Decimal
    best_ask: Decimal

    def __post_init__(self) -> None:
        if self.best_bid <= 0:
            raise ValueError("best_bid must be positive.")
        if self.best_ask <= 0:
            raise ValueError("best_ask must be positive.")
        if self.best_ask < self.best_bid:
            raise ValueError("best_ask must be >= best_bid.")


@dataclass(frozen=True)
class TradingStatusSnapshot:
    """Trading status returned by a gateway."""

    status: TradingStatus
    raw_status: str | None = None

    @property
    def is_trading(self) -> bool:
        return self.status is TradingStatus.TRADING


@dataclass(frozen=True)
class OrderRequest:
    """Gateway order request."""

    account_ref: str
    instrument_id: str
    side: ExecutionSide
    order_type: ExecutionOrderType
    quantity: Decimal
    price: Decimal | None
    idempotency_key: str


@dataclass(frozen=True)
class BrokerOrderAck:
    """Gateway order acknowledgement."""

    order_id: str
    status: ExecutionOrderStatus = ExecutionOrderStatus.NEW


class ExecutionGateway(Protocol):
    """Broker gateway protocol consumed by SmartLimitExecutor."""

    is_live: bool

    def get_trading_status(self, instrument_id: str) -> TradingStatusSnapshot:
        """Return current trading status for an instrument."""

    def submit_order(self, request: OrderRequest) -> BrokerOrderAck:
        """Submit an order request."""

    def cancel_order(self, order_id: str) -> None:
        """Cancel an active order."""


@dataclass
class ActiveOrder:
    """Executor-side order state."""

    order_id: str
    account_ref: str
    instrument_id: str
    side: ExecutionSide
    order_type: ExecutionOrderType
    requested_quantity: Decimal
    remaining_quantity: Decimal
    limit_price: Decimal | None
    idempotency_key: str
    status: ExecutionOrderStatus = ExecutionOrderStatus.NEW
    filled_quantity: Decimal = Decimal("0")
    avg_fill_price: Decimal | None = None
    parent_order_id: str | None = None

    @property
    def is_terminal(self) -> bool:
        return self.status in {
            ExecutionOrderStatus.FILLED,
            ExecutionOrderStatus.CANCELED,
            ExecutionOrderStatus.REJECTED,
        }


@dataclass(frozen=True)
class ExecutionReport:
    """Executor decision and order state output."""

    accepted: bool
    action: ExecutionSide | None
    reason_codes: tuple[ExecutionReasonCode, ...]
    commit_hash: str = field(default_factory=get_runtime_commit_hash)
    order_id: str | None = None
    idempotency_key: str | None = None
    order_type: ExecutionOrderType | None = None
    limit_price: Decimal | None = None
    requested_quantity: Decimal = Decimal("0")
    filled_quantity: Decimal = Decimal("0")
    remaining_quantity: Decimal = Decimal("0")
    avg_fill_price: Decimal | None = None
    risk_decision: RiskDecision | None = None


@dataclass(frozen=True)
class SmartLimitExecutorConfig:
    """Configuration for smart limit execution."""

    marketable_limit_offset_bps: Decimal = Decimal("1")
    live_trading_enabled: bool = False

    def __post_init__(self) -> None:
        if self.marketable_limit_offset_bps < 0:
            raise ValueError("marketable_limit_offset_bps must be non-negative.")


class SmartLimitExecutor:
    """Marketable-limit executor with risk and trading-status gates."""

    def __init__(
        self,
        *,
        gateway: ExecutionGateway,
        risk_manager: RiskManager,
        config: SmartLimitExecutorConfig | None = None,
        settings: Settings | None = None,
        uuid_factory: UuidFactory | None = None,
    ) -> None:
        resolved_settings = settings or get_settings()
        self.gateway = gateway
        self.risk_manager = risk_manager
        self.config = config or SmartLimitExecutorConfig(
            live_trading_enabled=resolved_settings.live_trading_enabled,
        )
        self.uuid_factory = uuid_factory or _uuid4_string
        self._orders: dict[str, ActiveOrder] = {}

    @property
    def active_orders(self) -> tuple[ActiveOrder, ...]:
        return tuple(self._orders.values())

    def enter_marketable_limit(
        self,
        *,
        account_ref: str,
        instrument_id: str,
        side: ExecutionSide | str,
        quote: MarketQuote,
        risk_state: RiskState,
        current_time: datetime,
        stop_price: NumericInput,
        quantity_cap: NumericInput | None = None,
    ) -> ExecutionReport:
        """Submit a marketable limit entry after status and risk checks."""

        resolved_side = _normalize_side(side)
        limit_price = self._marketable_limit_price(resolved_side, quote)
        risk_decision = self.risk_manager.evaluate(
            desired_action=RiskAction(resolved_side.value),
            state=risk_state,
            current_time=current_time,
            entry_price=limit_price,
            stop_price=stop_price,
        )
        if not risk_decision.approved:
            return _rejected(
                ExecutionReasonCode.RISK_REJECTED,
                action=resolved_side,
                risk_decision=risk_decision,
            )

        quantity = risk_decision.position_size
        if quantity_cap is not None:
            quantity = min(quantity, _positive_decimal(quantity_cap, "quantity_cap"))
        if quantity <= 0:
            return _rejected(
                ExecutionReasonCode.RISK_REJECTED,
                action=resolved_side,
                risk_decision=risk_decision,
            )

        return self._submit_checked_order(
            account_ref=account_ref,
            instrument_id=instrument_id,
            side=resolved_side,
            order_type=ExecutionOrderType.LIMIT,
            quantity=quantity,
            price=limit_price,
            risk_decision=risk_decision,
            emergency_exit=False,
        )

    def cancel_replace(
        self,
        *,
        order_id: str,
        quote: MarketQuote,
        risk_state: RiskState,
        current_time: datetime,
        stop_price: NumericInput,
    ) -> ExecutionReport:
        """Cancel an active order and replace the unfilled quantity."""

        existing_order = self._orders.get(order_id)
        if existing_order is None:
            return _rejected(ExecutionReasonCode.ORDER_NOT_FOUND)
        if existing_order.is_terminal:
            return _order_report(
                existing_order,
                accepted=False,
                reason_codes=(ExecutionReasonCode.ORDER_ALREADY_TERMINAL,),
            )

        limit_price = self._marketable_limit_price(existing_order.side, quote)
        risk_decision = self.risk_manager.evaluate(
            desired_action=RiskAction(existing_order.side.value),
            state=risk_state,
            current_time=current_time,
            entry_price=limit_price,
            stop_price=stop_price,
        )
        if not risk_decision.approved:
            return _order_report(
                existing_order,
                accepted=False,
                reason_codes=(ExecutionReasonCode.RISK_REJECTED,),
                risk_decision=risk_decision,
            )

        replace_quantity = min(existing_order.remaining_quantity, risk_decision.position_size)
        if replace_quantity <= 0:
            return _order_report(
                existing_order,
                accepted=False,
                reason_codes=(ExecutionReasonCode.RISK_REJECTED,),
                risk_decision=risk_decision,
            )

        self.gateway.cancel_order(order_id)
        existing_order.status = ExecutionOrderStatus.CANCELED
        existing_order.remaining_quantity = Decimal("0")

        report = self._submit_checked_order(
            account_ref=existing_order.account_ref,
            instrument_id=existing_order.instrument_id,
            side=existing_order.side,
            order_type=ExecutionOrderType.LIMIT,
            quantity=replace_quantity,
            price=limit_price,
            risk_decision=risk_decision,
            emergency_exit=False,
            parent_order_id=order_id,
        )
        if report.accepted:
            return ExecutionReport(
                accepted=True,
                action=report.action,
                reason_codes=(ExecutionReasonCode.CANCEL_REPLACE_SUBMITTED,),
                order_id=report.order_id,
                idempotency_key=report.idempotency_key,
                order_type=report.order_type,
                limit_price=report.limit_price,
                requested_quantity=report.requested_quantity,
                filled_quantity=report.filled_quantity,
                remaining_quantity=report.remaining_quantity,
                avg_fill_price=report.avg_fill_price,
                risk_decision=report.risk_decision,
            )
        return report

    def cancel_remaining(self, *, order_id: str) -> ExecutionReport:
        """Cancel the unfilled remainder of an active order.

        This is always allowed because it reduces outstanding entry risk and
        does not submit a replacement order.
        """

        existing_order = self._orders.get(order_id)
        if existing_order is None:
            return _rejected(ExecutionReasonCode.ORDER_NOT_FOUND)
        if existing_order.is_terminal:
            return _order_report(
                existing_order,
                accepted=False,
                reason_codes=(ExecutionReasonCode.ORDER_ALREADY_TERMINAL,),
            )

        self.gateway.cancel_order(order_id)
        existing_order.status = ExecutionOrderStatus.CANCELED
        existing_order.remaining_quantity = Decimal("0")
        return _order_report(
            existing_order,
            accepted=True,
            reason_codes=(ExecutionReasonCode.CANCEL_REMAINING_SUBMITTED,),
        )

    def record_fill(
        self,
        *,
        order_id: str,
        fill_quantity: NumericInput,
        fill_price: NumericInput,
    ) -> ExecutionReport:
        """Apply a fill event and update partial-fill accounting."""

        order = self._orders.get(order_id)
        if order is None:
            return _rejected(ExecutionReasonCode.ORDER_NOT_FOUND)
        if order.is_terminal:
            return _order_report(
                order,
                accepted=False,
                reason_codes=(ExecutionReasonCode.ORDER_ALREADY_TERMINAL,),
            )

        quantity = _positive_decimal(fill_quantity, "fill_quantity")
        price = _positive_decimal(fill_price, "fill_price")
        if quantity > order.remaining_quantity:
            return _order_report(
                order,
                accepted=False,
                reason_codes=(ExecutionReasonCode.INVALID_FILL,),
            )

        previous_notional = (order.avg_fill_price or Decimal("0")) * order.filled_quantity
        new_notional = previous_notional + (price * quantity)
        order.filled_quantity += quantity
        order.remaining_quantity -= quantity
        order.avg_fill_price = new_notional / order.filled_quantity
        order.status = (
            ExecutionOrderStatus.FILLED
            if order.remaining_quantity == 0
            else ExecutionOrderStatus.PARTIALLY_FILLED
        )
        reason = (
            ExecutionReasonCode.FILLED
            if order.status is ExecutionOrderStatus.FILLED
            else ExecutionReasonCode.PARTIAL_FILL
        )
        return _order_report(order, accepted=True, reason_codes=(reason,))

    def emergency_exit(
        self,
        *,
        account_ref: str,
        instrument_id: str,
        quantity: NumericInput,
        risk_state: RiskState,
        current_time: datetime,
    ) -> ExecutionReport:
        """Submit an emergency market exit derived from the current position."""

        derived_side = _derive_emergency_exit_side(risk_state.position)
        if isinstance(derived_side, ExecutionReasonCode):
            return _rejected(derived_side)

        risk_decision = self.risk_manager.evaluate(
            desired_action=RiskAction.EXIT,
            state=risk_state,
            current_time=current_time,
        )
        if not risk_decision.approved:
            return _rejected(
                ExecutionReasonCode.RISK_REJECTED,
                action=derived_side,
                risk_decision=risk_decision,
            )

        requested_quantity = _positive_decimal(quantity, "quantity")
        exit_quantity = min(requested_quantity, risk_decision.position_size)
        if exit_quantity <= 0:
            return _rejected(
                ExecutionReasonCode.NO_POSITION_TO_EXIT,
                action=derived_side,
                risk_decision=risk_decision,
            )

        report = self._submit_checked_order(
            account_ref=account_ref,
            instrument_id=instrument_id,
            side=derived_side,
            order_type=ExecutionOrderType.MARKET,
            quantity=exit_quantity,
            price=None,
            risk_decision=risk_decision,
            emergency_exit=True,
        )
        if not report.accepted:
            return report
        return ExecutionReport(
            accepted=True,
            action=report.action,
            reason_codes=(
                ExecutionReasonCode.EMERGENCY_EXIT_SIDE_DERIVED,
                *report.reason_codes,
            ),
            order_id=report.order_id,
            idempotency_key=report.idempotency_key,
            order_type=report.order_type,
            limit_price=report.limit_price,
            requested_quantity=report.requested_quantity,
            filled_quantity=report.filled_quantity,
            remaining_quantity=report.remaining_quantity,
            avg_fill_price=report.avg_fill_price,
            risk_decision=report.risk_decision,
        )

    def _submit_order_test_only(
        self,
        *,
        account_ref: str,
        instrument_id: str,
        side: ExecutionSide | str,
        order_type: ExecutionOrderType | str,
        quantity: NumericInput,
        price: NumericInput | None,
        risk_decision: RiskDecision,
        emergency_exit: bool = False,
    ) -> ExecutionReport:
        """Internal test-only hook for submitting a pre-risked order request.

        Production callers must use ``enter_marketable_limit``,
        ``cancel_replace``, or ``emergency_exit``. This method exists only to
        unit-test shared safety gates that sit below the public API.
        """

        resolved_side = _normalize_side(side)
        resolved_order_type = _normalize_order_type(order_type)
        order_price = None if price is None else _positive_decimal(price, "price")
        return self._submit_checked_order(
            account_ref=account_ref,
            instrument_id=instrument_id,
            side=resolved_side,
            order_type=resolved_order_type,
            quantity=_positive_decimal(quantity, "quantity"),
            price=order_price,
            risk_decision=risk_decision,
            emergency_exit=emergency_exit,
        )

    def _submit_checked_order(
        self,
        *,
        account_ref: str,
        instrument_id: str,
        side: ExecutionSide,
        order_type: ExecutionOrderType,
        quantity: Decimal,
        price: Decimal | None,
        risk_decision: RiskDecision,
        emergency_exit: bool,
        parent_order_id: str | None = None,
    ) -> ExecutionReport:
        if not risk_decision.approved:
            return _rejected(
                ExecutionReasonCode.RISK_REJECTED,
                action=side,
                risk_decision=risk_decision,
            )
        if quantity > risk_decision.position_size:
            return _rejected(
                ExecutionReasonCode.QUANTITY_EXCEEDS_RISK_APPROVAL,
                action=side,
                risk_decision=risk_decision,
            )
        if order_type is ExecutionOrderType.MARKET and not emergency_exit:
            return _rejected(ExecutionReasonCode.MARKET_ORDER_FORBIDDEN, action=side)
        if self.gateway.is_live and not self.config.live_trading_enabled:
            return _rejected(ExecutionReasonCode.LIVE_TRADING_DISABLED, action=side)

        trading_status = self.gateway.get_trading_status(instrument_id)
        if not trading_status.is_trading:
            return _rejected(ExecutionReasonCode.TRADING_STATUS_BLOCKED, action=side)

        idempotency_key = self.uuid_factory()
        request = OrderRequest(
            account_ref=account_ref,
            instrument_id=instrument_id,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            idempotency_key=idempotency_key,
        )
        ack = self.gateway.submit_order(request)
        order = ActiveOrder(
            order_id=ack.order_id,
            account_ref=account_ref,
            instrument_id=instrument_id,
            side=side,
            order_type=order_type,
            requested_quantity=quantity,
            remaining_quantity=quantity,
            limit_price=price,
            idempotency_key=idempotency_key,
            status=ack.status,
            parent_order_id=parent_order_id,
        )
        self._orders[order.order_id] = order
        return _order_report(
            order,
            accepted=True,
            reason_codes=(ExecutionReasonCode.SUBMITTED,),
            risk_decision=risk_decision,
        )

    def _marketable_limit_price(self, side: ExecutionSide, quote: MarketQuote) -> Decimal:
        offset = self.config.marketable_limit_offset_bps / BPS_FACTOR
        if side is ExecutionSide.BUY:
            return quote.best_ask * (Decimal("1") + offset)
        return quote.best_bid * (Decimal("1") - offset)


def _order_report(
    order: ActiveOrder,
    *,
    accepted: bool,
    reason_codes: tuple[ExecutionReasonCode, ...],
    risk_decision: RiskDecision | None = None,
) -> ExecutionReport:
    return ExecutionReport(
        accepted=accepted,
        action=order.side,
        reason_codes=reason_codes,
        order_id=order.order_id,
        idempotency_key=order.idempotency_key,
        order_type=order.order_type,
        limit_price=order.limit_price,
        requested_quantity=order.requested_quantity,
        filled_quantity=order.filled_quantity,
        remaining_quantity=order.remaining_quantity,
        avg_fill_price=order.avg_fill_price,
        risk_decision=risk_decision,
    )


def _rejected(
    reason: ExecutionReasonCode,
    *,
    action: ExecutionSide | None = None,
    risk_decision: RiskDecision | None = None,
) -> ExecutionReport:
    return ExecutionReport(
        accepted=False,
        action=action,
        reason_codes=(reason,),
        risk_decision=risk_decision,
    )


def _uuid4_string() -> str:
    return str(uuid4())


def _normalize_side(value: ExecutionSide | str) -> ExecutionSide:
    if isinstance(value, ExecutionSide):
        return value
    return ExecutionSide(str(value).upper())


def _normalize_order_type(value: ExecutionOrderType | str) -> ExecutionOrderType:
    if isinstance(value, ExecutionOrderType):
        return value
    return ExecutionOrderType(str(value).upper())


def _derive_emergency_exit_side(position: RiskPosition) -> ExecutionSide | ExecutionReasonCode:
    if position.quantity <= 0 or position.is_flat:
        return ExecutionReasonCode.NO_POSITION_TO_EXIT
    try:
        position_side = RiskPositionSide(str(position.side).upper())
    except ValueError:
        return ExecutionReasonCode.INVALID_POSITION_SIDE
    if position_side is RiskPositionSide.LONG:
        return ExecutionSide.SELL
    if position_side is RiskPositionSide.SHORT:
        return ExecutionSide.BUY
    if position_side is RiskPositionSide.FLAT:
        return ExecutionReasonCode.NO_POSITION_TO_EXIT
    return ExecutionReasonCode.INVALID_POSITION_SIDE


def _positive_decimal(value: object, field_name: str) -> Decimal:
    decimal_value = _to_decimal(value)
    if decimal_value <= 0:
        raise ValueError(f"{field_name} must be positive.")
    return decimal_value


def _to_decimal(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        raise TypeError("boolean values are not valid numeric execution values.")
    if isinstance(value, int | str):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))
    raise TypeError(f"unsupported numeric value: {value!r}.")


__all__ = [
    "ActiveOrder",
    "BrokerOrderAck",
    "ExecutionGateway",
    "ExecutionOrderStatus",
    "ExecutionOrderType",
    "ExecutionReasonCode",
    "ExecutionReport",
    "ExecutionSide",
    "MarketQuote",
    "OrderRequest",
    "SmartLimitExecutor",
    "SmartLimitExecutorConfig",
    "TradingStatus",
    "TradingStatusSnapshot",
]
