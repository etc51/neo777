"""Broker-agnostic domain types for the Neobitcoin paper trading service.

The module intentionally contains no broker SDK imports.  Runtime strategies
can describe an intent, while only the paper execution engine may turn that
intent into the immutable order/fill/position records defined here.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from types import MappingProxyType
from typing import Any, TypeAlias

DecimalInput: TypeAlias = Decimal | int | str


class DomainValidationError(ValueError):
    """A domain object would violate a paper-trading invariant."""


class LookAheadError(DomainValidationError):
    """An event was used before it was available to the paper service."""


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"

    @property
    def opposite(self) -> Side:
        return Side.SELL if self is Side.BUY else Side.BUY


class ExecutionModel(StrEnum):
    AGGRESSIVE = "AGGRESSIVE"
    PASSIVE = "PASSIVE"


class FillStatus(StrEnum):
    FULL_FILL = "FULL_FILL"
    PARTIAL_FILL = "PARTIAL_FILL"
    NO_FILL = "NO_FILL"


class OrderStatus(StrEnum):
    PENDING = "PENDING"
    FULL_FILL = "FULL_FILL"
    PARTIAL_FILL = "PARTIAL_FILL"
    NO_FILL = "NO_FILL"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"


class PositionStatus(StrEnum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"


class StrategyStatus(StrEnum):
    DISCOVERY = "DISCOVERY"
    FROZEN_PAPER = "FROZEN_PAPER"
    FROZEN_PAPER_SECONDARY = "FROZEN_PAPER_SECONDARY"
    FROZEN_PAPER_DEGRADED_CONTINUE_OOS = "FROZEN_PAPER_DEGRADED_CONTINUE_OOS"
    FROZEN_PAPER_OOS_ACCUMULATION = "FROZEN_PAPER_OOS_ACCUMULATION"
    PAUSE_NEW_ENTRIES_OOS_FAILURE = "PAUSE_NEW_ENTRIES_OOS_FAILURE"
    OOS_FAIL_DAY1_DO_NOT_ACTIVATE = "OOS_FAIL_DAY1_DO_NOT_ACTIVATE"
    OBSERVATION_ONLY_NOT_FROZEN = "OBSERVATION_ONLY_NOT_FROZEN"
    OOS_ACCUMULATION = "OOS_ACCUMULATION"
    PAPER_VALIDATED = "PAPER_VALIDATED"
    CONTEXT_ONLY = "CONTEXT_ONLY"
    REJECTED_OOS_AS_FORMALIZED = "REJECTED_OOS_AS_FORMALIZED"
    PAUSED = "PAUSED"
    REJECTED = "REJECTED"
    ARCHIVED = "ARCHIVED"


class DataQuality(StrEnum):
    GOOD = "GOOD"
    STALE = "STALE"
    GAP = "GAP"
    INVALID_BOOK = "INVALID_BOOK"
    EXCESSIVE_LATENCY = "EXCESSIVE_LATENCY"
    UNKNOWN = "UNKNOWN"


class SessionLabel(StrEnum):
    PRE_OPEN = "PRE_OPEN"
    MORNING = "MORNING"
    MAIN = "MAIN"
    EVENING = "EVENING"
    NEO_LATE_SESSION = "NEO_LATE_SESSION"
    WEEKEND = "WEEKEND"
    HOLIDAY = "HOLIDAY"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"
    BREAK = "BREAK"
    DEALER_MODE = "DEALER_MODE"
    UNKNOWN = "UNKNOWN"


class ExitReason(StrEnum):
    FIXED_STOP = "FIXED_STOP"
    TAKE_PROFIT = "TAKE_PROFIT"
    TIME_EXIT = "TIME_EXIT"
    BREAKEVEN = "BREAKEVEN"
    TRAILING = "TRAILING"
    MICROSTRUCTURE = "MICROSTRUCTURE"
    ORDER_BOOK = "ORDER_BOOK"
    SESSION_END = "SESSION_END"
    MANUAL = "MANUAL"


_OPEN_TRADING_STATUSES = frozenset(
    {
        "NORMAL_TRADING",
        "TRADING_AT_CLOSING_AUCTION_PRICE",
        "DEALER_NORMAL_TRADING",
        "OPEN",
    }
)


def trading_status_is_open(value: str | None) -> bool:
    """Return whether a normalized live status permits an executable entry."""

    return value is not None and value.strip().upper() in _OPEN_TRADING_STATUSES


def as_utc(value: datetime, field_name: str = "timestamp") -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise DomainValidationError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def decimal_value(value: DecimalInput, field_name: str) -> Decimal:
    if isinstance(value, bool):
        raise TypeError(f"{field_name} must not be bool")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise DomainValidationError(f"{field_name} is not decimal-compatible") from exc
    if not result.is_finite():
        raise DomainValidationError(f"{field_name} must be finite")
    return result


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze(item) for key, item in value.items()})
    if isinstance(value, list | tuple):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, set | frozenset):
        return frozenset(_freeze(item) for item in value)
    return value


def _canonical(value: Any) -> Any:
    if isinstance(value, datetime):
        return as_utc(value).isoformat(timespec="microseconds")
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _canonical(item) for key, item in sorted(value.items())}
    if isinstance(value, tuple | list):
        return [_canonical(item) for item in value]
    if isinstance(value, frozenset | set):
        return sorted((_canonical(item) for item in value), key=repr)
    return value


def deterministic_id(namespace: str, *parts: Any) -> str:
    """Build a stable ID from point-in-time inputs, never process randomness."""

    namespace = namespace.strip().lower()
    if not namespace:
        raise DomainValidationError("ID namespace must not be blank")
    payload = json.dumps(
        _canonical(parts),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return f"{namespace}_{hashlib.sha256(payload).hexdigest()[:32]}"


def equity_point_id(
    account_id: str,
    event_ts: datetime,
    *,
    cash: Any,
    equity: Any,
    realized_pnl: Any,
    unrealized_pnl: Any,
    drawdown: Any,
) -> str:
    """Identify one complete equity state, including same-event close transitions."""

    values = tuple(
        format(decimal_value(value, name).normalize(), "f")
        for name, value in (
            ("cash", cash),
            ("equity", equity),
            ("realized_pnl", realized_pnl),
            ("unrealized_pnl", unrealized_pnl),
            ("drawdown", drawdown),
        )
    )
    return deterministic_id(
        "equity",
        account_id,
        as_utc(event_ts, "event_ts").isoformat(timespec="microseconds"),
        *values,
    )


@dataclass(frozen=True, slots=True)
class ExitPolicy:
    fixed_stop_ticks: Decimal | None = None
    take_profit_ticks: Decimal | None = None
    trailing_ticks: Decimal | None = None
    breakeven_trigger_ticks: Decimal | None = None
    time_exit_seconds: int | None = None
    close_at_session_end: bool = True

    def __post_init__(self) -> None:
        for name in (
            "fixed_stop_ticks",
            "take_profit_ticks",
            "trailing_ticks",
            "breakeven_trigger_ticks",
        ):
            raw = getattr(self, name)
            if raw is None:
                continue
            value = decimal_value(raw, name)
            if value <= 0:
                raise DomainValidationError(f"{name} must be positive")
            object.__setattr__(self, name, value)
        if self.time_exit_seconds is not None and self.time_exit_seconds <= 0:
            raise DomainValidationError("time_exit_seconds must be positive")


@dataclass(frozen=True, slots=True)
class StrategyVersion:
    strategy_id: str
    version: str
    status: StrategyStatus
    created_at: datetime
    activated_at: datetime | None
    deactivated_at: datetime | None = None
    discovery_source: str = ""
    description: str = ""
    parameters: Mapping[str, Any] = field(default_factory=dict)
    code_hash: str = ""
    config_hash: str = ""
    feature_schema_version: str = ""
    execution_model_version: str = ""
    risk_model_version: str = ""
    session_filters: tuple[SessionLabel, ...] = ()
    minimum_warmup: int = 0
    new_entry_cutoff_seconds: int = 0
    position_carry_policy: str = "INTRADAY_CLOSE"

    def __post_init__(self) -> None:
        strategy_id = self.strategy_id.strip()
        version = self.version.strip()
        if not strategy_id or not version:
            raise DomainValidationError("strategy_id and version must not be blank")
        status = (
            self.status if isinstance(self.status, StrategyStatus) else StrategyStatus(self.status)
        )
        created_at = as_utc(self.created_at, "created_at")
        activated_at = (
            as_utc(self.activated_at, "activated_at") if self.activated_at is not None else None
        )
        deactivated_at = (
            as_utc(self.deactivated_at, "deactivated_at")
            if self.deactivated_at is not None
            else None
        )
        if activated_at is not None and activated_at < created_at:
            raise DomainValidationError("activated_at cannot precede created_at")
        if deactivated_at is not None and (
            activated_at is None or deactivated_at < activated_at
        ):
            raise DomainValidationError("deactivated_at cannot precede activation")
        if self.minimum_warmup < 0 or self.new_entry_cutoff_seconds < 0:
            raise DomainValidationError("warmup and entry cutoff must be non-negative")
        object.__setattr__(self, "strategy_id", strategy_id)
        object.__setattr__(self, "version", version)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "activated_at", activated_at)
        object.__setattr__(self, "deactivated_at", deactivated_at)
        session_filters = tuple(
            item if isinstance(item, SessionLabel) else SessionLabel(item)
            for item in self.session_filters
        )
        object.__setattr__(self, "parameters", _freeze(self.parameters))
        object.__setattr__(self, "session_filters", session_filters)

    @property
    def key(self) -> str:
        return f"{self.strategy_id}_{self.version}"

    def is_active_at(self, timestamp: datetime) -> bool:
        timestamp = as_utc(timestamp)
        active_statuses = {
            StrategyStatus.FROZEN_PAPER,
            StrategyStatus.FROZEN_PAPER_SECONDARY,
            StrategyStatus.FROZEN_PAPER_DEGRADED_CONTINUE_OOS,
            StrategyStatus.FROZEN_PAPER_OOS_ACCUMULATION,
            StrategyStatus.OOS_ACCUMULATION,
            StrategyStatus.PAPER_VALIDATED,
        }
        return bool(
            self.status in active_statuses
            and self.activated_at is not None
            and self.activated_at <= timestamp
            and (self.deactivated_at is None or timestamp < self.deactivated_at)
        )


@dataclass(frozen=True, slots=True)
class PaperIntent:
    intent_id: str
    strategy_id: str
    strategy_version: str
    instrument_uid: str
    decision_ts: datetime
    eligible_ts: datetime
    side: Side
    quantity: Decimal
    execution_model: ExecutionModel
    reason: str
    confidence: Decimal = Decimal("0")
    decision_event_id: str | None = None
    limit_price: Decimal | None = None
    exit_policy: ExitPolicy = field(default_factory=ExitPolicy)
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("intent_id", "strategy_id", "strategy_version", "instrument_uid", "reason"):
            value = getattr(self, name).strip()
            if not value:
                raise DomainValidationError(f"{name} must not be blank")
            object.__setattr__(self, name, value)
        decision_ts = as_utc(self.decision_ts, "decision_ts")
        eligible_ts = as_utc(self.eligible_ts, "eligible_ts")
        if eligible_ts < decision_ts:
            raise LookAheadError("eligible_ts cannot precede the strategy decision")
        side = self.side if isinstance(self.side, Side) else Side(self.side)
        model = (
            self.execution_model
            if isinstance(self.execution_model, ExecutionModel)
            else ExecutionModel(self.execution_model)
        )
        quantity = decimal_value(self.quantity, "quantity")
        confidence = decimal_value(self.confidence, "confidence")
        if quantity <= 0:
            raise DomainValidationError("quantity must be positive")
        if not Decimal("0") <= confidence <= Decimal("1"):
            raise DomainValidationError("confidence must be between zero and one")
        limit_price = None
        if self.limit_price is not None:
            limit_price = decimal_value(self.limit_price, "limit_price")
            if limit_price <= 0:
                raise DomainValidationError("limit_price must be positive")
        if model is ExecutionModel.PASSIVE and limit_price is None:
            raise DomainValidationError("passive intent requires limit_price")
        object.__setattr__(self, "decision_ts", decision_ts)
        object.__setattr__(self, "eligible_ts", eligible_ts)
        object.__setattr__(self, "side", side)
        object.__setattr__(self, "execution_model", model)
        object.__setattr__(self, "quantity", quantity)
        object.__setattr__(self, "confidence", confidence)
        object.__setattr__(self, "limit_price", limit_price)
        object.__setattr__(self, "metadata", _freeze(self.metadata))

    @classmethod
    def create(
        cls,
        *,
        strategy_id: str,
        strategy_version: str,
        instrument_uid: str,
        decision_ts: datetime,
        eligible_ts: datetime,
        side: Side,
        quantity: DecimalInput,
        execution_model: ExecutionModel,
        reason: str,
        confidence: DecimalInput = Decimal("0"),
        decision_event_id: str | None = None,
        limit_price: DecimalInput | None = None,
        exit_policy: ExitPolicy | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> PaperIntent:
        normalized_quantity = decimal_value(quantity, "quantity")
        normalized_confidence = decimal_value(confidence, "confidence")
        normalized_limit = (
            decimal_value(limit_price, "limit_price") if limit_price is not None else None
        )
        intent_id = deterministic_id(
            "intent",
            strategy_id,
            strategy_version,
            instrument_uid,
            decision_ts,
            eligible_ts,
            side,
            normalized_quantity,
            execution_model,
            decision_event_id,
        )
        return cls(
            intent_id=intent_id,
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            instrument_uid=instrument_uid,
            decision_ts=decision_ts,
            eligible_ts=eligible_ts,
            side=side,
            quantity=normalized_quantity,
            execution_model=execution_model,
            reason=reason,
            confidence=normalized_confidence,
            decision_event_id=decision_event_id,
            limit_price=normalized_limit,
            exit_policy=exit_policy or ExitPolicy(),
            metadata=metadata or {},
        )


@dataclass(frozen=True, slots=True)
class MarketEvent:
    event_id: str
    instrument_uid: str
    event_type: str
    exchange_ts: datetime
    receive_ts: datetime
    processing_ts: datetime
    revision: int = 0
    sequence: int = 0
    source: str = "t-invest"
    trading_status: str | None = None
    data_quality: DataQuality = DataQuality.GOOD
    reconnect_generation: int = 0
    values: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("event_id", "instrument_uid", "event_type", "source"):
            value = getattr(self, name).strip()
            if not value:
                raise DomainValidationError(f"{name} must not be blank")
            object.__setattr__(self, name, value)
        exchange_ts = as_utc(self.exchange_ts, "exchange_ts")
        receive_ts = as_utc(self.receive_ts, "receive_ts")
        processing_ts = as_utc(self.processing_ts, "processing_ts")
        if processing_ts < receive_ts:
            raise DomainValidationError("processing_ts cannot precede receive_ts")
        if min(self.revision, self.sequence, self.reconnect_generation) < 0:
            raise DomainValidationError(
                "revision, sequence and reconnect generation are non-negative"
            )
        quality = (
            self.data_quality
            if isinstance(self.data_quality, DataQuality)
            else DataQuality(self.data_quality)
        )
        object.__setattr__(self, "exchange_ts", exchange_ts)
        object.__setattr__(self, "receive_ts", receive_ts)
        object.__setattr__(self, "processing_ts", processing_ts)
        object.__setattr__(self, "data_quality", quality)
        object.__setattr__(self, "values", _freeze(self.values))

    @property
    def canonical_order_key(self) -> tuple[datetime, int, int, datetime, str]:
        return (self.exchange_ts, self.revision, self.sequence, self.receive_ts, self.event_id)


@dataclass(frozen=True, slots=True, order=True)
class BookLevel:
    price: Decimal
    quantity: Decimal

    def __post_init__(self) -> None:
        price = decimal_value(self.price, "price")
        quantity = decimal_value(self.quantity, "quantity")
        if price <= 0 or quantity <= 0:
            raise DomainValidationError("book price and quantity must be positive")
        object.__setattr__(self, "price", price)
        object.__setattr__(self, "quantity", quantity)


@dataclass(frozen=True, slots=True)
class OrderBook:
    event_id: str
    instrument_uid: str
    exchange_ts: datetime
    receive_ts: datetime
    processing_ts: datetime
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    trading_status: str | None = "NORMAL_TRADING"
    data_quality: DataQuality = DataQuality.GOOD
    revision: int = 0
    sequence: int = 0
    reconnect_generation: int = 0

    def __post_init__(self) -> None:
        if not self.event_id.strip() or not self.instrument_uid.strip():
            raise DomainValidationError("book event_id and instrument_uid must not be blank")
        exchange_ts = as_utc(self.exchange_ts, "exchange_ts")
        receive_ts = as_utc(self.receive_ts, "receive_ts")
        processing_ts = as_utc(self.processing_ts, "processing_ts")
        if processing_ts < receive_ts:
            raise DomainValidationError("book processing_ts cannot precede receive_ts")
        bids = tuple(self.bids)
        asks = tuple(self.asks)
        if any(not isinstance(level, BookLevel) for level in bids + asks):
            raise TypeError("bids and asks must contain BookLevel")
        if any(left.price <= right.price for left, right in zip(bids, bids[1:], strict=False)):
            raise DomainValidationError("bid prices must be strictly descending")
        if any(left.price >= right.price for left, right in zip(asks, asks[1:], strict=False)):
            raise DomainValidationError("ask prices must be strictly ascending")
        if bids and asks and bids[0].price >= asks[0].price:
            raise DomainValidationError("order book must not be locked or crossed")
        quality = (
            self.data_quality
            if isinstance(self.data_quality, DataQuality)
            else DataQuality(self.data_quality)
        )
        object.__setattr__(self, "exchange_ts", exchange_ts)
        object.__setattr__(self, "receive_ts", receive_ts)
        object.__setattr__(self, "processing_ts", processing_ts)
        object.__setattr__(self, "bids", bids)
        object.__setattr__(self, "asks", asks)
        object.__setattr__(self, "data_quality", quality)

    @property
    def best_bid(self) -> Decimal | None:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Decimal | None:
        return self.asks[0].price if self.asks else None

    @property
    def mid_price(self) -> Decimal | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / Decimal("2")

    @property
    def is_executable(self) -> bool:
        return self.data_quality is DataQuality.GOOD and trading_status_is_open(
            self.trading_status
        )


@dataclass(frozen=True, slots=True)
class PaperOrder:
    order_id: str
    intent_id: str
    strategy_id: str
    strategy_version: str
    instrument_uid: str
    side: Side
    quantity: Decimal
    execution_model: ExecutionModel
    created_ts: datetime
    eligible_ts: datetime
    limit_price: Decimal | None = None
    status: OrderStatus = OrderStatus.PENDING

    def __post_init__(self) -> None:
        for name in (
            "order_id",
            "intent_id",
            "strategy_id",
            "strategy_version",
            "instrument_uid",
        ):
            if not getattr(self, name).strip():
                raise DomainValidationError(f"{name} must not be blank")
        quantity = decimal_value(self.quantity, "quantity")
        if quantity <= 0:
            raise DomainValidationError("order quantity must be positive")
        created_ts = as_utc(self.created_ts, "created_ts")
        eligible_ts = as_utc(self.eligible_ts, "eligible_ts")
        if eligible_ts < created_ts:
            raise LookAheadError("order eligibility cannot precede creation")
        side = self.side if isinstance(self.side, Side) else Side(self.side)
        model = (
            self.execution_model
            if isinstance(self.execution_model, ExecutionModel)
            else ExecutionModel(self.execution_model)
        )
        status = self.status if isinstance(self.status, OrderStatus) else OrderStatus(self.status)
        limit_price = None
        if self.limit_price is not None:
            limit_price = decimal_value(self.limit_price, "limit_price")
            if limit_price <= 0:
                raise DomainValidationError("limit_price must be positive")
        if model is ExecutionModel.PASSIVE and limit_price is None:
            raise DomainValidationError("passive paper order requires limit_price")
        object.__setattr__(self, "quantity", quantity)
        object.__setattr__(self, "created_ts", created_ts)
        object.__setattr__(self, "eligible_ts", eligible_ts)
        object.__setattr__(self, "side", side)
        object.__setattr__(self, "execution_model", model)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "limit_price", limit_price)

    @property
    def strategy_key(self) -> str:
        return f"{self.strategy_id}_{self.strategy_version}"


@dataclass(frozen=True, slots=True)
class PaperFill:
    fill_id: str
    order_id: str
    source_event_id: str
    fill_ts: datetime
    price: Decimal
    quantity: Decimal
    level_index: int

    def __post_init__(self) -> None:
        if (
            not self.fill_id.strip()
            or not self.order_id.strip()
            or not self.source_event_id.strip()
        ):
            raise DomainValidationError("fill IDs must not be blank")
        fill_ts = as_utc(self.fill_ts, "fill_ts")
        price = decimal_value(self.price, "price")
        quantity = decimal_value(self.quantity, "quantity")
        if price <= 0 or quantity <= 0 or self.level_index < 1:
            raise DomainValidationError("fill price/quantity/index must be positive")
        object.__setattr__(self, "fill_ts", fill_ts)
        object.__setattr__(self, "price", price)
        object.__setattr__(self, "quantity", quantity)


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    order_id: str
    status: FillStatus
    reason: str
    requested_quantity: Decimal
    filled_quantity: Decimal
    unfilled_quantity: Decimal
    vwap: Decimal | None
    fills: tuple[PaperFill, ...]
    source_event_id: str
    top_price: Decimal | None = None
    slippage_cost: Decimal = Decimal("0")
    queue_ahead_remaining: Decimal | None = None

    def __post_init__(self) -> None:
        status = self.status if isinstance(self.status, FillStatus) else FillStatus(self.status)
        requested = decimal_value(self.requested_quantity, "requested_quantity")
        filled = decimal_value(self.filled_quantity, "filled_quantity")
        unfilled = decimal_value(self.unfilled_quantity, "unfilled_quantity")
        fills = tuple(self.fills)
        if requested <= 0 or filled < 0 or filled > requested or unfilled != requested - filled:
            raise DomainValidationError("execution quantities do not reconcile")
        if sum((item.quantity for item in fills), Decimal("0")) != filled:
            raise DomainValidationError("fill rows do not reconcile to filled quantity")
        if status is FillStatus.FULL_FILL and filled != requested:
            raise DomainValidationError("FULL_FILL requires all requested quantity")
        if status is FillStatus.PARTIAL_FILL and not Decimal("0") < filled < requested:
            raise DomainValidationError("PARTIAL_FILL requires a positive partial quantity")
        if status is FillStatus.NO_FILL and filled != 0:
            raise DomainValidationError("NO_FILL requires zero quantity")
        vwap = None if self.vwap is None else decimal_value(self.vwap, "vwap")
        if filled == 0 and (vwap is not None or fills):
            raise DomainValidationError("unfilled execution cannot have VWAP or fills")
        if filled > 0:
            expected_vwap = (
                sum((item.price * item.quantity for item in fills), Decimal("0")) / filled
            )
            if vwap != expected_vwap:
                raise DomainValidationError("VWAP does not match fill rows")
        slippage_cost = decimal_value(self.slippage_cost, "slippage_cost")
        if slippage_cost < 0:
            raise DomainValidationError("slippage_cost must be non-negative")
        queue_remaining = self.queue_ahead_remaining
        if queue_remaining is not None:
            queue_remaining = decimal_value(queue_remaining, "queue_ahead_remaining")
            if queue_remaining < 0:
                raise DomainValidationError("queue_ahead_remaining must be non-negative")
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "requested_quantity", requested)
        object.__setattr__(self, "filled_quantity", filled)
        object.__setattr__(self, "unfilled_quantity", unfilled)
        object.__setattr__(self, "vwap", vwap)
        object.__setattr__(self, "fills", fills)
        object.__setattr__(self, "slippage_cost", slippage_cost)
        object.__setattr__(self, "queue_ahead_remaining", queue_remaining)


@dataclass(frozen=True, slots=True)
class PassiveQueueObservation:
    event_id: str
    receive_ts: datetime
    aggressive_quantity_at_price: Decimal
    volume_ahead: Decimal
    added_ahead: Decimal = Decimal("0")
    cancelled_displayed: Decimal = Decimal("0")
    timed_out: bool = False
    cancelled: bool = False
    data_quality: DataQuality = DataQuality.GOOD

    def __post_init__(self) -> None:
        if not self.event_id.strip():
            raise DomainValidationError("queue observation event_id must not be blank")
        receive_ts = as_utc(self.receive_ts, "receive_ts")
        for name in (
            "aggressive_quantity_at_price",
            "volume_ahead",
            "added_ahead",
            "cancelled_displayed",
        ):
            value = decimal_value(getattr(self, name), name)
            if value < 0:
                raise DomainValidationError(f"{name} must be non-negative")
            object.__setattr__(self, name, value)
        quality = (
            self.data_quality
            if isinstance(self.data_quality, DataQuality)
            else DataQuality(self.data_quality)
        )
        object.__setattr__(self, "receive_ts", receive_ts)
        object.__setattr__(self, "data_quality", quality)


@dataclass(frozen=True, slots=True)
class PaperPosition:
    position_id: str
    strategy_id: str
    strategy_version: str
    instrument_uid: str
    entry_side: Side
    quantity: Decimal
    entry_price: Decimal
    opened_ts: datetime
    entry_event_id: str
    status: PositionStatus = PositionStatus.OPEN
    peak_price: Decimal | None = None
    trough_price: Decimal | None = None
    mfe: Decimal = Decimal("0")
    mae: Decimal = Decimal("0")
    mfe_ts: datetime | None = None
    mae_ts: datetime | None = None
    mfe_event_id: str | None = None
    mae_event_id: str | None = None
    carry_cost: Decimal = Decimal("0")
    carry_accrued_through: datetime | None = None
    closed_ts: datetime | None = None
    exit_price: Decimal | None = None
    exit_event_id: str | None = None
    exit_reason: ExitReason | None = None
    realized_gross_pnl: Decimal = Decimal("0")
    realized_net_pnl: Decimal = Decimal("0")

    def __post_init__(self) -> None:
        for name in (
            "position_id",
            "strategy_id",
            "strategy_version",
            "instrument_uid",
            "entry_event_id",
        ):
            if not getattr(self, name).strip():
                raise DomainValidationError(f"{name} must not be blank")
        quantity = decimal_value(self.quantity, "quantity")
        entry_price = decimal_value(self.entry_price, "entry_price")
        if quantity <= 0 or entry_price <= 0:
            raise DomainValidationError("position quantity and entry_price must be positive")
        opened_ts = as_utc(self.opened_ts, "opened_ts")
        status = (
            self.status if isinstance(self.status, PositionStatus) else PositionStatus(self.status)
        )
        side = self.entry_side if isinstance(self.entry_side, Side) else Side(self.entry_side)
        peak = (
            entry_price
            if self.peak_price is None
            else decimal_value(self.peak_price, "peak_price")
        )
        trough = (
            entry_price
            if self.trough_price is None
            else decimal_value(self.trough_price, "trough_price")
        )
        mfe = decimal_value(self.mfe, "mfe")
        mae = decimal_value(self.mae, "mae")
        carry = decimal_value(self.carry_cost, "carry_cost")
        gross = decimal_value(self.realized_gross_pnl, "realized_gross_pnl")
        net = decimal_value(self.realized_net_pnl, "realized_net_pnl")
        if min(mfe, mae, carry) < 0:
            raise DomainValidationError("MFE, MAE and carry cost must be non-negative")
        mfe_ts = as_utc(self.mfe_ts, "mfe_ts") if self.mfe_ts is not None else None
        mae_ts = as_utc(self.mae_ts, "mae_ts") if self.mae_ts is not None else None
        if mfe_ts is not None and mfe_ts < opened_ts:
            raise LookAheadError("MFE timestamp cannot precede entry")
        if mae_ts is not None and mae_ts < opened_ts:
            raise LookAheadError("MAE timestamp cannot precede entry")
        if (mfe_ts is None) != (self.mfe_event_id is None):
            raise DomainValidationError("MFE timestamp and source event must be paired")
        if (mae_ts is None) != (self.mae_event_id is None):
            raise DomainValidationError("MAE timestamp and source event must be paired")
        if self.mfe_event_id is not None and not self.mfe_event_id.strip():
            raise DomainValidationError("MFE source event must not be blank")
        if self.mae_event_id is not None and not self.mae_event_id.strip():
            raise DomainValidationError("MAE source event must not be blank")
        carry_through = (
            as_utc(self.carry_accrued_through, "carry_accrued_through")
            if self.carry_accrued_through is not None
            else opened_ts
        )
        closed_ts = as_utc(self.closed_ts, "closed_ts") if self.closed_ts is not None else None
        exit_price = (
            decimal_value(self.exit_price, "exit_price") if self.exit_price is not None else None
        )
        if status is PositionStatus.CLOSED:
            if closed_ts is None or exit_price is None or self.exit_event_id is None:
                raise DomainValidationError(
                    "closed position requires exit timestamp, price and event"
                )
            if closed_ts < opened_ts:
                raise LookAheadError("position cannot close before it opens")
        elif any(value is not None for value in (closed_ts, exit_price, self.exit_event_id)):
            raise DomainValidationError("open position cannot contain exit fields")
        object.__setattr__(self, "quantity", quantity)
        object.__setattr__(self, "entry_price", entry_price)
        object.__setattr__(self, "opened_ts", opened_ts)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "entry_side", side)
        object.__setattr__(self, "peak_price", peak)
        object.__setattr__(self, "trough_price", trough)
        object.__setattr__(self, "mfe", mfe)
        object.__setattr__(self, "mae", mae)
        object.__setattr__(self, "mfe_ts", mfe_ts)
        object.__setattr__(self, "mae_ts", mae_ts)
        object.__setattr__(self, "carry_cost", carry)
        object.__setattr__(self, "carry_accrued_through", carry_through)
        object.__setattr__(self, "closed_ts", closed_ts)
        object.__setattr__(self, "exit_price", exit_price)
        object.__setattr__(self, "realized_gross_pnl", gross)
        object.__setattr__(self, "realized_net_pnl", net)

    @property
    def is_long(self) -> bool:
        return self.entry_side is Side.BUY

    @property
    def strategy_key(self) -> str:
        return f"{self.strategy_id}_{self.strategy_version}"


@dataclass(frozen=True, slots=True)
class ExitDecision:
    reason: ExitReason
    trigger_ts: datetime
    trigger_event_id: str
    trigger_price: Decimal

    def __post_init__(self) -> None:
        reason = self.reason if isinstance(self.reason, ExitReason) else ExitReason(self.reason)
        trigger_ts = as_utc(self.trigger_ts, "trigger_ts")
        trigger_price = decimal_value(self.trigger_price, "trigger_price")
        if trigger_price <= 0 or not self.trigger_event_id.strip():
            raise DomainValidationError("exit trigger price and event ID are required")
        object.__setattr__(self, "reason", reason)
        object.__setattr__(self, "trigger_ts", trigger_ts)
        object.__setattr__(self, "trigger_price", trigger_price)


__all__ = [
    "BookLevel",
    "DataQuality",
    "DomainValidationError",
    "ExecutionModel",
    "ExecutionResult",
    "ExitDecision",
    "ExitPolicy",
    "ExitReason",
    "FillStatus",
    "LookAheadError",
    "MarketEvent",
    "OrderBook",
    "OrderStatus",
    "PaperFill",
    "PaperIntent",
    "PaperOrder",
    "PaperPosition",
    "PassiveQueueObservation",
    "PositionStatus",
    "SessionLabel",
    "Side",
    "StrategyStatus",
    "StrategyVersion",
    "as_utc",
    "decimal_value",
    "deterministic_id",
    "trading_status_is_open",
]
