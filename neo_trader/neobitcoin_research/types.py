"""Core types for Neobitcoin research execution simulations.

The types in this module are deliberately independent from broker order APIs.
They describe validated market-data snapshots and deterministic simulation
results only.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import TypeAlias

DecimalInput: TypeAlias = Decimal | int | str


class OrderBookValidationError(ValueError):
    """An order-book snapshot is unsafe for execution research."""


class ExecutionSide(StrEnum):
    """Direction of the simulated order."""

    BUY = "BUY"
    SELL = "SELL"


class ExecutionModel(StrEnum):
    """Execution models that must remain separate in research reports."""

    IDEAL_TOP_OF_BOOK = "IDEAL_TOP_OF_BOOK"
    AGGRESSIVE_SWEEP = "AGGRESSIVE_SWEEP"
    PASSIVE_OPTIMISTIC = "PASSIVE_OPTIMISTIC"
    PASSIVE_BASE = "PASSIVE_BASE"
    PASSIVE_PESSIMISTIC = "PASSIVE_PESSIMISTIC"


class ExecutionStatus(StrEnum):
    """Final state of one bounded simulation observation."""

    FULL_FILL = "FULL_FILL"
    PARTIAL_FILL = "PARTIAL_FILL"
    NO_FILL = "NO_FILL"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"


class ExecutionReason(StrEnum):
    """Auditable reason for the reported execution status."""

    FULLY_FILLED = "FULLY_FILLED"
    NO_LIQUIDITY = "NO_LIQUIDITY"
    INSUFFICIENT_LIQUIDITY = "INSUFFICIENT_LIQUIDITY"
    LIMIT_PRICE_NOT_REACHED = "LIMIT_PRICE_NOT_REACHED"
    QUEUE_NOT_REACHED = "QUEUE_NOT_REACHED"
    PARTIAL_QUEUE_FILL = "PARTIAL_QUEUE_FILL"
    ORDER_CANCELLED = "ORDER_CANCELLED"
    TIMEOUT = "TIMEOUT"
    MARKET_MOVED_AWAY = "MARKET_MOVED_AWAY"


class PassiveQueueScenario(StrEnum):
    """Conservative approximations used when order-level queue data is absent."""

    OPTIMISTIC = "OPTIMISTIC"
    BASE = "BASE"
    PESSIMISTIC = "PESSIMISTIC"


@dataclass(frozen=True, slots=True)
class BookLevel:
    """One positive price/quantity level in a normalized order book."""

    price: Decimal
    quantity: Decimal

    def __post_init__(self) -> None:
        price = _decimal(self.price, "price")
        quantity = _decimal(self.quantity, "quantity")
        if price <= 0:
            raise OrderBookValidationError("book level price must be positive")
        if quantity <= 0:
            raise OrderBookValidationError("book level quantity must be positive")
        object.__setattr__(self, "price", price)
        object.__setattr__(self, "quantity", quantity)


@dataclass(frozen=True, slots=True)
class OrderBookSnapshot:
    """Strictly validated, internally consistent order-book snapshot.

    Empty sides are allowed so an execution simulation can explicitly report
    ``NO_FILL``/``NO_LIQUIDITY``. Present bid levels must be strictly descending,
    ask levels strictly ascending, and a two-sided book must not be locked or
    crossed.
    """

    instrument_uid: str
    exchange_timestamp: datetime
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    is_consistent: bool = True
    tick_size: Decimal | None = None
    snapshot_id: str | None = None

    def __post_init__(self) -> None:
        instrument_uid = self.instrument_uid.strip()
        if not instrument_uid:
            raise OrderBookValidationError("instrument_uid must not be empty")
        if not isinstance(self.exchange_timestamp, datetime):
            raise TypeError("exchange_timestamp must be a datetime")
        if self.exchange_timestamp.tzinfo is None or self.exchange_timestamp.utcoffset() is None:
            raise OrderBookValidationError("exchange_timestamp must be timezone-aware")
        if not isinstance(self.is_consistent, bool):
            raise TypeError("is_consistent must be bool")
        if not self.is_consistent:
            raise OrderBookValidationError("inconsistent order book cannot be simulated")

        bids = tuple(self.bids)
        asks = tuple(self.asks)
        if any(not isinstance(level, BookLevel) for level in bids + asks):
            raise TypeError("bids and asks must contain BookLevel instances")

        tick_size = None
        if self.tick_size is not None:
            tick_size = _decimal(self.tick_size, "tick_size")
            if tick_size <= 0:
                raise OrderBookValidationError("tick_size must be positive")

        _validate_side(bids, descending=True, side_name="bids", tick_size=tick_size)
        _validate_side(asks, descending=False, side_name="asks", tick_size=tick_size)
        if bids and asks and bids[0].price >= asks[0].price:
            raise OrderBookValidationError("order book must satisfy best_bid < best_ask")

        snapshot_id = self.snapshot_id
        if snapshot_id is not None:
            snapshot_id = snapshot_id.strip()
            if not snapshot_id:
                raise OrderBookValidationError("snapshot_id must not be blank")

        object.__setattr__(self, "instrument_uid", instrument_uid)
        object.__setattr__(self, "exchange_timestamp", self.exchange_timestamp.astimezone(UTC))
        object.__setattr__(self, "bids", bids)
        object.__setattr__(self, "asks", asks)
        object.__setattr__(self, "tick_size", tick_size)
        object.__setattr__(self, "snapshot_id", snapshot_id)

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


@dataclass(frozen=True, slots=True)
class ExecutionRequest:
    """Quantity and optional price constraint of a simulated order."""

    side: ExecutionSide
    quantity: Decimal
    limit_price: Decimal | None = None

    def __post_init__(self) -> None:
        side = _side(self.side)
        quantity = _decimal(self.quantity, "quantity")
        if quantity <= 0:
            raise ValueError("quantity must be positive")
        limit_price = None
        if self.limit_price is not None:
            limit_price = _decimal(self.limit_price, "limit_price")
            if limit_price <= 0:
                raise ValueError("limit_price must be positive")
        object.__setattr__(self, "side", side)
        object.__setattr__(self, "quantity", quantity)
        object.__setattr__(self, "limit_price", limit_price)


@dataclass(frozen=True, slots=True)
class PassiveQueueObservation:
    """Aggregate activity observed at a passive order's price level.

    ``cancelled_at_level`` and ``added_at_level`` are aggregate displayed-depth
    changes. The queue scenario determines how much of each is conservatively
    attributed ahead of the simulated order.
    """

    volume_ahead: Decimal
    traded_at_level: Decimal
    cancelled_at_level: Decimal = Decimal("0")
    added_at_level: Decimal = Decimal("0")
    elapsed_ms: int = 0
    timeout_ms: int = 1_000
    order_cancelled: bool = False
    market_moved_away: bool = False
    post_fill_mid_price: Decimal | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "volume_ahead",
            "traded_at_level",
            "cancelled_at_level",
            "added_at_level",
        ):
            value = _decimal(getattr(self, field_name), field_name)
            if value < 0:
                raise ValueError(f"{field_name} must be non-negative")
            object.__setattr__(self, field_name, value)

        elapsed_ms = _non_negative_int(self.elapsed_ms, "elapsed_ms")
        timeout_ms = _non_negative_int(self.timeout_ms, "timeout_ms")
        if timeout_ms == 0:
            raise ValueError("timeout_ms must be positive")
        if not isinstance(self.order_cancelled, bool):
            raise TypeError("order_cancelled must be bool")
        if not isinstance(self.market_moved_away, bool):
            raise TypeError("market_moved_away must be bool")
        if self.order_cancelled and self.market_moved_away:
            raise ValueError("order_cancelled and market_moved_away are mutually exclusive")

        post_fill_mid_price = None
        if self.post_fill_mid_price is not None:
            post_fill_mid_price = _decimal(self.post_fill_mid_price, "post_fill_mid_price")
            if post_fill_mid_price <= 0:
                raise ValueError("post_fill_mid_price must be positive")

        object.__setattr__(self, "elapsed_ms", elapsed_ms)
        object.__setattr__(self, "timeout_ms", timeout_ms)
        object.__setattr__(self, "post_fill_mid_price", post_fill_mid_price)


@dataclass(frozen=True, slots=True)
class LevelFill:
    """Quantity consumed at one displayed level."""

    level_index: int
    price: Decimal
    quantity: Decimal

    def __post_init__(self) -> None:
        level_index = _positive_int(self.level_index, "level_index")
        price = _decimal(self.price, "price")
        quantity = _decimal(self.quantity, "quantity")
        if price <= 0 or quantity <= 0:
            raise ValueError("fill price and quantity must be positive")
        object.__setattr__(self, "level_index", level_index)
        object.__setattr__(self, "price", price)
        object.__setattr__(self, "quantity", quantity)


@dataclass(frozen=True, slots=True)
class QueueDiagnostics:
    """Queue-accounting details retained with every passive simulation."""

    scenario: PassiveQueueScenario
    initial_volume_ahead: Decimal
    cancellation_credit: Decimal
    addition_penalty: Decimal
    effective_volume_ahead: Decimal
    traded_at_level: Decimal
    cancelled_at_level: Decimal
    added_at_level: Decimal
    volume_ahead_remaining: Decimal

    def __post_init__(self) -> None:
        scenario = _scenario(self.scenario)
        values: dict[str, Decimal] = {}
        for field_name in (
            "initial_volume_ahead",
            "cancellation_credit",
            "addition_penalty",
            "effective_volume_ahead",
            "traded_at_level",
            "cancelled_at_level",
            "added_at_level",
            "volume_ahead_remaining",
        ):
            value = _decimal(getattr(self, field_name), field_name)
            if value < 0:
                raise ValueError(f"{field_name} must be non-negative")
            values[field_name] = value
            object.__setattr__(self, field_name, value)

        expected_effective = max(
            values["initial_volume_ahead"]
            - values["cancellation_credit"]
            + values["addition_penalty"],
            Decimal("0"),
        )
        if values["effective_volume_ahead"] != expected_effective:
            raise ValueError("effective_volume_ahead violates queue accounting")
        expected_remaining = max(
            values["effective_volume_ahead"] - values["traded_at_level"],
            Decimal("0"),
        )
        if values["volume_ahead_remaining"] != expected_remaining:
            raise ValueError("volume_ahead_remaining violates queue accounting")
        object.__setattr__(self, "scenario", scenario)


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    """Complete, internally consistent result of one execution simulation."""

    model: ExecutionModel
    side: ExecutionSide
    status: ExecutionStatus
    reason: ExecutionReason
    instrument_uid: str
    book_timestamp: datetime
    snapshot_id: str | None
    requested_quantity: Decimal
    filled_quantity: Decimal
    unfilled_quantity: Decimal
    fill_ratio: Decimal
    limit_price: Decimal | None
    top_level_price: Decimal | None
    average_price: Decimal | None
    sweep_vwap: Decimal | None
    levels_consumed: int
    fills: tuple[LevelFill, ...]
    slippage_vs_top: Decimal | None
    slippage_vs_mid: Decimal | None
    slippage_cost: Decimal | None
    adverse_selection_per_unit: Decimal | None
    adverse_selection_cost: Decimal | None
    queue: QueueDiagnostics | None = None

    def __post_init__(self) -> None:
        model = _model(self.model)
        side = _side(self.side)
        status = _status(self.status)
        reason = _reason(self.reason)
        requested = _decimal(self.requested_quantity, "requested_quantity")
        filled = _decimal(self.filled_quantity, "filled_quantity")
        unfilled = _decimal(self.unfilled_quantity, "unfilled_quantity")
        fill_ratio = _decimal(self.fill_ratio, "fill_ratio")
        if requested <= 0:
            raise ValueError("requested_quantity must be positive")
        if filled < 0 or filled > requested:
            raise ValueError("filled_quantity must be within the requested quantity")
        if unfilled != requested - filled:
            raise ValueError("unfilled_quantity must equal requested - filled")
        if fill_ratio != filled / requested:
            raise ValueError("fill_ratio must equal filled / requested")

        fills = tuple(self.fills)
        if any(not isinstance(fill, LevelFill) for fill in fills):
            raise TypeError("fills must contain LevelFill instances")
        if sum((fill.quantity for fill in fills), Decimal("0")) != filled:
            raise ValueError("level fills must sum to filled_quantity")
        levels_consumed = _non_negative_int(self.levels_consumed, "levels_consumed")
        if levels_consumed != len(fills):
            raise ValueError("levels_consumed must equal the number of level fills")

        average_price = _optional_decimal(self.average_price, "average_price")
        if filled == 0:
            if average_price is not None or fills:
                raise ValueError("an unfilled result cannot have a fill price or levels")
        else:
            if average_price is None or not fills:
                raise ValueError("a filled result requires a fill price and levels")
            notional = sum((fill.price * fill.quantity for fill in fills), Decimal("0"))
            if average_price != notional / filled:
                raise ValueError("average_price must equal fill notional / filled quantity")

        sweep_vwap = _optional_decimal(self.sweep_vwap, "sweep_vwap")
        if model is ExecutionModel.AGGRESSIVE_SWEEP and filled > 0:
            if sweep_vwap != average_price:
                raise ValueError("aggressive sweep_vwap must equal average_price")
        elif sweep_vwap is not None:
            raise ValueError("sweep_vwap is only valid for a filled aggressive sweep")

        if status is ExecutionStatus.FULL_FILL and filled != requested:
            raise ValueError("FULL_FILL requires the full requested quantity")
        if status is ExecutionStatus.PARTIAL_FILL and not Decimal("0") < filled < requested:
            raise ValueError("PARTIAL_FILL requires a positive partial quantity")
        if status is ExecutionStatus.NO_FILL and filled != 0:
            raise ValueError("NO_FILL requires zero filled quantity")
        if status in {ExecutionStatus.CANCELLED, ExecutionStatus.EXPIRED} and filled == requested:
            raise ValueError("cancelled or expired orders cannot already be fully filled")

        slippage_vs_top = _optional_decimal(self.slippage_vs_top, "slippage_vs_top")
        slippage_vs_mid = _optional_decimal(self.slippage_vs_mid, "slippage_vs_mid")
        slippage_cost = _optional_decimal(self.slippage_cost, "slippage_cost")
        if slippage_vs_top is None:
            if slippage_cost is not None:
                raise ValueError("slippage_cost requires slippage_vs_top")
        else:
            if self.top_level_price is None:
                raise ValueError("slippage_cost requires top_level_price")
            if side is ExecutionSide.BUY:
                expected_slippage_cost = sum(
                    ((fill.price - self.top_level_price) * fill.quantity for fill in fills),
                    Decimal("0"),
                )
            else:
                expected_slippage_cost = sum(
                    ((self.top_level_price - fill.price) * fill.quantity for fill in fills),
                    Decimal("0"),
                )
            if slippage_cost != expected_slippage_cost:
                raise ValueError("slippage_cost must equal exact level-by-level sweep cost")

        adverse_per_unit = _optional_decimal(
            self.adverse_selection_per_unit,
            "adverse_selection_per_unit",
        )
        adverse_cost = _optional_decimal(self.adverse_selection_cost, "adverse_selection_cost")
        if adverse_per_unit is None:
            if adverse_cost is not None:
                raise ValueError("adverse_selection_cost requires a per-unit value")
        elif adverse_cost != adverse_per_unit * filled:
            raise ValueError(
                "adverse_selection_cost must equal per-unit adverse selection * filled"
            )

        if self.queue is not None and not isinstance(self.queue, QueueDiagnostics):
            raise TypeError("queue must be QueueDiagnostics or None")
        if model in {
            ExecutionModel.PASSIVE_OPTIMISTIC,
            ExecutionModel.PASSIVE_BASE,
            ExecutionModel.PASSIVE_PESSIMISTIC,
        }:
            if self.queue is None:
                raise ValueError("passive results require queue diagnostics")
        elif self.queue is not None:
            raise ValueError("only passive results may contain queue diagnostics")

        object.__setattr__(self, "model", model)
        object.__setattr__(self, "side", side)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "reason", reason)
        object.__setattr__(self, "requested_quantity", requested)
        object.__setattr__(self, "filled_quantity", filled)
        object.__setattr__(self, "unfilled_quantity", unfilled)
        object.__setattr__(self, "fill_ratio", fill_ratio)
        object.__setattr__(self, "fills", fills)
        object.__setattr__(self, "levels_consumed", levels_consumed)
        object.__setattr__(self, "average_price", average_price)
        object.__setattr__(self, "sweep_vwap", sweep_vwap)
        object.__setattr__(self, "slippage_vs_top", slippage_vs_top)
        object.__setattr__(self, "slippage_vs_mid", slippage_vs_mid)
        object.__setattr__(self, "slippage_cost", slippage_cost)
        object.__setattr__(self, "adverse_selection_per_unit", adverse_per_unit)
        object.__setattr__(self, "adverse_selection_cost", adverse_cost)

    @property
    def is_full_fill(self) -> bool:
        return self.filled_quantity == self.requested_quantity

    @property
    def is_partial_fill(self) -> bool:
        return Decimal("0") < self.filled_quantity < self.requested_quantity

    @property
    def is_no_fill(self) -> bool:
        return self.filled_quantity == 0


def _validate_side(
    levels: tuple[BookLevel, ...],
    *,
    descending: bool,
    side_name: str,
    tick_size: Decimal | None,
) -> None:
    for index, level in enumerate(levels):
        if tick_size is not None and level.price % tick_size != 0:
            raise OrderBookValidationError(
                f"{side_name} level {index + 1} is not aligned to tick_size"
            )
        if index == 0:
            continue
        previous = levels[index - 1]
        ordered = previous.price > level.price if descending else previous.price < level.price
        if not ordered:
            direction = "descending" if descending else "ascending"
            raise OrderBookValidationError(
                f"{side_name} must be strictly {direction} with unique prices"
            )


def _decimal(value: object, field_name: str) -> Decimal:
    if isinstance(value, bool):
        raise TypeError(f"{field_name} must not be bool")
    try:
        if isinstance(value, Decimal):
            result = value
        elif isinstance(value, int | str):
            result = Decimal(value)
        else:
            raise TypeError(f"{field_name} must be Decimal, int, or str")
    except InvalidOperation as exc:
        raise ValueError(f"{field_name} must be a valid decimal") from exc
    if not result.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return result


def _optional_decimal(value: object | None, field_name: str) -> Decimal | None:
    return None if value is None else _decimal(value, field_name)


def _positive_int(value: object, field_name: str) -> int:
    parsed = _non_negative_int(value, field_name)
    if parsed == 0:
        raise ValueError(f"{field_name} must be positive")
    return parsed


def _non_negative_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if value < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return value


def _enum_value(value: object, enum_type: type[StrEnum], field_name: str) -> StrEnum:
    if isinstance(value, enum_type):
        return value
    if isinstance(value, str):
        normalized = value.strip().upper()
        try:
            return enum_type(normalized)
        except ValueError as exc:
            raise ValueError(f"unsupported {field_name}: {value!r}") from exc
    raise TypeError(f"{field_name} must be {enum_type.__name__} or str")


def _side(value: object) -> ExecutionSide:
    return ExecutionSide(_enum_value(value, ExecutionSide, "side"))


def _model(value: object) -> ExecutionModel:
    return ExecutionModel(_enum_value(value, ExecutionModel, "model"))


def _status(value: object) -> ExecutionStatus:
    return ExecutionStatus(_enum_value(value, ExecutionStatus, "status"))


def _reason(value: object) -> ExecutionReason:
    return ExecutionReason(_enum_value(value, ExecutionReason, "reason"))


def _scenario(value: object) -> PassiveQueueScenario:
    return PassiveQueueScenario(_enum_value(value, PassiveQueueScenario, "scenario"))


__all__ = [
    "BookLevel",
    "DecimalInput",
    "ExecutionModel",
    "ExecutionReason",
    "ExecutionRequest",
    "ExecutionResult",
    "ExecutionSide",
    "ExecutionStatus",
    "LevelFill",
    "OrderBookSnapshot",
    "OrderBookValidationError",
    "PassiveQueueObservation",
    "PassiveQueueScenario",
    "QueueDiagnostics",
]
