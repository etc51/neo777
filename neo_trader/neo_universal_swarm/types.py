"""Core types for the Neo Universal Bot Swarm.

The swarm package is paper/simulation-only. It models bid/ask execution,
hedge-pair labels, and account-bot state without importing broker adapters or
placing orders.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Final, TypeAlias

JsonMapping: TypeAlias = Mapping[str, Any]

DEFAULT_TICK_SIZE: Final = Decimal("1")
DEFAULT_TICK_VALUE_RUB: Final = Decimal("1")


class SwarmInstrument(StrEnum):
    """Supported T-Bank neoasset instruments."""

    NEOBITOK = "NEOBITOK"
    NEOEFIR = "NEOEFIR"


class AccountBotState(StrEnum):
    """Runtime state assigned only by the curator."""

    IDLE = "IDLE"
    LONG_LEG = "LONG_LEG"
    SHORT_LEG = "SHORT_LEG"
    RUNNER_LONG = "RUNNER_LONG"
    RUNNER_SHORT = "RUNNER_SHORT"
    COOLDOWN = "COOLDOWN"
    DISABLED = "DISABLED"


class LegSide(StrEnum):
    """Direction of a hedge-pair leg."""

    LONG = "LONG"
    SHORT = "SHORT"


class PairStatus(StrEnum):
    """Lifecycle of a hedge-pair."""

    OPEN = "OPEN"
    RUNNER_ACTIVE = "RUNNER_ACTIVE"
    CLOSED = "CLOSED"
    FAILED = "FAILED"


class PairExitReason(StrEnum):
    """Reasons a simulated pair is closed."""

    TRAILING_STOP = "TRAILING_STOP"
    BREAKEVEN = "BREAKEVEN"
    FAILED_PAIR = "FAILED_PAIR"
    CUTOFF = "CUTOFF"
    END_OF_REPLAY = "END_OF_REPLAY"
    STALE_BOOK = "STALE_BOOK"
    WIDE_SPREAD = "WIDE_SPREAD"
    CHOP = "CHOP"


class RejectionReason(StrEnum):
    """Curator/model rejection buckets."""

    MODEL = "MODEL"
    SPREAD = "SPREAD"
    SPREAD_ENTRY_GATE = "SPREAD_ENTRY_GATE"
    STALE_BOOK = "STALE_BOOK"
    CHOP = "CHOP"
    LATENCY = "LATENCY"
    NO_FREE_BOTS = "NO_FREE_BOTS"
    DISABLED = "DISABLED"


@dataclass(frozen=True, order=True)
class OrderBookLevel:
    """One normalized order-book level."""

    price: Decimal
    quantity: Decimal


@dataclass(frozen=True)
class BookSnapshot:
    """Feature-ready order-book and last-trade snapshot."""

    timestamp: datetime
    instrument: SwarmInstrument
    bid_levels: tuple[OrderBookLevel, ...]
    ask_levels: tuple[OrderBookLevel, ...]
    tick_size: Decimal = DEFAULT_TICK_SIZE
    last_price: Decimal | None = None
    last_trade_size: Decimal = Decimal("0")
    trade_side: LegSide | None = None
    tick_direction: int = 0
    tick_velocity: Decimal = Decimal("0")
    volume_delta_1s: Decimal = Decimal("0")
    volume_delta_5s: Decimal = Decimal("0")
    volume_delta_15s: Decimal = Decimal("0")
    volatility_5s: Decimal = Decimal("0")
    volatility_15s: Decimal = Decimal("0")
    volatility_60s: Decimal = Decimal("0")
    mfi: Decimal | None = None
    own_orders: int = 0
    own_executions: int = 0
    latency_ms: int = 0
    slippage_ticks: Decimal = Decimal("0")

    def __post_init__(self) -> None:
        if not self.bid_levels:
            raise ValueError("bid_levels must not be empty.")
        if not self.ask_levels:
            raise ValueError("ask_levels must not be empty.")
        if self.tick_size <= 0:
            raise ValueError("tick_size must be positive.")
        if self.best_bid >= self.best_ask:
            raise ValueError("best bid must be lower than best ask.")

    @classmethod
    def from_levels(
        cls,
        *,
        timestamp: datetime,
        instrument: SwarmInstrument | str,
        bids: Sequence[Sequence[object] | Mapping[str, object] | OrderBookLevel],
        asks: Sequence[Sequence[object] | Mapping[str, object] | OrderBookLevel],
        tick_size: Decimal | int | str = DEFAULT_TICK_SIZE,
        last_price: Decimal | int | str | None = None,
        last_trade_size: Decimal | int | str = Decimal("0"),
        trade_side: LegSide | str | None = None,
        tick_direction: int = 0,
        tick_velocity: Decimal | int | str = Decimal("0"),
        volume_delta_1s: Decimal | int | str = Decimal("0"),
        volume_delta_5s: Decimal | int | str = Decimal("0"),
        volume_delta_15s: Decimal | int | str = Decimal("0"),
        volatility_5s: Decimal | int | str = Decimal("0"),
        volatility_15s: Decimal | int | str = Decimal("0"),
        volatility_60s: Decimal | int | str = Decimal("0"),
        mfi: Decimal | int | str | None = None,
        own_orders: int = 0,
        own_executions: int = 0,
        latency_ms: int = 0,
        slippage_ticks: Decimal | int | str = Decimal("0"),
    ) -> BookSnapshot:
        """Build a snapshot from common book-level shapes."""

        bid_levels = tuple(
            sorted(
                (_level_from_input(level) for level in bids),
                key=lambda level: level.price,
                reverse=True,
            )
        )
        ask_levels = tuple(
            sorted(
                (_level_from_input(level) for level in asks),
                key=lambda level: level.price,
            )
        )
        return cls(
            timestamp=as_utc(timestamp),
            instrument=SwarmInstrument(instrument),
            bid_levels=bid_levels,
            ask_levels=ask_levels,
            tick_size=to_decimal(tick_size),
            last_price=None if last_price is None else to_decimal(last_price),
            last_trade_size=to_decimal(last_trade_size),
            trade_side=None if trade_side is None else LegSide(trade_side),
            tick_direction=tick_direction,
            tick_velocity=to_decimal(tick_velocity),
            volume_delta_1s=to_decimal(volume_delta_1s),
            volume_delta_5s=to_decimal(volume_delta_5s),
            volume_delta_15s=to_decimal(volume_delta_15s),
            volatility_5s=to_decimal(volatility_5s),
            volatility_15s=to_decimal(volatility_15s),
            volatility_60s=to_decimal(volatility_60s),
            mfi=None if mfi is None else to_decimal(mfi),
            own_orders=own_orders,
            own_executions=own_executions,
            latency_ms=latency_ms,
            slippage_ticks=to_decimal(slippage_ticks),
        )

    @property
    def best_bid(self) -> Decimal:
        return self.bid_levels[0].price

    @property
    def best_ask(self) -> Decimal:
        return self.ask_levels[0].price

    @property
    def mid_price(self) -> Decimal:
        return (self.best_bid + self.best_ask) / Decimal("2")

    @property
    def spread_price(self) -> Decimal:
        return self.best_ask - self.best_bid

    @property
    def spread_ticks(self) -> Decimal:
        return self.spread_price / self.tick_size

    @property
    def microprice(self) -> Decimal:
        bid = self.bid_levels[0]
        ask = self.ask_levels[0]
        total_quantity = bid.quantity + ask.quantity
        if total_quantity == 0:
            return self.mid_price
        return ((ask.price * bid.quantity) + (bid.price * ask.quantity)) / total_quantity

    @property
    def microprice_edge_ticks(self) -> Decimal:
        return (self.microprice - self.mid_price) / self.tick_size

    def bid_volume(self, depth: int) -> Decimal:
        return sum((level.quantity for level in self.bid_levels[:depth]), Decimal("0"))

    def ask_volume(self, depth: int) -> Decimal:
        return sum((level.quantity for level in self.ask_levels[:depth]), Decimal("0"))

    def imbalance(self, depth: int) -> Decimal:
        bid_volume = self.bid_volume(depth)
        ask_volume = self.ask_volume(depth)
        total = bid_volume + ask_volume
        if total == 0:
            return Decimal("0")
        return (bid_volume - ask_volume) / total

    def model_feature_row(self) -> dict[str, object]:
        """Return a leak-free feature row for storage/model input.

        Future MFE/MAE fields are emitted as zeros for schema completeness and
        are not consumed by the EV model.
        """

        return {
            "timestamp": self.timestamp.isoformat(),
            "instrument": self.instrument.value,
            "best_bid": str(self.best_bid),
            "best_ask": str(self.best_ask),
            "mid_price": str(self.mid_price),
            "spread_ticks": str(self.spread_ticks),
            "last_price": str(self.last_price or self.mid_price),
            "last_trade_size": str(self.last_trade_size),
            "trade_side": None if self.trade_side is None else self.trade_side.value,
            "bid_volume_1": str(self.bid_volume(1)),
            "ask_volume_1": str(self.ask_volume(1)),
            "bid_volume_2": str(self.bid_volume(2)),
            "ask_volume_2": str(self.ask_volume(2)),
            "bid_volume_3": str(self.bid_volume(3)),
            "ask_volume_3": str(self.ask_volume(3)),
            "bid_volume_5": str(self.bid_volume(5)),
            "ask_volume_5": str(self.ask_volume(5)),
            "orderbook_imbalance_1": str(self.imbalance(1)),
            "orderbook_imbalance_3": str(self.imbalance(3)),
            "orderbook_imbalance_5": str(self.imbalance(5)),
            "microprice": str(self.microprice),
            "tick_direction": self.tick_direction,
            "tick_velocity": str(self.tick_velocity),
            "volume_delta_1s": str(self.volume_delta_1s),
            "volume_delta_5s": str(self.volume_delta_5s),
            "volume_delta_15s": str(self.volume_delta_15s),
            "volatility_5s": str(self.volatility_5s),
            "volatility_15s": str(self.volatility_15s),
            "volatility_60s": str(self.volatility_60s),
            "mfe_5s": "0",
            "mfe_15s": "0",
            "mfe_30s": "0",
            "mfe_60s": "0",
            "mae_5s": "0",
            "mae_15s": "0",
            "mae_30s": "0",
            "mae_60s": "0",
            "mfi": None if self.mfi is None else str(self.mfi),
            "own_orders": self.own_orders,
            "own_executions": self.own_executions,
            "latency_ms": self.latency_ms,
            "slippage_ticks": str(self.slippage_ticks),
        }


@dataclass(frozen=True)
class ActivePair:
    """Curator-created hedge-pair assignment."""

    pair_id: str
    instrument: SwarmInstrument
    long_bot_id: str
    short_bot_id: str
    opened_at: datetime
    stop_loss_ticks: int
    breakeven_ticks: int
    status: PairStatus = PairStatus.OPEN
    model_ev_ticks: Decimal = Decimal("0")
    entry_reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.stop_loss_ticks not in {2, 3}:
            raise ValueError("stop_loss_ticks must be 2 or 3.")
        if self.breakeven_ticks != self.stop_loss_ticks:
            raise ValueError("breakeven_ticks must match stop_loss_ticks.")


@dataclass(frozen=True)
class PairLabel:
    """Label produced by event-driven hedge-pair replay."""

    pair_id: str
    instrument: SwarmInstrument
    entry_timestamp: datetime
    exit_timestamp: datetime
    stop_loss_ticks: int
    loser_side: LegSide | None
    loser_loss_ticks: Decimal
    runner_side: LegSide | None
    runner_mfe_ticks: Decimal
    runner_mae_ticks: Decimal
    runner_exit_ticks: Decimal
    pair_total_pnl_ticks: Decimal
    pair_total_pnl_rub: Decimal
    exit_reason: PairExitReason
    max_adverse_excursion: Decimal
    max_favorable_excursion: Decimal
    fakeout_flag: bool
    runner_success_flag: bool
    entry_spread_cost_ticks: Decimal
    avg_slippage_ticks: Decimal
    latency_ms: int
    regime: str
    long_bot_id: str | None = None
    short_bot_id: str | None = None
    avoided_wide_spread_exits: int = 0
    missed_runner_profit_ticks: Decimal = Decimal("0")
    trail_lock_4_active: bool = False


@dataclass(frozen=True)
class SwarmMetrics:
    """Aggregate metrics for simulated or paper hedge pairs."""

    total_pairs: int
    profitable_pairs: int
    losing_pairs: int
    pair_winrate: Decimal
    pair_ev_ticks: Decimal
    pair_ev_rub: Decimal
    avg_runner_profit_ticks: Decimal
    avg_loser_loss_ticks: Decimal
    avg_spread_cost_ticks: Decimal
    avg_slippage_ticks: Decimal
    fakeout_rate: Decimal
    runner_to_breakeven_rate: Decimal
    runner_trailing_capture_ratio: Decimal
    profit_factor: Decimal
    max_drawdown: Decimal
    pnl_by_bot: dict[str, Decimal] = field(default_factory=dict)
    pnl_by_account: dict[str, Decimal] = field(default_factory=dict)
    pnl_by_instrument: dict[str, Decimal] = field(default_factory=dict)
    pnl_by_regime: dict[str, Decimal] = field(default_factory=dict)
    rejected_by_model: int = 0
    rejected_by_spread: int = 0
    rejected_by_spread_entry_gate: int = 0
    rejected_by_stale_book: int = 0
    rejected_by_chop: int = 0
    rejected_by_latency: int = 0


def to_decimal(value: Decimal | int | str) -> Decimal:
    """Convert safe numeric inputs to Decimal."""

    if isinstance(value, Decimal):
        return value
    if isinstance(value, int | str):
        return Decimal(value)
    raise TypeError(f"unsupported Decimal value: {value!r}")


def as_utc(value: datetime) -> datetime:
    """Normalize a datetime to UTC."""

    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def decimal_ratio(numerator: Decimal, denominator: Decimal) -> Decimal:
    """Return ``numerator / denominator`` with zero-safe semantics."""

    if denominator == 0:
        return Decimal("0")
    return numerator / denominator


def _level_from_input(
    level: Sequence[object] | Mapping[str, object] | OrderBookLevel,
) -> OrderBookLevel:
    if isinstance(level, OrderBookLevel):
        return level
    if isinstance(level, Mapping):
        price = level.get("price")
        quantity = level.get("quantity", level.get("qty", level.get("size")))
        if price is None or quantity is None:
            raise ValueError("level mapping must contain price and quantity.")
        return OrderBookLevel(price=to_decimal_value(price), quantity=to_decimal_value(quantity))
    if len(level) < 2:
        raise ValueError("level sequence must contain price and quantity.")
    return OrderBookLevel(price=to_decimal_value(level[0]), quantity=to_decimal_value(level[1]))


def to_decimal_value(value: object) -> Decimal:
    """Convert common runtime numeric inputs to Decimal."""

    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        raise TypeError("boolean values are not numeric.")
    if isinstance(value, int | str):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))
    raise TypeError(f"unsupported numeric value: {value!r}")
