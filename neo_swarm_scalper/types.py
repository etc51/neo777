"""Runtime data types for the paper neoasset scalper."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class PositionSide(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


class BotAction(StrEnum):
    OPEN_LONG = "open_long"
    OPEN_SHORT = "open_short"
    CLOSE_POSITION = "close_position"
    WAIT = "wait"


class CuratorAction(StrEnum):
    ALLOW_TRADE = "allow_trade"
    REJECT_TRADE = "reject_trade"
    CLOSE_POSITION = "close_position"
    UPDATE_WEIGHT = "update_weight"
    UPDATE_TP = "update_tp"
    UPDATE_SL = "update_sl"
    UPDATE_TIME_STOP = "update_time_stop"
    UPDATE_COOLDOWN = "update_cooldown"
    SWITCH_TO_SHADOW = "switch_to_shadow"
    ENABLE_BOT = "enable_bot"
    DISABLE_NEW_ENTRIES = "disable_new_entries"


@dataclass(frozen=True, order=True)
class BookLevel:
    price: Decimal
    quantity: Decimal


@dataclass(frozen=True)
class InstrumentMetadata:
    name: str
    display_name: str
    ticker: str
    figi: str
    class_code: str
    lot: int = 1
    min_price_increment: Decimal | None = None
    trading_status: str | None = None
    currency: str | None = None
    exchange: str | None = None
    instrument_type: str | None = None
    uid: str | None = None

    @property
    def instrument_id(self) -> str:
        return self.uid or self.figi or f"{self.ticker}_{self.class_code}"


@dataclass(frozen=True)
class MarketSnapshot:
    timestamp_utc: datetime
    instrument: str
    metadata: InstrumentMetadata
    last_price: Decimal | None
    bid_levels: tuple[BookLevel, ...] = ()
    ask_levels: tuple[BookLevel, ...] = ()
    exchange_timestamp: datetime | None = None
    orderbook_missing: bool = False
    stale: bool = False
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def tick_size(self) -> Decimal | None:
        return self.metadata.min_price_increment

    @property
    def best_bid(self) -> Decimal | None:
        return self.bid_levels[0].price if self.bid_levels else None

    @property
    def best_ask(self) -> Decimal | None:
        return self.ask_levels[0].price if self.ask_levels else None

    @property
    def executable_price(self) -> Decimal | None:
        if self.last_price is not None:
            return self.last_price
        if self.best_bid is not None and self.best_ask is not None:
            return (self.best_bid + self.best_ask) / Decimal("2")
        return self.best_bid or self.best_ask

    @property
    def mid_price(self) -> Decimal | None:
        if self.best_bid is not None and self.best_ask is not None:
            return (self.best_bid + self.best_ask) / Decimal("2")
        return self.executable_price

    @property
    def spread_abs(self) -> Decimal | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid

    @property
    def spread_ticks(self) -> Decimal | None:
        if self.spread_abs is None or not self.tick_size:
            return None
        return self.spread_abs / self.tick_size

    def bid_volume(self, depth: int) -> Decimal:
        return sum((level.quantity for level in self.bid_levels[:depth]), Decimal("0"))

    def ask_volume(self, depth: int) -> Decimal:
        return sum((level.quantity for level in self.ask_levels[:depth]), Decimal("0"))

    def imbalance(self, depth: int) -> Decimal:
        bid = self.bid_volume(depth)
        ask = self.ask_volume(depth)
        total = bid + ask
        if total == 0:
            return Decimal("0")
        return (bid - ask) / total


@dataclass(frozen=True)
class FeatureSnapshot:
    timestamp_utc: datetime
    instrument: str
    values: dict[str, Any]


@dataclass
class BotParams:
    bot_id: str
    account_ref: str
    enabled: bool
    weight: Decimal
    risk_multiplier: Decimal
    allowed_instruments: tuple[str, ...]
    allowed_sides: tuple[PositionSide, ...]
    tp_ticks: int
    sl_ticks: int
    time_stop_sec: int
    cooldown_sec: int
    current_state: str = "idle"
    last_signal_time: datetime | None = None
    last_trade_time: datetime | None = None
    last_error: str | None = None
    shadow_mode: bool = False


@dataclass(frozen=True)
class BotDecision:
    decision_id: str
    timestamp_utc: datetime
    bot_id: str
    account_ref: str
    instrument: str | None
    action: BotAction
    side: PositionSide | None
    confidence: Decimal
    reason: str
    features_snapshot: dict[str, Any]
    bot_params: dict[str, Any]


@dataclass
class Position:
    position_id: str
    bot_id: str
    account_ref: str
    instrument: str
    side: PositionSide
    qty: Decimal
    entry_price: Decimal
    entry_time: datetime
    status: str = "OPEN"
    exit_price: Decimal | None = None
    exit_time: datetime | None = None
    mfe: Decimal = Decimal("0")
    mae: Decimal = Decimal("0")


@dataclass
class VirtualAccount:
    account_ref: str
    bot_id: str
    cash: Decimal
    equity: Decimal
    realized_pnl: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    open_position: Position | None = None
    last_update_time: datetime = field(default_factory=lambda: datetime.now(UTC))


def utc_now() -> datetime:
    return datetime.now(UTC)


def decimal_or_none(value: object) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        raise TypeError("boolean is not decimal-compatible.")
    if isinstance(value, int | float | str):
        return Decimal(str(value))
    raise TypeError(f"unsupported decimal value: {value!r}")


def as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


__all__ = [
    "BookLevel",
    "BotAction",
    "BotDecision",
    "BotParams",
    "CuratorAction",
    "FeatureSnapshot",
    "InstrumentMetadata",
    "MarketSnapshot",
    "Position",
    "PositionSide",
    "Side",
    "VirtualAccount",
    "as_utc",
    "decimal_or_none",
    "utc_now",
]
