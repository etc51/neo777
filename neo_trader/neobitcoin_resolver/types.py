"""Types for the Dual-Bot Neobitcoin Resolver."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from neo_trader.neo_universal_swarm.types import BookSnapshot, SwarmInstrument


class PositionSide(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


class GateName(StrEnum):
    SESSION = "session"
    SPREAD = "spread"
    ORDERBOOK = "orderbook"
    MICROSTRUCTURE = "microstructure"
    VOLATILITY = "volatility"
    COOLDOWN = "cooldown"
    DATA_QUALITY = "data_quality"
    PAIR_EXECUTION = "pair_execution"


class ResolverReason(StrEnum):
    OK = "OK"
    SESSION_GATE = "SESSION_GATE"
    SPREAD_ENTRY_GATE = "SPREAD_ENTRY_GATE"
    BAD_ORDERBOOK = "BAD_ORDERBOOK"
    BAD_MICROSTRUCTURE = "BAD_MICROSTRUCTURE"
    BAD_VOLATILITY = "BAD_VOLATILITY"
    COOLDOWN = "COOLDOWN"
    STALE_DATA = "STALE_DATA"
    BAD_DATA = "BAD_DATA"
    PAIR_ENTRY_BAD = "PAIR_ENTRY_BAD"
    ACTIVE_PAIR_EXISTS = "ACTIVE_PAIR_EXISTS"
    DECISION_ZONE_UP = "DECISION_ZONE_UP"
    DECISION_ZONE_DOWN = "DECISION_ZONE_DOWN"
    CLOSE_SHORT_LOSER = "CLOSE_SHORT_LOSER"
    CLOSE_LONG_LOSER = "CLOSE_LONG_LOSER"
    WINNER_TRAILING = "WINNER_TRAILING"
    NO_LOSS_OR_PROFIT_ONLY = "NO_LOSS_OR_PROFIT_ONLY"
    SAFE_EXIT = "SAFE_EXIT"
    CHAOTIC_SAFE_EXIT = "CHAOTIC_SAFE_EXIT"


@dataclass(frozen=True)
class GateResult:
    name: GateName
    passed: bool
    reason: ResolverReason = ResolverReason.OK
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class GateSnapshot:
    """Current market snapshot plus data-quality flags used by hard gates."""

    book: BookSnapshot
    market_open: bool = True
    trading_halted: bool = False
    stale_quotes: bool = False
    stale_orderbook: bool = False
    stale_candles: bool = False
    fresh_trades: bool = True
    api_ok: bool = True
    server_time_delta_ms: int = 0
    update_speed_hz: Decimal = Decimal("1")
    trade_speed_hz: Decimal = Decimal("1")
    support_resistance_context: str | None = None
    momentum_context: str | None = None
    external_btc_context: str | None = None
    news_context: str | None = None

    @property
    def timestamp(self) -> datetime:
        return self.book.timestamp

    @property
    def instrument(self) -> SwarmInstrument:
        return self.book.instrument


@dataclass
class PositionLeg:
    bot_id: str
    side: PositionSide
    entry_price: Decimal
    entry_time: datetime
    state: str = "OPEN"
    exit_price: Decimal | None = None
    exit_time: datetime | None = None
    gross_pnl: Decimal = Decimal("0")
    estimated_net_pnl: Decimal = Decimal("0")
    mfe: Decimal = Decimal("0")
    mae: Decimal = Decimal("0")
    mfi_context: Decimal | None = None

    def update_pnl(self, *, exit_mark: Decimal, spread_slippage_cost: Decimal) -> None:
        if self.side is PositionSide.LONG:
            gross = exit_mark - self.entry_price
        else:
            gross = self.entry_price - exit_mark
        self.gross_pnl = gross
        self.estimated_net_pnl = gross - spread_slippage_cost
        self.mfe = max(self.mfe, self.estimated_net_pnl)
        self.mae = min(self.mae, self.estimated_net_pnl)


@dataclass
class PairState:
    pair_id: str
    opened_at: datetime
    entry_mid_price: Decimal
    entry_spread_ticks: Decimal
    expected_slippage_ticks: Decimal
    long_leg: PositionLeg
    short_leg: PositionLeg
    gate_results: tuple[GateResult, ...]
    state: str = "BOTH_OPEN"
    winner_side: PositionSide | None = None
    loser_closed: bool = False
    protection_active: bool = False
    protection_trigger_reason: str | None = None
    safe_exit_price: Decimal | None = None
    protection_audit: dict[str, object] | None = None
    closed_at: datetime | None = None

    @property
    def open_legs(self) -> tuple[PositionLeg, ...]:
        return tuple(leg for leg in (self.long_leg, self.short_leg) if leg.state != "CLOSED")


def utc_now() -> datetime:
    return datetime.now(UTC)


__all__ = [
    "GateName",
    "GateResult",
    "GateSnapshot",
    "PairState",
    "PositionLeg",
    "PositionSide",
    "ResolverReason",
    "utc_now",
]
