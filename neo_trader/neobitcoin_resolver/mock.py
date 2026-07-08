"""Deterministic mock data for local smoke tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from neo_trader.neo_universal_swarm.types import BookSnapshot, LegSide, SwarmInstrument
from neo_trader.neobitcoin_resolver.types import GateSnapshot


def smoke_snapshots(start: datetime | None = None) -> tuple[GateSnapshot, ...]:
    base_time = start or datetime(2026, 7, 8, 10, 0, tzinfo=UTC)
    specs = (
        ("10000", "10002", "140", "80", Decimal("0.8")),
        ("10022", "10024", "180", "55", Decimal("1.2")),
        ("10036", "10038", "220", "40", Decimal("1.4")),
        ("10020", "10022", "70", "150", Decimal("-1.0")),
    )
    return tuple(
        GateSnapshot(
            book=_book(
                timestamp=base_time + timedelta(seconds=index),
                bid=bid,
                ask=ask,
                bid_qty=bid_qty,
                ask_qty=ask_qty,
                tick_velocity=velocity,
            )
        )
        for index, (bid, ask, bid_qty, ask_qty, velocity) in enumerate(specs)
    )


def blocked_spread_snapshot() -> GateSnapshot:
    return GateSnapshot(
        book=_book(
            timestamp=datetime(2026, 7, 8, 10, 0, tzinfo=UTC),
            bid="10000",
            ask="10004",
            bid_qty="140",
            ask_qty="80",
            tick_velocity=Decimal("0.4"),
        )
    )


def _book(
    *,
    timestamp: datetime,
    bid: str,
    ask: str,
    bid_qty: str,
    ask_qty: str,
    tick_velocity: Decimal,
) -> BookSnapshot:
    best_bid = Decimal(bid)
    best_ask = Decimal(ask)
    return BookSnapshot.from_levels(
        timestamp=timestamp,
        instrument=SwarmInstrument.NEOBITOK,
        bids=_levels(best_bid, Decimal(bid_qty), descending=True),
        asks=_levels(best_ask, Decimal(ask_qty), descending=False),
        tick_size=Decimal("1"),
        last_price=(best_bid + best_ask) / Decimal("2"),
        last_trade_size=Decimal("1"),
        trade_side=LegSide.LONG if tick_velocity >= 0 else LegSide.SHORT,
        tick_direction=1 if tick_velocity >= 0 else -1,
        tick_velocity=tick_velocity,
        volume_delta_1s=Decimal(bid_qty) - Decimal(ask_qty),
        volume_delta_5s=(Decimal(bid_qty) - Decimal(ask_qty)) * Decimal("2"),
        volume_delta_15s=(Decimal(bid_qty) - Decimal(ask_qty)) * Decimal("3"),
        volatility_5s=Decimal("1.2"),
        volatility_15s=Decimal("1.4"),
        volatility_60s=Decimal("1.8"),
        mfi=Decimal("55"),
        latency_ms=100,
    )


def _levels(
    price: Decimal, quantity: Decimal, *, descending: bool
) -> tuple[tuple[Decimal, Decimal], ...]:
    sign = Decimal("-1") if descending else Decimal("1")
    return (
        (price, quantity),
        (price + sign * Decimal("1"), quantity * Decimal("0.7")),
        (price + sign * Decimal("2"), quantity * Decimal("0.5")),
        (price + sign * Decimal("3"), quantity * Decimal("0.3")),
        (price + sign * Decimal("4"), quantity * Decimal("0.2")),
    )


__all__ = ["blocked_spread_snapshot", "smoke_snapshots"]
