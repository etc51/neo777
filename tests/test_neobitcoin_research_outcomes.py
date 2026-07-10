from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from neo_trader.neobitcoin_research.config import ResearchConfig
from neo_trader.neobitcoin_research.outcomes import FutureOutcomeTracker
from neo_trader.neobitcoin_research.types import BookLevel, OrderBookSnapshot


def _book(timestamp: datetime, shift: Decimal = Decimal(0)) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        instrument_uid="uid",
        exchange_timestamp=timestamp,
        bids=tuple(
            BookLevel(Decimal("100") + shift - Decimal(index) / 10, Decimal(100))
            for index in range(20)
        ),
        asks=tuple(
            BookLevel(Decimal("100.1") + shift + Decimal(index) / 10, Decimal(100))
            for index in range(20)
        ),
        tick_size=Decimal("0.1"),
    )


def test_future_outcomes_use_book_after_latency_and_keep_no_fill_null() -> None:
    config = ResearchConfig(
        position_sizes_rub=(10_000,),
        latencies_ms=(0,),
        horizons_seconds=(5,),
    )
    tracker = FutureOutcomeTracker(config=config, lot_size=1, tick_size=Decimal("0.1"))
    start = datetime(2026, 7, 10, tzinfo=UTC)
    first = _book(start)
    tracker.add_book(first)
    assert (
        tracker.register(feature_snapshot_id="feature-1", signal_time=start, signal_book=first) == 1
    )
    rows = tracker.add_book(_book(start + timedelta(seconds=5), Decimal("0.5")))
    aggressive_long = next(
        row
        for row in rows
        if row.get("execution_model") == "AGGRESSIVE_SWEEP" and row.get("direction") == "LONG"
    )
    assert Decimal(str(aggressive_long["realized_net_pnl_rub"])) > 0
    passive = [row for row in rows if str(row.get("execution_model", "")).startswith("PASSIVE")]
    assert len(passive) == 6
    assert all(
        row["realized_net_pnl_rub"] is None
        for row in passive
        if row["status"] in {"NO_FILL", "EXPIRED", "CANCELLED"}
    )


def test_cross_midnight_outcomes_are_invalid_without_daily_fee_rate() -> None:
    config = ResearchConfig(
        position_sizes_rub=(10_000,),
        latencies_ms=(0,),
        horizons_seconds=(5,),
        daily_holding_fee_annual_rate=None,
    )
    tracker = FutureOutcomeTracker(config=config, lot_size=1, tick_size=Decimal("0.1"))
    start = datetime(2026, 7, 10, 20, 59, 58, tzinfo=UTC)
    first = _book(start)
    tracker.add_book(first)
    tracker.register(feature_snapshot_id="cross-midnight", signal_time=start, signal_book=first)

    rows = tracker.add_book(_book(start + timedelta(seconds=5), Decimal("0.1")))

    assert len(rows) == 8
    assert {row["status"] for row in rows} == {"INVALID_DATA"}
    assert {row["reason"] for row in rows} == {"daily_holding_fee_rate_not_configured"}
