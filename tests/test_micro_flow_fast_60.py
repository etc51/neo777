from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from neobitcoin_paper.calendar import SessionCalendar
from neobitcoin_paper.domain import (
    BookLevel,
    DataQuality,
    MarketEvent,
    OrderBook,
    Side,
    StrategyStatus,
)
from neobitcoin_paper.registry import StrategyRegistry
from neobitcoin_paper.strategies import (
    FeatureSnapshot,
    MicroFlowFast60Strategy,
    StrategyContext,
    frozen_micro_flow_fast_60_v1,
)

NOW = datetime(2026, 7, 22, 9, 0, tzinfo=UTC)
UID = "4effa274-4e8f-422c-93ff-04aa34fe8e39"


def _strategy() -> MicroFlowFast60Strategy:
    return MicroFlowFast60Strategy(
        frozen_micro_flow_fast_60_v1(
            created_at=NOW - timedelta(minutes=2),
            activated_at=NOW - timedelta(minutes=1),
        )
    )


def _context(
    strategy: MicroFlowFast60Strategy,
    at: datetime,
    kind: str,
    *,
    bid_qty: str = "90",
    ask_qty: str = "10",
    side: str | None = None,
    quantity: str = "10",
) -> StrategyContext:
    values: dict[str, object] = {}
    book = None
    if kind == "trade":
        values = {"side": side or "UNKNOWN", "quantity": quantity, "price": "100.1"}
    if kind == "orderbook":
        book = OrderBook(
            event_id=f"book-{at.timestamp()}-{bid_qty}",
            instrument_uid=UID,
            exchange_ts=at,
            receive_ts=at,
            processing_ts=at + timedelta(milliseconds=1),
            bids=(BookLevel(Decimal("100.0"), Decimal(bid_qty)),),
            asks=(BookLevel(Decimal("100.1"), Decimal(ask_qty)),),
            trading_status="NORMAL_TRADING",
            data_quality=DataQuality.GOOD,
            reconnect_generation=3,
        )
    event = MarketEvent(
        event_id=book.event_id if book else f"trade-{at.timestamp()}-{side}",
        instrument_uid=UID,
        event_type=kind,
        exchange_ts=at,
        receive_ts=at,
        processing_ts=at + timedelta(milliseconds=1),
        trading_status="NORMAL_TRADING",
        data_quality=DataQuality.GOOD,
        reconnect_generation=3,
        values=values,
    )
    return StrategyContext(
        event=event,
        book=book,
        features=FeatureSnapshot(at, (event.event_id,), False),
        session=SessionCalendar().resolve(at, "NORMAL_TRADING"),
        data_quality=DataQuality.GOOD,
        own_state={strategy.specification.key: {"pending": False, "open": False}},
    )


def test_registry_is_forward_only_active_and_uses_dedicated_account() -> None:
    spec = frozen_micro_flow_fast_60_v1(created_at=NOW, activated_at=NOW + timedelta(seconds=2))
    registry = StrategyRegistry().register(
        spec, registered_at=NOW + timedelta(seconds=1), initial_cash=Decimal("100000")
    )
    assert spec.status is StrategyStatus.FROZEN_PAPER_OOS_ACCUMULATION
    assert not spec.is_active_at(NOW + timedelta(seconds=1))
    assert spec.is_active_at(NOW + timedelta(seconds=2))
    assert spec.parameters["historical_backfill_to_live_account"] is False
    assert registry.account_for("MICRO_FLOW_FAST_60", "v1").account_id == "MICRO_FLOW_FAST_60_v1"


def test_completed_second_uses_last_book_and_receive_time_trade_window() -> None:
    strategy = _strategy()
    assert (
        strategy.evaluate(
            _context(strategy, NOW + timedelta(milliseconds=100), "trade", side="BUY", quantity="9")
        )
        is None
    )
    assert (
        strategy.evaluate(
            _context(
                strategy, NOW + timedelta(milliseconds=200), "trade", side="UNKNOWN", quantity="99"
            )
        )
        is None
    )
    assert (
        strategy.evaluate(
            _context(
                strategy, NOW + timedelta(milliseconds=300), "orderbook", bid_qty="70", ask_qty="30"
            )
        )
        is None
    )
    # The last valid book in the second wins: offset is (90-10)/(90+10)=0.80.
    assert (
        strategy.evaluate(_context(strategy, NOW + timedelta(milliseconds=900), "orderbook"))
        is None
    )
    decision = strategy.evaluate(
        _context(
            strategy,
            NOW + timedelta(seconds=1, milliseconds=10),
            "trade",
            side="SELL",
            quantity="100",
        )
    )
    assert decision is not None and decision.signal and decision.intent is not None
    assert decision.intent.side is Side.BUY
    assert decision.intent.exit_policy.time_exit_seconds == 60
    assert decision.conditions["microprice_offset"] == Decimal("0.8")
    assert decision.conditions["trade_flow_ratio_5s"] == Decimal("1")
    assert decision.conditions["unknown_volume_5s"] == Decimal("99")
    assert decision.conditions["sample_book_event_id"].endswith("-90")


def test_rising_edge_is_consumed_until_condition_turns_false() -> None:
    strategy = _strategy()
    strategy.evaluate(_context(strategy, NOW, "trade", side="BUY"))
    strategy.evaluate(_context(strategy, NOW + timedelta(milliseconds=900), "orderbook"))
    first = strategy.evaluate(_context(strategy, NOW + timedelta(seconds=1), "orderbook"))
    assert first is not None and first.signal and first.intent is not None
    strategy.on_intent_accepted(first.intent)
    held = strategy.evaluate(_context(strategy, NOW + timedelta(seconds=2), "orderbook"))
    assert held is not None and not held.signal
    assert held.conditions["directional_rising_edge"] is False
