"""Execution-model tests for the isolated Neobitcoin research system."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from neo_trader.neobitcoin_research.execution import (
    simulate_aggressive_sweep,
    simulate_ideal_top_of_book,
    simulate_passive_queue,
)
from neo_trader.neobitcoin_research.types import (
    BookLevel,
    ExecutionModel,
    ExecutionReason,
    ExecutionRequest,
    ExecutionSide,
    ExecutionStatus,
    OrderBookSnapshot,
    OrderBookValidationError,
    PassiveQueueObservation,
    PassiveQueueScenario,
)

NOW = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)


def test_order_book_validation_is_strict() -> None:
    with pytest.raises(OrderBookValidationError, match="strictly descending"):
        _book(bids=(("99", "1"), ("100", "1")))
    with pytest.raises(OrderBookValidationError, match="strictly ascending"):
        _book(asks=(("101", "1"), ("100", "1")))
    with pytest.raises(OrderBookValidationError, match="best_bid < best_ask"):
        _book(bids=(("101", "1"),), asks=(("101", "1"),))
    with pytest.raises(OrderBookValidationError, match="inconsistent"):
        _book(is_consistent=False)
    with pytest.raises(OrderBookValidationError, match="quantity must be positive"):
        BookLevel(Decimal("100"), Decimal("0"))


def test_tick_alignment_is_validated_for_books_and_orders() -> None:
    with pytest.raises(OrderBookValidationError, match="tick_size"):
        _book(bids=(("99.95", "1"),), tick_size="0.1")

    book = _book(tick_size="0.1")
    request = ExecutionRequest(ExecutionSide.BUY, Decimal("1"), Decimal("100.05"))
    with pytest.raises(ValueError, match="limit_price"):
        simulate_aggressive_sweep(book, request)


def test_ideal_top_of_book_is_explicitly_optimistic_for_buy_and_sell() -> None:
    book = _book(
        bids=(("99", "2"), ("98", "100")),
        asks=(("100", "3"), ("101", "100")),
    )

    buy = simulate_ideal_top_of_book(
        book,
        ExecutionRequest(ExecutionSide.BUY, Decimal("50")),
    )
    sell = simulate_ideal_top_of_book(
        book,
        ExecutionRequest(ExecutionSide.SELL, Decimal("40")),
    )

    assert buy.model is ExecutionModel.IDEAL_TOP_OF_BOOK
    assert buy.status is ExecutionStatus.FULL_FILL
    assert buy.average_price == Decimal("100")
    assert buy.filled_quantity == Decimal("50")
    assert buy.sweep_vwap is None
    assert buy.levels_consumed == 1
    assert sell.average_price == Decimal("99")
    assert sell.filled_quantity == Decimal("40")


def test_aggressive_buy_sweep_vwap_matches_required_two_level_example() -> None:
    book = _book(
        bids=(("99", "100"),),
        asks=(("100", "100"), ("101", "100")),
    )

    result = simulate_aggressive_sweep(
        book,
        ExecutionRequest(ExecutionSide.BUY, Decimal("150")),
    )

    assert result.model is ExecutionModel.AGGRESSIVE_SWEEP
    assert result.status is ExecutionStatus.FULL_FILL
    assert result.filled_quantity == Decimal("150")
    assert result.unfilled_quantity == Decimal("0")
    assert result.fill_ratio == Decimal("1")
    assert result.levels_consumed == 2
    assert result.top_level_price == Decimal("100")
    assert result.sweep_vwap == Decimal("100.3333333333333333333333333")
    assert result.average_price == result.sweep_vwap
    assert tuple(fill.quantity for fill in result.fills) == (
        Decimal("100"),
        Decimal("50"),
    )
    assert result.slippage_vs_top == Decimal("0.3333333333333333333333333")
    assert result.slippage_cost == Decimal("50.00000000000000000000000000")


def test_aggressive_sell_sweeps_bids_and_reports_partial_fill() -> None:
    book = _book(
        bids=(("100", "100"), ("99", "50")),
        asks=(("101", "100"),),
    )

    result = simulate_aggressive_sweep(
        book,
        ExecutionRequest(ExecutionSide.SELL, Decimal("200")),
    )

    assert result.status is ExecutionStatus.PARTIAL_FILL
    assert result.reason is ExecutionReason.INSUFFICIENT_LIQUIDITY
    assert result.filled_quantity == Decimal("150")
    assert result.unfilled_quantity == Decimal("50")
    assert result.fill_ratio == Decimal("0.75")
    assert result.levels_consumed == 2
    assert result.sweep_vwap == Decimal("99.66666666666666666666666667")
    assert result.slippage_vs_top == Decimal("0.33333333333333333333333333")


def test_aggressive_no_liquidity_and_unreached_limit_are_no_fill() -> None:
    no_asks = _book(bids=(("99", "10"),), asks=())
    no_liquidity = simulate_aggressive_sweep(
        no_asks,
        ExecutionRequest(ExecutionSide.BUY, Decimal("1")),
    )
    assert no_liquidity.status is ExecutionStatus.NO_FILL
    assert no_liquidity.reason is ExecutionReason.NO_LIQUIDITY
    assert no_liquidity.average_price is None

    book = _book()
    limit_not_reached = simulate_aggressive_sweep(
        book,
        ExecutionRequest(ExecutionSide.BUY, Decimal("1"), Decimal("99.5")),
    )
    assert limit_not_reached.status is ExecutionStatus.NO_FILL
    assert limit_not_reached.reason is ExecutionReason.LIMIT_PRICE_NOT_REACHED
    assert limit_not_reached.top_level_price == Decimal("100")


def test_passive_queue_scenarios_are_separate_and_conservative() -> None:
    book = _book()
    request = ExecutionRequest(ExecutionSide.BUY, Decimal("50"), Decimal("99"))
    observation = PassiveQueueObservation(
        volume_ahead=Decimal("100"),
        traded_at_level=Decimal("120"),
        cancelled_at_level=Decimal("40"),
        added_at_level=Decimal("20"),
        elapsed_ms=500,
        timeout_ms=1_000,
    )

    optimistic = simulate_passive_queue(
        book,
        request,
        observation,
        scenario=PassiveQueueScenario.OPTIMISTIC,
    )
    base = simulate_passive_queue(book, request, observation)
    pessimistic = simulate_passive_queue(
        book,
        request,
        observation,
        scenario=PassiveQueueScenario.PESSIMISTIC,
    )

    assert optimistic.model is ExecutionModel.PASSIVE_OPTIMISTIC
    assert base.model is ExecutionModel.PASSIVE_BASE
    assert pessimistic.model is ExecutionModel.PASSIVE_PESSIMISTIC
    assert optimistic.filled_quantity == Decimal("50")
    assert base.filled_quantity == Decimal("30")
    assert pessimistic.filled_quantity == Decimal("0")
    assert optimistic.queue is not None and optimistic.queue.effective_volume_ahead == 60
    assert base.queue is not None and base.queue.effective_volume_ahead == 90
    assert pessimistic.queue is not None and pessimistic.queue.effective_volume_ahead == 120
    assert base.sweep_vwap is None


def test_passive_touch_without_queue_consumption_is_no_fill() -> None:
    result = simulate_passive_queue(
        _book(),
        ExecutionRequest(ExecutionSide.BUY, Decimal("10"), Decimal("99")),
        PassiveQueueObservation(
            volume_ahead=Decimal("100"),
            traded_at_level=Decimal("100"),
            elapsed_ms=100,
            timeout_ms=1_000,
        ),
    )

    assert result.status is ExecutionStatus.NO_FILL
    assert result.reason is ExecutionReason.QUEUE_NOT_REACHED
    assert result.filled_quantity == 0


def test_passive_partial_then_cancel_and_timeout_statuses_are_preserved() -> None:
    book = _book()
    request = ExecutionRequest(ExecutionSide.SELL, Decimal("10"), Decimal("100"))
    cancelled = simulate_passive_queue(
        book,
        request,
        PassiveQueueObservation(
            volume_ahead=Decimal("0"),
            traded_at_level=Decimal("3"),
            elapsed_ms=200,
            timeout_ms=1_000,
            order_cancelled=True,
        ),
    )
    expired = simulate_passive_queue(
        book,
        request,
        PassiveQueueObservation(
            volume_ahead=Decimal("100"),
            traded_at_level=Decimal("0"),
            elapsed_ms=1_000,
            timeout_ms=1_000,
        ),
    )

    assert cancelled.status is ExecutionStatus.CANCELLED
    assert cancelled.reason is ExecutionReason.ORDER_CANCELLED
    assert cancelled.is_partial_fill
    assert cancelled.filled_quantity == Decimal("3")
    assert expired.status is ExecutionStatus.EXPIRED
    assert expired.reason is ExecutionReason.TIMEOUT
    assert expired.is_no_fill


def test_passive_market_moved_away_and_adverse_selection_are_recorded() -> None:
    moved = simulate_passive_queue(
        _book(),
        ExecutionRequest(ExecutionSide.BUY, Decimal("10"), Decimal("99")),
        PassiveQueueObservation(
            volume_ahead=Decimal("20"),
            traded_at_level=Decimal("0"),
            market_moved_away=True,
        ),
    )
    adverse = simulate_passive_queue(
        _book(),
        ExecutionRequest(ExecutionSide.BUY, Decimal("10"), Decimal("99")),
        PassiveQueueObservation(
            volume_ahead=Decimal("0"),
            traded_at_level=Decimal("10"),
            post_fill_mid_price=Decimal("98"),
        ),
    )

    assert moved.status is ExecutionStatus.NO_FILL
    assert moved.reason is ExecutionReason.MARKET_MOVED_AWAY
    assert adverse.status is ExecutionStatus.FULL_FILL
    assert adverse.adverse_selection_per_unit == Decimal("1")
    assert adverse.adverse_selection_cost == Decimal("10")


def test_passive_marketable_price_is_rejected_for_buy_and_sell() -> None:
    book = _book()
    observation = PassiveQueueObservation(volume_ahead=0, traded_at_level=0)
    with pytest.raises(ValueError, match="passive BUY"):
        simulate_passive_queue(
            book,
            ExecutionRequest(ExecutionSide.BUY, 1, Decimal("100")),
            observation,
        )
    with pytest.raises(ValueError, match="passive SELL"):
        simulate_passive_queue(
            book,
            ExecutionRequest(ExecutionSide.SELL, 1, Decimal("99")),
            observation,
        )


@given(
    first_quantity=st.integers(min_value=1, max_value=1_000),
    second_quantity=st.integers(min_value=1, max_value=1_000),
    requested=st.integers(min_value=1, max_value=3_000),
)
def test_aggressive_sweep_quantity_and_notional_invariants(
    first_quantity: int,
    second_quantity: int,
    requested: int,
) -> None:
    result = simulate_aggressive_sweep(
        _book(
            asks=(("100", str(first_quantity)), ("101", str(second_quantity))),
        ),
        ExecutionRequest(ExecutionSide.BUY, requested),
    )

    available = Decimal(first_quantity + second_quantity)
    expected_filled = min(Decimal(requested), available)
    assert Decimal("0") <= result.filled_quantity <= Decimal(requested)
    assert result.filled_quantity == expected_filled
    assert result.unfilled_quantity == Decimal(requested) - result.filled_quantity
    assert result.fill_ratio == result.filled_quantity / Decimal(requested)
    assert sum((fill.quantity for fill in result.fills), Decimal("0")) == result.filled_quantity
    assert result.levels_consumed == len(result.fills)
    assert result.sweep_vwap is not None
    assert Decimal("100") <= result.sweep_vwap <= Decimal("101")
    assert result.slippage_vs_top is not None and result.slippage_vs_top >= 0


@given(
    volume_ahead=st.integers(min_value=0, max_value=1_000),
    traded=st.integers(min_value=0, max_value=2_000),
    cancelled=st.integers(min_value=0, max_value=1_000),
    added=st.integers(min_value=0, max_value=1_000),
    requested=st.integers(min_value=1, max_value=1_000),
)
def test_passive_scenario_fill_monotonicity(
    volume_ahead: int,
    traded: int,
    cancelled: int,
    added: int,
    requested: int,
) -> None:
    book = _book()
    request = ExecutionRequest(ExecutionSide.BUY, requested, Decimal("99"))
    observation = PassiveQueueObservation(
        volume_ahead=volume_ahead,
        traded_at_level=traded,
        cancelled_at_level=cancelled,
        added_at_level=added,
    )
    results = {
        scenario: simulate_passive_queue(
            book,
            request,
            observation,
            scenario=scenario,
        )
        for scenario in PassiveQueueScenario
    }

    optimistic = results[PassiveQueueScenario.OPTIMISTIC]
    base = results[PassiveQueueScenario.BASE]
    pessimistic = results[PassiveQueueScenario.PESSIMISTIC]
    assert optimistic.filled_quantity >= base.filled_quantity
    assert base.filled_quantity >= pessimistic.filled_quantity
    for result in results.values():
        assert Decimal("0") <= result.filled_quantity <= Decimal(requested)
        assert result.unfilled_quantity == Decimal(requested) - result.filled_quantity
        assert result.fill_ratio == result.filled_quantity / Decimal(requested)


def _book(
    *,
    bids: tuple[tuple[str, str], ...] = (("99", "100"), ("98", "100")),
    asks: tuple[tuple[str, str], ...] = (("100", "100"), ("101", "100")),
    is_consistent: bool = True,
    tick_size: str | None = None,
) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        instrument_uid="NEOBITCOIN-UID",
        exchange_timestamp=NOW,
        bids=tuple(BookLevel(Decimal(price), Decimal(quantity)) for price, quantity in bids),
        asks=tuple(BookLevel(Decimal(price), Decimal(quantity)) for price, quantity in asks),
        is_consistent=is_consistent,
        tick_size=None if tick_size is None else Decimal(tick_size),
        snapshot_id="snapshot-1",
    )
