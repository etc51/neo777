"""Deterministic, order-free execution models for Neobitcoin research."""

from __future__ import annotations

from decimal import Decimal

from neo_trader.neobitcoin_research.types import (
    BookLevel,
    ExecutionModel,
    ExecutionReason,
    ExecutionRequest,
    ExecutionResult,
    ExecutionSide,
    ExecutionStatus,
    LevelFill,
    OrderBookSnapshot,
    PassiveQueueObservation,
    PassiveQueueScenario,
    QueueDiagnostics,
)

_QUEUE_FACTORS: dict[PassiveQueueScenario, tuple[Decimal, Decimal]] = {
    PassiveQueueScenario.OPTIMISTIC: (Decimal("1"), Decimal("0")),
    PassiveQueueScenario.BASE: (Decimal("0.5"), Decimal("0.5")),
    PassiveQueueScenario.PESSIMISTIC: (Decimal("0"), Decimal("1")),
}

_PASSIVE_MODELS: dict[PassiveQueueScenario, ExecutionModel] = {
    PassiveQueueScenario.OPTIMISTIC: ExecutionModel.PASSIVE_OPTIMISTIC,
    PassiveQueueScenario.BASE: ExecutionModel.PASSIVE_BASE,
    PassiveQueueScenario.PESSIMISTIC: ExecutionModel.PASSIVE_PESSIMISTIC,
}


def simulate_ideal_top_of_book(
    book: OrderBookSnapshot,
    request: ExecutionRequest,
) -> ExecutionResult:
    """Fill the whole request at best ask/bid, ignoring displayed top size.

    This is an intentionally optimistic diagnostic bound. It must not be mixed
    with aggressive sweep or passive queue results.
    """

    _validate_request_for_book(book, request)
    levels = _consuming_levels(book, request.side)
    top_price = levels[0].price if levels else None
    if top_price is None:
        return _empty_result(
            book=book,
            request=request,
            model=ExecutionModel.IDEAL_TOP_OF_BOOK,
            reason=ExecutionReason.NO_LIQUIDITY,
            top_level_price=None,
        )
    if not _price_allowed(request.side, top_price, request.limit_price):
        return _empty_result(
            book=book,
            request=request,
            model=ExecutionModel.IDEAL_TOP_OF_BOOK,
            reason=ExecutionReason.LIMIT_PRICE_NOT_REACHED,
            top_level_price=top_price,
        )

    fills = (LevelFill(level_index=1, price=top_price, quantity=request.quantity),)
    return _filled_result(
        book=book,
        request=request,
        model=ExecutionModel.IDEAL_TOP_OF_BOOK,
        status=ExecutionStatus.FULL_FILL,
        reason=ExecutionReason.FULLY_FILLED,
        fills=fills,
        top_level_price=top_price,
        sweep_vwap=None,
        queue=None,
        adverse_selection_per_unit=None,
    )


def simulate_aggressive_sweep(
    book: OrderBookSnapshot,
    request: ExecutionRequest,
) -> ExecutionResult:
    """Consume opposite-side depth and report a partial-fill-aware sweep VWAP."""

    _validate_request_for_book(book, request)
    levels = _consuming_levels(book, request.side)
    top_price = levels[0].price if levels else None
    if not levels:
        return _empty_result(
            book=book,
            request=request,
            model=ExecutionModel.AGGRESSIVE_SWEEP,
            reason=ExecutionReason.NO_LIQUIDITY,
            top_level_price=None,
        )

    remaining = request.quantity
    fills: list[LevelFill] = []
    for level_index, level in enumerate(levels, start=1):
        if remaining <= 0:
            break
        if not _price_allowed(request.side, level.price, request.limit_price):
            break
        consumed = min(remaining, level.quantity)
        if consumed <= 0:
            continue
        fills.append(LevelFill(level_index=level_index, price=level.price, quantity=consumed))
        remaining -= consumed

    if not fills:
        return _empty_result(
            book=book,
            request=request,
            model=ExecutionModel.AGGRESSIVE_SWEEP,
            reason=ExecutionReason.LIMIT_PRICE_NOT_REACHED,
            top_level_price=top_price,
        )

    filled = request.quantity - remaining
    sweep_vwap = (
        sum(
            (fill.price * fill.quantity for fill in fills),
            Decimal("0"),
        )
        / filled
    )
    full = remaining == 0
    return _filled_result(
        book=book,
        request=request,
        model=ExecutionModel.AGGRESSIVE_SWEEP,
        status=ExecutionStatus.FULL_FILL if full else ExecutionStatus.PARTIAL_FILL,
        reason=(ExecutionReason.FULLY_FILLED if full else ExecutionReason.INSUFFICIENT_LIQUIDITY),
        fills=tuple(fills),
        top_level_price=top_price,
        sweep_vwap=sweep_vwap,
        queue=None,
        adverse_selection_per_unit=None,
    )


def simulate_passive_queue(
    book: OrderBookSnapshot,
    request: ExecutionRequest,
    observation: PassiveQueueObservation,
    *,
    scenario: PassiveQueueScenario = PassiveQueueScenario.BASE,
) -> ExecutionResult:
    """Simulate a non-marketable limit order with an aggregate queue model.

    The aggregate feed cannot identify individual cancellations/additions. The
    three scenarios therefore apply explicit attribution assumptions:

    - optimistic: all cancellations reduce volume ahead; additions join behind;
    - base: half of cancellations and half of additions affect volume ahead;
    - pessimistic: cancellations give no credit; all additions join ahead.

    Observed traded volume first consumes effective volume ahead and only then
    fills the simulated order. A mere price touch never creates a fill.
    """

    _validate_request_for_book(book, request)
    scenario = _queue_scenario(scenario)
    if request.limit_price is None:
        raise ValueError("passive queue simulation requires limit_price")
    _require_passive_price(book, request)

    cancellation_factor, addition_factor = _QUEUE_FACTORS[scenario]
    cancellation_credit = observation.cancelled_at_level * cancellation_factor
    addition_penalty = observation.added_at_level * addition_factor
    effective_ahead = max(
        observation.volume_ahead - cancellation_credit + addition_penalty,
        Decimal("0"),
    )
    volume_ahead_remaining = max(
        effective_ahead - observation.traded_at_level,
        Decimal("0"),
    )
    volume_reaching_order = max(
        observation.traded_at_level - effective_ahead,
        Decimal("0"),
    )
    filled = min(request.quantity, volume_reaching_order)

    queue = QueueDiagnostics(
        scenario=scenario,
        initial_volume_ahead=observation.volume_ahead,
        cancellation_credit=cancellation_credit,
        addition_penalty=addition_penalty,
        effective_volume_ahead=effective_ahead,
        traded_at_level=observation.traded_at_level,
        cancelled_at_level=observation.cancelled_at_level,
        added_at_level=observation.added_at_level,
        volume_ahead_remaining=volume_ahead_remaining,
    )
    status, reason = _passive_status(request, observation, filled)
    fills = (
        (LevelFill(level_index=1, price=request.limit_price, quantity=filled),)
        if filled > 0
        else ()
    )
    adverse_selection = _adverse_selection(
        side=request.side,
        fill_price=request.limit_price,
        filled_quantity=filled,
        post_fill_mid_price=observation.post_fill_mid_price,
    )
    top_price = _top_price(book, request.side)

    if not fills:
        return _result(
            book=book,
            request=request,
            model=_PASSIVE_MODELS[scenario],
            status=status,
            reason=reason,
            fills=(),
            top_level_price=top_price,
            average_price=None,
            sweep_vwap=None,
            queue=queue,
            adverse_selection_per_unit=None,
        )
    return _filled_result(
        book=book,
        request=request,
        model=_PASSIVE_MODELS[scenario],
        status=status,
        reason=reason,
        fills=fills,
        top_level_price=top_price,
        sweep_vwap=None,
        queue=queue,
        adverse_selection_per_unit=adverse_selection,
    )


def _passive_status(
    request: ExecutionRequest,
    observation: PassiveQueueObservation,
    filled: Decimal,
) -> tuple[ExecutionStatus, ExecutionReason]:
    if filled == request.quantity:
        return ExecutionStatus.FULL_FILL, ExecutionReason.FULLY_FILLED
    if observation.order_cancelled:
        return ExecutionStatus.CANCELLED, ExecutionReason.ORDER_CANCELLED
    if observation.elapsed_ms >= observation.timeout_ms:
        return ExecutionStatus.EXPIRED, ExecutionReason.TIMEOUT
    if observation.market_moved_away:
        status = ExecutionStatus.PARTIAL_FILL if filled > 0 else ExecutionStatus.NO_FILL
        return status, ExecutionReason.MARKET_MOVED_AWAY
    if filled > 0:
        return ExecutionStatus.PARTIAL_FILL, ExecutionReason.PARTIAL_QUEUE_FILL
    return ExecutionStatus.NO_FILL, ExecutionReason.QUEUE_NOT_REACHED


def _empty_result(
    *,
    book: OrderBookSnapshot,
    request: ExecutionRequest,
    model: ExecutionModel,
    reason: ExecutionReason,
    top_level_price: Decimal | None,
) -> ExecutionResult:
    return _result(
        book=book,
        request=request,
        model=model,
        status=ExecutionStatus.NO_FILL,
        reason=reason,
        fills=(),
        top_level_price=top_level_price,
        average_price=None,
        sweep_vwap=None,
        queue=None,
        adverse_selection_per_unit=None,
    )


def _filled_result(
    *,
    book: OrderBookSnapshot,
    request: ExecutionRequest,
    model: ExecutionModel,
    status: ExecutionStatus,
    reason: ExecutionReason,
    fills: tuple[LevelFill, ...],
    top_level_price: Decimal | None,
    sweep_vwap: Decimal | None,
    queue: QueueDiagnostics | None,
    adverse_selection_per_unit: Decimal | None,
) -> ExecutionResult:
    filled = sum((fill.quantity for fill in fills), Decimal("0"))
    average_price = (
        sum(
            (fill.price * fill.quantity for fill in fills),
            Decimal("0"),
        )
        / filled
    )
    return _result(
        book=book,
        request=request,
        model=model,
        status=status,
        reason=reason,
        fills=fills,
        top_level_price=top_level_price,
        average_price=average_price,
        sweep_vwap=sweep_vwap,
        queue=queue,
        adverse_selection_per_unit=adverse_selection_per_unit,
    )


def _result(
    *,
    book: OrderBookSnapshot,
    request: ExecutionRequest,
    model: ExecutionModel,
    status: ExecutionStatus,
    reason: ExecutionReason,
    fills: tuple[LevelFill, ...],
    top_level_price: Decimal | None,
    average_price: Decimal | None,
    sweep_vwap: Decimal | None,
    queue: QueueDiagnostics | None,
    adverse_selection_per_unit: Decimal | None,
) -> ExecutionResult:
    filled = sum((fill.quantity for fill in fills), Decimal("0"))
    unfilled = request.quantity - filled
    slippage_vs_top, slippage_vs_mid = _slippage(
        side=request.side,
        average_price=average_price,
        top_level_price=top_level_price,
        mid_price=book.mid_price,
    )
    return ExecutionResult(
        model=model,
        side=request.side,
        status=status,
        reason=reason,
        instrument_uid=book.instrument_uid,
        book_timestamp=book.exchange_timestamp,
        snapshot_id=book.snapshot_id,
        requested_quantity=request.quantity,
        filled_quantity=filled,
        unfilled_quantity=unfilled,
        fill_ratio=filled / request.quantity,
        limit_price=request.limit_price,
        top_level_price=top_level_price,
        average_price=average_price,
        sweep_vwap=sweep_vwap,
        levels_consumed=len(fills),
        fills=fills,
        slippage_vs_top=slippage_vs_top,
        slippage_vs_mid=slippage_vs_mid,
        slippage_cost=_slippage_cost(
            side=request.side,
            fills=fills,
            top_level_price=top_level_price,
        ),
        adverse_selection_per_unit=adverse_selection_per_unit,
        adverse_selection_cost=(
            None if adverse_selection_per_unit is None else adverse_selection_per_unit * filled
        ),
        queue=queue,
    )


def _validate_request_for_book(
    book: OrderBookSnapshot,
    request: ExecutionRequest,
) -> None:
    if not isinstance(book, OrderBookSnapshot):
        raise TypeError("book must be OrderBookSnapshot")
    if not isinstance(request, ExecutionRequest):
        raise TypeError("request must be ExecutionRequest")
    if (
        request.limit_price is not None
        and book.tick_size is not None
        and request.limit_price % book.tick_size != 0
    ):
        raise ValueError("limit_price must be aligned to book.tick_size")


def _require_passive_price(book: OrderBookSnapshot, request: ExecutionRequest) -> None:
    limit_price = request.limit_price
    if limit_price is None:
        raise ValueError("passive order requires limit_price")
    if (
        request.side is ExecutionSide.BUY
        and book.best_ask is not None
        and limit_price >= book.best_ask
    ):
        raise ValueError("passive BUY must be below best ask; use aggressive sweep")
    if (
        request.side is ExecutionSide.SELL
        and book.best_bid is not None
        and limit_price <= book.best_bid
    ):
        raise ValueError("passive SELL must be above best bid; use aggressive sweep")


def _consuming_levels(
    book: OrderBookSnapshot,
    side: ExecutionSide,
) -> tuple[BookLevel, ...]:
    return book.asks if side is ExecutionSide.BUY else book.bids


def _top_price(book: OrderBookSnapshot, side: ExecutionSide) -> Decimal | None:
    return book.best_ask if side is ExecutionSide.BUY else book.best_bid


def _price_allowed(
    side: ExecutionSide,
    price: Decimal,
    limit_price: Decimal | None,
) -> bool:
    if limit_price is None:
        return True
    if side is ExecutionSide.BUY:
        return price <= limit_price
    return price >= limit_price


def _slippage(
    *,
    side: ExecutionSide,
    average_price: Decimal | None,
    top_level_price: Decimal | None,
    mid_price: Decimal | None,
) -> tuple[Decimal | None, Decimal | None]:
    if average_price is None:
        return None, None
    if side is ExecutionSide.BUY:
        versus_top = None if top_level_price is None else average_price - top_level_price
        versus_mid = None if mid_price is None else average_price - mid_price
    else:
        versus_top = None if top_level_price is None else top_level_price - average_price
        versus_mid = None if mid_price is None else mid_price - average_price
    return versus_top, versus_mid


def _slippage_cost(
    *,
    side: ExecutionSide,
    fills: tuple[LevelFill, ...],
    top_level_price: Decimal | None,
) -> Decimal | None:
    """Return exact displayed-depth cost without repeating-decimal rounding."""

    if not fills or top_level_price is None:
        return None
    if side is ExecutionSide.BUY:
        return sum(
            ((fill.price - top_level_price) * fill.quantity for fill in fills),
            Decimal("0"),
        )
    return sum(
        ((top_level_price - fill.price) * fill.quantity for fill in fills),
        Decimal("0"),
    )


def _adverse_selection(
    *,
    side: ExecutionSide,
    fill_price: Decimal,
    filled_quantity: Decimal,
    post_fill_mid_price: Decimal | None,
) -> Decimal | None:
    if filled_quantity == 0 or post_fill_mid_price is None:
        return None
    if side is ExecutionSide.BUY:
        return fill_price - post_fill_mid_price
    return post_fill_mid_price - fill_price


def _queue_scenario(value: PassiveQueueScenario | str) -> PassiveQueueScenario:
    if isinstance(value, PassiveQueueScenario):
        return value
    if isinstance(value, str):
        try:
            return PassiveQueueScenario(value.strip().upper())
        except ValueError as exc:
            raise ValueError(f"unsupported passive queue scenario: {value!r}") from exc
    raise TypeError("scenario must be PassiveQueueScenario or str")


__all__ = [
    "simulate_aggressive_sweep",
    "simulate_ideal_top_of_book",
    "simulate_passive_queue",
]
