from __future__ import annotations

import inspect
from dataclasses import FrozenInstanceError
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

import neobitcoin_paper.oracle as oracle_module
from neobitcoin_paper.calendar import CalendarRuleNotFound, SessionCalendar
from neobitcoin_paper.domain import (
    BookLevel,
    DataQuality,
    ExecutionModel,
    ExitDecision,
    ExitPolicy,
    ExitReason,
    FillStatus,
    LookAheadError,
    OrderBook,
    PaperIntent,
    PositionStatus,
    SessionLabel,
    Side,
    StrategyStatus,
    StrategyVersion,
)
from neobitcoin_paper.execution import PaperExecutionAdapter
from neobitcoin_paper.oracle import IndependentOracle
from neobitcoin_paper.registry import ActivationError, DuplicateStrategyVersion, StrategyRegistry

MSK = ZoneInfo("Europe/Moscow")
UID = "neo-bitcoin-uid"


def utc(year: int, month: int, day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=UTC)


def msk(year: int, month: int, day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=MSK).astimezone(UTC)


def book(
    received: datetime,
    *,
    bids: tuple[tuple[str, str], ...] = (("100", "10"),),
    asks: tuple[tuple[str, str], ...] = (("101", "10"),),
    event_id: str = "book-1",
    status: str | None = "NORMAL_TRADING",
    quality: DataQuality = DataQuality.GOOD,
) -> OrderBook:
    return OrderBook(
        event_id=event_id,
        instrument_uid=UID,
        exchange_ts=received - timedelta(milliseconds=20),
        receive_ts=received,
        processing_ts=received + timedelta(milliseconds=1),
        bids=tuple(BookLevel(Decimal(price), Decimal(qty)) for price, qty in bids),
        asks=tuple(BookLevel(Decimal(price), Decimal(qty)) for price, qty in asks),
        trading_status=status,
        data_quality=quality,
    )


def intent(
    decision: datetime,
    *,
    side: Side = Side.BUY,
    quantity: str = "1",
    model: ExecutionModel = ExecutionModel.AGGRESSIVE,
    limit: str | None = None,
) -> PaperIntent:
    return PaperIntent.create(
        strategy_id="ABSORB",
        strategy_version="v1",
        instrument_uid=UID,
        decision_ts=decision,
        eligible_ts=decision,
        side=side,
        quantity=quantity,
        execution_model=model,
        reason="point-in-time signal",
        confidence="0.75",
        decision_event_id="signal-event",
        limit_price=limit,
    )


def strategy_version(version: str, created: datetime, activated: datetime) -> StrategyVersion:
    return StrategyVersion(
        strategy_id="ABSORB",
        version=version,
        status=StrategyStatus.FROZEN_PAPER,
        created_at=created,
        activated_at=activated,
        discovery_source="approved-report",
        parameters={"wave": 1000, "stops": [5, 6, 8, 10]},
        code_hash=f"code-{version}",
        config_hash=f"config-{version}",
        feature_schema_version="schema-v4.1-golden",
        execution_model_version="paper-v1",
        risk_model_version="risk-v1",
    )


def test_domain_records_are_deeply_immutable_and_ids_are_deterministic() -> None:
    created = utc(2026, 7, 15, 8)
    version = strategy_version("v1", created, created + timedelta(minutes=10))
    with pytest.raises(FrozenInstanceError):
        version.version = "v2"  # type: ignore[misc]
    with pytest.raises(TypeError):
        version.parameters["wave"] = 1  # type: ignore[index]
    assert version.parameters["stops"] == (5, 6, 8, 10)

    first = intent(created)
    second = intent(created)
    assert first.intent_id == second.intent_id
    assert first.intent_id == PaperIntent.create(
        strategy_id="ABSORB",
        strategy_version="v1",
        instrument_uid=UID,
        decision_ts=created,
        eligible_ts=created,
        side=Side.BUY,
        quantity=1,
        execution_model=ExecutionModel.AGGRESSIVE,
        reason="point-in-time signal",
        confidence="0.75",
        decision_event_id="signal-event",
    ).intent_id
    with pytest.raises(FrozenInstanceError):
        first.quantity = Decimal("2")  # type: ignore[misc]


def test_calendar_effective_rule_moscow_windows_and_live_status_priority() -> None:
    calendar = SessionCalendar(holidays={date(2026, 7, 16)})

    morning = calendar.resolve(msk(2026, 7, 15, 7), "NORMAL_TRADING")
    assert morning.session_label is SessionLabel.MORNING
    assert morning.session_date_msk == date(2026, 7, 15)
    assert morning.entry_allowed
    assert morning.calendar_rule_version == "neo-moscow-2026-07-14-v1"

    main_closed = calendar.resolve(msk(2026, 7, 15, 11), "NOT_AVAILABLE_FOR_TRADING")
    assert main_closed.session_label is SessionLabel.CLOSED
    assert main_closed.scheduled_open
    assert not main_closed.entry_allowed
    unknown_live = calendar.resolve(msk(2026, 7, 15, 11))
    assert unknown_live.session_label is SessionLabel.MAIN
    assert not unknown_live.entry_allowed

    late = calendar.resolve(msk(2026, 7, 15, 23, 55), "NORMAL_TRADING")
    assert late.session_label is SessionLabel.NEO_LATE_SESSION

    after_midnight = calendar.resolve(msk(2026, 7, 16, 0, 5), "CLOSED")
    assert after_midnight.session_date_msk == date(2026, 7, 15)
    assert not after_midnight.scheduled_open

    holiday = calendar.resolve(msk(2026, 7, 16, 10), "NORMAL_TRADING")
    assert holiday.session_label is SessionLabel.HOLIDAY
    weekend_preopen = calendar.resolve(msk(2026, 7, 18, 9, 55), "OPENING_PERIOD")
    assert weekend_preopen.session_label is SessionLabel.PRE_OPEN
    assert not weekend_preopen.entry_allowed
    weekend = calendar.resolve(msk(2026, 7, 18, 10), "NORMAL_TRADING")
    assert weekend.session_label is SessionLabel.WEEKEND
    assert weekend.entry_allowed

    assert calendar.finalizer_at(date(2026, 7, 15)) == msk(2026, 7, 16, 0, 5)
    with pytest.raises(CalendarRuleNotFound):
        calendar.window_for(date(2026, 7, 13))


def test_registry_enforces_future_activation_and_separate_version_accounts() -> None:
    created = utc(2026, 7, 15, 8)
    registered = created + timedelta(minutes=1)
    registry = StrategyRegistry()
    v1 = strategy_version("v1", created, registered + timedelta(minutes=4))
    registry_v1 = registry.register(v1, registered_at=registered, initial_cash=Decimal("10000"))
    assert not registry.versions
    assert not registry_v1.active_versions(registered)
    assert registry_v1.active_versions(v1.activated_at) == (v1,)

    v2 = strategy_version("v2", created, registered + timedelta(minutes=5))
    registry_v2 = registry_v1.register(v2, registered_at=registered, initial_cash=Decimal("10000"))
    account_v1 = registry_v2.account_for("ABSORB", "v1")
    account_v2 = registry_v2.account_for("ABSORB", "v2")
    assert account_v1.account_id != account_v2.account_id
    assert account_v1 is not account_v2
    with pytest.raises(TypeError):
        registry_v2.versions[v1.key] = v2  # type: ignore[index]
    with pytest.raises(DuplicateStrategyVersion):
        registry_v2.register(v1, registered_at=registered, initial_cash=Decimal("10000"))

    retroactive = strategy_version("v3", created, registered)
    with pytest.raises(ActivationError):
        registry.register(retroactive, registered_at=registered, initial_cash=Decimal("10000"))


def test_aggressive_depth_vwap_full_partial_no_fill_and_no_lookahead() -> None:
    adapter = PaperExecutionAdapter()
    decision = utc(2026, 7, 15, 9)
    order = adapter.create_order(intent(decision, quantity="4"))
    depth = book(
        decision,
        asks=(("101", "2"), ("102", "3")),
        event_id="depth",
    )
    full = adapter.execute_aggressive(order, depth)
    assert full.status is FillStatus.FULL_FILL
    assert full.filled_quantity == Decimal("4")
    assert full.vwap == Decimal("101.5")
    assert full.slippage_cost == Decimal("2")

    large_order = adapter.create_order(intent(decision, quantity="10"))
    partial = adapter.execute_aggressive(large_order, depth)
    assert partial.status is FillStatus.PARTIAL_FILL
    assert partial.filled_quantity == Decimal("5")
    assert partial.vwap == Decimal("101.6")

    empty_ask = book(decision, asks=(), event_id="empty")
    assert adapter.execute_aggressive(order, empty_ask).status is FillStatus.NO_FILL
    stale = book(decision, event_id="stale", quality=DataQuality.STALE)
    assert adapter.execute_aggressive(order, stale).status is FillStatus.NO_FILL
    with pytest.raises(LookAheadError):
        adapter.execute_aggressive(order, book(decision - timedelta(microseconds=1)))

    report = IndependentOracle.validate_aggressive(order, depth, full)
    assert report.passed
    assert report.unexplained_count == 0


def test_passive_queue_never_treats_displayed_cancellation_as_a_fill() -> None:
    from neobitcoin_paper.domain import PassiveQueueObservation

    adapter = PaperExecutionAdapter()
    now = utc(2026, 7, 15, 9)
    order = adapter.create_order(
        intent(now, model=ExecutionModel.PASSIVE, quantity="4", limit="100")
    )
    snapshot = book(now)
    cancellation_only = PassiveQueueObservation(
        event_id="queue-1",
        receive_ts=now,
        aggressive_quantity_at_price=Decimal("4"),
        volume_ahead=Decimal("5"),
        cancelled_displayed=Decimal("100"),
    )
    no_fill = adapter.execute_passive(order, snapshot, cancellation_only)
    assert no_fill.status is FillStatus.NO_FILL
    assert no_fill.queue_ahead_remaining == Decimal("1")

    traded_through = PassiveQueueObservation(
        event_id="queue-2",
        receive_ts=now + timedelta(seconds=1),
        aggressive_quantity_at_price=Decimal("7"),
        volume_ahead=Decimal("5"),
    )
    partial = adapter.execute_passive(order, snapshot, traded_through)
    assert partial.status is FillStatus.PARTIAL_FILL
    assert partial.filled_quantity == Decimal("2")
    assert partial.vwap == Decimal("100")


def test_position_stops_trailing_carry_session_close_and_oracle_pnl() -> None:
    adapter = PaperExecutionAdapter()
    opened_at = utc(2026, 7, 15, 9)
    entry_order = adapter.create_order(intent(opened_at))
    entry_result = adapter.execute_aggressive(
        entry_order,
        book(opened_at, bids=(("99", "10"),), asks=(("100", "10"),), event_id="entry"),
    )
    position = adapter.open_position(entry_order, entry_result)
    assert position.entry_price == Decimal("100")

    favorable_book = book(
        opened_at + timedelta(seconds=1),
        bids=(("105", "10"),),
        asks=(("106", "10"),),
        event_id="favorable",
    )
    position = adapter.mark_from_book(position, favorable_book)
    adverse_book = book(
        opened_at + timedelta(seconds=2),
        bids=(("97", "10"),),
        asks=(("98", "10"),),
        event_id="adverse",
    )
    position = adapter.mark_from_book(position, adverse_book)
    assert position.mfe == Decimal("5")
    assert position.mae == Decimal("3")

    stop = adapter.evaluate_exit(
        position,
        adverse_book,
        tick_size=Decimal("1"),
        policy=ExitPolicy(fixed_stop_ticks=Decimal("2")),
    )
    assert stop is not None and stop.reason is ExitReason.FIXED_STOP

    trailing_book = book(
        opened_at + timedelta(seconds=3),
        bids=(("102", "10"),),
        asks=(("103", "10"),),
        event_id="trailing",
    )
    trailing = adapter.evaluate_exit(
        position,
        trailing_book,
        tick_size=Decimal("1"),
        policy=ExitPolicy(trailing_ticks=Decimal("2")),
    )
    assert trailing is not None and trailing.reason is ExitReason.TRAILING

    session = adapter.evaluate_exit(
        position,
        trailing_book,
        tick_size=Decimal("1"),
        policy=ExitPolicy(),
        session_closing=True,
    )
    assert session is not None and session.reason is ExitReason.SESSION_END

    carried = adapter.accrue_carry(
        position,
        as_of=opened_at + timedelta(days=2),
        daily_rate=Decimal("0.001"),
    )
    assert carried.carry_cost == Decimal("0.200")

    exit_book = book(
        opened_at + timedelta(days=2, seconds=1),
        bids=(("110", "10"),),
        asks=(("111", "10"),),
        event_id="exit",
    )
    decision = ExitDecision(
        reason=ExitReason.MANUAL,
        trigger_ts=exit_book.receive_ts,
        trigger_event_id="manual-trigger",
        trigger_price=Decimal("110"),
    )
    closed, exit_result = adapter.close_position(carried, decision, exit_book)
    assert exit_result.status is FillStatus.FULL_FILL
    assert closed.status is PositionStatus.CLOSED
    assert closed.realized_gross_pnl == Decimal("10")
    assert closed.realized_net_pnl == Decimal("9.800")
    assert closed.mfe == Decimal("10")
    assert closed.mae == Decimal("3")

    report = IndependentOracle.validate_closed_position(
        closed,
        marks=(
            (favorable_book.receive_ts, Decimal("105")),
            (adverse_book.receive_ts, Decimal("97")),
        ),
    )
    assert report.passed, report.discrepancies


def test_oracle_has_no_dependency_on_execution_business_logic() -> None:
    source = inspect.getsource(oracle_module)
    assert "from .execution" not in source
    assert "neobitcoin_paper.execution" not in source
