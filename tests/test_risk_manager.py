"""Tests for risk management gates and sizing."""

from dataclasses import replace
from datetime import datetime, time, timedelta
from decimal import Decimal

from neo_trader.risk.manager import (
    RiskAction,
    RiskConfig,
    RiskManager,
    RiskPosition,
    RiskPositionSide,
    RiskReasonCode,
    RiskState,
)


def test_calculate_position_size_uses_risk_and_notional_limits() -> None:
    manager = RiskManager(_config())

    size = manager.calculate_position_size(
        account_equity=Decimal("100000"),
        entry_price=Decimal("100"),
        stop_price=Decimal("98"),
    )

    assert size == Decimal("500")


def test_calculate_position_size_applies_notional_cap() -> None:
    config = replace(_config(), max_position_notional_fraction=Decimal("0.10"))
    manager = RiskManager(config)

    size = manager.calculate_position_size(
        account_equity=Decimal("100000"),
        entry_price=Decimal("100"),
        stop_price=Decimal("98"),
    )

    assert size == Decimal("100")


def test_approved_entry_returns_position_size() -> None:
    manager = RiskManager(_config())

    decision = manager.evaluate(
        desired_action=RiskAction.BUY,
        state=_state(),
        current_time=_now(),
        entry_price=Decimal("100"),
        stop_price=Decimal("98"),
    )

    assert decision.approved is True
    assert decision.action is RiskAction.BUY
    assert decision.reason_codes == (RiskReasonCode.APPROVED,)
    assert decision.position_size == Decimal("500")


def test_daily_loss_limit_forces_flatten_when_position_is_open() -> None:
    manager = RiskManager(_config())
    state = replace(
        _state(),
        daily_realized_pnl=Decimal("-1000"),
        position=RiskPosition(RiskPositionSide.LONG, Decimal("12")),
    )

    decision = manager.evaluate(
        desired_action=RiskAction.BUY,
        state=state,
        current_time=_now(),
        entry_price=Decimal("100"),
        stop_price=Decimal("98"),
    )

    assert decision.action is RiskAction.EXIT
    assert decision.approved is True
    assert decision.reason_codes == (
        RiskReasonCode.DAILY_LOSS_LIMIT,
        RiskReasonCode.POSITION_FLATTEN_REQUIRED,
    )
    assert decision.position_size == Decimal("12")


def test_daily_loss_limit_blocks_new_entry_when_flat() -> None:
    manager = RiskManager(_config())
    state = replace(_state(), daily_realized_pnl=Decimal("-1000"))

    decision = manager.evaluate(
        desired_action=RiskAction.SELL,
        state=state,
        current_time=_now(),
        entry_price=Decimal("100"),
        stop_price=Decimal("102"),
    )

    assert decision.action is RiskAction.HOLD
    assert decision.approved is False
    assert decision.reason_codes == (RiskReasonCode.DAILY_LOSS_LIMIT,)


def test_trade_count_limit_blocks_new_entry() -> None:
    manager = RiskManager(_config())
    state = replace(_state(), trades_today=3)

    decision = manager.evaluate(
        desired_action=RiskAction.BUY,
        state=state,
        current_time=_now(),
        entry_price=Decimal("100"),
        stop_price=Decimal("98"),
    )

    assert decision.approved is False
    assert decision.action is RiskAction.HOLD
    assert decision.reason_codes == (RiskReasonCode.TRADE_COUNT_LIMIT,)


def test_no_new_entries_after_blocks_entry() -> None:
    manager = RiskManager(_config())

    decision = manager.evaluate(
        desired_action=RiskAction.BUY,
        state=_state(),
        current_time=datetime(2026, 7, 6, 18, 31),
        entry_price=Decimal("100"),
        stop_price=Decimal("98"),
    )

    assert decision.approved is False
    assert decision.reason_codes == (RiskReasonCode.NO_NEW_ENTRIES_TIME,)


def test_force_flatten_at_returns_exit_for_open_position() -> None:
    manager = RiskManager(_config())
    state = replace(
        _state(),
        position=RiskPosition(RiskPositionSide.SHORT, Decimal("7")),
    )

    decision = manager.evaluate(
        desired_action=RiskAction.HOLD,
        state=state,
        current_time=datetime(2026, 7, 6, 18, 41),
    )

    assert decision.approved is True
    assert decision.action is RiskAction.EXIT
    assert decision.reason_codes == (
        RiskReasonCode.FORCE_FLATTEN_TIME,
        RiskReasonCode.POSITION_FLATTEN_REQUIRED,
    )
    assert decision.position_size == Decimal("7")


def test_stale_market_data_blocks_new_entry() -> None:
    manager = RiskManager(_config())
    state = replace(
        _state(),
        market_data_last_seen_at=_now() - timedelta(seconds=6),
    )

    decision = manager.evaluate(
        desired_action=RiskAction.BUY,
        state=state,
        current_time=_now(),
        entry_price=Decimal("100"),
        stop_price=Decimal("98"),
    )

    assert decision.approved is False
    assert decision.reason_codes == (RiskReasonCode.STALE_MARKET_DATA,)


def test_spread_limit_blocks_new_entry() -> None:
    manager = RiskManager(_config())
    state = replace(_state(), spread_bps=Decimal("16"))

    decision = manager.evaluate(
        desired_action=RiskAction.BUY,
        state=state,
        current_time=_now(),
        entry_price=Decimal("100"),
        stop_price=Decimal("98"),
    )

    assert decision.approved is False
    assert decision.reason_codes == (RiskReasonCode.SPREAD_LIMIT,)


def test_slippage_limit_blocks_new_entry() -> None:
    manager = RiskManager(_config())
    state = replace(_state(), expected_slippage_bps=Decimal("21"))

    decision = manager.evaluate(
        desired_action=RiskAction.BUY,
        state=state,
        current_time=_now(),
        entry_price=Decimal("100"),
        stop_price=Decimal("98"),
    )

    assert decision.approved is False
    assert decision.reason_codes == (RiskReasonCode.SLIPPAGE_LIMIT,)


def test_long_position_blocks_additional_buy_entry() -> None:
    decision = RiskManager(_config()).evaluate(
        desired_action=RiskAction.BUY,
        state=replace(
            _state(),
            position=RiskPosition(RiskPositionSide.LONG, Decimal("5")),
        ),
        current_time=_now(),
        entry_price=Decimal("100"),
        stop_price=Decimal("98"),
    )

    assert decision.action is RiskAction.HOLD
    assert decision.approved is False
    assert decision.reason_codes == (RiskReasonCode.POSITION_ALREADY_OPEN,)


def test_long_position_blocks_sell_entry() -> None:
    decision = RiskManager(_config()).evaluate(
        desired_action=RiskAction.SELL,
        state=replace(
            _state(),
            position=RiskPosition(RiskPositionSide.LONG, Decimal("5")),
        ),
        current_time=_now(),
        entry_price=Decimal("100"),
        stop_price=Decimal("102"),
    )

    assert decision.action is RiskAction.HOLD
    assert decision.approved is False
    assert decision.reason_codes == (RiskReasonCode.POSITION_ALREADY_OPEN,)


def test_short_position_blocks_additional_sell_entry() -> None:
    decision = RiskManager(_config()).evaluate(
        desired_action=RiskAction.SELL,
        state=replace(
            _state(),
            position=RiskPosition(RiskPositionSide.SHORT, Decimal("5")),
        ),
        current_time=_now(),
        entry_price=Decimal("100"),
        stop_price=Decimal("102"),
    )

    assert decision.action is RiskAction.HOLD
    assert decision.approved is False
    assert decision.reason_codes == (RiskReasonCode.POSITION_ALREADY_OPEN,)


def test_short_position_blocks_buy_entry() -> None:
    decision = RiskManager(_config()).evaluate(
        desired_action=RiskAction.BUY,
        state=replace(
            _state(),
            position=RiskPosition(RiskPositionSide.SHORT, Decimal("5")),
        ),
        current_time=_now(),
        entry_price=Decimal("100"),
        stop_price=Decimal("98"),
    )

    assert decision.action is RiskAction.HOLD
    assert decision.approved is False
    assert decision.reason_codes == (RiskReasonCode.POSITION_ALREADY_OPEN,)


def test_kill_switch_blocks_when_flat_and_forces_exit_when_open() -> None:
    manager = RiskManager(replace(_config(), kill_switch=True))

    flat_decision = manager.evaluate(
        desired_action=RiskAction.BUY,
        state=_state(),
        current_time=_now(),
        entry_price=Decimal("100"),
        stop_price=Decimal("98"),
    )
    open_decision = manager.evaluate(
        desired_action=RiskAction.HOLD,
        state=replace(
            _state(),
            position=RiskPosition(RiskPositionSide.LONG, Decimal("5")),
        ),
        current_time=_now(),
    )

    assert flat_decision.action is RiskAction.HOLD
    assert flat_decision.approved is False
    assert flat_decision.reason_codes == (RiskReasonCode.KILL_SWITCH,)
    assert open_decision.action is RiskAction.EXIT
    assert open_decision.reason_codes == (
        RiskReasonCode.KILL_SWITCH,
        RiskReasonCode.POSITION_FLATTEN_REQUIRED,
    )


def test_exit_signal_is_allowed_even_when_entry_count_limit_is_reached() -> None:
    manager = RiskManager(_config())
    state = replace(
        _state(),
        trades_today=3,
        position=RiskPosition(RiskPositionSide.LONG, Decimal("3")),
    )

    decision = manager.evaluate(
        desired_action=RiskAction.EXIT,
        state=state,
        current_time=_now(),
    )

    assert decision.approved is True
    assert decision.action is RiskAction.EXIT
    assert decision.reason_codes == (RiskReasonCode.EXIT_ALLOWED,)
    assert decision.position_size == Decimal("3")


def _config() -> RiskConfig:
    return RiskConfig(
        max_daily_loss=Decimal("1000"),
        max_trades_per_day=3,
        no_new_entries_after=time(18, 30),
        force_flatten_at=time(18, 40),
        max_market_data_stale_seconds=Decimal("5"),
        max_spread_bps=Decimal("15"),
        max_slippage_bps=Decimal("20"),
        risk_per_trade_fraction=Decimal("0.01"),
        max_position_notional_fraction=Decimal("1"),
        quantity_step=Decimal("1"),
    )


def _state() -> RiskState:
    return RiskState(
        account_equity=Decimal("100000"),
        daily_realized_pnl=Decimal("0"),
        trades_today=0,
        market_data_last_seen_at=_now() - timedelta(seconds=1),
        spread_bps=Decimal("2"),
        expected_slippage_bps=Decimal("5"),
    )


def _now() -> datetime:
    return datetime(2026, 7, 6, 12, 0)
