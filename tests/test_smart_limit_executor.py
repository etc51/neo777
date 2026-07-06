"""Tests for SmartLimitExecutor safety and accounting."""

from datetime import datetime, timedelta
from decimal import Decimal

from neo_trader.execution.smart_limit import (
    BrokerOrderAck,
    ExecutionOrderType,
    ExecutionReasonCode,
    ExecutionSide,
    MarketQuote,
    OrderRequest,
    SmartLimitExecutor,
    SmartLimitExecutorConfig,
    TradingStatus,
    TradingStatusSnapshot,
)
from neo_trader.risk.manager import (
    RiskAction,
    RiskConfig,
    RiskDecision,
    RiskManager,
    RiskPosition,
    RiskPositionSide,
    RiskReasonCode,
    RiskState,
)


class FakeGateway:
    def __init__(
        self,
        *,
        is_live: bool = False,
        trading_status: TradingStatus = TradingStatus.TRADING,
    ) -> None:
        self.is_live = is_live
        self.trading_status = trading_status
        self.submitted: list[OrderRequest] = []
        self.canceled: list[str] = []

    def get_trading_status(self, instrument_id: str) -> TradingStatusSnapshot:
        assert instrument_id
        return TradingStatusSnapshot(self.trading_status)

    def submit_order(self, request: OrderRequest) -> BrokerOrderAck:
        self.submitted.append(request)
        return BrokerOrderAck(order_id=f"order-{len(self.submitted)}")

    def cancel_order(self, order_id: str) -> None:
        self.canceled.append(order_id)


class CountingRiskManager(RiskManager):
    def __init__(self) -> None:
        super().__init__(
            RiskConfig(
                max_trades_per_day=10,
                no_new_entries_after=datetime(2026, 7, 6, 18, 30).time(),
                force_flatten_at=datetime(2026, 7, 6, 18, 40).time(),
                max_spread_bps=Decimal("20"),
                max_slippage_bps=Decimal("20"),
                risk_per_trade_fraction=Decimal("0.01"),
                quantity_step=Decimal("1"),
            )
        )
        self.calls = 0

    def evaluate(self, **kwargs: object):  # type: ignore[no-untyped-def]
        self.calls += 1
        return super().evaluate(**kwargs)  # type: ignore[arg-type]


def test_marketable_limit_entry_passes_risk_and_submits_limit_order() -> None:
    gateway = FakeGateway()
    risk_manager = CountingRiskManager()
    executor = _executor(gateway, risk_manager, uuids=["uuid-entry"])

    report = executor.enter_marketable_limit(
        account_ref="acc",
        instrument_id="uid",
        side=ExecutionSide.BUY,
        quote=MarketQuote(best_bid=Decimal("99"), best_ask=Decimal("100")),
        risk_state=_risk_state(),
        current_time=_now(),
        stop_price=Decimal("98"),
    )

    assert report.accepted is True
    assert report.commit_hash
    assert report.reason_codes == (ExecutionReasonCode.SUBMITTED,)
    assert report.order_type is ExecutionOrderType.LIMIT
    assert report.idempotency_key == "uuid-entry"
    assert report.limit_price == Decimal("100.0100")
    assert report.requested_quantity == Decimal("497")
    assert risk_manager.calls == 1
    assert gateway.submitted[0].order_type is ExecutionOrderType.LIMIT
    assert gateway.submitted[0].idempotency_key == "uuid-entry"


def test_partial_fills_update_remaining_and_average_price() -> None:
    executor = _executor(FakeGateway(), CountingRiskManager(), uuids=["uuid-entry"])
    report = executor.enter_marketable_limit(
        account_ref="acc",
        instrument_id="uid",
        side=ExecutionSide.BUY,
        quote=MarketQuote(best_bid=Decimal("99"), best_ask=Decimal("100")),
        risk_state=_risk_state(),
        current_time=_now(),
        stop_price=Decimal("98"),
        quantity_cap=Decimal("10"),
    )

    partial = executor.record_fill(
        order_id=report.order_id or "",
        fill_quantity=Decimal("4"),
        fill_price=Decimal("100.01"),
    )
    filled = executor.record_fill(
        order_id=report.order_id or "",
        fill_quantity=Decimal("6"),
        fill_price=Decimal("100.03"),
    )

    assert partial.reason_codes == (ExecutionReasonCode.PARTIAL_FILL,)
    assert partial.filled_quantity == Decimal("4")
    assert partial.remaining_quantity == Decimal("6")
    assert partial.avg_fill_price == Decimal("100.01")
    assert filled.reason_codes == (ExecutionReasonCode.FILLED,)
    assert filled.filled_quantity == Decimal("10")
    assert filled.remaining_quantity == Decimal("0")
    assert filled.avg_fill_price == Decimal("100.022")


def test_cancel_replace_cancels_original_and_submits_remaining_quantity() -> None:
    gateway = FakeGateway()
    risk_manager = CountingRiskManager()
    executor = _executor(gateway, risk_manager, uuids=["uuid-entry", "uuid-replace"])
    initial = executor.enter_marketable_limit(
        account_ref="acc",
        instrument_id="uid",
        side=ExecutionSide.BUY,
        quote=MarketQuote(best_bid=Decimal("99"), best_ask=Decimal("100")),
        risk_state=_risk_state(),
        current_time=_now(),
        stop_price=Decimal("98"),
        quantity_cap=Decimal("10"),
    )
    executor.record_fill(
        order_id=initial.order_id or "",
        fill_quantity=Decimal("3"),
        fill_price=Decimal("100.01"),
    )

    replacement = executor.cancel_replace(
        order_id=initial.order_id or "",
        quote=MarketQuote(best_bid=Decimal("100"), best_ask=Decimal("101")),
        risk_state=_risk_state(),
        current_time=_now(),
        stop_price=Decimal("98"),
    )

    assert gateway.canceled == ["order-1"]
    assert replacement.accepted is True
    assert replacement.reason_codes == (ExecutionReasonCode.CANCEL_REPLACE_SUBMITTED,)
    assert replacement.order_id == "order-2"
    assert replacement.idempotency_key == "uuid-replace"
    assert replacement.requested_quantity == Decimal("7")
    assert risk_manager.calls == 2


def test_market_order_is_forbidden_outside_emergency_exit() -> None:
    risk_manager = CountingRiskManager()
    risk_decision = risk_manager.evaluate(
        desired_action=RiskAction.EXIT,
        state=_risk_state(),
        current_time=_now(),
    )
    executor = _executor(FakeGateway(), risk_manager, uuids=["uuid-market"])

    report = executor._submit_order_test_only(
        account_ref="acc",
        instrument_id="uid",
        side=ExecutionSide.SELL,
        order_type=ExecutionOrderType.MARKET,
        quantity=Decimal("1"),
        price=None,
        risk_decision=RiskDecision(
            action=RiskAction.EXIT,
            approved=True,
            reason_codes=risk_decision.reason_codes,
            position_size=Decimal("1"),
        ),
    )

    assert report.accepted is False
    assert report.reason_codes == (ExecutionReasonCode.MARKET_ORDER_FORBIDDEN,)


def test_quantity_exceeding_risk_approval_is_rejected() -> None:
    gateway = FakeGateway()
    executor = _executor(gateway, CountingRiskManager(), uuids=["uuid-entry"])

    report = executor._submit_order_test_only(
        account_ref="acc",
        instrument_id="uid",
        side=ExecutionSide.BUY,
        order_type=ExecutionOrderType.LIMIT,
        quantity=Decimal("999999"),
        price=Decimal("100"),
        risk_decision=RiskDecision(
            action=RiskAction.BUY,
            approved=True,
            reason_codes=(RiskReasonCode.APPROVED,),
            position_size=Decimal("1"),
        ),
    )

    assert report.accepted is False
    assert report.reason_codes == (ExecutionReasonCode.QUANTITY_EXCEEDS_RISK_APPROVAL,)
    assert gateway.submitted == []


def test_emergency_exit_allows_market_order_after_risk_and_status_checks() -> None:
    gateway = FakeGateway()
    risk_manager = CountingRiskManager()
    executor = _executor(gateway, risk_manager, uuids=["uuid-exit"])

    report = executor.emergency_exit(
        account_ref="acc",
        instrument_id="uid",
        side=ExecutionSide.SELL,
        quantity=Decimal("5"),
        risk_state=RiskState(
            account_equity=Decimal("100000"),
            daily_realized_pnl=Decimal("0"),
            trades_today=0,
            market_data_last_seen_at=_now() - timedelta(seconds=1),
            spread_bps=Decimal("2"),
            expected_slippage_bps=Decimal("5"),
            position=RiskPosition(RiskPositionSide.LONG, Decimal("5")),
        ),
        current_time=_now(),
    )

    assert report.accepted is True
    assert report.order_type is ExecutionOrderType.MARKET
    assert gateway.submitted[0].order_type is ExecutionOrderType.MARKET
    assert gateway.submitted[0].quantity == Decimal("5")
    assert risk_manager.calls == 1


def test_emergency_exit_caps_quantity_to_current_position_size() -> None:
    gateway = FakeGateway()
    risk_manager = CountingRiskManager()
    executor = _executor(gateway, risk_manager, uuids=["uuid-exit"])
    risk_state = RiskState(
        account_equity=Decimal("100000"),
        daily_realized_pnl=Decimal("0"),
        trades_today=0,
        market_data_last_seen_at=_now() - timedelta(seconds=1),
        spread_bps=Decimal("2"),
        expected_slippage_bps=Decimal("5"),
        position=RiskPosition(RiskPositionSide.LONG, Decimal("3")),
    )

    report = executor.emergency_exit(
        account_ref="acc",
        instrument_id="uid",
        side=ExecutionSide.SELL,
        quantity=Decimal("999999"),
        risk_state=risk_state,
        current_time=_now(),
    )

    assert report.accepted is True
    assert report.requested_quantity == Decimal("3")
    assert gateway.submitted[0].quantity == Decimal("3")


def test_trading_status_blocks_order_before_submit() -> None:
    gateway = FakeGateway(trading_status=TradingStatus.HALTED)
    executor = _executor(gateway, CountingRiskManager(), uuids=["uuid-entry"])

    report = executor.enter_marketable_limit(
        account_ref="acc",
        instrument_id="uid",
        side=ExecutionSide.BUY,
        quote=MarketQuote(best_bid=Decimal("99"), best_ask=Decimal("100")),
        risk_state=_risk_state(),
        current_time=_now(),
        stop_price=Decimal("98"),
    )

    assert report.accepted is False
    assert report.reason_codes == (ExecutionReasonCode.TRADING_STATUS_BLOCKED,)
    assert gateway.submitted == []


def test_risk_rejection_blocks_order_before_submit() -> None:
    gateway = FakeGateway()
    executor = _executor(gateway, CountingRiskManager(), uuids=["uuid-entry"])
    stale_state = RiskState(
        account_equity=Decimal("100000"),
        daily_realized_pnl=Decimal("0"),
        trades_today=0,
        market_data_last_seen_at=_now() - timedelta(seconds=30),
        spread_bps=Decimal("2"),
    )

    report = executor.enter_marketable_limit(
        account_ref="acc",
        instrument_id="uid",
        side=ExecutionSide.BUY,
        quote=MarketQuote(best_bid=Decimal("99"), best_ask=Decimal("100")),
        risk_state=stale_state,
        current_time=_now(),
        stop_price=Decimal("98"),
    )

    assert report.accepted is False
    assert report.reason_codes == (ExecutionReasonCode.RISK_REJECTED,)
    assert gateway.submitted == []


def test_live_gateway_is_blocked_without_live_flag() -> None:
    gateway = FakeGateway(is_live=True)
    executor = _executor(
        gateway,
        CountingRiskManager(),
        uuids=["uuid-entry"],
        live_trading_enabled=False,
    )

    report = executor.enter_marketable_limit(
        account_ref="acc",
        instrument_id="uid",
        side=ExecutionSide.BUY,
        quote=MarketQuote(best_bid=Decimal("99"), best_ask=Decimal("100")),
        risk_state=_risk_state(),
        current_time=_now(),
        stop_price=Decimal("98"),
    )

    assert report.accepted is False
    assert report.reason_codes == (ExecutionReasonCode.LIVE_TRADING_DISABLED,)
    assert gateway.submitted == []


def test_live_gateway_can_submit_only_when_live_flag_is_true() -> None:
    gateway = FakeGateway(is_live=True)
    executor = _executor(
        gateway,
        CountingRiskManager(),
        uuids=["uuid-entry"],
        live_trading_enabled=True,
    )

    report = executor.enter_marketable_limit(
        account_ref="acc",
        instrument_id="uid",
        side=ExecutionSide.BUY,
        quote=MarketQuote(best_bid=Decimal("99"), best_ask=Decimal("100")),
        risk_state=_risk_state(),
        current_time=_now(),
        stop_price=Decimal("98"),
    )

    assert report.accepted is True
    assert gateway.submitted


def _executor(
    gateway: FakeGateway,
    risk_manager: RiskManager,
    *,
    uuids: list[str],
    live_trading_enabled: bool = False,
) -> SmartLimitExecutor:
    uuid_iter = iter(uuids)
    return SmartLimitExecutor(
        gateway=gateway,
        risk_manager=risk_manager,
        config=SmartLimitExecutorConfig(
            marketable_limit_offset_bps=Decimal("1"),
            live_trading_enabled=live_trading_enabled,
        ),
        uuid_factory=lambda: next(uuid_iter),
    )


def _risk_state() -> RiskState:
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
