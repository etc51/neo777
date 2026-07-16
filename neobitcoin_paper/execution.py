"""Deterministic paper-only execution, position and exit simulation."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from .domain import (
    DataQuality,
    DomainValidationError,
    ExecutionModel,
    ExecutionResult,
    ExitDecision,
    ExitPolicy,
    ExitReason,
    FillStatus,
    LookAheadError,
    OrderBook,
    OrderStatus,
    PaperFill,
    PaperIntent,
    PaperOrder,
    PaperPosition,
    PassiveQueueObservation,
    PositionStatus,
    Side,
    as_utc,
    decimal_value,
    deterministic_id,
)

_MOSCOW = ZoneInfo("Europe/Moscow")


class PaperExecutionAdapter:
    """The sole execution interface: it creates virtual records only."""

    def __init__(self, decision_latency: timedelta = timedelta(0)) -> None:
        if decision_latency < timedelta(0):
            raise DomainValidationError("decision latency must be non-negative")
        self._decision_latency = decision_latency

    @property
    def decision_latency(self) -> timedelta:
        return self._decision_latency

    def create_order(
        self,
        intent: PaperIntent,
        *,
        created_ts: datetime | None = None,
    ) -> PaperOrder:
        created = as_utc(created_ts or intent.decision_ts, "created_ts")
        if created < intent.decision_ts:
            raise LookAheadError("paper order cannot be created before its intent")
        # Approved next-book strategies measure latency but always attempt the
        # first strictly subsequent received book.  Other models retain the
        # configurable simulated decision delay.
        latency = (
            timedelta(0)
            if intent.metadata.get("max_entry_wait_seconds") is not None
            else self._decision_latency
        )
        eligible = max(intent.eligible_ts, created + latency)
        order_id = deterministic_id(
            "order",
            intent.intent_id,
            created,
            eligible,
            intent.execution_model,
            intent.quantity,
            intent.limit_price,
        )
        return PaperOrder(
            order_id=order_id,
            intent_id=intent.intent_id,
            strategy_id=intent.strategy_id,
            strategy_version=intent.strategy_version,
            instrument_uid=intent.instrument_uid,
            side=intent.side,
            quantity=intent.quantity,
            execution_model=intent.execution_model,
            created_ts=created,
            eligible_ts=eligible,
            limit_price=intent.limit_price,
        )

    def execute_aggressive(self, order: PaperOrder, book: OrderBook) -> ExecutionResult:
        if order.execution_model is not ExecutionModel.AGGRESSIVE:
            raise DomainValidationError("aggressive simulation requires an AGGRESSIVE paper order")
        self._validate_causal_book(order, book)
        if not book.is_executable:
            return self._empty_result(
                order,
                source_event_id=book.event_id,
                reason=(
                    f"NON_EXECUTABLE:{book.data_quality.value}:"
                    f"{book.trading_status or 'UNKNOWN'}"
                ),
            )

        levels = book.asks if order.side is Side.BUY else book.bids
        top_price = levels[0].price if levels else None
        if not levels:
            return self._empty_result(
                order,
                source_event_id=book.event_id,
                reason="NO_LIQUIDITY",
                top_price=None,
            )
        assert top_price is not None

        remaining = order.quantity
        fills: list[PaperFill] = []
        for level_index, level in enumerate(levels, start=1):
            if remaining <= 0:
                break
            if not self._price_allowed(order.side, level.price, order.limit_price):
                break
            quantity = min(remaining, level.quantity)
            fill_id = deterministic_id(
                "fill",
                order.order_id,
                book.event_id,
                level_index,
                level.price,
                quantity,
            )
            fills.append(
                PaperFill(
                    fill_id=fill_id,
                    order_id=order.order_id,
                    source_event_id=book.event_id,
                    fill_ts=book.receive_ts,
                    price=level.price,
                    quantity=quantity,
                    level_index=level_index,
                )
            )
            remaining -= quantity

        if not fills:
            return self._empty_result(
                order,
                source_event_id=book.event_id,
                reason="LIMIT_NOT_REACHED",
                top_price=top_price,
            )
        filled = order.quantity - remaining
        status = FillStatus.FULL_FILL if remaining == 0 else FillStatus.PARTIAL_FILL
        vwap = sum((item.price * item.quantity for item in fills), Decimal("0")) / filled
        if order.side is Side.BUY:
            slippage = sum(
                ((item.price - top_price) * item.quantity for item in fills),
                Decimal("0"),
            )
        else:
            slippage = sum(
                ((top_price - item.price) * item.quantity for item in fills),
                Decimal("0"),
            )
        return ExecutionResult(
            order_id=order.order_id,
            status=status,
            reason="FULL_DEPTH" if status is FillStatus.FULL_FILL else "INSUFFICIENT_DEPTH",
            requested_quantity=order.quantity,
            filled_quantity=filled,
            unfilled_quantity=remaining,
            vwap=vwap,
            fills=tuple(fills),
            source_event_id=book.event_id,
            top_price=top_price,
            slippage_cost=slippage,
        )

    def execute_passive(
        self,
        order: PaperOrder,
        book: OrderBook,
        observation: PassiveQueueObservation,
    ) -> ExecutionResult:
        """Conservatively model one passive queue observation.

        Only aggressive traded volume can progress the simulated order.  A
        displayed cancellation is retained for audit but never converted into
        our fill.  Added volume is conservatively treated as joining ahead.
        """

        if order.execution_model is not ExecutionModel.PASSIVE:
            raise DomainValidationError("passive simulation requires a PASSIVE paper order")
        self._validate_causal_book(order, book)
        if observation.receive_ts < order.eligible_ts:
            raise LookAheadError("queue observation predates order eligibility")
        if order.limit_price is None:  # Defensive; PaperOrder already enforces this.
            raise DomainValidationError("passive order requires limit_price")
        if (
            order.side is Side.BUY
            and book.best_ask is not None
            and order.limit_price >= book.best_ask
        ):
            raise DomainValidationError("marketable BUY belongs to the aggressive model")
        if (
            order.side is Side.SELL
            and book.best_bid is not None
            and order.limit_price <= book.best_bid
        ):
            raise DomainValidationError("marketable SELL belongs to the aggressive model")
        if not book.is_executable or observation.data_quality is not DataQuality.GOOD:
            return self._empty_result(
                order,
                source_event_id=observation.event_id,
                reason="NON_EXECUTABLE_QUEUE_OBSERVATION",
                queue_ahead_remaining=observation.volume_ahead + observation.added_ahead,
            )

        # Conservative queue policy: displayed cancellation gives no queue
        # credit because aggregate depth changes cannot identify our place.
        effective_ahead = observation.volume_ahead + observation.added_ahead
        volume_reaching_order = max(
            observation.aggressive_quantity_at_price - effective_ahead,
            Decimal("0"),
        )
        filled = min(order.quantity, volume_reaching_order)
        remaining = order.quantity - filled
        queue_remaining = max(
            effective_ahead - observation.aggressive_quantity_at_price,
            Decimal("0"),
        )

        if filled == 0:
            if observation.cancelled:
                reason = "PAPER_ORDER_CANCELLED"
            elif observation.timed_out:
                reason = "PASSIVE_TIMEOUT"
            else:
                reason = "QUEUE_NOT_REACHED"
            return self._empty_result(
                order,
                source_event_id=observation.event_id,
                reason=reason,
                top_price=order.limit_price,
                queue_ahead_remaining=queue_remaining,
            )

        fill = PaperFill(
            fill_id=deterministic_id(
                "fill",
                order.order_id,
                observation.event_id,
                1,
                order.limit_price,
                filled,
            ),
            order_id=order.order_id,
            source_event_id=observation.event_id,
            fill_ts=observation.receive_ts,
            price=order.limit_price,
            quantity=filled,
            level_index=1,
        )
        status = FillStatus.FULL_FILL if remaining == 0 else FillStatus.PARTIAL_FILL
        reason = "QUEUE_FILLED" if status is FillStatus.FULL_FILL else "PARTIAL_QUEUE_FILL"
        return ExecutionResult(
            order_id=order.order_id,
            status=status,
            reason=reason,
            requested_quantity=order.quantity,
            filled_quantity=filled,
            unfilled_quantity=remaining,
            vwap=order.limit_price,
            fills=(fill,),
            source_event_id=observation.event_id,
            top_price=order.limit_price,
            slippage_cost=Decimal("0"),
            queue_ahead_remaining=queue_remaining,
        )

    def open_position(self, order: PaperOrder, execution: ExecutionResult) -> PaperPosition:
        if execution.order_id != order.order_id:
            raise DomainValidationError("execution belongs to a different paper order")
        if execution.filled_quantity <= 0 or execution.vwap is None:
            raise DomainValidationError("an unfilled paper order cannot open a position")
        opened_ts = max(fill.fill_ts for fill in execution.fills)
        position_id = deterministic_id(
            "position",
            order.strategy_id,
            order.strategy_version,
            order.order_id,
            execution.source_event_id,
        )
        return PaperPosition(
            position_id=position_id,
            strategy_id=order.strategy_id,
            strategy_version=order.strategy_version,
            instrument_uid=order.instrument_uid,
            entry_side=order.side,
            quantity=execution.filled_quantity,
            entry_price=execution.vwap,
            opened_ts=opened_ts,
            entry_event_id=execution.source_event_id,
        )

    @staticmethod
    def mark_position(
        position: PaperPosition,
        mark_price: Decimal,
        mark_ts: datetime,
        mark_event_id: str | None = None,
    ) -> PaperPosition:
        if position.status is not PositionStatus.OPEN:
            raise DomainValidationError("only an open paper position can be marked")
        mark_ts = as_utc(mark_ts, "mark_ts")
        if mark_ts < position.opened_ts:
            raise LookAheadError("position mark cannot precede entry")
        mark = decimal_value(mark_price, "mark_price")
        if mark <= 0:
            raise DomainValidationError("mark price must be positive")
        peak = max(position.peak_price or position.entry_price, mark)
        trough = min(position.trough_price or position.entry_price, mark)
        if position.is_long:
            favorable = max(peak - position.entry_price, Decimal("0")) * position.quantity
            adverse = max(position.entry_price - trough, Decimal("0")) * position.quantity
        else:
            favorable = max(position.entry_price - trough, Decimal("0")) * position.quantity
            adverse = max(peak - position.entry_price, Decimal("0")) * position.quantity
        new_mfe = favorable > position.mfe
        new_mae = adverse > position.mae
        event_id = mark_event_id.strip() if mark_event_id is not None else None
        if event_id == "":
            raise DomainValidationError("mark_event_id must not be blank")
        return replace(
            position,
            peak_price=peak,
            trough_price=trough,
            mfe=max(position.mfe, favorable),
            mae=max(position.mae, adverse),
            mfe_ts=mark_ts if new_mfe and event_id is not None else position.mfe_ts,
            mae_ts=mark_ts if new_mae and event_id is not None else position.mae_ts,
            mfe_event_id=(
                event_id if new_mfe and event_id is not None else position.mfe_event_id
            ),
            mae_event_id=(
                event_id if new_mae and event_id is not None else position.mae_event_id
            ),
        )

    def mark_from_book(self, position: PaperPosition, book: OrderBook) -> PaperPosition:
        self._validate_position_book(position, book)
        price = book.best_bid if position.is_long else book.best_ask
        if price is None:
            raise DomainValidationError("book has no executable exit side")
        return self.mark_position(position, price, book.receive_ts, book.event_id)

    def evaluate_exit(
        self,
        position: PaperPosition,
        book: OrderBook,
        *,
        tick_size: Decimal,
        policy: ExitPolicy,
        session_closing: bool = False,
    ) -> ExitDecision | None:
        self._validate_position_book(position, book)
        tick = decimal_value(tick_size, "tick_size")
        if tick <= 0:
            raise DomainValidationError("tick_size must be positive")
        current = book.best_bid if position.is_long else book.best_ask
        if current is None:
            return None
        tracked = self.mark_position(position, current, book.receive_ts, book.event_id)

        stop = policy.fixed_stop_ticks
        if stop is not None:
            stop_price = (
                position.entry_price - stop * tick
                if position.is_long
                else position.entry_price + stop * tick
            )
            if (position.is_long and current <= stop_price) or (
                not position.is_long and current >= stop_price
            ):
                return self._exit_decision(ExitReason.FIXED_STOP, book, current)

        target = policy.take_profit_ticks
        if target is not None:
            target_price = (
                position.entry_price + target * tick
                if position.is_long
                else position.entry_price - target * tick
            )
            if (position.is_long and current >= target_price) or (
                not position.is_long and current <= target_price
            ):
                return self._exit_decision(ExitReason.TAKE_PROFIT, book, current)

        trailing = policy.trailing_ticks
        if trailing is not None:
            if position.is_long and tracked.peak_price is not None:
                triggered = tracked.peak_price > position.entry_price and current <= (
                    tracked.peak_price - trailing * tick
                )
            elif tracked.trough_price is not None:
                triggered = tracked.trough_price < position.entry_price and current >= (
                    tracked.trough_price + trailing * tick
                )
            else:
                triggered = False
            if triggered:
                return self._exit_decision(ExitReason.TRAILING, book, current)

        breakeven = policy.breakeven_trigger_ticks
        if breakeven is not None:
            if position.is_long:
                armed = (tracked.peak_price or position.entry_price) >= (
                    position.entry_price + breakeven * tick
                )
                triggered = armed and current <= position.entry_price
            else:
                armed = (tracked.trough_price or position.entry_price) <= (
                    position.entry_price - breakeven * tick
                )
                triggered = armed and current >= position.entry_price
            if triggered:
                return self._exit_decision(ExitReason.BREAKEVEN, book, current)

        if policy.time_exit_seconds is not None:
            elapsed = book.receive_ts - position.opened_ts
            if elapsed >= timedelta(seconds=policy.time_exit_seconds):
                return self._exit_decision(ExitReason.TIME_EXIT, book, current)

        if session_closing and policy.close_at_session_end:
            return self._exit_decision(ExitReason.SESSION_END, book, current)
        return None

    @staticmethod
    def accrue_carry(
        position: PaperPosition,
        *,
        as_of: datetime,
        daily_rate: Decimal,
    ) -> PaperPosition:
        """Accrue versioned carry for Moscow calendar boundaries crossed."""

        if position.status is not PositionStatus.OPEN:
            return position
        as_of = as_utc(as_of, "as_of")
        through = position.carry_accrued_through or position.opened_ts
        if as_of < through:
            raise LookAheadError("carry cannot accrue backwards")
        rate = decimal_value(daily_rate, "daily_rate")
        if rate < 0:
            raise DomainValidationError("daily carry rate must be non-negative")
        days = (as_of.astimezone(_MOSCOW).date() - through.astimezone(_MOSCOW).date()).days
        if days <= 0:
            return position
        charge = position.entry_price * position.quantity * rate * days
        return replace(
            position,
            carry_cost=position.carry_cost + charge,
            carry_accrued_through=as_of,
        )

    def close_position(
        self,
        position: PaperPosition,
        decision: ExitDecision,
        book: OrderBook,
        *,
        additional_cost: Decimal = Decimal("0"),
    ) -> tuple[PaperPosition, ExecutionResult]:
        """Apply an exit to the first causal executable book after its trigger."""

        self._validate_position_book(position, book)
        if book.receive_ts < decision.trigger_ts:
            raise LookAheadError("exit fill book predates its trigger")
        exit_side = position.entry_side.opposite
        order = PaperOrder(
            order_id=deterministic_id(
                "exit-order",
                position.position_id,
                decision.trigger_event_id,
                book.event_id,
                exit_side,
            ),
            intent_id=deterministic_id(
                "exit-intent",
                position.position_id,
                decision.trigger_event_id,
            ),
            strategy_id=position.strategy_id,
            strategy_version=position.strategy_version,
            instrument_uid=position.instrument_uid,
            side=exit_side,
            quantity=position.quantity,
            execution_model=ExecutionModel.AGGRESSIVE,
            created_ts=decision.trigger_ts,
            eligible_ts=decision.trigger_ts + self._decision_latency,
        )
        execution = self.execute_aggressive(order, book)
        if execution.filled_quantity == 0 or execution.vwap is None:
            return position, execution

        extra = decimal_value(additional_cost, "additional_cost")
        if extra < 0:
            raise DomainValidationError("additional cost must be non-negative")
        closed_quantity = execution.filled_quantity
        direction = Decimal("1") if position.is_long else Decimal("-1")
        gross = (execution.vwap - position.entry_price) * closed_quantity * direction
        carry_allocated = position.carry_cost * closed_quantity / position.quantity
        net = gross - carry_allocated - execution.slippage_cost - extra
        total_gross = position.realized_gross_pnl + gross
        total_net = position.realized_net_pnl + net
        last_fill_ts = max(fill.fill_ts for fill in execution.fills)
        tracked = self.mark_position(
            position,
            execution.vwap,
            last_fill_ts,
            execution.source_event_id,
        )

        if closed_quantity == position.quantity:
            closed = replace(
                tracked,
                status=PositionStatus.CLOSED,
                closed_ts=last_fill_ts,
                exit_price=execution.vwap,
                exit_event_id=execution.source_event_id,
                exit_reason=decision.reason,
                realized_gross_pnl=total_gross,
                realized_net_pnl=total_net,
            )
            return closed, execution

        remaining = position.quantity - closed_quantity
        partially_closed = replace(
            tracked,
            quantity=remaining,
            carry_cost=position.carry_cost - carry_allocated,
            realized_gross_pnl=total_gross,
            realized_net_pnl=total_net,
        )
        return partially_closed, execution

    @staticmethod
    def unrealized_pnl(position: PaperPosition, price: Decimal) -> Decimal:
        price = decimal_value(price, "price")
        direction = Decimal("1") if position.is_long else Decimal("-1")
        return (price - position.entry_price) * position.quantity * direction

    @staticmethod
    def _exit_decision(reason: ExitReason, book: OrderBook, price: Decimal) -> ExitDecision:
        return ExitDecision(
            reason=reason,
            trigger_ts=book.receive_ts,
            trigger_event_id=book.event_id,
            trigger_price=price,
        )

    @staticmethod
    def _price_allowed(side: Side, price: Decimal, limit_price: Decimal | None) -> bool:
        if limit_price is None:
            return True
        return price <= limit_price if side is Side.BUY else price >= limit_price

    @staticmethod
    def _validate_causal_book(order: PaperOrder, book: OrderBook) -> None:
        if order.instrument_uid != book.instrument_uid:
            raise DomainValidationError("paper order and book instrument UID differ")
        if book.receive_ts < order.eligible_ts:
            raise LookAheadError("book was received before the paper order became eligible")

    @staticmethod
    def _validate_position_book(position: PaperPosition, book: OrderBook) -> None:
        if position.status is not PositionStatus.OPEN:
            raise DomainValidationError("position is not open")
        if position.instrument_uid != book.instrument_uid:
            raise DomainValidationError("position and book instrument UID differ")
        if book.receive_ts < position.opened_ts:
            raise LookAheadError("position book predates entry")

    @staticmethod
    def _empty_result(
        order: PaperOrder,
        *,
        source_event_id: str,
        reason: str,
        top_price: Decimal | None = None,
        queue_ahead_remaining: Decimal | None = None,
    ) -> ExecutionResult:
        return ExecutionResult(
            order_id=order.order_id,
            status=FillStatus.NO_FILL,
            reason=reason,
            requested_quantity=order.quantity,
            filled_quantity=Decimal("0"),
            unfilled_quantity=order.quantity,
            vwap=None,
            fills=(),
            source_event_id=source_event_id,
            top_price=top_price,
            slippage_cost=Decimal("0"),
            queue_ahead_remaining=queue_ahead_remaining,
        )


def order_status_for(result: ExecutionResult) -> OrderStatus:
    """Map an immutable fill outcome to the order state store vocabulary."""

    return OrderStatus(result.status.value)


__all__ = ["PaperExecutionAdapter", "order_status_for"]
