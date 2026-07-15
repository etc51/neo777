"""Independent arithmetic oracle for daily paper archive validation.

This module depends only on immutable domain records.  It deliberately
recomputes fills and trade metrics instead of sharing simulator helpers.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from .domain import (
    DomainValidationError,
    ExecutionResult,
    FillStatus,
    LookAheadError,
    OrderBook,
    PaperOrder,
    PaperPosition,
    PositionStatus,
    Side,
    as_utc,
    decimal_value,
)


@dataclass(frozen=True, slots=True)
class OracleLevelFill:
    level_index: int
    price: Decimal
    quantity: Decimal


@dataclass(frozen=True, slots=True)
class OracleExecution:
    status: FillStatus
    filled_quantity: Decimal
    unfilled_quantity: Decimal
    vwap: Decimal | None
    fills: tuple[OracleLevelFill, ...]
    source_event_id: str


@dataclass(frozen=True, slots=True)
class OracleTradeMetrics:
    gross_pnl: Decimal
    net_pnl: Decimal
    mfe: Decimal
    mae: Decimal


@dataclass(frozen=True, slots=True)
class OracleReport:
    passed: bool
    discrepancies: tuple[str, ...]

    @property
    def unexplained_count(self) -> int:
        return len(self.discrepancies)


class IndependentOracle:
    """Recompute source selection, fills, PnL and excursions independently."""

    @staticmethod
    def aggressive(order: PaperOrder, book: OrderBook) -> OracleExecution:
        if order.instrument_uid != book.instrument_uid:
            raise DomainValidationError("oracle order/book instrument mismatch")
        if book.receive_ts < order.eligible_ts:
            raise LookAheadError("oracle refuses a book received before eligibility")
        if not book.is_executable:
            return OracleExecution(
                status=FillStatus.NO_FILL,
                filled_quantity=Decimal("0"),
                unfilled_quantity=order.quantity,
                vwap=None,
                fills=(),
                source_event_id=book.event_id,
            )

        levels = book.asks if order.side is Side.BUY else book.bids
        outstanding = order.quantity
        rows: list[OracleLevelFill] = []
        for index, level in enumerate(levels, start=1):
            within_limit = (
                order.limit_price is None
                or (order.side is Side.BUY and level.price <= order.limit_price)
                or (order.side is Side.SELL and level.price >= order.limit_price)
            )
            if not within_limit:
                break
            matched = min(outstanding, level.quantity)
            if matched > 0:
                rows.append(OracleLevelFill(index, level.price, matched))
                outstanding -= matched
            if outstanding == 0:
                break

        filled = order.quantity - outstanding
        if filled == 0:
            status = FillStatus.NO_FILL
            vwap = None
        elif outstanding == 0:
            status = FillStatus.FULL_FILL
            vwap = sum((row.price * row.quantity for row in rows), Decimal("0")) / filled
        else:
            status = FillStatus.PARTIAL_FILL
            vwap = sum((row.price * row.quantity for row in rows), Decimal("0")) / filled
        return OracleExecution(
            status=status,
            filled_quantity=filled,
            unfilled_quantity=outstanding,
            vwap=vwap,
            fills=tuple(rows),
            source_event_id=book.event_id,
        )

    @classmethod
    def validate_aggressive(
        cls,
        order: PaperOrder,
        book: OrderBook,
        actual: ExecutionResult,
    ) -> OracleReport:
        expected = cls.aggressive(order, book)
        differences: list[str] = []
        cls._compare(
            "source_event_id",
            expected.source_event_id,
            actual.source_event_id,
            differences,
        )
        cls._compare("status", expected.status, actual.status, differences)
        cls._compare(
            "filled_quantity",
            expected.filled_quantity,
            actual.filled_quantity,
            differences,
        )
        cls._compare(
            "unfilled_quantity",
            expected.unfilled_quantity,
            actual.unfilled_quantity,
            differences,
        )
        cls._compare("vwap", expected.vwap, actual.vwap, differences)
        expected_rows = tuple((row.level_index, row.price, row.quantity) for row in expected.fills)
        actual_rows = tuple((row.level_index, row.price, row.quantity) for row in actual.fills)
        cls._compare("fill_rows", expected_rows, actual_rows, differences)
        return OracleReport(passed=not differences, discrepancies=tuple(differences))

    @staticmethod
    def trade_metrics(
        *,
        entry_side: Side,
        quantity: Decimal,
        entry_price: Decimal,
        exit_price: Decimal,
        marks: Iterable[tuple[datetime, Decimal]],
        opened_ts: datetime,
        closed_ts: datetime,
        carry_cost: Decimal = Decimal("0"),
        other_costs: Decimal = Decimal("0"),
    ) -> OracleTradeMetrics:
        quantity = decimal_value(quantity, "quantity")
        entry = decimal_value(entry_price, "entry_price")
        exit_value = decimal_value(exit_price, "exit_price")
        carry = decimal_value(carry_cost, "carry_cost")
        other = decimal_value(other_costs, "other_costs")
        opened_ts = as_utc(opened_ts, "opened_ts")
        closed_ts = as_utc(closed_ts, "closed_ts")
        if quantity <= 0 or entry <= 0 or exit_value <= 0:
            raise DomainValidationError("oracle quantity and prices must be positive")
        if min(carry, other) < 0:
            raise DomainValidationError("oracle costs must be non-negative")
        if closed_ts < opened_ts:
            raise LookAheadError("oracle exit cannot precede entry")

        direction = Decimal("1") if entry_side is Side.BUY else Decimal("-1")
        favorable = Decimal("0")
        adverse = Decimal("0")
        previous_ts = opened_ts
        observed_prices = [(opened_ts, entry)]
        for mark_ts, mark_price in marks:
            mark_ts = as_utc(mark_ts, "mark_ts")
            mark = decimal_value(mark_price, "mark_price")
            if mark_ts < previous_ts or mark_ts < opened_ts or mark_ts > closed_ts:
                raise LookAheadError("oracle mark path is not causal")
            if mark <= 0:
                raise DomainValidationError("oracle mark must be positive")
            observed_prices.append((mark_ts, mark))
            previous_ts = mark_ts
        observed_prices.append((closed_ts, exit_value))
        for _, price in observed_prices:
            pnl = (price - entry) * quantity * direction
            favorable = max(favorable, pnl)
            adverse = max(adverse, -pnl)
        gross = (exit_value - entry) * quantity * direction
        return OracleTradeMetrics(
            gross_pnl=gross,
            net_pnl=gross - carry - other,
            mfe=favorable,
            mae=adverse,
        )

    @classmethod
    def validate_closed_position(
        cls,
        position: PaperPosition,
        *,
        marks: Iterable[tuple[datetime, Decimal]],
        other_costs: Decimal = Decimal("0"),
    ) -> OracleReport:
        if position.status is not PositionStatus.CLOSED:
            raise DomainValidationError("oracle validation requires a closed position")
        assert position.closed_ts is not None
        assert position.exit_price is not None
        metrics = cls.trade_metrics(
            entry_side=position.entry_side,
            quantity=position.quantity,
            entry_price=position.entry_price,
            exit_price=position.exit_price,
            marks=marks,
            opened_ts=position.opened_ts,
            closed_ts=position.closed_ts,
            carry_cost=position.carry_cost,
            other_costs=other_costs,
        )
        differences: list[str] = []
        cls._compare("gross_pnl", metrics.gross_pnl, position.realized_gross_pnl, differences)
        cls._compare("net_pnl", metrics.net_pnl, position.realized_net_pnl, differences)
        cls._compare("mfe", metrics.mfe, position.mfe, differences)
        cls._compare("mae", metrics.mae, position.mae, differences)
        return OracleReport(passed=not differences, discrepancies=tuple(differences))

    @staticmethod
    def validate_equity(*, cash: Decimal, unrealized: Decimal, equity: Decimal) -> OracleReport:
        cash = decimal_value(cash, "cash")
        unrealized = decimal_value(unrealized, "unrealized")
        equity = decimal_value(equity, "equity")
        expected = cash + unrealized
        differences = (
            ()
            if expected == equity
            else (f"equity: expected {expected!r}, got {equity!r}",)
        )
        return OracleReport(passed=not differences, discrepancies=differences)

    @staticmethod
    def _compare(name: str, expected: object, actual: object, output: list[str]) -> None:
        if expected != actual:
            output.append(f"{name}: expected {expected!r}, got {actual!r}")


__all__ = [
    "IndependentOracle",
    "OracleExecution",
    "OracleLevelFill",
    "OracleReport",
    "OracleTradeMetrics",
]
