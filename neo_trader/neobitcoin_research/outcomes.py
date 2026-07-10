"""Future executable outcomes from timestamp-correct order books."""

from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass, is_dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import Enum
from uuid import uuid4
from zoneinfo import ZoneInfo

from .config import ResearchConfig
from .execution import simulate_aggressive_sweep, simulate_passive_queue
from .features import TapeTrade
from .types import (
    ExecutionRequest,
    ExecutionResult,
    ExecutionSide,
    OrderBookSnapshot,
    PassiveQueueObservation,
    PassiveQueueScenario,
)


@dataclass(frozen=True)
class PendingOutcome:
    candidate_id: str
    feature_snapshot_id: str
    signal_time: datetime
    horizon_seconds: int
    latency_ms: int
    position_size_rub: int
    quantity_lots: Decimal
    signal_book: OrderBookSnapshot

    @property
    def entry_time(self) -> datetime:
        return self.signal_time + timedelta(milliseconds=self.latency_ms)

    @property
    def exit_time(self) -> datetime:
        return self.signal_time + timedelta(seconds=self.horizon_seconds)


class FutureOutcomeTracker:
    """Resolve registered feature snapshots only from subsequently seen data."""

    def __init__(self, *, config: ResearchConfig, lot_size: int, tick_size: Decimal) -> None:
        self.config = config
        self.lot_size = lot_size
        self.tick_size = tick_size
        self.books: deque[OrderBookSnapshot] = deque()
        self.trades: deque[TapeTrade] = deque()
        self.pending: list[PendingOutcome] = []

    def add_trade(self, trade: TapeTrade) -> None:
        self.trades.append(trade)
        self._trim(trade.timestamp)

    def add_book(self, book: OrderBookSnapshot) -> tuple[dict[str, object], ...]:
        self.books.append(book)
        self._trim(book.exchange_timestamp)
        resolved: list[dict[str, object]] = []
        still_pending: list[PendingOutcome] = []
        for candidate in self.pending:
            if candidate.exit_time <= book.exchange_timestamp:
                resolved.extend(self._resolve(candidate))
            else:
                still_pending.append(candidate)
        self.pending = still_pending
        return tuple(resolved)

    def register(
        self,
        *,
        feature_snapshot_id: str,
        signal_time: datetime,
        signal_book: OrderBookSnapshot,
    ) -> int:
        mid = signal_book.mid_price
        if mid is None:
            return 0
        added = 0
        for notional in self.config.position_sizes_rub:
            quantity = int(Decimal(notional) / (mid * self.lot_size))
            if quantity <= 0:
                continue
            for latency in self.config.latencies_ms:
                for horizon in self.config.horizons_seconds:
                    if latency >= horizon * 1_000:
                        continue
                    self.pending.append(
                        PendingOutcome(
                            candidate_id=uuid4().hex,
                            feature_snapshot_id=feature_snapshot_id,
                            signal_time=signal_time.astimezone(UTC),
                            horizon_seconds=horizon,
                            latency_ms=latency,
                            position_size_rub=notional,
                            quantity_lots=Decimal(quantity),
                            signal_book=signal_book,
                        )
                    )
                    added += 1
        return added

    def _resolve(self, candidate: PendingOutcome) -> list[dict[str, object]]:
        if self._crosses_crypto_fee_boundary(candidate) and (
            self.config.daily_holding_fee_annual_rate is None
        ):
            return [
                self._invalid_row(
                    candidate,
                    "daily_holding_fee_rate_not_configured",
                    direction=direction,
                    model=model,
                )
                for direction in ("LONG", "SHORT")
                for model in (
                    "AGGRESSIVE_SWEEP",
                    "PASSIVE_OPTIMISTIC",
                    "PASSIVE_BASE",
                    "PASSIVE_PESSIMISTIC",
                )
            ]
        entry_book = self._first_book_at_or_after(candidate.entry_time)
        exit_book = self._first_book_at_or_after(candidate.exit_time)
        if entry_book is None or exit_book is None:
            return [self._invalid_row(candidate, "missing_timestamp_aligned_book")]
        rows = [
            self._aggressive(candidate, entry_book, exit_book, direction="LONG"),
            self._aggressive(candidate, entry_book, exit_book, direction="SHORT"),
        ]
        for direction in ("LONG", "SHORT"):
            for scenario in (
                PassiveQueueScenario.OPTIMISTIC,
                PassiveQueueScenario.BASE,
                PassiveQueueScenario.PESSIMISTIC,
            ):
                rows.append(self._passive(candidate, entry_book, exit_book, direction, scenario))
        return rows

    def _aggressive(
        self,
        candidate: PendingOutcome,
        entry_book: OrderBookSnapshot,
        exit_book: OrderBookSnapshot,
        *,
        direction: str,
    ) -> dict[str, object]:
        entry_side = ExecutionSide.BUY if direction == "LONG" else ExecutionSide.SELL
        exit_side = ExecutionSide.SELL if direction == "LONG" else ExecutionSide.BUY
        entry = simulate_aggressive_sweep(
            entry_book,
            ExecutionRequest(entry_side, candidate.quantity_lots),
        )
        if entry.filled_quantity == 0:
            return self._row(
                candidate,
                direction=direction,
                model="AGGRESSIVE_SWEEP",
                status="NO_FILL",
                entry=entry,
                exit_result=None,
                pnl=None,
                entry_book=entry_book,
                exit_book=exit_book,
            )
        exit_result = simulate_aggressive_sweep(
            exit_book,
            ExecutionRequest(exit_side, entry.filled_quantity),
        )
        matched = min(entry.filled_quantity, exit_result.filled_quantity)
        pnl = self._pnl(candidate, direction, entry, exit_result, matched)
        status = "FULL_FILL" if matched == candidate.quantity_lots else "PARTIAL_FILL"
        return self._row(
            candidate,
            direction=direction,
            model="AGGRESSIVE_SWEEP",
            status=status,
            entry=entry,
            exit_result=exit_result,
            pnl=pnl,
            entry_book=entry_book,
            exit_book=exit_book,
        )

    def _passive(
        self,
        candidate: PendingOutcome,
        entry_book: OrderBookSnapshot,
        exit_book: OrderBookSnapshot,
        direction: str,
        scenario: PassiveQueueScenario,
    ) -> dict[str, object]:
        side = ExecutionSide.BUY if direction == "LONG" else ExecutionSide.SELL
        exit_side = ExecutionSide.SELL if direction == "LONG" else ExecutionSide.BUY
        levels = (
            candidate.signal_book.bids if side is ExecutionSide.BUY else candidate.signal_book.asks
        )
        if not levels:
            return self._invalid_row(candidate, "missing_passive_price_level")
        limit_price = levels[0].price
        initial_volume = levels[0].quantity
        current_quantity = self._quantity_at(entry_book, side, limit_price)
        traded = sum(
            (
                trade.quantity
                for trade in self.trades
                if candidate.signal_time < trade.timestamp <= candidate.entry_time
                and trade.price == limit_price
            ),
            Decimal(0),
        )
        cancelled = max(initial_volume - current_quantity - traded, Decimal(0))
        added = max(current_quantity + traded - initial_volume, Decimal(0))
        observation = PassiveQueueObservation(
            volume_ahead=initial_volume,
            traded_at_level=traded,
            cancelled_at_level=cancelled,
            added_at_level=added,
            elapsed_ms=candidate.latency_ms,
            timeout_ms=max(candidate.horizon_seconds * 1_000, 1),
            market_moved_away=self._market_moved_away(entry_book, side, limit_price),
            post_fill_mid_price=entry_book.mid_price,
        )
        entry = simulate_passive_queue(
            candidate.signal_book,
            ExecutionRequest(side, candidate.quantity_lots, limit_price),
            observation,
            scenario=scenario,
        )
        model = f"PASSIVE_{scenario.value}"
        if entry.filled_quantity == 0:
            return self._row(
                candidate,
                direction=direction,
                model=model,
                status=entry.status.value,
                entry=entry,
                exit_result=None,
                pnl=None,
                entry_book=entry_book,
                exit_book=exit_book,
            )
        exit_result = simulate_aggressive_sweep(
            exit_book,
            ExecutionRequest(exit_side, entry.filled_quantity),
        )
        matched = min(entry.filled_quantity, exit_result.filled_quantity)
        pnl = self._pnl(candidate, direction, entry, exit_result, matched)
        status = "FULL_FILL" if matched == candidate.quantity_lots else "PARTIAL_FILL"
        return self._row(
            candidate,
            direction=direction,
            model=model,
            status=status,
            entry=entry,
            exit_result=exit_result,
            pnl=pnl,
            entry_book=entry_book,
            exit_book=exit_book,
        )

    def _pnl(
        self,
        candidate: PendingOutcome,
        direction: str,
        entry: ExecutionResult,
        exit_result: ExecutionResult,
        matched: Decimal,
    ) -> Decimal | None:
        if matched <= 0 or entry.average_price is None or exit_result.average_price is None:
            return None
        if direction == "LONG":
            gross = (exit_result.average_price - entry.average_price) * matched * self.lot_size
        else:
            gross = (entry.average_price - exit_result.average_price) * matched * self.lot_size
        mid = (entry.average_price + exit_result.average_price) / 2
        extra_per_unit = self.tick_size * Decimal(
            str(self.config.adverse_slippage_ticks)
        ) + mid * Decimal(str(self.config.adverse_slippage_bps)) / Decimal(10_000)
        extra = extra_per_unit * matched * self.lot_size * 2
        extra += Decimal(str(self.config.adverse_slippage_rub)) * 2
        return gross - extra - self._holding_fee(candidate, entry, matched)

    def _row(
        self,
        candidate: PendingOutcome,
        *,
        direction: str,
        model: str,
        status: str,
        entry: ExecutionResult,
        exit_result: ExecutionResult | None,
        pnl: Decimal | None,
        entry_book: OrderBookSnapshot,
        exit_book: OrderBookSnapshot,
    ) -> dict[str, object]:
        path = [
            book
            for book in self.books
            if entry_book.exchange_timestamp
            <= book.exchange_timestamp
            <= exit_book.exchange_timestamp
            and book.mid_price is not None
        ]
        entry_mid = entry_book.mid_price
        moves = [
            book.mid_price - entry_mid for book in path if book.mid_price is not None and entry_mid
        ]
        if direction == "SHORT":
            moves = [-move for move in moves]
        mfe = max(moves, default=Decimal(0)) * candidate.quantity_lots * self.lot_size
        mae = min(moves, default=Decimal(0)) * candidate.quantity_lots * self.lot_size
        matched = (
            min(entry.filled_quantity, exit_result.filled_quantity)
            if exit_result is not None
            else Decimal(0)
        )
        return {
            "timestamp": candidate.signal_time.isoformat(),
            "candidate_id": candidate.candidate_id,
            "feature_snapshot_id": candidate.feature_snapshot_id,
            "direction": direction,
            "execution_model": model,
            "horizon_seconds": candidate.horizon_seconds,
            "latency_ms": candidate.latency_ms,
            "position_size_rub": candidate.position_size_rub,
            "requested_quantity": str(candidate.quantity_lots),
            "matched_round_trip_quantity": str(matched),
            "status": status,
            "realized_net_pnl_rub": str(pnl) if pnl is not None else None,
            "profitable_after_costs": pnl > 0 if pnl is not None else None,
            "mfe_rub": str(mfe),
            "mae_rub": str(mae),
            "midprice_change": (
                str(exit_book.mid_price - entry_book.mid_price)
                if entry_book.mid_price is not None and exit_book.mid_price is not None
                else None
            ),
            "entry": _json_value(entry),
            "exit": _json_value(exit_result) if exit_result is not None else None,
            "price_path": [
                {
                    "timestamp": book.exchange_timestamp.isoformat(),
                    "midprice": str(book.mid_price),
                }
                for book in path
            ],
            "no_fill_is_not_zero_pnl": entry.filled_quantity == 0,
            "entry_exit_commission_rub": "0",
            "holding_fee_rub": str(self._holding_fee(candidate, entry, matched)),
            "commission_basis": (
                "T-Bank neoassets: no buy/sell fee; daily fee applies across 00:00 MSK"
            ),
        }

    def _invalid_row(
        self,
        candidate: PendingOutcome,
        reason: str,
        *,
        direction: str | None = None,
        model: str = "INVALID_DATA",
    ) -> dict[str, object]:
        return {
            "timestamp": candidate.signal_time.isoformat(),
            "candidate_id": candidate.candidate_id,
            "feature_snapshot_id": candidate.feature_snapshot_id,
            "direction": direction,
            "execution_model": model,
            "status": "INVALID_DATA",
            "reason": reason,
            "horizon_seconds": candidate.horizon_seconds,
            "latency_ms": candidate.latency_ms,
            "position_size_rub": candidate.position_size_rub,
            "realized_net_pnl_rub": None,
        }

    def _holding_fee(
        self,
        candidate: PendingOutcome,
        entry: ExecutionResult,
        matched: Decimal,
    ) -> Decimal:
        rate = self.config.daily_holding_fee_annual_rate
        if (
            rate is None
            or not self._crosses_crypto_fee_boundary(candidate)
            or entry.average_price is None
            or matched <= 0
        ):
            return Decimal(0)
        notional = entry.average_price * matched * self.lot_size
        return notional * Decimal(str(rate)) / Decimal(365)

    @staticmethod
    def _crosses_crypto_fee_boundary(candidate: PendingOutcome) -> bool:
        moscow = ZoneInfo("Europe/Moscow")
        return (
            candidate.signal_time.astimezone(moscow).date()
            != candidate.exit_time.astimezone(moscow).date()
        )

    def _first_book_at_or_after(self, timestamp: datetime) -> OrderBookSnapshot | None:
        return next((book for book in self.books if book.exchange_timestamp >= timestamp), None)

    @staticmethod
    def _quantity_at(book: OrderBookSnapshot, side: ExecutionSide, price: Decimal) -> Decimal:
        levels = book.bids if side is ExecutionSide.BUY else book.asks
        return next((level.quantity for level in levels if level.price == price), Decimal(0))

    @staticmethod
    def _market_moved_away(
        book: OrderBookSnapshot, side: ExecutionSide, limit_price: Decimal
    ) -> bool:
        if side is ExecutionSide.BUY:
            return book.best_bid is not None and book.best_bid < limit_price
        return book.best_ask is not None and book.best_ask > limit_price

    def _trim(self, now: datetime) -> None:
        keep = max(self.config.horizons_seconds) + max(self.config.latencies_ms) / 1_000 + 60
        cutoff = now.astimezone(UTC) - timedelta(seconds=keep)
        while self.books and self.books[0].exchange_timestamp < cutoff:
            self.books.popleft()
        while self.trades and self.trades[0].timestamp < cutoff:
            self.trades.popleft()


def _json_value(value: object) -> object:
    if value is None:
        return None
    if is_dataclass(value):
        return _json_value(asdict(value))  # type: ignore[arg-type]
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [_json_value(item) for item in value]
    return value
