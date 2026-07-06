"""Event-driven backtester built on recorded parquet market data.

The module is simulation-only. It does not import broker gateways and cannot
submit real orders.
"""

from __future__ import annotations

import csv
import html
import importlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Protocol, TypeAlias, cast

from neo_trader.features.orderbook import (
    book_wall_score,
    imbalance,
    microprice,
    mid_price,
    spread_bps,
    weighted_imbalance,
)
from neo_trader.strategy.opening_range_book_momentum import (
    OpeningRangeBookMomentumConfig,
    OpeningRangeBookMomentumStrategy,
    PositionSide,
    Signal,
    SignalAction,
    StrategyPosition,
)

JsonMapping: TypeAlias = Mapping[str, Any]
BookInput: TypeAlias = Mapping[str, object]
CandleInput: TypeAlias = Mapping[str, object]
LevelInput: TypeAlias = Mapping[str, object] | Sequence[object] | object
BPS_FACTOR = Decimal("10000")


class _StrategyProtocol(Protocol):
    def evaluate(
        self,
        *,
        rolling_candles: Sequence[CandleInput],
        latest_orderbook_features: Mapping[str, object],
        current_position: StrategyPosition,
        current_time: datetime,
        config: OpeningRangeBookMomentumConfig | None = None,
    ) -> Signal:
        """Return a strategy signal without placing orders."""


StrategyLike: TypeAlias = OpeningRangeBookMomentumStrategy | _StrategyProtocol


class BacktestEventType(StrEnum):
    """Supported raw event types consumed by the backtester."""

    ORDERBOOK = "orderbook"
    TRADES = "trades"
    CANDLES = "candles"


class SimulatedExecutionSide(StrEnum):
    """Aggressive simulated order side."""

    BUY = "BUY"
    SELL = "SELL"


@dataclass(frozen=True)
class MarketDataEvent:
    """Normalized event read from parquet."""

    timestamp: datetime
    instrument_uid: str
    event_type: BacktestEventType
    payload: JsonMapping
    sequence: int | None = None


@dataclass(frozen=True)
class OrderBookFeatureConfig:
    """Feature-engine parameters for order-book snapshots."""

    depth_levels: int = 5
    weighted_imbalance_lambda: Decimal = Decimal("0.7")
    wall_distance_bps: Decimal = Decimal("10")

    def __post_init__(self) -> None:
        if self.depth_levels <= 0:
            raise ValueError("depth_levels must be positive.")
        if self.weighted_imbalance_lambda < 0:
            raise ValueError("weighted_imbalance_lambda must be non-negative.")
        if self.wall_distance_bps < 0:
            raise ValueError("wall_distance_bps must be non-negative.")


@dataclass(frozen=True)
class EventDrivenBacktestConfig:
    """Backtest execution parameters."""

    fixed_quantity: Decimal = Decimal("1")
    max_rolling_candles: int = 500
    flatten_at_end: bool = True
    feature_config: OrderBookFeatureConfig = field(default_factory=OrderBookFeatureConfig)

    def __post_init__(self) -> None:
        if self.fixed_quantity <= 0:
            raise ValueError("fixed_quantity must be positive.")
        if self.max_rolling_candles <= 0:
            raise ValueError("max_rolling_candles must be positive.")


@dataclass(frozen=True)
class BacktestFill:
    """One simulated book fill."""

    timestamp: datetime
    instrument_uid: str
    side: SimulatedExecutionSide
    requested_quantity: Decimal
    filled_quantity: Decimal
    avg_price: Decimal
    mid_price: Decimal
    slippage_bps: Decimal
    partial: bool
    reason: str


@dataclass(frozen=True)
class BacktestTrade:
    """Closed simulated trade with excursion statistics."""

    instrument_uid: str
    side: PositionSide
    entry_time: datetime
    exit_time: datetime
    quantity: Decimal
    entry_price: Decimal
    exit_price: Decimal
    pnl: Decimal
    mae: Decimal
    mfe: Decimal
    entry_slippage_bps: Decimal
    exit_slippage_bps: Decimal
    entry_reason_codes: tuple[str, ...]
    exit_reason_codes: tuple[str, ...]


@dataclass(frozen=True)
class BacktestMetrics:
    """Aggregate backtest result metrics."""

    total_fills: int
    total_trades: int
    closed_trades: int
    total_pnl: Decimal
    gross_profit: Decimal
    gross_loss: Decimal
    win_rate: Decimal
    avg_slippage_bps: Decimal
    max_mae: Decimal
    max_mfe: Decimal


@dataclass(frozen=True)
class BacktestCsvExport:
    """Paths written by ``BacktestReport.export_csv``."""

    metrics_path: Path
    trades_path: Path
    fills_path: Path


@dataclass(frozen=True)
class BacktestReport:
    """Backtest output with CSV and HTML exporters."""

    metrics: BacktestMetrics
    trades: tuple[BacktestTrade, ...]
    fills: tuple[BacktestFill, ...]

    def export_csv(self, directory: Path | str) -> BacktestCsvExport:
        """Write metrics, trades, and fills CSV files into ``directory``."""

        output_dir = Path(directory)
        output_dir.mkdir(parents=True, exist_ok=True)
        metrics_path = output_dir / "metrics.csv"
        trades_path = output_dir / "trades.csv"
        fills_path = output_dir / "fills.csv"
        self.metrics_to_csv(metrics_path)
        self.trades_to_csv(trades_path)
        self.fills_to_csv(fills_path)
        return BacktestCsvExport(
            metrics_path=metrics_path,
            trades_path=trades_path,
            fills_path=fills_path,
        )

    def to_csv(self, path: Path | str) -> None:
        """Write a compact trade report CSV to ``path``."""

        self.trades_to_csv(path)

    def metrics_to_csv(self, path: Path | str) -> None:
        with Path(path).open("w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(["metric", "value"])
            for key, value in _metrics_rows(self.metrics):
                writer.writerow([key, value])

    def trades_to_csv(self, path: Path | str) -> None:
        with Path(path).open("w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(
                [
                    "instrument_uid",
                    "side",
                    "entry_time",
                    "exit_time",
                    "quantity",
                    "entry_price",
                    "exit_price",
                    "pnl",
                    "mae",
                    "mfe",
                    "entry_slippage_bps",
                    "exit_slippage_bps",
                    "entry_reason_codes",
                    "exit_reason_codes",
                ]
            )
            for trade in self.trades:
                writer.writerow(
                    [
                        trade.instrument_uid,
                        trade.side.value,
                        trade.entry_time.isoformat(),
                        trade.exit_time.isoformat(),
                        str(trade.quantity),
                        str(trade.entry_price),
                        str(trade.exit_price),
                        str(trade.pnl),
                        str(trade.mae),
                        str(trade.mfe),
                        str(trade.entry_slippage_bps),
                        str(trade.exit_slippage_bps),
                        "|".join(trade.entry_reason_codes),
                        "|".join(trade.exit_reason_codes),
                    ]
                )

    def fills_to_csv(self, path: Path | str) -> None:
        with Path(path).open("w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(
                [
                    "timestamp",
                    "instrument_uid",
                    "side",
                    "requested_quantity",
                    "filled_quantity",
                    "avg_price",
                    "mid_price",
                    "slippage_bps",
                    "partial",
                    "reason",
                ]
            )
            for fill in self.fills:
                writer.writerow(
                    [
                        fill.timestamp.isoformat(),
                        fill.instrument_uid,
                        fill.side.value,
                        str(fill.requested_quantity),
                        str(fill.filled_quantity),
                        str(fill.avg_price),
                        str(fill.mid_price),
                        str(fill.slippage_bps),
                        str(fill.partial),
                        fill.reason,
                    ]
                )

    def to_html(self, path: Path | str) -> None:
        """Write a standalone HTML report."""

        metrics_html = _html_table(
            ("Metric", "Value"),
            tuple((name, value) for name, value in _metrics_rows(self.metrics)),
        )
        trade_rows = tuple(_trade_html_row(trade) for trade in self.trades)
        fills_rows = tuple(_fill_html_row(fill) for fill in self.fills)
        document = (
            "<!doctype html>\n"
            "<html><head><meta charset=\"utf-8\">"
            "<title>neo_trader backtest report</title>"
            "<style>"
            "body{font-family:Arial,sans-serif;margin:24px;color:#17202a;}"
            "table{border-collapse:collapse;margin:12px 0 24px;width:100%;}"
            "th,td{border:1px solid #d5d8dc;padding:6px 8px;text-align:left;}"
            "th{background:#f4f6f7;}"
            "</style></head><body>"
            "<h1>neo_trader backtest report</h1>"
            "<h2>Metrics</h2>"
            f"{metrics_html}"
            "<h2>Trades</h2>"
            f"{_html_table(_trade_headers(), trade_rows)}"
            "<h2>Fills</h2>"
            f"{_html_table(_fill_headers(), fills_rows)}"
            "</body></html>\n"
        )
        Path(path).write_text(document, encoding="utf-8")


@dataclass
class _OpenTrade:
    instrument_uid: str
    side: PositionSide
    entry_time: datetime
    quantity: Decimal
    entry_price: Decimal
    entry_slippage_bps: Decimal
    entry_reason_codes: tuple[str, ...]
    mae: Decimal = Decimal("0")
    mfe: Decimal = Decimal("0")


@dataclass(frozen=True)
class _BookLevel:
    price: Decimal
    quantity: Decimal


class ParquetMarketDataReader:
    """Read raw recorder parquet files into a sorted event stream."""

    def read(self, root: Path | str) -> tuple[MarketDataEvent, ...]:
        path = Path(root)
        files = [path] if path.is_file() else sorted(path.rglob("*.parquet"))
        events: list[MarketDataEvent] = []
        pq = cast(Any, importlib.import_module("pyarrow.parquet"))

        for file_path in files:
            table = pq.read_table(file_path)
            rows = cast(list[dict[str, Any]], table.to_pylist())
            for row in rows:
                events.append(_event_from_row(row, file_path))

        return tuple(
            sorted(
                events,
                key=lambda event: (
                    event.timestamp,
                    event.sequence if event.sequence is not None else -1,
                    event.event_type.value,
                ),
            )
        )


class OrderBookFeatureEngine:
    """Compute strategy-ready features from an order-book snapshot."""

    def __init__(self, config: OrderBookFeatureConfig | None = None) -> None:
        self.config = config or OrderBookFeatureConfig()

    def compute(self, book: BookInput) -> dict[str, Decimal]:
        return {
            "mid_price": mid_price(book),
            "spread_bps": spread_bps(book),
            "imbalance": imbalance(book, self.config.depth_levels),
            "weighted_imbalance": weighted_imbalance(
                book,
                self.config.weighted_imbalance_lambda,
            ),
            "microprice": microprice(book),
            "bid_wall_score": book_wall_score(book, "bid", self.config.wall_distance_bps),
            "ask_wall_score": book_wall_score(book, "ask", self.config.wall_distance_bps),
        }


class SimulatedBookExecutor:
    """Fill aggressive simulated orders against displayed book depth."""

    def fill(
        self,
        *,
        book: BookInput,
        side: SimulatedExecutionSide,
        quantity: Decimal,
        timestamp: datetime,
        instrument_uid: str,
        reason: str,
    ) -> BacktestFill:
        if quantity <= 0:
            raise ValueError("quantity must be positive.")

        book_mid = mid_price(book)
        levels = _book_levels(book, "ask" if side is SimulatedExecutionSide.BUY else "bid")
        remaining = quantity
        filled = Decimal("0")
        notional = Decimal("0")

        for level in levels:
            if remaining <= 0:
                break
            consumed = min(remaining, level.quantity)
            if consumed <= 0:
                continue
            filled += consumed
            notional += consumed * level.price
            remaining -= consumed

        if filled == 0:
            avg_price = Decimal("0")
            slippage = Decimal("0")
        else:
            avg_price = notional / filled
            slippage = _fill_slippage_bps(side=side, avg_price=avg_price, book_mid=book_mid)

        return BacktestFill(
            timestamp=timestamp,
            instrument_uid=instrument_uid,
            side=side,
            requested_quantity=quantity,
            filled_quantity=filled,
            avg_price=avg_price,
            mid_price=book_mid,
            slippage_bps=slippage,
            partial=filled < quantity,
            reason=reason,
        )


class EventDrivenBacktester:
    """Replay raw market data, call a strategy, and simulate book fills."""

    def __init__(
        self,
        *,
        strategy: StrategyLike | None = None,
        strategy_config: OpeningRangeBookMomentumConfig | None = None,
        config: EventDrivenBacktestConfig | None = None,
        reader: ParquetMarketDataReader | None = None,
        feature_engine: OrderBookFeatureEngine | None = None,
        executor: SimulatedBookExecutor | None = None,
    ) -> None:
        self.config = config or EventDrivenBacktestConfig()
        self.strategy_config = strategy_config
        self.strategy = strategy or OpeningRangeBookMomentumStrategy(strategy_config)
        self.reader = reader or ParquetMarketDataReader()
        self.feature_engine = feature_engine or OrderBookFeatureEngine(self.config.feature_config)
        self.executor = executor or SimulatedBookExecutor()

    def run_from_parquet(self, root: Path | str) -> BacktestReport:
        """Load recorder parquet files and run the event-driven simulation."""

        return self.run(self.reader.read(root))

    def run(self, events: Sequence[MarketDataEvent]) -> BacktestReport:
        """Run a backtest over pre-normalized events."""

        rolling_candles: list[CandleInput] = []
        latest_book: BookInput | None = None
        latest_features: Mapping[str, object] | None = None
        open_trade: _OpenTrade | None = None
        fills: list[BacktestFill] = []
        trades: list[BacktestTrade] = []

        for event in events:
            if event.event_type is BacktestEventType.CANDLES:
                candle = _extract_candle(event.payload, event.timestamp)
                rolling_candles.append(candle)
                if len(rolling_candles) > self.config.max_rolling_candles:
                    rolling_candles = rolling_candles[-self.config.max_rolling_candles :]
                open_trade = _update_trade_excursion(open_trade, _candle_close(candle))
                continue

            if event.event_type is BacktestEventType.TRADES:
                trade_price = _extract_trade_price(event.payload)
                if trade_price is not None:
                    open_trade = _update_trade_excursion(open_trade, trade_price)
                continue

            latest_book = _extract_book(event.payload)
            latest_features = self.feature_engine.compute(latest_book)
            open_trade = _update_trade_excursion(
                open_trade,
                latest_features["mid_price"],
            )

            signal = self.strategy.evaluate(
                rolling_candles=rolling_candles,
                latest_orderbook_features=latest_features,
                current_position=_strategy_position(open_trade),
                current_time=_strategy_datetime(event.timestamp),
                config=self.strategy_config,
            )
            open_trade = self._apply_signal(
                signal=signal,
                event=event,
                book=latest_book,
                open_trade=open_trade,
                fills=fills,
                trades=trades,
            )

        if (
            self.config.flatten_at_end
            and open_trade is not None
            and latest_book is not None
            and latest_features is not None
        ):
            last_event = events[-1] if events else None
            if last_event is not None:
                fill = self._exit_fill(
                    book=latest_book,
                    open_trade=open_trade,
                    timestamp=last_event.timestamp,
                    reason="END_OF_DATA",
                )
                fills.append(fill)
                open_trade = self._close_or_reduce_position(
                    open_trade=open_trade,
                    exit_fill=fill,
                    exit_reason_codes=("END_OF_DATA",),
                    trades=trades,
                )

        return BacktestReport(
            metrics=_build_metrics(trades=trades, fills=fills),
            trades=tuple(trades),
            fills=tuple(fills),
        )

    def _apply_signal(
        self,
        *,
        signal: Signal,
        event: MarketDataEvent,
        book: BookInput,
        open_trade: _OpenTrade | None,
        fills: list[BacktestFill],
        trades: list[BacktestTrade],
    ) -> _OpenTrade | None:
        reason_codes = tuple(str(reason) for reason in signal.reason_codes)
        if signal.action is SignalAction.HOLD:
            return open_trade

        if signal.action is SignalAction.EXIT:
            if open_trade is None:
                return None
            fill = self._exit_fill(
                book=book,
                open_trade=open_trade,
                timestamp=event.timestamp,
                reason="|".join(reason_codes),
            )
            fills.append(fill)
            return self._close_or_reduce_position(
                open_trade=open_trade,
                exit_fill=fill,
                exit_reason_codes=reason_codes,
                trades=trades,
            )

        if open_trade is not None:
            return open_trade

        if signal.action is SignalAction.BUY:
            entry_side = SimulatedExecutionSide.BUY
            position_side = PositionSide.LONG
        elif signal.action is SignalAction.SELL:
            entry_side = SimulatedExecutionSide.SELL
            position_side = PositionSide.SHORT
        else:
            return open_trade

        fill = self.executor.fill(
            book=book,
            side=entry_side,
            quantity=self.config.fixed_quantity,
            timestamp=event.timestamp,
            instrument_uid=event.instrument_uid,
            reason="|".join(reason_codes),
        )
        fills.append(fill)
        if fill.filled_quantity <= 0:
            return None

        opened = _OpenTrade(
            instrument_uid=event.instrument_uid,
            side=position_side,
            entry_time=event.timestamp,
            quantity=fill.filled_quantity,
            entry_price=fill.avg_price,
            entry_slippage_bps=fill.slippage_bps,
            entry_reason_codes=reason_codes,
        )
        return _update_trade_excursion(opened, fill.mid_price)

    def _exit_fill(
        self,
        *,
        book: BookInput,
        open_trade: _OpenTrade,
        timestamp: datetime,
        reason: str,
    ) -> BacktestFill:
        exit_side = (
            SimulatedExecutionSide.SELL
            if open_trade.side is PositionSide.LONG
            else SimulatedExecutionSide.BUY
        )
        return self.executor.fill(
            book=book,
            side=exit_side,
            quantity=open_trade.quantity,
            timestamp=timestamp,
            instrument_uid=open_trade.instrument_uid,
            reason=reason,
        )

    def _close_or_reduce_position(
        self,
        *,
        open_trade: _OpenTrade,
        exit_fill: BacktestFill,
        exit_reason_codes: tuple[str, ...],
        trades: list[BacktestTrade],
    ) -> _OpenTrade | None:
        if exit_fill.filled_quantity <= 0:
            return open_trade

        closed_quantity = min(open_trade.quantity, exit_fill.filled_quantity)
        trades.append(
            _closed_trade(
                open_trade=open_trade,
                exit_fill=exit_fill,
                closed_quantity=closed_quantity,
                exit_reason_codes=exit_reason_codes,
            )
        )

        remaining = open_trade.quantity - closed_quantity
        if remaining <= 0:
            return None

        return _OpenTrade(
            instrument_uid=open_trade.instrument_uid,
            side=open_trade.side,
            entry_time=open_trade.entry_time,
            quantity=remaining,
            entry_price=open_trade.entry_price,
            entry_slippage_bps=open_trade.entry_slippage_bps,
            entry_reason_codes=open_trade.entry_reason_codes,
            mae=_scale_currency(open_trade.mae, remaining, open_trade.quantity),
            mfe=_scale_currency(open_trade.mfe, remaining, open_trade.quantity),
        )


def _event_from_row(row: Mapping[str, Any], path: Path) -> MarketDataEvent:
    payload = _payload_from_row(row)
    event_type = _normalize_event_type(row.get("event_type") or _event_type_from_path(path))
    instrument_uid = _normalize_instrument_uid(
        row.get("instrument_uid")
        or _instrument_uid_from_path(path)
        or payload.get("instrument_uid")
    )
    timestamp = _row_timestamp(row, payload)
    sequence = _optional_int(row.get("sequence"))
    return MarketDataEvent(
        timestamp=timestamp,
        instrument_uid=instrument_uid,
        event_type=event_type,
        payload=payload,
        sequence=sequence,
    )


def _payload_from_row(row: Mapping[str, Any]) -> dict[str, Any]:
    value = row.get("payload_json")
    if isinstance(value, str):
        loaded = json.loads(value)
        if not isinstance(loaded, Mapping):
            raise ValueError("payload_json must decode to an object.")
        return dict(cast(Mapping[str, Any], loaded))
    if isinstance(value, Mapping):
        return dict(cast(Mapping[str, Any], value))
    payload = row.get("payload")
    if isinstance(payload, Mapping):
        return dict(cast(Mapping[str, Any], payload))
    return {}


def _row_timestamp(row: Mapping[str, Any], payload: JsonMapping) -> datetime:
    for value in (
        row.get("event_time"),
        row.get("received_at"),
        payload.get("time"),
        payload.get("timestamp"),
        payload.get("ts"),
    ):
        if value is not None:
            return _parse_datetime(value)
    return datetime.fromtimestamp(0, UTC)


def _event_type_from_path(path: Path) -> str | None:
    candidates = [path.stem, path.parent.name]
    for candidate in candidates:
        if candidate.startswith("type="):
            return candidate.removeprefix("type=")
    return None


def _instrument_uid_from_path(path: Path) -> str | None:
    for parent in path.parents:
        if parent.name.startswith("instrument="):
            return parent.name.removeprefix("instrument=")
    return None


def _normalize_event_type(value: object) -> BacktestEventType:
    normalized = str(value or "").strip().lower()
    if normalized in {"orderbook", "order_book", "book"}:
        return BacktestEventType.ORDERBOOK
    if normalized in {"trades", "trade"}:
        return BacktestEventType.TRADES
    if normalized in {"candles", "candle"}:
        return BacktestEventType.CANDLES
    raise ValueError(f"unsupported backtest event type: {value!r}.")


def _normalize_instrument_uid(value: object) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError("instrument_uid is required.")
    return normalized


def _extract_book(payload: JsonMapping) -> BookInput:
    for key in ("orderbook", "order_book", "book"):
        value = payload.get(key)
        if isinstance(value, Mapping):
            return dict(cast(Mapping[str, object], value))
    return dict(cast(Mapping[str, object], payload))


def _extract_candle(payload: JsonMapping, timestamp: datetime) -> CandleInput:
    source: Mapping[str, object]
    source = dict(cast(Mapping[str, object], payload))
    for key in ("candle", "candles"):
        value = payload.get(key)
        if isinstance(value, Mapping):
            source = cast(Mapping[str, object], value)
            break

    candle = dict(source)
    if not any(key in candle for key in ("timestamp", "time", "datetime", "ts")):
        candle["timestamp"] = _strategy_datetime(timestamp)
    else:
        for key in ("timestamp", "time", "datetime", "ts"):
            value = candle.get(key)
            if value is not None:
                candle[key] = _strategy_datetime(_parse_datetime(value))
    return candle


def _extract_trade_price(payload: JsonMapping) -> Decimal | None:
    source = payload
    for key in ("trade", "last_trade"):
        value = payload.get(key)
        if isinstance(value, Mapping):
            source = cast(JsonMapping, value)
            break

    for key in ("price", "last_price", "p"):
        value = source.get(key)
        if value is not None:
            return _to_decimal(value)
    return None


def _candle_close(candle: CandleInput) -> Decimal:
    for key in ("close", "c"):
        value = candle.get(key)
        if value is not None:
            return _to_decimal(value)
    raise ValueError("candle close is required.")


def _strategy_position(open_trade: _OpenTrade | None) -> StrategyPosition:
    if open_trade is None:
        return StrategyPosition()
    return StrategyPosition(
        side=open_trade.side,
        quantity=open_trade.quantity,
        avg_entry_price=open_trade.entry_price,
    )


def _update_trade_excursion(
    open_trade: _OpenTrade | None,
    mark_price: Decimal,
) -> _OpenTrade | None:
    if open_trade is None:
        return None

    if open_trade.side is PositionSide.LONG:
        unrealized = (mark_price - open_trade.entry_price) * open_trade.quantity
    else:
        unrealized = (open_trade.entry_price - mark_price) * open_trade.quantity

    open_trade.mae = min(open_trade.mae, unrealized)
    open_trade.mfe = max(open_trade.mfe, unrealized)
    return open_trade


def _closed_trade(
    *,
    open_trade: _OpenTrade,
    exit_fill: BacktestFill,
    closed_quantity: Decimal,
    exit_reason_codes: tuple[str, ...],
) -> BacktestTrade:
    if open_trade.side is PositionSide.LONG:
        pnl = (exit_fill.avg_price - open_trade.entry_price) * closed_quantity
    else:
        pnl = (open_trade.entry_price - exit_fill.avg_price) * closed_quantity

    mae = _scale_currency(open_trade.mae, closed_quantity, open_trade.quantity)
    mfe = _scale_currency(open_trade.mfe, closed_quantity, open_trade.quantity)
    return BacktestTrade(
        instrument_uid=open_trade.instrument_uid,
        side=open_trade.side,
        entry_time=open_trade.entry_time,
        exit_time=exit_fill.timestamp,
        quantity=closed_quantity,
        entry_price=open_trade.entry_price,
        exit_price=exit_fill.avg_price,
        pnl=pnl,
        mae=mae,
        mfe=mfe,
        entry_slippage_bps=open_trade.entry_slippage_bps,
        exit_slippage_bps=exit_fill.slippage_bps,
        entry_reason_codes=open_trade.entry_reason_codes,
        exit_reason_codes=exit_reason_codes,
    )


def _scale_currency(value: Decimal, numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator == 0:
        return Decimal("0")
    return value * numerator / denominator


def _build_metrics(
    *,
    trades: Sequence[BacktestTrade],
    fills: Sequence[BacktestFill],
) -> BacktestMetrics:
    total_pnl = sum((trade.pnl for trade in trades), Decimal("0"))
    winners = [trade for trade in trades if trade.pnl > 0]
    gross_profit = sum((trade.pnl for trade in trades if trade.pnl > 0), Decimal("0"))
    gross_loss = sum((trade.pnl for trade in trades if trade.pnl < 0), Decimal("0"))
    avg_slippage = (
        sum((fill.slippage_bps for fill in fills), Decimal("0")) / Decimal(len(fills))
        if fills
        else Decimal("0")
    )
    return BacktestMetrics(
        total_fills=len(fills),
        total_trades=len(trades),
        closed_trades=len(trades),
        total_pnl=total_pnl,
        gross_profit=gross_profit,
        gross_loss=gross_loss,
        win_rate=Decimal(len(winners)) / Decimal(len(trades)) if trades else Decimal("0"),
        avg_slippage_bps=avg_slippage,
        max_mae=min((trade.mae for trade in trades), default=Decimal("0")),
        max_mfe=max((trade.mfe for trade in trades), default=Decimal("0")),
    )


def _book_levels(book: BookInput, side: Literal["bid", "ask"]) -> list[_BookLevel]:
    key_names = ("bids", "bid") if side == "bid" else ("asks", "ask")
    raw_levels: Sequence[LevelInput] | None = None
    for key in key_names:
        value = book.get(key)
        if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
            raw_levels = cast(Sequence[LevelInput], value)
            break
    if raw_levels is None:
        raise ValueError(f"book has no {side} side.")

    levels = [_normalize_level(level) for level in raw_levels]
    levels = [level for level in levels if level.quantity > 0]
    if not levels:
        raise ValueError(f"book has no positive {side} quantity.")
    return sorted(levels, key=lambda level: level.price, reverse=side == "bid")


def _normalize_level(level: LevelInput) -> _BookLevel:
    if isinstance(level, Mapping):
        price = _to_decimal(_required_mapping_value(level, "price"))
        quantity = _to_decimal(_first_mapping_value(level, "quantity", "qty", "size", "volume"))
        return _BookLevel(price=price, quantity=quantity)

    if isinstance(level, Sequence) and not isinstance(level, str | bytes | bytearray):
        if len(level) < 2:
            raise ValueError("book level must include price and quantity.")
        return _BookLevel(price=_to_decimal(level[0]), quantity=_to_decimal(level[1]))

    raw_price = getattr(level, "price", None)
    raw_quantity = getattr(level, "quantity", None)
    if raw_quantity is None:
        raw_quantity = getattr(level, "qty", None)
    if raw_price is None or raw_quantity is None:
        raise ValueError("book level object must expose price and quantity.")
    return _BookLevel(price=_to_decimal(raw_price), quantity=_to_decimal(raw_quantity))


def _required_mapping_value(mapping: Mapping[str, object], key: str) -> object:
    value = mapping.get(key)
    if value is None:
        raise ValueError(f"missing mapping field: {key}.")
    return value


def _first_mapping_value(mapping: Mapping[str, object], *keys: str) -> object:
    for key in keys:
        value = mapping.get(key)
        if value is not None:
            return value
    joined = ", ".join(keys)
    raise ValueError(f"missing mapping field, expected one of: {joined}.")


def _fill_slippage_bps(
    *,
    side: SimulatedExecutionSide,
    avg_price: Decimal,
    book_mid: Decimal,
) -> Decimal:
    if book_mid == 0:
        return Decimal("0")
    if side is SimulatedExecutionSide.BUY:
        return ((avg_price - book_mid) / book_mid) * BPS_FACTOR
    return ((book_mid - avg_price) / book_mid) * BPS_FACTOR


def _parse_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return _as_utc(value)
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        return _as_utc(datetime.fromisoformat(normalized))
    raise TypeError(f"unsupported datetime value: {value!r}.")


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _strategy_datetime(value: datetime) -> datetime:
    return _as_utc(value).replace(tzinfo=None)


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise TypeError("sequence must be an integer, not bool.")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value)
    raise TypeError(f"unsupported sequence value: {value!r}.")


def _to_decimal(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        raise TypeError("boolean values are not valid numeric backtest values.")
    if isinstance(value, int | str):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, Mapping) and ("units" in value or "nano" in value):
        units = _to_int(value.get("units", 0), "units")
        nano = _to_int(value.get("nano", 0), "nano")
        return Decimal(units) + (Decimal(nano) / Decimal("1000000000"))
    raise TypeError(f"unsupported decimal value: {value!r}.")


def _to_int(value: object, field_name: str) -> int:
    if isinstance(value, bool):
        raise TypeError(f"{field_name} must be an integer, not bool.")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value)
    raise TypeError(f"{field_name} must be integer-compatible.")


def _metrics_rows(metrics: BacktestMetrics) -> tuple[tuple[str, str], ...]:
    return (
        ("total_fills", str(metrics.total_fills)),
        ("total_trades", str(metrics.total_trades)),
        ("closed_trades", str(metrics.closed_trades)),
        ("total_pnl", str(metrics.total_pnl)),
        ("gross_profit", str(metrics.gross_profit)),
        ("gross_loss", str(metrics.gross_loss)),
        ("win_rate", str(metrics.win_rate)),
        ("avg_slippage_bps", str(metrics.avg_slippage_bps)),
        ("max_mae", str(metrics.max_mae)),
        ("max_mfe", str(metrics.max_mfe)),
    )


def _trade_headers() -> tuple[str, ...]:
    return (
        "Instrument",
        "Side",
        "Entry",
        "Exit",
        "Qty",
        "Entry Price",
        "Exit Price",
        "PnL",
        "MAE",
        "MFE",
    )


def _fill_headers() -> tuple[str, ...]:
    return (
        "Timestamp",
        "Instrument",
        "Side",
        "Requested",
        "Filled",
        "Avg Price",
        "Slippage bps",
        "Partial",
        "Reason",
    )


def _trade_html_row(trade: BacktestTrade) -> tuple[str, ...]:
    return (
        trade.instrument_uid,
        trade.side.value,
        trade.entry_time.isoformat(),
        trade.exit_time.isoformat(),
        str(trade.quantity),
        str(trade.entry_price),
        str(trade.exit_price),
        str(trade.pnl),
        str(trade.mae),
        str(trade.mfe),
    )


def _fill_html_row(fill: BacktestFill) -> tuple[str, ...]:
    return (
        fill.timestamp.isoformat(),
        fill.instrument_uid,
        fill.side.value,
        str(fill.requested_quantity),
        str(fill.filled_quantity),
        str(fill.avg_price),
        str(fill.slippage_bps),
        str(fill.partial),
        fill.reason,
    )


def _html_table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    header_html = "".join(f"<th>{html.escape(header)}</th>" for header in headers)
    row_html = "".join(
        "<tr>" + "".join(f"<td>{html.escape(value)}</td>" for value in row) + "</tr>"
        for row in rows
    )
    return f"<table><thead><tr>{header_html}</tr></thead><tbody>{row_html}</tbody></table>"


__all__ = [
    "BacktestCsvExport",
    "BacktestEventType",
    "BacktestFill",
    "BacktestMetrics",
    "BacktestReport",
    "BacktestTrade",
    "EventDrivenBacktestConfig",
    "EventDrivenBacktester",
    "MarketDataEvent",
    "OrderBookFeatureConfig",
    "OrderBookFeatureEngine",
    "ParquetMarketDataReader",
    "SimulatedBookExecutor",
    "SimulatedExecutionSide",
]
