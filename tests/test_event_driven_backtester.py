"""Tests for the event-driven parquet backtester."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from neo_trader.backtest.event_driven import (
    BacktestEventType,
    EventDrivenBacktestConfig,
    EventDrivenBacktester,
    OrderBookFeatureConfig,
    ParquetMarketDataReader,
    SimulatedBookExecutor,
    SimulatedExecutionSide,
)
from neo_trader.data.market_data_recorder import (
    MarketDataEventType,
    ParquetRawEventWriter,
    RawMarketDataEvent,
)
from neo_trader.strategy.opening_range_book_momentum import (
    OpeningRangeBookMomentumConfig,
)


def test_event_driven_backtester_replays_parquet_and_exports_report(tmp_path: Path) -> None:
    raw_root = tmp_path / "data" / "raw"
    _write_backtest_events(raw_root)
    backtester = EventDrivenBacktester(
        strategy_config=_strategy_config(),
        config=EventDrivenBacktestConfig(
            fixed_quantity=Decimal("1"),
            feature_config=OrderBookFeatureConfig(
                depth_levels=2,
                weighted_imbalance_lambda=Decimal("0.5"),
                wall_distance_bps=Decimal("5"),
            ),
        ),
    )

    report = backtester.run_from_parquet(raw_root)

    assert report.metrics.total_fills == 2
    assert report.metrics.total_trades == 1
    assert report.metrics.closed_trades == 1
    assert report.metrics.total_pnl == Decimal("1.99")
    assert report.metrics.max_mae < 0
    assert report.metrics.max_mfe > 0
    assert report.trades[0].entry_price == Decimal("102.01")
    assert report.trades[0].exit_price == Decimal("104")
    assert report.trades[0].entry_reason_codes == (
        "LONG_BREAKOUT",
        "ORDERBOOK_CONFIRMATION",
    )
    assert report.trades[0].exit_reason_codes == ("TAKE_PROFIT",)

    csv_paths = report.export_csv(tmp_path / "report_csv")
    html_path = tmp_path / "report.html"
    report.to_html(html_path)
    compact_csv_path = tmp_path / "trades.csv"
    report.to_csv(compact_csv_path)

    assert csv_paths.metrics_path.read_text(encoding="utf-8").startswith("metric,value")
    assert "instrument_uid,side,entry_time" in csv_paths.trades_path.read_text(encoding="utf-8")
    assert "requested_quantity,filled_quantity" in csv_paths.fills_path.read_text(
        encoding="utf-8"
    )
    assert "neo_trader backtest report" in html_path.read_text(encoding="utf-8")
    assert "UID1,LONG" in compact_csv_path.read_text(encoding="utf-8")


def test_parquet_reader_reads_and_sorts_recorder_partitions(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    _write_backtest_events(raw_root)

    events = ParquetMarketDataReader().read(raw_root)

    assert len(events) == 7
    assert events[0].event_type is BacktestEventType.CANDLES
    assert events[-1].event_type is BacktestEventType.ORDERBOOK
    assert list(events) == sorted(events, key=lambda event: event.timestamp)


def test_simulated_book_executor_reports_partial_fill() -> None:
    fill = SimulatedBookExecutor().fill(
        book={
            "bids": [["99", "2"]],
            "asks": [["100", "2"]],
        },
        side=SimulatedExecutionSide.BUY,
        quantity=Decimal("5"),
        timestamp=datetime(2026, 7, 6, 10, 0, tzinfo=UTC),
        instrument_uid="UID1",
        reason="TEST",
    )

    assert fill.partial is True
    assert fill.requested_quantity == Decimal("5")
    assert fill.filled_quantity == Decimal("2")
    assert fill.avg_price == Decimal("100")
    assert fill.slippage_bps > 0


def _strategy_config() -> OpeningRangeBookMomentumConfig:
    return OpeningRangeBookMomentumConfig(
        opening_range_candle_count=2,
        breakout_buffer_bps=Decimal("1"),
        max_spread_bps=Decimal("10"),
        min_imbalance=Decimal("0.15"),
        min_weighted_imbalance=Decimal("0.10"),
        min_microprice_edge_bps=Decimal("0.2"),
        min_confidence=Decimal("0.50"),
        atr_window=2,
        take_profit_r_multiples=(Decimal("0.2"),),
    )


def _write_backtest_events(raw_root: Path) -> None:
    writer = ParquetRawEventWriter(root=raw_root, flush_rows=1)
    start = datetime(2026, 7, 6, 10, 0, tzinfo=UTC)
    events = [
        _event(start, MarketDataEventType.CANDLES, {"candle": _candle(start, "100", "99", "99.5")}),
        _event(
            start + timedelta(minutes=5),
            MarketDataEventType.CANDLES,
            {"candle": _candle(start + timedelta(minutes=5), "101", "99", "100")},
        ),
        _event(
            start + timedelta(minutes=35),
            MarketDataEventType.CANDLES,
            {"candle": _candle(start + timedelta(minutes=35), "102.2", "101.2", "102")},
        ),
        _event(
            start + timedelta(minutes=35, seconds=1),
            MarketDataEventType.ORDERBOOK,
            {
                "orderbook": {
                    "bids": [["101.99", "100"], ["101.90", "100"]],
                    "asks": [["102.01", "10"], ["102.05", "10"]],
                }
            },
        ),
        _event(
            start + timedelta(minutes=35, seconds=30),
            MarketDataEventType.TRADES,
            {"trade": {"price": "103", "quantity": "5", "side": "BUY"}},
        ),
        _event(
            start + timedelta(minutes=36),
            MarketDataEventType.CANDLES,
            {"candle": _candle(start + timedelta(minutes=36), "104.2", "103.5", "104")},
        ),
        _event(
            start + timedelta(minutes=36, seconds=1),
            MarketDataEventType.ORDERBOOK,
            {
                "orderbook": {
                    "bids": [["104", "100"], ["103.95", "100"]],
                    "asks": [["104.02", "10"], ["104.08", "10"]],
                }
            },
        ),
    ]
    for sequence, event in enumerate(events, start=1):
        writer.write(
            RawMarketDataEvent.from_payload(
                instrument_uid="UID1",
                event_type=event.event_type,
                event_time=event.event_time,
                received_at=event.event_time,
                payload=event.payload,
                sequence=sequence,
            )
        )
    writer.close()


def _event(
    event_time: datetime,
    event_type: MarketDataEventType,
    payload: dict[str, object],
) -> RawMarketDataEvent:
    return RawMarketDataEvent.from_payload(
        instrument_uid="UID1",
        event_type=event_type,
        event_time=event_time,
        received_at=event_time,
        payload=payload,
    )


def _candle(timestamp: datetime, high: str, low: str, close: str) -> dict[str, str]:
    return {
        "timestamp": timestamp.isoformat(),
        "high": high,
        "low": low,
        "close": close,
    }
