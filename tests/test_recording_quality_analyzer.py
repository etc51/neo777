"""Tests for recording quality analyzer outputs."""

from __future__ import annotations

import importlib
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import yaml

from neo_trader.data.market_data_recorder import (
    MarketDataEventType,
    ParquetRawEventWriter,
    RawMarketDataEvent,
)

analyzer = importlib.import_module("scripts.analyze_recording_quality")


def test_analyzer_writes_liquidity_reports_and_active_universe(tmp_path: Path) -> None:
    raw_path = tmp_path / "raw"
    reports_dir = tmp_path / "reports"
    active_universe = tmp_path / "configs" / "active_universe.yaml"
    instruments_config = tmp_path / "configs" / "instruments.yaml"
    instruments_config.parent.mkdir(parents=True)
    instruments_config.write_text(
        "\n".join(
            (
                "instruments:",
                "  - ticker: SBER",
                "    class_code: TQBR",
                "    uid: UID1",
                "    enabled: true",
                "",
            )
        ),
        encoding="utf-8",
    )
    _write_synthetic_parquet(raw_path)

    report = analyzer.analyze_recording_quality(
        raw_path=raw_path,
        reports_dir=reports_dir,
        active_universe_path=active_universe,
        instruments_config=instruments_config,
        test_sizes=[Decimal("10")],
        gap_threshold_seconds=Decimal("30"),
        top_n=10,
    )

    json_path = Path(str(report["json_path"]))
    csv_path = Path(str(report["csv_path"]))
    assert json_path.exists()
    assert csv_path.exists()
    assert active_universe.exists()

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert "UNCONFIGURED" not in payload["instruments"]
    instrument = payload["instruments"]["UID1"]
    assert instrument["ticker"] == "SBER"
    assert instrument["rows_by_type"] == {"orderbook": 2, "trades": 1, "candles": 2}
    assert Decimal(instrument["events_per_minute"]) > 0
    assert instrument["spread_bps"]["p50"] is not None
    assert instrument["expected_slippage_bps"]["10"]["buy_p50"] is not None
    assert payload["ranked_universe"][0]["instrument_uid"] == "UID1"
    assert "score" in csv_path.read_text(encoding="utf-8")

    active_payload = yaml.safe_load(active_universe.read_text(encoding="utf-8"))
    assert active_payload["instruments"][0]["uid"] == "UID1"
    assert active_payload["instruments"][0]["ticker"] == "SBER"


def _write_synthetic_parquet(raw_path: Path) -> None:
    writer = ParquetRawEventWriter(root=raw_path, flush_rows=1)
    received_at = datetime(2026, 7, 7, 10, 0, tzinfo=UTC)
    events = [
        RawMarketDataEvent.from_payload(
            instrument_uid="UID1",
            event_type=MarketDataEventType.ORDERBOOK,
            received_at=received_at,
            event_time=received_at,
            payload=_book_payload("100", "100.10"),
        ),
        RawMarketDataEvent.from_payload(
            instrument_uid="UID1",
            event_type=MarketDataEventType.TRADES,
            received_at=received_at + timedelta(seconds=1),
            event_time=received_at + timedelta(seconds=1),
            payload={"price": "100.05", "quantity": "25"},
        ),
        RawMarketDataEvent.from_payload(
            instrument_uid="UID1",
            event_type=MarketDataEventType.CANDLES,
            received_at=received_at + timedelta(seconds=2),
            event_time=received_at + timedelta(seconds=2),
            payload={"open": "100", "high": "101", "low": "99", "close": "100.5", "volume": "1000"},
        ),
        RawMarketDataEvent.from_payload(
            instrument_uid="UID1",
            event_type=MarketDataEventType.ORDERBOOK,
            received_at=received_at + timedelta(seconds=3),
            event_time=received_at + timedelta(seconds=3),
            payload=_book_payload("100.01", "100.11"),
        ),
        RawMarketDataEvent.from_payload(
            instrument_uid="UID1",
            event_type=MarketDataEventType.CANDLES,
            received_at=received_at + timedelta(seconds=4),
            event_time=received_at + timedelta(seconds=4),
            payload={
                "open": "100.5",
                "high": "101",
                "low": "100",
                "close": "100.8",
                "volume": "1100",
            },
        ),
        RawMarketDataEvent.from_payload(
            instrument_uid="UNCONFIGURED",
            event_type=MarketDataEventType.ORDERBOOK,
            received_at=received_at + timedelta(seconds=5),
            event_time=received_at + timedelta(seconds=5),
            payload=_book_payload("10", "10.10"),
        ),
    ]
    for event in events:
        writer.write(event)
    writer.close()


def _book_payload(best_bid: str, best_ask: str) -> dict[str, object]:
    return {
        "bids": [
            {"price": best_bid, "quantity": "20"},
            {"price": "99.90", "quantity": "30"},
            {"price": "99.80", "quantity": "40"},
            {"price": "99.70", "quantity": "50"},
            {"price": "99.60", "quantity": "60"},
            {"price": "99.50", "quantity": "70"},
            {"price": "99.40", "quantity": "80"},
            {"price": "99.30", "quantity": "90"},
            {"price": "99.20", "quantity": "100"},
            {"price": "99.10", "quantity": "110"},
        ],
        "asks": [
            {"price": best_ask, "quantity": "20"},
            {"price": "100.20", "quantity": "30"},
            {"price": "100.30", "quantity": "40"},
            {"price": "100.40", "quantity": "50"},
            {"price": "100.50", "quantity": "60"},
            {"price": "100.60", "quantity": "70"},
            {"price": "100.70", "quantity": "80"},
            {"price": "100.80", "quantity": "90"},
            {"price": "100.90", "quantity": "100"},
            {"price": "101.00", "quantity": "110"},
        ],
    }
