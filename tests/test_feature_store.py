"""Tests for offline feature-store generation."""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pyarrow.parquet as pq

from neo_trader.data.market_data_recorder import (
    MarketDataEventType,
    ParquetRawEventWriter,
    RawMarketDataEvent,
)
from neo_trader.research.feature_store import (
    FEATURE_COLUMNS,
    FeatureStoreConfig,
    build_feature_store,
)


def test_feature_store_builds_from_synthetic_raw_parquet(tmp_path: Path) -> None:
    raw_path = tmp_path / "raw"
    output_path = tmp_path / "features"
    active_universe = _active_universe(tmp_path)
    _write_synthetic_raw(raw_path)

    result = build_feature_store(
        raw_path=raw_path,
        output_path=output_path,
        active_universe_path=active_universe,
        config=FeatureStoreConfig(expected_fill_quantity=Decimal("10")),
    )

    assert result.rows_written == 2
    assert len(result.files_written) == 1
    rows = _read_rows(result.files_written[0])
    assert set(FEATURE_COLUMNS).issubset(rows[0])
    assert rows[0]["instrument"] == "SBER"
    assert rows[0]["usable_row"] is True
    assert rows[0]["mid"] > 0
    assert "date=20260707" in result.files_written[0].parts


def test_non_finite_features_are_marked_unusable(tmp_path: Path) -> None:
    raw_path = tmp_path / "raw"
    output_path = tmp_path / "features"
    active_universe = _active_universe(tmp_path)
    writer = ParquetRawEventWriter(root=raw_path, flush_rows=1)
    timestamp = datetime(2026, 7, 7, 7, 0, tzinfo=UTC)
    writer.write(
        RawMarketDataEvent.from_payload(
            instrument_uid="UID1",
            event_type=MarketDataEventType.ORDERBOOK,
            received_at=timestamp,
            event_time=timestamp,
            payload={
                "bids": [{"price": "NaN", "quantity": "10"}],
                "asks": [{"price": "100.10", "quantity": "10"}],
            },
        )
    )
    writer.close()

    result = build_feature_store(
        raw_path=raw_path,
        output_path=output_path,
        active_universe_path=active_universe,
        config=FeatureStoreConfig(expected_fill_quantity=Decimal("1")),
    )

    rows = _read_rows(result.files_written[0])
    assert rows[0]["usable_row"] is False
    assert rows[0]["unusable_reason"]
    for key in FEATURE_COLUMNS:
        value = rows[0].get(key)
        if isinstance(value, float):
            assert math.isfinite(value)


def _write_synthetic_raw(raw_path: Path) -> None:
    writer = ParquetRawEventWriter(root=raw_path, flush_rows=1)
    base = datetime(2026, 7, 7, 7, 0, tzinfo=UTC)
    events = [
        RawMarketDataEvent.from_payload(
            instrument_uid="UID1",
            event_type=MarketDataEventType.CANDLES,
            received_at=base,
            event_time=base,
            payload={
                "open": "100",
                "high": "100.5",
                "low": "99.5",
                "close": "100",
                "volume": "1000",
            },
        ),
        RawMarketDataEvent.from_payload(
            instrument_uid="UID1",
            event_type=MarketDataEventType.TRADES,
            received_at=base + timedelta(seconds=1),
            event_time=base + timedelta(seconds=1),
            payload={"price": "100.02", "quantity": "25"},
        ),
        RawMarketDataEvent.from_payload(
            instrument_uid="UID1",
            event_type=MarketDataEventType.ORDERBOOK,
            received_at=base + timedelta(seconds=2),
            event_time=base + timedelta(seconds=2),
            payload=_book_payload("100.00", "100.10"),
        ),
        RawMarketDataEvent.from_payload(
            instrument_uid="UID1",
            event_type=MarketDataEventType.ORDERBOOK,
            received_at=base + timedelta(seconds=3),
            event_time=base + timedelta(seconds=3),
            payload=_book_payload("100.01", "100.11"),
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


def _active_universe(tmp_path: Path) -> Path:
    path = tmp_path / "active_universe.yaml"
    path.write_text(
        "\n".join(
            (
                "generated_by: test",
                "instruments:",
                "- ticker: SBER",
                "  uid: UID1",
                "  enabled: true",
                "",
            )
        ),
        encoding="utf-8",
    )
    return path


def _read_rows(path: Path) -> list[dict[str, Any]]:
    return cast(list[dict[str, Any]], pq.ParquetFile(path).read().to_pylist())
