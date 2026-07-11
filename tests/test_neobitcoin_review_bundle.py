from __future__ import annotations

import json
import tarfile
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import zstandard

from neo_trader.neobitcoin_research.archive import ArchiveValidationError
from neo_trader.neobitcoin_research.review_bundle import (
    DATASETS,
    _enrich_candle_features,
    create_neobitcoin_review_bundle,
)
from neo_trader.neobitcoin_research.review_datasets import build_review_datasets
from neo_trader.neobitcoin_research.review_raw import COMMON_FIELDS, RAW_SCHEMAS


def _common(event_id: str, when: datetime) -> dict[str, object]:
    return {
        "schema_version": "schema-v3",
        "event_id": event_id,
        "instrument_uid": "uid-neobitcoin",
        "instrument_ticker": "NEOBITCOIN",
        "exchange_ts": when,
        "receive_ts": when,
        "processing_ts": when,
        "session_id": "session-1",
        "collector_instance_id": "collector-1",
        "source": "test",
        "code_commit": "abc123",
        "config_hash": "config123",
        "data_quality_flags": [],
    }


def _source(root: Path, *, token: str | None = None) -> None:
    start = datetime(2026, 7, 11, 10, 50, tzinfo=UTC)
    candidate_ts = start + timedelta(minutes=10)
    support_end = candidate_ts + timedelta(minutes=30)
    level = [{"price": 99.9, "quantity": 10.0}]
    raw: dict[str, pa.Table] = {}
    books = []
    path = []
    for index in range(0, 42):
        when = start + timedelta(minutes=index)
        mid = 100.0 + index * 0.01
        path.append({"exchange_ts": when, "mid_price": mid})
        books.append(
            _common(f"book-{index}", when)
            | {
                "revision": index,
                "depth": 50,
                "best_bid": mid - 0.1,
                "best_ask": mid + 0.1,
                "mid_price": mid,
                "spread_price": 0.2,
                "spread_percent": 0.2,
                "spread_ticks": 2.0,
                "trading_status": "NORMAL_TRADING",
                "feed_latency_ms": 1.0,
                "is_duplicate_snapshot": False,
                "previous_event_id": None if index == 0 else f"book-{index - 1}",
                "bids": level,
                "asks": [{"price": 100.1, "quantity": 10.0}],
            }
        )
    path = [
        {"exchange_ts": start + timedelta(seconds=index), "mid_price": 100 + index / 100_000}
        for index in range(42 * 60)
    ]
    raw["raw_orderbook"] = pa.Table.from_pylist(books, schema=RAW_SCHEMAS["raw_orderbook"])
    trade_rows = [
        _common(f"trade-{index}", start + timedelta(minutes=index))
        | {
            "trade_id": f"trade-{index}",
            "price": 99.9 if index % 2 else 100.1,
            "quantity": 1.0,
            "direction": "SELL" if index % 2 else "BUY",
            "aggressor_side": "SELL" if index % 2 else "BUY",
            "feed_latency_ms": 1.0,
        }
        for index in range(42)
    ]
    trade_rows.append(
        _common("trade-partial", candidate_ts + timedelta(seconds=1))
        | {
            "trade_id": "trade-partial",
            "price": 99.9,
            "quantity": 12.0,
            "direction": "SELL",
            "aggressor_side": "SELL",
            "feed_latency_ms": 1.0,
        }
    )
    raw["raw_trades"] = pa.Table.from_pylist(trade_rows, schema=RAW_SCHEMAS["raw_trades"])
    raw["raw_last_price"] = pa.Table.from_pylist(
        [
            _common(f"last-{index}", start + timedelta(minutes=index))
            | {"last_price": 100.0 + index * 0.01, "feed_latency_ms": 1.0}
            for index in range(42)
        ],
        schema=RAW_SCHEMAS["raw_last_price"],
    )
    raw["market_status_events"] = pa.Table.from_pylist(
        [
            _common("status-1", candidate_ts)
            | {
                "status_type": "trading_status",
                "trading_status": "NORMAL_TRADING",
                "connection_state": "connected",
                "market_order_available": True,
                "limit_order_available": True,
            }
        ],
        schema=RAW_SCHEMAS["market_status_events"],
    )
    for minutes in (1, 5, 15):
        when = start - timedelta(hours=6)
        raw[f"candles_{minutes}m"] = pa.Table.from_pylist(
            [
                _common(f"candle-{minutes}", when)
                | {
                    "candle_start": when,
                    "candle_end": when + timedelta(minutes=minutes),
                    "open": 99.0,
                    "high": 101.0,
                    "low": 98.0,
                    "close": 100.0,
                    "volume": 1000.0,
                    "is_complete": True,
                    "source_timeframe": f"{minutes}m",
                    "is_backfilled": True,
                }
            ],
            schema=RAW_SCHEMAS[f"candles_{minutes}m"],
        )
    feature = _common("feature-event", candidate_ts) | {
        "feature_snapshot_id": "feature-1",
        "best_bid": 99.9,
        "best_ask": 100.1,
        "mid_price": 100.0,
        "spread_ticks": 2.0,
        "spread_percent": 0.2,
        "microprice": 100.0,
        "realized_volatility": 0.01,
        "atr_1m": 0.1,
        "atr_5m": 0.2,
        "atr_15m": 0.3,
        "candle_volume_1m": 100.0,
        "candle_volume_5m": 500.0,
        "candle_volume_15m": 1500.0,
        "bid_price_01": 99.9,
        "bid_quantity_01": 10.0,
        "ask_price_01": 100.1,
        "ask_quantity_01": 10.0,
        "feature_ready": True,
        "raw_signal": "LONG",
    }
    research = build_review_datasets(
        [feature],
        path,
        candidate_start=start,
        candidate_end=candidate_ts,
        orderbook_rows=books,
        trade_rows=trade_rows,
    )
    if token:
        candidates = research["candidate_events"].to_pylist()
        candidates[0]["rejection_reason"] = token
        research["candidate_events"] = pa.Table.from_pylist(
            candidates, schema=research["candidate_events"].schema
        )
    quality_schema = pa.schema(
        [
            *COMMON_FIELDS,
            pa.field("event_type", pa.string(), nullable=False),
            pa.field("severity", pa.string(), nullable=False),
            pa.field("gap_seconds", pa.float64()),
            pa.field("details", pa.string()),
        ]
    )
    raw["data_quality_events"] = pa.Table.from_pylist([], schema=quality_schema)
    all_tables = raw | research
    assert set(all_tables) == set(DATASETS)
    for dataset, table in all_tables.items():
        target = root / dataset
        target.mkdir(parents=True)
        pq.write_table(table, target / "part.parquet", compression="zstd")
    assert support_end == start + timedelta(minutes=40)


def test_review_bundle_has_twenty_verified_members(tmp_path: Path) -> None:
    source = tmp_path / "review_parquet"
    _source(source)

    result = create_neobitcoin_review_bundle(
        tmp_path,
        now=datetime(2026, 7, 11, 12, 0, tzinfo=UTC),
    )

    assert result.file_count == 20
    assert result.archive_path.name.startswith("neobitcoin_review_10m_")
    assert result.sha256_path.read_text(encoding="utf-8").startswith(result.archive_sha256)
    with tempfile.TemporaryDirectory() as directory:
        with (
            result.archive_path.open("rb") as raw,
            zstandard.ZstdDecompressor().stream_reader(raw) as stream,
            tarfile.open(fileobj=stream, mode="r|") as archive,
        ):
            archive.extractall(directory, filter="data")
        files = [path for path in Path(directory).rglob("*") if path.is_file()]
        assert len(files) == 20
        manifest = json.loads((Path(directory) / "MANIFEST.json").read_text(encoding="utf-8"))
        assert manifest["validation"] == "PASS"
        assert manifest["dataset_file_count"] == 15
        assert manifest["rows_by_dataset"]["future_outcomes"] == 54
        assert {
            item["dataset"] for item in manifest["files"] if item["path"].endswith(".parquet")
        } == set(DATASETS)
        assert len(manifest["files"]) == 19


def test_review_bundle_rejects_token_material(tmp_path: Path) -> None:
    source = tmp_path / "review_parquet"
    token = "t.sensitive-review-token-1234567890"
    _source(source, token=token)
    token_file = tmp_path / "desktop.txt"
    token_file.write_text(token, encoding="utf-8")

    with pytest.raises(ArchiveValidationError, match="secret material"):
        create_neobitcoin_review_bundle(
            tmp_path,
            now=datetime(2026, 7, 11, 12, 0, tzinfo=UTC),
            token_files=(token_file,),
        )

    assert not list((tmp_path / "review_bundles").glob("*.tar.zst"))


def test_candle_features_are_completed_and_lookahead_safe(tmp_path: Path) -> None:
    feature_ts = datetime(2026, 7, 11, 12, 0, tzinfo=UTC)
    paths: dict[str, Path] = {}
    for minutes in (1, 5, 15):
        candles = []
        for index in range(15):
            start = feature_ts - timedelta(minutes=minutes * (15 - index))
            candles.append(
                _common(f"candle-{minutes}-{index}", start)
                | {
                    "candle_start": start,
                    "candle_end": start + timedelta(minutes=minutes),
                    "open": 100.0 + index,
                    "high": 102.0 + index,
                    "low": 99.0 + index,
                    "close": 101.0 + index,
                    "volume": 1000.0 + index,
                    "is_complete": True,
                    "source_timeframe": f"{minutes}m",
                    "is_backfilled": True,
                }
            )
        path = tmp_path / f"candles_{minutes}m.parquet"
        pq.write_table(
            pa.Table.from_pylist(candles, schema=RAW_SCHEMAS[f"candles_{minutes}m"]), path
        )
        paths[f"candles_{minutes}m"] = path

    features: list[dict[str, object]] = [{"timestamp": feature_ts}]
    _enrich_candle_features(features, paths)

    assert features[0]["feature_ready"] is True
    assert features[0]["missing_reason"] is None
    for minutes in (1, 5, 15):
        assert features[0][f"atr_{minutes}m"] == pytest.approx(3.0)
        assert features[0][f"candle_volume_{minutes}m"] == 1014.0
