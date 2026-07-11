from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from neo_trader.neobitcoin_research.review_raw import RAW_SCHEMAS, extract_review_raw

BASE = datetime(2026, 7, 11, 12, 0, tzinfo=UTC)
UID = "neo-uid"


def test_extracts_mature_window_to_one_typed_parquet_per_raw_dataset(tmp_path: Path) -> None:
    source = tmp_path / "active" / "raw" / UID / "events.jsonl"
    source.parent.mkdir(parents=True)
    records: list[dict[str, object]] = []
    # Enough receive-time coverage for a mature 10m window plus 30m support.
    for minute in range(51):
        received = BASE + timedelta(minutes=minute)
        records.append(_event(f"book-{minute}", "orderbook", received, _book()))
        if minute % 2 == 0:
            records.append(_event(f"trade-{minute}", "trade", received, _trade()))
        if minute % 3 == 0:
            records.append(_event(f"last-{minute}", "last_price", received, _last()))
    records.append(_event("status", "trading_status", BASE + timedelta(minutes=5), _status()))
    for interval, minutes in ((1, 1), (2, 5), (3, 15)):
        records.append(
            _event(
                f"candle-{minutes}",
                "candle",
                BASE + timedelta(minutes=1),
                _candle(interval, BASE + timedelta(minutes=10) - timedelta(hours=6)),
                exchange=BASE + timedelta(minutes=10) - timedelta(hours=6),
            )
        )
    source.write_text("".join(json.dumps(row) + "\n" for row in records), encoding="utf-8")

    result = extract_review_raw(
        tmp_path / "active" / "raw",
        tmp_path / "review",
        now=BASE + timedelta(hours=2),
        critical_gap_seconds=61,
        code_commit="abc123",
        config_hash="cfg123",
    )

    assert result.window.candidate_start == BASE + timedelta(minutes=9)
    assert result.window.candidate_end == BASE + timedelta(minutes=19)
    assert result.window.support_end == BASE + timedelta(minutes=49)
    assert set(result.paths) == set(RAW_SCHEMAS)
    assert len(list((tmp_path / "review" / "data").glob("*.parquet"))) == 7
    for dataset, path in result.paths.items():
        table = pq.read_table(path)
        assert table.schema == RAW_SCHEMAS[dataset]
        assert pa.types.is_timestamp(table.schema.field("receive_ts").type)
        assert table.schema.field("receive_ts").type.tz == "UTC"

    book_table = pq.read_table(result.paths["raw_orderbook"])
    assert pa.types.is_list(book_table.schema.field("bids").type)
    assert pa.types.is_struct(book_table.schema.field("bids").type.value_type)
    assert book_table.column("depth")[0].as_py() == 2
    assert book_table.column("bids")[0].as_py()[0] == {"price": 100.0, "quantity": 5.0}
    assert "payload_json" not in book_table.column_names
    assert result.rows_by_dataset["raw_orderbook"] >= 42
    assert result.rows_by_dataset["raw_trades"] >= 25
    assert result.rows_by_dataset["raw_last_price"] >= 14
    assert result.rows_by_dataset["candles_1m"] == 1


def test_skips_latest_candidate_window_containing_reconnect(tmp_path: Path) -> None:
    source = tmp_path / "events.jsonl"
    records: list[dict[str, object]] = []
    for minute in range(61):
        received = BASE + timedelta(minutes=minute)
        records.append(_event(f"book-{minute}", "orderbook", received, _book()))
        records.append(_event(f"trade-{minute}", "trade", received, _trade()))
        records.append(_event(f"last-{minute}", "last_price", received, _last()))
    # Latest mature candidate is [12:20, 12:30]; force minute-wise fallback.
    records.append(_event("reconnect", "reconnect", BASE + timedelta(minutes=25), {}))
    source.write_text("".join(json.dumps(row) + "\n" for row in records), encoding="utf-8")

    result = extract_review_raw(
        source,
        tmp_path / "review",
        now=BASE + timedelta(hours=2),
        critical_gap_seconds=61,
    )

    assert result.window.candidate_end == BASE + timedelta(minutes=24)
    assert result.window.candidate_start == BASE + timedelta(minutes=14)


def _event(
    event_id: str,
    event_type: str,
    receive: datetime,
    nested: dict[str, object],
    *,
    exchange: datetime | None = None,
) -> dict[str, object]:
    exchange = exchange or receive - timedelta(milliseconds=250)
    return {
        "event_id": event_id,
        "instrument_uid": UID,
        "ticker": "BTCUSDperpA",
        "event_type": event_type,
        "exchange_timestamp": exchange.isoformat(),
        "receive_timestamp": receive.isoformat(),
        "latency_ms": 250.0,
        "stream_id": "session-1",
        "source": "tbank_market_data_stream",
        "is_consistent": True,
        "connection_state": "connected",
        "payload": {event_type: nested},
    }


def _book() -> dict[str, object]:
    return {
        "depth": 2,
        "bids": [
            {"price": {"units": 100, "nano": 0}, "quantity": 5},
            {"price": {"units": 99, "nano": 0}, "quantity": 4},
        ],
        "asks": [
            {"price": {"units": 101, "nano": 0}, "quantity": 6},
            {"price": {"units": 102, "nano": 0}, "quantity": 7},
        ],
    }


def _trade() -> dict[str, object]:
    return {"price": {"units": 101, "nano": 0}, "quantity": 2, "direction": 1}


def _last() -> dict[str, object]:
    return {"price": {"units": 101, "nano": 0}}


def _status() -> dict[str, object]:
    return {
        "trading_status": 5,
        "market_order_available_flag": True,
        "limit_order_available_flag": True,
    }


def _candle(interval: int, start: datetime) -> dict[str, object]:
    price = {"units": 100, "nano": 500_000_000}
    return {
        "interval": interval,
        "time": start.isoformat(),
        "open": price,
        "high": {"units": 102, "nano": 0},
        "low": {"units": 99, "nano": 0},
        "close": {"units": 101, "nano": 0},
        "volume": 123,
    }
