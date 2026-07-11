from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from neo_trader.neobitcoin_research.derived_schemas import (
    derived_arrow_schema,
    flatten_derived_row,
)
from neo_trader.neobitcoin_research.storage import DERIVED_DATASETS, ResearchStorage

BASE = datetime(2026, 7, 11, 12, 0, tzinfo=UTC)


def test_every_derived_dataset_has_stable_typed_schema_and_diagnostic_json() -> None:
    for dataset in DERIVED_DATASETS:
        schema = derived_arrow_schema(dataset)
        assert len(schema.names) == len(set(schema.names))
        assert schema.field("event_id").type == pa.string()
        assert schema.field("record_json").type == pa.string()

    features = derived_arrow_schema("feature_snapshots")
    assert features.field("midprice").type == pa.float64()
    assert features.field("imbalance_20").type == pa.float64()
    assert features.field("trades_60s_buy_count").type == pa.int64()
    assert features.field("gap_before").type == pa.bool_()

    outcomes = derived_arrow_schema("future_outcomes")
    assert outcomes.field("realized_net_pnl_rub").type == pa.float64()
    assert outcomes.field("horizon_seconds").type == pa.int64()
    assert outcomes.field("entry_average_price").type == pa.float64()


def test_flattened_row_converts_decimal_strings_and_keeps_full_record() -> None:
    record = {
        "timestamp": BASE.isoformat(),
        "candidate_id": "candidate-1",
        "horizon_seconds": 30,
        "latency_ms": "250",
        "realized_net_pnl_rub": "12.375",
        "mfe_rub": "17.5",
        "mae_rub": "-3.25",
        "profitable_after_costs": True,
        "entry": {"average_price": "63748.9", "filled_quantity": "1"},
        "price_path": [{"timestamp": BASE.isoformat(), "midprice": "63746.25"}],
    }
    row = flatten_derived_row(
        "future_outcomes",
        event_id="outcome-1",
        recorded_at=BASE.isoformat(),
        schema_version="1",
        record=record,
        record_json=json.dumps(record),
    )

    assert row["horizon_seconds"] == 30
    assert row["latency_ms"] == 250
    assert row["realized_net_pnl_rub"] == 12.375
    assert row["entry_average_price"] == 63748.9
    assert row["profitable_after_costs"] is True
    assert json.loads(row["record_json"])["price_path"] == record["price_path"]


def test_buffered_storage_publishes_queryable_typed_columns(tmp_path: Path) -> None:
    root = tmp_path / "data"
    record = {
        "event_id": "feature-1",
        "timestamp": BASE,
        "instrument_uid": "uid-1",
        "schema_version": "2",
        "midprice": "63746.25",
        "spread_bps": 0.8314,
        "imbalance_20": "0.125",
        "trades_60s_buy_count": 7,
        "gap_before": False,
        "diagnostics": {"warmup": "complete"},
    }

    with ResearchStorage(root, fsync=False) as storage:
        result = storage.append_feature_snapshot(record)
        assert not result.parquet_path.exists()
        storage.finalize_derived_writers()

    table = pq.read_table(result.parquet_path)
    assert table.schema.field("midprice").type == pa.float64()
    assert table.schema.field("trades_60s_buy_count").type == pa.int64()
    row = table.to_pylist()[0]
    assert row["midprice"] == 63746.25
    assert row["imbalance_20"] == 0.125
    assert row["trades_60s_buy_count"] == 7
    assert json.loads(row["record_json"])["diagnostics"] == {"warmup": "complete"}
