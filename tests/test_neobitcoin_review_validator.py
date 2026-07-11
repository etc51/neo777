from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
from test_neobitcoin_review_bundle import _source

from neo_trader.neobitcoin_research.review_bundle import (
    DATASETS,
    _filter_time_with_prefix,
    _read_dataset,
    _validate_tables,
    _validation_report,
)


def _fixture_tables(tmp_path: Path):  # type: ignore[no-untyped-def]
    source = tmp_path / "review_parquet"
    _source(source)
    return {name: _read_dataset(source, name) for name in DATASETS}


def test_validator_rejects_incomplete_raw_preroll_and_future_coverage(
    tmp_path: Path,
) -> None:
    tables = _fixture_tables(tmp_path)
    t0 = datetime(2026, 7, 11, 10, 50, tzinfo=UTC)
    t1 = t0 + timedelta(minutes=10)

    result = _validate_tables(
        tables,
        t0,
        t1,
        t0 - timedelta(hours=6),
        t1 + timedelta(minutes=30),
        [],
    )
    statuses = {item["check"]: item["status"] for item in result["checks"]}

    assert result["status"] == "FAIL"
    assert statuses["raw_orderbook_coverage"] == "FAIL"
    assert statuses["raw_trades_coverage"] == "FAIL"
    assert statuses["raw_last_price_coverage"] == "FAIL"
    assert result["raw_coverage"]["raw_trades"]["start"] == t0.isoformat()


def test_validator_exposes_temporal_feature_and_realism_failures(tmp_path: Path) -> None:
    tables = _fixture_tables(tmp_path)
    t0 = datetime(2026, 7, 11, 10, 50, tzinfo=UTC)
    t1 = t0 + timedelta(minutes=10)
    executions = tables["execution_simulations"].to_pylist()
    for row in executions:
        if str(row["execution_model"]).startswith("passive"):
            row["fill_status"] = "FULL_FILL"
            row["fill_delay_ms"] = 100
            row["fill_ts"] = row["order_ts"] + timedelta(milliseconds=100)
            row["filled_quantity"] = row["requested_quantity"]
            row["remaining_quantity"] = 0.0
            row["fill_price"] = row["order_price"]
    tables["execution_simulations"] = pa.Table.from_pylist(
        executions, schema=tables["execution_simulations"].schema
    )
    features = tables["feature_snapshots"].to_pylist()
    for row in features:
        for name in (
            "trade_flow_10s",
            "trade_flow_180s",
            "trade_flow_300s",
            "trade_flow_900s",
            "data_age_ms",
        ):
            row[name] = None
        row["feature_ready"] = True
    tables["feature_snapshots"] = pa.Table.from_pylist(
        features, schema=tables["feature_snapshots"].schema
    )
    exits = tables["shadow_exit_results"].to_pylist()
    baseline = {}
    copied = (
        "activation_ts",
        "exit_ts",
        "exit_price",
        "exit_reason",
        "trigger_value",
        "trigger_threshold",
    )
    for row in exits:
        source = baseline.setdefault(row["simulation_id"], row)
        for name in copied:
            row[name] = source[name]
    tables["shadow_exit_results"] = pa.Table.from_pylist(
        exits, schema=tables["shadow_exit_results"].schema
    )

    result = _validate_tables(
        tables,
        t0,
        t1,
        t0 - timedelta(hours=6),
        t1 + timedelta(minutes=30),
        [],
    )
    statuses = {item["check"]: item["status"] for item in result["checks"]}

    assert statuses["passive_not_all_full_fill"] == "FAIL"
    assert statuses["passive_has_no_fill"] == "FAIL"
    assert statuses["required_features_not_all_null"] == "FAIL"
    assert statuses["feature_ready_has_no_required_nulls"] == "FAIL"
    assert statuses["exit_models_independent"] == "FAIL"


def test_validation_report_has_detailed_check_table(tmp_path: Path) -> None:
    tables = _fixture_tables(tmp_path)
    t0 = datetime(2026, 7, 11, 10, 50, tzinfo=UTC)
    result = _validate_tables(
        tables,
        t0,
        t0 + timedelta(minutes=10),
        t0 - timedelta(hours=6),
        t0 + timedelta(minutes=40),
        [],
    )

    report = _validation_report(result, tables)

    assert "| Check | Status | Measured value | Threshold | Explanation |" in report
    assert "raw_trades_coverage" in report
    assert "passive_not_all_full_fill" in report
    assert "feature_ready_has_no_required_nulls" in report


def test_event_stream_filter_retains_last_asof_prefix() -> None:
    boundary = datetime(2026, 7, 11, 12, 0, tzinfo=UTC)
    table = pa.table(
        {
            "receive_ts": pa.array(
                [
                    boundary - timedelta(seconds=10),
                    boundary - timedelta(seconds=1),
                    boundary + timedelta(seconds=2),
                    boundary + timedelta(minutes=2),
                ],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "value": [1, 2, 3, 4],
        }
    )

    selected = _filter_time_with_prefix(
        table, "receive_ts", boundary, boundary + timedelta(minutes=1)
    )

    assert selected["value"].to_pylist() == [2, 3]
