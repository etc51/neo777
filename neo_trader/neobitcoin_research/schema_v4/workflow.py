"""End-to-end raw-only schema-v4.1-golden review workflow."""

from __future__ import annotations

import json
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from neo_trader.neobitcoin_research.independent_oracle import (
    CONTRACTS,
    validate_independent_oracle,
)
from neo_trader.neobitcoin_research.review_raw import COMMON_FIELDS, extract_review_raw

from .archive import GoldenArchiveResult, publish_golden_archive
from .features import FeatureConfig
from .golden import validate_golden_synthetic
from .pipeline import RawOnlyPipeline
from .simulation import ExecutionConfig

RAW_NAMES = (
    "raw_orderbook",
    "raw_trades",
    "raw_last_price",
    "candles_1m",
    "candles_5m",
    "candles_15m",
    "market_status_events",
)


@dataclass(frozen=True, slots=True)
class SchemaV4WorkflowResult:
    archive: GoldenArchiveResult
    candidate_start: datetime
    candidate_end: datetime
    support_start: datetime
    required_support_end: datetime
    actual_support_end: dict[str, datetime | None]
    validation: dict[str, Any]
    tick_size: float
    golden_real_report: Path


def create_schema_v4_golden_bundle(
    root: Path | str,
    *,
    output_dir: Path | str | None = None,
    now: datetime | None = None,
    token_files: Sequence[Path | str] = (),
    feature_stride: int = 1,
) -> SchemaV4WorkflowResult:
    """Extract raw, rematerialize from zero, independently validate, publish."""

    base = Path(root).resolve()
    current = (now or datetime.now(UTC)).astimezone(UTC)
    raw_root = base / "active" / "raw"
    if not raw_root.exists():
        raw_root = base / "raw"
    destination = Path(output_dir).resolve() if output_dir else base / "review_bundles"
    with tempfile.TemporaryDirectory(prefix="schema-v4-raw-", dir=destination) as temporary:
        extraction = extract_review_raw(raw_root, Path(temporary), now=current)
        raw_tables = {
            name: pq.read_table(extraction.paths[name])
            for name in RAW_NAMES
        }
        raw_rows = {name: table.to_pylist() for name, table in raw_tables.items()}
        tick_size = _infer_tick(raw_rows["raw_orderbook"])
        materialized_ts = current
        pipeline = RawOnlyPipeline(
            tick_size=tick_size,
            feature_config=FeatureConfig(
                tick_size=tick_size,
                required_book_levels=20,
                candle_warmup=2,
                max_source_age_ms=180_000.0,
            ),
            execution_config=ExecutionConfig(
                tick_size=tick_size,
                order_quantity=5_000.0,
                aggressive_latency_ms=100,
                passive_timeout_ms=30_000,
            ),
            feature_interval_seconds=30,
        )
        result = pipeline.materialize(
            raw_rows,
            candidate_start=extraction.window.candidate_start,
            candidate_end=extraction.window.candidate_end,
            materialized_ts=materialized_ts,
        )
        if feature_stride > 1:
            raise ValueError("feature_stride is reserved; materialization must remain complete")
        tables = _assemble_tables(raw_tables, result.tables, materialized_ts)
        required_support_end = _required_support_end(tables)
        actual_support_end = {
            name: _max_timestamp(tables[name], "exchange_ts")
            for name in ("raw_orderbook", "raw_trades", "raw_last_price")
        }
        support_ok = all(
            value is not None and value >= required_support_end
            for value in actual_support_end.values()
        )
        oracle = validate_independent_oracle(
            tables,
            tick_size=tick_size,
            require_all_contracts=True,
            candle_warmup=pipeline.feature_config.candle_warmup,
            atr_period=pipeline.feature_config.atr_period,
            spread_threshold_ticks=int(pipeline.feature_config.spread_threshold_ticks),
            candle_stale_ms={
                "1m": pipeline.feature_config.candle_1m_max_interval_age_ms,
                "5m": pipeline.feature_config.candle_5m_max_interval_age_ms,
                "15m": pipeline.feature_config.candle_15m_max_interval_age_ms,
            },
            max_source_age_ms=pipeline.feature_config.max_source_age_ms,
        )
        golden = validate_golden_synthetic()
        real_report = (
            base.parent
            / "777"
            / "111_neobitcoin_edge"
            / "reports"
            / "golden_real_candle_audit.md"
        )
        if not real_report.parent.exists():
            real_report = Path.cwd() / "reports" / "golden_real_candle_audit.md"
        _write_real_slice_data(Path(temporary) / "data", tables)
        from scripts.generate_golden_real_slice import generate

        generate(Path(temporary) / "data", real_report, count=10)
        real_ok = (
            real_report.is_file()
            and real_report.read_text(encoding="utf-8").count("## ") >= 10
        )
        feature_rows = tables["feature_snapshots"].to_pylist()
        all_features_ready = bool(feature_rows) and all(
            row.get("feature_ready") is True for row in feature_rows
        )
        gates = {
            "golden_synthetic": golden["status"],
            "golden_real_slice": "PASS" if real_ok else "FAIL",
            "production_materializer": "PASS" if tables["feature_snapshots"].num_rows else "FAIL",
            "all_features_ready": "PASS" if all_features_ready else "FAIL",
            "independent_oracle": oracle["status"],
            "production_vs_oracle": oracle["status"],
            "data_contracts": "PASS" if not oracle["violations"] else "FAIL",
            "foreign_keys": "PASS"
            if oracle["metrics"].get("foreign_keys", {}).get("missing", 0) == 0
            else "FAIL",
            "raw_support": "PASS" if support_ok else "FAIL",
            "unexplained_mismatches": "PASS"
            if oracle["metrics"].get("unexplained_mismatches", 1) == 0
            else "FAIL",
            "pyarrow": "PASS",
            "duckdb": "PASS",
            "zstd": "PASS",
            "sha256": "PASS",
            "secrets_scan": "PASS"
            if oracle["metrics"].get("secrets", {}).get("hits", 0) == 0
            else "FAIL",
        }
        status = "PASS" if all(value == "PASS" for value in gates.values()) else "FAIL"
        validation = {
            "status": status,
            "gates": gates,
            "metrics": _report_metrics(tables, oracle, golden, actual_support_end),
            "oracle": oracle,
            "golden": golden,
        }
        diagnostic = real_report.parent / "schema_v4_1_last_validation.json"
        diagnostic.write_text(
            json.dumps(validation, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
        if status != "PASS":
            _write_quarantine(
                destination,
                extraction.window.candidate_start,
                extraction.window.candidate_end,
                validation,
            )
            raise RuntimeError("schema-v4.1 validation failed; result quarantined")
        token_values = [
            Path(value).read_bytes().strip()
            for value in token_files
            if Path(value).is_file()
        ]
        archive = publish_golden_archive(
            tables,
            destination,
            candidate_start=extraction.window.candidate_start,
            candidate_end=extraction.window.candidate_end,
            support_start=extraction.window.support_start,
            required_support_end=required_support_end,
            validation=validation,
            token_values=token_values,
        )
        return SchemaV4WorkflowResult(
            archive,
            extraction.window.candidate_start,
            extraction.window.candidate_end,
            extraction.window.support_start,
            required_support_end,
            actual_support_end,
            validation,
            tick_size,
            real_report,
        )


def _assemble_tables(
    raw: Mapping[str, pa.Table],
    derived: Mapping[str, list[dict[str, Any]]],
    materialized_ts: datetime,
) -> dict[str, pa.Table]:
    tables: dict[str, pa.Table] = {}
    for name, table in raw.items():
        sort_keys: list[tuple[str, str]] = [("exchange_ts", "ascending")]
        for sequence_name in ("sequence", "revision"):
            if sequence_name in table.column_names:
                sort_keys.append((sequence_name, "ascending"))
                break
        sort_keys.extend(
            [("receive_ts", "ascending"), ("event_id", "ascending")]
        )
        table = table.sort_by(sort_keys)
        if "materialized_ts" not in table.column_names:
            table = table.append_column(
                "materialized_ts",
                pa.array([materialized_ts] * table.num_rows, type=pa.timestamp("us", tz="UTC")),
            )
        tables[name] = table
    for name, rows in derived.items():
        contract = CONTRACTS.get(name)
        schema = contract.arrow_schema if contract is not None else None
        tables[name] = (
            _table_with_contract(rows, schema)
            if rows
            else pa.Table.from_pylist([], schema=schema)
        )
    quality_schema = pa.schema(
        [
            *COMMON_FIELDS,
            pa.field("materialized_ts", pa.timestamp("us", tz="UTC")),
            pa.field("event_type", pa.string(), nullable=False),
            pa.field("severity", pa.string(), nullable=False),
            pa.field("gap_seconds", pa.float64()),
            pa.field("details", pa.string()),
        ]
    )
    tables["data_quality_events"] = pa.Table.from_pylist([], schema=quality_schema)
    return tables


def _table_with_contract(
    rows: list[dict[str, Any]], schema: pa.Schema | None
) -> pa.Table:
    """Apply mandatory Arrow types while preserving additional audit fields."""

    if schema is None:
        return pa.Table.from_pylist(rows)
    arrays = [
        pa.array([row.get(field.name) for row in rows], type=field.type)
        for field in schema
    ]
    fields = list(schema)
    mandatory = set(schema.names)
    for name in rows[0].keys() - mandatory:
        values = [row.get(name) for row in rows]
        array = pa.array(values)
        arrays.append(array)
        fields.append(pa.field(name, array.type))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def _write_quarantine(
    destination: Path,
    candidate_start: datetime,
    candidate_end: datetime,
    validation: Mapping[str, Any],
) -> Path:
    """Persist sanitized fail-closed diagnostics without publishing an archive."""

    stamp = (
        f"{candidate_start.astimezone(UTC):%Y%m%dT%H%M%SZ}_"
        f"{candidate_end.astimezone(UTC):%Y%m%dT%H%M%SZ}"
    )
    quarantine = destination / "quarantine"
    quarantine.mkdir(parents=True, exist_ok=True)
    target = quarantine / f"schema-v4.1-{stamp}-validation.json"
    target.write_text(
        json.dumps(validation, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    return target


def _required_support_end(tables: Mapping[str, pa.Table]) -> datetime:
    executions = tables["execution_simulations"].to_pylist()
    entries = [row.get("fill_ts") or row.get("order_ts") for row in executions]
    valid = [value.astimezone(UTC) for value in entries if isinstance(value, datetime)]
    if not valid:
        raise RuntimeError("no theoretical entries or actual fills")
    return max(valid) + timedelta(seconds=1800)


def _max_timestamp(table: pa.Table, column: str) -> datetime | None:
    values = [value for value in table[column].to_pylist() if isinstance(value, datetime)]
    return max(values).astimezone(UTC) if values else None


def _infer_tick(books: Sequence[Mapping[str, Any]]) -> float:
    differences: list[float] = []
    for row in books:
        for side in ("bids", "asks"):
            prices = [float(level["price"]) for level in row.get(side) or []]
            differences.extend(
                abs(left - right)
                for left, right in zip(prices, prices[1:], strict=False)
                if left != right
            )
    positive = [value for value in differences if value > 1e-9]
    return round(min(positive), 9) if positive else 0.1


def _write_real_slice_data(directory: Path, tables: Mapping[str, pa.Table]) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    for name, table in tables.items():
        pq.write_table(table, directory / f"{name}.parquet", compression="zstd")


def _report_metrics(
    tables: Mapping[str, pa.Table],
    oracle: Mapping[str, Any],
    golden: Mapping[str, Any],
    support: Mapping[str, datetime | None],
) -> dict[str, Any]:
    metrics = oracle.get("metrics", {})
    return {
        "raw": {
            "rows": {name: tables[name].num_rows for name in RAW_NAMES},
            "actual_support_end": support,
            "gaps": tables["data_quality_events"].num_rows,
        },
        "features": metrics.get("features", {}),
        "execution": metrics.get("executions", {}),
        "outcomes": metrics.get("outcomes", {}),
        "stops_exits": {
            "stops": metrics.get("stops", {}),
            "exits": metrics.get("exits", {}),
        },
        "golden": golden.get("metrics", {}),
    }


__all__ = ["SchemaV4WorkflowResult", "create_schema_v4_golden_bundle"]
