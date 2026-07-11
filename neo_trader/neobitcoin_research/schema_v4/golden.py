"""Callable schema-v4 golden synthetic publication gate."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]

from ..independent_oracle import CONTRACTS, validate_independent_oracle
from .features import FeatureConfig
from .pipeline import RawOnlyPipeline
from .simulation import ExecutionConfig


def _ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _fixture_path() -> Path:
    return Path(__file__).parents[3] / "tests" / "fixtures" / "schema_v4_golden_synthetic.json"


def _raw(data: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    def stamp(row: dict[str, Any], event_id: str) -> dict[str, Any]:
        timestamp = _ts(row["exchange_ts"])
        return {
            **row,
            "event_id": event_id,
            "exchange_ts": timestamp,
            "receive_ts": timestamp,
            "processing_ts": timestamp,
            "materialized_ts": timestamp,
        }

    books = [stamp(row, f"book-{i:02d}") for i, row in enumerate(data["orderbook"])]
    trades = [
        stamp({**row, "aggressor_side": row["side"]}, f"trade-{i:02d}")
        for i, row in enumerate(data["trades"])
    ]
    lasts = [
        stamp({**row, "last_price": row["price"]}, f"last-{i:02d}")
        for i, row in enumerate(data["last_prices"])
    ]
    candles: dict[str, list[dict[str, Any]]] = {}
    for minutes in (1, 5, 15):
        candles[f"candles_{minutes}m"] = [
            stamp(
                {
                    **row,
                    "exchange_ts": row["close_ts"],
                    "candle_end": _ts(row["close_ts"]),
                    "is_complete": True,
                },
                f"candle-{minutes}m-{i}",
            )
            for i, row in enumerate(data["candles"]["complete"])
        ]
    raw = {
        "raw_orderbook": books,
        "raw_trades": trades,
        "raw_last_price": lasts,
        **candles,
        "market_status_events": [
            stamp(
                {
                    "exchange_ts": data["candidate_start"],
                    "trading_status": "NORMAL_TRADING",
                },
                "status-0",
            )
        ],
    }
    for rows in raw.values():
        rows.sort(key=lambda row: (row["exchange_ts"], row["event_id"]))
    return raw


def validate_golden_synthetic(fixture_path: Path | None = None) -> dict[str, Any]:
    """Run production materialization, literal expectations, and independent oracle."""
    path = fixture_path or _fixture_path()
    data = json.loads(path.read_text(encoding="utf-8"))
    raw = _raw(data)
    pipeline = RawOnlyPipeline(
        tick_size=float(data["tick_size"]),
        feature_config=FeatureConfig(
            tick_size=float(data["tick_size"]), required_book_levels=20, candle_warmup=1
        ),
        execution_config=ExecutionConfig(
            tick_size=float(data["tick_size"]),
            order_quantity=10.0,
            aggressive_latency_ms=100,
            passive_timeout_ms=5_000,
        ),
    )
    t0 = _ts(data["candidate_start"])
    result = pipeline.materialize(
        raw,
        candidate_start=t0,
        candidate_end=t0 + timedelta(milliseconds=1),
        materialized_ts=_ts(data["candidate_end"]) + timedelta(minutes=30),
    )
    expected = data["manual_expected"]
    actual = {
        f"{row['side']}/{row['execution_model']}": row for row in result.execution_simulations
    }
    mismatches: list[str] = []
    field_map = {
        "status": "fill_status",
        "filled": "filled_quantity",
        "remaining": "remaining_quantity",
        "fill_price": "fill_price",
    }
    for key, values in expected["schema_v4_executions"].items():
        for expected_name, production_name in field_map.items():
            observed, wanted = actual[key][production_name], values[expected_name]
            equal = observed == wanted
            if isinstance(wanted, float) and observed is not None:
                equal = abs(float(observed) - wanted) <= 1e-9
            if not equal:
                mismatches.append(f"{key}.{expected_name}: {observed!r} != {wanted!r}")
    feature = result.feature_snapshots[0]
    if feature["trade_flow_10s"] != expected["trade_flow_10s"]:
        mismatches.append("trade_flow_10s")
    tables: dict[str, pa.Table] = {}
    for name, rows in {**raw, **result.tables}.items():
        contract = CONTRACTS.get(name)
        if contract is None or not rows:
            tables[name] = pa.Table.from_pylist(rows)
            continue
        expected_names = set(contract.arrow_schema.names)
        arrays = [
            pa.array([row.get(field.name) for row in rows], type=field.type)
            for field in contract.arrow_schema
        ]
        fields = list(contract.arrow_schema)
        for field_name in rows[0].keys() - expected_names:
            values = [row.get(field_name) for row in rows]
            try:
                array = pa.array(values)
            except (pa.ArrowInvalid, pa.ArrowTypeError):
                continue
            arrays.append(array)
            fields.append(pa.field(field_name, array.type))
        tables[name] = pa.Table.from_arrays(arrays, schema=pa.schema(fields))
    oracle = validate_independent_oracle(
        tables, tick_size=float(data["tick_size"]), require_all_contracts=False
    )
    if oracle["status"] != "PASS":
        mismatches.append(f"independent_oracle:{len(oracle['violations'])}_violations")
    return {
        "status": "PASS" if not mismatches else "FAIL",
        "fixture": str(path),
        "mismatches": mismatches,
        "metrics": {
            "books": len(raw["raw_orderbook"]),
            "trades": len(raw["raw_trades"]),
            "features": len(result.feature_snapshots),
            "executions": len(result.execution_simulations),
            "outcomes": len(result.future_outcomes),
            "stops": len(result.shadow_stop_results),
            "exits": len(result.shadow_exit_results),
            "independent_oracle_status": oracle["status"],
            "independent_oracle_violations": len(oracle["violations"]),
        },
    }


__all__ = ["validate_golden_synthetic"]
