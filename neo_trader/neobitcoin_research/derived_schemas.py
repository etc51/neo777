"""Stable Arrow schemas and row flattening for derived research datasets.

The complete source record remains in ``record_json`` for diagnostics and
forward compatibility.  Frequently queried identifiers, decisions and
numeric measures are also promoted to typed columns so DuckDB/PyArrow users
do not need to parse JSON for normal research queries.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Final, Literal, TypeAlias, cast

JsonMapping: TypeAlias = Mapping[str, Any]
ArrowKind: TypeAlias = Literal["string", "float64", "int64", "bool"]


@dataclass(frozen=True, slots=True)
class TypedField:
    """One promoted Parquet column and its source path in a record."""

    name: str
    kind: ArrowKind
    path: tuple[str, ...] = ()

    @property
    def source_path(self) -> tuple[str, ...]:
        return self.path or (self.name,)


COMMON_FIELDS: Final = (
    TypedField("instrument_uid", "string"),
    TypedField("timestamp", "string"),
    TypedField("exchange_timestamp", "string"),
    TypedField("raw_event_id", "string"),
    TypedField("feature_snapshot_id", "string"),
    TypedField("candidate_id", "string"),
    TypedField("experiment_id", "string"),
    TypedField("configuration_hash", "string"),
    TypedField("code_commit", "string"),
)


def _feature_fields() -> tuple[TypedField, ...]:
    fields = [
        TypedField(name, "float64")
        for name in (
            "best_bid",
            "best_ask",
            "midprice",
            "last_price",
            "spread_absolute",
            "spread_ticks",
            "spread_bps",
            "microprice",
            "microprice_minus_mid",
            "latency_ms",
            "stale_age_seconds",
            "seconds_after_reconnect",
            "bid_age_seconds",
            "ask_age_seconds",
            "distance_weighted_imbalance",
            "bid_slope",
            "ask_slope",
            "bid_convexity",
            "ask_convexity",
            "depth_asymmetry",
            "bid_max_gap_ticks",
            "ask_max_gap_ticks",
            "volume_concentration",
            "new_bid_volume",
            "new_ask_volume",
            "removed_bid_volume",
            "removed_ask_volume",
            "multi_level_ofi",
            "ofi_change",
            "ofi_acceleration",
            "top_level_pressure",
            "last_trade_classification_confidence",
            "trade_price_divergence",
            "realized_volatility",
        )
    ]
    fields.extend(
        TypedField(name, "bool") for name in ("gap_before", "best_bid_changed", "best_ask_changed")
    )
    fields.extend(TypedField(name, "string") for name in ("trading_status", "last_trade_direction"))
    for level in range(1, 21):
        fields.extend(
            TypedField(f"{prefix}_{level}", "float64")
            for prefix in (
                "bid_price",
                "ask_price",
                "bid_volume",
                "ask_volume",
                "bid_distance",
                "ask_distance",
                "bid_volume_delta",
                "ask_volume_delta",
            )
        )
    for depth in (1, 3, 5, 10, 20):
        fields.extend(
            TypedField(f"{prefix}_{depth}", "float64")
            for prefix in ("cumulative_bid_depth", "cumulative_ask_depth", "imbalance")
        )
    for seconds in (1, 5, 15, 30, 60, 180, 300, 900):
        prefix = f"trades_{seconds}s"
        fields.extend(
            TypedField(f"{prefix}_{suffix}", "int64")
            for suffix in ("buy_count", "sell_count", "large_count")
        )
        fields.extend(
            TypedField(f"{prefix}_{suffix}", "float64")
            for suffix in (
                "buy_volume",
                "sell_volume",
                "signed_volume",
                "imbalance",
                "mean_size",
                "median_size",
                "max_size",
                "intensity",
            )
        )
    for label in ("1s", "5s", "15s", "30s", "1m", "3m", "5m", "15m"):
        fields.append(TypedField(f"return_{label}", "float64"))
    for interval in ("1m", "5m", "15m"):
        fields.extend(
            (
                TypedField(f"atr_{interval}", "float64"),
                TypedField(f"candle_volume_{interval}", "float64"),
            )
        )
    return tuple(fields)


EXECUTION_FIELDS: Final = tuple(
    [
        TypedField(name, "string")
        for name in ("model", "side", "status", "reason", "book_timestamp", "snapshot_id")
    ]
    + [
        TypedField(name, "float64")
        for name in (
            "requested_quantity",
            "filled_quantity",
            "unfilled_quantity",
            "fill_ratio",
            "limit_price",
            "top_level_price",
            "average_price",
            "sweep_vwap",
            "slippage_vs_top",
            "slippage_vs_mid",
            "slippage_cost",
            "adverse_selection_per_unit",
            "adverse_selection_cost",
            "position_size_rub",
            "latency_ms",
        )
    ]
    + [TypedField("levels_consumed", "int64")]
)

OUTCOME_FIELDS: Final = (
    TypedField("direction", "string"),
    TypedField("execution_model", "string"),
    TypedField("status", "string"),
    TypedField("reason", "string"),
    TypedField("horizon_seconds", "int64"),
    TypedField("latency_ms", "int64"),
    TypedField("position_size_rub", "float64"),
    TypedField("requested_quantity", "float64"),
    TypedField("matched_round_trip_quantity", "float64"),
    TypedField("realized_net_pnl_rub", "float64"),
    TypedField("profitable_after_costs", "bool"),
    TypedField("mfe_rub", "float64"),
    TypedField("mae_rub", "float64"),
    TypedField("midprice_change", "float64"),
    TypedField("holding_fee_rub", "float64"),
    TypedField("entry_fill_ratio", "float64", ("entry", "fill_ratio")),
    TypedField("entry_average_price", "float64", ("entry", "average_price")),
    TypedField("entry_filled_quantity", "float64", ("entry", "filled_quantity")),
    TypedField("exit_average_price", "float64", ("exit", "average_price")),
    TypedField("exit_filled_quantity", "float64", ("exit", "filled_quantity")),
    TypedField("no_fill_is_not_zero_pnl", "bool"),
)

DATASET_FIELDS: Final[dict[str, tuple[TypedField, ...]]] = {
    "feature_snapshots": _feature_fields(),
    "candidate_events": (
        TypedField("raw_signal", "string"),
        TypedField("independent_signal", "bool"),
    ),
    "future_outcomes": OUTCOME_FIELDS,
    "execution_simulations": EXECUTION_FIELDS,
    "shadow_predictions": (
        TypedField("decision", "string"),
        TypedField("probability_long", "float64"),
        TypedField("probability_short", "float64"),
        TypedField("probability_no_trade", "float64"),
        TypedField("expected_aggressive_long_pnl_rub", "float64"),
        TypedField("expected_aggressive_short_pnl_rub", "float64"),
        TypedField("expected_passive_long_pnl_rub", "float64"),
        TypedField("expected_passive_short_pnl_rub", "float64"),
        TypedField("position_size_rub", "float64"),
        TypedField("independent_signal", "bool"),
        TypedField("model_id", "string"),
        TypedField("model_version", "string"),
        TypedField("model_execution", "string"),
    ),
    "shadow_trades": (
        TypedField("direction", "string"),
        TypedField("status", "string"),
        TypedField("entry_price", "float64"),
        TypedField("exit_price", "float64"),
        TypedField("quantity", "float64"),
        TypedField("pnl_rub", "float64"),
        TypedField("mfe_rub", "float64"),
        TypedField("mae_rub", "float64"),
    ),
    "model_registry": (
        TypedField("model_id", "string"),
        TypedField("model_version", "string"),
        TypedField("trained_samples", "int64"),
        TypedField("profitable_fold_ratio", "float64"),
    ),
    "experiment_registry": (
        TypedField("holdout_used_for_training", "bool"),
        TypedField("folds", "int64"),
        TypedField("samples", "int64"),
        TypedField("net_pnl_rub", "float64"),
    ),
    "data_quality_metrics": (
        TypedField("events_total", "int64"),
        TypedField("duplicates", "int64"),
        TypedField("gaps", "int64"),
        TypedField("null_fraction", "float64"),
        TypedField("latency_ms", "float64"),
    ),
}


def derived_arrow_schema(dataset: str) -> Any:
    """Return the stable PyArrow schema for one derived dataset."""

    pa = _pyarrow()
    promoted = (*COMMON_FIELDS, *DATASET_FIELDS.get(dataset, ()))
    return pa.schema(
        [
            ("dataset", pa.string()),
            ("event_id", pa.string()),
            ("recorded_at", pa.string()),
            ("schema_version", pa.string()),
            *[(field.name, _arrow_type(pa, field.kind)) for field in promoted],
            ("record_json", pa.string()),
        ]
    )


def flatten_derived_row(
    dataset: str,
    *,
    event_id: str,
    recorded_at: str,
    schema_version: str,
    record: JsonMapping,
    record_json: str,
) -> dict[str, Any]:
    """Promote stable fields while retaining the complete diagnostic JSON."""

    row: dict[str, Any] = {
        "dataset": dataset,
        "event_id": event_id,
        "recorded_at": recorded_at,
        "schema_version": schema_version,
    }
    for field in (*COMMON_FIELDS, *DATASET_FIELDS.get(dataset, ())):
        row[field.name] = _coerce(_nested(record, field.source_path), field.kind)
    row["record_json"] = record_json
    return row


def _nested(record: JsonMapping, path: Sequence[str]) -> Any:
    value: Any = record
    for part in path:
        if not isinstance(value, Mapping):
            return None
        value = value.get(part)
    return value


def _coerce(value: Any, kind: ArrowKind) -> str | float | int | bool | None:
    if value is None:
        return None
    if kind == "string":
        return str(value)
    if kind == "bool":
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "1", "yes"}:
                return True
            if normalized in {"false", "0", "no"}:
                return False
        return None
    if isinstance(value, bool):
        return None
    if kind == "int64":
        try:
            parsed = Decimal(str(value))
        except (InvalidOperation, ValueError):
            return None
        return int(parsed) if parsed.is_finite() and parsed == parsed.to_integral_value() else None
    try:
        parsed_float = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed_float if math.isfinite(parsed_float) else None


def _arrow_type(pa: Any, kind: ArrowKind) -> Any:
    return {
        "string": pa.string,
        "float64": pa.float64,
        "int64": pa.int64,
        "bool": pa.bool_,
    }[kind]()


def _pyarrow() -> Any:
    return cast(Any, __import__("pyarrow"))


__all__ = [
    "COMMON_FIELDS",
    "DATASET_FIELDS",
    "TypedField",
    "derived_arrow_schema",
    "flatten_derived_row",
]
