"""Schema-v4 data contracts, deliberately free of materialization logic."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final

import pyarrow as pa  # type: ignore[import-untyped]

UTC_TS: Final = pa.timestamp("us", tz="UTC")
PRICE_LEVELS: Final = (1, 3, 5, 10, 20)
TRADE_WINDOWS: Final = (5, 10, 30, 60, 180, 300, 900)


@dataclass(frozen=True)
class ForeignKey:
    fields: tuple[str, ...]
    target_dataset: str
    target_fields: tuple[str, ...]
    nullable: bool = False


@dataclass(frozen=True)
class DatasetContract:
    name: str
    arrow_schema: pa.Schema
    primary_key: tuple[str, ...]
    required: frozenset[str]
    nullable: frozenset[str]
    units: Mapping[str, str]
    description: str
    foreign_keys: tuple[ForeignKey, ...] = ()


@dataclass(frozen=True)
class ContractViolation:
    code: str
    dataset: str
    detail: str
    row: int | None = None
    field: str | None = None


def _schema(fields: list[tuple[str, pa.DataType, bool]]) -> pa.Schema:
    return pa.schema([pa.field(name, typ, nullable=nullable) for name, typ, nullable in fields])


_TIMELINE = [
    ("event_id", pa.string(), False),
    ("exchange_ts", UTC_TS, False),
    ("receive_ts", UTC_TS, False),
    ("processing_ts", UTC_TS, False),
    ("materialized_ts", UTC_TS, True),
]
_LEVEL = pa.list_(pa.struct([pa.field("price", pa.float64()), pa.field("quantity", pa.float64())]))


def _raw_contract(
    name: str, extra: list[tuple[str, pa.DataType, bool]], description: str
) -> DatasetContract:
    schema = _schema(_TIMELINE + extra)
    required = frozenset(field.name for field in schema if not field.nullable)
    return DatasetContract(
        name,
        schema,
        ("event_id",),
        required,
        frozenset(field.name for field in schema if field.nullable),
        {"exchange_ts": "UTC", "receive_ts": "UTC", "processing_ts": "UTC"},
        description,
    )


CONTRACTS: Final[dict[str, DatasetContract]] = {
    "raw_orderbook": _raw_contract(
        "raw_orderbook",
        [
            ("bids", _LEVEL, False),
            ("asks", _LEVEL, False),
            ("best_bid", pa.float64(), False),
            ("best_ask", pa.float64(), False),
        ],
        "Full typed order-book snapshots.",
    ),
    "raw_trades": _raw_contract(
        "raw_trades",
        [
            ("price", pa.float64(), False),
            ("quantity", pa.float64(), False),
            ("aggressor_side", pa.string(), False),
        ],
        "Deduplicated public trades with explicit aggressor side.",
    ),
    "raw_last_price": _raw_contract(
        "raw_last_price", [("last_price", pa.float64(), False)], "Raw last-price events."
    ),
    "market_status_events": _raw_contract(
        "market_status_events", [("trading_status", pa.string(), True)], "Trading-status timeline."
    ),
}

for _interval in ("1m", "5m", "15m"):
    _name = f"candles_{_interval}"
    CONTRACTS[_name] = _raw_contract(
        _name,
        [
            ("candle_start", UTC_TS, True),
            ("candle_end", UTC_TS, False),
            ("is_complete", pa.bool_(), True),
            ("revision", pa.int64(), True),
            ("open", pa.float64(), False),
            ("high", pa.float64(), False),
            ("low", pa.float64(), False),
            ("close", pa.float64(), False),
            ("volume", pa.float64(), False),
        ],
        f"Completed and in-progress {_interval} candles.",
    )

_feature_fields = [
    ("schema_version", pa.string(), True),
    ("feature_snapshot_id", pa.string(), False),
    ("feature_ready", pa.bool_(), False),
    ("feature_ts", UTC_TS, False),
    ("feature_receive_ts", UTC_TS, False),
    ("feature_processing_ts", UTC_TS, False),
    ("materialized_ts", UTC_TS, False),
    ("trend", pa.string(), True),
    ("regime", pa.string(), True),
    ("orderbook_source_event_id", pa.string(), True),
    ("best_bid", pa.float64(), True),
    ("best_ask", pa.float64(), True),
    ("spread", pa.float64(), True),
    ("spread_price", pa.float64(), True),
    ("tick_size", pa.float64(), True),
    ("spread_ticks", pa.float64(), True),
    ("spread_ticks_decimal", pa.float64(), True),
    ("spread_ticks_int", pa.int64(), True),
    ("tick_grid_error", pa.float64(), True),
    ("spread_threshold_ticks", pa.int64(), True),
    ("spread_gate_passed", pa.bool_(), True),
    ("microprice", pa.float64(), True),
    ("ofi", pa.float64(), True),
    ("ofi_delta", pa.float64(), True),
    ("ofi_source_event_id", pa.string(), True),
    ("ofi_previous_event_id", pa.string(), True),
    ("ofi_continuity_valid", pa.bool_(), True),
    ("ofi_reset_reason", pa.string(), True),
    ("last_price_source_event_id", pa.string(), True),
    ("last_price", pa.float64(), True),
    ("missing_fields", pa.list_(pa.string()), True),
    ("missing_reason", pa.string(), True),
    ("source_status", pa.string(), True),
    ("readiness_checks_passed", pa.int64(), True),
    ("readiness_checks_total", pa.int64(), True),
    ("readiness_version", pa.string(), True),
]
for _interval in ("1m", "5m", "15m"):
    _feature_fields += [
        (f"candle_{_interval}_source_event_id", pa.string(), True),
        (f"candle_{_interval}_start", UTC_TS, True),
        (f"candle_{_interval}_end", UTC_TS, True),
        (f"candle_{_interval}_revision", pa.int64(), True),
        (f"candle_{_interval}_source_is_complete", pa.bool_(), True),
        (f"candle_{_interval}_canonical_is_complete", pa.bool_(), True),
        (f"candle_{_interval}_completion_reason", pa.string(), True),
        (f"candle_{_interval}_revision_receive_ts", UTC_TS, True),
        (f"candle_{_interval}_interval_age_ms", pa.float64(), True),
        (f"candle_{_interval}_receive_age_ms", pa.float64(), True),
        (f"candle_{_interval}_stale", pa.bool_(), True),
        (f"candle_volume_{_interval}", pa.float64(), True),
        (f"return_{_interval}", pa.float64(), True),
        (f"atr_{_interval}", pa.float64(), True),
        (f"atr_{_interval}_warmup_remaining", pa.int64(), True),
    ]
for _level in range(1, 21):
    for _side in ("bid", "ask"):
        _feature_fields += [
            (f"{_side}_price_{_level:02d}", pa.float64(), True),
            (f"{_side}_quantity_{_level:02d}", pa.float64(), True),
        ]
for _depth in PRICE_LEVELS:
    _feature_fields += [
        (f"bid_depth_{_depth}", pa.float64(), True),
        (f"ask_depth_{_depth}", pa.float64(), True),
        (f"imbalance_{_depth}", pa.float64(), True),
    ]
for _window in TRADE_WINDOWS:
    _feature_fields += [
        (f"trade_flow_{_window}s", pa.float64(), False),
        (f"trade_count_{_window}s", pa.int64(), False),
    ]

CONTRACTS["feature_snapshots"] = DatasetContract(
    "feature_snapshots",
    _schema(_feature_fields),
    ("feature_snapshot_id",),
    frozenset(field.name for field in _schema(_feature_fields) if not field.nullable),
    frozenset(field.name for field in _schema(_feature_fields) if field.nullable),
    {"*_price_*": "price", "*_quantity_*": "quantity", "*_depth_*": "quantity", "*_age_ms": "ms"},
    "Point-in-time feature snapshots reproducible from raw data.",
    (ForeignKey(("orderbook_source_event_id",), "raw_orderbook", ("event_id",), True),),
)


def _derived(
    name: str,
    pk: str,
    fields: list[tuple[str, pa.DataType, bool]],
    fks: tuple[ForeignKey, ...],
    description: str,
) -> None:
    schema = _schema(fields)
    CONTRACTS[name] = DatasetContract(
        name,
        schema,
        (pk,),
        frozenset(f.name for f in schema if not f.nullable),
        frozenset(f.name for f in schema if f.nullable),
        {},
        description,
        fks,
    )


_derived(
    "candidate_events",
    "candidate_id",
    [
        ("candidate_id", pa.string(), False),
        ("feature_snapshot_id", pa.string(), False),
        ("side", pa.string(), False),
        ("candidate_ts", UTC_TS, False),
    ],
    (ForeignKey(("feature_snapshot_id",), "feature_snapshots", ("feature_snapshot_id",)),),
    "LONG/SHORT candidates.",
)
_derived(
    "filter_decisions",
    "filter_decision_id",
    [
        ("filter_decision_id", pa.string(), False),
        ("candidate_id", pa.string(), False),
        ("feature_snapshot_id", pa.string(), False),
        ("filter_name", pa.string(), False),
        ("passed", pa.bool_(), False),
    ],
    (
        ForeignKey(("candidate_id",), "candidate_events", ("candidate_id",)),
        ForeignKey(("feature_snapshot_id",), "feature_snapshots", ("feature_snapshot_id",)),
    ),
    "One auditable row per candidate filter.",
)
_derived(
    "execution_simulations",
    "simulation_id",
    [
        ("simulation_id", pa.string(), False),
        ("candidate_id", pa.string(), False),
        ("feature_snapshot_id", pa.string(), False),
        ("side", pa.string(), False),
        ("order_ts", UTC_TS, False),
        ("fill_ts", UTC_TS, True),
        ("fill_status", pa.string(), False),
        ("filled_quantity", pa.float64(), False),
        ("fill_price", pa.float64(), True),
    ],
    (
        ForeignKey(("candidate_id",), "candidate_events", ("candidate_id",)),
        ForeignKey(("feature_snapshot_id",), "feature_snapshots", ("feature_snapshot_id",)),
    ),
    "Raw-evidence execution simulations.",
)
_derived(
    "future_outcomes",
    "outcome_id",
    [
        ("outcome_id", pa.string(), False),
        ("candidate_id", pa.string(), False),
        ("simulation_id", pa.string(), False),
        ("side", pa.string(), False),
        ("entry_ts", UTC_TS, False),
        ("entry_price", pa.float64(), False),
        ("target_ts", UTC_TS, False),
        ("price_source_event_id", pa.string(), True),
        ("outcome_complete", pa.bool_(), False),
        ("executable_exit_price", pa.float64(), True),
        ("mfe_ticks", pa.float64(), True),
        ("mae_ticks", pa.float64(), True),
        ("mfe_source_event_id", pa.string(), True),
        ("mae_source_event_id", pa.string(), True),
    ],
    (
        ForeignKey(("candidate_id",), "candidate_events", ("candidate_id",)),
        ForeignKey(("simulation_id",), "execution_simulations", ("simulation_id",)),
        ForeignKey(("price_source_event_id",), "raw_orderbook", ("event_id",), True),
        ForeignKey(("mfe_source_event_id",), "raw_orderbook", ("event_id",), True),
        ForeignKey(("mae_source_event_id",), "raw_orderbook", ("event_id",), True),
    ),
    "Executable future outcomes and excursions.",
)
for _name, _pk in (("shadow_stop_results", "result_id"), ("shadow_exit_results", "result_id")):
    _derived(
        _name,
        _pk,
        [
            ("result_id", pa.string(), False),
            ("candidate_id", pa.string(), False),
            ("simulation_id", pa.string(), False),
            ("side", pa.string(), False),
            ("entry_ts", UTC_TS, False),
            ("exit_ts", UTC_TS, False),
            ("exit_source_event_id", pa.string(), True),
        ],
        (
            ForeignKey(("candidate_id",), "candidate_events", ("candidate_id",)),
            ForeignKey(("simulation_id",), "execution_simulations", ("simulation_id",)),
            ForeignKey(("exit_source_event_id",), "raw_orderbook", ("event_id",), True),
        ),
        f"Independent {_name} audit rows.",
    )


def validate_data_contracts(
    tables: Mapping[str, pa.Table], *, require_all: bool = True
) -> list[ContractViolation]:
    """Validate structural contracts and generic invariants without business formulas."""
    out: list[ContractViolation] = []
    if require_all:
        for name in CONTRACTS.keys() - tables.keys():
            out.append(ContractViolation("missing_dataset", name, "required dataset is absent"))
    for name, table in tables.items():
        contract = CONTRACTS.get(name)
        if contract is None:
            continue
        missing = contract.required - set(table.column_names)
        for field in sorted(missing):
            out.append(
                ContractViolation("missing_field", name, "required field is absent", field=field)
            )
        for expected in contract.arrow_schema:
            if expected.name not in table.column_names:
                continue
            actual = table.schema.field(expected.name).type
            wanted = expected.type
            compatible = actual == wanted or (expected.nullable and pa.types.is_null(actual))
            if pa.types.is_floating(wanted):
                compatible = pa.types.is_floating(actual)
            elif pa.types.is_integer(wanted):
                compatible = pa.types.is_integer(actual)
            elif pa.types.is_timestamp(wanted):
                compatible = pa.types.is_timestamp(actual) and actual.tz == "UTC"
            if not compatible:
                out.append(
                    ContractViolation(
                        "arrow_type", name, f"expected {wanted}, got {actual}", field=expected.name
                    )
                )
        rows = table.to_pylist()
        seen: set[tuple[Any, ...]] = set()
        previous: datetime | None = None
        for index, row in enumerate(rows):
            if name == "feature_snapshots" and row.get("schema_version") == "schema-v4.1":
                required_v41 = {
                    "missing_fields", "readiness_checks_passed", "readiness_checks_total",
                    "readiness_version", "ofi_source_event_id", "ofi_continuity_valid",
                    "spread_price", "tick_size", "spread_ticks_decimal", "spread_ticks_int",
                    "tick_grid_error", "spread_threshold_ticks",
                }
                for field_name in sorted(required_v41):
                    if field_name not in row or row.get(field_name) is None:
                        out.append(ContractViolation(
                            "missing_v41_field", name, "schema-v4.1 field is absent/null",
                            index, field_name,
                        ))
            for field in contract.required:
                if field in row and row[field] is None:
                    out.append(
                        ContractViolation(
                            "null_required", name, "required value is null", index, field
                        )
                    )
            key = tuple(row.get(field) for field in contract.primary_key)
            if key in seen:
                out.append(ContractViolation("duplicate_primary_key", name, repr(key), index))
            seen.add(key)
            ts = row.get("exchange_ts")
            if isinstance(ts, datetime) and previous is not None and ts < previous:
                out.append(
                    ContractViolation(
                        "non_monotonic_timestamp",
                        name,
                        "exchange_ts decreased",
                        index,
                        "exchange_ts",
                    )
                )
            if isinstance(ts, datetime):
                previous = ts
            for field, value in row.items():
                if value is None:
                    continue
                if (
                    (
                        "quantity" in field
                        or field == "volume"
                        or field.startswith(("bid_depth_", "ask_depth_", "mfe_", "mae_"))
                    )
                    and isinstance(value, (int, float))
                    and value < 0
                ):
                    out.append(ContractViolation("negative_value", name, f"{value}", index, field))
            if (
                name in {"shadow_stop_results", "shadow_exit_results"}
                and isinstance(row.get("entry_ts"), datetime)
                and isinstance(row.get("exit_ts"), datetime)
                and row["exit_ts"] < row["entry_ts"]
            ):
                out.append(
                    ContractViolation(
                        "exit_before_entry", name, "exit_ts < entry_ts", index, "exit_ts"
                    )
                )
            if (
                name == "future_outcomes"
                and row.get("outcome_complete")
                and not row.get("price_source_event_id")
            ):
                out.append(
                    ContractViolation(
                        "incomplete_outcome_marked_complete",
                        name,
                        "complete outcome lacks source",
                        index,
                    )
                )
    # Foreign keys are validated after all primary-key indexes are known.
    for name, table in tables.items():
        contract = CONTRACTS.get(name)
        if not contract:
            continue
        for fk in contract.foreign_keys:
            target = tables.get(fk.target_dataset)
            if target is None:
                continue
            valid = {tuple(row.get(f) for f in fk.target_fields) for row in target.to_pylist()}
            for index, row in enumerate(table.to_pylist()):
                value = tuple(row.get(f) for f in fk.fields)
                if all(item is None for item in value) and fk.nullable:
                    continue
                if value not in valid:
                    out.append(
                        ContractViolation(
                            "foreign_key",
                            name,
                            f"{fk.fields}={value} -> {fk.target_dataset}",
                            index,
                        )
                    )
    return out


__all__ = [
    "CONTRACTS",
    "ContractViolation",
    "DatasetContract",
    "ForeignKey",
    "PRICE_LEVELS",
    "TRADE_WINDOWS",
    "validate_data_contracts",
]
