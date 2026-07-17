"""Durable JSONL-to-Parquet datasets for one paper-trading session.

Active writers append crash-recoverable JSONL files ending in
``.inprogress``.  Successful close materializes exactly one typed ZSTD
Parquet file for every required dataset, including empty datasets, and then
removes every active marker.  Market events therefore never need to enter the
small operational SQLite database.
"""

from __future__ import annotations

import json
import os
import threading
from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Final, TextIO

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

REQUIRED_DATASETS: Final = (
    "market_status_events.parquet",
    "health_events.parquet",
    "data_quality_events.parquet",
    "strategy_evaluations.parquet",
    "candidate_signals.parquet",
    "filter_decisions.parquet",
    "paper_orders.parquet",
    "paper_fills.parquet",
    "paper_positions.parquet",
    "paper_trades.parquet",
    "equity_curve.parquet",
    "mfe_mae.parquet",
    "shadow_stop_results.parquet",
    "shadow_exit_results.parquet",
    "raw_orderbook_event_windows.parquet",
    "raw_trades_event_windows.parquet",
    "raw_last_price_event_windows.parquet",
    "raw_candles_event_windows.parquet",
    "strategy_errors.parquet",
    "delivery_events.parquet",
)
DATASET_NAMES: Final = tuple(name.removesuffix(".parquet") for name in REQUIRED_DATASETS)
UTC_TIMESTAMP: Final = pa.timestamp("us", tz="UTC")
_JSONL_BATCH_MAX_ROWS: Final = 2_000
_JSONL_BATCH_MAX_BYTES: Final = 16 * 1024 * 1024
_RAW_EVENT_WINDOW_DATASETS: Final = frozenset(
    {
        "raw_orderbook_event_windows.parquet",
        "raw_trades_event_windows.parquet",
        "raw_last_price_event_windows.parquet",
        "raw_candles_event_windows.parquet",
    }
)


class DatasetStoreError(RuntimeError):
    """Base dataset-store error."""


class UnknownDatasetError(DatasetStoreError, KeyError):
    """A name outside the fixed daily dataset contract was requested."""


class DatasetClosedError(DatasetStoreError):
    """A finalized session dataset was opened for append."""


class DatasetMaterializationError(DatasetStoreError):
    """An active JSONL file could not be safely materialized."""


class DuplicatePrimaryKeyError(DatasetMaterializationError):
    """A dataset contains the same stable primary ID more than once."""


def _field(name: str, data_type: pa.DataType, *, nullable: bool = True) -> pa.Field:
    return pa.field(name, data_type, nullable=nullable)


def _schema(
    dataset: str,
    primary_key: str,
    fields: Sequence[pa.Field],
    *,
    foreign_keys: Mapping[str, str] | None = None,
) -> pa.Schema:
    names = {field.name for field in fields}
    if primary_key not in names:
        raise AssertionError(f"{dataset}: missing primary key field")
    full_fields = list(fields)
    if "extra_json" not in names:
        full_fields.append(_field("extra_json", pa.large_string()))
    metadata = {
        b"dataset": dataset.encode("utf-8"),
        b"schema_version": b"neobitcoin-paper-v1",
        b"primary_key": primary_key.encode("utf-8"),
        b"foreign_keys": json.dumps(
            foreign_keys or {}, sort_keys=True, separators=(",", ":")
        ).encode("utf-8"),
        b"timestamp_timezone": b"UTC",
    }
    return pa.schema(full_fields, metadata=metadata)


_SESSION_EVENT = (
    _field("session_date", pa.date32(), nullable=False),
    _field("event_ts", UTC_TIMESTAMP, nullable=False),
)


DATASET_SCHEMAS: Final[dict[str, pa.Schema]] = {
    "market_status_events.parquet": _schema(
        "market_status_events.parquet",
        "event_id",
        (
            _field("event_id", pa.string(), nullable=False),
            *_SESSION_EVENT,
            _field("instrument_uid", pa.string()),
            _field("trading_status", pa.string()),
            _field("is_trading_allowed", pa.bool_()),
            _field("source", pa.string()),
            _field("payload_json", pa.large_string()),
        ),
    ),
    "health_events.parquet": _schema(
        "health_events.parquet",
        "event_id",
        (
            _field("event_id", pa.string(), nullable=False),
            *_SESSION_EVENT,
            _field("component", pa.string()),
            _field("status", pa.string()),
            _field("heartbeat_age_ms", pa.float64()),
            _field("queue_size", pa.int64()),
            _field("details_json", pa.large_string()),
        ),
    ),
    "data_quality_events.parquet": _schema(
        "data_quality_events.parquet",
        "event_id",
        (
            _field("event_id", pa.string(), nullable=False),
            *_SESSION_EVENT,
            _field("instrument_uid", pa.string()),
            _field("kind", pa.string()),
            _field("severity", pa.string()),
            _field("feature_ready", pa.bool_()),
            _field("source_event_id", pa.string()),
            _field("reconnect_generation", pa.int64()),
            _field("gap_status", pa.string()),
            _field("latency_ms", pa.float64()),
            _field("details_json", pa.large_string()),
        ),
    ),
    "strategy_evaluations.parquet": _schema(
        "strategy_evaluations.parquet",
        "evaluation_id",
        (
            _field("evaluation_id", pa.string(), nullable=False),
            *_SESSION_EVENT,
            _field("strategy_id", pa.string()),
            _field("strategy_version", pa.string()),
            _field("account_id", pa.string()),
            _field("instrument_uid", pa.string()),
            _field("decision", pa.string()),
            _field("reason", pa.string()),
            _field("features_json", pa.large_string()),
        ),
        foreign_keys={"account_id": "virtual_accounts.account_id"},
    ),
    "candidate_signals.parquet": _schema(
        "candidate_signals.parquet",
        "signal_id",
        (
            _field("signal_id", pa.string(), nullable=False),
            *_SESSION_EVENT,
            _field("evaluation_id", pa.string()),
            _field("strategy_id", pa.string()),
            _field("strategy_version", pa.string()),
            _field("account_id", pa.string()),
            _field("instrument_uid", pa.string()),
            _field("side", pa.string()),
            _field("reference_price", pa.float64()),
            _field("accepted", pa.bool_()),
            _field("rejection_reason", pa.string()),
        ),
        foreign_keys={"evaluation_id": "strategy_evaluations.evaluation_id"},
    ),
    "filter_decisions.parquet": _schema(
        "filter_decisions.parquet",
        "decision_id",
        (
            _field("decision_id", pa.string(), nullable=False),
            *_SESSION_EVENT,
            _field("signal_id", pa.string()),
            _field("strategy_id", pa.string()),
            _field("strategy_version", pa.string()),
            _field("filter_name", pa.string()),
            _field("passed", pa.bool_()),
            _field("value", pa.float64()),
            _field("threshold", pa.float64()),
            _field("reason", pa.string()),
        ),
        foreign_keys={"signal_id": "candidate_signals.signal_id"},
    ),
    "paper_orders.parquet": _schema(
        "paper_orders.parquet",
        "order_id",
        (
            _field("order_id", pa.string(), nullable=False),
            *_SESSION_EVENT,
            _field("idempotency_key", pa.string()),
            _field("signal_id", pa.string()),
            _field("strategy_id", pa.string()),
            _field("strategy_version", pa.string()),
            _field("account_id", pa.string()),
            _field("instrument_uid", pa.string()),
            _field("side", pa.string()),
            _field("order_type", pa.string()),
            _field("status", pa.string()),
            _field("requested_quantity", pa.float64()),
            _field("filled_quantity", pa.float64()),
            _field("limit_price", pa.float64()),
            _field("stop_price", pa.float64()),
            _field("source_event_id", pa.string()),
            _field("session_label", pa.string()),
            _field("vwap", pa.float64()),
            _field("top_price", pa.float64()),
            _field("spread_cost", pa.float64()),
            _field("slippage_cost", pa.float64()),
            _field("simulated_latency_cost", pa.float64()),
            _field("holding_cost", pa.float64()),
        ),
        foreign_keys={"signal_id": "candidate_signals.signal_id"},
    ),
    "paper_fills.parquet": _schema(
        "paper_fills.parquet",
        "fill_id",
        (
            _field("fill_id", pa.string(), nullable=False),
            *_SESSION_EVENT,
            _field("order_id", pa.string()),
            _field("account_id", pa.string()),
            _field("instrument_uid", pa.string()),
            _field("side", pa.string()),
            _field("quantity", pa.float64()),
            _field("price", pa.float64()),
            _field("fee", pa.float64()),
            _field("liquidity", pa.string()),
            _field("source_event_id", pa.string()),
        ),
        foreign_keys={"order_id": "paper_orders.order_id"},
    ),
    "paper_positions.parquet": _schema(
        "paper_positions.parquet",
        "position_event_id",
        (
            _field("position_event_id", pa.string(), nullable=False),
            *_SESSION_EVENT,
            _field("position_id", pa.string()),
            _field("account_id", pa.string()),
            _field("strategy_id", pa.string()),
            _field("strategy_version", pa.string()),
            _field("instrument_uid", pa.string()),
            _field("side", pa.string()),
            _field("quantity", pa.float64()),
            _field("average_entry_price", pa.float64()),
            _field("realized_pnl", pa.float64()),
            _field("unrealized_pnl", pa.float64()),
            _field("status", pa.string()),
            _field("event_kind", pa.string()),
        ),
        foreign_keys={"account_id": "virtual_accounts.account_id"},
    ),
    "paper_trades.parquet": _schema(
        "paper_trades.parquet",
        "trade_id",
        (
            _field("trade_id", pa.string(), nullable=False),
            *_SESSION_EVENT,
            _field("position_id", pa.string()),
            _field("entry_fill_id", pa.string()),
            _field("exit_fill_id", pa.string()),
            _field("strategy_id", pa.string()),
            _field("strategy_version", pa.string()),
            _field("account_id", pa.string()),
            _field("instrument_uid", pa.string()),
            _field("side", pa.string()),
            _field("quantity", pa.float64()),
            _field("entry_price", pa.float64()),
            _field("exit_price", pa.float64()),
            _field("gross_pnl", pa.float64()),
            _field("fees", pa.float64()),
            _field("net_pnl", pa.float64()),
            _field("raw_pnl_ticks", pa.float64()),
            _field("stress_entry_price", pa.float64()),
            _field("stress_exit_price", pa.float64()),
            _field("stress_pnl", pa.float64()),
            _field("stress_net_pnl", pa.float64()),
            _field("stress_pnl_ticks", pa.float64()),
            _field("stress_slippage_ticks_each_side", pa.float64()),
            _field("entry_ts", UTC_TIMESTAMP),
            _field("exit_ts", UTC_TIMESTAMP),
            _field("entry_event_id", pa.string()),
            _field("exit_event_id", pa.string()),
            _field("exit_reason", pa.string()),
            _field("holding_duration_seconds", pa.float64()),
            _field("spread_cost", pa.float64()),
            _field("slippage_cost", pa.float64()),
            _field("simulated_latency_cost", pa.float64()),
            _field("holding_cost", pa.float64()),
        ),
        foreign_keys={
            "entry_fill_id": "paper_fills.fill_id",
            "exit_fill_id": "paper_fills.fill_id",
        },
    ),
    "equity_curve.parquet": _schema(
        "equity_curve.parquet",
        "equity_id",
        (
            _field("equity_id", pa.string(), nullable=False),
            *_SESSION_EVENT,
            _field("account_id", pa.string()),
            _field("strategy_id", pa.string()),
            _field("strategy_version", pa.string()),
            _field("cash", pa.float64()),
            _field("equity", pa.float64()),
            _field("realized_pnl", pa.float64()),
            _field("unrealized_pnl", pa.float64()),
            _field("drawdown", pa.float64()),
        ),
        foreign_keys={"account_id": "virtual_accounts.account_id"},
    ),
    "mfe_mae.parquet": _schema(
        "mfe_mae.parquet",
        "result_id",
        (
            _field("result_id", pa.string(), nullable=False),
            *_SESSION_EVENT,
            _field("trade_id", pa.string()),
            _field("position_id", pa.string()),
            _field("strategy_id", pa.string()),
            _field("strategy_version", pa.string()),
            _field("horizon_seconds", pa.int32()),
            _field("mfe", pa.float64()),
            _field("mae", pa.float64()),
            _field("mfe_ticks", pa.float64()),
            _field("mae_ticks", pa.float64()),
            _field("mfe_source_event_id", pa.string()),
            _field("mae_source_event_id", pa.string()),
            _field("time_to_mfe_seconds", pa.float64()),
            _field("time_to_mae_seconds", pa.float64()),
        ),
        foreign_keys={"trade_id": "paper_trades.trade_id"},
    ),
    "shadow_stop_results.parquet": _schema(
        "shadow_stop_results.parquet",
        "result_id",
        (
            _field("result_id", pa.string(), nullable=False),
            *_SESSION_EVENT,
            _field("signal_id", pa.string()),
            _field("order_id", pa.string()),
            _field("position_id", pa.string()),
            _field("strategy_id", pa.string()),
            _field("strategy_version", pa.string()),
            _field("stop_ticks", pa.int32()),
            _field("stop_level", pa.float64()),
            _field("triggered", pa.bool_()),
            _field("exit_price", pa.float64()),
            _field("pnl", pa.float64()),
            _field("source_event_id", pa.string()),
        ),
        foreign_keys={"signal_id": "candidate_signals.signal_id"},
    ),
    "shadow_exit_results.parquet": _schema(
        "shadow_exit_results.parquet",
        "result_id",
        (
            _field("result_id", pa.string(), nullable=False),
            *_SESSION_EVENT,
            _field("signal_id", pa.string()),
            _field("order_id", pa.string()),
            _field("position_id", pa.string()),
            _field("strategy_id", pa.string()),
            _field("strategy_version", pa.string()),
            _field("exit_model", pa.string()),
            _field("triggered", pa.bool_()),
            _field("trigger_reason", pa.string()),
            _field("exit_price", pa.float64()),
            _field("pnl", pa.float64()),
            _field("source_event_id", pa.string()),
        ),
        foreign_keys={"signal_id": "candidate_signals.signal_id"},
    ),
    "raw_orderbook_event_windows.parquet": _schema(
        "raw_orderbook_event_windows.parquet",
        "raw_event_id",
        (
            _field("raw_event_id", pa.string(), nullable=False),
            _field("source_event_id", pa.string()),
            *_SESSION_EVENT,
            _field("receive_ts", UTC_TIMESTAMP),
            _field("window_id", pa.string()),
            _field("signal_id", pa.string()),
            _field("instrument_uid", pa.string()),
            _field("best_bid", pa.float64()),
            _field("best_ask", pa.float64()),
            _field("bids_json", pa.large_string()),
            _field("asks_json", pa.large_string()),
            _field("payload_json", pa.large_string()),
        ),
        foreign_keys={"signal_id": "candidate_signals.signal_id"},
    ),
    "raw_trades_event_windows.parquet": _schema(
        "raw_trades_event_windows.parquet",
        "raw_event_id",
        (
            _field("raw_event_id", pa.string(), nullable=False),
            _field("source_event_id", pa.string()),
            *_SESSION_EVENT,
            _field("receive_ts", UTC_TIMESTAMP),
            _field("window_id", pa.string()),
            _field("signal_id", pa.string()),
            _field("instrument_uid", pa.string()),
            _field("direction", pa.string()),
            _field("price", pa.float64()),
            _field("quantity", pa.float64()),
            _field("payload_json", pa.large_string()),
        ),
        foreign_keys={"signal_id": "candidate_signals.signal_id"},
    ),
    "raw_last_price_event_windows.parquet": _schema(
        "raw_last_price_event_windows.parquet",
        "raw_event_id",
        (
            _field("raw_event_id", pa.string(), nullable=False),
            _field("source_event_id", pa.string()),
            *_SESSION_EVENT,
            _field("receive_ts", UTC_TIMESTAMP),
            _field("window_id", pa.string()),
            _field("signal_id", pa.string()),
            _field("instrument_uid", pa.string()),
            _field("price", pa.float64()),
            _field("payload_json", pa.large_string()),
        ),
        foreign_keys={"signal_id": "candidate_signals.signal_id"},
    ),
    "raw_candles_event_windows.parquet": _schema(
        "raw_candles_event_windows.parquet",
        "raw_event_id",
        (
            _field("raw_event_id", pa.string(), nullable=False),
            _field("source_event_id", pa.string()),
            *_SESSION_EVENT,
            _field("receive_ts", UTC_TIMESTAMP),
            _field("window_id", pa.string()),
            _field("signal_id", pa.string()),
            _field("instrument_uid", pa.string()),
            _field("interval", pa.string()),
            _field("open", pa.float64()),
            _field("high", pa.float64()),
            _field("low", pa.float64()),
            _field("close", pa.float64()),
            _field("volume", pa.float64()),
            _field("is_complete", pa.bool_()),
            _field("payload_json", pa.large_string()),
        ),
        foreign_keys={"signal_id": "candidate_signals.signal_id"},
    ),
    "strategy_errors.parquet": _schema(
        "strategy_errors.parquet",
        "error_id",
        (
            _field("error_id", pa.string(), nullable=False),
            *_SESSION_EVENT,
            _field("strategy_id", pa.string()),
            _field("strategy_version", pa.string()),
            _field("component", pa.string()),
            _field("error_type", pa.string()),
            _field("message", pa.large_string()),
            _field("details_json", pa.large_string()),
        ),
    ),
    "delivery_events.parquet": _schema(
        "delivery_events.parquet",
        "delivery_event_id",
        (
            _field("delivery_event_id", pa.string(), nullable=False),
            *_SESSION_EVENT,
            _field("archive_id", pa.string()),
            _field("archive_sha256", pa.string()),
            _field("thread_id", pa.string()),
            _field("turn_run_id", pa.string()),
            _field("status", pa.string()),
            _field("attempt_count", pa.int32()),
            _field("error", pa.large_string()),
        ),
        foreign_keys={"archive_id": "archives.archive_id"},
    ),
}

if tuple(DATASET_SCHEMAS) != REQUIRED_DATASETS:
    raise AssertionError("dataset schema order must match the required contract")


class JsonlDatasetWriter:
    """One flush-on-append crash-recoverable JSONL writer."""

    def __init__(self, path: Path, *, durable_writes: bool) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._durable_writes = durable_writes
        self._handle: TextIO = self.path.open("a", encoding="utf-8", newline="\n")
        self._lock = threading.Lock()
        self._closed = False

    def append(self, row: Mapping[str, Any]) -> None:
        if self._closed:
            raise DatasetClosedError(f"writer is closed: {self.path.name}")
        payload = json.dumps(
            row,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=_json_default,
        )
        with self._lock:
            self._handle.write(payload + "\n")
            self._handle.flush()
            if self._durable_writes:
                os.fsync(self._handle.fileno())

    def close(self) -> None:
        if self._closed:
            return
        with self._lock:
            self._handle.flush()
            if self._durable_writes:
                os.fsync(self._handle.fileno())
            self._handle.close()
            self._closed = True


class DatasetStore:
    """Session-scoped dataset writer and atomic Parquet finalizer."""

    def __init__(
        self,
        root: str | Path,
        session_date: date | str,
        *,
        durable_writes: bool = True,
    ) -> None:
        self.root = Path(root).resolve()
        self.session_date = _date(session_date)
        self.active_dir = self.root / "active" / self.session_date.isoformat()
        self.parquet_dir = self.root / "parquet" / self.session_date.isoformat()
        self.active_dir.mkdir(parents=True, exist_ok=True)
        self.parquet_dir.mkdir(parents=True, exist_ok=True)
        self._durable_writes = durable_writes
        self._writers: dict[str, JsonlDatasetWriter] = {}
        self._lock = threading.RLock()
        self._closed = False

    def __enter__(self) -> DatasetStore:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if exc_type is None:
            self.close()
        else:
            self.abort()

    def active_path(self, dataset: str) -> Path:
        canonical = _dataset_file(dataset)
        stem = canonical.removesuffix(".parquet")
        return self.active_dir / f"{stem}.jsonl.inprogress"

    def parquet_path(self, dataset: str) -> Path:
        return self.parquet_dir / _dataset_file(dataset)

    def writer(self, dataset: str) -> JsonlDatasetWriter:
        canonical = _dataset_file(dataset)
        with self._lock:
            self._ensure_open()
            if self.parquet_path(canonical).exists() and not self.active_path(canonical).exists():
                raise DatasetClosedError(f"dataset is already finalized: {canonical}")
            writer = self._writers.get(canonical)
            if writer is None:
                writer = JsonlDatasetWriter(
                    self.active_path(canonical), durable_writes=self._durable_writes
                )
                self._writers[canonical] = writer
            return writer

    def append(self, dataset: str, row: Mapping[str, Any]) -> None:
        payload = dict(row)
        payload.setdefault("session_date", self.session_date.isoformat())
        self.writer(dataset).append(payload)

    def close(self) -> dict[str, Path]:
        """Finalize every required dataset and remove all active markers."""

        with self._lock:
            if self._closed:
                return {name: self.parquet_path(name) for name in REQUIRED_DATASETS}
            for writer in self._writers.values():
                writer.close()

            staged: dict[str, tuple[Path, Path, Path | None]] = {}
            result: dict[str, Path] = {}
            for dataset in REQUIRED_DATASETS:
                active = self.active_path(dataset)
                final = self.parquet_path(dataset)
                result[dataset] = final
                if final.exists() and not active.exists():
                    continue
                temporary = final.with_suffix(final.suffix + ".inprogress")
                temporary.parent.mkdir(parents=True, exist_ok=True)
                _materialize_parquet(
                    dataset,
                    active if active.exists() else None,
                    temporary,
                    self.session_date,
                )
                written_schema = pq.read_schema(temporary)
                if not written_schema.equals(DATASET_SCHEMAS[dataset], check_metadata=True):
                    raise DatasetMaterializationError(f"schema changed while writing {dataset}")
                staged[dataset] = (temporary, final, active if active.exists() else None)

            for temporary, final, _active in staged.values():
                os.replace(temporary, final)
            for _temporary, _final, active_marker in staged.values():
                if active_marker is not None:
                    active_marker.unlink(missing_ok=True)

            leftovers = (
                *self.active_dir.rglob("*.inprogress"),
                *self.parquet_dir.rglob("*.inprogress"),
            )
            if leftovers:
                names = ", ".join(str(path) for path in leftovers[:3])
                raise DatasetMaterializationError(f"active markers remain after close: {names}")
            self._closed = True
            return result

    def abort(self) -> None:
        """Close handles but retain JSONL markers for restart recovery."""

        with self._lock:
            for writer in self._writers.values():
                writer.close()
            self._closed = True

    def row_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for dataset in REQUIRED_DATASETS:
            path = self.parquet_path(dataset)
            if not path.is_file():
                raise DatasetMaterializationError(f"missing finalized dataset: {dataset}")
            counts[dataset] = pq.ParquetFile(path).metadata.num_rows
        return counts

    def _ensure_open(self) -> None:
        if self._closed:
            raise DatasetClosedError("dataset store is closed")


def _build_table(
    dataset: str,
    rows: Sequence[Mapping[str, Any]],
    session: date,
    *,
    seen_primary_keys: set[str] | None = None,
) -> pa.Table:
    schema = DATASET_SCHEMAS[dataset]
    primary_key = (schema.metadata or {})[b"primary_key"].decode("utf-8")
    normalized = [_normalize_row(row, schema, primary_key, session) for row in rows]
    seen = seen_primary_keys if seen_primary_keys is not None else set()
    for row in normalized:
        identifier = str(row[primary_key])
        if identifier in seen:
            raise DuplicatePrimaryKeyError(f"{dataset}: duplicate {primary_key}={identifier}")
        seen.add(identifier)
    normalized.sort(
        key=lambda row: (
            row.get("event_ts") or datetime.max.replace(tzinfo=UTC),
            str(row.get(primary_key) or ""),
        )
    )
    try:
        return pa.Table.from_pylist(normalized, schema=schema)
    except (ArrowError, TypeError, ValueError) as exc:
        raise DatasetMaterializationError(f"{dataset}: {exc}") from exc


def _materialize_parquet(
    dataset: str,
    active: Path | None,
    temporary: Path,
    session: date,
) -> None:
    """Write bounded row groups instead of loading a whole trading day in RAM."""

    schema = DATASET_SCHEMAS[dataset]
    # Raw windows can contain millions of stable IDs.  Keeping every ID in a
    # Python set would make finalization memory-linear; the archive validator
    # performs the global DISTINCT check with DuckDB after Parquet is written.
    # A fresh per-batch set still rejects immediate duplicates while writing.
    seen_primary_keys: set[str] | None = (
        None if dataset in _RAW_EVENT_WINDOW_DATASETS else set()
    )
    previous_event_ts: datetime | None = None
    writer: pq.ParquetWriter | None = None
    try:
        batches = _iter_jsonl_batches(active) if active is not None else ()
        for rows in batches:
            table = _build_table(
                dataset,
                rows,
                session,
                seen_primary_keys=seen_primary_keys,
            )
            timestamps = table["event_ts"].to_pylist()
            if (
                dataset not in _RAW_EVENT_WINDOW_DATASETS
                and timestamps
                and previous_event_ts is not None
                and timestamps[0] < previous_event_ts
            ):
                raise DatasetMaterializationError(
                    f"{dataset}: event_ts order crosses a streaming row-group boundary"
                )
            if timestamps:
                previous_event_ts = timestamps[-1]
            if writer is None:
                writer = pq.ParquetWriter(
                    temporary,
                    schema,
                    compression="zstd",
                    use_dictionary=True,
                    write_statistics=True,
                    version="2.6",
                    data_page_version="2.0",
                )
            writer.write_table(table)
        if writer is None:
            pq.write_table(
                _build_table(dataset, (), session),
                temporary,
                compression="zstd",
                use_dictionary=True,
                write_statistics=True,
                version="2.6",
                data_page_version="2.0",
            )
    finally:
        if writer is not None:
            writer.close()


def _normalize_row(
    row: Mapping[str, Any], schema: pa.Schema, primary_key: str, session: date
) -> dict[str, Any]:
    source = dict(row)
    if primary_key not in source:
        for alias in ("event_id", "id"):
            if source.get(alias) is not None:
                source[primary_key] = source[alias]
                break
    if source.get(primary_key) in (None, ""):
        raise DatasetMaterializationError(f"missing non-empty primary key {primary_key}")
    source.setdefault("session_date", session)
    if source.get("event_ts") is None:
        for alias in (
            "exchange_ts",
            "timestamp",
            "created_at",
            "updated_at",
            "receive_ts",
        ):
            if source.get(alias) is not None:
                source["event_ts"] = source[alias]
                break
    if source.get("event_ts") is None:
        raise DatasetMaterializationError("event_ts (or a timestamp alias) is required")

    field_names = set(schema.names)
    extras = {key: value for key, value in source.items() if key not in field_names}
    if extras:
        existing = source.get("extra_json")
        if existing:
            extras = {"declared_extra": existing, **extras}
        source["extra_json"] = extras
    normalized: dict[str, Any] = {}
    for field in schema:
        value = source.get(field.name)
        if not field.nullable and value is None:
            raise DatasetMaterializationError(f"missing required field {field.name}")
        normalized[field.name] = _coerce(value, field.type)
    return normalized


def _coerce(value: Any, data_type: pa.DataType) -> Any:
    if value is None:
        return None
    if pa.types.is_timestamp(data_type):
        parsed = (
            value
            if isinstance(value, datetime)
            else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        )
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    if pa.types.is_date(data_type):
        if isinstance(value, datetime):
            return value.date()
        return value if isinstance(value, date) else date.fromisoformat(str(value))
    if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        if isinstance(value, (Mapping, list, tuple)):
            return json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                default=_json_default,
            )
        return str(value)
    if pa.types.is_boolean(data_type):
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "1", "yes", "on"}:
                return True
            if normalized in {"false", "0", "no", "off"}:
                return False
            raise ValueError(f"invalid boolean value {value!r}")
        return bool(value)
    if pa.types.is_integer(data_type):
        return int(value)
    if pa.types.is_floating(data_type):
        numeric = float(value)
        if not numeric == numeric or numeric in {float("inf"), float("-inf")}:
            raise ValueError("numeric dataset values must be finite")
        return numeric
    return value


def _iter_jsonl_batches(path: Path) -> Iterator[list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    batch_bytes = 0
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise DatasetMaterializationError(
                    f"{path.name}:{line_number}: malformed JSONL"
                ) from exc
            if not isinstance(value, dict):
                raise DatasetMaterializationError(
                    f"{path.name}:{line_number}: row must be an object"
                )
            rows.append(value)
            batch_bytes += len(line.encode("utf-8"))
            if len(rows) >= _JSONL_BATCH_MAX_ROWS or batch_bytes >= _JSONL_BATCH_MAX_BYTES:
                yield rows
                rows = []
                batch_bytes = 0
    if rows:
        yield rows


def _dataset_file(dataset: str) -> str:
    canonical = str(dataset).strip()
    if not canonical.endswith(".parquet"):
        canonical += ".parquet"
    if canonical not in DATASET_SCHEMAS:
        raise UnknownDatasetError(canonical)
    return canonical


def _date(value: date | str) -> date:
    return value if isinstance(value, date) else date.fromisoformat(value)


def _json_default(value: object) -> object:
    if isinstance(value, datetime):
        parsed = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return parsed.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


# PyArrow exposes ArrowException in different public locations across releases.
ArrowError = getattr(pa, "ArrowException", Exception)


__all__ = [
    "DATASET_NAMES",
    "DATASET_SCHEMAS",
    "DatasetClosedError",
    "DatasetMaterializationError",
    "DatasetStore",
    "DatasetStoreError",
    "DuplicatePrimaryKeyError",
    "JsonlDatasetWriter",
    "REQUIRED_DATASETS",
    "UTC_TIMESTAMP",
    "UnknownDatasetError",
]
