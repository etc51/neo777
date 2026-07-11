"""Durable append-only storage for Neobitcoin market-data research.

Raw events are written to hourly JSONL write-ahead logs before their
idempotency record is committed to SQLite.  Parquet compaction reads an
immutable snapshot of a WAL and atomically replaces only the derived parquet
file; it never truncates or rewrites the source WAL.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import sqlite3
import threading
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Final, TypeAlias, cast
from uuid import uuid4

from .buffered_parquet import BufferedParquetWriter, ParquetBatchPolicy

JsonMapping: TypeAlias = Mapping[str, Any]

RAW_REQUIRED_FIELDS: Final = frozenset(
    {
        "event_id",
        "instrument_uid",
        "event_type",
        "exchange_timestamp",
        "receive_timestamp",
        "payload",
        "schema_version",
    }
)

DERIVED_DATASETS: Final = (
    "feature_snapshots",
    "candidate_events",
    "future_outcomes",
    "execution_simulations",
    "shadow_predictions",
    "shadow_trades",
    "model_registry",
    "experiment_registry",
    "data_quality_metrics",
)


class StorageError(RuntimeError):
    """Base error for research storage failures."""


class InvalidRecordError(StorageError, ValueError):
    """A raw or derived record is invalid."""


class ConcurrentWalUpdateError(StorageError):
    """A WAL changed while a compaction snapshot was being written."""


@dataclass(frozen=True)
class RawAppendResult:
    """Outcome of one idempotent raw-event append."""

    event_id: str
    appended: bool
    wal_path: Path
    byte_offset: int


@dataclass(frozen=True)
class DerivedAppendResult:
    """Outcome of one idempotent derived-dataset append."""

    dataset: str
    event_id: str
    appended: bool
    parquet_path: Path


@dataclass(frozen=True)
class CompactionResult:
    """Metadata for one atomic hourly WAL compaction."""

    wal_path: Path
    parquet_path: Path
    row_count: int
    source_sha256: str
    source_size: int


@dataclass(frozen=True)
class DuckDBCatalogResult:
    """Persistent DuckDB catalog refresh result."""

    catalog_path: Path
    views: tuple[str, ...]


class ResearchStorage:
    """Append-only raw/derived storage with SQLite idempotency state."""

    def __init__(
        self,
        root: Path | str = Path("data"),
        *,
        state_db_path: Path | str | None = None,
        fsync: bool = False,
    ) -> None:
        self.root = Path(root)
        self.raw_root = self.root / "raw"
        self.parquet_root = self.root / "parquet"
        self.derived_root = self.root / "derived"
        self.derived_parquet_root = self.root / "derived_parquet"
        self.state_db_path = (
            Path(state_db_path)
            if state_db_path is not None
            else self.root / "research_state.sqlite"
        )
        self.fsync = fsync
        self._lock = threading.RLock()
        # One active private writer per dataset/hour.  The writer publishes a
        # single final part only when the partition rotates or storage closes.
        self._derived_writers: dict[tuple[str, str, str], BufferedParquetWriter] = {}
        self.state_db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(
            self.state_db_path,
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute(f"PRAGMA synchronous={'FULL' if fsync else 'NORMAL'}")
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._initialize_schema()
        # Research payloads are authoritative in Parquet.  Never backfill
        # record_json from parts: that turns the bounded runtime catalog back
        # into an unbounded market-data store.

    def __enter__(self) -> ResearchStorage:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def close(self) -> None:
        """Close the SQLite state catalog."""

        with self._lock:
            for writer in self._derived_writers.values():
                writer.finalize()
            self._derived_writers.clear()
            self._connection.close()

    def append_raw_event(self, event: JsonMapping) -> RawAppendResult:
        """Append one raw event unless its event_id was already committed."""

        normalized = _normalize_raw_event(event)
        event_id = cast(str, normalized["event_id"])
        instrument_uid = cast(str, normalized["instrument_uid"])
        receive_timestamp = _parse_timestamp(normalized["receive_timestamp"], "receive_timestamp")
        wal_path = self._wal_path(instrument_uid, receive_timestamp)
        line = _json_bytes(normalized) + b"\n"

        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                existing = self._connection.execute(
                    "SELECT wal_path, byte_offset FROM raw_event_index WHERE event_id = ?",
                    (event_id,),
                ).fetchone()
                if existing is not None:
                    self._connection.commit()
                    return RawAppendResult(
                        event_id=event_id,
                        appended=False,
                        wal_path=Path(existing["wal_path"]),
                        byte_offset=int(existing["byte_offset"]),
                    )

                wal_path.parent.mkdir(parents=True, exist_ok=True)
                with wal_path.open("ab+") as stream:
                    stream.seek(0, os.SEEK_END)
                    offset = stream.tell()
                    try:
                        stream.write(line)
                        stream.flush()
                        if self.fsync:
                            os.fsync(stream.fileno())
                        self._connection.execute(
                            """
                            INSERT INTO raw_event_index(
                                event_id, instrument_uid, event_type,
                                exchange_timestamp, receive_timestamp,
                                wal_path, byte_offset, byte_length, stored_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                event_id,
                                instrument_uid,
                                normalized["event_type"],
                                normalized["exchange_timestamp"],
                                normalized["receive_timestamp"],
                                str(wal_path),
                                offset,
                                len(line),
                                _utc_now().isoformat(),
                            ),
                        )
                        self._connection.commit()
                    except BaseException:
                        stream.seek(offset)
                        stream.truncate()
                        stream.flush()
                        if self.fsync:
                            os.fsync(stream.fileno())
                        raise
            except BaseException:
                if self._connection.in_transaction:
                    self._connection.rollback()
                raise

        return RawAppendResult(
            event_id=event_id,
            appended=True,
            wal_path=wal_path,
            byte_offset=offset,
        )

    def append_raw_events(self, events: Iterable[JsonMapping]) -> tuple[RawAppendResult, ...]:
        """Append a sequence of raw events using the same idempotency rules."""

        return tuple(self.append_raw_event(event) for event in events)

    def set_state(
        self,
        key: str,
        value: Any,
        *,
        updated_at: datetime | str | None = None,
    ) -> None:
        """Atomically upsert a JSON service-state value."""

        normalized_key = _required_text(key, "key")
        timestamp = _as_utc_timestamp(updated_at or _utc_now(), "updated_at")
        value_json = _json_text(value)
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO service_state(key, value_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value_json = excluded.value_json,
                    updated_at = excluded.updated_at
                """,
                (normalized_key, value_json, timestamp),
            )

    def get_state(self, key: str, default: Any = None) -> Any:
        """Read and deserialize one service-state value."""

        row = self._connection.execute(
            "SELECT value_json FROM service_state WHERE key = ?",
            (_required_text(key, "key"),),
        ).fetchone()
        return default if row is None else json.loads(row["value_json"])

    def start_session(
        self,
        session_id: str,
        *,
        source: str,
        started_at: datetime | str,
        stream_id: str | None = None,
        metadata: JsonMapping | None = None,
    ) -> bool:
        """Register a stream session; duplicate session ids are no-ops."""

        with self._lock, self._connection:
            cursor = self._connection.execute(
                """
                INSERT OR IGNORE INTO stream_sessions(
                    session_id, stream_id, source, started_at, status, metadata_json
                ) VALUES (?, ?, ?, ?, 'running', ?)
                """,
                (
                    _required_text(session_id, "session_id"),
                    _optional_text(stream_id),
                    _required_text(source, "source"),
                    _as_utc_timestamp(started_at, "started_at"),
                    _json_text(metadata or {}),
                ),
            )
        return cursor.rowcount == 1

    def finish_session(
        self,
        session_id: str,
        *,
        ended_at: datetime | str,
        status: str = "closed",
        error: str | None = None,
    ) -> bool:
        """Close an existing stream session."""

        with self._lock, self._connection:
            cursor = self._connection.execute(
                """
                UPDATE stream_sessions
                SET ended_at = ?, status = ?, error = ?
                WHERE session_id = ?
                """,
                (
                    _as_utc_timestamp(ended_at, "ended_at"),
                    _required_text(status, "status"),
                    error,
                    _required_text(session_id, "session_id"),
                ),
            )
        return cursor.rowcount == 1

    def record_subscription(
        self,
        *,
        session_id: str,
        subscription_id: str,
        instrument_uid: str,
        event_type: str,
        action: str,
        event_time: datetime | str,
        payload: JsonMapping | None = None,
        event_id: str | None = None,
    ) -> bool:
        """Append an idempotent subscription lifecycle event."""

        normalized = {
            "session_id": _required_text(session_id, "session_id"),
            "subscription_id": _required_text(subscription_id, "subscription_id"),
            "instrument_uid": _partition_value(instrument_uid, "instrument_uid"),
            "event_type": _required_text(event_type, "event_type"),
            "action": _required_text(action, "action"),
            "event_time": _as_utc_timestamp(event_time, "event_time"),
            "payload": dict(payload or {}),
        }
        normalized_event_id = event_id or _content_id("subscription", normalized)
        with self._lock, self._connection:
            cursor = self._connection.execute(
                """
                INSERT OR IGNORE INTO subscription_events(
                    event_id, session_id, subscription_id, instrument_uid,
                    event_type, action, event_time, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_event_id,
                    normalized["session_id"],
                    normalized["subscription_id"],
                    normalized["instrument_uid"],
                    normalized["event_type"],
                    normalized["action"],
                    normalized["event_time"],
                    _json_text(normalized["payload"]),
                ),
            )
        return cursor.rowcount == 1

    def record_gap(
        self,
        *,
        gap_id: str,
        session_id: str | None,
        instrument_uid: str,
        event_type: str,
        started_at: datetime | str,
        ended_at: datetime | str | None = None,
        recoverable: bool = False,
        reason: str | None = None,
        metadata: JsonMapping | None = None,
    ) -> bool:
        """Append an idempotent data-gap record."""

        start = _parse_timestamp(started_at, "started_at")
        end = _parse_timestamp(ended_at, "ended_at") if ended_at is not None else None
        if end is not None and end < start:
            raise InvalidRecordError("ended_at must not precede started_at.")
        duration = None if end is None else (end - start).total_seconds()
        with self._lock, self._connection:
            cursor = self._connection.execute(
                """
                INSERT OR IGNORE INTO data_gaps(
                    gap_id, session_id, instrument_uid, event_type,
                    started_at, ended_at, duration_seconds, recoverable,
                    reason, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _required_text(gap_id, "gap_id"),
                    _optional_text(session_id),
                    _partition_value(instrument_uid, "instrument_uid"),
                    _required_text(event_type, "event_type"),
                    start.isoformat(),
                    None if end is None else end.isoformat(),
                    duration,
                    int(recoverable),
                    reason,
                    _json_text(metadata or {}),
                ),
            )
        return cursor.rowcount == 1

    def append_derived(
        self,
        dataset: str,
        record: JsonMapping,
        *,
        event_id: str | None = None,
        recorded_at: datetime | str | None = None,
    ) -> DerivedAppendResult:
        """Write derived data directly to immutable ZSTD Parquet.

        SQLite retains only a compact deduplication key and file location.
        """

        normalized_dataset = _dataset_name(dataset)
        normalized_record = _json_compatible(dict(record))
        timestamp = _parse_timestamp(
            recorded_at
            or record.get("recorded_at")
            or record.get("exchange_timestamp")
            or record.get("timestamp")
            or _utc_now(),
            "recorded_at",
        )
        normalized_event_id = event_id or _derived_event_id(normalized_dataset, record)
        partition = (
            normalized_dataset,
            timestamp.date().isoformat(),
            timestamp.strftime("%H"),
        )
        self._rotate_derived_writers_before(timestamp)
        writer = self._derived_writer(partition)
        target = writer.final_path

        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                existing = self._connection.execute(
                    """
                    SELECT parquet_path FROM derived_event_index
                    WHERE dataset = ? AND event_id = ?
                    """,
                    (normalized_dataset, normalized_event_id),
                ).fetchone()
                if existing is not None:
                    self._connection.commit()
                    return DerivedAppendResult(
                        dataset=normalized_dataset,
                        event_id=normalized_event_id,
                        appended=False,
                        parquet_path=Path(existing["parquet_path"]),
                    )

                writer.append(
                    {
                        "dataset": normalized_dataset,
                        "event_id": normalized_event_id,
                        "recorded_at": timestamp.isoformat(),
                        "schema_version": str(normalized_record.get("schema_version", "1")),
                        "record_json": _json_text(normalized_record),
                    }
                )
                self._connection.execute(
                    """
                    INSERT INTO derived_event_index(
                        dataset, event_id, recorded_at, parquet_path,
                        stored_at, record_json
                    ) VALUES (?, ?, ?, ?, ?, NULL)
                    """,
                    (
                        normalized_dataset,
                        normalized_event_id,
                        timestamp.isoformat(),
                        str(target),
                        _utc_now().isoformat(),
                    ),
                )
                self._connection.commit()
            except BaseException:
                if self._connection.in_transaction:
                    self._connection.rollback()
                raise

        return DerivedAppendResult(
            dataset=normalized_dataset,
            event_id=normalized_event_id,
            appended=True,
            parquet_path=target,
        )

    def flush_derived_writers(self) -> None:
        """Flush aged/full batches to private .inprogress files.

        This deliberately does not publish a part; publication happens only on
        rotation/close, so a busy hour cannot produce per-record Parquet files.
        """

        with self._lock:
            for writer in self._derived_writers.values():
                writer.flush_if_due()

    def finalize_derived_writers(self) -> None:
        """Publish all active derived partitions for a consistent read/report."""

        with self._lock:
            for writer in self._derived_writers.values():
                writer.finalize()
            self._derived_writers.clear()

    def _derived_writer(
        self, partition: tuple[str, str, str]
    ) -> BufferedParquetWriter:
        existing = self._derived_writers.get(partition)
        if existing is not None:
            return existing
        dataset, partition_date, hour = partition
        pa = cast(Any, importlib.import_module("pyarrow"))
        target = (
            self.derived_parquet_root
            / dataset
            / f"date={partition_date}"
            / f"hour={hour}"
            / "part-00000.parquet"
        )
        writer = BufferedParquetWriter(
            target,
            pa.schema(
                [
                    ("dataset", pa.string()),
                    ("event_id", pa.string()),
                    ("recorded_at", pa.string()),
                    ("schema_version", pa.string()),
                    ("record_json", pa.string()),
                ]
            ),
            policy=ParquetBatchPolicy(),
            fsync=self.fsync,
        )
        self._derived_writers[partition] = writer
        return writer

    def _rotate_derived_writers_before(self, timestamp: datetime) -> None:
        """Publish partitions older than the record currently being appended."""

        current = (timestamp.date().isoformat(), timestamp.strftime("%H"))
        for partition, writer in tuple(self._derived_writers.items()):
            if partition[1:] != current:
                writer.finalize()
                del self._derived_writers[partition]

    def append_derived_many(
        self,
        dataset: str,
        records: Iterable[JsonMapping],
    ) -> tuple[DerivedAppendResult, ...]:
        """Append several records without retaining payload JSON in SQLite."""

        return tuple(self.append_derived(dataset, record) for record in records)

        normalized_dataset = _dataset_name(dataset)
        prepared: list[tuple[str, datetime, dict[str, Any], Path]] = []
        for record in records:
            normalized_record = cast(dict[str, Any], _json_compatible(dict(record)))
            timestamp = _parse_timestamp(
                record.get("recorded_at")
                or record.get("exchange_timestamp")
                or record.get("timestamp")
                or _utc_now(),
                "recorded_at",
            )
            event_id = _derived_event_id(normalized_dataset, record)
            target = (
                self.derived_parquet_root
                / normalized_dataset
                / f"date={timestamp.date().isoformat()}"
                / f"hour={timestamp.strftime('%H')}.parquet"
            )
            prepared.append((event_id, timestamp, normalized_record, target))
        if not prepared:
            return ()

        results: list[DerivedAppendResult] = []
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                for event_id, timestamp, record, target in prepared:
                    existing = self._connection.execute(
                        """
                        SELECT parquet_path FROM derived_event_index
                        WHERE dataset = ? AND event_id = ?
                        """,
                        (normalized_dataset, event_id),
                    ).fetchone()
                    if existing is not None:
                        results.append(
                            DerivedAppendResult(
                                dataset=normalized_dataset,
                                event_id=event_id,
                                appended=False,
                                parquet_path=Path(existing["parquet_path"]),
                            )
                        )
                        continue
                    self._connection.execute(
                        """
                        INSERT INTO derived_event_index(
                            dataset, event_id, recorded_at, parquet_path,
                            stored_at, record_json
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            normalized_dataset,
                            event_id,
                            timestamp.isoformat(),
                            str(target),
                            _utc_now().isoformat(),
                            _json_text(record),
                        ),
                    )
                    results.append(
                        DerivedAppendResult(
                            dataset=normalized_dataset,
                            event_id=event_id,
                            appended=True,
                            parquet_path=target,
                        )
                    )
                self._connection.commit()
            except BaseException:
                if self._connection.in_transaction:
                    self._connection.rollback()
                raise
        return tuple(results)

    def append_feature_snapshot(self, record: JsonMapping) -> DerivedAppendResult:
        return self.append_derived("feature_snapshots", record)

    def append_candidate_event(self, record: JsonMapping) -> DerivedAppendResult:
        return self.append_derived("candidate_events", record)

    def append_future_outcome(self, record: JsonMapping) -> DerivedAppendResult:
        return self.append_derived("future_outcomes", record)

    def append_execution_simulation(self, record: JsonMapping) -> DerivedAppendResult:
        return self.append_derived("execution_simulations", record)

    def append_shadow_prediction(self, record: JsonMapping) -> DerivedAppendResult:
        return self.append_derived("shadow_predictions", record)

    def append_shadow_trade(self, record: JsonMapping) -> DerivedAppendResult:
        return self.append_derived("shadow_trades", record)

    def append_model_registry(self, record: JsonMapping) -> DerivedAppendResult:
        return self.append_derived("model_registry", record)

    def append_experiment_registry(self, record: JsonMapping) -> DerivedAppendResult:
        return self.append_derived("experiment_registry", record)

    def append_data_quality_metric(self, record: JsonMapping) -> DerivedAppendResult:
        return self.append_derived("data_quality_metrics", record)

    def compact_hour(
        self,
        instrument_uid: str,
        partition_date: date | str,
        hour: int | str,
    ) -> CompactionResult:
        """Atomically compact one hourly JSONL WAL to zstd parquet."""

        uid = _partition_value(instrument_uid, "instrument_uid")
        day = _partition_date(partition_date)
        hour_text = _partition_hour(hour)
        wal_path = self.raw_root / uid / day / hour_text / "events.jsonl"
        if not wal_path.is_file():
            raise FileNotFoundError(wal_path)

        with self._lock:
            source_bytes = wal_path.read_bytes()
            source_sha256 = hashlib.sha256(source_bytes).hexdigest()
            records = _parse_jsonl_snapshot(source_bytes, wal_path)
            rows = [_raw_parquet_row(record) for record in records]
            target = self.parquet_root / uid / day / hour_text / "events.parquet"
            target.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = target.with_name(f".{target.name}.{uuid4().hex}.tmp")
            try:
                _write_raw_parquet(tmp_path, rows)
                if self.fsync:
                    with tmp_path.open("r+b") as stream:
                        os.fsync(stream.fileno())
                if hashlib.sha256(wal_path.read_bytes()).hexdigest() != source_sha256:
                    raise ConcurrentWalUpdateError(f"WAL changed during compaction: {wal_path}")
                os.replace(tmp_path, target)
            finally:
                if tmp_path.exists():
                    tmp_path.unlink()

            with self._connection:
                self._connection.execute(
                    """
                    INSERT INTO parquet_compactions(
                        wal_path, parquet_path, source_sha256,
                        source_size, row_count, compacted_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(wal_path) DO UPDATE SET
                        parquet_path = excluded.parquet_path,
                        source_sha256 = excluded.source_sha256,
                        source_size = excluded.source_size,
                        row_count = excluded.row_count,
                        compacted_at = excluded.compacted_at
                    """,
                    (
                        str(wal_path),
                        str(target),
                        source_sha256,
                        len(source_bytes),
                        len(rows),
                        _utc_now().isoformat(),
                    ),
                )

        return CompactionResult(
            wal_path=wal_path,
            parquet_path=target,
            row_count=len(rows),
            source_sha256=source_sha256,
            source_size=len(source_bytes),
        )

    def compact_all(self) -> tuple[CompactionResult, ...]:
        """Compact every hourly WAL currently present under raw_root."""

        results: list[CompactionResult] = []
        if self.raw_root.exists():
            for wal_path in sorted(self.raw_root.glob("*/*/*/events.jsonl")):
                relative = wal_path.relative_to(self.raw_root)
                instrument_uid, day, hour, _name = relative.parts
                results.append(self.compact_hour(instrument_uid, day, hour))
        self.compact_derived_all()
        return tuple(results)

    def compact_derived_all(self) -> tuple[Path, ...]:
        """Atomically export SQLite-derived records into hourly zstd Parquet."""

        rows = self._connection.execute(
            """
            SELECT dataset, event_id, recorded_at, record_json
            FROM derived_event_index
            WHERE record_json IS NOT NULL
            ORDER BY dataset, recorded_at, event_id
            """
        ).fetchall()
        groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        for row in rows:
            recorded_at = _parse_timestamp(row["recorded_at"], "recorded_at")
            key = (row["dataset"], recorded_at.date().isoformat(), recorded_at.strftime("%H"))
            groups.setdefault(key, []).append(
                {
                    "dataset": row["dataset"],
                    "event_id": row["event_id"],
                    "recorded_at": recorded_at.isoformat(),
                    "schema_version": str(
                        json.loads(row["record_json"]).get("schema_version", "1")
                    ),
                    "record_json": row["record_json"],
                }
            )
        pa = cast(Any, importlib.import_module("pyarrow"))
        pq = cast(Any, importlib.import_module("pyarrow.parquet"))
        outputs: list[Path] = []
        for (dataset, day, hour), records in groups.items():
            target = self.derived_parquet_root / dataset / f"date={day}" / f"hour={hour}.parquet"
            target.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = target.with_name(f".{target.name}.{uuid4().hex}.tmp")
            try:
                pq.write_table(pa.Table.from_pylist(records), tmp_path, compression="zstd")
                if self.fsync:
                    with tmp_path.open("r+b") as stream:
                        os.fsync(stream.fileno())
                os.replace(tmp_path, target)
            finally:
                if tmp_path.exists():
                    tmp_path.unlink()
            outputs.append(target)
        return tuple(outputs)

    def iter_raw_events(self, partition_date: date | str | None = None) -> Iterator[dict[str, Any]]:
        """Iterate raw WAL records without mutating or compacting them."""

        if not self.raw_root.exists():
            return
        pattern = "*/*/*/events.jsonl"
        if partition_date is not None:
            pattern = f"*/{_partition_date(partition_date)}/*/events.jsonl"
        for path in sorted(self.raw_root.glob(pattern)):
            for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                if not line.strip():
                    continue
                loaded = json.loads(line)
                if not isinstance(loaded, dict):
                    raise StorageError(f"Expected JSON object at {path}:{line_number}")
                yield loaded

    def iter_derived(
        self,
        dataset: str,
        partition_date: date | str | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Iterate derived records from authoritative immutable Parquet parts."""

        normalized_dataset = _dataset_name(dataset)
        # A buffered part contains many event ids, so read each physical file
        # once (the former per-event layout did not need DISTINCT).
        self.finalize_derived_writers()
        query = (
            "SELECT parquet_path, MIN(recorded_at) AS first_recorded_at "
            "FROM derived_event_index WHERE dataset = ?"
        )
        parameters: list[object] = [normalized_dataset]
        if partition_date is not None:
            query += " AND substr(recorded_at, 1, 10) = ?"
            parameters.append(_partition_date(partition_date))
        query += " GROUP BY parquet_path ORDER BY first_recorded_at, parquet_path"
        pq = cast(Any, importlib.import_module("pyarrow.parquet"))
        for row in self._connection.execute(query, parameters):
            path = Path(row["parquet_path"])
            if not path.is_file():
                continue
            for stored in pq.read_table(path).to_pylist():
                loaded = json.loads(stored["record_json"])
                if isinstance(loaded, dict):
                    yield loaded

    def daily_control_metrics(self, partition_date: date | str) -> dict[str, Any]:
        """Return session/subscription/gap counts used by daily reporting."""

        day = _partition_date(partition_date)
        sessions = self._connection.execute(
            "SELECT COUNT(*) AS count FROM stream_sessions WHERE substr(started_at, 1, 10) = ?",
            (day,),
        ).fetchone()["count"]
        subscriptions = self._connection.execute(
            """
            SELECT COUNT(*) AS count FROM subscription_events
            WHERE substr(event_time, 1, 10) = ?
            """,
            (day,),
        ).fetchone()["count"]
        gap_row = self._connection.execute(
            """
            SELECT COUNT(*) AS count, COALESCE(SUM(duration_seconds), 0) AS duration
            FROM data_gaps WHERE substr(started_at, 1, 10) = ?
            """,
            (day,),
        ).fetchone()
        return {
            "stream_sessions": int(sessions),
            "reconnects": max(int(sessions) - 1, 0),
            "subscription_events": int(subscriptions),
            "data_gaps": int(gap_row["count"]),
            "gap_duration_seconds": float(gap_row["duration"]),
        }

    def refresh_duckdb_catalog(
        self,
        catalog_path: Path | str | None = None,
    ) -> DuckDBCatalogResult:
        """Create persistent DuckDB views over raw and derived parquet files."""

        self.compact_derived_all()
        duckdb = cast(Any, importlib.import_module("duckdb"))
        target = Path(catalog_path) if catalog_path is not None else self.root / "research.duckdb"
        target.parent.mkdir(parents=True, exist_ok=True)
        connection = duckdb.connect(str(target))
        views = ("raw_events", *DERIVED_DATASETS)
        try:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS research_catalog(
                    dataset VARCHAR PRIMARY KEY,
                    dataset_kind VARCHAR NOT NULL,
                    path_glob VARCHAR NOT NULL,
                    file_count BIGINT NOT NULL,
                    refreshed_at TIMESTAMP NOT NULL
                )
                """
            )
            raw_files = sorted(self.parquet_root.glob("*/*/*/events.parquet"))
            _create_duckdb_view(connection, "raw_events", raw_files, raw=True)
            self._upsert_catalog_row(
                connection,
                dataset="raw_events",
                kind="raw",
                path_glob=str(self.parquet_root / "*" / "*" / "*" / "events.parquet"),
                file_count=len(raw_files),
            )
            for dataset in DERIVED_DATASETS:
                files = sorted(
                    (self.derived_parquet_root / dataset).glob("date=*/hour=*/*.parquet")
                )
                _create_duckdb_view(connection, dataset, files, raw=False)
                self._upsert_catalog_row(
                    connection,
                    dataset=dataset,
                    kind="derived",
                    path_glob=str(
                        self.derived_parquet_root / dataset / "date=*" / "hour=*" / "*.parquet"
                    ),
                    file_count=len(files),
                )
            connection.commit()
        finally:
            connection.close()
        return DuckDBCatalogResult(catalog_path=target, views=views)

    def _wal_path(self, instrument_uid: str, timestamp: datetime) -> Path:
        return (
            self.raw_root
            / instrument_uid
            / timestamp.date().isoformat()
            / timestamp.strftime("%H")
            / "events.jsonl"
        )

    def _initialize_schema(self) -> None:
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS raw_event_index(
                event_id TEXT PRIMARY KEY,
                instrument_uid TEXT NOT NULL,
                event_type TEXT NOT NULL,
                exchange_timestamp TEXT NOT NULL,
                receive_timestamp TEXT NOT NULL,
                wal_path TEXT NOT NULL,
                byte_offset INTEGER NOT NULL,
                byte_length INTEGER NOT NULL,
                stored_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_raw_receive_timestamp
                ON raw_event_index(receive_timestamp);

            CREATE TABLE IF NOT EXISTS service_state(
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS stream_sessions(
                session_id TEXT PRIMARY KEY,
                stream_id TEXT,
                source TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                status TEXT NOT NULL,
                error TEXT,
                metadata_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS subscription_events(
                event_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                subscription_id TEXT NOT NULL,
                instrument_uid TEXT NOT NULL,
                event_type TEXT NOT NULL,
                action TEXT NOT NULL,
                event_time TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_subscription_session
                ON subscription_events(session_id, event_time);

            CREATE TABLE IF NOT EXISTS data_gaps(
                gap_id TEXT PRIMARY KEY,
                session_id TEXT,
                instrument_uid TEXT NOT NULL,
                event_type TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                duration_seconds REAL,
                recoverable INTEGER NOT NULL,
                reason TEXT,
                metadata_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS derived_event_index(
                dataset TEXT NOT NULL,
                event_id TEXT NOT NULL,
                recorded_at TEXT NOT NULL,
                parquet_path TEXT NOT NULL,
                stored_at TEXT NOT NULL,
                record_json TEXT,
                PRIMARY KEY(dataset, event_id)
            );

            CREATE TABLE IF NOT EXISTS parquet_compactions(
                wal_path TEXT PRIMARY KEY,
                parquet_path TEXT NOT NULL,
                source_sha256 TEXT NOT NULL,
                source_size INTEGER NOT NULL,
                row_count INTEGER NOT NULL,
                compacted_at TEXT NOT NULL
            );
            """
        )

    def _migrate_derived_record_json(self) -> None:
        """Backfill records created by the earlier per-record Parquet layout."""

        columns = {
            row["name"]
            for row in self._connection.execute("PRAGMA table_info(derived_event_index)")
        }
        if "record_json" not in columns:
            self._connection.execute("ALTER TABLE derived_event_index ADD COLUMN record_json TEXT")
        pending = self._connection.execute(
            """
            SELECT dataset, event_id, parquet_path FROM derived_event_index
            WHERE record_json IS NULL
            """
        ).fetchall()
        if not pending:
            return
        pq = cast(Any, importlib.import_module("pyarrow.parquet"))
        with self._connection:
            for row in pending:
                path = Path(row["parquet_path"])
                if not path.is_file():
                    continue
                table_rows = pq.read_table(path).to_pylist()
                if not table_rows:
                    continue
                self._connection.execute(
                    """
                    UPDATE derived_event_index SET record_json = ?
                    WHERE dataset = ? AND event_id = ?
                    """,
                    (table_rows[0].get("record_json"), row["dataset"], row["event_id"]),
                )

    def _write_derived_part(
        self,
        path: Path,
        *,
        dataset: str,
        event_id: str,
        recorded_at: datetime,
        record: dict[str, Any],
    ) -> None:
        pa = cast(Any, importlib.import_module("pyarrow"))
        pq = cast(Any, importlib.import_module("pyarrow.parquet"))
        table = pa.Table.from_pylist(
            [
                {
                    "dataset": dataset,
                    "event_id": event_id,
                    "recorded_at": recorded_at.isoformat(),
                    "schema_version": str(record.get("schema_version", "1")),
                    "record_json": _json_text(record),
                }
            ]
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        pq.write_table(table, temporary, compression="zstd")
        if self.fsync:
            with temporary.open("r+b") as stream:
                os.fsync(stream.fileno())
        os.replace(temporary, path)

    @staticmethod
    def _upsert_catalog_row(
        connection: Any,
        *,
        dataset: str,
        kind: str,
        path_glob: str,
        file_count: int,
    ) -> None:
        connection.execute(
            """
            INSERT INTO research_catalog
                (dataset, dataset_kind, path_glob, file_count, refreshed_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(dataset) DO UPDATE SET
                dataset_kind = excluded.dataset_kind,
                path_glob = excluded.path_glob,
                file_count = excluded.file_count,
                refreshed_at = excluded.refreshed_at
            """,
            [dataset, kind, path_glob, file_count, _utc_now()],
        )


def _normalize_raw_event(event: JsonMapping) -> dict[str, Any]:
    missing = sorted(RAW_REQUIRED_FIELDS.difference(event))
    if missing:
        raise InvalidRecordError(f"Raw event is missing required fields: {', '.join(missing)}")
    payload = event["payload"]
    if not isinstance(payload, Mapping):
        raise InvalidRecordError("payload must be a mapping.")
    normalized = cast(dict[str, Any], _json_compatible(dict(event)))
    normalized["event_id"] = _required_text(str(event["event_id"]), "event_id")
    normalized["instrument_uid"] = _partition_value(str(event["instrument_uid"]), "instrument_uid")
    normalized["event_type"] = _required_text(str(event["event_type"]), "event_type")
    normalized["exchange_timestamp"] = _as_utc_timestamp(
        event["exchange_timestamp"], "exchange_timestamp"
    )
    normalized["receive_timestamp"] = _as_utc_timestamp(
        event["receive_timestamp"], "receive_timestamp"
    )
    normalized["schema_version"] = _required_text(str(event["schema_version"]), "schema_version")
    normalized["payload"] = cast(dict[str, Any], _json_compatible(dict(payload)))
    return normalized


def _raw_parquet_row(record: Mapping[str, Any]) -> dict[str, Any]:
    payload = record.get("payload")
    return {
        "event_id": str(record.get("event_id", "")),
        "instrument_uid": str(record.get("instrument_uid", "")),
        "event_type": str(record.get("event_type", "")),
        "exchange_timestamp": _nullable_text(record.get("exchange_timestamp")),
        "receive_timestamp": _nullable_text(record.get("receive_timestamp")),
        "monotonic_receive_time": _nullable_text(record.get("monotonic_receive_time")),
        "latency_ms": _optional_float(record.get("latency_ms")),
        "sequence": _optional_int(record.get("sequence")),
        "stream_id": _nullable_text(record.get("stream_id")),
        "subscription_id": _nullable_text(record.get("subscription_id")),
        "source": _nullable_text(record.get("source")),
        "is_consistent": _optional_bool(record.get("is_consistent")),
        "connection_state": _nullable_text(record.get("connection_state")),
        "schema_version": str(record.get("schema_version", "")),
        "payload_json": _json_text(payload if isinstance(payload, Mapping) else {}),
        "event_json": _json_text(record),
    }


def _write_raw_parquet(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    pa = cast(Any, importlib.import_module("pyarrow"))
    pq = cast(Any, importlib.import_module("pyarrow.parquet"))
    schema = pa.schema(
        [
            ("event_id", pa.string()),
            ("instrument_uid", pa.string()),
            ("event_type", pa.string()),
            ("exchange_timestamp", pa.string()),
            ("receive_timestamp", pa.string()),
            ("monotonic_receive_time", pa.string()),
            ("latency_ms", pa.float64()),
            ("sequence", pa.int64()),
            ("stream_id", pa.string()),
            ("subscription_id", pa.string()),
            ("source", pa.string()),
            ("is_consistent", pa.bool_()),
            ("connection_state", pa.string()),
            ("schema_version", pa.string()),
            ("payload_json", pa.string()),
            ("event_json", pa.string()),
        ]
    )
    table = pa.Table.from_pylist(list(rows), schema=schema)
    pq.write_table(table, path, compression="zstd", use_dictionary=True)


def _create_duckdb_view(
    connection: Any,
    view_name: str,
    files: Sequence[Path],
    *,
    raw: bool,
) -> None:
    if files:
        file_list = ", ".join(f"'{_sql_text(path.resolve().as_posix())}'" for path in files)
        connection.execute(
            f"CREATE OR REPLACE VIEW {view_name} AS "
            f"SELECT * FROM read_parquet([{file_list}], union_by_name=true, filename=true)"
        )
        return
    if raw:
        connection.execute(
            f"""
            CREATE OR REPLACE VIEW {view_name} AS SELECT
                CAST(NULL AS VARCHAR) AS event_id,
                CAST(NULL AS VARCHAR) AS instrument_uid,
                CAST(NULL AS VARCHAR) AS event_type,
                CAST(NULL AS VARCHAR) AS exchange_timestamp,
                CAST(NULL AS VARCHAR) AS receive_timestamp,
                CAST(NULL AS DOUBLE) AS latency_ms,
                CAST(NULL AS BOOLEAN) AS is_consistent,
                CAST(NULL AS VARCHAR) AS payload_json,
                CAST(NULL AS VARCHAR) AS event_json
            WHERE FALSE
            """
        )
    else:
        connection.execute(
            f"""
            CREATE OR REPLACE VIEW {view_name} AS SELECT
                CAST(NULL AS VARCHAR) AS dataset,
                CAST(NULL AS VARCHAR) AS event_id,
                CAST(NULL AS VARCHAR) AS recorded_at,
                CAST(NULL AS VARCHAR) AS schema_version,
                CAST(NULL AS VARCHAR) AS record_json
            WHERE FALSE
            """
        )


def _derived_event_id(dataset: str, record: JsonMapping) -> str:
    for key in ("event_id", "prediction_id", "trade_id", "id"):
        value = record.get(key)
        if value is not None and str(value).strip():
            return _required_text(str(value), key)
    if dataset == "experiment_registry" and record.get("experiment_id"):
        return _content_id(
            dataset,
            {
                "experiment_id": record.get("experiment_id"),
                "configuration_hash": record.get("configuration_hash"),
            },
        )
    if dataset == "model_registry" and record.get("model_id"):
        return _content_id(
            dataset,
            {
                "model_id": record.get("model_id"),
                "model_version": record.get("model_version"),
            },
        )
    return _content_id(dataset, record)


def _content_id(namespace: str, payload: Any) -> str:
    digest = hashlib.sha256()
    digest.update(namespace.encode("utf-8"))
    digest.update(b"\0")
    digest.update(_json_bytes(payload))
    return digest.hexdigest()


def _parse_jsonl_snapshot(source: bytes, path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(source.splitlines(), 1):
        if not line.strip():
            continue
        try:
            loaded = json.loads(line)
        except json.JSONDecodeError as exc:
            raise StorageError(f"Invalid JSON at {path}:{line_number}") from exc
        if not isinstance(loaded, dict):
            raise StorageError(f"Expected JSON object at {path}:{line_number}")
        records.append(loaded)
    return records


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        _json_compatible(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _json_text(value: Any) -> str:
    return _json_bytes(value).decode("utf-8")


def _json_compatible(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_compatible(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_compatible(item) for item in value]
    if isinstance(value, datetime):
        return _as_utc(value).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _dataset_name(value: str) -> str:
    normalized = _required_text(value, "dataset")
    if normalized not in DERIVED_DATASETS:
        allowed = ", ".join(DERIVED_DATASETS)
        raise InvalidRecordError(f"Unsupported derived dataset {normalized!r}; expected: {allowed}")
    return normalized


def _partition_value(value: str, field_name: str) -> str:
    normalized = _required_text(value, field_name)
    invalid = {"/", "\\", ":", "*", "?", '"', "<", ">", "|", "."}
    if normalized in {".", ".."} or any(char in normalized for char in invalid):
        raise InvalidRecordError(f"{field_name} contains unsafe path characters.")
    return normalized


def _partition_date(value: date | str) -> str:
    try:
        parsed = value if isinstance(value, date) else date.fromisoformat(value)
    except ValueError as exc:
        raise InvalidRecordError("partition_date must be ISO YYYY-MM-DD.") from exc
    return parsed.isoformat()


def _partition_hour(value: int | str) -> str:
    try:
        hour = int(value)
    except (TypeError, ValueError) as exc:
        raise InvalidRecordError("hour must be an integer in 0..23.") from exc
    if not 0 <= hour <= 23:
        raise InvalidRecordError("hour must be an integer in 0..23.")
    return f"{hour:02d}"


def _required_text(value: str, field_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise InvalidRecordError(f"{field_name} must not be empty.")
    return normalized


def _optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _nullable_text(value: Any) -> str | None:
    return None if value is None else str(value)


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
    return bool(value)


def _parse_timestamp(value: Any, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return _as_utc(value)
    if isinstance(value, str):
        normalized = value.strip().replace("Z", "+00:00")
        try:
            return _as_utc(datetime.fromisoformat(normalized))
        except ValueError as exc:
            raise InvalidRecordError(f"{field_name} must be an ISO timestamp.") from exc
    raise InvalidRecordError(f"{field_name} must be a datetime or ISO timestamp.")


def _as_utc_timestamp(value: Any, field_name: str) -> str:
    return _parse_timestamp(value, field_name).isoformat()


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _sql_text(value: str) -> str:
    return value.replace("'", "''")


__all__ = [
    "ConcurrentWalUpdateError",
    "CompactionResult",
    "DERIVED_DATASETS",
    "DerivedAppendResult",
    "DuckDBCatalogResult",
    "InvalidRecordError",
    "RAW_REQUIRED_FIELDS",
    "RawAppendResult",
    "ResearchStorage",
    "StorageError",
]
