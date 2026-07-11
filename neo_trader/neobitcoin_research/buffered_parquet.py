"""Bounded, crash-safe writer for one final Parquet part per partition.

The writer deliberately keeps the active file private (``.inprogress``).
Only a closed file with its footer fsynced is published, by atomic rename, as
``part-00000.parquet``.  Callers create one writer per dataset/instrument/hour
and rotate it at the partition boundary; batches are written to the active
writer by row count, estimated bytes, or elapsed time.
"""

from __future__ import annotations

import os
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ParquetBatchPolicy:
    """Bound memory and write cadence without creating public micro-files."""

    max_rows: int = 20_000
    max_buffer_bytes: int = 16 * 1024 * 1024
    max_interval_seconds: float = 30.0
    row_group_size: int = 10_000


class BufferedParquetWriter:
    """Buffer rows and publish exactly one atomically-written Parquet part.

    ``schema`` must be stable for the whole dataset.  This is intentional:
    ParquetWriter cannot evolve a schema after it has been opened.  The
    production storage adapter therefore converts records to typed, stable
    rows before calling :meth:`append`.
    """

    def __init__(
        self,
        final_path: Path | str,
        schema: Any,
        *,
        policy: ParquetBatchPolicy | None = None,
        fsync: bool = True,
    ) -> None:
        selected_policy = policy or ParquetBatchPolicy()
        if (
            selected_policy.max_rows <= 0
            or selected_policy.max_buffer_bytes <= 0
            or selected_policy.max_interval_seconds <= 0
        ):
            raise ValueError("Parquet batch limits must be positive.")
        self.final_path = Path(final_path)
        self.inprogress_path = self.final_path.with_suffix(self.final_path.suffix + ".inprogress")
        self.schema = schema
        self.policy = selected_policy
        self.fsync = fsync
        self._rows: list[dict[str, Any]] = []
        self._buffer_bytes = 0
        self._opened_at = time.monotonic()
        self._last_flush_at = self._opened_at
        self._stream: Any | None = None
        self._writer: Any | None = None
        self.rows_written = 0

    @property
    def pending_rows(self) -> int:
        return len(self._rows)

    def append(self, row: Mapping[str, Any]) -> bool:
        """Buffer a row and flush it if one of the batch limits is reached.

        Returns whether this call flushed an internal batch.  A flush writes
        only to the private ``.inprogress`` file, never a new public part.
        """

        normalized = {str(key): value for key, value in row.items()}
        self._rows.append(normalized)
        self._buffer_bytes += _estimate_row_size(normalized)
        return self.flush_if_due()

    def flush_if_due(self, *, now: float | None = None) -> bool:
        """Flush a full or aged batch to the active private Parquet writer."""

        if not self._rows:
            return False
        current = time.monotonic() if now is None else now
        if (
            len(self._rows) < self.policy.max_rows
            and self._buffer_bytes < self.policy.max_buffer_bytes
            and current - self._last_flush_at < self.policy.max_interval_seconds
        ):
            return False
        self.flush()
        return True

    def flush(self) -> None:
        """Write the buffered batch, retaining one private file per partition."""

        if not self._rows:
            return
        pa = _pyarrow()
        if self._writer is None:
            self.final_path.parent.mkdir(parents=True, exist_ok=True)
            self._stream = self.inprogress_path.open("xb")
            self._writer = _parquet().ParquetWriter(
                self._stream,
                self.schema,
                compression="zstd",
                use_dictionary=True,
                write_statistics=True,
            )
        table = pa.Table.from_pylist(self._rows, schema=self.schema)
        self._writer.write_table(table, row_group_size=self.policy.row_group_size)
        self.rows_written += len(self._rows)
        self._rows.clear()
        self._buffer_bytes = 0
        self._last_flush_at = time.monotonic()
        self._stream.flush()
        if self.fsync:
            os.fsync(self._stream.fileno())

    def finalize(self) -> Path | None:
        """Close, fsync, and atomically publish this partition's sole part."""

        self.flush()
        if self._writer is None:
            return None
        try:
            self._writer.close()  # writes the Parquet footer
            self._stream.flush()
            if self.fsync:
                os.fsync(self._stream.fileno())
        finally:
            self._stream.close()
            self._writer = None
            self._stream = None
        os.replace(self.inprogress_path, self.final_path)
        _fsync_directory(self.final_path.parent)
        return self.final_path

    def close(self) -> Path | None:
        return self.finalize()


def recover_or_quarantine_inprogress(
    inprogress_path: Path | str, *, quarantine_root: Path | str
) -> Path | None:
    """Publish a readable complete temporary file or preserve it for triage.

    An interrupted ParquetWriter generally lacks a footer and cannot be made
    valid by appending.  It is therefore moved to quarantine rather than being
    silently deleted.  A completed-but-not-renamed file is published safely.
    """

    source = Path(inprogress_path)
    if not source.exists():
        return None
    final = source.with_suffix("")
    try:
        # Keep the native file handle scoped: Windows otherwise can retain a
        # failed ParquetFile open long enough to make the quarantine rename
        # fail with WinError 32.
        with source.open("rb") as stream:
            _parquet().read_metadata(stream)
    except Exception:
        quarantine = Path(quarantine_root)
        quarantine.mkdir(parents=True, exist_ok=True)
        target = quarantine / source.name
        os.replace(source, target)
        _fsync_directory(quarantine)
        return target
    os.replace(source, final)
    _fsync_directory(final.parent)
    return final


def _estimate_row_size(row: Mapping[str, Any]) -> int:
    # Conservative bounded-memory estimate; exact serialized size is not
    # needed because max_rows and max_interval are independent hard limits.
    return sum(len(str(key)) + len(str(value)) for key, value in row.items()) + 32


def _pyarrow() -> Any:
    import pyarrow

    return pyarrow


def _parquet() -> Any:
    import pyarrow.parquet

    return pyarrow.parquet


def _fsync_directory(path: Path) -> None:
    # Windows cannot fsync a directory handle.  os.replace is atomic there;
    # POSIX gets the stronger durability guarantee when supported.
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


__all__ = ["BufferedParquetWriter", "ParquetBatchPolicy", "recover_or_quarantine_inprogress"]
