from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from neo_trader.neobitcoin_research.buffered_parquet import (
    BufferedParquetWriter,
    ParquetBatchPolicy,
    recover_or_quarantine_inprogress,
)


SCHEMA = pa.schema([("event_id", pa.string()), ("recorded_at", pa.string()), ("value", pa.int64())])


def test_one_thousand_rows_publish_one_final_part_after_batches(tmp_path: Path) -> None:
    target = tmp_path / "feature_snapshots" / "date=2026-07-11" / "hour=12" / "part-00000.parquet"
    writer = BufferedParquetWriter(
        target,
        SCHEMA,
        policy=ParquetBatchPolicy(max_rows=100, max_buffer_bytes=1_000_000, max_interval_seconds=60),
    )
    for index in range(1_000):
        writer.append({"event_id": str(index), "recorded_at": "2026-07-11T12:00:00+00:00", "value": index})

    assert list(target.parent.glob("*.parquet")) == []
    assert writer.finalize() == target
    assert list(target.parent.glob("*.parquet")) == [target]
    assert not writer.inprogress_path.exists()
    assert pq.read_table(target).num_rows == 1_000


def test_time_limit_flushes_private_file_then_atomic_finalize(tmp_path: Path) -> None:
    target = tmp_path / "dataset" / "part-00000.parquet"
    writer = BufferedParquetWriter(
        target,
        SCHEMA,
        policy=ParquetBatchPolicy(max_rows=100, max_buffer_bytes=1_000_000, max_interval_seconds=1),
    )
    writer.append({"event_id": "a", "recorded_at": "2026-07-11T12:00:00+00:00", "value": 1})
    assert writer.flush_if_due(now=writer._last_flush_at + 2)
    assert writer.inprogress_path.exists()
    assert not target.exists()
    writer.finalize()
    assert target.exists()
    assert pq.read_table(target).to_pylist()[0]["event_id"] == "a"


def test_corrupt_inprogress_is_quarantined_never_deleted(tmp_path: Path) -> None:
    source = tmp_path / "part-00000.parquet.inprogress"
    source.write_bytes(b"not parquet")
    result = recover_or_quarantine_inprogress(source, quarantine_root=tmp_path / "quarantine")
    assert result == tmp_path / "quarantine" / source.name
    assert result.read_bytes() == b"not parquet"
