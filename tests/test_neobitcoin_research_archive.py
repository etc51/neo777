from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from neo_trader.neobitcoin_research.archive import (
    ArchiveValidationError,
    create_verified_archive,
    verify_archive,
)


def _parquet(root: Path, *, values: list[str] | None = None) -> Path:
    path = root / "compacted" / "orderbook" / "instrument=neo" / "date=2026-07-11"
    path.mkdir(parents=True, exist_ok=True)
    target = path / "part-00000.parquet"
    rows = values or ["alpha", "beta"]
    table = pa.table(
        {
            "event_id": pa.array(range(len(rows)), type=pa.int64()),
            "timestamp": pa.array(
                [datetime(2026, 7, 11, 12, index, tzinfo=UTC) for index in range(len(rows))]
            ),
            "value": rows,
        }
    )
    pq.write_table(table, target, compression="zstd")
    return target


def test_archive_waits_for_minimum_without_force(tmp_path: Path) -> None:
    source = _parquet(tmp_path)

    result = create_verified_archive(tmp_path, minimum_bytes=source.stat().st_size + 1)

    assert not result.created
    assert result.waiting_bytes == source.stat().st_size
    assert list((tmp_path / "archives").iterdir()) == []


def test_force_archive_is_verified_and_keeps_sources(tmp_path: Path) -> None:
    source = _parquet(tmp_path)

    result = create_verified_archive(
        tmp_path,
        force=True,
        schema_version="raw-v2",
        code_commit="abc123",
        config_hash="config123",
    )

    assert result.created
    assert source.exists()
    assert result.archive_path is not None and result.archive_path.exists()
    assert result.manifest_json_path is not None
    manifest = json.loads(result.manifest_json_path.read_text(encoding="utf-8"))
    assert manifest["archive_sha256"] == result.archive_sha256
    assert manifest["validation"] == "passed"
    assert manifest["rows_by_dataset"] == {"orderbook": 2}
    assert manifest["files"][0]["instrument"] == "neo"
    assert manifest["files"][0]["row_count"] == 2
    verify_archive(result.archive_path, result.manifest_json_path)

    repeated = create_verified_archive(tmp_path, force=True)
    assert not repeated.created
    assert repeated.waiting_bytes == 0


def test_secret_in_parquet_quarantines_instead_of_publishing(tmp_path: Path) -> None:
    token = "t." + "sensitive-value-123456789"
    _parquet(tmp_path, values=[token])
    token_path = tmp_path / "desktop-token.txt"
    token_path.write_text(token, encoding="utf-8")

    with pytest.raises(ArchiveValidationError, match="quarantined"):
        create_verified_archive(tmp_path, force=True, token_files=[token_path])

    assert list((tmp_path / "archives").iterdir()) == []
    reports = list((tmp_path / "quarantine").glob("*.failure.json"))
    assert len(reports) == 1
    failure = json.loads(reports[0].read_text(encoding="utf-8"))
    assert failure["error_type"] == "ArchiveValidationError"
    assert token not in reports[0].read_text(encoding="utf-8")


def test_verify_rejects_modified_archive(tmp_path: Path) -> None:
    _parquet(tmp_path)
    result = create_verified_archive(tmp_path, force=True)
    assert result.archive_path is not None
    assert result.manifest_json_path is not None
    with result.archive_path.open("ab") as stream:
        stream.write(b"corruption")

    with pytest.raises(ArchiveValidationError, match="archive SHA-256 mismatch"):
        verify_archive(result.archive_path, result.manifest_json_path)
