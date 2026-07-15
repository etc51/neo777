from __future__ import annotations

import hashlib
import json
import shutil
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pyarrow.parquet as pq  # type: ignore[import-untyped]
import pytest

from neobitcoin_paper.archive import (
    REQUIRED_DOCUMENTS,
    ArchiveBuildRequest,
    ArchiveError,
    ArchiveValidationResult,
    ArchiveValidator,
    BuiltArchive,
    DailyArchiveBuilder,
    _extract_tar_zst,
)
from neobitcoin_paper.datasets import DATASET_SCHEMAS, REQUIRED_DATASETS, DatasetStore

SESSION_DATE = date(2026, 7, 15)


def _request(root: Path, *, config: dict[str, object] | None = None) -> ArchiveBuildRequest:
    return ArchiveBuildRequest(
        data_root=root,
        session_date=SESSION_DATE,
        start_utc=datetime(2026, 7, 15, 4, 0, tzinfo=UTC),
        end_utc=datetime(2026, 7, 15, 21, 0, tzinfo=UTC),
        strategy_registry=(),
        session_calendar={
            "timezone": "Europe/Moscow",
            "session_date": SESSION_DATE.isoformat(),
            "source": "TEST",
        },
        config_snapshot=config or {"PAPER_ONLY": True, "test_run": True},
        code_version={"commit": "test-archive", "dirty": False},
        instrument_snapshot={
            "name": "Neo Bitcoin",
            "ticker": "BTCUSDperpA",
            "uid": "4effa274-4e8f-422c-93ff-04aa34fe8e39",
        },
        test_archive=True,
    )


def _zero_event_archive(root: Path) -> BuiltArchive:
    datasets = DatasetStore(root, SESSION_DATE, durable_writes=False)
    return DailyArchiveBuilder().build(_request(root), dataset_store=datasets)


def test_zero_event_test_archive_has_exact_typed_bundle_and_hashes(
    tmp_path: Path,
) -> None:
    root = tmp_path / "paper"
    built = _zero_event_archive(root)

    assert built.archive_id.startswith("TEST-")
    assert "_TEST.tar.zst" in built.archive_path.name
    assert built.validation.passed
    assert built.validation.pyarrow_verified
    assert built.validation.duckdb_verified
    assert built.validation.zstd_verified
    assert built.sha256 == hashlib.sha256(built.archive_path.read_bytes()).hexdigest()
    assert built.sha256_path.read_text(encoding="ascii") == (
        f"{built.sha256}  {built.archive_path.name}\n"
    )
    assert not tuple(root.rglob("*.inprogress"))

    extracted = tmp_path / "extracted"
    extracted.mkdir()
    _extract_tar_zst(built.archive_path, extracted)
    expected_files = set(REQUIRED_DOCUMENTS) | {
        f"data/{name}" for name in REQUIRED_DATASETS
    }
    actual_files = {
        path.relative_to(extracted).as_posix()
        for path in extracted.rglob("*")
        if path.is_file()
    }
    assert actual_files == expected_files
    assert len(tuple((extracted / "data").glob("*.parquet"))) == 20
    assert not tuple(extracted.rglob("*.inprogress"))

    manifest = json.loads((extracted / "MANIFEST.json").read_text(encoding="utf-8"))
    assert manifest["archive_type"] == "TEST"
    assert manifest["paper_only"] is True
    assert manifest["datasets"] == [f"data/{name}" for name in REQUIRED_DATASETS]
    assert manifest["row_counts"] == {name: 0 for name in REQUIRED_DATASETS}

    connection = duckdb.connect(":memory:")
    try:
        for name in REQUIRED_DATASETS:
            path = extracted / "data" / name
            table = pq.read_table(path)
            assert table.num_rows == 0
            assert table.schema.equals(DATASET_SCHEMAS[name], check_metadata=True)
            result = connection.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(path)]
            ).fetchone()
            assert result is not None and int(result[0]) == 0
    finally:
        connection.close()

    sum_lines = (extracted / "SHA256SUMS").read_text(encoding="ascii").splitlines()
    declared: dict[str, str] = {}
    for line in sum_lines:
        digest, relative = line.split("  ", 1)
        declared[relative] = digest
        assert digest == hashlib.sha256((extracted / relative).read_bytes()).hexdigest()
    assert set(declared) == expected_files - {"SHA256SUMS"}
    assert ArchiveValidator().validate_archive(
        built.archive_path, expected_sha256=built.sha256
    ).passed

    reused = DailyArchiveBuilder().build(_request(root))
    assert reused.archive_path == built.archive_path
    assert reused.sha256 == built.sha256


def test_archive_validator_rejects_payload_and_checksum_tampering(tmp_path: Path) -> None:
    built = _zero_event_archive(tmp_path / "paper")
    tampered = tmp_path / "tampered.tar.zst"
    shutil.copy2(built.archive_path, tampered)
    payload = bytearray(tampered.read_bytes())
    payload[len(payload) // 2] ^= 0x01
    tampered.write_bytes(payload)

    result = ArchiveValidator().validate_archive(tampered, expected_sha256=built.sha256)
    assert not result.passed
    assert "archive SHA-256 mismatch" in result.errors

    extracted = tmp_path / "bundle"
    extracted.mkdir()
    _extract_tar_zst(built.archive_path, extracted)
    with (extracted / "README.md").open("a", encoding="utf-8") as handle:
        handle.write("tampered\n")
    bundle_result = ArchiveValidator().validate_bundle(extracted, verify_sums=True)
    assert not bundle_result.passed
    assert "file hash mismatch README.md" in bundle_result.errors


class _PostCompressionTamperValidator(ArchiveValidator):
    def validate_archive(
        self, archive: Path, *, expected_sha256: str | None = None
    ) -> ArchiveValidationResult:
        payload = bytearray(archive.read_bytes())
        payload[len(payload) // 2] ^= 0x01
        archive.write_bytes(payload)
        return super().validate_archive(archive, expected_sha256=expected_sha256)


def test_post_compression_failure_moves_artifacts_to_quarantine(tmp_path: Path) -> None:
    root = tmp_path / "paper"
    datasets = DatasetStore(root, SESSION_DATE, durable_writes=False)
    builder = DailyArchiveBuilder(validator=_PostCompressionTamperValidator())

    with pytest.raises(ArchiveError, match="compressed archive validation failed"):
        builder.build(_request(root), dataset_store=datasets)

    assert not tuple((root / "daily_archives").glob("*.tar.zst"))
    assert not tuple(root.rglob("*.inprogress"))
    quarantined_archives = tuple((root / "quarantine").glob("*.tar.zst"))
    assert len(quarantined_archives) == 1
    assert tuple((root / "quarantine").glob("*.tar.zst.sha256"))
    failure_markers = tuple((root / "quarantine").glob("*.failure.json"))
    assert len(failure_markers) == 1
    failure = json.loads(failure_markers[0].read_text(encoding="utf-8"))
    assert failure["status"] == "QUARANTINED"
    assert failure["error_type"] == "ArchiveError"
    assert tuple((root / "reports").glob("archive_failure_*.json"))
