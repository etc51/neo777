from __future__ import annotations

import hashlib
import json
import shutil
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pyarrow as pa  # type: ignore[import-untyped]
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
    _contains_secret,
    _extract_tar_zst,
    _normalize_daily_carryovers,
    _validate_carryover_references,
    _validate_foreign_key_paths,
    _validate_temporal_and_position_reconciliation,
)
from neobitcoin_paper.datasets import DATASET_SCHEMAS, REQUIRED_DATASETS, DatasetStore

SESSION_DATE = date(2026, 7, 15)


def test_secret_scan_reads_logical_parquet_strings(tmp_path: Path) -> None:
    clean = tmp_path / "clean"
    clean.mkdir()
    pq.write_table(pa.table({"payload": ["ordinary market data"]}), clean / "data.parquet")
    assert not _contains_secret(clean)

    unsafe = tmp_path / "unsafe"
    unsafe.mkdir()
    pq.write_table(
        pa.table({"payload": ["token=1234567890abcdef"]}),
        unsafe / "data.parquet",
    )
    assert _contains_secret(unsafe)


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
    assert "_schema-v2_TEST.tar.zst" in built.archive_path.name
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
    expected_files.add("SESSION_COVERAGE.json")
    expected_files.add("CARRYOVER_REFERENCES.json")
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
    assert manifest["schema_version"] == "neobitcoin-paper-schema-v2"
    assert manifest["paper_only"] is True
    assert manifest["datasets"] == [f"data/{name}" for name in REQUIRED_DATASETS]
    assert manifest["row_counts"] == {name: 0 for name in REQUIRED_DATASETS}
    assert set(manifest["dataset_schema_sha256"]) == set(REQUIRED_DATASETS)
    for name in REQUIRED_DATASETS:
        expected_schema_sha = hashlib.sha256(
            DATASET_SCHEMAS[name].serialize().to_pybytes()
        ).hexdigest()
        assert manifest["dataset_schema_sha256"][name] == expected_schema_sha

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


class _SourceMutationValidator(ArchiveValidator):
    def __init__(self, source: Path) -> None:
        self._source = source

    def validate_archive(
        self, archive: Path, *, expected_sha256: str | None = None
    ) -> ArchiveValidationResult:
        result = super().validate_archive(archive, expected_sha256=expected_sha256)
        with self._source.open("ab") as stream:
            stream.write(b"changed-after-snapshot")
        return result


def test_post_compression_failure_leaves_no_archive_artifacts(tmp_path: Path) -> None:
    root = tmp_path / "paper"
    datasets = DatasetStore(root, SESSION_DATE, durable_writes=False)
    builder = DailyArchiveBuilder(validator=_PostCompressionTamperValidator())

    with pytest.raises(ArchiveError, match="compressed archive validation failed"):
        builder.build(_request(root), dataset_store=datasets)

    assert not tuple((root / "daily_archives").glob("*.tar.zst"))
    assert not tuple(root.rglob("*.inprogress"))
    assert not tuple((root / "quarantine").glob("*.tar.zst*"))
    assert not tuple(root.rglob("*.tar.zst"))
    failure_markers = tuple((root / "reports").glob("archive_failure_*.json"))
    assert len(failure_markers) == 1
    failure = json.loads(failure_markers[0].read_text(encoding="utf-8"))
    assert failure["status"] == "FAILED_VALIDATION_NO_ARCHIVE"
    assert failure["error_type"] == "ArchiveError"


def test_corrupt_finalized_source_never_reaches_archive_directory(tmp_path: Path) -> None:
    root = tmp_path / "paper"
    datasets = DatasetStore(root, SESSION_DATE, durable_writes=False)
    datasets.close()
    corrupt = root / "parquet" / SESSION_DATE.isoformat() / "paper_trades.parquet"
    corrupt.write_bytes(b"not parquet")

    with pytest.raises(ArchiveError, match="daily source is unreadable"):
        DailyArchiveBuilder().build(_request(root))

    assert not tuple((root / "daily_archives").iterdir())
    assert not tuple(root.rglob("*.tar.zst"))
    assert not tuple(root.rglob("*.inprogress"))
    failure_markers = tuple((root / "reports").glob("archive_failure_*.json"))
    assert len(failure_markers) == 1
    failure = json.loads(failure_markers[0].read_text(encoding="utf-8"))
    assert failure["status"] == "FAILED_VALIDATION_NO_ARCHIVE"


def test_source_change_during_build_prevents_atomic_publication(tmp_path: Path) -> None:
    root = tmp_path / "paper"
    datasets = DatasetStore(root, SESSION_DATE, durable_writes=False)
    datasets.close()
    source = root / "parquet" / SESSION_DATE.isoformat() / "paper_trades.parquet"
    builder = DailyArchiveBuilder(validator=_SourceMutationValidator(source))

    with pytest.raises(ArchiveError, match="daily source changed during archive build"):
        builder.build(_request(root))

    assert not tuple((root / "daily_archives").iterdir())
    assert not tuple(root.rglob("*.tar.zst"))
    assert not tuple(root.rglob("*.inprogress"))


def test_daily_archive_normalizes_only_proven_carryover_references(tmp_path: Path) -> None:
    root = tmp_path / "bundle"
    data = root / "data"
    data.mkdir(parents=True)
    started = datetime(2026, 7, 15, 4, 0, tzinfo=UTC)
    closed = datetime(2026, 7, 15, 7, 0, tzinfo=UTC)
    rows: dict[str, list[dict[str, object]]] = {name: [] for name in REQUIRED_DATASETS}
    rows["paper_orders.parquet"] = [
        {
            "order_id": "exit-order",
            "session_date": SESSION_DATE,
            "event_ts": closed,
            "signal_id": "external-signal",
            "status": "FULL_FILL",
        }
    ]
    rows["paper_fills.parquet"] = [
        {
            "fill_id": "exit-fill",
            "session_date": SESSION_DATE,
            "event_ts": closed,
            "order_id": "exit-order",
            "quantity": 1,
            "price": 95,
            "fee": 0,
        }
    ]
    rows["paper_positions.parquet"] = [
        {
            "position_event_id": "mark-at-close",
            "session_date": SESSION_DATE,
            "event_ts": closed,
            "position_id": "carry-position",
            "quantity": 1,
            "realized_pnl": 0,
            "status": "OPEN",
            "event_kind": "MARK",
        },
        {
            "position_event_id": "closed-at-same-time",
            "session_date": SESSION_DATE,
            "event_ts": closed,
            "position_id": "carry-position",
            "quantity": 1,
            "realized_pnl": -5,
            "status": "CLOSED",
            "event_kind": "CLOSED",
        },
    ]
    rows["paper_trades.parquet"] = [
        {
            "trade_id": "carry-trade",
            "session_date": SESSION_DATE,
            "event_ts": closed,
            "position_id": "carry-position",
            "entry_fill_id": "external-entry-fill",
            "exit_fill_id": "exit-fill",
            "side": "LONG",
            "quantity": 1,
            "entry_price": 100,
            "exit_price": 95,
            "gross_pnl": -5,
            "fees": 0,
            "net_pnl": -5,
            "entry_ts": started.replace(day=14),
            "exit_ts": closed,
            "holding_duration_seconds": (closed - started.replace(day=14)).total_seconds(),
            "spread_cost": 0,
            "slippage_cost": 0,
            "simulated_latency_cost": 0,
            "holding_cost": 0,
        }
    ]
    rows["mfe_mae.parquet"] = [
        {
            "result_id": "carry-mfe",
            "session_date": SESSION_DATE,
            "event_ts": closed,
            "trade_id": "carry-trade",
            "position_id": "carry-position",
            "mfe": 1,
            "mae": 5,
            "mfe_source_event_id": "external-mfe-source",
            "mae_source_event_id": "external-mae-source",
        }
    ]
    rows["shadow_stop_results.parquet"] = [
        {
            "result_id": "carry-shadow-stop",
            "session_date": SESSION_DATE,
            "event_ts": closed,
            "signal_id": "external-signal",
            "position_id": "carry-position",
            "source_event_id": "external-stop-source",
        }
    ]
    rows["raw_orderbook_event_windows.parquet"] = [
        {
            "raw_event_id": "raw-current",
            "source_event_id": "source-current",
            "session_date": SESSION_DATE,
            "event_ts": closed,
            "receive_ts": closed,
            "window_id": "external-signal",
            "signal_id": "external-signal",
        }
    ]
    for name, values in rows.items():
        pq.write_table(
            pa.Table.from_pylist(values, schema=DATASET_SCHEMAS[name]),
            data / name,
            compression="zstd",
        )

    report = _normalize_daily_carryovers(data, start_utc=started)
    (root / "CARRYOVER_REFERENCES.json").write_text(
        json.dumps(report), encoding="utf-8"
    )

    assert report["reference_count"] == 7
    assert pq.read_table(data / "paper_trades.parquet").to_pylist()[0]["entry_fill_id"] is None
    positions = pq.read_table(data / "paper_positions.parquet")
    tables = {
        name: pq.read_table(data / name)
        for name in REQUIRED_DATASETS
        if name not in {
            "raw_orderbook_event_windows.parquet",
            "raw_trades_event_windows.parquet",
            "raw_last_price_event_windows.parquet",
            "raw_candles_event_windows.parquet",
        }
    }
    assert positions.num_rows == 2
    errors: list[str] = []
    connection = duckdb.connect(":memory:")
    try:
        _validate_foreign_key_paths(root, connection, errors)
        _validate_carryover_references(root, connection, errors)
    finally:
        connection.close()
    _validate_temporal_and_position_reconciliation(tables, errors)
    assert errors == []
