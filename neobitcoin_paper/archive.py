"""Daily typed archive materialization and independent validation."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import tarfile
import tempfile
import threading
from collections import Counter
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import asdict, dataclass, is_dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Final
from zoneinfo import ZoneInfo

import duckdb
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.compute as pc  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
import zstandard

from neobitcoin_paper.coverage import classify_session, verify_coverage_report
from neobitcoin_paper.datasets import DATASET_SCHEMAS, REQUIRED_DATASETS, DatasetStore

REQUIRED_DOCUMENTS: Final = (
    "README.md",
    "DAILY_SUMMARY.md",
    "MANIFEST.json",
    "MANIFEST.md",
    "SCHEMA_DICTIONARY.md",
    "VALIDATION_REPORT.md",
    "SESSION_CALENDAR.json",
    "STRATEGY_REGISTRY.json",
    "CONFIG_SNAPSHOT.json",
    "CODE_VERSION.json",
    "SHA256SUMS",
)
ARCHIVE_FORMAT_VERSION: Final = 2
ARCHIVE_SCHEMA_VERSION: Final = "neobitcoin-paper-schema-v2"
_TOKEN_PATTERN: Final = re.compile(rb"(?<![A-Za-z0-9_.=-])t\.[A-Za-z0-9_.=-]{20,}")
_SECRET_ASSIGNMENT: Final = re.compile(
    rb"(?i)[\"']?(authorization|token|api[_-]?key|secret)[\"']?"
    rb"\s*[:=]\s*[\"']?[^\s,;\"']{8,}"
)
_SHA256_PATTERN: Final = re.compile(r"^[0-9a-f]{64}$")
_MOSCOW: Final = ZoneInfo("Europe/Moscow")
_STREAMED_VALIDATION_DATASETS: Final = frozenset(
    {
        "raw_orderbook_event_windows.parquet",
        "raw_trades_event_windows.parquet",
        "raw_last_price_event_windows.parquet",
        "raw_candles_event_windows.parquet",
    }
)
_ARCHIVE_THREAD_LOCK = threading.Lock()


@contextmanager
def _exclusive_archive_lock(path: Path) -> Iterator[None]:
    """Serialize archive publication across threads and Linux processes."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with _ARCHIVE_THREAD_LOCK, path.open("a+b") as handle:
        if os.name == "posix":
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)  # type: ignore[attr-defined]
        try:
            yield
        finally:
            if os.name == "posix":
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)  # type: ignore[attr-defined]


class ArchiveError(RuntimeError):
    """Archive creation or validation failed closed."""


@dataclass(frozen=True, slots=True)
class ArchiveBuildRequest:
    data_root: Path
    session_date: date
    start_utc: datetime
    end_utc: datetime
    strategy_registry: tuple[dict[str, object], ...]
    session_calendar: dict[str, object]
    config_snapshot: dict[str, object]
    code_version: dict[str, object]
    instrument_snapshot: dict[str, object]
    test_archive: bool = False


@dataclass(frozen=True, slots=True)
class ArchiveValidationResult:
    passed: bool
    errors: tuple[str, ...]
    warnings: tuple[str, ...]
    row_counts: dict[str, int]
    archive_sha256: str | None = None
    zstd_verified: bool = False
    duckdb_verified: bool = False
    pyarrow_verified: bool = False
    independent_reconciliation_verified: bool = False
    unexplained_discrepancies: int = 0


@dataclass(frozen=True, slots=True)
class BuiltArchive:
    archive_id: str
    archive_path: Path
    sha256_path: Path
    sha256: str
    size_bytes: int
    validation: ArchiveValidationResult
    manifest: dict[str, object]


class DailyArchiveBuilder:
    def __init__(self, validator: ArchiveValidator | None = None) -> None:
        self._validator = validator or ArchiveValidator()

    def build(
        self,
        request: ArchiveBuildRequest,
        *,
        dataset_store: DatasetStore | None = None,
    ) -> BuiltArchive:
        root = request.data_root.resolve()
        lock_path = root / "state" / "archive-build.lock"
        with _exclusive_archive_lock(lock_path):
            return self._build_locked(request, dataset_store=dataset_store)

    def _build_locked(
        self,
        request: ArchiveBuildRequest,
        *,
        dataset_store: DatasetStore | None = None,
    ) -> BuiltArchive:
        root = request.data_root.resolve()
        start_utc = _utc(request.start_utc)
        end_utc = _utc(request.end_utc)
        if end_utc <= start_utc:
            raise ArchiveError("archive end must follow start")
        if dataset_store is not None:
            dataset_store.close()
        parquet_dir = root / "parquet" / request.session_date.isoformat()
        missing = [name for name in REQUIRED_DATASETS if not (parquet_dir / name).is_file()]
        if missing:
            raise ArchiveError("daily datasets are not finalized: " + ", ".join(missing))
        active_dir = root / "active" / request.session_date.isoformat()
        session_markers = (
            *active_dir.rglob("*.inprogress"),
            *parquet_dir.rglob("*.inprogress"),
        )
        if session_markers:
            raise ArchiveError("session inprogress files remain before archive build")

        archive_id = _archive_id(request)
        destination = root / "daily_archives"
        destination.mkdir(parents=True, exist_ok=True)
        filename = _archive_filename(request)
        final_path = destination / filename
        sidecar = Path(str(final_path) + ".sha256")
        existing = _reuse_existing_archive(
            archive_id=archive_id,
            final_path=final_path,
            sidecar=sidecar,
            validator=self._validator,
        )
        if existing is not None:
            return existing

        work_root = root / "state" / "archive_work"
        work_root.mkdir(parents=True, exist_ok=True)
        work_dir = Path(tempfile.mkdtemp(prefix=f".{archive_id}-", dir=work_root))
        stage = work_dir / "bundle"
        stage.mkdir()
        temporary = work_dir / "payload.tar.zst"
        sidecar_tmp = work_dir / "payload.tar.zst.sha256"
        published_archive = False
        published_sidecar = False
        try:
            data_dir = stage / "data"
            data_dir.mkdir()
            source_fingerprints = _inspect_daily_parquet_sources(parquet_dir)
            for name in REQUIRED_DATASETS:
                source = parquet_dir / name
                copied = data_dir / name
                os.link(source, copied)
                expected_size, expected_mtime_ns, expected_sha = source_fingerprints[name]
                source_stat = source.stat()
                if (
                    source_stat.st_size != expected_size
                    or source_stat.st_mtime_ns != expected_mtime_ns
                    or not os.path.samefile(source, copied)
                    or copied.stat().st_size != expected_size
                    or _sha256_file(copied) != expected_sha
                ):
                    raise ArchiveError(f"daily source changed while staging {name}")

            carryovers = _normalize_daily_carryovers(data_dir, start_utc=start_utc)
            _write_json(stage / "CARRYOVER_REFERENCES.json", carryovers)

            row_counts = {
                name: int(pq.ParquetFile(data_dir / name).metadata.num_rows)
                for name in REQUIRED_DATASETS
            }
            summary = _daily_summary(data_dir, request.strategy_registry, row_counts)
            coverage = classify_session(
                data_dir,
                start_utc=start_utc,
                end_utc=end_utc,
                test_archive=request.test_archive,
            )
            _write_json(stage / "SESSION_COVERAGE.json", coverage)
            _write_json(stage / "SESSION_CALENDAR.json", request.session_calendar)
            _write_json(stage / "STRATEGY_REGISTRY.json", request.strategy_registry)
            safe_config = dict(request.config_snapshot)
            safe_config["paper_only"] = True
            _write_json(stage / "CONFIG_SNAPSHOT.json", safe_config)
            _write_json(stage / "CODE_VERSION.json", request.code_version)
            (stage / "README.md").write_text(
                _readme(request, archive_id), encoding="utf-8"
            )
            (stage / "DAILY_SUMMARY.md").write_text(summary, encoding="utf-8")
            (stage / "SCHEMA_DICTIONARY.md").write_text(
                _schema_dictionary(), encoding="utf-8"
            )
            manifest: dict[str, object] = {
                "archive_id": archive_id,
                "archive_type": "TEST" if request.test_archive else "OOS_DAILY",
                "schema_version": ARCHIVE_SCHEMA_VERSION,
                "session_date": request.session_date.isoformat(),
                "start_utc": start_utc.isoformat(),
                "end_utc": end_utc.isoformat(),
                "created_at": datetime.now(UTC).isoformat(),
                "paper_only": True,
                "instrument": request.instrument_snapshot,
                "row_counts": row_counts,
                "strategies": len(request.strategy_registry),
                "datasets": [f"data/{name}" for name in REQUIRED_DATASETS],
                "dataset_schema_sha256": {
                    name: _schema_sha256(pq.read_schema(data_dir / name))
                    for name in REQUIRED_DATASETS
                },
                "validation_policy": "zero unexplained discrepancies",
                "coverage_schema_version": 1,
                "session_classification": coverage["classification"],
                "oos_included": coverage["oos_included"],
                "investigation_required": coverage["investigation_required"],
                "coverage_report_sha256": coverage["report_sha256"],
                "carryover_schema_version": 1,
                "carryover_reference_count": carryovers["reference_count"],
                "carryover_report_sha256": carryovers["report_sha256"],
            }
            _write_json(stage / "MANIFEST.json", manifest)
            (stage / "MANIFEST.md").write_text(
                _manifest_markdown(manifest, row_counts), encoding="utf-8"
            )

            preliminary = self._validator.validate_bundle(stage, verify_sums=False)
            if not preliminary.passed:
                raise ArchiveError("bundle validation failed: " + "; ".join(preliminary.errors))
            (stage / "VALIDATION_REPORT.md").write_text(
                _validation_report(preliminary), encoding="utf-8"
            )
            _write_sums(stage)
            final_bundle = self._validator.validate_bundle(stage, verify_sums=True)
            if not final_bundle.passed:
                raise ArchiveError(
                    "final bundle validation failed: " + "; ".join(final_bundle.errors)
                )

            _write_tar_zst(stage, temporary)
            sha = _sha256_file(temporary)
            archive_validation = self._validator.validate_archive(
                temporary, expected_sha256=sha
            )
            if not archive_validation.passed:
                raise ArchiveError(
                    "compressed archive validation failed: "
                    + "; ".join(archive_validation.errors)
                )
            _verify_daily_parquet_sources(parquet_dir, source_fingerprints)
            sidecar_tmp.write_text(f"{sha}  {final_path.name}\n", encoding="ascii")
            _fsync_file(sidecar_tmp)
            temporary.chmod(0o640)
            sidecar_tmp.chmod(0o640)
            os.replace(sidecar_tmp, sidecar)
            published_sidecar = True
            _fsync_directory(destination)
            os.replace(temporary, final_path)
            published_archive = True
            _fsync_directory(destination)
            return BuiltArchive(
                archive_id=archive_id,
                archive_path=final_path,
                sha256_path=sidecar,
                sha256=sha,
                size_bytes=final_path.stat().st_size,
                validation=archive_validation,
                manifest=manifest,
            )
        except Exception as exc:
            if published_archive:
                final_path.unlink(missing_ok=True)
            if published_sidecar:
                sidecar.unlink(missing_ok=True)
            _fsync_directory(destination)
            failure_payload = {
                "archive_id": archive_id,
                "failed_at": datetime.now(UTC).isoformat(),
                "status": "FAILED_VALIDATION_NO_ARCHIVE",
                "error_type": type(exc).__name__,
            }
            report_dir = root / "reports"
            report_dir.mkdir(parents=True, exist_ok=True)
            failure = report_dir / f"archive_failure_{archive_id}.json"
            _write_json(failure, failure_payload)
            raise
        finally:
            shutil.rmtree(work_dir, ignore_errors=True)


def _reuse_existing_archive(
    *,
    archive_id: str,
    final_path: Path,
    sidecar: Path,
    validator: ArchiveValidator,
) -> BuiltArchive | None:
    """Reuse a verified finalization; discard and rebuild any incomplete pair."""

    if not final_path.exists() and not sidecar.exists():
        return None
    digest = _read_sidecar_digest(sidecar, final_path.name)
    if final_path.is_file() and digest is not None:
        validation = validator.validate_archive(final_path, expected_sha256=digest)
        if validation.passed:
            try:
                manifest = _read_archive_manifest(final_path)
            except (ArchiveError, OSError, json.JSONDecodeError):
                pass
            else:
                if manifest.get("archive_id") == archive_id:
                    return BuiltArchive(
                        archive_id=archive_id,
                        archive_path=final_path,
                        sha256_path=sidecar,
                        sha256=digest,
                        size_bytes=final_path.stat().st_size,
                        validation=validation,
                        manifest=manifest,
                    )
    final_path.unlink(missing_ok=True)
    sidecar.unlink(missing_ok=True)
    return None


def _inspect_daily_parquet_sources(parquet_dir: Path) -> dict[str, tuple[int, int, str]]:
    """Prove the finalized source set is immutable and readable before copying."""

    fingerprints: dict[str, tuple[int, int, str]] = {}
    connection = duckdb.connect(":memory:")
    try:
        for name in REQUIRED_DATASETS:
            path = parquet_dir / name
            if path.is_symlink() or not path.is_file():
                raise ArchiveError(f"daily source is not a regular file: {name}")
            try:
                schema = pq.read_schema(path)
                metadata = pq.read_metadata(path)
                counted = connection.execute(
                    "SELECT count(*) FROM read_parquet(?)", [str(path)]
                ).fetchone()
            except (duckdb.Error, OSError, pa.ArrowException) as exc:
                raise ArchiveError(f"daily source is unreadable: {name}") from exc
            if not schema.equals(DATASET_SCHEMAS[name], check_metadata=True):
                raise ArchiveError(f"daily source schema mismatch: {name}")
            if counted is None or int(counted[0]) != metadata.num_rows:
                raise ArchiveError(f"daily source row-count mismatch: {name}")
            source_stat = path.stat()
            fingerprints[name] = (
                source_stat.st_size,
                source_stat.st_mtime_ns,
                _sha256_file(path),
            )
    finally:
        connection.close()
    return fingerprints


def _verify_daily_parquet_sources(
    parquet_dir: Path,
    fingerprints: dict[str, tuple[int, int, str]],
) -> None:
    """Reject publication if any finalized source changed during the build."""

    for name in REQUIRED_DATASETS:
        path = parquet_dir / name
        expected_size, expected_mtime_ns, expected_sha = fingerprints[name]
        if path.is_symlink() or not path.is_file():
            raise ArchiveError(f"daily source changed during archive build: {name}")
        current = path.stat()
        if (
            current.st_size != expected_size
            or current.st_mtime_ns != expected_mtime_ns
            or _sha256_file(path) != expected_sha
        ):
            raise ArchiveError(f"daily source changed during archive build: {name}")


def _read_sidecar_digest(sidecar: Path, archive_name: str) -> str | None:
    if not sidecar.is_file():
        return None
    try:
        line = sidecar.read_text(encoding="ascii").strip()
    except (OSError, UnicodeError):
        return None
    if "  " not in line:
        return None
    digest, filename = line.split("  ", 1)
    normalized = digest.strip().lower()
    if filename.strip() != archive_name or not _SHA256_PATTERN.fullmatch(normalized):
        return None
    return normalized


def _read_archive_manifest(archive: Path) -> dict[str, object]:
    extracted = Path(tempfile.mkdtemp(prefix="neobitcoin-paper-manifest-"))
    try:
        _extract_tar_zst(archive, extracted)
        loaded = json.loads((extracted / "MANIFEST.json").read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            raise ArchiveError("archive manifest is not an object")
        return {str(key): value for key, value in loaded.items()}
    finally:
        shutil.rmtree(extracted, ignore_errors=True)


def _normalize_daily_carryovers(data_dir: Path, *, start_utc: datetime) -> dict[str, object]:
    """Make cross-session references explicit without weakening daily validation."""

    small_tables = {
        name: pq.read_table(data_dir / name).to_pylist()
        for name in REQUIRED_DATASETS
        if name not in _STREAMED_VALIDATION_DATASETS
    }
    signal_ids = {
        str(row["signal_id"])
        for row in small_tables["candidate_signals.parquet"]
        if row.get("signal_id") not in (None, "")
    }
    fill_ids = {
        str(row["fill_id"])
        for row in small_tables["paper_fills.parquet"]
        if row.get("fill_id") not in (None, "")
    }
    position_rows = small_tables["paper_positions.parquet"]
    position_ids = {
        str(row["position_id"])
        for row in position_rows
        if row.get("position_id") not in (None, "")
    }
    opened_here = {
        str(row["position_id"])
        for row in position_rows
        if str(row.get("event_kind") or "").upper() == "OPEN"
    }
    carryover_positions = position_ids - opened_here

    external_signal_ids: set[str] = set()
    for name in ("shadow_stop_results.parquet", "shadow_exit_results.parquet"):
        for row in small_tables[name]:
            signal_id = str(row.get("signal_id") or "")
            if (
                signal_id
                and signal_id not in signal_ids
                and str(row.get("position_id") or "") in carryover_positions
            ):
                external_signal_ids.add(signal_id)

    raw_source_ids: set[str] = set()
    raw_signal_ids: dict[str, set[str]] = {}
    for name in _STREAMED_VALIDATION_DATASETS:
        parquet = pq.ParquetFile(data_dir / name)
        dataset_signal_ids: set[str] = set()
        try:
            for batch in parquet.iter_batches(
                columns=["raw_event_id", "source_event_id", "signal_id"]
            ):
                for column in range(2):
                    raw_source_ids.update(
                        str(value)
                        for value in batch.column(column).to_pylist()
                        if value
                    )
                dataset_signal_ids.update(
                    str(value) for value in batch.column(2).to_pylist() if value
                )
        finally:
            parquet.close(force=True)
        raw_signal_ids[name] = dataset_signal_ids

    references: list[dict[str, str]] = []

    def externalize(
        dataset: str,
        row: dict[str, Any],
        field: str,
        reason: str,
    ) -> bool:
        external_id = str(row.get(field) or "")
        if not external_id:
            return False
        schema = DATASET_SCHEMAS[dataset]
        primary_key = (schema.metadata or {})[b"primary_key"].decode("utf-8")
        raw_extra = row.get("extra_json")
        try:
            extra = json.loads(str(raw_extra)) if raw_extra else {}
        except json.JSONDecodeError as exc:
            raise ArchiveError(f"invalid extra_json in {dataset}") from exc
        if not isinstance(extra, dict):
            raise ArchiveError(f"invalid extra_json object in {dataset}")
        external = extra.setdefault("external_references", {})
        if not isinstance(external, dict):
            raise ArchiveError(f"invalid external references in {dataset}")
        declared = {"id": external_id, "reason": reason}
        existing = external.get(field)
        if existing not in (None, declared):
            raise ArchiveError(f"conflicting external reference in {dataset}.{field}")
        external[field] = declared
        row[field] = None
        row["extra_json"] = json.dumps(
            extra, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        references.append(
            {
                "dataset": dataset,
                "primary_key": primary_key,
                "row_id": str(row.get(primary_key) or ""),
                "field": field,
                "external_id": external_id,
                "reason": reason,
            }
        )
        return True

    mutable: dict[str, list[dict[str, Any]]] = {
        name: [dict(row) for row in rows] for name, rows in small_tables.items()
    }
    for name in (
        "paper_orders.parquet",
        "shadow_stop_results.parquet",
        "shadow_exit_results.parquet",
    ):
        for row in mutable[name]:
            signal_id = str(row.get("signal_id") or "")
            if signal_id in external_signal_ids:
                externalize(name, row, "signal_id", "CARRYOVER_SIGNAL")

    for row in mutable["paper_trades.parquet"]:
        position_id = str(row.get("position_id") or "")
        entry_fill_id = str(row.get("entry_fill_id") or "")
        entry_ts = row.get("entry_ts")
        if (
            position_id in carryover_positions
            and entry_fill_id
            and entry_fill_id not in fill_ids
            and isinstance(entry_ts, datetime)
            and entry_ts < start_utc
        ):
            externalize(
                "paper_trades.parquet",
                row,
                "entry_fill_id",
                "CARRYOVER_ENTRY_FILL",
            )

    for name, fields in (
        ("mfe_mae.parquet", ("mfe_source_event_id", "mae_source_event_id")),
        ("shadow_stop_results.parquet", ("source_event_id",)),
        ("shadow_exit_results.parquet", ("source_event_id",)),
    ):
        for row in mutable[name]:
            if str(row.get("position_id") or "") not in carryover_positions:
                continue
            for field in fields:
                source_id = str(row.get(field) or "")
                if source_id and source_id not in raw_source_ids:
                    externalize(name, row, field, "CARRYOVER_SOURCE_EVENT")

    for name, rows in mutable.items():
        if rows != small_tables[name]:
            _rewrite_parquet_rows(data_dir / name, name, lambda _row: False, rows=rows)

    if external_signal_ids:
        def raw_mutator(dataset: str) -> Callable[[dict[str, Any]], bool]:
            def mutate(row: dict[str, Any]) -> bool:
                if str(row.get("signal_id") or "") not in external_signal_ids:
                    return False
                return externalize(dataset, row, "signal_id", "CARRYOVER_SIGNAL")

            return mutate

        for name in _STREAMED_VALIDATION_DATASETS:
            if not external_signal_ids.intersection(raw_signal_ids[name]):
                continue
            _rewrite_parquet_rows(
                data_dir / name,
                name,
                raw_mutator(name),
            )

    references.sort(
        key=lambda item: (
            item["dataset"],
            item["row_id"],
            item["field"],
            item["external_id"],
        )
    )
    report: dict[str, object] = {
        "schema_version": 1,
        "session_start_utc": start_utc.isoformat(),
        "reference_count": len(references),
        "references": references,
    }
    report["report_sha256"] = hashlib.sha256(
        json.dumps(report, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return report


def _rewrite_parquet_rows(
    path: Path,
    dataset: str,
    mutator: Callable[[dict[str, Any]], bool],
    *,
    rows: list[dict[str, Any]] | None = None,
) -> None:
    """Atomically rewrite one archive-view Parquet while preserving its schema."""

    schema = DATASET_SCHEMAS[dataset]
    temporary = path.with_suffix(path.suffix + ".carryover")
    source_path: Path | None = None
    writer: pq.ParquetWriter | None = None
    changed = rows is not None

    def write_batches(batches: Iterable[list[dict[str, Any]]]) -> None:
        nonlocal writer, changed
        for batch_rows in batches:
            for row in batch_rows:
                changed = mutator(row) or changed
            if writer is None:
                writer = pq.ParquetWriter(
                    temporary,
                    schema,
                    compression="zstd",
                    use_dictionary=dataset not in _STREAMED_VALIDATION_DATASETS,
                    write_statistics=dataset not in _STREAMED_VALIDATION_DATASETS,
                    version="2.6",
                    data_page_version="2.0",
                )
            writer.write_table(pa.Table.from_pylist(batch_rows, schema=schema))
    try:
        try:
            if rows is not None:
                write_batches((rows,))
            else:
                source_fd, source_name = tempfile.mkstemp(
                    prefix=f".{path.name}.",
                    suffix=".source",
                    dir=path.parent.parent.parent,
                )
                os.close(source_fd)
                source_path = Path(source_name)
                source_path.unlink()
                os.replace(path, source_path)
                with source_path.open("rb") as source:
                    parquet = pq.ParquetFile(source)
                    try:
                        write_batches(
                            [dict(row) for row in batch.to_pylist()]
                            for batch in parquet.iter_batches(batch_size=2_048)
                        )
                    finally:
                        parquet.close(force=True)
        finally:
            if writer is not None:
                writer.close()
        if changed:
            os.replace(temporary, path)
        else:
            temporary.unlink(missing_ok=True)
            if source_path is not None:
                os.replace(source_path, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        if source_path is not None and source_path.exists() and not path.exists():
            os.replace(source_path, path)
        raise
    finally:
        if source_path is not None and path.exists():
            # The source lives outside the staged bundle. A delayed Windows
            # handle release must not invalidate an otherwise complete rewrite.
            with suppress(OSError):
                source_path.unlink(missing_ok=True)


class ArchiveValidator:
    """Independent bundle checks using PyArrow, DuckDB, hashes, and zstd."""

    def validate_bundle(
        self, root: Path, *, verify_sums: bool = True
    ) -> ArchiveValidationResult:
        root = root.resolve()
        errors: list[str] = []
        warnings: list[str] = []
        row_counts: dict[str, int] = {}
        for name in REQUIRED_DOCUMENTS:
            if name == "VALIDATION_REPORT.md" and not verify_sums:
                continue
            if name == "SHA256SUMS" and not verify_sums:
                continue
            if not (root / name).is_file():
                errors.append(f"missing document {name}")
        if tuple(root.rglob("*.inprogress")):
            errors.append("bundle contains .inprogress")
        expected_files = _expected_bundle_files(verify_sums=verify_sums)
        actual_files = {
            path.relative_to(root).as_posix()
            for path in root.rglob("*")
            if path.is_file()
        }
        for relative in sorted(actual_files - expected_files):
            errors.append(f"unexpected bundle file {relative}")

        tables: dict[str, pa.Table] = {}
        declared_schema_hashes = _declared_schema_hashes(root)
        pyarrow_ok = True
        duckdb_ok = True
        connection = duckdb.connect(":memory:")
        try:
            for name in REQUIRED_DATASETS:
                path = root / "data" / name
                if not path.is_file():
                    errors.append(f"missing dataset {name}")
                    pyarrow_ok = False
                    duckdb_ok = False
                    continue
                try:
                    parquet = pq.ParquetFile(path)
                    for batch in parquet.iter_batches(batch_size=2_048):
                        batch.validate(full=True)
                except Exception as exc:
                    pyarrow_ok = False
                    duckdb_ok = False
                    errors.append(f"PyArrow cannot read {name}: {type(exc).__name__}")
                    continue
                expected_schema_hash = (
                    declared_schema_hashes.get(name)
                    if declared_schema_hashes is not None
                    else None
                )
                if (
                    declared_schema_hashes is not None
                    and _schema_sha256(parquet.schema_arrow) != expected_schema_hash
                ):
                    errors.append(f"schema mismatch {name}")
                if name not in _STREAMED_VALIDATION_DATASETS:
                    tables[name] = pq.read_table(path)
                _validate_primary_key_path(name, path, parquet, connection, errors)
                _validate_parquet_compression(name, path, errors)
                try:
                    result = connection.execute(
                        "SELECT count(*) FROM read_parquet(?)", [str(path)]
                    ).fetchone()
                    if result is None:
                        raise ArchiveError("DuckDB returned no count row")
                    count = int(
                        result[0]
                    )
                    if count != parquet.metadata.num_rows:
                        errors.append(f"DuckDB/PyArrow row mismatch {name}")
                    row_counts[name] = count
                except Exception as exc:
                    duckdb_ok = False
                    errors.append(f"DuckDB cannot read {name}: {type(exc).__name__}")

            _validate_foreign_key_paths(root, connection, errors)
            _validate_carryover_references(root, connection, errors)
            _validate_source_lineage_paths(root, connection, tables, errors)
            _validate_event_window_coverage_paths(root, connection, tables, errors)
        finally:
            connection.close()

        reconciliation_errors: list[str] = []
        _validate_financial_reconciliation(tables, reconciliation_errors)
        _validate_temporal_and_position_reconciliation(tables, reconciliation_errors)
        errors.extend(reconciliation_errors)
        _validate_strategy_registry(root, errors)
        _validate_manifest(root, row_counts, errors)
        if _contains_secret(root):
            errors.append("secret scan failed")
        if verify_sums:
            _verify_sums(root, errors)
        return ArchiveValidationResult(
            passed=not errors,
            errors=tuple(errors),
            warnings=tuple(warnings),
            row_counts=row_counts,
            duckdb_verified=duckdb_ok and len(row_counts) == len(REQUIRED_DATASETS),
            pyarrow_verified=pyarrow_ok and len(row_counts) == len(REQUIRED_DATASETS),
            independent_reconciliation_verified=not reconciliation_errors,
            unexplained_discrepancies=len(reconciliation_errors),
        )

    def validate_archive(
        self, archive: Path, *, expected_sha256: str | None = None
    ) -> ArchiveValidationResult:
        errors: list[str] = []
        sha = _sha256_file(archive)
        if expected_sha256 is not None and sha != expected_sha256:
            errors.append("archive SHA-256 mismatch")
        extracted = Path(tempfile.mkdtemp(prefix="neobitcoin-paper-verify-"))
        zstd_ok = False
        try:
            try:
                _extract_tar_zst(archive, extracted)
                zstd_ok = True
            except Exception as exc:
                errors.append(f"zstd/tar verification failed: {type(exc).__name__}")
            if errors:
                return ArchiveValidationResult(
                    False,
                    tuple(errors),
                    (),
                    {},
                    archive_sha256=sha,
                    zstd_verified=zstd_ok,
                )
            bundle = self.validate_bundle(extracted, verify_sums=True)
            return ArchiveValidationResult(
                passed=bundle.passed,
                errors=bundle.errors,
                warnings=bundle.warnings,
                row_counts=bundle.row_counts,
                archive_sha256=sha,
                zstd_verified=zstd_ok,
                duckdb_verified=bundle.duckdb_verified,
                pyarrow_verified=bundle.pyarrow_verified,
                independent_reconciliation_verified=(
                    bundle.independent_reconciliation_verified
                ),
                unexplained_discrepancies=bundle.unexplained_discrepancies,
            )
        finally:
            shutil.rmtree(extracted, ignore_errors=True)


def _expected_bundle_files(*, verify_sums: bool) -> set[str]:
    documents = set(REQUIRED_DOCUMENTS)
    if not verify_sums:
        documents.discard("VALIDATION_REPORT.md")
        documents.discard("SHA256SUMS")
    # SESSION_COVERAGE is mandatory for newly built archives (declared by the
    # manifest) but is allow-listed here so older schema-v1 archives remain
    # independently verifiable.
    documents.add("SESSION_COVERAGE.json")
    documents.add("CARRYOVER_REFERENCES.json")
    return documents | {f"data/{name}" for name in REQUIRED_DATASETS}


def _declared_schema_hashes(root: Path) -> dict[str, str] | None:
    try:
        manifest = json.loads((root / "MANIFEST.json").read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    if not isinstance(manifest, dict) or "dataset_schema_sha256" not in manifest:
        return None
    declared = manifest.get("dataset_schema_sha256")
    if not isinstance(declared, dict):
        return {}
    return {str(name): str(digest) for name, digest in declared.items()}


def _validate_manifest(
    root: Path,
    row_counts: dict[str, int],
    errors: list[str],
) -> None:
    path = root / "MANIFEST.json"
    if not path.is_file():
        return
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        errors.append("MANIFEST.json is unreadable")
        return
    if not isinstance(manifest, dict):
        errors.append("MANIFEST.json is not an object")
        return
    if manifest.get("paper_only") is not True:
        errors.append("manifest PAPER_ONLY assertion is missing")
    if manifest.get("archive_type") not in {"TEST", "OOS_DAILY"}:
        errors.append("manifest archive_type is invalid")
    if manifest.get("coverage_schema_version") is not None:
        coverage_path = root / "SESSION_COVERAGE.json"
        try:
            coverage = json.loads(coverage_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            errors.append("SESSION_COVERAGE.json is unreadable")
        else:
            if not isinstance(coverage, dict) or not verify_coverage_report(coverage):
                errors.append("session coverage report hash mismatch")
            elif (
                manifest.get("coverage_report_sha256") != coverage.get("report_sha256")
                or manifest.get("session_classification") != coverage.get("classification")
                or manifest.get("oos_included") != coverage.get("oos_included")
                or manifest.get("investigation_required")
                != coverage.get("investigation_required")
            ):
                errors.append("manifest session coverage mismatch")
    if manifest.get("carryover_schema_version") is not None:
        carryover_path = root / "CARRYOVER_REFERENCES.json"
        try:
            carryovers = json.loads(carryover_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            errors.append("CARRYOVER_REFERENCES.json is unreadable")
        else:
            if not isinstance(carryovers, dict):
                errors.append("carryover report is not an object")
            elif (
                manifest.get("carryover_schema_version")
                != carryovers.get("schema_version")
                or manifest.get("carryover_reference_count")
                != carryovers.get("reference_count")
                or manifest.get("carryover_report_sha256")
                != carryovers.get("report_sha256")
            ):
                errors.append("manifest carryover report mismatch")
    expected_datasets = [f"data/{name}" for name in REQUIRED_DATASETS]
    if manifest.get("datasets") != expected_datasets:
        errors.append("manifest dataset list mismatch")
    declared_counts = manifest.get("row_counts")
    if not isinstance(declared_counts, dict):
        errors.append("manifest row_counts is invalid")
        return
    normalized_counts: dict[str, int] = {}
    try:
        normalized_counts = {str(key): int(value) for key, value in declared_counts.items()}
    except (TypeError, ValueError):
        errors.append("manifest row_counts contains a non-integer")
        return
    if normalized_counts != row_counts:
        errors.append("manifest row_counts mismatch")
    if manifest.get("schema_version") == ARCHIVE_SCHEMA_VERSION:
        schema_hashes = manifest.get("dataset_schema_sha256")
        if (
            not isinstance(schema_hashes, dict)
            or set(schema_hashes) != set(REQUIRED_DATASETS)
            or any(
                not isinstance(value, str) or not _SHA256_PATTERN.fullmatch(value)
                for value in schema_hashes.values()
            )
        ):
            errors.append("manifest dataset schema fingerprints mismatch")


def _validate_carryover_references(
    root: Path,
    connection: duckdb.DuckDBPyConnection,
    errors: list[str],
) -> None:
    path = root / "CARRYOVER_REFERENCES.json"
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return
    if not isinstance(report, dict):
        return
    expected_hash = report.get("report_sha256")
    body = dict(report)
    body.pop("report_sha256", None)
    actual_hash = hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    references = report.get("references")
    if (
        report.get("schema_version") != 1
        or expected_hash != actual_hash
        or not isinstance(references, list)
        or report.get("reference_count") != len(references)
    ):
        errors.append("carryover report hash or count mismatch")
        return
    try:
        session_start = datetime.fromisoformat(
            str(report.get("session_start_utc") or "").replace("Z", "+00:00")
        )
    except ValueError:
        errors.append("carryover session start is invalid")
        return
    if session_start.tzinfo is None:
        errors.append("carryover session start is not UTC-aware")
        return

    position_path = root / "data" / "paper_positions.parquet"
    carryover_positions: set[str] = set()
    if position_path.is_file():
        rows = connection.execute(
            "SELECT position_id FROM "
            f"{_duckdb_parquet(position_path)} GROUP BY position_id "
            "HAVING sum(CASE WHEN upper(coalesce(event_kind, '')) = 'OPEN' THEN 1 ELSE 0 END) = 0"
        ).fetchall()
        carryover_positions = {str(row[0]) for row in rows if row and row[0]}

    seen: set[tuple[str, str, str, str]] = set()
    anchored_signals: set[str] = set()
    deferred_signals: list[str] = []
    allowed_reasons = {
        "CARRYOVER_SIGNAL",
        "CARRYOVER_ENTRY_FILL",
        "CARRYOVER_SOURCE_EVENT",
    }
    for item in references:
        if not isinstance(item, dict):
            errors.append("carryover reference row is invalid")
            continue
        dataset = str(item.get("dataset") or "")
        primary_key = str(item.get("primary_key") or "")
        row_id = str(item.get("row_id") or "")
        field = str(item.get("field") or "")
        external_id = str(item.get("external_id") or "")
        reason = str(item.get("reason") or "")
        key = (dataset, row_id, field, external_id)
        if key in seen:
            errors.append("duplicate carryover reference")
            continue
        seen.add(key)
        schema = DATASET_SCHEMAS.get(dataset)
        expected_primary = (
            (schema.metadata or {}).get(b"primary_key", b"").decode() if schema else ""
        )
        if (
            schema is None
            or primary_key != expected_primary
            or field not in schema.names
            or not row_id
            or not external_id
            or reason not in allowed_reasons
        ):
            errors.append(f"invalid carryover reference {dataset}:{row_id}:{field}")
            continue
        dataset_path = root / "data" / dataset
        row = connection.execute(
            f"SELECT {_duckdb_identifier(field)}, extra_json"
            f" FROM {_duckdb_parquet(dataset_path)}"
            f" WHERE {_duckdb_identifier(primary_key)} = ?",
            [row_id],
        ).fetchall()
        if len(row) != 1 or row[0][0] is not None:
            errors.append(f"carryover row mismatch {dataset}:{row_id}:{field}")
            continue
        try:
            extra = json.loads(str(row[0][1])) if row[0][1] else {}
            declared = extra["external_references"][field]
        except (KeyError, TypeError, json.JSONDecodeError):
            errors.append(f"carryover metadata missing {dataset}:{row_id}:{field}")
            continue
        if declared != {"id": external_id, "reason": reason}:
            errors.append(f"carryover metadata mismatch {dataset}:{row_id}:{field}")
            continue

        names = set(schema.names)
        position_id = ""
        if "position_id" in names:
            found = connection.execute(
                f"SELECT position_id FROM {_duckdb_parquet(dataset_path)}"
                f" WHERE {_duckdb_identifier(primary_key)} = ?",
                [row_id],
            ).fetchone()
            position_id = str(found[0] or "") if found else ""
        if reason == "CARRYOVER_ENTRY_FILL":
            found = connection.execute(
                f"SELECT position_id, CAST(entry_ts AS VARCHAR)"
                f" FROM {_duckdb_parquet(dataset_path)}"
                f" WHERE {_duckdb_identifier(primary_key)} = ?",
                [row_id],
            ).fetchone()
            try:
                entry_ts = (
                    datetime.fromisoformat(str(found[1]).replace("Z", "+00:00"))
                    if found
                    else None
                )
            except ValueError:
                entry_ts = None
            if (
                not found
                or str(found[0] or "") not in carryover_positions
                or entry_ts is None
                or entry_ts.tzinfo is None
                or entry_ts >= session_start
            ):
                errors.append(f"invalid carryover entry fill {dataset}:{row_id}")
        elif reason == "CARRYOVER_SOURCE_EVENT":
            if position_id not in carryover_positions:
                errors.append(f"invalid carryover source event {dataset}:{row_id}")
        elif reason == "CARRYOVER_SIGNAL":
            if position_id:
                if position_id not in carryover_positions:
                    errors.append(f"invalid carryover signal {dataset}:{row_id}")
                else:
                    anchored_signals.add(external_id)
            else:
                deferred_signals.append(external_id)
    if any(signal_id not in anchored_signals for signal_id in deferred_signals):
        errors.append("unanchored carryover signal reference")


def _validate_parquet_compression(name: str, path: Path, errors: list[str]) -> None:
    metadata = pq.ParquetFile(path).metadata
    codecs = {
        str(metadata.row_group(group).column(column).compression).upper()
        for group in range(metadata.num_row_groups)
        for column in range(metadata.row_group(group).num_columns)
    }
    if codecs and codecs != {"ZSTD"}:
        errors.append(f"non-ZSTD Parquet compression {name}")


def _validate_primary_key(name: str, table: pa.Table, errors: list[str]) -> None:
    metadata = table.schema.metadata or {}
    primary = metadata.get(b"primary_key", b"").decode()
    if not primary or primary not in table.column_names:
        errors.append(f"missing primary key metadata {name}")
        return
    values = table[primary]
    if values.null_count:
        errors.append(f"null primary key {name}")
    if pc.count_distinct(values).as_py() != table.num_rows:
        errors.append(f"duplicate primary key {name}")
    if "event_ts" in table.column_names and table.num_rows > 1:
        timestamps = table["event_ts"].to_pylist()
        if timestamps != sorted(timestamps):
            errors.append(f"timestamp sort failure {name}")


def _validate_primary_key_path(
    name: str,
    path: Path,
    parquet: pq.ParquetFile,
    connection: duckdb.DuckDBPyConnection,
    errors: list[str],
) -> None:
    metadata = parquet.schema_arrow.metadata or {}
    primary = metadata.get(b"primary_key", b"").decode()
    if not primary or primary not in parquet.schema_arrow.names:
        errors.append(f"missing primary key metadata {name}")
        return
    relation = _duckdb_parquet(path)
    key = _duckdb_identifier(primary)
    try:
        row = connection.execute(
            f"SELECT count(*), count({key}), count(DISTINCT {key}) FROM {relation}"
        ).fetchone()
        if row is None:
            raise ArchiveError("DuckDB returned no primary-key row")
        total, nonnull, distinct = (int(value) for value in row)
        if nonnull != total:
            errors.append(f"null primary key {name}")
        if distinct != total:
            errors.append(f"duplicate primary key {name}")
    except Exception as exc:
        errors.append(f"primary key validation failed {name}: {type(exc).__name__}")

    if (
        name in _STREAMED_VALIDATION_DATASETS
        or "event_ts" not in parquet.schema_arrow.names
        or parquet.metadata.num_rows <= 1
    ):
        return
    previous: datetime | None = None
    try:
        for batch in parquet.iter_batches(batch_size=65_536, columns=["event_ts"]):
            for value in batch.column(0).to_pylist():
                if previous is not None and value < previous:
                    errors.append(f"timestamp sort failure {name}")
                    return
                previous = value
    except Exception as exc:
        errors.append(f"timestamp validation failed {name}: {type(exc).__name__}")


def _validate_foreign_key_paths(
    root: Path,
    connection: duckdb.DuckDBPyConnection,
    errors: list[str],
) -> None:
    for name, schema in DATASET_SCHEMAS.items():
        source_path = root / "data" / name
        if not source_path.is_file():
            continue
        metadata = schema.metadata or {}
        raw_foreign_keys = metadata.get(b"foreign_keys", b"{}").decode()
        for field, target in json.loads(raw_foreign_keys).items():
            target_table, target_field = str(target).split(".", 1)
            target_name = f"{target_table}.parquet"
            target_path = root / "data" / target_name
            if target_name not in DATASET_SCHEMAS or not target_path.is_file():
                continue
            source_column = _duckdb_identifier(str(field))
            target_column = _duckdb_identifier(target_field)
            try:
                row = connection.execute(
                    "SELECT count(*) FROM "
                    f"{_duckdb_parquet(source_path)} AS source "
                    f"WHERE source.{source_column} IS NOT NULL "
                    f"AND CAST(source.{source_column} AS VARCHAR) <> '' "
                    "AND NOT EXISTS (SELECT 1 FROM "
                    f"{_duckdb_parquet(target_path)} AS target "
                    f"WHERE target.{target_column} = source.{source_column})"
                ).fetchone()
                if row is None or int(row[0]) != 0:
                    errors.append(f"foreign key failure {name}.{field}")
            except Exception as exc:
                errors.append(
                    f"foreign key validation failed {name}.{field}: {type(exc).__name__}"
                )


def _raw_union(root: Path, column: str) -> str:
    identifier = _duckdb_identifier(column)
    selects = [
        f"SELECT {identifier} AS value FROM {_duckdb_parquet(root / 'data' / name)} "
        f"WHERE {identifier} IS NOT NULL AND CAST({identifier} AS VARCHAR) <> ''"
        for name in sorted(_STREAMED_VALIDATION_DATASETS)
        if (root / "data" / name).is_file()
    ]
    return " UNION ".join(selects) if selects else "SELECT NULL AS value WHERE false"


def _validate_source_lineage_paths(
    root: Path,
    connection: duckdb.DuckDBPyConnection,
    tables: dict[str, pa.Table],
    errors: list[str],
) -> None:
    signals = tables.get("candidate_signals.parquet")
    if signals is None or signals.num_rows == 0:
        return
    raw_sources = (
        f"{_raw_union(root, 'raw_event_id')} UNION "
        f"{_raw_union(root, 'source_event_id')}"
    )
    for dataset, source_fields in (
        ("paper_fills.parquet", ("source_event_id",)),
        ("mfe_mae.parquet", ("mfe_source_event_id", "mae_source_event_id")),
        ("shadow_stop_results.parquet", ("source_event_id",)),
        ("shadow_exit_results.parquet", ("source_event_id",)),
    ):
        path = root / "data" / dataset
        if not path.is_file():
            continue
        for field in source_fields:
            column = _duckdb_identifier(field)
            row = connection.execute(
                f"WITH raw_sources AS ({raw_sources}) "
                f"SELECT count(*) FROM {_duckdb_parquet(path)} AS item "
                f"WHERE item.{column} IS NOT NULL AND CAST(item.{column} AS VARCHAR) <> '' "
                "AND NOT EXISTS (SELECT 1 FROM raw_sources "
                f"WHERE raw_sources.value = item.{column})"
            ).fetchone()
            if row is None or int(row[0]) != 0:
                errors.append(f"source lineage failure {dataset}:{field}")


def _validate_event_window_coverage_paths(
    root: Path,
    connection: duckdb.DuckDBPyConnection,
    tables: dict[str, pa.Table],
    errors: list[str],
) -> None:
    signals = tables.get("candidate_signals.parquet")
    if signals is None or signals.num_rows == 0:
        return
    signal_path = root / "data" / "candidate_signals.parquet"
    covered = _raw_union(root, "signal_id")
    row = connection.execute(
        f"WITH covered AS ({covered}) "
        f"SELECT count(*) FROM {_duckdb_parquet(signal_path)} AS signal "
        "WHERE signal.signal_id IS NOT NULL AND CAST(signal.signal_id AS VARCHAR) <> '' "
        "AND NOT EXISTS (SELECT 1 FROM covered WHERE covered.value = signal.signal_id)"
    ).fetchone()
    if row is None or int(row[0]) != 0:
        errors.append("event-window coverage failure")


def _duckdb_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _duckdb_parquet(path: Path) -> str:
    escaped = str(path).replace("'", "''")
    return f"read_parquet('{escaped}')"


def _validate_foreign_keys(tables: dict[str, pa.Table], errors: list[str]) -> None:
    for name, table in tables.items():
        metadata = table.schema.metadata or {}
        raw = metadata.get(b"foreign_keys", b"{}").decode()
        for field, target in json.loads(raw).items():
            target_table, target_field = str(target).split(".", 1)
            target_name = f"{target_table}.parquet"
            if target_name not in tables:
                continue
            source_values = {value for value in table[field].to_pylist() if value not in (None, "")}
            target_values = {
                value
                for value in tables[target_name][target_field].to_pylist()
                if value not in (None, "")
            }
            if not source_values.issubset(target_values):
                errors.append(f"foreign key failure {name}.{field}")


def _validate_financial_reconciliation(
    tables: dict[str, pa.Table], errors: list[str]
) -> None:
    trades = tables.get("paper_trades.parquet")
    if trades is not None:
        for row in trades.to_pylist():
            required = ("quantity", "entry_price", "exit_price", "gross_pnl", "fees", "net_pnl")
            if any(row.get(key) is None for key in required):
                continue
            side = str(row.get("side", "")).upper()
            if side in {"BUY", "LONG"}:
                direction = 1.0
            elif side in {"SELL", "SHORT"}:
                direction = -1.0
            else:
                errors.append(f"unknown trade side {row['trade_id']}")
                continue
            gross = (float(row["exit_price"]) - float(row["entry_price"])) * float(
                row["quantity"]
            ) * direction
            if not math.isclose(gross, float(row["gross_pnl"]), abs_tol=1e-8):
                errors.append(f"PnL reconciliation failed {row['trade_id']}")
            if not math.isclose(
                gross - float(row["fees"]), float(row["net_pnl"]), abs_tol=1e-8
            ):
                errors.append(f"net PnL reconciliation failed {row['trade_id']}")
            explicit_costs = sum(
                _numeric(row.get(field))
                for field in (
                    "spread_cost",
                    "slippage_cost",
                    "simulated_latency_cost",
                    "holding_cost",
                )
            )
            if not math.isclose(
                explicit_costs, _numeric(row.get("fees")), abs_tol=1e-8
            ):
                errors.append(f"cost reconciliation failed {row['trade_id']}")
    metrics = tables.get("mfe_mae.parquet")
    if metrics is not None:
        for row in metrics.to_pylist():
            if (row.get("mfe") is not None and float(row["mfe"]) < 0) or (
                row.get("mae") is not None and float(row["mae"]) < 0
            ):
                errors.append(f"negative MFE/MAE {row['result_id']}")
            extra = _extra_fields(row)
            for field in ("time_to_mfe_seconds", "time_to_mae_seconds"):
                value = extra.get(field)
                if value is not None and _numeric(value) < 0:
                    errors.append(f"negative {field} {row['result_id']}")
    equity = tables.get("equity_curve.parquet")
    if equity is not None:
        for row in equity.to_pylist():
            if all(row.get(key) is not None for key in ("cash", "equity", "unrealized_pnl")):
                expected = float(row["cash"]) + float(row["unrealized_pnl"])
                if not math.isclose(expected, float(row["equity"]), abs_tol=1e-8):
                    errors.append(f"equity reconciliation failed {row['equity_id']}")


def _validate_temporal_and_position_reconciliation(
    tables: dict[str, pa.Table], errors: list[str]
) -> None:
    """Independently reconcile fill chronology, positions, and closed trades."""

    fills_table = tables.get("paper_fills.parquet")
    positions_table = tables.get("paper_positions.parquet")
    trades_table = tables.get("paper_trades.parquet")
    if fills_table is None or positions_table is None or trades_table is None:
        return
    fills = {
        str(row["fill_id"]): row
        for row in fills_table.to_pylist()
        if row.get("fill_id") not in (None, "")
    }
    position_events: dict[str, list[dict[str, object]]] = {}
    for row in positions_table.to_pylist():
        position_id = str(row.get("position_id") or "")
        if not position_id:
            errors.append(f"position event has no position_id {row.get('position_event_id')}")
            continue
        position_events.setdefault(position_id, []).append(row)

    trade_pnl_by_position: dict[str, float] = {}
    for row in trades_table.to_pylist():
        trade_id = str(row.get("trade_id") or "UNKNOWN")
        entry = fills.get(str(row.get("entry_fill_id") or ""))
        exit_fill = fills.get(str(row.get("exit_fill_id") or ""))
        direct_entry_ts = row.get("entry_ts")
        direct_exit_ts = row.get("exit_ts")
        if isinstance(direct_entry_ts, datetime) and isinstance(direct_exit_ts, datetime):
            if direct_exit_ts < direct_entry_ts:
                errors.append(f"exit precedes entry {trade_id}")
            holding = row.get("holding_duration_seconds")
            expected_holding = (direct_exit_ts - direct_entry_ts).total_seconds()
            if holding is not None and not math.isclose(
                _numeric(holding), expected_holding, abs_tol=1e-6
            ):
                errors.append(f"holding duration reconciliation failed {trade_id}")
        if entry is None or exit_fill is None:
            # The foreign-key validator reports the missing identifier precisely.
            continue
        entry_ts = entry.get("event_ts")
        exit_ts = exit_fill.get("event_ts")
        trade_ts = row.get("event_ts")
        if not isinstance(entry_ts, datetime) or not isinstance(exit_ts, datetime):
            errors.append(f"fill timestamp missing {trade_id}")
        elif exit_ts < entry_ts:
            errors.append(f"exit precedes entry {trade_id}")
        if isinstance(exit_ts, datetime) and isinstance(trade_ts, datetime) and trade_ts < exit_ts:
            errors.append(f"trade timestamp precedes exit fill {trade_id}")
        position_id = str(row.get("position_id") or "")
        if not position_id or position_id not in position_events:
            errors.append(f"trade has no position history {trade_id}")
            continue
        trade_pnl_by_position[position_id] = (
            trade_pnl_by_position.get(position_id, 0.0) + _numeric(row.get("net_pnl"))
        )

    for position_id, rows in position_events.items():
        ordered = sorted(
            rows,
            key=_position_event_sort_key,
        )
        closed_seen = False
        for row in ordered:
            status = str(row.get("status") or "").upper()
            quantity = row.get("quantity")
            if quantity is not None and _numeric(quantity) < 0:
                errors.append(f"negative position quantity {position_id}")
            if closed_seen and status not in {"CLOSED", ""}:
                errors.append(f"position reopened after close {position_id}")
            closed_seen = closed_seen or status == "CLOSED"
        if position_id in trade_pnl_by_position:
            last = ordered[-1]
            if str(last.get("status") or "").upper() != "CLOSED":
                errors.append(f"closed trade has non-closed position {position_id}")
            realized = last.get("realized_pnl")
            if realized is not None and not math.isclose(
                _numeric(realized), trade_pnl_by_position[position_id], abs_tol=1e-8
            ):
                errors.append(f"position PnL reconciliation failed {position_id}")


def _row_event_timestamp(row: dict[str, object]) -> datetime:
    value = row.get("event_ts")
    return value if isinstance(value, datetime) else datetime.min.replace(tzinfo=UTC)


def _position_event_sort_key(row: dict[str, object]) -> tuple[datetime, int, str]:
    event_kind = str(row.get("event_kind") or "").upper()
    status = str(row.get("status") or "").upper()
    transition_rank = 2 if status == "CLOSED" or event_kind == "CLOSED" else 0
    if event_kind == "MARK":
        transition_rank = 1
    return (
        _row_event_timestamp(row),
        transition_rank,
        str(row.get("position_event_id") or ""),
    )


def _validate_source_lineage(tables: dict[str, pa.Table], errors: list[str]) -> None:
    raw_ids: set[str] = set()
    for name in (
        "raw_orderbook_event_windows.parquet",
        "raw_trades_event_windows.parquet",
        "raw_last_price_event_windows.parquet",
        "raw_candles_event_windows.parquet",
    ):
        table = tables.get(name)
        if table is not None:
            for row in table.to_pylist():
                raw_id = row.get("raw_event_id")
                if raw_id not in (None, ""):
                    raw_ids.add(str(raw_id))
                source_id = row.get("source_event_id")
                if source_id not in (None, ""):
                    raw_ids.add(str(source_id))
                extra = row.get("extra_json")
                if isinstance(extra, str) and extra:
                    try:
                        extra_fields = json.loads(extra)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(extra_fields, dict):
                        source_id = extra_fields.get("source_event_id")
                        if source_id not in (None, ""):
                            raw_ids.add(str(source_id))
    signals = tables.get("candidate_signals.parquet")
    if signals is None or signals.num_rows == 0:
        return
    for dataset, primary_key, source_fields in (
        ("paper_fills.parquet", "fill_id", ("source_event_id",)),
        (
            "mfe_mae.parquet",
            "result_id",
            ("mfe_source_event_id", "mae_source_event_id"),
        ),
        ("shadow_stop_results.parquet", "result_id", ("source_event_id",)),
        ("shadow_exit_results.parquet", "result_id", ("source_event_id",)),
    ):
        table = tables.get(dataset)
        if table is None:
            continue
        for row in table.to_pylist():
            for field in source_fields:
                source_id = str(row.get(field) or "")
                if source_id and source_id not in raw_ids:
                    errors.append(
                        f"source lineage failure {dataset}:{row.get(primary_key)}:{field}"
                    )


def _validate_strategy_registry(root: Path, errors: list[str]) -> None:
    path = root / "STRATEGY_REGISTRY.json"
    if not path.is_file():
        return
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        errors.append("STRATEGY_REGISTRY.json is unreadable")
        return
    if not isinstance(payload, list):
        errors.append("strategy registry is not a list")
        return
    keys: set[tuple[str, str]] = set()
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            errors.append(f"strategy registry row is invalid {index}")
            continue
        strategy_id = str(item.get("strategy_id") or "").strip()
        version = str(item.get("strategy_version") or item.get("version") or "").strip()
        key = (strategy_id, version)
        if not all(key) or key in keys:
            errors.append(f"strategy registry key is invalid {index}")
        keys.add(key)
        for field in ("code_hash", "config_hash"):
            digest = str(item.get(field) or "").lower()
            if not _SHA256_PATTERN.fullmatch(digest):
                errors.append(f"strategy {field} is invalid {strategy_id}/{version}")
        activated = item.get("activated_at")
        try:
            parsed = datetime.fromisoformat(str(activated).replace("Z", "+00:00"))
        except ValueError:
            errors.append(f"strategy activation is invalid {strategy_id}/{version}")
        else:
            if parsed.tzinfo is None:
                errors.append(f"strategy activation is not UTC-aware {strategy_id}/{version}")


def _validate_event_window_coverage(
    tables: dict[str, pa.Table], errors: list[str]
) -> None:
    signals = tables.get("candidate_signals.parquet")
    if signals is None or signals.num_rows == 0:
        return
    required = {
        value
        for value in signals["signal_id"].to_pylist()
        if value not in (None, "")
    }
    covered: set[str] = set()
    for name in (
        "raw_orderbook_event_windows.parquet",
        "raw_trades_event_windows.parquet",
        "raw_last_price_event_windows.parquet",
        "raw_candles_event_windows.parquet",
    ):
        table = tables.get(name)
        if table is not None:
            covered.update(
                value for value in table["signal_id"].to_pylist() if value not in (None, "")
            )
    if not required.issubset(covered):
        errors.append("event-window coverage failure")


def _contains_secret(root: Path) -> bool:
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.casefold() == ".parquet":
            if _parquet_contains_secret(path):
                return True
            continue
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                if _TOKEN_PATTERN.search(chunk) or _SECRET_ASSIGNMENT.search(chunk):
                    return True
    return False


def _parquet_contains_secret(path: Path) -> bool:
    """Scan logical Parquet strings, never compressed binary container bytes."""

    try:
        parquet = pq.ParquetFile(path)
        columns = [
            field.name
            for field in parquet.schema_arrow
            if pa.types.is_string(field.type) or pa.types.is_large_string(field.type)
        ]
        if not columns:
            return False
        for batch in parquet.iter_batches(batch_size=2048, columns=columns):
            for column in batch.columns:
                for value in column.to_pylist():
                    if value is None:
                        continue
                    payload = str(value).encode("utf-8", errors="replace")
                    if _TOKEN_PATTERN.search(payload) or _SECRET_ASSIGNMENT.search(payload):
                        return True
    except (OSError, pa.ArrowException):
        return True
    return False


def _write_sums(root: Path) -> None:
    lines = []
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        if path.name == "SHA256SUMS":
            continue
        lines.append(f"{_sha256_file(path)}  {path.relative_to(root).as_posix()}")
    (root / "SHA256SUMS").write_text("\n".join(lines) + "\n", encoding="ascii")


def _verify_sums(root: Path, errors: list[str]) -> None:
    sums = root / "SHA256SUMS"
    if not sums.is_file():
        errors.append("missing SHA256SUMS")
        return
    expected_paths = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file() and path != sums
    }
    declared_paths: set[str] = set()
    try:
        lines = sums.read_text(encoding="ascii").splitlines()
    except (OSError, UnicodeError):
        errors.append("SHA256SUMS is unreadable")
        return
    for line in lines:
        if "  " not in line:
            errors.append("invalid SHA256SUMS line")
            continue
        expected, relative = line.split("  ", 1)
        expected = expected.strip().lower()
        relative = relative.strip()
        if not _SHA256_PATTERN.fullmatch(expected):
            errors.append(f"invalid SHA256 digest {relative}")
            continue
        if relative in declared_paths:
            errors.append(f"duplicate SHA256SUMS path {relative}")
            continue
        declared_paths.add(relative)
        path = (root / relative).resolve()
        try:
            path.relative_to(root.resolve())
        except ValueError:
            errors.append("unsafe SHA256SUMS path")
            continue
        if not path.is_file() or _sha256_file(path) != expected:
            errors.append(f"file hash mismatch {relative}")
    for relative in sorted(expected_paths - declared_paths):
        errors.append(f"missing SHA256SUMS entry {relative}")
    for relative in sorted(declared_paths - expected_paths):
        errors.append(f"unexpected SHA256SUMS entry {relative}")


def _write_tar_zst(source: Path, destination: Path) -> None:
    compressor = zstandard.ZstdCompressor(level=10, write_checksum=True)
    with (
        destination.open("wb") as raw,
        compressor.stream_writer(raw, closefd=False) as compressed,
        tarfile.open(fileobj=compressed, mode="w|") as archive,
    ):
        for path in sorted(source.rglob("*")):
            archive.add(
                path,
                arcname=path.relative_to(source).as_posix(),
                recursive=False,
            )
    _fsync_file(destination)


def _fsync_file(path: Path) -> None:
    with path.open("r+b") as stream:
        stream.flush()
        os.fsync(stream.fileno())


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _extract_tar_zst(archive: Path, destination: Path) -> None:
    decompressor = zstandard.ZstdDecompressor()
    seen: set[str] = set()
    with (
        archive.open("rb") as raw,
        decompressor.stream_reader(raw) as decompressed,
        tarfile.open(fileobj=decompressed, mode="r|") as tar,
    ):
        for member in tar:
            if member.name in seen:
                raise ArchiveError("duplicate tar member")
            seen.add(member.name)
            target = (destination / member.name).resolve()
            try:
                target.relative_to(destination.resolve())
            except ValueError as exc:
                raise ArchiveError("unsafe tar member") from exc
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            elif member.isfile():
                target.parent.mkdir(parents=True, exist_ok=True)
                source = tar.extractfile(member)
                if source is None:
                    raise ArchiveError("tar member is unreadable")
                with target.open("wb") as output:
                    shutil.copyfileobj(source, output)
            else:
                raise ArchiveError("unsupported tar member type")


def _archive_filename(request: ArchiveBuildRequest) -> str:
    marker = "_TEST" if request.test_archive else ""
    return (
        f"neobitcoin_paper_{request.session_date.isoformat()}_"
        f"{_stamp(request.start_utc)}_{_stamp(request.end_utc)}_"
        f"schema-v{ARCHIVE_FORMAT_VERSION}{marker}.tar.zst"
    )


def _archive_id(request: ArchiveBuildRequest) -> str:
    prefix = "TEST" if request.test_archive else "DAILY"
    raw = (
        f"{request.session_date}:{_stamp(request.start_utc)}:{_stamp(request.end_utc)}:"
        f"schema-v{ARCHIVE_FORMAT_VERSION}"
    )
    return f"{prefix}-{request.session_date}-{hashlib.sha256(raw.encode()).hexdigest()[:12]}"


def _stamp(value: datetime) -> str:
    return _utc(value).strftime("%Y%m%dT%H%M%SZ")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ArchiveError("archive timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _schema_sha256(schema: pa.Schema) -> str:
    return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()


def _write_json(path: Path, value: object) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=_json_default)
        + "\n",
        encoding="utf-8",
    )


def _json_default(value: object) -> object:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, Path):
        return str(value)
    return str(value)


def _readme(request: ArchiveBuildRequest, archive_id: str) -> str:
    marker = "TEST — excluded from real OOS statistics.\n\n" if request.test_archive else ""
    return (
        "# Neobitcoin paper archive\n\n"
        f"{marker}Archive ID: `{archive_id}`. Session date: "
        f"`{request.session_date.isoformat()}`. All timestamps and Parquet fields are typed; "
        "market decisions use point-in-time data and PAPER_ONLY execution.\n"
    )


def _manifest_markdown(manifest: dict[str, object], row_counts: dict[str, int]) -> str:
    lines = [
        "# Manifest",
        "",
        f"- Archive ID: `{manifest['archive_id']}`",
        f"- Type: `{manifest['archive_type']}`",
        f"- Session: `{manifest['session_date']}`",
        "- PAPER_ONLY: `true`",
        "",
        "| Dataset | Rows |",
        "|---|---:|",
    ]
    lines.extend(f"| {name} | {count} |" for name, count in row_counts.items())
    return "\n".join(lines) + "\n"


def _schema_dictionary() -> str:
    lines = ["# Schema dictionary", ""]
    for name, schema in DATASET_SCHEMAS.items():
        lines.extend((f"## {name}", "", "| Field | Arrow type | Nullable |", "|---|---|---|"))
        lines.extend(
            f"| {field.name} | `{field.type}` | {field.nullable} |" for field in schema
        )
        metadata = schema.metadata or {}
        lines.append("")
        lines.append(f"Primary key: `{metadata[b'primary_key'].decode()}`.")
        lines.append("")
    return "\n".join(lines)


def _daily_summary(
    data_dir: Path,
    registry: tuple[dict[str, object], ...],
    row_counts: dict[str, int],
) -> str:
    trades = pq.read_table(data_dir / "paper_trades.parquet").to_pylist()
    signals = pq.read_table(data_dir / "candidate_signals.parquet").to_pylist()
    evaluations = pq.read_table(data_dir / "strategy_evaluations.parquet").to_pylist()
    orders = pq.read_table(data_dir / "paper_orders.parquet").to_pylist()
    fills = pq.read_table(data_dir / "paper_fills.parquet").to_pylist()
    positions = pq.read_table(data_dir / "paper_positions.parquet").to_pylist()
    equity = pq.read_table(data_dir / "equity_curve.parquet").to_pylist()
    excursions = pq.read_table(data_dir / "mfe_mae.parquet").to_pylist()
    quality = pq.read_table(data_dir / "data_quality_events.parquet").to_pylist()
    health = pq.read_table(data_dir / "health_events.parquet").to_pylist()
    lines = ["# Daily summary", ""]
    for strategy in registry:
        strategy_id = str(strategy.get("strategy_id", "UNKNOWN"))
        version = str(
            strategy.get("strategy_version") or strategy.get("version") or "UNKNOWN"
        )
        selected_evaluations = _strategy_rows(evaluations, strategy_id, version)
        selected_orders = _strategy_rows(orders, strategy_id, version)
        selected_positions = _strategy_rows(positions, strategy_id, version)
        selected_equity = _strategy_rows(equity, strategy_id, version)
        selected_excursions = _strategy_rows(excursions, strategy_id, version)
        selected_trades = [
            row
            for row in trades
            if row.get("strategy_id") == strategy_id
            and row.get("strategy_version") == version
        ]
        selected_signals = [
            row
            for row in signals
            if row.get("strategy_id") == strategy_id
            and row.get("strategy_version") == version
        ]
        pnl_values = [_numeric(row.get("net_pnl")) for row in selected_trades]
        pnl = sum(pnl_values)
        wins = [value for value in pnl_values if value > 0]
        losses = [value for value in pnl_values if value < 0]
        win_rate = len(wins) / len(selected_trades) if selected_trades else 0.0
        rejected = sum(1 for row in selected_signals if not row.get("accepted"))
        account_ids = {
            str(row.get("account_id"))
            for row in (*selected_signals, *selected_orders, *selected_equity)
            if row.get("account_id") not in (None, "")
        }
        fill_count = sum(1 for row in fills if str(row.get("account_id")) in account_ids)
        order_statuses = Counter(str(row.get("status") or "UNKNOWN") for row in selected_orders)
        position_last: dict[str, dict[str, object]] = {}
        for row in selected_positions:
            position_last[str(row.get("position_id") or row.get("position_event_id"))] = row
        open_positions = sum(
            1
            for row in position_last.values()
            if str(row.get("status") or "").upper() != "CLOSED"
        )
        entries = sum(
            1
            for row in selected_positions
            if str(row.get("event_kind") or _extra_fields(row).get("event_kind") or "").upper()
            == "OPEN"
        )
        sides = Counter(str(row.get("side") or "UNKNOWN") for row in selected_trades)
        if not selected_trades:
            sides.update(
                str(row.get("side") or "UNKNOWN")
                for row in selected_positions
                if str(
                    row.get("event_kind") or _extra_fields(row).get("event_kind") or ""
                ).upper()
                == "OPEN"
            )
        gross = sum(_numeric(row.get("gross_pnl")) for row in selected_trades)
        expectancy = pnl / len(selected_trades) if selected_trades else 0.0
        gross_wins = sum(wins)
        gross_losses = abs(sum(losses))
        profit_factor = gross_wins / gross_losses if gross_losses else 0.0
        maximum_drawdown = max(
            (_numeric(row.get("drawdown")) for row in selected_equity), default=0.0
        )
        average_mfe = _mean(_numeric(row.get("mfe")) for row in selected_excursions)
        average_mae = _mean(_numeric(row.get("mae")) for row in selected_excursions)
        by_session = Counter(
            str(row.get("session_label") or _extra_fields(row).get("session_label") or "UNKNOWN")
            for row in selected_orders
        )
        by_execution = Counter(
            str(row.get("order_type") or "UNKNOWN") for row in selected_orders
        )
        by_hour: Counter[str] = Counter()
        for row in selected_trades:
            timestamp = row.get("event_ts")
            if isinstance(timestamp, datetime):
                by_hour[timestamp.astimezone(_MOSCOW).strftime("%H:00")] += 1
        lines.extend(
            (
                f"## {strategy_id}_{version}",
                "",
                f"- Status: `{strategy.get('status', 'UNKNOWN')}`",
                f"- Activation: `{strategy.get('activated_at', 'UNKNOWN')}`",
                f"- Evaluations: {len(selected_evaluations)}",
                f"- Signals: {len(selected_signals)}",
                "- Independent episodes: "
                f"{len({row.get('signal_id') for row in selected_signals})}",
                f"- Rejected signals: {rejected}",
                f"- Orders: {len(selected_orders)}",
                "- FULL/PARTIAL/NO_FILL: "
                f"{order_statuses['FULL_FILL']}/{order_statuses['PARTIAL_FILL']}/"
                f"{order_statuses['NO_FILL']}",
                f"- Fills: {fill_count}",
                f"- Entries / exits / open positions: "
                f"{entries}/{len(selected_trades)}/{open_positions}",
                f"- LONG / SHORT: {sides['LONG']}/{sides['SHORT']}",
                f"- Trades: {len(selected_trades)}",
                f"- Gross PnL: {gross:.8f}",
                f"- Net PnL: {pnl:.8f}",
                f"- Win rate: {win_rate:.6f}",
                f"- Average win: {(sum(wins) / len(wins) if wins else 0):.8f}",
                f"- Average loss: {(sum(losses) / len(losses) if losses else 0):.8f}",
                f"- Expectancy: {expectancy:.8f}",
                f"- Profit factor: {profit_factor:.8f}",
                f"- Maximum drawdown: {maximum_drawdown:.8f}",
                f"- Mean MFE / MAE: {average_mfe:.8f}/{average_mae:.8f}",
                "- Result without best 1/3/5: "
                + "/".join(f"{_without_best(selected_trades, count):.8f}" for count in (1, 3, 5)),
                f"- Orders by session: {_counter_summary(by_session)}",
                f"- Trades by Moscow hour: {_counter_summary(by_hour)}",
                f"- Orders by execution model: {_counter_summary(by_execution)}",
                "",
            )
        )
    quality_kinds = Counter(str(row.get("kind") or "UNKNOWN") for row in quality)
    health_statuses = Counter(str(row.get("status") or "UNKNOWN") for row in health)
    reconnect_generations = {
        int(_numeric(value))
        for row in quality
        if (
            value := row.get("reconnect_generation")
            or _extra_fields(row).get("reconnect_generation")
        )
        is not None
    }
    gaps = sum(
        1
        for row in quality
        if str(row.get("gap_status") or _extra_fields(row).get("gap_status") or "OK")
        != "OK"
    )
    lines.extend(
        (
            "## Runtime continuity",
            "",
            f"- Stream reconnects: {max(0, len(reconnect_generations) - 1)}",
            f"- Gaps: {gaps + quality_kinds['ORDERBOOK_GAP']}",
            f"- Stale intervals: {quality_kinds['STALE_DATA']}",
            f"- Health events: {_counter_summary(health_statuses)}",
            "- Downtime: derived from timestamped health/data-quality intervals in Parquet",
            "",
            "## Archive quality",
            "",
            f"- Dataset count: {len(row_counts)}",
            f"- Total rows: {sum(row_counts.values())}",
            "- Validation: independent reconciliation is completed before publication",
            "",
        )
    )
    return "\n".join(lines)


def _strategy_rows(
    rows: list[dict[str, object]], strategy_id: str, version: str
) -> list[dict[str, object]]:
    return [
        row
        for row in rows
        if row.get("strategy_id") == strategy_id
        and row.get("strategy_version") == version
    ]


def _extra_fields(row: dict[str, object]) -> dict[str, object]:
    raw = row.get("extra_json")
    if not isinstance(raw, str) or not raw:
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _mean(values: Iterable[float]) -> float:
    materialized = list(values)
    return sum(materialized) / len(materialized) if materialized else 0.0


def _counter_summary(values: Counter[str]) -> str:
    return ", ".join(f"{key}={count}" for key, count in sorted(values.items())) or "none"


def _without_best(rows: list[dict[str, object]], count: int) -> float:
    values = sorted((_numeric(row.get("net_pnl")) for row in rows), reverse=True)
    return sum(values[min(count, len(values)) :])


def _numeric(value: object) -> float:
    return float(str(value or 0))


def _validation_report(result: ArchiveValidationResult) -> str:
    lines = [
        "# Validation report",
        "",
        f"Overall: **{'PASS' if result.passed else 'FAIL'}**",
        "",
        f"- PyArrow: {'PASS' if result.pyarrow_verified else 'FAIL'}",
        f"- DuckDB: {'PASS' if result.duckdb_verified else 'FAIL'}",
        f"- Independent reconciliation: "
        f"{'PASS' if result.independent_reconciliation_verified else 'FAIL'}",
        f"- Unexplained discrepancies: {result.unexplained_discrepancies}",
        "- Typed schemas / PK / FK / timestamps: covered by overall result",
        "- PnL / positions / equity / MFE / MAE: independently reconciled",
        "- Event-window coverage / source lineage: covered by overall result",
        "- Secret scan: covered by overall result",
        "",
    ]
    if result.errors:
        lines.extend(("## Errors", "", *(f"- {item}" for item in result.errors)))
    return "\n".join(lines) + "\n"


__all__ = [
    "ArchiveBuildRequest",
    "ArchiveError",
    "ArchiveValidationResult",
    "ArchiveValidator",
    "BuiltArchive",
    "DailyArchiveBuilder",
    "REQUIRED_DOCUMENTS",
]
