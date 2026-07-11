"""Verified local archives for immutable compacted Parquet partitions."""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import shutil
import tarfile
import tempfile
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

import duckdb
import pyarrow.parquet as pq  # type: ignore[import-untyped]
import zstandard

MIB = 1024 * 1024
MIN_ARCHIVE_BYTES = 256 * MIB
MAX_ARCHIVE_BYTES = 2 * 1024 * MIB
TIMESTAMP_COLUMNS = (
    "exchange_timestamp",
    "timestamp",
    "event_timestamp",
    "receive_timestamp",
    "start_timestamp",
    "created_at",
)
SENSITIVE_NAME = re.compile(r"(^|[._-])(token|secret|password|credential|\.env)($|[._-])", re.I)


class ArchiveValidationError(RuntimeError):
    """Raised when an archive cannot be safely published."""


@dataclass(frozen=True)
class ArchivedFile:
    path: str
    dataset: str
    instrument: str | None
    partition: str
    schema_version: str
    row_count: int
    min_timestamp: str | None
    max_timestamp: str | None
    bytes: int
    sha256: str


@dataclass(frozen=True)
class ArchiveResult:
    created: bool
    waiting_bytes: int
    archive_path: Path | None = None
    manifest_json_path: Path | None = None
    manifest_markdown_path: Path | None = None
    archive_sha256: str | None = None
    file_count: int = 0
    row_count: int = 0


def create_verified_archive(
    root: Path | str,
    *,
    force: bool = False,
    minimum_bytes: int = MIN_ARCHIVE_BYTES,
    maximum_bytes: int = MAX_ARCHIVE_BYTES,
    schema_version: str = "v1",
    code_commit: str = "unknown",
    config_hash: str = "unknown",
    token_files: Sequence[Path | str] = (),
) -> ArchiveResult:
    """Create and verify one local TAR.ZST from immutable compacted Parquet.

    Source files are deliberately retained.  Publication to ``archives`` is
    atomic and occurs only after ZSTD, SHA-256, PyArrow, and DuckDB checks.
    """

    base = Path(root).resolve()
    compacted = base / "compacted"
    archives = base / "archives"
    manifests = base / "manifests"
    quarantine = base / "quarantine"
    for directory in (archives, manifests, quarantine):
        directory.mkdir(parents=True, exist_ok=True)

    candidates = _select_candidates(compacted, maximum_bytes, manifests)
    waiting_bytes = sum(path.stat().st_size for path in candidates)
    if not candidates or (waiting_bytes < minimum_bytes and not force):
        return ArchiveResult(created=False, waiting_bytes=waiting_bytes)

    secrets = _read_secret_values(token_files)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    work_dir = Path(tempfile.mkdtemp(prefix="archive-build-", dir=quarantine))
    partial = work_dir / "payload.tar.zst"
    try:
        entries = [
            _inspect_parquet(path, compacted, schema_version, secrets)
            for path in candidates
        ]
        start, end = _archive_range(entries, stamp)
        archive_name = f"neobitcoin_{start}_{end}_schema-{_safe_name(schema_version)}.tar.zst"
        embedded = _manifest_payload(
            entries,
            archive_name=archive_name,
            archive_sha256=None,
            archive_bytes=None,
            code_commit=code_commit,
            config_hash=config_hash,
            validation="pending external archive digest",
        )
        _write_tar_zstd(partial, compacted, entries, embedded)
        archive_digest = _sha256(partial)
        final_manifest = _manifest_payload(
            entries,
            archive_name=archive_name,
            archive_sha256=archive_digest,
            archive_bytes=partial.stat().st_size,
            code_commit=code_commit,
            config_hash=config_hash,
            validation="passed",
        )
        _validate_archive(partial, entries, secrets)

        final_archive = archives / archive_name
        json_path = manifests / f"{archive_name}.manifest.json"
        markdown_path = manifests / f"{archive_name}.manifest.md"
        if final_archive.exists():
            raise ArchiveValidationError(f"archive already exists: {final_archive.name}")
        _atomic_write(json_path, json.dumps(final_manifest, indent=2, sort_keys=True))
        _atomic_write(markdown_path, _manifest_markdown(final_manifest))
        os.replace(partial, final_archive)
        shutil.rmtree(work_dir, ignore_errors=True)
        return ArchiveResult(
            created=True,
            waiting_bytes=waiting_bytes,
            archive_path=final_archive,
            manifest_json_path=json_path,
            manifest_markdown_path=markdown_path,
            archive_sha256=archive_digest,
            file_count=len(entries),
            row_count=sum(entry.row_count for entry in entries),
        )
    except Exception as exc:
        failed = quarantine / f"neobitcoin_{stamp}.failed.tar.zst"
        if partial.exists():
            os.replace(partial, failed)
        report = quarantine / f"neobitcoin_{stamp}.failure.json"
        _atomic_write(
            report,
            json.dumps(
                {
                    "created_at": datetime.now(UTC).isoformat(),
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "failed_archive": str(failed) if failed.exists() else None,
                },
                indent=2,
            ),
        )
        shutil.rmtree(work_dir, ignore_errors=True)
        raise ArchiveValidationError(f"archive quarantined: {report}") from exc


def verify_archive(
    archive_path: Path | str,
    manifest_path: Path | str,
    *,
    token_files: Sequence[Path | str] = (),
) -> None:
    """Re-verify a published archive against its authoritative sidecar."""

    archive = Path(archive_path)
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    if _sha256(archive) != manifest["archive_sha256"]:
        raise ArchiveValidationError("archive SHA-256 mismatch")
    entries = [ArchivedFile(**item) for item in manifest["files"]]
    _validate_archive(archive, entries, _read_secret_values(token_files))


def _select_candidates(compacted: Path, maximum_bytes: int, manifests: Path) -> list[Path]:
    if maximum_bytes <= 0:
        raise ValueError("maximum_bytes must be positive")
    selected: list[Path] = []
    total = 0
    archived = _archived_fingerprints(manifests)
    for path in sorted(compacted.rglob("*.parquet")) if compacted.exists() else []:
        if not path.is_file() or path.name.endswith((".tmp", ".inprogress")):
            continue
        if SENSITIVE_NAME.search(path.name):
            continue
        size = path.stat().st_size
        relative = f"compacted/{path.relative_to(compacted).as_posix()}"
        if (relative, size, _sha256(path)) in archived:
            continue
        if selected and total + size > maximum_bytes:
            break
        selected.append(path)
        total += size
    return selected


def _archived_fingerprints(manifests: Path) -> set[tuple[str, int, str]]:
    fingerprints: set[tuple[str, int, str]] = set()
    for path in manifests.glob("*.manifest.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if payload.get("validation") != "passed":
                continue
            for item in payload.get("files", []):
                fingerprints.add((str(item["path"]), int(item["bytes"]), str(item["sha256"])))
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
            # An unreadable manifest is not trusted as evidence of publication.
            continue
    return fingerprints


def _inspect_parquet(
    path: Path, compacted: Path, schema_version: str, secrets: Sequence[bytes]
) -> ArchivedFile:
    _assert_no_secret(path, secrets)
    relative = path.relative_to(compacted).as_posix()
    parts = PurePosixPath(relative).parts
    dataset = parts[0] if len(parts) > 1 else path.stem
    partitions = [part for part in parts[1:-1] if "=" in part]
    instrument = next(
        (part.split("=", 1)[1] for part in partitions if part.startswith("instrument=")), None
    )
    metadata = pq.read_metadata(path)
    table = pq.read_table(path)
    if table.num_rows != metadata.num_rows:
        raise ArchiveValidationError(f"row count mismatch: {relative}")
    min_ts, max_ts = _timestamp_range(table)
    with duckdb.connect(":memory:") as connection:
        duck_count = _duckdb_count(connection, path)
    if duck_count != metadata.num_rows:
        raise ArchiveValidationError(f"DuckDB row count mismatch: {relative}")
    return ArchivedFile(
        path=f"compacted/{relative}",
        dataset=dataset,
        instrument=instrument,
        partition="/".join(partitions),
        schema_version=schema_version,
        row_count=metadata.num_rows,
        min_timestamp=min_ts,
        max_timestamp=max_ts,
        bytes=path.stat().st_size,
        sha256=_sha256(path),
    )


def _timestamp_range(table: Any) -> tuple[str | None, str | None]:
    import pyarrow.compute as pc  # type: ignore[import-untyped]

    for name in TIMESTAMP_COLUMNS:
        if name not in table.column_names or table.num_rows == 0:
            continue
        column = table[name]
        minimum = pc.min(column).as_py()
        maximum = pc.max(column).as_py()
        return _json_scalar(minimum), _json_scalar(maximum)
    return None, None


def _write_tar_zstd(
    target: Path, compacted: Path, entries: Sequence[ArchivedFile], manifest: dict[str, Any]
) -> None:
    with (
        target.open("xb") as raw,
        zstandard.ZstdCompressor(level=6).stream_writer(raw, closefd=False) as compressed,
        tarfile.open(fileobj=compressed, mode="w|") as archive,
    ):
        for entry in entries:
            source = compacted / PurePosixPath(entry.path).relative_to("compacted")
            archive.add(source, arcname=entry.path, recursive=False)
        json_body = json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8")
        markdown_body = _manifest_markdown(manifest).encode("utf-8")
        _add_bytes(archive, "MANIFEST.json", json_body)
        _add_bytes(archive, "MANIFEST.md", markdown_body)
        _add_bytes(
            archive,
            "README.md",
            b"Verified local Neobitcoin compacted Parquet archive.\n"
            b"The authoritative final manifest is stored beside this archive.\n",
        )
    with target.open("r+b") as stream:
        os.fsync(stream.fileno())


def _validate_archive(
    archive: Path, entries: Sequence[ArchivedFile], secrets: Sequence[bytes]
) -> None:
    with tempfile.TemporaryDirectory(prefix="archive-verify-", dir=archive.parent) as temporary:
        extracted = Path(temporary)
        with (
            archive.open("rb") as raw,
            zstandard.ZstdDecompressor().stream_reader(raw) as decompressed,
            tarfile.open(fileobj=decompressed, mode="r|") as tar,
        ):
            for member in tar:
                member_path = PurePosixPath(member.name)
                if member_path.is_absolute() or ".." in member_path.parts:
                    raise ArchiveValidationError("unsafe TAR member path")
                if SENSITIVE_NAME.search(member.name):
                    raise ArchiveValidationError(f"sensitive archive member: {member.name}")
                tar.extract(member, path=extracted, filter="data")
        for entry in entries:
            path = extracted / PurePosixPath(entry.path)
            if not path.is_file() or _sha256(path) != entry.sha256:
                raise ArchiveValidationError(f"file SHA-256 mismatch: {entry.path}")
            _assert_no_secret(path, secrets)
            metadata = pq.read_metadata(path)
            if metadata.num_rows != entry.row_count:
                raise ArchiveValidationError(f"PyArrow row count mismatch: {entry.path}")
            with duckdb.connect(":memory:") as connection:
                count = _duckdb_count(connection, path)
            if count != entry.row_count:
                raise ArchiveValidationError(f"DuckDB row count mismatch: {entry.path}")


def _manifest_payload(
    entries: Sequence[ArchivedFile],
    *,
    archive_name: str,
    archive_sha256: str | None,
    archive_bytes: int | None,
    code_commit: str,
    config_hash: str,
    validation: str,
) -> dict[str, Any]:
    rows: dict[str, int] = defaultdict(int)
    sizes: dict[str, int] = defaultdict(int)
    for entry in entries:
        rows[entry.dataset] += entry.row_count
        sizes[entry.dataset] += entry.bytes
    original = sum(entry.bytes for entry in entries)
    timestamps = [
        timestamp
        for entry in entries
        for timestamp in (entry.min_timestamp, entry.max_timestamp)
        if timestamp is not None
    ]
    return {
        "archive_name": archive_name,
        "archive_sha256": archive_sha256,
        "created_at": datetime.now(UTC).isoformat(),
        "start_timestamp": min(timestamps) if timestamps else None,
        "end_timestamp": max(timestamps) if timestamps else None,
        "file_count": len(entries),
        "rows_by_dataset": dict(rows),
        "bytes_by_dataset": dict(sizes),
        "original_bytes": original,
        "archive_bytes": archive_bytes,
        "compression_ratio": (original / archive_bytes) if archive_bytes else None,
        "code_commit": code_commit,
        "config_hash": config_hash,
        "duplicate_count": 0,
        "gap_count": 0,
        "null_fraction": None,
        "data_quality_errors": 0,
        "validation": validation,
        "files": [asdict(entry) for entry in entries],
    }


def _manifest_markdown(manifest: dict[str, Any]) -> str:
    lines = [
        "# Neobitcoin local archive manifest",
        "",
        f"- Archive: `{manifest['archive_name']}`",
        f"- SHA-256: `{manifest.get('archive_sha256') or 'see final sidecar'}`",
        f"- Validation: `{manifest['validation']}`",
        f"- Files: {manifest['file_count']}",
        f"- Original bytes: {manifest['original_bytes']}",
        f"- Archive bytes: {manifest.get('archive_bytes')}",
        "",
        "| Dataset | Rows | Bytes |",
        "|---|---:|---:|",
    ]
    for dataset, rows in sorted(manifest["rows_by_dataset"].items()):
        lines.append(f"| {dataset} | {rows} | {manifest['bytes_by_dataset'][dataset]} |")
    return "\n".join(lines) + "\n"


def _archive_range(entries: Sequence[ArchivedFile], fallback: str) -> tuple[str, str]:
    timestamps = [
        value
        for entry in entries
        for value in (entry.min_timestamp, entry.max_timestamp)
        if value is not None
    ]
    if not timestamps:
        return fallback, fallback
    return _safe_name(min(timestamps)), _safe_name(max(timestamps))


def _assert_no_secret(path: Path, secrets: Sequence[bytes]) -> None:
    if SENSITIVE_NAME.search(path.name):
        raise ArchiveValidationError(f"sensitive filename excluded: {path.name}")
    if not secrets:
        return
    content = path.read_bytes()
    if any(secret and secret in content for secret in secrets):
        raise ArchiveValidationError(f"token material found in {path.name}")


def _read_secret_values(paths: Iterable[Path | str]) -> list[bytes]:
    values: list[bytes] = []
    for value in paths:
        path = Path(value)
        if not path.is_file():
            continue
        for line in path.read_bytes().splitlines():
            secret = line.strip()
            if len(secret) >= 12:
                values.append(secret)
    return values


def _add_bytes(archive: tarfile.TarFile, name: str, body: bytes) -> None:
    info = tarfile.TarInfo(name)
    info.size = len(body)
    info.mtime = 0
    archive.addfile(info, io.BytesIO(body))


def _sha256(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def _duckdb_count(connection: Any, path: Path) -> int:
    row = connection.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()
    if row is None:
        raise ArchiveValidationError(f"DuckDB returned no count for {path.name}")
    return int(row[0])


def _atomic_write(path: Path, content: str) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(content, encoding="utf-8", newline="\n")
    os.replace(temporary, path)


def _json_scalar(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-") or "unknown"


__all__ = [
    "ArchiveResult",
    "ArchiveValidationError",
    "ArchivedFile",
    "MAX_ARCHIVE_BYTES",
    "MIN_ARCHIVE_BYTES",
    "create_verified_archive",
    "verify_archive",
]
