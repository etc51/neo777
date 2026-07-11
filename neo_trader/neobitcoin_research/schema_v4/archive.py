"""Fail-closed publisher for schema-v4.1-golden review archives."""

from __future__ import annotations

import hashlib
import json
import os
import tarfile
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

import duckdb
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
import zstandard

DATASETS: Final = (
    "raw_orderbook",
    "raw_trades",
    "raw_last_price",
    "candles_1m",
    "candles_5m",
    "candles_15m",
    "feature_snapshots",
    "candidate_events",
    "filter_decisions",
    "execution_simulations",
    "future_outcomes",
    "shadow_stop_results",
    "shadow_exit_results",
    "market_status_events",
    "data_quality_events",
)
DOCUMENTS: Final = (
    "README.md",
    "MANIFEST.json",
    "MANIFEST.md",
    "SCHEMA_DICTIONARY.md",
    "VALIDATION_REPORT.md",
)


class GoldenArchiveError(RuntimeError):
    """A fail-closed publication gate rejected the archive."""


@dataclass(frozen=True, slots=True)
class GoldenArchiveResult:
    archive_path: Path
    sha256_path: Path
    sha256: str
    size_bytes: int
    file_count: int
    rows_by_dataset: dict[str, int]


def publish_golden_archive(
    tables: Mapping[str, pa.Table],
    output_dir: Path | str,
    *,
    candidate_start: datetime,
    candidate_end: datetime,
    support_start: datetime,
    required_support_end: datetime,
    validation: Mapping[str, Any],
    token_values: Sequence[bytes] = (),
) -> GoldenArchiveResult:
    """Publish exactly twenty verified files, or publish nothing."""

    missing = set(DATASETS) - tables.keys()
    extra = tables.keys() - set(DATASETS)
    if missing or extra:
        raise GoldenArchiveError(
            f"dataset mismatch: missing={sorted(missing)}, extra={sorted(extra)}"
        )
    if validation.get("status") != "PASS":
        raise GoldenArchiveError("full independent validation did not pass")
    gates = validation.get("gates", {})
    failed = sorted(name for name, value in gates.items() if value != "PASS")
    if failed:
        raise GoldenArchiveError(f"publication gates failed: {failed}")

    destination = Path(output_dir).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    t0, t1 = _utc(candidate_start), _utc(candidate_end)
    archive_name = (
        f"neobitcoin_review_10m_{_stamp(t0)}_{_stamp(t1)}_schema-v4.1-golden.tar.zst"
    )
    final_path = destination / archive_name
    if final_path.exists():
        raise GoldenArchiveError(f"archive already exists: {final_path}")
    work = Path(tempfile.mkdtemp(prefix="schema-v4.1-golden-", dir=destination))
    partial = destination / f".{archive_name}.inprogress"
    try:
        data = work / "data"
        data.mkdir()
        rows: dict[str, int] = {}
        dataset_manifest: list[dict[str, Any]] = []
        for name in DATASETS:
            table = tables[name]
            rows[name] = table.num_rows
            path = data / f"{name}.parquet"
            pq.write_table(
                table,
                path,
                compression="zstd",
                use_dictionary=True,
                write_statistics=True,
                row_group_size=64_000,
            )
            dataset_manifest.append(_dataset_manifest(name, table, path))

        manifest = {
            "schema_version": "schema-v4.1-golden",
            "candidate_window_start": t0.isoformat(),
            "candidate_window_end": t1.isoformat(),
            "support_start": _utc(support_start).isoformat(),
            "required_support_end": _utc(required_support_end).isoformat(),
            "rows_by_dataset": rows,
            "datasets": dataset_manifest,
            "validation": dict(validation),
            "file_count": 20,
            "dataset_file_count": 15,
            "compression": "zstd",
            "timestamp_type": "timestamp[us, UTC]",
        }
        (work / "MANIFEST.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8"
        )
        (work / "README.md").write_text(_readme(t0, t1), encoding="utf-8")
        (work / "MANIFEST.md").write_text(_manifest_md(manifest), encoding="utf-8")
        (work / "SCHEMA_DICTIONARY.md").write_text(_schema_dictionary(tables), encoding="utf-8")
        (work / "VALIDATION_REPORT.md").write_text(_validation_report(validation), encoding="utf-8")
        members = sorted(path for path in work.rglob("*") if path.is_file())
        if len(members) != 20:
            raise GoldenArchiveError(f"archive must contain 20 files, got {len(members)}")
        _scan_secrets(members, token_values)
        _verify_parquet(work, tables)
        _write_tar_zstd(work, members, partial)
        _test_zstd(partial)
        digest = _sha256(partial)
        os.replace(partial, final_path)
        sidecar = final_path.with_suffix(final_path.suffix + ".sha256")
        sidecar.write_text(f"{digest}  {final_path.name}\n", encoding="utf-8")
        if _sha256(final_path) != digest:
            raise GoldenArchiveError("post-publication SHA-256 mismatch")
        return GoldenArchiveResult(
            final_path, sidecar, digest, final_path.stat().st_size, len(members), rows
        )
    finally:
        if partial.exists():
            partial.unlink()
        import shutil

        shutil.rmtree(work, ignore_errors=True)


def _dataset_manifest(name: str, table: pa.Table, path: Path) -> dict[str, Any]:
    return {
        "dataset": name,
        "path": f"data/{name}.parquet",
        "rows": table.num_rows,
        "bytes": path.stat().st_size,
        "sha256": _sha256(path),
        "columns": table.column_names,
        "null_fraction": {
            field.name: (table[field.name].null_count / table.num_rows if table.num_rows else 0.0)
            for field in table.schema
        },
    }


def _verify_parquet(work: Path, expected: Mapping[str, pa.Table]) -> None:
    connection = duckdb.connect()
    try:
        for name, source in expected.items():
            path = work / "data" / f"{name}.parquet"
            arrow = pq.read_table(path)
            if arrow.num_rows != source.num_rows or arrow.schema != source.schema:
                raise GoldenArchiveError(f"PyArrow round-trip mismatch: {name}")
            count_row = connection.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(path)]
            ).fetchone()
            if count_row is None:
                raise GoldenArchiveError(f"DuckDB returned no count: {name}")
            count = count_row[0]
            if count != source.num_rows:
                raise GoldenArchiveError(f"DuckDB row-count mismatch: {name}")
    finally:
        connection.close()


def _scan_secrets(paths: Sequence[Path], values: Sequence[bytes]) -> None:
    secrets = [value.strip() for value in values if len(value.strip()) >= 12]
    for path in paths:
        body = path.read_bytes()
        if any(secret in body for secret in secrets):
            raise GoldenArchiveError(f"secret material detected in {path.name}")


def _write_tar_zstd(root: Path, members: Sequence[Path], target: Path) -> None:
    compressor = zstandard.ZstdCompressor(level=12)
    with (
        target.open("xb") as raw,
        compressor.stream_writer(raw, closefd=False) as encoded,
        tarfile.open(fileobj=encoded, mode="w|") as archive,
    ):
        for path in members:
            info = archive.gettarinfo(str(path), arcname=path.relative_to(root).as_posix())
            info.mtime = 0
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            with path.open("rb") as handle:
                archive.addfile(info, handle)


def _test_zstd(path: Path) -> None:
    with path.open("rb") as raw, zstandard.ZstdDecompressor().stream_reader(raw) as decoded:
        while decoded.read(1024 * 1024):
            pass


def _schema_dictionary(tables: Mapping[str, pa.Table]) -> str:
    lines = ["# Schema dictionary", "", "All timestamps are typed UTC microseconds.", ""]
    for name in DATASETS:
        lines.extend((f"## {name}", "", "| Column | Arrow type | Nullable |", "|---|---|---|"))
        for field in tables[name].schema:
            lines.append(f"| {field.name} | `{field.type}` | {field.nullable} |")
        lines.append("")
    return "\n".join(lines)


def _validation_report(validation: Mapping[str, Any]) -> str:
    metrics = validation.get("metrics", {})
    gates = validation.get("gates", {})
    lines = ["# Validation report", "", f"Overall: **{validation.get('status')}**", ""]
    for section in ("raw", "features", "execution", "outcomes", "stops_exits", "golden"):
        lines.extend((f"## {section.replace('_', ' ').title()}", ""))
        payload = metrics.get(section, {})
        if isinstance(payload, Mapping):
            for key, value in sorted(payload.items()):
                lines.append(f"- {key}: `{value}`")
        lines.append("")
    lines.extend(("## Gates", ""))
    for name, value in sorted(gates.items()):
        lines.append(f"- {name}: **{value}**")
    return "\n".join(lines) + "\n"


def _manifest_md(manifest: Mapping[str, Any]) -> str:
    lines = [
        "# Manifest",
        "",
        f"Schema: `{manifest['schema_version']}`",
        "",
        "| Dataset | Rows |",
        "|---|---:|",
    ]
    for name, rows in manifest["rows_by_dataset"].items():
        lines.append(f"| {name} | {rows} |")
    return "\n".join(lines) + "\n"


def _readme(t0: datetime, t1: datetime) -> str:
    return (
        "# Neobitcoin schema-v4.1-golden review bundle\n\n"
        "All derived tables were materialized exclusively from the archived raw datasets.\n"
        "The independent oracle does not import production calculation logic.\n\n"
        f"Candidate window: `{t0.isoformat()}` — `{t1.isoformat()}`.\n"
    )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise GoldenArchiveError("naive timestamp")
    return value.astimezone(UTC)


def _stamp(value: datetime) -> str:
    return _utc(value).strftime("%Y%m%dT%H%M%SZ")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = ["DATASETS", "GoldenArchiveError", "GoldenArchiveResult", "publish_golden_archive"]
