"""Build a small, self-validating Neobitcoin review bundle.

The bundle writer is deliberately independent from the live collector.  It
consumes immutable, typed review Parquet files, selects the latest mature
ten-minute candidate window, and publishes only after every validation check
passes.  An archive contains exactly one Parquet per dataset and five text
documents (twenty members in total).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tarfile
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any, Final

import duckdb
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.compute as pc  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
import zstandard

from .archive import ArchiveValidationError

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
HORIZONS: Final = (5, 10, 30, 60, 180, 300, 600, 900, 1800)
STOPS: Final = (2, 3, 4, 5)
EXIT_VARIANTS: Final = (
    "take_profit",
    "time_exit",
    "breakeven",
    "dynamic_breakeven",
    "trailing",
    "microstructure",
    "orderbook",
)
COMMON_COLUMNS: Final = (
    "schema_version",
    "event_id",
    "instrument_uid",
    "instrument_ticker",
    "exchange_ts",
    "receive_ts",
    "processing_ts",
    "session_id",
    "collector_instance_id",
    "source",
    "code_commit",
    "config_hash",
    "data_quality_flags",
)
TIMESTAMP_COLUMNS: Final = (
    "exchange_ts",
    "receive_ts",
    "processing_ts",
    "candidate_ts",
    "decision_ts",
    "entry_ts",
    "fill_ts",
    "future_ts",
    "exit_ts",
    "candle_start",
    "candle_end",
)
PRIMARY_KEYS: Final = {
    "raw_orderbook": "event_id",
    "raw_trades": "event_id",
    "raw_last_price": "event_id",
    "candles_1m": "event_id",
    "candles_5m": "event_id",
    "candles_15m": "event_id",
    "feature_snapshots": "feature_snapshot_id",
    "candidate_events": "candidate_id",
    "execution_simulations": "simulation_id",
    "future_outcomes": "outcome_id",
    "shadow_stop_results": "result_id",
    "shadow_exit_results": "result_id",
}
FOREIGN_KEYS: Final = {
    "candidate_events": (("feature_snapshot_id", "feature_snapshots", "feature_snapshot_id"),),
    "filter_decisions": (("candidate_id", "candidate_events", "candidate_id"),),
    "execution_simulations": (
        ("candidate_id", "candidate_events", "candidate_id"),
        ("feature_snapshot_id", "feature_snapshots", "feature_snapshot_id"),
    ),
    "future_outcomes": (
        ("candidate_id", "candidate_events", "candidate_id"),
        ("simulation_id", "execution_simulations", "simulation_id"),
    ),
    "shadow_stop_results": (
        ("candidate_id", "candidate_events", "candidate_id"),
        ("simulation_id", "execution_simulations", "simulation_id"),
    ),
    "shadow_exit_results": (
        ("candidate_id", "candidate_events", "candidate_id"),
        ("simulation_id", "execution_simulations", "simulation_id"),
    ),
}
SECRET_PATTERN = re.compile(rb"(?:t\.[A-Za-z0-9_-]{20,}|token\s*[=:]\s*[^\s,;]+)", re.I)


@dataclass(frozen=True, slots=True)
class ReviewBundleResult:
    archive_path: Path
    sha256_path: Path
    archive_sha256: str
    candidate_window_start: datetime
    candidate_window_end: datetime
    support_data_start: datetime
    support_data_end: datetime
    file_count: int
    rows_by_dataset: dict[str, int]


def create_neobitcoin_review_bundle(
    root: Path | str,
    *,
    output_dir: Path | str | None = None,
    now: datetime | None = None,
    candidate_window_minutes: int = 10,
    max_outcome_horizon_minutes: int = 30,
    candle_context_hours: int = 6,
    instrument: str = "NEOBITCOIN",
    token_files: tuple[Path | str, ...] = (),
) -> ReviewBundleResult:
    """Create a schema-v3 review archive from typed review source Parquet.

    Source discovery supports the stable ``review_parquet/<dataset>`` layout
    produced by :mod:`review_raw` and :mod:`review_research`; ``root`` itself
    may also be that directory, which keeps fixtures and offline tools simple.
    """

    base = Path(root).resolve()
    destination = Path(output_dir).resolve() if output_dir else base / "review_bundles"
    destination.mkdir(parents=True, exist_ok=True)
    current = _utc(now or datetime.now(UTC))
    if (base / "active" / "raw").exists() or (base / "raw").exists():
        from .schema_v4.workflow import create_schema_v4_golden_bundle

        result = create_schema_v4_golden_bundle(
            base,
            output_dir=destination,
            now=current,
            token_files=token_files,
        )
        return ReviewBundleResult(
            archive_path=result.archive.archive_path,
            sha256_path=result.archive.sha256_path,
            archive_sha256=result.archive.sha256,
            candidate_window_start=result.candidate_start,
            candidate_window_end=result.candidate_end,
            support_data_start=result.support_start,
            support_data_end=result.required_support_end,
            file_count=result.archive.file_count,
            rows_by_dataset=result.archive.rows_by_dataset,
        )
    source = _discover_source(base)
    tables = {name: _read_dataset(source, name) for name in DATASETS}
    t0, t1 = _select_window(
        tables,
        now=current,
        minutes=candidate_window_minutes,
        maturity=timedelta(minutes=max_outcome_horizon_minutes),
    )
    support_start = t0 - timedelta(hours=candle_context_hours)
    support_end = _required_support_end(tables, t0, t1, max_outcome_horizon_minutes)
    selected = _select_tables(tables, t0, t1, support_start, support_end)
    secrets = _secret_values(token_files)

    stamp0 = _name_ts(t0)
    stamp1 = _name_ts(t1)
    archive_name = f"neobitcoin_review_10m_{stamp0}_{stamp1}_schema-v3-fixed2.tar.zst"
    work = Path(tempfile.mkdtemp(prefix="review-bundle-", dir=destination))
    try:
        data_dir = work / "data"
        data_dir.mkdir()
        for dataset, table in selected.items():
            target = data_dir / f"{dataset}.parquet"
            pq.write_table(
                table,
                target,
                compression="zstd",
                use_dictionary=True,
                write_statistics=True,
                row_group_size=64_000,
            )
        validation = _validate_tables(selected, t0, t1, support_start, support_end, secrets)
        manifest = _build_manifest(
            work,
            selected,
            t0,
            t1,
            support_start,
            support_end,
            instrument,
            validation,
        )
        documents = {
            "README.md": _readme(t0, t1, support_start, support_end),
            "MANIFEST.md": _manifest_md(manifest),
            "SCHEMA_DICTIONARY.md": _schema_dictionary(selected),
            "VALIDATION_REPORT.md": _validation_report(validation, selected),
        }
        for name, body in documents.items():
            (work / name).write_text(body, encoding="utf-8", newline="\n")
        for name in documents:
            path = work / name
            manifest["files"].append(
                {
                    "path": name,
                    "dataset": "document",
                    "schema_version": None,
                    "row_count": None,
                    "min_timestamp": None,
                    "max_timestamp": None,
                    "bytes": path.stat().st_size,
                    "sha256": _sha256(path),
                    "columns": [],
                    "null_fraction": {},
                    "duplicate_count": 0,
                    "primary_key": None,
                    "foreign_keys": [],
                }
            )
        manifest["manifest_self_hash"] = "excluded: self-referential digest is impossible"
        (work / "MANIFEST.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
            newline="\n",
        )
        members = sorted(path for path in work.rglob("*") if path.is_file())
        if len(members) != 20:
            raise ArchiveValidationError(f"review bundle must contain 20 files, got {len(members)}")
        _scan_files(members, secrets)
        partial = destination / f".{archive_name}.inprogress"
        _write_tar_zst(partial, work, members)
        _verify_tar(partial, manifest, secrets)
        digest = _sha256(partial)
        final = destination / archive_name
        sidecar = destination / f"{archive_name}.sha256"
        if final.exists() or sidecar.exists():
            raise ArchiveValidationError(f"review bundle already exists: {archive_name}")
        os.replace(partial, final)
        _atomic_text(sidecar, f"{digest}  {archive_name}\n")
        return ReviewBundleResult(
            final,
            sidecar,
            digest,
            t0,
            t1,
            support_start,
            support_end,
            20,
            {name: table.num_rows for name, table in selected.items()},
        )
    except Exception:
        for partial in destination.glob(".*.inprogress"):
            partial.unlink(missing_ok=True)
        raise
    finally:
        shutil.rmtree(work, ignore_errors=True)


def _discover_source(root: Path) -> Path:
    for candidate in (
        root / "review_parquet" / "data",
        root / "review_parquet",
        root / "active" / "review_parquet",
        root,
    ):
        if all(
            (candidate / name).exists() or (candidate / f"{name}.parquet").exists()
            for name in DATASETS
        ):
            return candidate
    raise ArchiveValidationError("typed review_parquet datasets are not available")


def _prepare_source(
    root: Path,
    *,
    now: datetime,
    candidate_window_minutes: int,
    max_outcome_horizon_minutes: int,
    candle_context_hours: int,
    token_files: tuple[Path | str, ...],
) -> Path:
    """Integrate the local raw and counterfactual review builders."""

    from .review_datasets import build_review_datasets
    from .review_raw import COMMON_FIELDS as RAW_COMMON_FIELDS
    from .review_raw import extract_review_raw

    raw_root = root / "active" / "raw"
    if not raw_root.exists():
        raw_root = root / "raw"
    output = root / "review_parquet"
    if output.exists():
        # Generated offline cache; collector-owned capture is never modified.
        shutil.rmtree(output)
    extraction = extract_review_raw(
        raw_root,
        output,
        now=now,
        candidate_window_minutes=candidate_window_minutes,
        max_outcome_horizon_minutes=max_outcome_horizon_minutes,
        candle_context_hours=candle_context_hours,
    )
    data = output / "data"
    feature_paths = sorted(
        (root / "active" / "derived_parquet" / "feature_snapshots").rglob("*.parquet")
    )
    if not feature_paths:
        feature_paths = sorted((root / "compacted" / "feature_snapshots").rglob("*.parquet"))
    if not feature_paths:
        raise ArchiveValidationError("feature snapshots are unavailable for review window")
    feature_rows: list[dict[str, Any]] = []
    for path in feature_paths:
        feature_rows.extend(pq.read_table(path).to_pylist())
    signal_paths = sorted(
        (root / "active" / "derived_parquet" / "candidate_events").rglob("*.parquet")
    )
    if not signal_paths:
        signal_paths = sorted((root / "compacted" / "candidate_events").rglob("*.parquet"))
    signals: dict[str, str] = {}
    for path in signal_paths:
        for row in pq.read_table(path).to_pylist():
            feature_id = str(row.get("feature_snapshot_id") or "")
            raw_signal = row.get("raw_signal")
            if feature_id and raw_signal:
                signals[feature_id] = str(raw_signal)
    for row in feature_rows:
        feature_id = str(row.get("feature_snapshot_id") or row.get("raw_event_id") or "")
        if feature_id in signals:
            row["raw_signal"] = signals[feature_id]
    # A review window audits both counterfactual sides and does not require a
    # directional production signal.  Pinning selection to an old rare signal
    # can regress to a pre-warmup window even when newer complete data exists.
    _backfill_review_candles(extraction, token_files)
    _enrich_candle_features(feature_rows, extraction.paths)
    orderbook_rows = pq.read_table(extraction.paths["raw_orderbook"]).to_pylist()
    trade_rows = pq.read_table(extraction.paths["raw_trades"]).to_pylist()
    last_price_rows = pq.read_table(extraction.paths["raw_last_price"]).to_pylist()
    candle_rows = {
        f"{minutes}m": pq.read_table(extraction.paths[f"candles_{minutes}m"]).to_pylist()
        for minutes in (1, 5, 15)
    }
    price_path = [
        {"exchange_ts": row["exchange_ts"], "mid_price": row["mid_price"]} for row in orderbook_rows
    ]
    research = build_review_datasets(
        feature_rows,
        price_path,
        candidate_start=extraction.window.candidate_start,
        candidate_end=extraction.window.candidate_end,
        orderbook_rows=orderbook_rows,
        trade_rows=trade_rows,
        last_price_rows=last_price_rows,
        candle_rows_by_interval=candle_rows,
        # About 0.7% of observed median depth: large enough to expose partial
        # fills while remaining a modest diagnostic order for this instrument.
        virtual_order_quantity=5_000.0,
    )
    for dataset, table in research.items():
        pq.write_table(table, data / f"{dataset}.parquet", compression="zstd")
    quality_schema = pa.schema(
        [
            *RAW_COMMON_FIELDS,
            pa.field("event_type", pa.string(), nullable=False),
            pa.field("severity", pa.string(), nullable=False),
            pa.field("gap_seconds", pa.float64()),
            pa.field("details", pa.string()),
        ],
        metadata={b"schema_version": b"schema-v3"},
    )
    pq.write_table(
        pa.Table.from_pylist([], schema=quality_schema),
        data / "data_quality_events.parquet",
        compression="zstd",
    )
    return data


def _backfill_review_candles(extraction: Any, token_files: tuple[Path | str, ...]) -> None:
    """Replace stream candle replays with a complete local readonly REST backfill."""

    from neo_trader.neobitcoin_research.review_raw import RAW_SCHEMAS
    from neo_trader.neobitcoin_research.safety import TBankTokenFileError, load_tbank_token
    from neo_trader.neobitcoin_research.tbank import TBankResearchClient

    client = None
    for token_file in token_files:
        try:
            load_tbank_token(token_file)
            client = TBankResearchClient.from_token_file(token_file)
            break
        except TBankTokenFileError:
            continue
    if client is None:
        raise ArchiveValidationError(
            "a valid local T-Bank token file is required for candle warmup"
        )

    books = pq.read_table(extraction.paths["raw_orderbook"])
    if not books.num_rows:
        raise ArchiveValidationError("raw order book is empty")
    common = books.slice(0, 1).to_pylist()[0]
    instrument_uid = str(common["instrument_uid"])
    interval_names = {
        1: "CANDLE_INTERVAL_1_MIN",
        5: "CANDLE_INTERVAL_5_MIN",
        15: "CANDLE_INTERVAL_15_MIN",
    }
    received = datetime.now(UTC)
    try:
        for minutes, interval_name in interval_names.items():
            existing_rows = pq.read_table(extraction.paths[f"candles_{minutes}m"]).to_pylist()
            payloads = client.get_candles(
                instrument_uid,
                from_time=extraction.window.support_start,
                to_time=extraction.window.support_end,
                interval=interval_name,
            )
            rows: list[dict[str, Any]] = []
            for payload in payloads:
                start = _timestamp_value(payload["time"])
                end = start + timedelta(minutes=minutes)
                if not extraction.window.support_start <= start <= extraction.window.support_end:
                    continue
                event_id = hashlib.sha256(
                    f"review-candle|{instrument_uid}|{minutes}|{start.isoformat()}".encode()
                ).hexdigest()
                rows.append(
                    {
                        "schema_version": "schema-v3",
                        "event_id": event_id,
                        "instrument_uid": instrument_uid,
                        "instrument_ticker": common["instrument_ticker"],
                        "exchange_ts": start,
                        "receive_ts": received,
                        "processing_ts": received,
                        "session_id": common["session_id"],
                        "collector_instance_id": common["collector_instance_id"],
                        "source": "tbank_get_candles_local_backfill",
                        "code_commit": common["code_commit"],
                        "config_hash": common["config_hash"],
                        "data_quality_flags": ["historical_backfill"],
                        "candle_start": start,
                        "candle_end": end,
                        "open": _quotation_number(payload["open"]),
                        "high": _quotation_number(payload["high"]),
                        "low": _quotation_number(payload["low"]),
                        "close": _quotation_number(payload["close"]),
                        "volume": float(str(payload.get("volume") or 0)),
                        "is_complete": bool(payload.get("isComplete")) or end <= received,
                        "source_timeframe": f"{minutes}m",
                        "is_backfilled": True,
                    }
                )
            if not rows:
                raise ArchiveValidationError(f"T-Bank returned no {minutes}m candle warmup")
            merged = {_utc(row["candle_start"]): row for row in rows}
            # Prefer the earliest actually streamed copy for a candle. Its
            # receive timestamp is valid lineage for historical feature ages;
            # REST backfill remains only for candle starts absent in the stream.
            for row in sorted(
                existing_rows, key=lambda item: _utc(item["receive_ts"]), reverse=True
            ):
                if not row.get("is_complete"):
                    continue
                merged[_utc(row["candle_start"])] = row
            rows = [merged[key] for key in sorted(merged)]
            pq.write_table(
                pa.Table.from_pylist(rows, schema=RAW_SCHEMAS[f"candles_{minutes}m"]),
                extraction.paths[f"candles_{minutes}m"],
                compression="zstd",
                use_dictionary=True,
                write_statistics=True,
            )
    finally:
        client.close()


def _quotation_number(value: Any) -> float:
    if isinstance(value, Mapping):
        units = float(str(value.get("units", 0)))
        nanos = float(str(value.get("nano", 0)))
        return units + nanos / 1_000_000_000
    return float(value)


def _enrich_candle_features(
    feature_rows: list[dict[str, Any]], raw_paths: Mapping[str, Path]
) -> None:
    """Fill candle features from completed candles without using future data."""

    timelines: dict[int, list[dict[str, Any]]] = {}
    for minutes in (1, 5, 15):
        table = pq.read_table(raw_paths[f"candles_{minutes}m"])
        # Reconnects can replay the same historical candle. Keep the latest
        # received copy, then order the unique exchange-time candles.
        unique: dict[datetime, dict[str, Any]] = {}
        for candle in sorted(table.to_pylist(), key=lambda row: _utc(row["receive_ts"])):
            if candle.get("is_complete"):
                unique[_utc(candle["candle_start"])] = candle
        timelines[minutes] = sorted(unique.values(), key=lambda row: _utc(row["candle_start"]))

    required = tuple(
        name for minutes in (1, 5, 15) for name in (f"atr_{minutes}m", f"candle_volume_{minutes}m")
    )
    for feature in feature_rows:
        feature_ts = _timestamp_value(
            feature.get("exchange_ts")
            or feature.get("exchange_timestamp")
            or feature.get("timestamp")
            or feature.get("recorded_at")
        )
        for minutes, candles in timelines.items():
            available = [row for row in candles if _utc(row["candle_end"]) <= feature_ts]
            if not available:
                continue
            feature[f"candle_volume_{minutes}m"] = float(available[-1]["volume"])
            true_ranges: list[float] = []
            for index, candle in enumerate(available):
                high = float(candle["high"])
                low = float(candle["low"])
                previous_close = (
                    float(available[index - 1]["close"]) if index else float(candle["open"])
                )
                true_ranges.append(
                    max(high - low, abs(high - previous_close), abs(low - previous_close))
                )
            feature[f"atr_{minutes}m"] = sum(true_ranges[-14:]) / min(14, len(true_ranges))
        missing = [name for name in required if feature.get(name) is None]
        source_ready = bool(feature.get("feature_ready", True))
        feature["feature_ready"] = source_ready and not missing
        if missing:
            feature["missing_reason"] = "missing completed candle features: " + ", ".join(missing)
        else:
            feature["missing_reason"] = None


def _read_dataset(source: Path, dataset: str) -> pa.Table:
    directory = source / dataset
    paths = sorted(directory.rglob("*.parquet")) if directory.exists() else []
    flat = source / f"{dataset}.parquet"
    if flat.is_file():
        paths.append(flat)
    if not paths:
        raise ArchiveValidationError(f"missing review dataset: {dataset}")
    tables = [pq.read_table(path) for path in paths]
    table = pa.concat_tables(tables, promote_options="default")
    _validate_schema(dataset, table)
    sort_column = next(
        (
            name
            for name in ("candidate_ts", "exchange_ts", "receive_ts")
            if name in table.column_names
        ),
        None,
    )
    if sort_column and table.num_rows:
        table = table.take(pc.sort_indices(table, sort_keys=[(sort_column, "ascending")]))
    return table


def _validate_schema(dataset: str, table: pa.Table) -> None:
    missing = [name for name in COMMON_COLUMNS if name not in table.column_names]
    if missing:
        raise ArchiveValidationError(f"{dataset}: missing common columns {missing}")
    for name in TIMESTAMP_COLUMNS:
        if name in table.column_names and not pa.types.is_timestamp(table.schema.field(name).type):
            raise ArchiveValidationError(f"{dataset}.{name} must be Arrow timestamp")
        if name in table.column_names and table.schema.field(name).type.tz != "UTC":
            raise ArchiveValidationError(f"{dataset}.{name} must use UTC")
    if dataset == "raw_orderbook":
        for name in ("bids", "asks"):
            if name not in table.column_names or not pa.types.is_list(
                table.schema.field(name).type
            ):
                raise ArchiveValidationError(f"raw_orderbook.{name} must be typed list")
    for forbidden in ("payload_json", "event_json", "record_json"):
        if forbidden in table.column_names and dataset.startswith("raw_"):
            raise ArchiveValidationError(f"{dataset}: raw values must not be stored in {forbidden}")


def _select_window(
    tables: dict[str, pa.Table], *, now: datetime, minutes: int, maturity: timedelta
) -> tuple[datetime, datetime]:
    candidates = tables["candidate_events"]
    if "candidate_ts" not in candidates.column_names or candidates.num_rows == 0:
        raise ArchiveValidationError("candidate_events has no candidate timestamps")
    times = [_utc(value) for value in candidates["candidate_ts"].to_pylist() if value is not None]
    cutoff = now - maturity
    possible = sorted(
        {
            value.replace(second=0, microsecond=0) + timedelta(minutes=1)
            for value in times
            if value + timedelta(minutes=1) <= cutoff
        },
        reverse=True,
    )
    for t1 in possible:
        t0 = t1 - timedelta(minutes=minutes)
        selected_ids = _candidate_ids(candidates, t0, t1)
        if selected_ids and _window_is_complete(tables, selected_ids, t0, t1):
            return t0, t1
    raise ArchiveValidationError("no stable ten-minute window with mature outcomes")


def _window_is_complete(
    tables: dict[str, pa.Table], candidate_ids: set[str], t0: datetime, t1: datetime
) -> bool:
    raw_end = t1 + timedelta(minutes=30)
    required_starts = {
        "raw_orderbook": t0,
        "raw_trades": t0 - timedelta(seconds=900),
        "raw_last_price": t0,
    }
    for dataset in ("raw_orderbook", "raw_trades", "raw_last_price"):
        table = tables[dataset]
        if "receive_ts" not in table.column_names or not table.num_rows:
            return False
        values = [_utc(value) for value in table["receive_ts"].to_pylist() if value is not None]
        if not values or min(values) > required_starts[dataset] or max(values) < raw_end:
            return False
    candidates = _filter_time(tables["candidate_events"], "candidate_ts", t0, t1)
    sides = set(candidates["side"].to_pylist()) if "side" in candidates.column_names else set()
    decisions = (
        set(candidates["final_decision"].to_pylist())
        if "final_decision" in candidates.column_names
        else set()
    )
    if not {"LONG", "SHORT"}.issubset(sides) or not decisions:
        return False
    if "feature_ready" not in candidates.column_names or not all(
        candidates["feature_ready"].to_pylist()
    ):
        return False
    feature_ids = {str(value) for value in candidates["feature_snapshot_id"].to_pylist() if value}
    features = _filter_ids(tables["feature_snapshots"], "feature_snapshot_id", feature_ids)
    if features.num_rows != len(feature_ids):
        return False
    for column in (
        "trade_flow_10s",
        "trade_flow_180s",
        "trade_flow_300s",
        "trade_flow_900s",
        "data_age_ms",
    ):
        if column not in features.column_names or any(
            value is None for value in features[column].to_pylist()
        ):
            return False
    outcomes = _filter_ids(tables["future_outcomes"], "candidate_id", candidate_ids)
    present = (
        set(outcomes["horizon_seconds"].to_pylist())
        if "horizon_seconds" in outcomes.column_names
        else set()
    )
    if not set(HORIZONS).issubset(present):
        return False
    if "outcome_complete" in outcomes.column_names and not all(
        outcomes["outcome_complete"].to_pylist()
    ):
        return False
    quality = _filter_time(tables["data_quality_events"], "receive_ts", t0, t1)
    if "severity" in quality.column_names and any(
        value == "critical" for value in quality["severity"].to_pylist()
    ):
        return False
    return not (
        "event_type" in quality.column_names
        and any(
            value in {"reconnect", "collector_stop", "gap"}
            for value in quality["event_type"].to_pylist()
        )
    )


def _select_tables(
    tables: dict[str, pa.Table],
    t0: datetime,
    t1: datetime,
    support_start: datetime,
    support_end: datetime,
) -> dict[str, pa.Table]:
    raw_start = t0 - timedelta(seconds=60)
    candidate_ids = _candidate_ids(tables["candidate_events"], t0, t1)
    candidates = _filter_ids(
        _filter_time(tables["candidate_events"], "candidate_ts", t0, t1),
        "candidate_id",
        candidate_ids,
    )
    feature_ids = set(str(v) for v in candidates["feature_snapshot_id"].to_pylist() if v)
    simulations_all = _filter_ids(tables["execution_simulations"], "candidate_id", candidate_ids)
    simulation_ids = set(str(v) for v in simulations_all["simulation_id"].to_pylist() if v)
    result: dict[str, pa.Table] = {}
    for name, table in tables.items():
        if name.startswith("candles_"):
            result[name] = _filter_time(table, "candle_start", support_start, support_end)
        elif name == "raw_trades":
            # The longest signed-volume feature needs a directly auditable 900s history.
            result[name] = _filter_time_with_context(
                table, "receive_ts", t0 - timedelta(seconds=900), support_end
            )
        elif name in {"raw_orderbook", "raw_last_price"}:
            result[name] = _filter_time_with_context(table, "receive_ts", raw_start, support_end)
        elif name in {"market_status_events", "data_quality_events"}:
            result[name] = _filter_time(table, "receive_ts", raw_start, support_end)
        elif name == "feature_snapshots":
            result[name] = _filter_ids(table, "feature_snapshot_id", feature_ids)
        elif name == "candidate_events":
            result[name] = candidates
        elif name in {
            "filter_decisions",
            "execution_simulations",
            "future_outcomes",
            "shadow_stop_results",
            "shadow_exit_results",
        }:
            narrowed = _filter_ids(table, "candidate_id", candidate_ids)
            if "simulation_id" in narrowed.column_names and name != "execution_simulations":
                narrowed = _filter_ids(narrowed, "simulation_id", simulation_ids)
            result[name] = narrowed
        else:
            result[name] = table.slice(0, 0)
    return result


def _required_support_end(
    tables: dict[str, pa.Table],
    t0: datetime,
    t1: datetime,
    max_outcome_horizon_minutes: int,
) -> datetime:
    """Derive support from candidate entries/fills, never merely candidate end."""

    candidate_ids = _candidate_ids(tables["candidate_events"], t0, t1)
    executions = _filter_ids(tables["execution_simulations"], "candidate_id", candidate_ids)
    outcomes = _filter_ids(tables["future_outcomes"], "candidate_id", candidate_ids)
    entries = [
        _utc(value)
        for table, column in ((executions, "order_ts"), (outcomes, "entry_ts"))
        if column in table.column_names
        for value in table[column].to_pylist()
        if value is not None
    ]
    fills = (
        [_utc(value) for value in executions["fill_ts"].to_pylist() if value is not None]
        if "fill_ts" in executions.column_names
        else []
    )
    max_required_entry = max([t1, *entries, *fills])
    return max_required_entry + timedelta(minutes=max_outcome_horizon_minutes)


def _filter_time(table: pa.Table, column: str, start: datetime, end: datetime) -> pa.Table:
    if column not in table.column_names or not table.num_rows:
        return table.slice(0, 0)
    mask = pc.and_(
        pc.greater_equal(table[column], pa.scalar(start)),
        pc.less_equal(table[column], pa.scalar(end)),
    )
    return table.filter(mask)


def _filter_time_with_prefix(
    table: pa.Table, column: str, start: datetime, end: datetime
) -> pa.Table:
    """Filter an event stream while retaining its last as-of event before start."""

    selected = _filter_time(table, column, start, end)
    if column not in table.column_names or not table.num_rows:
        return selected
    values = table[column].to_pylist()
    before = [
        (index, _utc(value))
        for index, value in enumerate(values)
        if value is not None and _utc(value) < start
    ]
    if not before:
        return selected
    prefix_index = max(before, key=lambda item: item[1])[0]
    return pa.concat_tables([table.take(pa.array([prefix_index])), selected])


def _filter_time_with_context(
    table: pa.Table, column: str, start: datetime, end: datetime
) -> pa.Table:
    """Retain as-of prefix and the first event proving coverage beyond end."""

    selected = _filter_time_with_prefix(table, column, start, end)
    if column not in table.column_names or not table.num_rows:
        return selected
    after = [
        (index, _utc(value))
        for index, value in enumerate(table[column].to_pylist())
        if value is not None and _utc(value) > end
    ]
    if not after:
        return selected
    suffix_index = min(after, key=lambda item: item[1])[0]
    return pa.concat_tables([selected, table.take(pa.array([suffix_index]))])


def _filter_ids(table: pa.Table, column: str, values: set[str]) -> pa.Table:
    if column not in table.column_names or not table.num_rows:
        return table.slice(0, 0)
    return table.filter(pc.is_in(table[column], value_set=pa.array(sorted(values))))


def _candidate_ids(table: pa.Table, start: datetime, end: datetime) -> set[str]:
    selected = _filter_time(table, "candidate_ts", start, end)
    return {str(value) for value in selected["candidate_id"].to_pylist() if value}


def _validate_tables(
    tables: dict[str, pa.Table],
    t0: datetime,
    t1: datetime,
    support_start: datetime,
    support_end: datetime,
    secrets: list[bytes],
) -> dict[str, Any]:
    from .review_reconciliation import reconcile_review_tables

    errors: list[str] = []
    checks: list[dict[str, str]] = []

    def check(
        name: str,
        passed: bool,
        measured: Any,
        threshold: str,
        explanation: str,
    ) -> None:
        status = "PASS" if passed else "FAIL"
        checks.append(
            {
                "check": name,
                "status": status,
                "measured_value": str(measured),
                "threshold": threshold,
                "explanation": explanation,
            }
        )
        if not passed:
            errors.append(f"{name}: {explanation} (measured {measured})")

    check(
        "candidate_window_duration",
        t1 - t0 == timedelta(minutes=10),
        (t1 - t0).total_seconds(),
        "exactly 600 seconds",
        "candidate window must be exactly ten minutes",
    )
    raw_coverage: dict[str, dict[str, str | None]] = {}
    raw_required_end = t1 + timedelta(minutes=30)
    raw_required_starts = {
        "raw_orderbook": t0,
        "raw_trades": t0 - timedelta(seconds=900),
        "raw_last_price": t0,
    }
    for dataset in ("raw_orderbook", "raw_trades", "raw_last_price"):
        table = tables[dataset]
        values = (
            [_utc(value) for value in table["receive_ts"].to_pylist() if value is not None]
            if "receive_ts" in table.column_names
            else []
        )
        start = min(values) if values else None
        end = max(values) if values else None
        raw_coverage[dataset] = {
            "start": start.isoformat() if start else None,
            "end": end.isoformat() if end else None,
        }
        check(
            f"{dataset}_coverage",
            bool(
                start
                and end
                and start <= raw_required_starts[dataset]
                and end >= raw_required_end
            ),
            f"{start.isoformat() if start else 'empty'} .. {end.isoformat() if end else 'empty'}",
            f"{raw_required_starts[dataset].isoformat()} .. {raw_required_end.isoformat()}",
            "raw stream must include source-specific pre-roll and 30 minutes of outcomes",
        )
    duplicate_counts: dict[str, int] = {}
    for dataset, table in tables.items():
        pk = PRIMARY_KEYS.get(dataset)
        duplicate_counts[dataset] = 0
        if pk and pk in table.column_names:
            values = table[pk].to_pylist()
            if any(value in (None, "") for value in values):
                errors.append(f"{dataset}: empty {pk}")
            duplicate_counts[dataset] = len(values) - len(set(values))
            if duplicate_counts[dataset]:
                errors.append(f"{dataset}: duplicate {pk}")
    fk_results: list[dict[str, Any]] = []
    for child, constraints in FOREIGN_KEYS.items():
        for column, parent, parent_column in constraints:
            child_values = {v for v in tables[child][column].to_pylist() if v not in (None, "")}
            parent_values = {
                v for v in tables[parent][parent_column].to_pylist() if v not in (None, "")
            }
            missing = child_values - parent_values
            fk_results.append(
                {
                    "child": f"{child}.{column}",
                    "parent": f"{parent}.{parent_column}",
                    "missing": len(missing),
                }
            )
            if missing:
                errors.append(f"foreign key {child}.{column}: {len(missing)} missing")
    candidates = tables["candidate_events"]
    sides = set(candidates["side"].to_pylist())
    decisions = set(candidates["final_decision"].to_pylist())
    horizons = sorted(
        set(
            int(v)
            for v in tables["future_outcomes"]["horizon_seconds"].to_pylist()
            if v is not None
        )
    )
    stops = sorted(
        set(
            int(v) for v in tables["shadow_stop_results"]["stop_ticks"].to_pylist() if v is not None
        )
    )
    if not {"LONG", "SHORT"}.issubset(sides):
        errors.append("LONG and SHORT candidates required")
    if not decisions:
        errors.append("explicit candidate decisions required")
    if horizons != list(HORIZONS):
        errors.append(f"horizons incomplete: {horizons}")
    if stops != list(STOPS):
        errors.append(f"stops incomplete: {stops}")
    simulation_ids = {
        str(row["simulation_id"])
        for row in tables["execution_simulations"].to_pylist()
        if row.get("simulation_id")
        and row.get("fill_status") in {"FULL_FILL", "PARTIAL_FILL"}
        and row.get("fill_ts") is not None
        and float(row.get("filled_quantity") or 0.0) > 0.0
    }
    actual_outcomes = {
        (str(row["simulation_id"]), int(row["horizon_seconds"]))
        for row in tables["future_outcomes"]
        .select(["simulation_id", "horizon_seconds"])
        .to_pylist()
    }
    expected_outcomes = {
        (simulation_id, horizon) for simulation_id in simulation_ids for horizon in HORIZONS
    }
    if actual_outcomes != expected_outcomes:
        errors.append(
            "outcomes missing for simulation/horizon pairs: "
            f"{len(expected_outcomes - actual_outcomes)}"
        )
    actual_stops = {
        (str(row["simulation_id"]), int(row["stop_ticks"]))
        for row in tables["shadow_stop_results"].select(["simulation_id", "stop_ticks"]).to_pylist()
    }
    expected_stops = {(simulation_id, stop) for simulation_id in simulation_ids for stop in STOPS}
    if actual_stops != expected_stops:
        errors.append(
            f"stops missing for simulation/tick pairs: {len(expected_stops - actual_stops)}"
        )
    if tables["shadow_exit_results"].num_rows == 0:
        errors.append("shadow exits missing")
    if tables["raw_orderbook"].num_rows == 0:
        errors.append("raw orderbook missing")
    check(
        "primary_keys_unique",
        sum(duplicate_counts.values()) == 0,
        sum(duplicate_counts.values()),
        "0 duplicates",
        "every declared primary key must be populated and unique",
    )
    missing_foreign_keys = sum(int(item["missing"]) for item in fk_results)
    check(
        "foreign_keys_complete",
        missing_foreign_keys == 0,
        missing_foreign_keys,
        "0 missing references",
        "all cross-dataset IDs must resolve",
    )
    check(
        "candidate_sides_and_decisions",
        {"LONG", "SHORT"}.issubset(sides) and bool(decisions),
        f"sides={sorted(sides)}, decisions={sorted(str(value) for value in decisions)}",
        "LONG+SHORT and explicit decisions",
        "review must include both counterfactual sides and explicit decisions",
    )
    check(
        "future_horizons_complete",
        horizons == list(HORIZONS) and actual_outcomes == expected_outcomes,
        f"horizons={horizons}, missing_pairs={len(expected_outcomes - actual_outcomes)}",
        f"{list(HORIZONS)} and 0 missing pairs",
        "every filled simulation needs every required future horizon",
    )
    check(
        "stop_variants_complete",
        stops == list(STOPS) and actual_stops == expected_stops,
        f"stops={stops}, missing_pairs={len(expected_stops - actual_stops)}",
        f"{list(STOPS)} and 0 missing pairs",
        "every filled simulation needs each stop distance",
    )
    check(
        "shadow_exits_present",
        tables["shadow_exit_results"].num_rows > 0,
        tables["shadow_exit_results"].num_rows,
        "> 0",
        "independent shadow exit simulations are required",
    )
    actual_exit_pairs = {
        (str(row["simulation_id"]), str(row["exit_variant"]))
        for row in tables["shadow_exit_results"].to_pylist()
    }
    expected_exit_pairs = {
        (simulation_id, variant) for simulation_id in simulation_ids for variant in EXIT_VARIANTS
    }
    check(
        "exit_variants_complete",
        actual_exit_pairs == expected_exit_pairs,
        f"missing={len(expected_exit_pairs - actual_exit_pairs)}",
        f"all {len(EXIT_VARIANTS)} variants for every filled simulation",
        "every filled simulation needs every independent exit model",
    )
    book_rows = sorted(
        tables["raw_orderbook"].select(["receive_ts", "session_id"]).to_pylist(),
        key=lambda row: _utc(row["receive_ts"]),
    )
    reconnect_count = sum(
        left["session_id"] != right["session_id"]
        for left, right in zip(book_rows, book_rows[1:], strict=False)
    )
    candidate_books = [row for row in book_rows if t0 <= _utc(row["receive_ts"]) <= t1]
    candidate_reconnect_count = sum(
        left["session_id"] != right["session_id"]
        for left, right in zip(candidate_books, candidate_books[1:], strict=False)
    )
    if candidate_reconnect_count:
        errors.append(f"candidate window contains {candidate_reconnect_count} reconnects")
    quality_rows = tables["data_quality_events"].to_pylist()
    gap_count = sum(str(row.get("event_type")) in {"gap", "critical_gap"} for row in quality_rows)
    critical_candidate_quality = sum(
        row.get("receive_ts") is not None
        and t0 <= _utc(row["receive_ts"]) <= t1
        and (
            str(row.get("severity", "")).lower() == "critical"
            or str(row.get("event_type"))
            in {"gap", "critical_gap", "reconnect", "disconnect", "collector_stop"}
        )
        for row in quality_rows
    )
    check(
        "candidate_window_no_critical_gap_or_reconnect",
        candidate_reconnect_count == 0 and critical_candidate_quality == 0,
        (
            f"session_reconnects={candidate_reconnect_count}, "
            f"quality_events={critical_candidate_quality}"
        ),
        "0",
        "candidate window cannot contain a critical gap, reconnect or disconnect",
    )

    executions = tables["execution_simulations"].to_pylist()
    candidate_times = {
        str(row["candidate_id"]): _utc(row["candidate_ts"])
        for row in tables["candidate_events"].to_pylist()
    }
    fill_before_order = 0
    for row in executions:
        fill = row.get("fill_ts")
        if fill is None or row.get("fill_status") == "NO_FILL":
            continue
        order = row.get("order_ts") or candidate_times.get(str(row.get("candidate_id")))
        if order is None or _utc(fill) < _utc(order):
            fill_before_order += 1
    check(
        "fill_not_before_order",
        fill_before_order == 0,
        fill_before_order,
        "0",
        "every fill must occur at or after order placement",
    )
    passive = [
        row for row in executions if str(row.get("execution_model", "")).startswith("passive")
    ]
    passive_statuses = [str(row.get("fill_status")) for row in passive]
    passive_filled = [
        row for row in passive if row.get("fill_status") in {"FULL_FILL", "PARTIAL_FILL"}
    ]
    all_passive_full = bool(passive) and all(value == "FULL_FILL" for value in passive_statuses)
    check(
        "passive_not_all_full_fill",
        not all_passive_full,
        f"{sum(value == 'FULL_FILL' for value in passive_statuses)}/{len(passive)}",
        "less than 100% FULL_FILL",
        "passive placement cannot guarantee complete execution",
    )
    no_fill_count = sum(value == "NO_FILL" for value in passive_statuses)
    check(
        "passive_has_no_fill",
        not passive or no_fill_count > 0,
        no_fill_count,
        ">= 1 when passive models exist",
        "at least one passive simulation must remain unfilled",
    )
    fill_status_counts: dict[str, dict[str, int]] = {}
    for row in executions:
        model = str(row.get("execution_model"))
        status = str(row.get("fill_status"))
        counts = fill_status_counts.setdefault(model, {})
        counts[status] = counts.get(status, 0) + 1
    observed_statuses = {
        status
        for counts in fill_status_counts.values()
        for status, count in counts.items()
        if count
    }
    check(
        "execution_statuses_complete",
        {"FULL_FILL", "PARTIAL_FILL", "NO_FILL"}.issubset(observed_statuses),
        sorted(observed_statuses),
        "FULL_FILL, PARTIAL_FILL and NO_FILL",
        "review simulations must expose full, partial and unfilled outcomes",
    )
    passive_delays = {
        int(row["fill_delay_ms"]) for row in passive_filled if row.get("fill_delay_ms") is not None
    }
    check(
        "passive_fill_delays_vary",
        len(passive_filled) < 2 or len(passive_delays) > 1,
        sorted(passive_delays),
        "> 1 distinct delay for multiple fills",
        "passive fills must be driven by market events, not one timer",
    )
    artificial = bool(passive_filled) and passive_delays.issubset({100, 250, 500})
    check(
        "no_fixed_artificial_passive_timing",
        not artificial,
        sorted(passive_delays),
        "not exclusively 100/250/500 ms",
        "fixed artificial passive fill timing is forbidden",
    )
    raw_bounds = [
        _utc(value)
        for name in ("raw_orderbook", "raw_trades")
        for value in tables[name]["receive_ts"].to_pylist()
        if value is not None
    ]
    unconfirmed = sum(
        not raw_bounds
        or row.get("fill_ts") is None
        or not min(raw_bounds) <= _utc(row["fill_ts"]) <= max(raw_bounds)
        for row in passive_filled
    )
    check(
        "passive_fill_has_raw_support",
        unconfirmed == 0,
        unconfirmed,
        "0",
        "filled passive orders need temporally available raw trade/order-book evidence",
    )

    exit_before_entry = 0
    negative_holding = 0
    for dataset in ("shadow_stop_results", "shadow_exit_results"):
        for row in tables[dataset].to_pylist():
            if row.get("entry_ts") is not None and row.get("exit_ts") is not None:
                exit_before_entry += _utc(row["exit_ts"]) < _utc(row["entry_ts"])
            if row.get("holding_ms") is not None:
                negative_holding += int(row["holding_ms"]) < 0
    check(
        "exit_not_before_entry",
        exit_before_entry == 0,
        exit_before_entry,
        "0",
        "exit_ts must be >= entry_ts",
    )
    check(
        "holding_time_nonnegative",
        negative_holding == 0,
        negative_holding,
        "0",
        "holding_ms must be non-negative",
    )
    invalid_stop_direction = sum(
        row.get("exit_reason") == "stop_triggered"
        and (
            (row.get("side") == "LONG" and float(row["exit_price"]) > float(row["entry_price"]))
            or (row.get("side") == "SHORT" and float(row["exit_price"]) < float(row["entry_price"]))
        )
        for row in tables["shadow_stop_results"].to_pylist()
    )
    check(
        "stop_direction_valid",
        invalid_stop_direction == 0,
        invalid_stop_direction,
        "0",
        "triggered LONG stops exit at/below entry and SHORT stops at/above entry",
    )
    early_outcomes = sum(
        _utc(row["future_ts"]) < _utc(row["entry_ts"])
        for row in tables["future_outcomes"].to_pylist()
        if row.get("future_ts") is not None and row.get("entry_ts") is not None
    )
    check(
        "outcome_not_before_entry",
        early_outcomes == 0,
        early_outcomes,
        "0",
        "future outcome must not precede entry",
    )

    exit_rows = tables["shadow_exit_results"].to_pylist()
    signatures: dict[str, dict[str, tuple[Any, ...]]] = {}
    for row in exit_rows:
        variant = str(row.get("exit_variant"))
        signatures.setdefault(variant, {})[str(row.get("simulation_id"))] = (
            row.get("activation_ts"),
            row.get("exit_ts"),
            row.get("exit_price"),
            row.get("exit_reason"),
            row.get("trigger_value"),
            row.get("trigger_threshold"),
        )
    identical_pairs: list[str] = []
    exit_comparisons = 0
    matching_exits = 0
    variants = sorted(signatures)
    for index, left in enumerate(variants):
        for right in variants[index + 1 :]:
            common_ids = set(signatures[left]) & set(signatures[right])
            exit_comparisons += len(common_ids)
            matching_exits += sum(
                signatures[left][key] == signatures[right][key] for key in common_ids
            )
            if common_ids and all(
                signatures[left][key] == signatures[right][key] for key in common_ids
            ):
                identical_pairs.append(f"{left}={right}")
    check(
        "exit_models_independent",
        not identical_pairs,
        ", ".join(identical_pairs) or "no identical pairs",
        "no pair fully identical",
        "each exit model must have independent trigger/results",
    )
    exit_match_percent = 100.0 * matching_exits / exit_comparisons if exit_comparisons else 0.0
    generic_reasons = {"horizon_end", "time_exit", ""}
    missing_logic = sum(
        not str(row.get("variant_parameters") or "").strip()
        or not str(row.get("exit_reason") or "").strip()
        for row in exit_rows
    )
    all_generic = bool(exit_rows) and all(
        str(row.get("exit_reason") or "") in generic_reasons for row in exit_rows
    )
    reason_mismatches = sum(
        not str(row.get("exit_reason") or "").startswith(str(row.get("exit_variant") or ""))
        and not (
            row.get("exit_variant") == "trailing"
            and str(row.get("exit_reason") or "") == "trailing_stop"
        )
        for row in exit_rows
    )
    check(
        "exit_models_have_trigger_logic",
        missing_logic == 0 and not all_generic and reason_mismatches == 0,
        (
            f"missing={missing_logic}, all_generic={all_generic}, "
            f"reason_mismatches={reason_mismatches}"
        ),
        "0 missing/mismatched and model-specific reasons",
        "exit variants need parameters and model-specific trigger reasons",
    )

    required_features = (
        "trade_flow_10s",
        "trade_flow_180s",
        "trade_flow_300s",
        "trade_flow_900s",
        "data_age_ms",
    )
    feature_rows = tables["feature_snapshots"].to_pylist()
    wholly_null = [
        name
        for name in required_features
        if name not in tables["feature_snapshots"].column_names
        or all(row.get(name) is None for row in feature_rows)
    ]
    ready_missing = sum(
        bool(row.get("feature_ready")) and any(row.get(name) is None for name in required_features)
        for row in feature_rows
    )
    invalid_missing_markers = sum(
        any(row.get(name) is None for name in required_features)
        and (
            not str(row.get("missing_reason") or "").strip()
            or row.get("warmup_remaining") is None
            or not str(row.get("source_status") or "").strip()
        )
        for row in feature_rows
    )
    negative_age = sum((row.get("data_age_ms") or 0) < 0 for row in feature_rows)
    required_feature_null_fraction = {
        name: (
            sum(row.get(name) is None for row in feature_rows) / len(feature_rows)
            if feature_rows
            else 1.0
        )
        for name in required_features
    }
    check(
        "required_features_not_all_null",
        not wholly_null,
        wholly_null or "none",
        "no wholly-null required columns",
        "required trade flow and event-age features must be populated",
    )
    check(
        "feature_ready_has_no_required_nulls",
        ready_missing == 0,
        ready_missing,
        "0",
        "ready feature rows cannot contain required nulls",
    )
    check(
        "missing_features_have_markers",
        invalid_missing_markers == 0,
        invalid_missing_markers,
        "0",
        "missing features require reason, warmup and source status",
    )
    check(
        "feature_data_age_no_lookahead",
        negative_age == 0,
        negative_age,
        "0",
        "data_age_ms cannot be negative",
    )
    incomplete_warmup = sum(
        not bool(row.get("feature_ready")) or int(row.get("warmup_remaining") or 0) != 0
        for row in feature_rows
    )
    check(
        "candidate_features_warmup_complete",
        incomplete_warmup == 0,
        incomplete_warmup,
        "0",
        "the selected review window must contain only ready post-warmup features",
    )

    negative_excursions: dict[str, int] = {}
    for dataset, columns in (
        ("future_outcomes", ("mfe_ticks", "mae_ticks", "mfe_percent", "mae_percent")),
        ("shadow_stop_results", ("mfe_before_exit", "mae_before_exit")),
        ("shadow_exit_results", ("mfe_before_exit", "mae_before_exit")),
    ):
        count = sum(
            value is not None and float(value) < 0
            for column in columns
            if column in tables[dataset].column_names
            for value in tables[dataset][column].to_pylist()
        )
        negative_excursions[dataset] = count
    check(
        "mfe_mae_nonnegative",
        sum(negative_excursions.values()) == 0,
        negative_excursions,
        "0 negative values",
        "MFE and MAE are non-negative magnitudes",
    )
    mfe_values = [
        float(value)
        for column in ("mfe_ticks", "mfe_before_exit")
        for dataset in ("future_outcomes", "shadow_stop_results", "shadow_exit_results")
        if column in tables[dataset].column_names
        for value in tables[dataset][column].to_pylist()
        if value is not None
    ]
    mae_values = [
        float(value)
        for column in ("mae_ticks", "mae_before_exit")
        for dataset in ("future_outcomes", "shadow_stop_results", "shadow_exit_results")
        if column in tables[dataset].column_names
        for value in tables[dataset][column].to_pylist()
        if value is not None
    ]
    # In-memory secret scan catches token material before files are written.
    secret_datasets: list[str] = []
    for dataset, table in tables.items():
        body = table.to_pydict().__repr__().encode("utf-8", errors="ignore")
        if SECRET_PATTERN.search(body) or any(secret and secret in body for secret in secrets):
            secret_datasets.append(dataset)
    check(
        "secret_scan",
        not secret_datasets,
        secret_datasets or "none",
        "no secret material",
        "secret material such as tokens and credentials must never enter the bundle",
    )
    reconciliation = reconcile_review_tables(tables)
    for section in ("trade_flow", "source_ages", "outcomes", "stops"):
        payload = reconciliation[section]
        passed = (
            all(item.get("status") == "PASS" for item in payload.values())
            if section == "trade_flow"
            else not any(
                error.startswith(section.replace("source_ages", "source-age"))
                for error in reconciliation["errors"]
            )
        )
        check(
            f"independent_{section}_reconciliation",
            passed,
            payload,
            "100% reproduced; 0 critical mismatches",
            "independent archive-only recalculation must reproduce published values",
        )
    check(
        "independent_reconciliation",
        reconciliation["status"] == "PASS",
        reconciliation["errors"] or "no errors",
        "PASS",
        "trade flow, support, source ages, outcomes and stops must all reconcile",
    )
    errors.extend(error for error in reconciliation["errors"] if error not in errors)
    return {
        "status": "PASS" if not errors else "FAIL",
        "errors": errors,
        "pyarrow": "PASS",
        "duckdb": "PENDING",
        "zstd_test": "PENDING",
        "candidate_window_start": t0.isoformat(),
        "candidate_window_end": t1.isoformat(),
        "support_data_start": support_start.isoformat(),
        "support_data_end": support_end.isoformat(),
        "horizons": horizons,
        "stops": stops,
        "sides": sorted(sides),
        "decisions": sorted(str(v) for v in decisions),
        "duplicate_counts": duplicate_counts,
        "foreign_keys": fk_results,
        "gap_count": gap_count,
        "reconnect_count": reconnect_count,
        "candidate_reconnect_count": candidate_reconnect_count,
        "raw_coverage": raw_coverage,
        "checks": checks,
        "fill_status_counts_by_model": fill_status_counts,
        "minimum_holding_ms": min(
            (
                int(row["holding_ms"])
                for dataset in ("shadow_stop_results", "shadow_exit_results")
                for row in tables[dataset].to_pylist()
                if row.get("holding_ms") is not None
            ),
            default=None,
        ),
        "exit_before_entry_count": exit_before_entry,
        "exit_model_match_percent": exit_match_percent,
        "required_feature_null_fraction": required_feature_null_fraction,
        "mfe_range": [min(mfe_values), max(mfe_values)] if mfe_values else None,
        "mae_range": [min(mae_values), max(mae_values)] if mae_values else None,
        "reconciliation": reconciliation,
    }


def _build_manifest(
    work: Path,
    tables: dict[str, pa.Table],
    t0: datetime,
    t1: datetime,
    support_start: datetime,
    support_end: datetime,
    instrument: str,
    validation: dict[str, Any],
) -> dict[str, Any]:
    if validation["status"] != "PASS":
        raise ArchiveValidationError("review validation failed: " + "; ".join(validation["errors"]))
    files: list[dict[str, Any]] = []
    exchange_values: list[datetime] = []
    receive_values: list[datetime] = []
    code_commits: set[str] = set()
    config_hashes: set[str] = set()
    collector_instances: set[str] = set()
    quality_flags: dict[str, int] = {}
    for dataset, table in tables.items():
        path = work / "data" / f"{dataset}.parquet"
        nulls = {
            name: (table[name].null_count / table.num_rows if table.num_rows else 0.0)
            for name in table.column_names
        }
        timestamp_values: list[datetime] = []
        for column, timestamp_target in (
            ("exchange_ts", exchange_values),
            ("receive_ts", receive_values),
        ):
            if column in table.column_names and table.num_rows:
                timestamp_target.extend(
                    _utc(value) for value in table[column].to_pylist() if value is not None
                )
        for column, text_target in (
            ("code_commit", code_commits),
            ("config_hash", config_hashes),
            ("collector_instance_id", collector_instances),
        ):
            if column in table.column_names:
                text_target.update(str(value) for value in table[column].to_pylist() if value)
        if "data_quality_flags" in table.column_names:
            for values in table["data_quality_flags"].to_pylist():
                for flag in values or []:
                    quality_flags[str(flag)] = quality_flags.get(str(flag), 0) + 1
        for name in TIMESTAMP_COLUMNS:
            if name in table.column_names and table.num_rows:
                minimum, maximum = pc.min_max(table[name]).as_py().values()
                if minimum is not None:
                    timestamp_values.extend((_utc(minimum), _utc(maximum)))
        files.append(
            {
                "path": f"data/{dataset}.parquet",
                "dataset": dataset,
                "schema_version": "v3",
                "row_count": table.num_rows,
                "min_timestamp": min(timestamp_values).isoformat() if timestamp_values else None,
                "max_timestamp": max(timestamp_values).isoformat() if timestamp_values else None,
                "bytes": path.stat().st_size,
                "sha256": _sha256(path),
                "columns": table.column_names,
                "null_fraction": nulls,
                "duplicate_count": validation["duplicate_counts"].get(dataset, 0),
                "primary_key": PRIMARY_KEYS.get(dataset),
                "foreign_keys": [list(item) for item in FOREIGN_KEYS.get(dataset, ())],
            }
        )
    reconciliation = validation["reconciliation"]
    raw_support = reconciliation["raw_support"]
    return {
        "schema_version": "v3",
        "instrument": instrument,
        "candidate_window_start": t0.isoformat(),
        "candidate_window_end": t1.isoformat(),
        "outcome_support_end": support_end.isoformat(),
        "support_data_start": support_start.isoformat(),
        "support_data_end": support_end.isoformat(),
        "max_actual_fill_ts": raw_support["max_actual_fill_ts"],
        "max_theoretical_entry_ts": raw_support["max_theoretical_entry_ts"],
        "max_required_entry_ts": raw_support["max_required_entry_ts"],
        "required_support_end": raw_support["required_support_end"],
        "actual_support_end": raw_support["actual_support_end"],
        "trade_flow_reconciliation_summary": reconciliation["trade_flow"],
        "source_age_reconciliation_summary": reconciliation["source_ages"],
        "outcome_source_reconciliation_summary": reconciliation["outcomes"],
        "stop_trigger_reconciliation_summary": reconciliation["stops"],
        "unexplained_identical_stop_count": reconciliation["stops"]["unexplained_identical_count"],
        "raw_orderbook_coverage_start": validation["raw_coverage"]["raw_orderbook"]["start"],
        "raw_orderbook_coverage_end": validation["raw_coverage"]["raw_orderbook"]["end"],
        "raw_trades_coverage_start": validation["raw_coverage"]["raw_trades"]["start"],
        "raw_trades_coverage_end": validation["raw_coverage"]["raw_trades"]["end"],
        "raw_last_price_coverage_start": validation["raw_coverage"]["raw_last_price"]["start"],
        "raw_last_price_coverage_end": validation["raw_coverage"]["raw_last_price"]["end"],
        "exchange_timestamp_min": min(exchange_values).isoformat(),
        "exchange_timestamp_max": max(exchange_values).isoformat(),
        "receive_timestamp_min": min(receive_values).isoformat(),
        "receive_timestamp_max": max(receive_values).isoformat(),
        "code_commits": sorted(code_commits),
        "config_hashes": sorted(config_hashes),
        "collector_instances": sorted(collector_instances),
        "file_count": 20,
        "dataset_file_count": 15,
        "rows_by_dataset": {name: table.num_rows for name, table in tables.items()},
        "bytes_by_dataset": {
            item["dataset"]: item["bytes"] for item in files if item.get("dataset")
        },
        "reconnect_count": validation["reconnect_count"],
        "candidate_reconnect_count": validation["candidate_reconnect_count"],
        "gap_count": validation["gap_count"],
        "skipped_identical_orderbooks": sum(
            bool(value) for value in tables["raw_orderbook"]["is_duplicate_snapshot"].to_pylist()
        ),
        "data_quality_summary": quality_flags,
        "duplicate_counts": validation["duplicate_counts"],
        "foreign_key_results": validation["foreign_keys"],
        "horizons": validation["horizons"],
        "stop_ticks": validation["stops"],
        "sides": validation["sides"],
        "decisions": validation["decisions"],
        "shadow_stop_simulations": tables["shadow_stop_results"].num_rows,
        "shadow_exit_simulations": tables["shadow_exit_results"].num_rows,
        "validation": "PASS",
        "validation_checks": validation["checks"],
        "fill_status_counts_by_model": validation["fill_status_counts_by_model"],
        "minimum_holding_ms": validation["minimum_holding_ms"],
        "exit_before_entry_count": validation["exit_before_entry_count"],
        "exit_model_match_percent": validation["exit_model_match_percent"],
        "required_feature_null_fraction": validation["required_feature_null_fraction"],
        "mfe_range": validation["mfe_range"],
        "mae_range": validation["mae_range"],
        "files": files,
    }


def _verify_tar(archive: Path, manifest: dict[str, Any], secrets: list[bytes]) -> None:
    with tempfile.TemporaryDirectory(prefix="review-verify-", dir=archive.parent) as directory:
        root = Path(directory)
        with (
            archive.open("rb") as raw,
            zstandard.ZstdDecompressor().stream_reader(raw) as stream,
            tarfile.open(fileobj=stream, mode="r|") as tar,
        ):
            members = []
            for member in tar:
                pure = PurePosixPath(member.name)
                if pure.is_absolute() or ".." in pure.parts:
                    raise ArchiveValidationError("unsafe TAR path")
                members.append(member.name)
                tar.extract(member, root, filter="data")
        if len(members) != 20 or len(set(members)) != 20:
            raise ArchiveValidationError("archive must contain 20 unique members")
        _scan_files([path for path in root.rglob("*") if path.is_file()], secrets)
        for item in manifest["files"]:
            path = root / item["path"]
            if _sha256(path) != item["sha256"]:
                raise ArchiveValidationError(f"digest mismatch: {item['path']}")
            if path.suffix != ".parquet":
                continue
            if pq.read_table(path).num_rows != item["row_count"]:
                raise ArchiveValidationError("PyArrow row mismatch")
            with duckdb.connect(":memory:") as connection:
                row = connection.execute(
                    "SELECT count(*) FROM read_parquet(?)", [str(path)]
                ).fetchone()
                if row is None:
                    raise ArchiveValidationError("DuckDB returned no row count")
                count = row[0]
            if count != item["row_count"]:
                raise ArchiveValidationError("DuckDB row mismatch")


def _write_tar_zst(target: Path, root: Path, members: list[Path]) -> None:
    with (
        target.open("xb") as raw,
        zstandard.ZstdCompressor(level=9).stream_writer(raw, closefd=False) as stream,
        tarfile.open(fileobj=stream, mode="w|") as tar,
    ):
        for path in members:
            tar.add(path, arcname=path.relative_to(root).as_posix(), recursive=False)


def _schema_dictionary(tables: dict[str, pa.Table]) -> str:
    lines = [
        "# Schema dictionary",
        "",
        "All timestamps use `timestamp[us, UTC]`. Schema version: `v3`.",
        "",
        "`trade_flow_<N>s` is signed traded volume (aggressive BUY volume minus "
        "aggressive SELL volume) from events in `(feature_ts-Ns, feature_ts]`; future "
        "events are excluded. `data_age_ms` is feature timestamp minus the newest "
        "market-event timestamp used by the feature.",
        "",
        "Outcome prices use the last order-book event at or before `target_ts`. "
        "LONG exits use executable bid and SHORT exits use executable ask; the source "
        "event ID and both exchange/receive timestamps are retained. Mid price is "
        "reported separately and is never silently substituted for executable PnL.",
        "",
        "MFE and MAE fields are non-negative magnitudes. MFE is the maximum favourable "
        "move; MAE is the absolute maximum adverse move, side-adjusted for LONG/SHORT.",
        "",
    ]
    for dataset, table in tables.items():
        lines.extend((f"## {dataset}", "", "| Column | Arrow type |", "|---|---|"))
        lines.extend(f"| `{field.name}` | `{field.type}` |" for field in table.schema)
        lines.append("")
    return "\n".join(lines)


def _readme(t0: datetime, t1: datetime, start: datetime, end: datetime) -> str:
    return (
        "# Neobitcoin review bundle\n\n"
        f"Candidate window: `{t0.isoformat()}` - `{t1.isoformat()}`.\n\n"
        f"Supporting data: `{start.isoformat()}` - `{end.isoformat()}`.\n"
    )


def _manifest_md(manifest: dict[str, Any]) -> str:
    lines = [
        "# Manifest",
        "",
        f"- Validation: `{manifest['validation']}`",
        f"- Files: {manifest['file_count']}",
        "",
        "| Dataset | Rows |",
        "|---|---:|",
    ]
    lines.extend(f"| {name} | {rows} |" for name, rows in manifest["rows_by_dataset"].items())
    return "\n".join(lines) + "\n"


def _validation_report(validation: dict[str, Any], tables: dict[str, pa.Table]) -> str:
    def cell(value: Any) -> str:
        return str(value).replace("|", "\\|").replace("\n", " ")

    lines = [
        "# Validation report",
        "",
        f"- Overall: `{validation['status']}`",
        "- PyArrow: `PASS`",
        "- DuckDB: `PASS`",
        "- zstd stream test: `PASS`",
        "- Secret scan: `PASS`",
        f"- Files: 20; datasets: {len(tables)}",
        "",
        "| Check | Status | Measured value | Threshold | Explanation |",
        "|---|---|---|---|---|",
    ]
    lines.extend(
        "| {check} | **{status}** | {measured} | {threshold} | {explanation} |".format(
            check=cell(item["check"]),
            status=cell(item["status"]),
            measured=cell(item["measured_value"]),
            threshold=cell(item["threshold"]),
            explanation=cell(item["explanation"]),
        )
        for item in validation["checks"]
    )
    reconciliation = validation["reconciliation"]
    lines.extend(
        (
            "",
            "## Trade flow",
            "",
            "| Window | Checked | Matched | Mismatched | Maximum error |",
            "|---|---:|---:|---:|---:|",
        )
    )
    for window, item in reconciliation["trade_flow"].items():
        lines.append(
            f"| {window} | {item['checked']} | {item['matched']} | "
            f"{item['mismatched']} | {item['max_error']} |"
        )
    support = reconciliation["raw_support"]
    lines.extend(
        (
            "",
            "## Raw support",
            "",
            f"- Candidate end: `{validation['candidate_window_end']}`",
            f"- Maximum fill timestamp: `{support['max_actual_fill_ts']}`",
            f"- Required support end: `{support['required_support_end']}`",
            f"- Actual orderbook end: `{support['actual_support_end']['raw_orderbook']}`",
            f"- Actual trades end: `{support['actual_support_end']['raw_trades']}`",
            f"- Actual last-price end: `{support['actual_support_end']['raw_last_price']}`",
            "",
            "## Source ages",
            "",
            "| Age | Null fraction | Min | Median | p95 | Max | Reconciled |",
            "|---|---:|---:|---:|---:|---:|---:|",
        )
    )
    for name, item in reconciliation["source_ages"].items():
        if "null_fraction" not in item:
            continue
        lines.append(
            f"| {name} | {item.get('null_fraction')} | {item.get('min')} | "
            f"{item.get('median')} | {item.get('p95')} | {item.get('max')} | "
            f"{item.get('reconciled')} |"
        )
    outcome = reconciliation["outcomes"]
    stop = reconciliation["stops"]
    lines.extend(
        (
            "",
            "## Outcomes",
            "",
            f"- Complete: {outcome['complete_count']}",
            f"- Source IDs present: {outcome['source_ids_present']}",
            f"- Reproduced prices: {outcome['reproduced_prices']}",
            f"- Reproduced PnL: {outcome['reproduced_pnl']}",
            f"- Outside support: {outcome['outside_support_count']}",
            "",
            "## Stops",
            "",
            f"- Triggered: {stop['triggered_count']}",
            f"- Source IDs present: {stop['source_ids_present']}",
            f"- Reproduced: {stop['reproduced_count']}",
            f"- Identical results: {stop['identical_result_count']}",
            f"- Gap-explained identical: {stop['gap_explained_identical_count']}",
            f"- Unexplained identical: {stop['unexplained_identical_count']}",
            f"- Trigger examples: {', '.join(stop['trigger_examples']) or 'none'}",
        )
    )
    lines.extend(
        (
            "",
            f"Foreign keys: `PASS` ({len(validation['foreign_keys'])} constraints).",
            f"Duplicates: {sum(validation['duplicate_counts'].values())}; "
            f"gaps: {validation['gap_count']}; reconnects: {validation['reconnect_count']}.",
            "",
        )
    )
    return "\n".join(lines)


def _scan_files(paths: list[Path], secrets: list[bytes]) -> None:
    for path in paths:
        if re.search(r"(?:token|secret|password|credential|\.env)", path.name, re.I):
            raise ArchiveValidationError(f"sensitive filename: {path.name}")
        body = path.read_bytes()
        if SECRET_PATTERN.search(body) or any(secret and secret in body for secret in secrets):
            raise ArchiveValidationError(f"secret material found: {path.name}")


def _secret_values(paths: tuple[Path | str, ...]) -> list[bytes]:
    values: list[bytes] = []
    for value in paths:
        path = Path(value)
        if path.is_file():
            values.extend(
                line.strip() for line in path.read_bytes().splitlines() if len(line.strip()) >= 12
            )
    return values


def _atomic_text(path: Path, body: str) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(body, encoding="utf-8", newline="\n")
    os.replace(temporary, path)


def _sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ArchiveValidationError("naive timestamp is forbidden")
    return value.astimezone(UTC)


def _timestamp_value(value: Any) -> datetime:
    if isinstance(value, datetime):
        return _utc(value)
    if isinstance(value, str):
        return _utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    raise ArchiveValidationError(f"invalid timestamp value: {value!r}")


def _name_ts(value: datetime) -> str:
    return _utc(value).strftime("%Y%m%dT%H%M%SZ")


__all__ = ["DATASETS", "HORIZONS", "ReviewBundleResult", "create_neobitcoin_review_bundle"]
