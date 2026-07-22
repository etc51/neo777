"""Fail-closed daily paper coverage classification.

This is operational evidence only; it does not alter strategy inputs or the
fixed twenty-Parquet trading dataset contract.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq  # type: ignore[import-untyped]


class SessionClassification(StrEnum):
    VALID_OOS_SESSION = "VALID_OOS_SESSION"
    VALID_NO_TRADE_SESSION = "VALID_NO_TRADE_SESSION"
    INVALID_DATA_COVERAGE = "INVALID_DATA_COVERAGE"
    VALID_MARKET_CLOSED_SESSION = "VALID_MARKET_CLOSED_SESSION"


_CLOSED = {"NOT_AVAILABLE_FOR_TRADING", "DEALER_NOT_AVAILABLE_FOR_TRADING", "CLOSED"}
_MAX_SCHEDULED_EVENT_GAP_SECONDS = 61.0


def classify_session(
    parquet_dir: Path,
    *,
    start_utc: datetime,
    end_utc: datetime,
    test_archive: bool = False,
) -> dict[str, Any]:
    quality = pq.read_table(
        parquet_dir / "data_quality_events.parquet",
        columns=["event_ts", "feature_ready", "gap_status"],
    ).to_pylist()
    statuses = pq.read_table(
        parquet_dir / "market_status_events.parquet",
        columns=["event_ts", "trading_status"],
    ).to_pylist()
    evaluations = pq.ParquetFile(parquet_dir / "strategy_evaluations.parquet").metadata.num_rows
    signals = pq.ParquetFile(parquet_dir / "candidate_signals.parquet").metadata.num_rows
    trades = pq.ParquetFile(parquet_dir / "paper_trades.parquet").metadata.num_rows
    timestamps = sorted(
        row["event_ts"].astimezone(UTC) for row in quality if row.get("event_ts") is not None
    )
    expected = max(1.0, (end_utc - start_utc).total_seconds())
    observed_span = 0.0 if len(timestamps) < 2 else (timestamps[-1] - timestamps[0]).total_seconds()
    coverage_ratio = min(1.0, max(0.0, observed_span / expected))
    max_gap = max(
        (
            (right - left).total_seconds()
            for left, right in zip(timestamps, timestamps[1:], strict=False)
        ),
        default=expected,
    )
    ready_values = [bool(row.get("feature_ready")) for row in quality]
    ready_ratio = sum(ready_values) / len(ready_values) if ready_values else 0.0
    gap_events = sum(1 for row in quality if row.get("gap_status") not in {None, "OK"})
    status_values = {str(row.get("trading_status") or "UNKNOWN") for row in statuses}
    closed_only = bool(status_values) and status_values.issubset(_CLOSED)
    # The SDK emits closed/quiet-stream maintenance traffic on an approximate
    # 60-second cadence.  One second of scheduler tolerance prevents normal
    # millisecond jitter from invalidating an otherwise continuous full day;
    # explicit unresolved gap events remain fail-closed below.
    adequate_transport = (
        coverage_ratio >= 0.95 and max_gap <= _MAX_SCHEDULED_EVENT_GAP_SECONDS
    )
    adequate_features = ready_ratio >= 0.90 and gap_events == 0
    reasons: list[str] = []
    if test_archive:
        classification = SessionClassification.INVALID_DATA_COVERAGE
        reasons.append("TEST_ARCHIVE_EXCLUDED")
    elif closed_only and adequate_transport:
        classification = SessionClassification.VALID_MARKET_CLOSED_SESSION
    elif not adequate_transport or not adequate_features:
        classification = SessionClassification.INVALID_DATA_COVERAGE
        if coverage_ratio < 0.95:
            reasons.append("TEMPORAL_COVERAGE_BELOW_95_PERCENT")
        if max_gap > _MAX_SCHEDULED_EVENT_GAP_SECONDS:
            reasons.append("LONGEST_OBSERVED_GAP_EXCEEDS_61_SECONDS")
        if ready_ratio < 0.90:
            reasons.append("FEATURE_READY_BELOW_90_PERCENT")
        if gap_events:
            reasons.append("UNRESOLVED_GAP_EVENTS")
    elif signals or trades:
        classification = SessionClassification.VALID_OOS_SESSION
    elif evaluations:
        classification = SessionClassification.VALID_NO_TRADE_SESSION
    else:
        classification = SessionClassification.INVALID_DATA_COVERAGE
        reasons.append("NO_STRATEGY_EVALUATIONS")
    oos_included = classification in {
        SessionClassification.VALID_OOS_SESSION,
        SessionClassification.VALID_NO_TRADE_SESSION,
    }
    report: dict[str, Any] = {
        "coverage_schema_version": 1,
        "classification": classification.value,
        "oos_included": oos_included,
        "investigation_required": classification is SessionClassification.INVALID_DATA_COVERAGE,
        "reasons": reasons,
        "expected_start_utc": start_utc.astimezone(UTC).isoformat(),
        "expected_end_utc": end_utc.astimezone(UTC).isoformat(),
        "observed_span_seconds": observed_span,
        "temporal_coverage_ratio": coverage_ratio,
        "longest_observed_gap_seconds": max_gap,
        "feature_ready_ratio": ready_ratio,
        "unresolved_gap_events": gap_events,
        "status_values": sorted(status_values),
        "evaluation_count": evaluations,
        "signal_count": signals,
        "trade_count": trades,
    }
    canonical = json.dumps(report, sort_keys=True, separators=(",", ":")).encode()
    report["report_sha256"] = hashlib.sha256(canonical).hexdigest()
    return report


def verify_coverage_report(report: dict[str, Any]) -> bool:
    expected = report.get("report_sha256")
    body = dict(report)
    body.pop("report_sha256", None)
    actual = hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return isinstance(expected, str) and expected == actual
