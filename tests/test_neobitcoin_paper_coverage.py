from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from neobitcoin_paper.coverage import SessionClassification, classify_session
from neobitcoin_paper.datasets import DATASET_SCHEMAS, REQUIRED_DATASETS


def _datasets(root: Path, quality_rows: list[dict[str, object]]) -> Path:
    root.mkdir(parents=True)
    for name in REQUIRED_DATASETS:
        rows: list[dict[str, object]] = []
        if name == "data_quality_events.parquet":
            rows = quality_rows
        elif name == "strategy_evaluations.parquet":
            rows = [
                {
                    "evaluation_id": "eval-1",
                    "session_date": date(2026, 7, 18),
                    "event_ts": datetime(2026, 7, 18, 7, tzinfo=UTC),
                }
            ]
        table = pa.Table.from_pylist(rows, schema=DATASET_SCHEMAS[name])
        pq.write_table(table, root / name, compression="zstd")
    return root


def test_five_minute_structural_archive_is_invalid_coverage(tmp_path: Path) -> None:
    start = datetime(2026, 7, 18, 7, tzinfo=UTC)
    rows = [
        {
            "event_id": f"quality-{second}",
            "session_date": date(2026, 7, 18),
            "event_ts": start + timedelta(seconds=second),
            "feature_ready": False,
            "gap_status": "ORDERBOOK_GAP",
        }
        for second in range(0, 301, 30)
    ]
    report = classify_session(
        _datasets(tmp_path / "data", rows),
        start_utc=start,
        end_utc=start + timedelta(hours=14),
    )
    assert report["classification"] == SessionClassification.INVALID_DATA_COVERAGE
    assert report["oos_included"] is False
    assert report["investigation_required"] is True


def test_complete_feature_ready_zero_signal_day_is_valid_no_trade(tmp_path: Path) -> None:
    start = datetime(2026, 7, 18, 7, tzinfo=UTC)
    end = start + timedelta(hours=1)
    rows = [
        {
            "event_id": f"quality-{second}",
            "session_date": date(2026, 7, 18),
            "event_ts": start + timedelta(seconds=second),
            "feature_ready": True,
            "gap_status": "OK",
        }
        for second in range(0, 3601, 30)
    ]
    report = classify_session(_datasets(tmp_path / "data", rows), start_utc=start, end_utc=end)
    assert report["classification"] == SessionClassification.VALID_NO_TRADE_SESSION
    assert report["oos_included"] is True
    assert report["investigation_required"] is False


def test_sixty_second_maintenance_cadence_allows_scheduler_jitter(tmp_path: Path) -> None:
    start = datetime(2026, 7, 18, 7, tzinfo=UTC)
    end = start + timedelta(minutes=10)
    rows = [
        {
            "event_id": f"quality-{index}",
            "session_date": date(2026, 7, 18),
            "event_ts": start + timedelta(seconds=index * 60.05),
            "feature_ready": True,
            "gap_status": "OK",
        }
        for index in range(11)
    ]

    report = classify_session(_datasets(tmp_path / "data", rows), start_utc=start, end_utc=end)

    assert report["classification"] == SessionClassification.VALID_NO_TRADE_SESSION
    assert report["longest_observed_gap_seconds"] == pytest.approx(60.05)
