from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from scripts.generate_golden_real_slice import generate


def test_real_candle_audit_contains_ten_full_lineage_timestamps(tmp_path: Path) -> None:
    base = datetime(2026, 7, 11, 10, 0, tzinfo=UTC)
    candles: dict[int, list[dict[str, object]]] = {}
    for minutes in (1, 5, 15):
        candles[minutes] = [
            {
                "event_id": f"candle-{minutes}",
                "candle_start": base - timedelta(minutes=minutes),
                "candle_end": base,
                "receive_ts": base - timedelta(milliseconds=100),
                "revision": 2,
                "is_complete": False,
            }
        ]
        pq.write_table(
            pa.Table.from_pylist(candles[minutes]),
            tmp_path / f"candles_{minutes}m.parquet",
        )
    features = []
    for index in range(10):
        timestamp = base + timedelta(seconds=index)
        row: dict[str, object] = {
            "event_id": f"feature-{index}",
            "feature_snapshot_id": f"feature-{index}",
            "feature_ts": timestamp,
            "exchange_ts": timestamp,
            "feature_ready": True,
            "missing_fields": [],
            "readiness_checks_passed": 99,
            "readiness_checks_total": 99,
            "readiness_version": "schema-v4.1",
            "trend": "up",
            "regime": "up",
            "ofi": 2.0,
            "ofi_delta": 1.0,
            "ofi_source_event_id": f"book-{index}",
            "ofi_previous_event_id": f"book-{index - 1}",
            "ofi_continuity_valid": True,
            "spread_price": 0.3,
            "tick_size": 0.1,
            "spread_ticks_decimal": 3.000000000029,
            "spread_ticks_int": 3,
            "tick_grid_error": 0.000000000029,
            "spread_threshold_ticks": 3,
            "spread_gate_passed": True,
        }
        for minutes in (1, 5, 15):
            prefix = f"candle_{minutes}m"
            row.update(
                {
                    f"{prefix}_source_event_id": f"candle-{minutes}",
                    f"{prefix}_source_is_complete": False,
                    f"{prefix}_canonical_is_complete": True,
                    f"{prefix}_start": base - timedelta(minutes=minutes),
                    f"{prefix}_end": base,
                    f"{prefix}_revision": 2,
                    f"{prefix}_revision_receive_ts": base - timedelta(milliseconds=100),
                    f"{prefix}_interval_age_ms": float(index * 1000),
                    f"{prefix}_receive_age_ms": float(index * 1000 + 100),
                    f"{prefix}_stale": False,
                    f"candle_volume_{minutes}m": 42.0,
                    f"return_{minutes}m": 0.01,
                    f"atr_{minutes}m": 3.0,
                }
            )
        features.append(row)
    pq.write_table(pa.Table.from_pylist(features), tmp_path / "feature_snapshots.parquet")
    report = tmp_path / "golden_real_candle_audit.md"
    generate(tmp_path, report, count=10)
    text = report.read_text(encoding="utf-8")
    assert sum(line.startswith("## ") for line in text.splitlines()) == 10
    assert text.count("### 1m candle") == 10
    assert "Source/canonical complete: False / True" in text
    assert "decimal=3.000000000029; integer=3" in text
    assert "previous=`book-" in text
    assert "Readiness: True; checks=99/99; version=schema-v4.1" in text
