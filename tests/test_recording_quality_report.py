"""Tests for readonly recording quality reports."""

import json
from datetime import UTC, datetime
from pathlib import Path

from neo_trader.data.market_data_recorder import (
    MarketDataEventType,
    MarketDataQualitySnapshot,
    MarketDataRecorderResult,
)
from neo_trader.data.recording_quality_report import (
    build_recording_quality_report,
    write_recording_quality_report,
)


def test_recording_quality_report_is_written_atomically(tmp_path: Path) -> None:
    started = datetime(2026, 7, 6, 10, 0, tzinfo=UTC)
    finished = datetime(2026, 7, 6, 10, 0, 5, tzinfo=UTC)
    result = MarketDataRecorderResult(
        events_recorded=3,
        reconnects=1,
        quality=(
            MarketDataQualitySnapshot(
                instrument_uid="MOCK-SBER-TQBR",
                event_type=MarketDataEventType.ORDERBOOK,
                events_total=3,
                events_per_second=1.0,
                stale_seconds=0.25,
                gaps=0,
                last_event_at=finished,
                last_heartbeat_at=finished,
            ),
        ),
        commit_hash="abc1234",
    )
    report = build_recording_quality_report(
        mode="mock",
        started_at=started,
        finished_at=finished,
        result=result,
        instruments=["MOCK-SBER-TQBR"],
        output_path=tmp_path / "raw",
        dashboard_state_path=tmp_path / "dashboard_state.json",
        safety_flags={"TRADING_MODE": "readonly", "LIVE_TRADING_ENABLED": "false"},
    )

    path = write_recording_quality_report(report, reports_dir=tmp_path / "reports")
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert path.name == "recording_quality_20260706_100005.json"
    assert payload["commit_hash"] == "abc1234"
    assert payload["mode"] == "mock"
    assert payload["events_recorded"] == 3
    assert payload["event_counts_by_instrument_and_type"]["MOCK-SBER-TQBR"]["orderbook"] == 3
    assert not list((tmp_path / "reports").glob("*.tmp"))
