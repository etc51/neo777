"""Quality report writer for readonly market-data recording runs."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from neo_trader.data.market_data_recorder import MarketDataRecorderResult


@dataclass(frozen=True)
class RecordingQualityReport:
    """Serializable summary of one recorder run."""

    commit_hash: str
    mode: str
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    events_recorded: int
    reconnects: int
    instruments: tuple[str, ...]
    event_counts_by_instrument_and_type: Mapping[str, Mapping[str, int]]
    stale_seconds: Mapping[str, Mapping[str, float]]
    gaps: Mapping[str, Mapping[str, int]]
    output_path: str
    dashboard_state_path: str
    safety_flags: Mapping[str, str]

    def to_json_dict(self) -> dict[str, object]:
        """Return a JSON-compatible report payload."""

        return {
            "commit_hash": self.commit_hash,
            "mode": self.mode,
            "started_at": _as_utc(self.started_at).isoformat(),
            "finished_at": _as_utc(self.finished_at).isoformat(),
            "duration_seconds": self.duration_seconds,
            "events_recorded": self.events_recorded,
            "reconnects": self.reconnects,
            "instruments": list(self.instruments),
            "event_counts_by_instrument_and_type": self.event_counts_by_instrument_and_type,
            "stale_seconds": self.stale_seconds,
            "gaps": self.gaps,
            "output_path": self.output_path,
            "dashboard_state_path": self.dashboard_state_path,
            "safety_flags": dict(self.safety_flags),
        }


def build_recording_quality_report(
    *,
    mode: str,
    started_at: datetime,
    finished_at: datetime,
    result: MarketDataRecorderResult,
    instruments: Sequence[str],
    output_path: Path | str,
    dashboard_state_path: Path | str,
    safety_flags: Mapping[str, str],
) -> RecordingQualityReport:
    """Build a quality report from recorder result counters."""

    counts: dict[str, dict[str, int]] = {}
    stale: dict[str, dict[str, float]] = {}
    gaps: dict[str, dict[str, int]] = {}
    for snapshot in result.quality:
        event_type = snapshot.event_type.value
        counts.setdefault(snapshot.instrument_uid, {})[event_type] = snapshot.events_total
        stale.setdefault(snapshot.instrument_uid, {})[event_type] = snapshot.stale_seconds
        gaps.setdefault(snapshot.instrument_uid, {})[event_type] = snapshot.gaps

    return RecordingQualityReport(
        commit_hash=result.commit_hash,
        mode=mode,
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=max((_as_utc(finished_at) - _as_utc(started_at)).total_seconds(), 0.0),
        events_recorded=result.events_recorded,
        reconnects=result.reconnects,
        instruments=tuple(instruments),
        event_counts_by_instrument_and_type=counts,
        stale_seconds=stale,
        gaps=gaps,
        output_path=str(output_path),
        dashboard_state_path=str(dashboard_state_path),
        safety_flags=dict(safety_flags),
    )


def write_recording_quality_report(
    report: RecordingQualityReport,
    *,
    reports_dir: Path | str = Path("data/reports"),
) -> Path:
    """Atomically write ``recording_quality_YYYYMMDD_HHMMSS.json``."""

    directory = Path(reports_dir)
    directory.mkdir(parents=True, exist_ok=True)
    timestamp = _as_utc(report.finished_at).strftime("%Y%m%d_%H%M%S")
    path = directory / f"recording_quality_{timestamp}.json"
    _atomic_write_json(path, report.to_json_dict())
    return path


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    tmp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        tmp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


__all__ = [
    "RecordingQualityReport",
    "build_recording_quality_report",
    "write_recording_quality_report",
]
