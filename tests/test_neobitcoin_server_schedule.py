from __future__ import annotations

import json
import zipfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from neo_trader.neobitcoin_research.config import ResearchConfig
from scripts.create_neobitcoin_session_archive import (
    RAW_MEMBER,
    create_session_archive,
    verify_session_archive,
)
from scripts.neobitcoin_server_schedule import (
    cache_is_fresh,
    fallback_session,
    sessions_from_api,
    should_collect,
)


def test_fallback_schedule_matches_spb_future_hours() -> None:
    weekday = fallback_session(date(2026, 7, 15))
    weekend = fallback_session(date(2026, 7, 18))
    assert weekday.start.isoformat() == "2026-07-15T07:00:00+03:00"
    assert weekday.end.isoformat() == "2026-07-16T00:00:00+03:00"
    assert weekend.start.isoformat() == "2026-07-18T10:00:00+03:00"
    assert weekend.end.isoformat() == "2026-07-19T00:00:00+03:00"


def test_server_can_disable_expensive_full_compaction_on_shutdown() -> None:
    config = ResearchConfig.from_env({"NEOBITCOIN_RESEARCH_COMPACT_ON_SHUTDOWN": "false"})
    assert config.compact_closed_hours is False


def test_api_schedule_takes_exact_instrument_hours() -> None:
    schedules = [
        {
            "exchange": "SPB_FUTURE",
            "days": [
                {
                    "date": "2026-07-15T00:00:00Z",
                    "isTradingDay": True,
                    "startTime": "2026-07-15T04:00:00Z",
                    "endTime": "2026-07-15T21:00:00Z",
                }
            ],
        }
    ]
    session = sessions_from_api(schedules)["2026-07-15"]
    assert session.start.isoformat() == "2026-07-15T07:00:00+03:00"
    assert session.end.isoformat() == "2026-07-16T00:00:00+03:00"
    assert session.source == "tbank_schedule"


def test_schedule_cache_has_a_bounded_refresh_period(tmp_path: Path) -> None:
    now = datetime(2026, 7, 15, 4, 0, tzinfo=UTC)
    cache = tmp_path / "schedule.json"
    cache.write_text(json.dumps({"updated_at": now.isoformat()}), encoding="utf-8")
    assert cache_is_fresh(cache, now + timedelta(minutes=29), maximum_age=timedelta(minutes=30))
    assert not cache_is_fresh(cache, now + timedelta(minutes=31), maximum_age=timedelta(minutes=30))


def test_collection_warms_up_and_preserves_the_final_timestamp() -> None:
    session = fallback_session(date(2026, 7, 15))
    next_session = fallback_session(date(2026, 7, 16))
    assert not should_collect(session.start - timedelta(minutes=3), session)
    assert should_collect(session.start - timedelta(minutes=2), session)
    assert should_collect(session.end - timedelta(microseconds=1), session)
    assert should_collect(session.end, next_session, session)
    assert not should_collect(session.end + timedelta(minutes=1), next_session, session)


def test_session_archive_contains_only_validated_raw_jsonl(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    raw = data_root / "raw" / "uid" / "2026-07-15" / "04" / "events.jsonl"
    raw.parent.mkdir(parents=True)
    records = [
        _record("before", "orderbook", "2026-07-15T03:59:59+00:00"),
        _record("one", "subscription_ack", "2026-07-15T04:00:00+00:00"),
        _record("two", "orderbook", "2026-07-15T04:00:01+00:00"),
    ]
    end_raw = data_root / "raw" / "uid" / "2026-07-15" / "20" / "events.jsonl"
    end_raw.parent.mkdir(parents=True)
    raw.write_text("".join(json.dumps(row) + "\n" for row in records), encoding="utf-8")
    end_raw.write_text(
        json.dumps(_record("three", "trade", "2026-07-15T20:59:59+00:00")) + "\n",
        encoding="utf-8",
    )
    (data_root / "state.sqlite").write_bytes(b"not a consistent database")
    (data_root / "active.parquet.inprogress").write_bytes(b"partial")

    session = fallback_session(date(2026, 7, 15))
    result = create_session_archive(
        data_root,
        tmp_path / "out",
        session,
        now=datetime(2026, 7, 16, 0, 6, tzinfo=session.start.tzinfo),
    )
    archive_path = Path(str(result["archive"]))
    manifest = verify_session_archive(archive_path)
    assert manifest["validation"] == "PASS"
    assert manifest["rows"] == 3
    assert manifest["market_rows"] == 2
    with zipfile.ZipFile(archive_path) as archive:
        assert archive.namelist() == [RAW_MEMBER, "MANIFEST.json", "README.md"]
        body = archive.read(RAW_MEMBER)
        assert b'"before"' not in body
        assert b'"three"' in body


def _record(event_id: str, event_type: str, timestamp: str) -> dict[str, object]:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "receive_timestamp": timestamp,
        "exchange_timestamp": timestamp,
        "payload": {},
    }
