"""Tests for durable Neobitcoin research storage and daily reporting."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

from neo_trader.neobitcoin_research import storage as storage_module
from neo_trader.neobitcoin_research.reporting import generate_daily_research_report
from neo_trader.neobitcoin_research.storage import (
    DERIVED_DATASETS,
    InvalidRecordError,
    ResearchStorage,
)

BASE = datetime(2026, 7, 10, 10, 15, tzinfo=UTC)
UID = "uid-neobitcoin"


def test_raw_wal_is_hourly_fsynced_and_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fsync_calls: list[int] = []
    monkeypatch.setattr(
        storage_module.os,
        "fsync",
        lambda descriptor: fsync_calls.append(descriptor),
    )
    storage = ResearchStorage(tmp_path / "data", fsync=True)
    try:
        event = _raw_event("event-1", event_type="orderbook")
        first = storage.append_raw_event(event)
        duplicate = storage.append_raw_event(event)

        expected = tmp_path / "data" / "raw" / UID / "2026-07-10" / "10" / "events.jsonl"
        assert first.appended is True
        assert first.wal_path == expected
        assert duplicate.appended is False
        assert duplicate.wal_path == expected
        assert fsync_calls
        lines = expected.read_text(encoding="utf-8").splitlines()
        assert len(lines) == 1
        assert json.loads(lines[0])["event_id"] == "event-1"

        with sqlite3.connect(storage.state_db_path) as connection:
            assert connection.execute("SELECT COUNT(*) FROM raw_event_index").fetchone()[0] == 1
            table_names = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
        assert {
            "service_state",
            "stream_sessions",
            "subscription_events",
            "data_gaps",
        }.issubset(table_names)
    finally:
        storage.close()


def test_raw_event_contract_and_control_tables(tmp_path: Path) -> None:
    with ResearchStorage(tmp_path / "data") as storage:
        with pytest.raises(InvalidRecordError, match="missing required fields"):
            storage.append_raw_event({"event_id": "broken"})

        storage.set_state("checkpoint", {"sequence": 42}, updated_at=BASE)
        assert storage.get_state("checkpoint") == {"sequence": 42}
        assert storage.get_state("unknown", "fallback") == "fallback"

        assert storage.start_session(
            "session-1",
            source="tbank-readonly",
            stream_id="stream-1",
            started_at=BASE,
            metadata={"schema_version": "1"},
        )
        assert not storage.start_session(
            "session-1",
            source="tbank-readonly",
            started_at=BASE,
        )
        assert storage.record_subscription(
            session_id="session-1",
            subscription_id="subscription-1",
            instrument_uid=UID,
            event_type="orderbook",
            action="subscribe",
            event_time=BASE,
        )
        assert storage.record_gap(
            gap_id="gap-1",
            session_id="session-1",
            instrument_uid=UID,
            event_type="trades",
            started_at=BASE,
            ended_at=BASE + timedelta(seconds=7),
            recoverable=False,
            reason="disconnect",
        )
        assert storage.finish_session(
            "session-1",
            ended_at=BASE + timedelta(minutes=1),
        )

        metrics = storage.daily_control_metrics(date(2026, 7, 10))
        assert metrics == {
            "stream_sessions": 1,
            "reconnects": 0,
            "subscription_events": 1,
            "data_gaps": 1,
            "gap_duration_seconds": 7.0,
        }


def test_parquet_compaction_is_atomic_zstd_and_preserves_wal(tmp_path: Path) -> None:
    with ResearchStorage(tmp_path / "data") as storage:
        storage.append_raw_events(
            [
                _raw_event("event-1", event_type="orderbook", latency_ms=2.5),
                _raw_event(
                    "event-2",
                    event_type="trades",
                    timestamp=BASE + timedelta(minutes=1),
                    latency_ms=4.5,
                ),
            ]
        )
        wal_path = tmp_path / "data" / "raw" / UID / "2026-07-10" / "10" / "events.jsonl"
        before = wal_path.read_bytes()

        result = storage.compact_hour(UID, "2026-07-10", 10)

        assert wal_path.read_bytes() == before
        assert result.row_count == 2
        assert result.source_size == len(before)
        assert result.parquet_path.is_file()
        table = pq.read_table(result.parquet_path)
        assert table.column("event_id").to_pylist() == ["event-1", "event-2"]
        metadata = pq.ParquetFile(result.parquet_path).metadata
        assert metadata.row_group(0).column(0).compression == "ZSTD"
        assert not list(result.parquet_path.parent.glob("*.tmp"))


def test_all_derived_appenders_and_duckdb_views_are_idempotent(tmp_path: Path) -> None:
    with ResearchStorage(tmp_path / "data") as storage:
        storage.append_raw_event(_raw_event("raw-1", event_type="orderbook"))
        storage.compact_all()
        appenders = {
            "feature_snapshots": storage.append_feature_snapshot,
            "candidate_events": storage.append_candidate_event,
            "future_outcomes": storage.append_future_outcome,
            "execution_simulations": storage.append_execution_simulation,
            "shadow_predictions": storage.append_shadow_prediction,
            "shadow_trades": storage.append_shadow_trade,
            "model_registry": storage.append_model_registry,
            "experiment_registry": storage.append_experiment_registry,
            "data_quality_metrics": storage.append_data_quality_metric,
        }
        assert tuple(appenders) == DERIVED_DATASETS

        for dataset, append in appenders.items():
            record = {
                "event_id": f"{dataset}-1",
                "exchange_timestamp": BASE,
                "schema_version": "1",
                "value": dataset,
            }
            first = append(record)
            duplicate = append(record)
            assert first.appended is True
            assert duplicate.appended is False
            assert first.parquet_path == duplicate.parquet_path
            assert list(storage.iter_derived(dataset, "2026-07-10")) == [
                {
                    "event_id": f"{dataset}-1",
                    "exchange_timestamp": BASE.isoformat(),
                    "schema_version": "1",
                    "value": dataset,
                }
            ]

        catalog = storage.refresh_duckdb_catalog()
        assert catalog.catalog_path.is_file()
        assert catalog.views == ("raw_events", *DERIVED_DATASETS)
        with duckdb.connect(str(catalog.catalog_path), read_only=True) as connection:
            assert connection.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0] == 1
            for dataset in DERIVED_DATASETS:
                assert connection.execute(f"SELECT COUNT(*) FROM {dataset}").fetchone()[0] == 1
            assert connection.execute("SELECT COUNT(*) FROM research_catalog").fetchone()[0] == 10


def test_daily_markdown_and_json_report_quality_and_execution(tmp_path: Path) -> None:
    with ResearchStorage(tmp_path / "data") as storage:
        storage.append_raw_event(
            _raw_event(
                "book-1",
                event_type="orderbook",
                latency_ms=1,
                is_consistent=True,
                is_stale=False,
                payload={"depth": 20},
            )
        )
        storage.append_raw_event(
            _raw_event(
                "trade-1",
                event_type="trades",
                timestamp=BASE + timedelta(seconds=1),
                latency_ms=2,
                is_consistent=False,
                is_stale=False,
            )
        )
        storage.append_raw_event(
            _raw_event(
                "candle-1",
                event_type="candles",
                timestamp=BASE + timedelta(seconds=2),
                latency_ms=100,
                is_consistent=True,
                is_stale=True,
            )
        )
        for number in (1, 2):
            storage.start_session(
                f"session-{number}",
                source="tbank-readonly",
                started_at=BASE + timedelta(seconds=number),
            )
        storage.record_subscription(
            session_id="session-1",
            subscription_id="subscription-1",
            instrument_uid=UID,
            event_type="orderbook",
            action="subscribe",
            event_time=BASE,
        )
        storage.record_gap(
            gap_id="gap-1",
            session_id="session-1",
            instrument_uid=UID,
            event_type="trades",
            started_at=BASE,
            ended_at=BASE + timedelta(seconds=5),
            reason="disconnect",
        )
        storage.append_derived_many(
            "execution_simulations",
            [
                _execution("execution-1", "filled", 10, 10, fill_time_ms=10, pnl=1.0),
                _execution("execution-2", "partial", 10, 5, fill_time_ms=20, pnl=-0.2),
                _execution("execution-3", "no_fill", 10, 0, fill_time_ms=None, pnl=0.0),
            ],
        )

        paths = generate_daily_research_report(
            storage,
            "2026-07-10",
            reports_dir=tmp_path / "reports",
            generated_at=BASE + timedelta(hours=12),
        )

        payload = json.loads(paths.json_path.read_text(encoding="utf-8"))
        quality = payload["quality"]
        execution = payload["execution"]["models"]["aggressive_sweep"]
        assert quality["events_total"] == 3
        assert quality["event_counts"] == {"candles": 1, "orderbook": 1, "trades": 1}
        assert quality["uptime_seconds"] == 2.0
        assert quality["reconnects"] == 1
        assert quality["data_gaps"] == 1
        assert quality["gap_duration_seconds"] == 5.0
        assert quality["consistent_fraction"] == pytest.approx(2 / 3)
        assert quality["stale_fraction"] == pytest.approx(1 / 3)
        assert quality["latency_ms"]["p50"] == 2.0
        assert quality["latency_ms"]["p95"] == pytest.approx(90.2)
        assert quality["excluded_events"] == 2
        assert execution["fill_rate"] == pytest.approx(1 / 3)
        assert execution["partial_fill_rate"] == pytest.approx(1 / 3)
        assert execution["no_fill_rate"] == pytest.approx(1 / 3)
        assert execution["median_fill_time_ms"] == 15.0
        assert execution["net_pnl"] == pytest.approx(0.8)
        markdown = paths.markdown_path.read_text(encoding="utf-8")
        assert "## Data quality" in markdown
        assert "## Execution simulation" in markdown
        assert "aggressive_sweep" in markdown
        assert not list((tmp_path / "reports").glob("*.tmp"))


def _raw_event(
    event_id: str,
    *,
    event_type: str,
    timestamp: datetime = BASE,
    latency_ms: float = 1.0,
    is_consistent: bool = True,
    is_stale: bool = False,
    payload: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "instrument_uid": UID,
        "event_type": event_type,
        "exchange_timestamp": timestamp,
        "receive_timestamp": timestamp + timedelta(milliseconds=latency_ms),
        "latency_ms": latency_ms,
        "is_consistent": is_consistent,
        "is_stale": is_stale,
        "payload": payload or {"value": event_id},
        "schema_version": "1",
        "source": "tbank-readonly",
    }


def _execution(
    event_id: str,
    status: str,
    requested: int,
    filled: int,
    *,
    fill_time_ms: int | None,
    pnl: float,
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "exchange_timestamp": BASE,
        "schema_version": "1",
        "model": "aggressive_sweep",
        "status": status,
        "requested_quantity": requested,
        "filled_quantity": filled,
        "fill_time_ms": fill_time_ms,
        "levels_consumed": 2,
        "sweep_cost_ticks": 0.5,
        "slippage_ticks": 0.25,
        "adverse_selection_ticks": 0.1,
        "net_pnl": pnl,
    }
