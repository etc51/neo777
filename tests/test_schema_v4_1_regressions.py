"""Focused regressions for the schema-v4.1 candle/readiness/spread fixes."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
import pytest

from neo_trader.neobitcoin_research.schema_v4 import (
    CandidateConfig,
    CandidateMaterializer,
    CanonicalTimeline,
    FeatureConfig,
    FeatureMaterializer,
)
from neo_trader.neobitcoin_research.schema_v4.archive import DATASETS, publish_golden_archive

NOW = datetime(2026, 7, 11, 10, 0, tzinfo=UTC)


def _event(event_id: str, at: datetime, **values: Any) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "exchange_ts": at,
        "receive_ts": at,
        "processing_ts": at,
        **values,
    }


def _book(event_id: str, at: datetime, *, bid: float = 100.0, ask: float = 100.2,
          bid_qty: float = 12.0, ask_qty: float = 10.0) -> dict[str, Any]:
    bids = [{"price": bid - 0.1 * index, "quantity": bid_qty + index} for index in range(20)]
    asks = [{"price": ask + 0.1 * index, "quantity": ask_qty + index} for index in range(20)]
    return _event(event_id, at, bids=bids, asks=asks, best_bid=bid, best_ask=ask, session_id="s1")


def _candle(minutes: int, number: int, end: datetime, *, revision: int = 1,
            complete: bool = True, receive: datetime | None = None,
            volume: float | None = None) -> dict[str, Any]:
    received = receive or end - timedelta(milliseconds=200)
    row = _event(
        f"c{minutes}-{number}-r{revision}",
        end - timedelta(milliseconds=300),
        candle_start=end - timedelta(minutes=minutes),
        candle_end=end,
        open=99.0 + number,
        high=102.0 + number,
        low=99.0 + number,
        close=100.0 + number,
        volume=float(volume if volume is not None else 10 + number),
        is_complete=complete,
        revision=revision,
        timeframe=f"{minutes}m",
        instrument_uid="NEO",
    )
    row["receive_ts"] = received
    row["processing_ts"] = received + timedelta(milliseconds=1)
    return row


def _raw() -> dict[str, list[dict[str, Any]]]:
    raw: dict[str, list[dict[str, Any]]] = {
        "raw_orderbook": [
            _book("book-previous", NOW - timedelta(seconds=1), bid_qty=10, ask_qty=10),
            _book("book-current", NOW),
        ],
        "raw_trades": [
            _event("trade", NOW - timedelta(milliseconds=500), price=100.1, quantity=1.0,
                   aggressor_side="BUY")
        ],
        "raw_last_price": [
            _event("last", NOW - timedelta(milliseconds=500), last_price=100.1)
        ],
        "market_status_events": [],
    }
    for minutes in (1, 5, 15):
        raw[f"candles_{minutes}m"] = [
            _candle(minutes, index, NOW - timedelta(minutes=minutes * (2 - index)))
            for index in range(3)
        ]
    return raw


def _feature(raw: dict[str, list[dict[str, Any]]] | None = None, *,
             event_id: str = "book-current", materialized: datetime | None = None,
             **config: Any) -> dict[str, Any]:
    timeline = CanonicalTimeline(raw or _raw())
    event = next(row for row in timeline.events("raw_orderbook") if row.event_id == event_id)
    return FeatureMaterializer(
        timeline,
        FeatureConfig(tick_size=0.1, required_book_levels=20, candle_warmup=2,
                      atr_period=2, **config),
        materialized_ts=materialized or NOW + timedelta(hours=1),
    ).materialize([event])[0]


def test_01_source_false_but_elapsed_candle_is_selected() -> None:
    raw = _raw()
    raw["candles_1m"][-1]["is_complete"] = False
    feature = _feature(raw)
    assert feature["candle_1m_source_event_id"] == "c1-2-r1"
    assert feature["candle_1m_source_is_complete"] is False
    assert feature["candle_1m_canonical_is_complete"] is True
    assert feature["candle_1m_completion_reason"] == "interval_elapsed"


def test_02_old_source_complete_does_not_beat_fresher_elapsed_candle() -> None:
    raw = _raw()
    raw["candles_1m"][-1]["is_complete"] = False
    assert _feature(raw)["candle_1m_source_event_id"] == "c1-2-r1"


def test_03_future_revision_is_not_used() -> None:
    raw = _raw()
    early = raw["candles_1m"][-1]
    late = {**early, "event_id": "c1-late", "revision": 2,
            "receive_ts": NOW + timedelta(seconds=1),
            "processing_ts": NOW + timedelta(seconds=1)}
    raw["candles_1m"].append(late)
    assert _feature(raw)["candle_1m_source_event_id"] == early["event_id"]


def test_04_latest_revision_available_point_in_time_is_used() -> None:
    raw = _raw()
    early = raw["candles_1m"][-1]
    revision = {**early, "event_id": "c1-revision-2", "revision": 2,
                "receive_ts": NOW - timedelta(milliseconds=100),
                "processing_ts": NOW - timedelta(milliseconds=50), "volume": 77.0}
    raw["candles_1m"].append(revision)
    feature = _feature(raw)
    assert feature["candle_1m_source_event_id"] == "c1-revision-2"
    assert feature["candle_1m_selected_revision"] == 2


def test_05_unfinished_candle_is_not_used() -> None:
    raw = _raw()
    raw["candles_1m"].append(_candle(1, 3, NOW + timedelta(minutes=1), complete=True))
    assert _feature(raw)["candle_1m_source_event_id"] == "c1-2-r1"


def test_06_candle_source_id_matches_the_ohlcv_source() -> None:
    raw = _raw()
    source = raw["candles_1m"][-1]
    feature = _feature(raw)
    assert feature["candle_1m_source_event_id"] == source["event_id"]
    assert feature["candle_1m_start"] == source["candle_start"]
    assert feature["candle_1m_end"] == source["candle_end"]


def test_07_candle_volume_matches_raw_source() -> None:
    raw = _raw()
    raw["candles_5m"][-1]["volume"] = 1234.5
    assert _feature(raw)["candle_volume_5m"] == 1234.5


def test_08_atr_matches_point_in_time_true_range() -> None:
    feature = _feature()
    assert feature["atr_1m"] == pytest.approx(3.0)
    assert feature["atr_5m"] == pytest.approx(3.0)
    assert feature["atr_15m"] == pytest.approx(3.0)


def test_09_stale_candle_blocks_readiness() -> None:
    raw = _raw()
    raw["candles_1m"] = raw["candles_1m"][:1]
    feature = _feature(raw)
    assert feature["candle_1m_interval_age_ms"] == 120_000
    assert feature["candle_1m_stale"] is True
    assert feature["source_status"] == "stale_candle"
    assert feature["feature_ready"] is False


def test_10_null_ofi_delta_blocks_readiness() -> None:
    raw = _raw()
    raw["raw_orderbook"] = [raw["raw_orderbook"][-1]]
    feature = _feature(raw)
    assert feature["ofi_delta"] is None
    assert feature["feature_ready"] is False
    assert "ofi_delta" in feature["missing_fields"]


def test_11_first_snapshot_after_reconnect_blocks_readiness() -> None:
    raw = _raw()
    raw["market_status_events"] = [
        _event("reconnect", NOW - timedelta(milliseconds=500), event_type="reconnect")
    ]
    feature = _feature(raw)
    assert feature["ofi_reset_reason"] == "reconnect"
    assert feature["ofi_continuity_valid"] is False
    assert feature["feature_ready"] is False


def test_12_next_valid_snapshot_gets_ofi_delta() -> None:
    raw = _raw()
    raw["market_status_events"] = [
        _event("reconnect", NOW - timedelta(milliseconds=500), event_type="reconnect")
    ]
    next_book = _book("book-next", NOW + timedelta(seconds=1), bid_qty=15, ask_qty=9)
    raw["raw_orderbook"].append(next_book)
    feature = _feature(raw, event_id="book-next")
    assert feature["ofi_previous_event_id"] == "book-current"
    assert feature["ofi_continuity_valid"] is True
    assert feature["ofi_delta"] == pytest.approx(4.0)


def _spread_feature(ask: float) -> dict[str, Any]:
    raw = _raw()
    raw["raw_orderbook"][-1] = _book("book-current", NOW, bid=100.0, ask=ask)
    return _feature(raw, spread_grid_epsilon=1e-9)


def _spread_gate(feature: dict[str, Any]) -> bool:
    _, decisions = CandidateMaterializer(CandidateConfig(spread_gate_ticks=3)).materialize(
        [feature]
    )
    return bool(next(row["passed"] for row in decisions if row["filter_name"] == "spread"))


def test_13_float_three_ticks_passes_integer_spread_gate() -> None:
    feature = _spread_feature(100.30000000000291)
    assert feature["spread_ticks_decimal"] == pytest.approx(3.000000000029)
    assert feature["spread_ticks_int"] == 3
    assert _spread_gate(feature) is True


def test_14_four_ticks_fails_three_tick_gate() -> None:
    feature = _spread_feature(100.4)
    assert feature["spread_ticks_int"] == 4
    assert _spread_gate(feature) is False


def test_15_off_grid_price_sets_data_quality_flag() -> None:
    feature = _spread_feature(100.35)
    assert feature["spread_ticks_int"] is None
    assert feature["spread_on_tick_grid"] is False
    assert "off_tick_grid" in feature["data_quality_flags"]


def test_16_materialization_timestamp_does_not_change_source_ages() -> None:
    first = _feature(materialized=NOW + timedelta(hours=1))
    second = _feature(materialized=NOW + timedelta(days=1))
    age_fields = [name for name in first if name.endswith(("_receive_age_ms", "_interval_age_ms"))]
    assert {name: first[name] for name in age_fields} == {name: second[name] for name in age_fields}
    assert first["materialization_lag_ms"] != second["materialization_lag_ms"]


def test_17_correct_schema_v4_1_bundle_passes_publication(tmp_path: Path) -> None:
    tables = {
        name: pa.table({"event_id": pa.array([], type=pa.string())}) for name in DATASETS
    }
    result = publish_golden_archive(
        tables,
        tmp_path,
        candidate_start=NOW,
        candidate_end=NOW + timedelta(minutes=10),
        support_start=NOW - timedelta(minutes=30),
        required_support_end=NOW + timedelta(minutes=40),
        validation={"status": "PASS", "gates": {"independent_oracle": "PASS"}},
    )
    assert result.file_count == 20
    assert "schema-v4.1-golden" in result.archive_path.name
    assert result.sha256_path.is_file()
