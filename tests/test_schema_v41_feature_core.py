from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from neo_trader.neobitcoin_research.schema_v4.candidates import (
    CandidateConfig,
    CandidateMaterializer,
)
from neo_trader.neobitcoin_research.schema_v4.features import FeatureConfig, FeatureMaterializer
from neo_trader.neobitcoin_research.schema_v4.timeline import CanonicalTimeline

T0 = datetime(2026, 7, 12, 10, 0, tzinfo=UTC)


def _common(event_id: str, when: datetime, *, sequence: int = 0) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "exchange_ts": when,
        "receive_ts": when + timedelta(milliseconds=5),
        "processing_ts": when + timedelta(milliseconds=10),
        "sequence": sequence,
        "session_id": "s1",
    }


def _book(event_id: str, when: datetime, *, ask: float = 100.03) -> dict[str, Any]:
    return {
        **_common(event_id, when),
        "bids": [{"price": 100.0 - index * 0.01, "quantity": 10.0} for index in range(20)],
        "asks": [{"price": ask + index * 0.01, "quantity": 8.0} for index in range(20)],
    }


def _candle(event_id: str, start: datetime, revision: int, complete: bool) -> dict[str, Any]:
    receive = start + timedelta(minutes=1, milliseconds=revision)
    return {
        **_common(event_id, receive - timedelta(milliseconds=5), sequence=revision),
        "candle_start": start,
        "candle_end": start + timedelta(minutes=1),
        "revision": revision,
        "open": 100.0,
        "high": 102.0,
        "low": 99.0,
        "close": 100.0 + revision,
        "volume": 10.0 + revision,
        "is_complete": complete,
    }


def _raw(books: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    return {
        "raw_orderbook": books,
        "raw_trades": [],
        "raw_last_price": [],
        "candles_1m": [],
        "candles_5m": [],
        "candles_15m": [],
        "market_status_events": [],
    }


def test_closed_source_false_candle_and_latest_point_in_time_revision_are_selected() -> None:
    feature_time = T0 + timedelta(minutes=4)
    raw = _raw([_book("seed", feature_time - timedelta(seconds=1)), _book("feature", feature_time)])
    early = _candle("early", T0, 1, True)
    selected = _candle("selected", T0 + timedelta(minutes=1), 1, False)
    late = _candle("late", T0 + timedelta(minutes=1), 2, True)
    late["receive_ts"] = feature_time + timedelta(seconds=1)
    late["processing_ts"] = feature_time + timedelta(seconds=1)
    raw["candles_1m"] = [early, selected, late]
    timeline = CanonicalTimeline(raw)
    row = FeatureMaterializer(
        timeline,
        FeatureConfig(tick_size=0.01, atr_required=False),
        materialized_ts=feature_time + timedelta(hours=1),
    ).materialize([timeline.events("raw_orderbook")[-1]])[0]

    assert row["candle_1m_source_event_id"] == "selected"
    assert row["candle_1m_source_is_complete"] is False
    assert row["candle_1m_canonical_is_complete"] is True
    assert row["candle_1m_completion_reason"] == "interval_elapsed"
    assert row["candle_volume_1m"] == 11.0


def test_ofi_uses_previous_raw_snapshot_before_materialized_window() -> None:
    raw = _raw([_book("seed", T0), _book("feature", T0 + timedelta(seconds=5))])
    raw["raw_orderbook"][1]["bids"][0]["quantity"] = 13.0
    timeline = CanonicalTimeline(raw)
    row = FeatureMaterializer(
        timeline,
        FeatureConfig(tick_size=0.01),
        materialized_ts=T0 + timedelta(hours=1),
    ).materialize([timeline.events("raw_orderbook")[-1]])[0]

    assert row["ofi_previous_event_id"] == "seed"
    assert row["ofi_continuity_valid"] is True
    assert row["ofi_delta"] == 3.0


def test_decimal_spread_normalization_drives_integer_gate() -> None:
    raw = _raw([_book("seed", T0), _book("feature", T0 + timedelta(seconds=5))])
    timeline = CanonicalTimeline(raw)
    feature = FeatureMaterializer(
        timeline,
        FeatureConfig(tick_size=0.01),
        materialized_ts=T0 + timedelta(hours=1),
    ).materialize([timeline.events("raw_orderbook")[-1]])[0]
    candidates, decisions = CandidateMaterializer(CandidateConfig(spread_gate_ticks=3)).materialize(
        [feature]
    )

    assert feature["spread_ticks_decimal"] > 3.0
    assert feature["spread_ticks_int"] == 3
    assert feature["spread_threshold_ticks"] == 3
    assert feature["spread_gate_passed"] is True
    assert all(row["spread_ticks_int"] == 3 for row in candidates)
    assert all(row["passed"] for row in decisions if row["filter_name"] == "spread")
