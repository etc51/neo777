from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from neo_trader.neobitcoin_research.schema_v4 import (
    CanonicalTimeline,
    ExecutionConfig,
    ExecutionSimulator,
    FeatureConfig,
    FeatureMaterializer,
    RawOnlyPipeline,
    TimelineError,
)

BASE = datetime(2026, 7, 11, 10, 0, tzinfo=UTC)


def _common(event_id: str, seconds: float) -> dict[str, Any]:
    exchange = BASE + timedelta(seconds=seconds)
    return {
        "event_id": event_id,
        "exchange_ts": exchange,
        "receive_ts": exchange + timedelta(milliseconds=10),
        "processing_ts": exchange + timedelta(milliseconds=20),
    }


def _book(event_id: str, seconds: float, mid: float = 100.0) -> dict[str, Any]:
    bids = [{"price": mid - 0.01 * (i + 1), "quantity": float(i + 1)} for i in range(20)]
    asks = [{"price": mid + 0.01 * (i + 1), "quantity": float(i + 2)} for i in range(20)]
    return {
        **_common(event_id, seconds),
        "revision": int(seconds),
        "bids": bids,
        "asks": asks,
        "best_bid": bids[0]["price"],
        "best_ask": asks[0]["price"],
    }


def _candle(interval: int, number: int, *, complete: bool = True) -> dict[str, Any]:
    end = BASE + timedelta(seconds=800 + number * interval * 60)
    return {
        **_common(f"c{interval}-{number}", (end - BASE).total_seconds() - 1),
        "candle_end": end,
        "open": 99.0 + number,
        "high": 101.0 + number,
        "low": 98.0 + number,
        "close": 100.0 + number,
        "volume": 10.0,
        "is_complete": complete,
    }


def _raw() -> dict[str, list[dict[str, Any]]]:
    books = [_book("b0", 999)]
    books += [_book(f"b{i}", 1000 + (i - 1) * 5, 100 + i * 0.01) for i in range(1, 8)]
    books += [_book("support", 2810, 101.0)]
    return {
        "raw_orderbook": books,
        "raw_trades": [
            {**_common("t-buy", 996), "price": 100.0, "quantity": 3.0, "aggressor_side": "BUY"},
            {**_common("t-sell", 998), "price": 100.0, "quantity": 1.0, "aggressor_side": "SELL"},
            {
                **_common("t-unknown", 999),
                "price": 100.0,
                "quantity": 8.0,
                "aggressor_side": "UNKNOWN",
            },
            {
                **_common("t-future", 1002),
                "price": 100.0,
                "quantity": 99.0,
                "aggressor_side": "BUY",
            },
        ],
        "raw_last_price": [{**_common("last", 999), "last_price": 100.0}],
        "candles_1m": [_candle(1, 1), _candle(1, 2), _candle(1, 3, complete=False)],
        "candles_5m": [_candle(5, 0), _candle(5, 1)],
        "candles_15m": [_candle(15, 0), _candle(15, 1)],
        "market_status_events": [],
    }


def test_timeline_rejects_untyped_time_and_non_raw_input() -> None:
    row = _book("bad", 1)
    row["exchange_ts"] = "2026-01-01T00:00:00Z"
    with pytest.raises(TimelineError, match="typed datetime"):
        CanonicalTimeline({"raw_orderbook": [row]})
    with pytest.raises(TimelineError, match="non-raw"):
        CanonicalTimeline({"feature_snapshots": []})


def test_timeline_processing_cutoff_prevents_lookahead() -> None:
    late = _common("late", 10)
    late["receive_ts"] = BASE + timedelta(seconds=30)
    late["processing_ts"] = BASE + timedelta(seconds=31)
    timeline = CanonicalTimeline({"raw_trades": [late]})
    assert timeline.latest("raw_trades", BASE + timedelta(seconds=20)) is not None
    assert (
        timeline.latest(
            "raw_trades",
            BASE + timedelta(seconds=20),
            processing_cutoff=BASE + timedelta(seconds=20),
        )
        is None
    )


def test_features_have_twenty_levels_depth_flow_and_real_lineage() -> None:
    timeline = CanonicalTimeline(_raw())
    feature = FeatureMaterializer(
        timeline,
        FeatureConfig(tick_size=0.01, candle_warmup=2),
        materialized_ts=BASE + timedelta(hours=2),
    ).materialize([timeline.events("raw_orderbook")[1]])[0]
    assert feature["bid_quantity_20"] == 20.0
    assert feature["ask_quantity_20"] == 21.0
    assert feature["bid_depth_3"] == 6.0
    assert feature["ask_depth_3"] == 9.0
    assert feature["imbalance_3"] == pytest.approx(-0.2)
    assert feature["trade_flow_5s"] == 2.0
    assert feature["unknown_side_volume_5s"] == 8.0
    assert feature["trade_window_last_event_id_5s"] == "t-unknown"
    assert feature["candle_1m_source_event_id"] == "c1-3"
    assert feature["candle_1m_source_is_complete"] is False
    assert feature["candle_1m_canonical_is_complete"] is True
    assert feature["last_trade_source_event_id"] == "t-unknown"
    assert feature["last_trade_receive_age_ms"] == pytest.approx(1010.0)
    assert feature["materialization_lag_ms"] > feature["last_trade_receive_age_ms"]


def test_public_pipeline_is_raw_only_and_returns_seven_derived_tables() -> None:
    result = RawOnlyPipeline(
        tick_size=0.01,
        feature_config=FeatureConfig(tick_size=0.01, candle_warmup=2),
    ).materialize(
        _raw(),
        candidate_start=BASE + timedelta(seconds=1000),
        candidate_end=BASE + timedelta(seconds=1001),
        materialized_ts=BASE + timedelta(hours=2),
    )
    assert set(result.tables) == {
        "feature_snapshots",
        "candidate_events",
        "filter_decisions",
        "execution_simulations",
        "future_outcomes",
        "shadow_stop_results",
        "shadow_exit_results",
    }
    assert len(result.feature_snapshots) == 1
    assert {row["side"] for row in result.candidate_events} == {"LONG", "SHORT"}
    assert len(result.filter_decisions) == 14
    for rows in result.tables.values():
        assert all(
            row["event_id"] and row["materialized_ts"] == BASE + timedelta(hours=2) for row in rows
        )


def test_aggressive_sweep_vwap_is_partial_when_depth_is_insufficient() -> None:
    raw = _raw()
    raw["raw_orderbook"] = [_book("thin", 1000)]
    raw["raw_orderbook"][0]["asks"] = [
        {"price": 100.01, "quantity": 1.0},
        {"price": 100.02, "quantity": 2.0},
    ]
    timeline = CanonicalTimeline(raw)
    candidate = {
        "candidate_id": "c",
        "feature_snapshot_id": "f",
        "side": "LONG",
        "exchange_ts": BASE + timedelta(seconds=1000),
        "receive_ts": BASE + timedelta(seconds=1000, milliseconds=10),
        "processing_ts": BASE + timedelta(seconds=1000, milliseconds=20),
        "materialized_ts": BASE + timedelta(hours=2),
        "reference_price": 100.01,
        "best_bid": 99.99,
        "best_ask": 100.01,
    }
    row = ExecutionSimulator(
        timeline, ExecutionConfig(tick_size=0.01, order_quantity=4.0)
    ).materialize([candidate])[0]
    assert row["fill_status"] == "PARTIAL"
    assert row["filled_quantity"] == 3.0
    assert row["fill_price"] == pytest.approx((100.01 + 200.04) / 3)
    assert row["source_orderbook_event_id"] == "thin"


def test_outcomes_and_stops_use_actual_post_fill_book_events() -> None:
    result = RawOnlyPipeline(
        tick_size=0.01,
        feature_config=FeatureConfig(tick_size=0.01, candle_warmup=2),
    ).materialize(
        _raw(),
        candidate_start=BASE + timedelta(seconds=1000),
        candidate_end=BASE + timedelta(seconds=1001),
        materialized_ts=BASE + timedelta(hours=2),
    )
    complete = [row for row in result.future_outcomes if row["outcome_complete"]]
    assert complete
    assert all(row["price_source_exchange_ts"] <= row["target_ts"] for row in complete)
    assert all((row["mfe_ticks"] or 0) >= 0 and (row["mae_ticks"] or 0) >= 0 for row in complete)
    triggered = [row for row in result.shadow_stop_results if row["triggered"]]
    assert all(row["trigger_event_id"] == row["exit_source_event_id"] for row in triggered)
    assert all(row["trigger_ts"] > row["entry_ts"] for row in triggered)
    assert {row["exit_model"] for row in result.shadow_exit_results} == {
        "fixed_take_profit",
        "time_exit",
        "breakeven",
        "dynamic_breakeven",
        "trailing",
        "microstructure",
        "orderbook",
    }
