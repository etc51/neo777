from datetime import UTC, datetime, timedelta

import pyarrow as pa

from neo_trader.neobitcoin_research.review_datasets import (
    HORIZONS,
    build_review_datasets,
    validate_review_links,
)


def test_review_datasets_are_typed_and_linked() -> None:
    t0 = datetime(2026, 7, 11, 10, 0, tzinfo=UTC)
    feature = {
        "event_id": "event-1",
        "raw_event_id": "raw-1",
        "instrument_uid": "uid",
        "ticker": "NEOBITCOIN",
        "timestamp": t0.isoformat(),
        "recorded_at": t0.isoformat(),
        "best_bid": 99.9,
        "best_ask": 100.1,
        "midprice": 100.0,
        "last_price": 100.0,
        "spread_ticks": 2.0,
        "spread_bps": 20.0,
        "microprice": 100.0,
        "multi_level_ofi": 1.0,
        "ofi_change": 0.1,
        "realized_volatility": 0.01,
        "feature_ready": True,
        "atr_1m": 1.0,
        "atr_5m": 2.0,
        "atr_15m": 3.0,
        "candle_volume_1m": 10.0,
        "candle_volume_5m": 20.0,
        "candle_volume_15m": 30.0,
    }
    path = [
        {"exchange_ts": t0 + timedelta(seconds=n), "mid_price": 100 + n / 1000} for n in range(1802)
    ]
    tables = build_review_datasets(
        [feature], path, candidate_start=t0, candidate_end=t0 + timedelta(minutes=10)
    )

    assert set(tables) == {
        "feature_snapshots",
        "candidate_events",
        "filter_decisions",
        "execution_simulations",
        "future_outcomes",
        "shadow_stop_results",
        "shadow_exit_results",
    }
    assert tables["candidate_events"].num_rows == 2
    assert tables["execution_simulations"].num_rows == 10
    assert tables["future_outcomes"].num_rows == 10 * len(HORIZONS)
    assert tables["shadow_stop_results"].num_rows == 40
    assert tables["shadow_exit_results"].num_rows == 70
    assert pa.types.is_timestamp(tables["future_outcomes"].schema.field("future_ts").type)
    assert tables["future_outcomes"].schema.field("future_ts").type.tz == "UTC"
    assert validate_review_links(tables) == []
    assert {r["side"] for r in tables["candidate_events"].to_pylist()} == {"LONG", "SHORT"}
    assert {r["horizon_seconds"] for r in tables["future_outcomes"].to_pylist()} == set(HORIZONS)
