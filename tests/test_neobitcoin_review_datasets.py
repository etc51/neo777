from datetime import UTC, datetime, timedelta

import pyarrow as pa

from neo_trader.neobitcoin_research.review_datasets import (
    HORIZONS,
    _nearest,
    _segment,
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
        "trades_10s_signed_volume": 1.0,
        "trades_180s_signed_volume": 2.0,
        "trades_300s_signed_volume": 3.0,
        "trades_900s_signed_volume": 4.0,
        "book_age_ms": 5.0,
        "bid_price_1": 99.9,
        "bid_volume_1": 4.0,
        "ask_price_1": 100.1,
        "ask_volume_1": 4.0,
    }
    path = []
    for n in range(1802):
        mid = 100 + n / 1000
        path.append(
            {
                "exchange_ts": t0 + timedelta(seconds=n),
                "mid_price": mid,
                "best_bid": mid - 0.1,
                "best_ask": mid + 0.1,
                "bids": [{"price": mid - 0.1, "quantity": 4.0}],
                "asks": [{"price": mid + 0.1, "quantity": 4.0}],
            }
        )
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
    filled = sum(r["fill_status"] != "NO_FILL" for r in tables["execution_simulations"].to_pylist())
    assert 0 < filled < 10
    assert {r["fill_status"] for r in tables["execution_simulations"].to_pylist()} == {
        "FULL_FILL",
        "PARTIAL_FILL",
        "NO_FILL",
    }
    aggressive = [
        row
        for row in tables["execution_simulations"].to_pylist()
        if row["execution_model"] == "aggressive_marketable"
    ]
    assert all(row["fill_delay_ms"] >= 100 for row in aggressive)
    assert all(row["filled_quantity"] == 4.0 for row in aggressive)
    assert tables["shadow_stop_results"].num_rows == filled * 4
    assert tables["shadow_exit_results"].num_rows == filled * 7
    assert pa.types.is_timestamp(tables["future_outcomes"].schema.field("future_ts").type)
    assert tables["future_outcomes"].schema.field("future_ts").type.tz == "UTC"
    assert validate_review_links(tables) == []
    assert {r["side"] for r in tables["candidate_events"].to_pylist()} == {"LONG", "SHORT"}
    assert {r["horizon_seconds"] for r in tables["future_outcomes"].to_pylist()} == set(HORIZONS)
    assert all(
        r["mfe_ticks"] >= 0 and r["mae_ticks"] >= 0 for r in tables["future_outcomes"].to_pylist()
    )
    assert all(
        r["holding_ms"] >= 0 and r["exit_ts"] >= r["entry_ts"]
        for r in tables["shadow_stop_results"].to_pylist()
    )
    exit_rows = tables["shadow_exit_results"].to_pylist()
    signatures = {
        variant: {
            (r["exit_ts"], r["exit_reason"], r["trigger_threshold"])
            for r in exit_rows
            if r["exit_variant"] == variant
        }
        for variant in {r["exit_variant"] for r in exit_rows}
    }
    assert len({frozenset(values) for values in signatures.values()}) > 1


def test_missing_required_trade_flow_disables_feature_ready() -> None:
    t0 = datetime(2026, 7, 11, 10, 0, tzinfo=UTC)
    feature = {
        "timestamp": t0,
        "best_bid": 99.9,
        "best_ask": 100.1,
        "midprice": 100.0,
        "spread_ticks": 2.0,
        "spread_bps": 20.0,
        "feature_ready": True,
    }
    tables = build_review_datasets(
        [feature],
        [{"exchange_ts": t0, "mid_price": 100.0}],
        candidate_start=t0,
        candidate_end=t0 + timedelta(minutes=10),
    )
    row = tables["feature_snapshots"].to_pylist()[0]
    assert row["feature_ready"] is False
    assert row["warmup_remaining"] == 4
    assert row["missing_reason"].startswith("missing_required_features:")


def test_trade_flow_and_data_age_are_strictly_as_of_feature() -> None:
    t0 = datetime(2026, 7, 11, 10, 0, tzinfo=UTC)
    feature = {
        "timestamp": t0,
        "best_bid": 99.9,
        "best_ask": 100.1,
        "midprice": 100.0,
        "spread_ticks": 2.0,
        "spread_bps": 20.0,
        "feature_ready": True,
    }
    trades = [
        {"exchange_ts": t0 - timedelta(seconds=5), "side": "BUY", "quantity": 3.0},
        {"exchange_ts": t0 - timedelta(seconds=2), "side": "SELL", "quantity": 1.0},
        {"exchange_ts": t0 + timedelta(seconds=1), "side": "BUY", "quantity": 1000.0},
    ]
    tables = build_review_datasets(
        [feature],
        [{"exchange_ts": t0 - timedelta(seconds=3), "mid_price": 100.0}],
        trade_rows=trades,
        candidate_start=t0,
        candidate_end=t0 + timedelta(minutes=10),
    )
    row = tables["feature_snapshots"].to_pylist()[0]
    assert row["trade_flow_10s"] == 2.0
    assert row["trade_flow_180s"] == 2.0
    assert row["trade_flow_300s"] == 2.0
    assert row["trade_flow_900s"] == 2.0
    assert row["data_age_ms"] == 2000.0
    assert row["feature_ready"] is True


def test_stops_use_post_fill_executable_gap_price_for_both_sides() -> None:
    t0 = datetime(2026, 7, 11, 10, 0, tzinfo=UTC)
    feature = {
        "timestamp": t0,
        "best_bid": 99.9,
        "best_ask": 100.1,
        "midprice": 100.0,
        "spread_ticks": 2.0,
        "spread_bps": 20.0,
        "trades_10s_signed_volume": 0.0,
        "trades_180s_signed_volume": 0.0,
        "trades_300s_signed_volume": 0.0,
        "trades_900s_signed_volume": 0.0,
        "data_age_ms": 0.0,
    }
    same_ts = t0 + timedelta(seconds=1)
    path = [
        {"exchange_ts": t0, "mid_price": 100.0, "best_bid": 99.9, "best_ask": 100.1},
        {"exchange_ts": same_ts, "mid_price": 97.0, "best_bid": 96.9, "best_ask": 97.1},
        {"exchange_ts": same_ts, "mid_price": 103.0, "best_bid": 102.9, "best_ask": 103.1},
    ]
    tables = build_review_datasets(
        [feature], path, candidate_start=t0, candidate_end=t0 + timedelta(minutes=10)
    )
    stops = [
        row
        for row in tables["shadow_stop_results"].to_pylist()
        if row["simulation_id"]
        in {
            execution["simulation_id"]
            for execution in tables["execution_simulations"].to_pylist()
            if execution["execution_model"] == "ideal_touch"
        }
    ]
    assert all(row["stop_triggered"] for row in stops)
    assert all(row["exit_ts"] == same_ts and row["holding_ms"] == 1000 for row in stops)
    assert all(row["exit_price"] == 96.9 for row in stops if row["side"] == "LONG")
    assert all(row["exit_price"] == 103.1 for row in stops if row["side"] == "SHORT")


def test_horizon_prices_do_not_look_ahead() -> None:
    start = datetime(2026, 7, 11, 10, 0, tzinfo=UTC)
    path = [(start, 100.0), (start + timedelta(seconds=11), 101.0)]
    times = [row[0] for row in path]
    horizon = start + timedelta(seconds=10)

    assert _nearest(path, times, horizon) == 100.0
    assert _segment(path, times, start, horizon) == [(start, 100.0)]
