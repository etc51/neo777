from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest

from neo_trader.neobitcoin_research.independent_oracle import (
    CONTRACTS,
    IndependentOracleValidator,
    validate_data_contracts,
)

T0 = datetime(2026, 7, 11, 12, tzinfo=UTC)


def _table(rows: list[dict[str, object]]) -> pa.Table:
    return pa.Table.from_pylist(rows)


def _book(
    event_id: str = "book-0", *, ts: datetime = T0, bid: float = 100.0, ask: float = 101.0
) -> dict[str, object]:
    bids = [{"price": bid - level, "quantity": float(level + 1)} for level in range(20)]
    asks = [{"price": ask + level, "quantity": float(level + 2)} for level in range(20)]
    return {
        "event_id": event_id,
        "exchange_ts": ts,
        "receive_ts": ts,
        "processing_ts": ts,
        "materialized_ts": ts,
        "bids": bids,
        "asks": asks,
        "best_bid": bid,
        "best_ask": ask,
    }


def _feature(book: dict[str, object]) -> dict[str, object]:
    bids, asks = book["bids"], book["asks"]
    row: dict[str, object] = {
        "feature_snapshot_id": "feature-0",
        "feature_ts": T0,
        "feature_receive_ts": T0,
        "feature_processing_ts": T0,
        "materialized_ts": T0,
        "feature_ready": True,
        "trend": "range",
        "regime": "range",
        "orderbook_source_event_id": "book-0",
    }
    for level in range(1, 21):
        for side, values in (("bid", bids), ("ask", asks)):
            row[f"{side}_price_{level:02d}"] = values[level - 1]["price"]
            row[f"{side}_quantity_{level:02d}"] = values[level - 1]["quantity"]
    for depth in (1, 3, 5, 10, 20):
        bd = sum(item["quantity"] for item in bids[:depth])
        ad = sum(item["quantity"] for item in asks[:depth])
        row[f"bid_depth_{depth}"], row[f"ask_depth_{depth}"] = bd, ad
        row[f"imbalance_{depth}"] = (bd - ad) / (bd + ad)
    for window in (5, 10, 30, 60, 180, 300, 900):
        row[f"trade_flow_{window}s"], row[f"trade_count_{window}s"] = 0.0, 0
    return row


def _base_feature_tables() -> dict[str, pa.Table]:
    book = _book()
    return {"raw_orderbook": _table([book]), "feature_snapshots": _table([_feature(book)])}


def _codes(result: object) -> set[str]:
    return {item.code for item in result.violations}


def test_contract_registry_has_every_schema_v4_dataset() -> None:
    assert set(CONTRACTS) == {
        "raw_orderbook",
        "raw_trades",
        "raw_last_price",
        "candles_1m",
        "candles_5m",
        "candles_15m",
        "market_status_events",
        "feature_snapshots",
        "candidate_events",
        "filter_decisions",
        "execution_simulations",
        "future_outcomes",
        "shadow_stop_results",
        "shadow_exit_results",
    }
    assert all(
        contract.arrow_schema and contract.primary_key and contract.description
        for contract in CONTRACTS.values()
    )


def test_contract_duplicate_primary_key_fails() -> None:
    rows = [_book(), _book()]
    assert "duplicate_primary_key" in {
        v.code for v in validate_data_contracts({"raw_orderbook": _table(rows)}, require_all=False)
    }


def test_contract_non_monotonic_timestamp_fails() -> None:
    rows = [_book("later", ts=T0 + timedelta(seconds=1)), _book("earlier")]
    assert "non_monotonic_timestamp" in {
        v.code for v in validate_data_contracts({"raw_orderbook": _table(rows)}, require_all=False)
    }


def test_contract_negative_quantity_fails() -> None:
    tables = _base_feature_tables()
    rows = tables["feature_snapshots"].to_pylist()
    rows[0]["bid_quantity_01"] = -1.0
    assert "negative_value" in {
        v.code
        for v in validate_data_contracts({"feature_snapshots": _table(rows)}, require_all=False)
    }


def test_orderbook_unsorted_bid_fails() -> None:
    book = _book()
    book["bids"][1]["price"] = 200.0
    result = IndependentOracleValidator(tick_size=1, require_all_contracts=False).validate(
        {"raw_orderbook": _table([book])}
    )
    assert "unsorted_bids" in _codes(result)


def test_orderbook_best_price_mismatch_fails() -> None:
    book = _book()
    book["best_bid"] = 99.0
    result = IndependentOracleValidator(tick_size=1, require_all_contracts=False).validate(
        {"raw_orderbook": _table([book])}
    )
    assert "best_price_mismatch" in _codes(result)


@pytest.mark.parametrize(
    ("field", "code"),
    [
        ("bid_quantity_01", "book_level_mismatch"),
        ("bid_depth_20", "depth_mismatch"),
        ("imbalance_20", "imbalance_mismatch"),
    ],
)
def test_feature_book_math_regressions_fail(field: str, code: str) -> None:
    tables = _base_feature_tables()
    rows = tables["feature_snapshots"].to_pylist()
    rows[0][field] = None if "quantity" in field else 999.0
    tables["feature_snapshots"] = _table(rows)
    result = IndependentOracleValidator(tick_size=1, require_all_contracts=False).validate(tables)
    assert code in _codes(result)


def test_ready_feature_with_missing_required_fails() -> None:
    tables = _base_feature_tables()
    rows = tables["feature_snapshots"].to_pylist()
    rows[0]["ask_price_20"] = None
    tables["feature_snapshots"] = _table(rows)
    assert "ready_with_missing_required" in _codes(
        IndependentOracleValidator(tick_size=1, require_all_contracts=False).validate(tables)
    )


def test_ready_feature_without_trend_fails() -> None:
    tables = _base_feature_tables()
    rows = tables["feature_snapshots"].to_pylist()
    rows[0]["trend"] = None
    tables["feature_snapshots"] = _table(rows)
    assert "ready_without_classification" in _codes(
        IndependentOracleValidator(tick_size=1, require_all_contracts=False).validate(tables)
    )


def test_future_orderbook_lineage_fails() -> None:
    tables = _base_feature_tables()
    rows = tables["feature_snapshots"].to_pylist()
    rows[0]["orderbook_source_event_id"] = "future"
    tables["feature_snapshots"] = _table(rows)
    tables["raw_orderbook"] = _table([_book(), _book("future", ts=T0 + timedelta(seconds=1))])
    assert "orderbook_lineage_mismatch" in _codes(
        IndependentOracleValidator(tick_size=1, require_all_contracts=False).validate(tables)
    )


def test_unknown_trade_is_counted_but_not_signed() -> None:
    tables = _base_feature_tables()
    trade = {
        "event_id": "t",
        "exchange_ts": T0,
        "receive_ts": T0,
        "processing_ts": T0,
        "materialized_ts": T0,
        "price": 100.0,
        "quantity": 7.0,
        "aggressor_side": "UNKNOWN",
    }
    tables["raw_trades"] = _table([trade])
    rows = tables["feature_snapshots"].to_pylist()
    for window in (5, 10, 30, 60, 180, 300, 900):
        rows[0][f"trade_count_{window}s"] = 1
        rows[0][f"unknown_side_volume_{window}s"] = 7.0
    tables["feature_snapshots"] = _table(rows)
    result = IndependentOracleValidator(tick_size=1, require_all_contracts=False).validate(tables)
    assert "trade_flow_mismatch" not in _codes(result)


def test_trade_flow_boundary_is_open_left_closed_right() -> None:
    tables = _base_feature_tables()
    trades = []
    for event_id, seconds in (("excluded", -5), ("included", 0)):
        trades.append(
            {
                "event_id": event_id,
                "exchange_ts": T0 + timedelta(seconds=seconds),
                "receive_ts": T0 + timedelta(seconds=seconds),
                "processing_ts": T0 + timedelta(seconds=seconds),
                "materialized_ts": T0,
                "price": 100.0,
                "quantity": 2.0,
                "aggressor_side": "BUY",
            }
        )
    tables["raw_trades"] = _table(trades)
    rows = tables["feature_snapshots"].to_pylist()
    rows[0]["trade_count_5s"] = 1
    rows[0]["trade_flow_5s"] = 2.0
    for window in (10, 30, 60, 180, 300, 900):
        rows[0][f"trade_count_{window}s"] = 2
        rows[0][f"trade_flow_{window}s"] = 4.0
    tables["feature_snapshots"] = _table(rows)
    assert "trade_flow_mismatch" not in _codes(
        IndependentOracleValidator(tick_size=1, require_all_contracts=False).validate(tables)
    )


def test_unfinished_candle_source_fails() -> None:
    tables = _base_feature_tables()
    candle = {
        "event_id": "c",
        "exchange_ts": T0 - timedelta(minutes=1),
        "receive_ts": T0,
        "processing_ts": T0,
        "materialized_ts": T0,
        "candle_end": T0 + timedelta(minutes=1),
        "open": 1.0,
        "high": 2.0,
        "low": 0.5,
        "close": 1.5,
        "volume": 2.0,
    }
    tables["candles_1m"] = _table([candle])
    rows = tables["feature_snapshots"].to_pylist()
    rows[0]["candle_1m_source_event_id"] = "c"
    tables["feature_snapshots"] = _table(rows)
    assert "candle_source_mismatch" in _codes(
        IndependentOracleValidator(tick_size=1, require_all_contracts=False).validate(tables)
    )


def test_artificial_source_age_fails() -> None:
    tables = _base_feature_tables()
    rows = tables["feature_snapshots"].to_pylist()
    rows[0]["orderbook_receive_age_ms"] = 123.0
    tables["feature_snapshots"] = _table(rows)
    assert "source_age_mismatch" in _codes(
        IndependentOracleValidator(tick_size=1, require_all_contracts=False).validate(tables)
    )


def _outcome_tables() -> dict[str, pa.Table]:
    books = [
        _book("b0", ts=T0, bid=100, ask=101),
        _book("b1", ts=T0 + timedelta(seconds=1), bid=103, ask=104),
        _book("b2", ts=T0 + timedelta(seconds=2), bid=98, ask=99),
        _book("b3", ts=T0 + timedelta(seconds=3), bid=102, ask=103),
    ]
    row = {
        "outcome_id": "o",
        "candidate_id": "c",
        "simulation_id": "s",
        "side": "LONG",
        "entry_ts": T0,
        "entry_price": 100.0,
        "target_ts": T0 + timedelta(seconds=3),
        "price_source_event_id": "b3",
        "outcome_complete": True,
        "executable_exit_price": 102.0,
        "mfe_ticks": 3.0,
        "mae_ticks": 2.0,
        "mfe_source_event_id": "b1",
        "mae_source_event_id": "b2",
        "gross_pnl": 2.0,
    }
    return {"raw_orderbook": _table(books), "future_outcomes": _table([row])}


@pytest.mark.parametrize(
    ("field", "code"),
    [
        ("price_source_event_id", "outcome_price_mismatch"),
        ("mfe_ticks", "mfe_mismatch"),
        ("mae_ticks", "mae_mismatch"),
        ("gross_pnl", "outcome_pnl_mismatch"),
    ],
)
def test_outcome_regressions_fail(field: str, code: str) -> None:
    tables = _outcome_tables()
    rows = tables["future_outcomes"].to_pylist()
    rows[0][field] = "b1" if field.endswith("id") else 999.0
    tables["future_outcomes"] = _table(rows)
    assert code in _codes(
        IndependentOracleValidator(tick_size=1, require_all_contracts=False).validate(tables)
    )


def _stop_tables() -> dict[str, pa.Table]:
    books = [
        _book("pre", ts=T0, bid=100, ask=101),
        _book("hit", ts=T0 + timedelta(seconds=1), bid=97, ask=98),
    ]
    execution = {
        "simulation_id": "s",
        "candidate_id": "c",
        "feature_snapshot_id": "f",
        "side": "LONG",
        "order_ts": T0,
        "fill_ts": T0,
        "fill_status": "FULL_FILL",
        "filled_quantity": 1.0,
        "fill_price": 100.0,
    }
    stop = {
        "result_id": "r",
        "candidate_id": "c",
        "simulation_id": "s",
        "side": "LONG",
        "entry_ts": T0,
        "exit_ts": T0 + timedelta(seconds=1),
        "exit_source_event_id": "hit",
        "stop_level": 98.0,
        "stop_triggered": True,
        "trigger_event_id": "hit",
        "trigger_ts": T0 + timedelta(seconds=1),
        "exit_price": 97.0,
        "gap_through_stop": True,
    }
    return {
        "raw_orderbook": _table(books),
        "execution_simulations": _table([execution]),
        "shadow_stop_results": _table([stop]),
    }


def test_stop_trigger_missing_fails() -> None:
    tables = _stop_tables()
    rows = tables["shadow_stop_results"].to_pylist()
    rows[0]["trigger_event_id"] = None
    tables["shadow_stop_results"] = _table(rows)
    assert "stop_trigger_mismatch" in _codes(
        IndependentOracleValidator(tick_size=1, require_all_contracts=False).validate(tables)
    )


def test_stop_before_fill_fails() -> None:
    tables = _stop_tables()
    rows = tables["shadow_stop_results"].to_pylist()
    rows[0]["trigger_ts"] = T0 - timedelta(seconds=1)
    tables["shadow_stop_results"] = _table(rows)
    assert "stop_trigger_mismatch" in _codes(
        IndependentOracleValidator(tick_size=1, require_all_contracts=False).validate(tables)
    )


def test_unexplained_identical_stops_fail() -> None:
    tables = _stop_tables()
    rows = tables["shadow_stop_results"].to_pylist()
    second = dict(rows[0], result_id="r2", stop_level=99.0, gap_through_stop=False)
    rows[0]["gap_through_stop"] = False
    tables["shadow_stop_results"] = _table([rows[0], second])
    assert "unexplained_identical_stops" in _codes(
        IndependentOracleValidator(tick_size=1, require_all_contracts=False).validate(tables)
    )


def test_exit_before_entry_fails_contract() -> None:
    row = {
        "result_id": "r",
        "candidate_id": "c",
        "simulation_id": "s",
        "side": "LONG",
        "entry_ts": T0,
        "exit_ts": T0 - timedelta(seconds=1),
        "exit_source_event_id": None,
    }
    assert "exit_before_entry" in {
        v.code
        for v in validate_data_contracts({"shadow_exit_results": _table([row])}, require_all=False)
    }


def test_foreign_key_failure_is_reported() -> None:
    row = {
        "candidate_id": "c",
        "feature_snapshot_id": "missing",
        "side": "LONG",
        "candidate_ts": T0,
    }
    result = validate_data_contracts(
        {"candidate_events": _table([row]), "feature_snapshots": _table([])}, require_all=False
    )
    assert "foreign_key" in {v.code for v in result}


def test_complete_outcome_without_source_fails_contract() -> None:
    row = {
        "outcome_id": "o",
        "candidate_id": "c",
        "simulation_id": "s",
        "side": "LONG",
        "entry_ts": T0,
        "entry_price": 100.0,
        "target_ts": T0 + timedelta(seconds=5),
        "price_source_event_id": None,
        "outcome_complete": True,
        "executable_exit_price": None,
        "mfe_ticks": None,
        "mae_ticks": None,
        "mfe_source_event_id": None,
        "mae_source_event_id": None,
    }
    violations = validate_data_contracts({"future_outcomes": _table([row])}, require_all=False)
    assert "incomplete_outcome_marked_complete" in {item.code for item in violations}


def test_identical_exit_models_fail() -> None:
    book = _book("exit", ts=T0 + timedelta(seconds=1), bid=99.0, ask=100.0)
    common = {
        "candidate_id": "c",
        "simulation_id": "s",
        "side": "LONG",
        "entry_ts": T0,
        "exit_ts": T0 + timedelta(seconds=1),
        "exit_source_event_id": "exit",
        "exit_price": 99.0,
        "exit_reason": "same",
    }
    exits = [
        {**common, "result_id": "e1", "exit_variant": "time"},
        {**common, "result_id": "e2", "exit_variant": "trailing"},
    ]
    result = IndependentOracleValidator(tick_size=1, require_all_contracts=False).validate(
        {"raw_orderbook": _table([book]), "shadow_exit_results": _table(exits)}
    )
    assert "identical_exit_models" in _codes(result)


def test_fixed_artificial_passive_delay_fails() -> None:
    trades = []
    simulations = []
    for index in range(3):
        trade = {
            "event_id": f"trade-{index}",
            "exchange_ts": T0 + timedelta(seconds=index + 1),
            "receive_ts": T0 + timedelta(seconds=index + 1),
            "processing_ts": T0 + timedelta(seconds=index + 1),
            "materialized_ts": T0,
            "price": 100.0,
            "quantity": 1.0,
            "aggressor_side": "SELL",
        }
        trades.append(trade)
        simulations.append(
            {
                "simulation_id": f"s-{index}",
                "candidate_id": f"c-{index}",
                "feature_snapshot_id": f"f-{index}",
                "side": "LONG",
                "execution_model": "passive_queue",
                "order_ts": T0 + timedelta(seconds=index),
                "requested_quantity": 1.0,
                "filled_quantity": 1.0,
                "remaining_quantity": 0.0,
                "fill_status": "FULL",
                "fill_ts": T0 + timedelta(seconds=index + 1),
                "fill_price": 100.0,
                "fill_lineage": [
                    {"source_event_id": f"trade-{index}", "price": 100.0, "quantity": 1.0}
                ],
            }
        )
    result = IndependentOracleValidator(tick_size=1, require_all_contracts=False).validate(
        {"raw_trades": _table(trades), "execution_simulations": _table(simulations)}
    )
    assert "artificial_passive_delay" in _codes(result)


def test_secret_in_any_table_fails() -> None:
    table = _table([{"message": {"nested": ["Authorization: Bearer abcdefghijklmnopqrstuvwxyz"]}}])
    result = IndependentOracleValidator(tick_size=1, require_all_contracts=False).validate(
        {"audit": table}
    )
    assert "secret_detected" in _codes(result)


def test_oracle_source_has_no_production_business_imports() -> None:
    from pathlib import Path

    source = (
        Path(__file__).parents[1] / "neo_trader/neobitcoin_research/independent_oracle/validator.py"
    ).read_text(encoding="utf-8")
    forbidden = (
        "review_datasets",
        "schema_v4.features",
        "schema_v4.simulation",
        "CanonicalTimeline",
        "FeatureMaterializer",
        "ExecutionSimulator",
        "OutcomeMaterializer",
        "StopAndExitSimulator",
    )
    assert not any(name in source for name in forbidden)


def _candle(
    event_id: str,
    interval_minutes: int,
    *,
    end: datetime = T0,
    receive: datetime | None = None,
    revision: int = 1,
    source_complete: bool = False,
    close: float = 100.0,
) -> dict[str, object]:
    receive_ts = receive or end
    return {
        "event_id": event_id,
        "exchange_ts": end - timedelta(minutes=interval_minutes),
        "receive_ts": receive_ts,
        "processing_ts": receive_ts,
        "materialized_ts": T0 + timedelta(hours=1),
        "candle_start": end - timedelta(minutes=interval_minutes),
        "candle_end": end,
        "is_complete": source_complete,
        "revision": revision,
        "open": close - 1,
        "high": close + 1,
        "low": close - 2,
        "close": close,
        "volume": 10.0 + revision,
    }


def test_canonical_candle_ignores_false_source_flag_after_interval_elapsed() -> None:
    tables = _base_feature_tables()
    candle = _candle("elapsed", 1, end=T0 - timedelta(seconds=1), source_complete=False)
    tables["candles_1m"] = _table([candle])
    feature = tables["feature_snapshots"].to_pylist()[0]
    feature["candle_1m_source_event_id"] = "elapsed"
    feature["candle_volume_1m"] = 11.0
    tables["feature_snapshots"] = _table([feature])
    result = IndependentOracleValidator(tick_size=1, require_all_contracts=False).validate(tables)
    assert "candle_source_mismatch" not in _codes(result)


def test_canonical_candle_excludes_revision_received_after_processing_cutoff() -> None:
    tables = _base_feature_tables()
    old = _candle("old", 1, end=T0 - timedelta(seconds=1), revision=1)
    future = _candle(
        "future-revision",
        1,
        end=T0 - timedelta(seconds=1),
        receive=T0 + timedelta(seconds=1),
        revision=2,
    )
    tables["candles_1m"] = _table([old, future])
    feature = tables["feature_snapshots"].to_pylist()[0]
    feature["candle_1m_source_event_id"] = "old"
    feature["candle_volume_1m"] = 11.0
    tables["feature_snapshots"] = _table([feature])
    result = IndependentOracleValidator(tick_size=1, require_all_contracts=False).validate(tables)
    assert "candle_source_mismatch" not in _codes(result)


def test_latest_available_candle_revision_and_volume_are_reproduced() -> None:
    tables = _base_feature_tables()
    first = _candle("r1", 1, end=T0 - timedelta(seconds=1), revision=1)
    second = _candle("r2", 1, end=T0 - timedelta(seconds=1), revision=2)
    tables["candles_1m"] = _table([first, second])
    feature = tables["feature_snapshots"].to_pylist()[0]
    feature["candle_1m_source_event_id"] = "r2"
    feature["candle_volume_1m"] = 999.0
    tables["feature_snapshots"] = _table([feature])
    result = IndependentOracleValidator(tick_size=1, require_all_contracts=False).validate(tables)
    assert "candle_value_mismatch" in _codes(result)


def test_first_ofi_snapshot_cannot_be_feature_ready_in_v41() -> None:
    tables = _base_feature_tables()
    feature = tables["feature_snapshots"].to_pylist()[0]
    feature.update(
        schema_version="schema-v4.1",
        ofi=-1.0,
        ofi_delta=None,
        ofi_source_event_id="book-0",
        ofi_previous_event_id=None,
        ofi_continuity_valid=False,
        ofi_reset_reason="previous_snapshot_absent",
        missing_fields=[],
        readiness_checks_passed=0,
        readiness_checks_total=0,
        readiness_version="v4.1",
    )
    tables["feature_snapshots"] = _table([feature])
    result = IndependentOracleValidator(tick_size=1, require_all_contracts=False).validate(tables)
    assert "feature_readiness_mismatch" in _codes(result)


def test_decimal_spread_exactly_three_ticks_passes_integer_gate() -> None:
    book = _book(ask=100.3)
    feature = _feature(book)
    feature.update(
        spread=0.3,
        spread_price=0.3,
        tick_size=0.1,
        spread_ticks_decimal=3.000000000029104,
        spread_ticks_int=3,
        tick_grid_error=0.0,
        spread_threshold_ticks=3,
        spread_gate_passed=True,
    )
    decision = {
        "filter_decision_id": "fd",
        "candidate_id": "c",
        "feature_snapshot_id": "feature-0",
        "filter_name": "spread_gate",
        "passed": True,
    }
    result = IndependentOracleValidator(tick_size=0.1, require_all_contracts=False).validate(
        {
            "raw_orderbook": _table([book]),
            "feature_snapshots": _table([feature]),
            "filter_decisions": _table([decision]),
        }
    )
    assert "spread_ticks_mismatch" not in _codes(result)
    assert "spread_gate_mismatch" not in _codes(result)


def test_four_tick_spread_fails_three_tick_gate() -> None:
    book = _book(ask=100.4)
    feature = _feature(book)
    feature.update(spread_ticks_int=4, spread_threshold_ticks=3)
    decision = {
        "filter_decision_id": "fd",
        "candidate_id": "c",
        "feature_snapshot_id": "feature-0",
        "filter_name": "spread_gate",
        "passed": True,
    }
    result = IndependentOracleValidator(tick_size=0.1, require_all_contracts=False).validate(
        {
            "raw_orderbook": _table([book]),
            "feature_snapshots": _table([feature]),
            "filter_decisions": _table([decision]),
        }
    )
    assert "spread_gate_mismatch" in _codes(result)


def test_off_grid_spread_requires_data_quality_flag() -> None:
    book = _book(ask=100.35)
    feature = _feature(book)
    result = IndependentOracleValidator(tick_size=0.1, require_all_contracts=False).validate(
        {"raw_orderbook": _table([book]), "feature_snapshots": _table([feature])}
    )
    assert "unflagged_tick_grid_error" in _codes(result)


def test_atr_is_recomputed_from_point_in_time_canonical_sequence() -> None:
    tables = _base_feature_tables()
    candles = [
        _candle("c0", 1, end=T0 - timedelta(minutes=2), close=99.0),
        _candle("c1", 1, end=T0 - timedelta(minutes=1), close=100.0),
        _candle("c2", 1, end=T0, close=101.0),
    ]
    tables["candles_1m"] = _table(candles)
    feature = tables["feature_snapshots"].to_pylist()[0]
    feature.update(
        candle_1m_source_event_id="c2",
        candle_volume_1m=11.0,
        return_1m=0.01,
        atr_1m=999.0,
        atr_1m_warmup_remaining=0,
    )
    tables["feature_snapshots"] = _table([feature])
    result = IndependentOracleValidator(tick_size=1, require_all_contracts=False).validate(tables)
    assert "candle_atr_mismatch" in _codes(result)


def test_valid_following_snapshot_has_reproducible_ofi_delta() -> None:
    first = _book("book-0", ts=T0 - timedelta(seconds=1))
    second = _book("book-1", ts=T0)
    second["bids"][0]["quantity"] = 5.0
    feature = _feature(second)
    feature["orderbook_source_event_id"] = "book-1"
    feature.update(
        ofi=3.0,
        ofi_delta=4.0,
        ofi_source_event_id="book-1",
        ofi_previous_event_id="book-0",
        ofi_continuity_valid=True,
        ofi_reset_reason=None,
    )
    result = IndependentOracleValidator(tick_size=1, require_all_contracts=False).validate(
        {"raw_orderbook": _table([first, second]), "feature_snapshots": _table([feature])}
    )
    assert "ofi_continuity_mismatch" not in _codes(result)


def test_stale_candle_cannot_be_ready_in_v41() -> None:
    tables = _base_feature_tables()
    old = _candle("old", 1, end=T0 - timedelta(minutes=3))
    tables["candles_1m"] = _table([old])
    feature = tables["feature_snapshots"].to_pylist()[0]
    feature.update(
        schema_version="schema-v4.1",
        candle_1m_source_event_id="old",
        candle_1m_stale=False,
        candle_volume_1m=11.0,
        ofi_source_event_id="book-0",
        ofi_continuity_valid=False,
        missing_fields=[],
        readiness_checks_passed=0,
        readiness_checks_total=0,
        readiness_version="schema-v4.1",
    )
    tables["feature_snapshots"] = _table([feature])
    result = IndependentOracleValidator(tick_size=1, require_all_contracts=False).validate(tables)
    assert "candle_lineage_mismatch" in _codes(result)
    assert "feature_readiness_mismatch" in _codes(result)
