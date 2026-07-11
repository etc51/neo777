from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pyarrow as pa

from neo_trader.neobitcoin_research.review_reconciliation import (
    TRADE_WINDOWS,
    reconcile_review_tables,
)


def _table(rows: list[dict[str, object]]) -> pa.Table:
    return pa.Table.from_pylist(rows)


def _valid_tables() -> dict[str, pa.Table]:
    ts = datetime(2026, 7, 11, 12, tzinfo=UTC)
    processing = ts + timedelta(milliseconds=250)
    trade = {
        "event_id": "trade-1",
        "trade_id": "tid-1",
        "exchange_ts": ts - timedelta(seconds=2),
        "receive_ts": ts - timedelta(milliseconds=100),
        "quantity": 3.0,
        "aggressor_side": "BUY",
    }
    feature: dict[str, object] = {
        "event_id": "feature-1",
        "exchange_ts": ts,
        "processing_ts": processing,
        "feature_ready": True,
        "orderbook_source_event_id": "book-1",
        "last_price_source_event_id": "last-1",
        "last_trade_source_event_id": "trade-1",
        "candle_1m_source_event_id": "c1-1",
        "candle_5m_source_event_id": "c5-1",
        "candle_15m_source_event_id": "c15-1",
    }
    receive_times = {
        "orderbook": ts - timedelta(milliseconds=50),
        "last_price": ts - timedelta(milliseconds=75),
        "last_trade": trade["receive_ts"],
        "candle_1m": ts - timedelta(milliseconds=125),
        "candle_5m": ts - timedelta(milliseconds=150),
        "candle_15m": ts - timedelta(milliseconds=175),
    }
    for name, receive in receive_times.items():
        feature[f"{name}_age_ms"] = (processing - receive).total_seconds() * 1000
    ages = [float(feature[f"{name}_age_ms"]) for name in receive_times]
    feature["oldest_required_source_age_ms"] = max(ages)
    feature["newest_source_age_ms"] = min(ages)
    feature["source_exchange_age_ms"] = 2000.0
    feature["book_age_ms"] = feature["orderbook_age_ms"]
    feature["data_age_ms"] = min(ages)
    for seconds in TRADE_WINDOWS:
        feature.update(
            {
                f"trade_count_{seconds}s": 1,
                f"known_side_trade_count_{seconds}s": 1,
                f"unknown_side_trade_count_{seconds}s": 0,
                f"buy_volume_{seconds}s": 3.0,
                f"sell_volume_{seconds}s": 0.0,
                f"trade_flow_{seconds}s": 3.0,
                f"trade_window_first_event_id_{seconds}s": "trade-1",
                f"trade_window_last_event_id_{seconds}s": "trade-1",
            }
        )
    return {
        "feature_snapshots": _table([feature]),
        "raw_trades": _table([trade]),
        "raw_orderbook": _table(
            [
                {
                    "event_id": "book-1",
                    "exchange_ts": ts,
                    "receive_ts": receive_times["orderbook"],
                    "best_bid": 99.0,
                    "best_ask": 101.0,
                }
            ]
        ),
        "raw_last_price": _table(
            [
                {
                    "event_id": "last-1",
                    "exchange_ts": ts,
                    "receive_ts": receive_times["last_price"],
                    "last_price": 100.0,
                }
            ]
        ),
        "candles_1m": _table(
            [{"event_id": "c1-1", "exchange_ts": ts, "receive_ts": receive_times["candle_1m"]}]
        ),
        "candles_5m": _table(
            [{"event_id": "c5-1", "exchange_ts": ts, "receive_ts": receive_times["candle_5m"]}]
        ),
        "candles_15m": _table(
            [{"event_id": "c15-1", "exchange_ts": ts, "receive_ts": receive_times["candle_15m"]}]
        ),
        "execution_simulations": _table([]),
        "future_outcomes": _table([]),
        "shadow_stop_results": _table([]),
    }


def test_independent_reconciliation_accepts_exact_trade_flow_and_real_ages() -> None:
    result = reconcile_review_tables(_valid_tables())

    assert result["status"] == "PASS"
    assert all(item["matched"] == 1 for item in result["trade_flow"].values())
    age_items = [item for name, item in result["source_ages"].items() if name.endswith("_age_ms")]
    assert all(item["reconciled"] == 1 for item in age_items)


def _assert_flow_mismatch(seconds: int) -> None:
    tables = _valid_tables()
    rows = tables["feature_snapshots"].to_pylist()
    rows[0][f"trade_flow_{seconds}s"] = 999.0
    tables["feature_snapshots"] = _table(rows)
    result = reconcile_review_tables(tables)
    assert result["status"] == "FAIL"
    assert result["trade_flow"][f"{seconds}s"]["mismatched"] == 1


def test_trade_flow_5s_mismatch_fails() -> None:
    _assert_flow_mismatch(5)


def test_trade_flow_900s_single_row_mismatch_fails() -> None:
    _assert_flow_mismatch(900)


def test_unknown_trade_is_counted_but_never_signed() -> None:
    tables = _valid_tables()
    trades = tables["raw_trades"].to_pylist()
    unknown = dict(
        trades[0], event_id="trade-2", trade_id="tid-2", quantity=8.0, aggressor_side="UNKNOWN"
    )
    tables["raw_trades"] = _table([*trades, unknown])

    result = reconcile_review_tables(tables)

    assert result["status"] == "FAIL"
    assert result["trade_flow"]["5s"]["mismatched"] == 1


def test_artificial_zero_age_fails() -> None:
    tables = _valid_tables()
    rows = tables["feature_snapshots"].to_pylist()
    rows[0]["orderbook_age_ms"] = 0.0
    tables["feature_snapshots"] = _table(rows)
    result = reconcile_review_tables(tables)
    assert result["status"] == "FAIL"
    assert result["source_ages"]["orderbook_age_ms"]["mismatched"] == 1


def test_missing_source_lineage_id_fails() -> None:
    tables = _valid_tables()
    rows = tables["feature_snapshots"].to_pylist()
    rows[0]["last_price_source_event_id"] = "does-not-exist"
    tables["feature_snapshots"] = _table(rows)
    result = reconcile_review_tables(tables)
    assert result["status"] == "FAIL"
    assert result["source_ages"]["last_price_age_ms"]["missing_source_ids"] == 1


def test_support_is_based_on_max_fill_not_candidate_end() -> None:
    tables = _valid_tables()
    ts = datetime(2026, 7, 11, 12, tzinfo=UTC)
    tables["execution_simulations"] = _table(
        [{"simulation_id": "sim", "fill_ts": ts + timedelta(seconds=26)}]
    )
    # The old candidate-end boundary reaches 12:30, but max fill requires 12:30:26.
    for dataset in ("raw_orderbook", "raw_trades", "raw_last_price"):
        rows = tables[dataset].to_pylist()
        rows.append(
            dict(
                rows[-1],
                event_id=f"{dataset}-end",
                exchange_ts=ts + timedelta(minutes=30),
                receive_ts=ts + timedelta(minutes=30),
            )
        )
        tables[dataset] = _table(rows)

    result = reconcile_review_tables(tables)

    assert result["status"] == "FAIL"
    assert result["raw_support"]["required_support_end"].endswith("12:30:26+00:00")
    assert any("raw support ends before" in error for error in result["errors"])


def test_triggered_stop_without_raw_trigger_fails() -> None:
    tables = _valid_tables()
    ts = datetime(2026, 7, 11, 12, tzinfo=UTC)
    tables["shadow_stop_results"] = _table(
        [
            {
                "simulation_id": "sim",
                "side": "LONG",
                "entry_ts": ts,
                "entry_price": 100.0,
                "stop_ticks": 2,
                "stop_level": 99.0,
                "stop_triggered": True,
                "trigger_event_id": None,
                "exit_source_event_id": None,
                "exit_ts": ts,
                "exit_price": 99.0,
                "exit_reason": "stop_triggered",
            }
        ]
    )

    result = reconcile_review_tables(tables)

    assert result["status"] == "FAIL"
    assert result["stops"]["missing_trigger_id_count"] == 1


def _identical_gap_tables(gap: bool) -> dict[str, pa.Table]:
    tables = _valid_tables()
    ts = datetime(2026, 7, 11, 12, tzinfo=UTC)
    trigger_ts = ts + timedelta(seconds=1)
    trigger = {
        "event_id": "gap-book",
        "exchange_ts": trigger_ts,
        "receive_ts": trigger_ts,
        "best_bid": 95.0,
        "best_ask": 96.0,
        "revision": 7,
    }
    books = tables["raw_orderbook"].to_pylist()
    support_end = dict(
        trigger,
        event_id="support-end-book",
        exchange_ts=ts + timedelta(minutes=30),
        receive_ts=ts + timedelta(minutes=30),
        best_bid=100.0,
        best_ask=101.0,
        revision=8,
    )
    tables["raw_orderbook"] = _table([*books, trigger, support_end])
    for dataset in ("raw_trades", "raw_last_price"):
        rows = tables[dataset].to_pylist()
        end = dict(rows[-1])
        end["event_id"] = f"support-end-{dataset}"
        end["exchange_ts"] = ts + timedelta(minutes=30)
        end["receive_ts"] = ts + timedelta(minutes=30)
        if dataset == "raw_trades":
            end["trade_id"] = "support-end-trade"
        tables[dataset] = _table([*rows, end])
    tables["execution_simulations"] = _table(
        [{"simulation_id": "sim", "fill_ts": ts, "spread_cost": 0.0}]
    )
    stop_rows = []
    for ticks, level in ((2, 98.0), (3, 97.0)):
        stop_rows.append(
            {
                "simulation_id": "sim",
                "side": "LONG",
                "entry_ts": ts,
                "entry_price": 100.0,
                "stop_ticks": ticks,
                "stop_level": level,
                "stop_triggered": True,
                "trigger_event_id": "gap-book",
                "trigger_event_type": "raw_orderbook",
                "trigger_ts": trigger_ts,
                "trigger_price": 95.0,
                "trigger_bid": 95.0,
                "trigger_ask": 96.0,
                "trigger_sequence": 0,
                "exit_source_event_id": "gap-book",
                "exit_source_ts": trigger_ts,
                "exit_ts": trigger_ts,
                "exit_price": 95.0,
                "exit_reason": "stop_triggered",
                "gap_through_stop": gap,
                "gross_pnl": -5.0,
                "net_pnl": -5.0,
                "holding_ms": 1000,
            }
        )
    tables["shadow_stop_results"] = _table(stop_rows)
    return tables


def test_identical_stops_with_confirmed_gap_pass() -> None:
    result = reconcile_review_tables(_identical_gap_tables(True))
    explained = result["stops"]
    assert result["status"] == "PASS"
    assert explained["gap_explained_identical_count"] == 1
    assert explained["unexplained_identical_count"] == 0


def test_identical_stops_without_confirmed_gap_fail() -> None:
    result = reconcile_review_tables(_identical_gap_tables(False))
    unexplained = result["stops"]
    assert result["status"] == "FAIL"
    assert unexplained["gap_explained_identical_count"] == 0
    assert unexplained["unexplained_identical_count"] == 1


def test_future_trade_in_published_flow_fails() -> None:
    tables = _valid_tables()
    rows = tables["feature_snapshots"].to_pylist()
    rows[0]["trade_count_5s"] = 2
    rows[0]["buy_volume_5s"] = 10.0
    rows[0]["trade_flow_5s"] = 10.0
    tables["feature_snapshots"] = _table(rows)
    assert reconcile_review_tables(tables)["trade_flow"]["5s"]["status"] == "FAIL"


def test_empty_trade_window_null_instead_of_zero_fails() -> None:
    tables = _valid_tables()
    tables["raw_trades"] = _table([])
    rows = tables["feature_snapshots"].to_pylist()
    rows[0]["trade_flow_5s"] = None
    tables["feature_snapshots"] = _table(rows)
    assert reconcile_review_tables(tables)["trade_flow"]["5s"]["status"] == "FAIL"


def _outcome_tables(*, source_id: str = "book-1", exit_price: float = 99.0) -> dict[str, pa.Table]:
    tables = _valid_tables()
    ts = datetime(2026, 7, 11, 12, tzinfo=UTC)
    tables["future_outcomes"] = _table(
        [
            {
                "entry_ts": ts,
                "entry_price": 100.0,
                "target_ts": ts,
                "future_ts": ts,
                "side": "LONG",
                "outcome_complete": True,
                "price_source_event_id": source_id,
                "price_source": "raw_orderbook",
                "price_source_exchange_ts": ts,
                "price_source_receive_ts": ts - timedelta(milliseconds=50),
                "price_selection_method": "last_executable_orderbook_quote_at_or_before_target",
                "future_mid_price": 100.0,
                "executable_exit_price": exit_price,
                "future_price": exit_price,
                "exit_bid": 99.0,
                "exit_ask": 101.0,
                "support_complete": True,
                "gap_detected": False,
                "raw_return": -1.0,
                "return_percent": -1.0,
                "gross_pnl": -1.0,
                "net_pnl": -1.0,
            }
        ]
    )
    return tables


def test_outcome_target_after_raw_support_fails() -> None:
    tables = _outcome_tables()
    rows = tables["future_outcomes"].to_pylist()
    rows[0]["target_ts"] += timedelta(seconds=1)
    rows[0]["future_ts"] += timedelta(seconds=1)
    tables["future_outcomes"] = _table(rows)
    result = reconcile_review_tables(tables)
    assert result["outcomes"]["outside_support_count"] == 1


def test_outcome_missing_price_source_id_fails() -> None:
    result = reconcile_review_tables(_outcome_tables(source_id="missing"))
    assert result["outcomes"]["missing_source_id_count"] == 1


def test_outcome_price_mismatch_source_event_fails() -> None:
    result = reconcile_review_tables(_outcome_tables(exit_price=98.0))
    assert result["outcomes"]["reproduced_prices"] == 0


def test_book_age_null_when_feature_ready_fails() -> None:
    tables = _valid_tables()
    rows = tables["feature_snapshots"].to_pylist()
    rows[0]["book_age_ms"] = None
    rows[0]["orderbook_age_ms"] = None
    tables["feature_snapshots"] = _table(rows)
    result = reconcile_review_tables(tables)
    assert result["status"] == "FAIL"


def test_source_age_timestamp_mismatch_fails() -> None:
    tables = _valid_tables()
    rows = tables["feature_snapshots"].to_pylist()
    rows[0]["candle_5m_age_ms"] = 123456.0
    tables["feature_snapshots"] = _table(rows)
    result = reconcile_review_tables(tables)
    assert result["source_ages"]["candle_5m_age_ms"]["mismatched"] == 1


def test_stop_trigger_before_fill_fails() -> None:
    tables = _identical_gap_tables(True)
    executions = tables["execution_simulations"].to_pylist()
    executions[0]["fill_ts"] += timedelta(seconds=2)
    tables["execution_simulations"] = _table(executions)
    result = reconcile_review_tables(tables)
    assert result["stops"]["reproduced_count"] == 0


def test_trigger_price_not_crossing_stop_fails() -> None:
    tables = _identical_gap_tables(True)
    books = tables["raw_orderbook"].to_pylist()
    next(row for row in books if row["event_id"] == "gap-book")["best_bid"] = 99.0
    tables["raw_orderbook"] = _table(books)
    result = reconcile_review_tables(tables)
    assert result["stops"]["reproduced_count"] == 0
