from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
from test_neobitcoin_review_bundle import _source

from neo_trader.neobitcoin_research.review_bundle import DATASETS, _read_dataset, _validate_tables

T0 = datetime(2026, 7, 11, 10, 50, tzinfo=UTC)
T1 = T0 + timedelta(minutes=10)
RAW_START = T0 - timedelta(seconds=60)
RAW_END = T1 + timedelta(minutes=30)


def _rows(table: pa.Table) -> list[dict[str, Any]]:
    return table.to_pylist()


def _replace(table: pa.Table, rows: list[dict[str, Any]]) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=table.schema)


def _clone_at(
    table: pa.Table,
    when: datetime,
    suffix: str,
    *,
    trade: bool = False,
    last: bool = False,
) -> dict[str, Any]:
    row = _rows(table)[-1 if last else 0].copy()
    row["event_id"] = f"{row['event_id']}-{suffix}"
    row["exchange_ts"] = when
    row["receive_ts"] = when
    row["processing_ts"] = when
    if trade:
        row["trade_id"] = f"{row['trade_id']}-{suffix}"
    return row


def _valid_tables(tmp_path: Path) -> dict[str, pa.Table]:
    source = tmp_path / "review_parquet"
    _source(source)
    tables = {name: _read_dataset(source, name) for name in DATASETS}

    for dataset in ("raw_orderbook", "raw_trades", "raw_last_price"):
        rows = _rows(tables[dataset])
        rows.extend(
            (
                _clone_at(
                    tables[dataset],
                    T0 - timedelta(seconds=900) if dataset == "raw_trades" else RAW_START,
                    "coverage-start",
                    trade=dataset == "raw_trades",
                ),
                _clone_at(
                    tables[dataset],
                    RAW_END + timedelta(seconds=2),
                    "coverage-end",
                    trade=dataset == "raw_trades",
                    last=True,
                ),
            )
        )
        tables[dataset] = _replace(tables[dataset], rows)

    feature_rows = _rows(tables["feature_snapshots"])
    trade_rows = _rows(tables["raw_trades"])
    for row in feature_rows:
        feature_ts = row["exchange_ts"]
        for seconds in (5, 10, 30, 60, 180, 300, 900):
            lower = feature_ts - timedelta(seconds=seconds)
            window = sorted(
                (
                    trade
                    for trade in trade_rows
                    if lower < trade["exchange_ts"] <= feature_ts
                ),
                key=lambda trade: (trade["exchange_ts"], trade["event_id"]),
            )
            buys = [trade for trade in window if trade["aggressor_side"] == "BUY"]
            sells = [trade for trade in window if trade["aggressor_side"] == "SELL"]
            unknown = [
                trade
                for trade in window
                if trade["aggressor_side"] not in {"BUY", "SELL"}
            ]
            suffix = f"{seconds}s"
            row[f"trade_count_{suffix}"] = len(window)
            row[f"known_side_trade_count_{suffix}"] = len(buys) + len(sells)
            row[f"unknown_side_trade_count_{suffix}"] = len(unknown)
            row[f"buy_volume_{suffix}"] = sum(float(trade["quantity"]) for trade in buys)
            row[f"sell_volume_{suffix}"] = sum(float(trade["quantity"]) for trade in sells)
            row[f"trade_flow_{suffix}"] = (
                row[f"buy_volume_{suffix}"] - row[f"sell_volume_{suffix}"]
            )
            row[f"trade_window_first_event_id_{suffix}"] = (
                window[0]["event_id"] if window else None
            )
            row[f"trade_window_last_event_id_{suffix}"] = (
                window[-1]["event_id"] if window else None
            )
        row.update(
            feature_ready=True,
            missing_reason=None,
            warmup_remaining=0,
            source_status="NORMAL_TRADING",
        )
    tables["feature_snapshots"] = _replace(tables["feature_snapshots"], feature_rows)

    candidate_rows = _rows(tables["candidate_events"])
    candidate_rows[0].update(
        allowed=True,
        final_decision="ACCEPTED",
        rejection_reason=None,
        feature_ready=True,
    )
    candidate_rows[1].update(
        allowed=False,
        final_decision="REJECTED",
        rejection_reason="counterfactual_side_not_signaled",
        feature_ready=True,
    )
    tables["candidate_events"] = _replace(tables["candidate_events"], candidate_rows)

    execution_rows = _rows(tables["execution_simulations"])
    passive = next(
        row for row in execution_rows if row["execution_model"] == "passive_conservative"
    )
    ideal = next(
        row
        for row in execution_rows
        if row["execution_model"] == "ideal_touch"
        and row["candidate_id"] == passive["candidate_id"]
    )
    requested = float(passive["requested_quantity"])
    passive.update(
        fill_status="PARTIAL_FILL",
        filled_quantity=requested / 2,
        remaining_quantity=requested / 2,
        fill_ts=ideal["fill_ts"],
        fill_price=passive["order_price"],
        fill_delay_ms=1234,
        slippage_ticks=0.0,
        slippage_percent=0.0,
        no_fill_reason=None,
    )
    tables["execution_simulations"] = _replace(tables["execution_simulations"], execution_rows)

    for dataset, primary_key in (
        ("future_outcomes", "outcome_id"),
        ("shadow_stop_results", "result_id"),
        ("shadow_exit_results", "result_id"),
    ):
        rows = _rows(tables[dataset])
        clones: list[dict[str, Any]] = []
        for row in rows:
            if row["simulation_id"] != ideal["simulation_id"]:
                continue
            clone = row.copy()
            clone["simulation_id"] = passive["simulation_id"]
            clone[primary_key] = f"{clone[primary_key]}-partial"
            clones.append(clone)
        tables[dataset] = _replace(tables[dataset], rows + clones)

    stop_rows = _rows(tables["shadow_stop_results"])
    for row in stop_rows:
        row["mfe_before_exit"] = abs(float(row["mfe_before_exit"]))
        row["mae_before_exit"] = abs(float(row["mae_before_exit"]))
    tables["shadow_stop_results"] = _replace(tables["shadow_stop_results"], stop_rows)

    outcome_rows = _rows(tables["future_outcomes"])
    for row in outcome_rows:
        for name in ("mfe_ticks", "mae_ticks", "mfe_percent", "mae_percent"):
            row[name] = abs(float(row[name]))
    tables["future_outcomes"] = _replace(tables["future_outcomes"], outcome_rows)

    offsets = {
        "take_profit": 1,
        "time_exit": 2,
        "breakeven": 3,
        "dynamic_breakeven": 4,
        "trailing": 5,
        "microstructure": 6,
        "orderbook": 7,
    }
    exit_rows = _rows(tables["shadow_exit_results"])
    for row in exit_rows:
        offset = offsets[row["exit_variant"]]
        row["activation_ts"] = row["entry_ts"] + timedelta(seconds=offset - 1)
        row["exit_ts"] = row["entry_ts"] + timedelta(seconds=offset)
        row["holding_ms"] = offset * 1000
        row["exit_price"] = float(row["entry_price"]) + offset / 1000
        row["exit_reason"] = f"{row['exit_variant']}_trigger"
        row["trigger_value"] = float(offset)
        row["trigger_threshold"] = float(offset) - 0.5
        row["mfe_before_exit"] = abs(float(row["mfe_before_exit"]))
        row["mae_before_exit"] = abs(float(row["mae_before_exit"]))
    tables["shadow_exit_results"] = _replace(tables["shadow_exit_results"], exit_rows)
    return tables


def _validate(tables: dict[str, pa.Table]) -> dict[str, Any]:
    return _validate_tables(tables, T0, T1, T0 - timedelta(hours=6), RAW_END, [])


def _assert_failed(result: dict[str, Any], check: str) -> None:
    statuses = {item["check"]: item["status"] for item in result["checks"]}
    assert result["status"] == "FAIL"
    assert statuses[check] == "FAIL"


def _filter_after(table: pa.Table, when: datetime) -> pa.Table:
    return table.filter(pc.greater(table["receive_ts"], pa.scalar(when)))


def test_raw_trades_start_after_candidate_window_fails(tmp_path: Path) -> None:
    tables = _valid_tables(tmp_path)
    tables["raw_trades"] = _filter_after(tables["raw_trades"], T1)
    _assert_failed(_validate(tables), "raw_trades_coverage")


def test_last_price_starts_after_candidate_window_fails(tmp_path: Path) -> None:
    tables = _valid_tables(tmp_path)
    tables["raw_last_price"] = _filter_after(tables["raw_last_price"], T1)
    _assert_failed(_validate(tables), "raw_last_price_coverage")


def test_all_passive_orders_filled_fails(tmp_path: Path) -> None:
    tables = _valid_tables(tmp_path)
    rows = _rows(tables["execution_simulations"])
    for row in rows:
        if str(row["execution_model"]).startswith("passive"):
            row.update(fill_status="FULL_FILL", fill_ts=row.get("fill_ts") or row["order_ts"])
    tables["execution_simulations"] = _replace(tables["execution_simulations"], rows)
    _assert_failed(_validate(tables), "passive_not_all_full_fill")


def test_fixed_passive_fill_delays_fail(tmp_path: Path) -> None:
    tables = _valid_tables(tmp_path)
    rows = _rows(tables["execution_simulations"])
    delays = {"passive_best": 100, "passive_conservative": 250, "passive_delayed": 500}
    for row in rows:
        if row["execution_model"] in delays:
            delay = delays[row["execution_model"]]
            row.update(
                fill_status="PARTIAL_FILL",
                fill_delay_ms=delay,
                fill_ts=row["order_ts"] + timedelta(milliseconds=delay),
            )
    tables["execution_simulations"] = _replace(tables["execution_simulations"], rows)
    _assert_failed(_validate(tables), "no_fixed_artificial_passive_timing")


def test_exit_before_entry_fails(tmp_path: Path) -> None:
    tables = _valid_tables(tmp_path)
    rows = _rows(tables["shadow_stop_results"])
    rows[0]["exit_ts"] = rows[0]["entry_ts"] - timedelta(microseconds=1)
    tables["shadow_stop_results"] = _replace(tables["shadow_stop_results"], rows)
    _assert_failed(_validate(tables), "exit_not_before_entry")


def test_negative_holding_time_fails(tmp_path: Path) -> None:
    tables = _valid_tables(tmp_path)
    rows = _rows(tables["shadow_exit_results"])
    rows[0]["holding_ms"] = -1
    tables["shadow_exit_results"] = _replace(tables["shadow_exit_results"], rows)
    _assert_failed(_validate(tables), "holding_time_nonnegative")


def test_identical_exit_models_fail(tmp_path: Path) -> None:
    tables = _valid_tables(tmp_path)
    rows = _rows(tables["shadow_exit_results"])
    by_simulation: dict[str, dict[str, Any]] = {}
    signature_fields = (
        "activation_ts",
        "exit_ts",
        "exit_price",
        "exit_reason",
        "trigger_value",
        "trigger_threshold",
    )
    for row in rows:
        baseline = by_simulation.setdefault(row["simulation_id"], row)
        if row["exit_variant"] in {"take_profit", "time_exit"}:
            for field in signature_fields:
                row[field] = baseline[field]
    tables["shadow_exit_results"] = _replace(tables["shadow_exit_results"], rows)
    _assert_failed(_validate(tables), "exit_models_independent")


def test_required_feature_entirely_null_fails(tmp_path: Path) -> None:
    tables = _valid_tables(tmp_path)
    rows = _rows(tables["feature_snapshots"])
    for row in rows:
        row["trade_flow_10s"] = None
        row.update(feature_ready=False, missing_reason="missing trade_flow_10s", warmup_remaining=1)
    tables["feature_snapshots"] = _replace(tables["feature_snapshots"], rows)
    _assert_failed(_validate(tables), "required_features_not_all_null")


def test_feature_ready_with_null_required_feature_fails(tmp_path: Path) -> None:
    tables = _valid_tables(tmp_path)
    rows = _rows(tables["feature_snapshots"])
    rows[0]["trade_flow_180s"] = None
    rows[0]["feature_ready"] = True
    tables["feature_snapshots"] = _replace(tables["feature_snapshots"], rows)
    _assert_failed(_validate(tables), "feature_ready_has_no_required_nulls")


def test_negative_mfe_fails(tmp_path: Path) -> None:
    tables = _valid_tables(tmp_path)
    rows = _rows(tables["future_outcomes"])
    rows[0]["mfe_ticks"] = -0.01
    tables["future_outcomes"] = _replace(tables["future_outcomes"], rows)
    _assert_failed(_validate(tables), "mfe_mae_nonnegative")


def test_negative_mae_fails(tmp_path: Path) -> None:
    tables = _valid_tables(tmp_path)
    rows = _rows(tables["future_outcomes"])
    rows[0]["mae_ticks"] = -0.01
    tables["future_outcomes"] = _replace(tables["future_outcomes"], rows)
    _assert_failed(_validate(tables), "mfe_mae_nonnegative")


def test_incomplete_future_support_window_fails(tmp_path: Path) -> None:
    tables = _valid_tables(tmp_path)
    for dataset in ("raw_orderbook", "raw_trades", "raw_last_price"):
        tables[dataset] = tables[dataset].filter(
            pc.less(tables[dataset]["receive_ts"], pa.scalar(RAW_END))
        )
    result = _validate(tables)
    for dataset in ("raw_orderbook", "raw_trades", "raw_last_price"):
        _assert_failed(result, f"{dataset}_coverage")


def test_correct_archive_tables_pass_all_validator_checks(tmp_path: Path) -> None:
    result = _validate(_valid_tables(tmp_path))
    failed = [item for item in result["checks"] if item["status"] != "PASS"]
    assert result["status"] == "PASS", result["errors"]
    assert not failed
