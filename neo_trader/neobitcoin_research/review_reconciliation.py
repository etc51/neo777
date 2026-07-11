"""Independent reconciliation for published Neobitcoin review tables.

This module intentionally does not import ``review_datasets`` (or any live
feature/outcome/stop calculator).  It reconstructs audit results only from the
typed rows that are about to be archived.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta
from statistics import median
from typing import Any, Final

import pyarrow as pa  # type: ignore[import-untyped]

TRADE_WINDOWS: Final = (5, 10, 30, 60, 180, 300, 900)
AGE_SOURCES: Final = {
    "orderbook": "raw_orderbook",
    "last_price": "raw_last_price",
    "last_trade": "raw_trades",
    "candle_1m": "candles_1m",
    "candle_5m": "candles_5m",
    "candle_15m": "candles_15m",
}
EPSILON: Final = 1e-9


def reconcile_review_tables(tables: dict[str, pa.Table]) -> dict[str, Any]:
    """Recalculate auditable values without invoking production business logic."""

    errors: list[str] = []
    trade_flow = _reconcile_trade_flow(tables, errors)
    ages = _reconcile_ages(tables, errors)
    support, outcomes = _reconcile_outcomes(tables, errors)
    stops = _reconcile_stops(tables, errors)
    return {
        "status": "PASS" if not errors else "FAIL",
        "errors": errors,
        "trade_flow": trade_flow,
        "source_ages": ages,
        "raw_support": support,
        "outcomes": outcomes,
        "stops": stops,
    }


def _rows(tables: dict[str, pa.Table], name: str) -> list[dict[str, Any]]:
    return tables[name].to_pylist() if name in tables else []


def _utc(value: Any) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"invalid UTC timestamp: {value!r}")
    return value.astimezone(UTC)


def _float_equal(left: Any, right: Any) -> tuple[bool, float]:
    if left is None or right is None:
        return left is right, float("inf") if left is not right else 0.0
    error = abs(float(left) - float(right))
    return error <= EPSILON, error


def _timestamp_equal(left: Any, right: datetime) -> bool:
    try:
        return _utc(left) == right
    except ValueError:
        return False


def _reconcile_trade_flow(
    tables: dict[str, pa.Table], errors: list[str]
) -> dict[str, dict[str, Any]]:
    trades = _rows(tables, "raw_trades")
    ids = [str(row.get("event_id") or "") for row in trades]
    trade_ids = [str(row.get("trade_id") or "") for row in trades if row.get("trade_id")]
    if len(ids) != len(set(ids)) or len(trade_ids) != len(set(trade_ids)):
        errors.append("trade-flow: duplicate raw trade event_id/trade_id")
    ordered = sorted(trades, key=lambda row: (_utc(row["exchange_ts"]), str(row["event_id"])))
    features = _rows(tables, "feature_snapshots")
    summary: dict[str, dict[str, Any]] = {}
    for seconds in TRADE_WINDOWS:
        checked = matched = mismatched = 0
        max_error = 0.0
        for feature in features:
            checked += 1
            feature_ts = _utc(feature["exchange_ts"])
            left = feature_ts - timedelta(seconds=seconds)
            window = [row for row in ordered if left < _utc(row["exchange_ts"]) <= feature_ts]
            buy = sell = 0.0
            known = unknown = 0
            for trade in window:
                side = str(trade.get("aggressor_side") or "UNKNOWN").upper()
                quantity = float(trade.get("quantity") or 0.0)
                if side == "BUY":
                    buy += quantity
                    known += 1
                elif side == "SELL":
                    sell += quantity
                    known += 1
                else:
                    unknown += 1
            expected: dict[str, Any] = {
                f"trade_count_{seconds}s": len(window),
                f"known_side_trade_count_{seconds}s": known,
                f"unknown_side_trade_count_{seconds}s": unknown,
                f"buy_volume_{seconds}s": buy,
                f"sell_volume_{seconds}s": sell,
                f"trade_flow_{seconds}s": buy - sell,
                f"trade_window_first_event_id_{seconds}s": (
                    str(window[0]["event_id"]) if window else None
                ),
                f"trade_window_last_event_id_{seconds}s": (
                    str(window[-1]["event_id"]) if window else None
                ),
            }
            row_ok = True
            for column, value in expected.items():
                actual = feature.get(column)
                if column.startswith(("buy_volume_", "sell_volume_", "trade_flow_")):
                    equal, error = _float_equal(actual, value)
                    max_error = max(max_error, error)
                else:
                    equal = actual == value
                row_ok &= equal
            matched += int(row_ok)
            mismatched += int(not row_ok)
        if mismatched:
            errors.append(f"trade-flow {seconds}s: {mismatched}/{checked} rows differ")
        summary[f"{seconds}s"] = {
            "checked": checked,
            "matched": matched,
            "mismatched": mismatched,
            "max_error": max_error,
            "status": "PASS" if mismatched == 0 else "FAIL",
        }
    return summary


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * percentile
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


def _reconcile_ages(tables: dict[str, pa.Table], errors: list[str]) -> dict[str, Any]:
    features = _rows(tables, "feature_snapshots")
    summary: dict[str, Any] = {}
    event_indexes: dict[str, dict[str, dict[str, Any]]] = {}
    for source, dataset in AGE_SOURCES.items():
        age_column = f"{source}_age_ms"
        id_column = f"{source}_source_event_id"
        raw = {str(row.get("event_id")): row for row in _rows(tables, dataset)}
        event_indexes[source] = raw
        values: list[float] = []
        reconciled = mismatched = missing_ids = 0
        for feature in features:
            source_id = feature.get(id_column)
            actual = feature.get(age_column)
            if not source_id:
                if feature.get("feature_ready"):
                    missing_ids += 1
                continue
            event = raw.get(str(source_id))
            if event is None:
                missing_ids += 1
                continue
            expected = (
                _utc(feature["processing_ts"]) - _utc(event["receive_ts"])
            ).total_seconds() * 1000
            equal, _ = _float_equal(actual, expected)
            if equal and expected >= 0:
                reconciled += 1
                values.append(float(expected))
            else:
                mismatched += 1
        nulls = sum(row.get(age_column) is None for row in features)
        null_fraction = nulls / len(features) if features else 1.0
        all_zero = bool(values) and all(abs(value) <= EPSILON for value in values)
        if missing_ids or mismatched or (features and null_fraction == 1.0) or all_zero:
            errors.append(
                f"source-age {source}: missing_ids={missing_ids}, mismatched={mismatched}, "
                f"null_fraction={null_fraction:.6f}, all_zero={all_zero}"
            )
        summary[age_column] = {
            "null_fraction": null_fraction,
            "min": min(values) if values else None,
            "median": median(values) if values else None,
            "p95": _percentile(values, 0.95),
            "max": max(values) if values else None,
            "reconciled": reconciled,
            "mismatched": mismatched,
            "missing_source_ids": missing_ids,
        }
    aggregate_errors = 0
    for feature in features:
        ages = [
            float(feature[f"{source}_age_ms"])
            for source in AGE_SOURCES
            if feature.get(f"{source}_age_ms") is not None
        ]
        exchange_ages: list[float] = []
        for source in AGE_SOURCES:
            event = event_indexes[source].get(str(feature.get(f"{source}_source_event_id") or ""))
            if event is not None:
                exchange_ages.append(
                    (_utc(feature["exchange_ts"]) - _utc(event["exchange_ts"])).total_seconds()
                    * 1000
                )
        expected_values = {
            "oldest_required_source_age_ms": max(ages) if ages else None,
            "newest_source_age_ms": min(ages) if ages else None,
            "source_exchange_age_ms": max(exchange_ages) if exchange_ages else None,
            "book_age_ms": feature.get("orderbook_age_ms"),
            "data_age_ms": min(ages) if ages else None,
        }
        aggregate_errors += sum(
            not _float_equal(feature.get(column), expected)[0]
            for column, expected in expected_values.items()
        )
    if aggregate_errors:
        errors.append(f"source-age aggregates: {aggregate_errors} mismatches")
    summary["aggregate_reconciliation"] = {
        "checked": len(features) * 5,
        "mismatched": aggregate_errors,
    }
    return summary


def _coverage_end(rows: list[dict[str, Any]]) -> datetime | None:
    values = [_utc(row["exchange_ts"]) for row in rows if row.get("exchange_ts") is not None]
    return max(values) if values else None


def _reconcile_outcomes(
    tables: dict[str, pa.Table], errors: list[str]
) -> tuple[dict[str, Any], dict[str, Any]]:
    outcomes = _rows(tables, "future_outcomes")
    executions = _rows(tables, "execution_simulations")
    fills = [_utc(row["fill_ts"]) for row in executions if row.get("fill_ts") is not None]
    theoretical = [_utc(row["entry_ts"]) for row in outcomes if row.get("entry_ts") is not None]
    max_fill = max(fills) if fills else None
    max_theoretical = max(theoretical) if theoretical else None
    entries = [value for value in (max_fill, max_theoretical) if value is not None]
    max_entry = max(entries) if entries else None
    required_end = max_entry + timedelta(seconds=1800) if max_entry else None
    coverage = {
        name: _coverage_end(_rows(tables, name))
        for name in ("raw_orderbook", "raw_trades", "raw_last_price")
    }
    deficient = [
        name for name, end in coverage.items() if required_end and (not end or end < required_end)
    ]
    if deficient:
        errors.append(f"raw support ends before required_support_end: {', '.join(deficient)}")

    raw_by_id: dict[str, tuple[str, dict[str, Any]]] = {}
    for dataset in ("raw_orderbook", "raw_trades", "raw_last_price"):
        raw_by_id.update(
            {str(row.get("event_id")): (dataset, row) for row in _rows(tables, dataset)}
        )
    complete = source_present = prices = pnl = outside = 0
    for row in outcomes:
        if not row.get("outcome_complete"):
            continue
        complete += 1
        source_id = row.get("price_source_event_id")
        source = raw_by_id.get(str(source_id)) if source_id else None
        if source is None:
            continue
        source_present += 1
        _, event = source
        target = _utc(row.get("target_ts") or row.get("future_ts"))
        source_ts = _utc(event["exchange_ts"])
        coverage_values = [value for value in coverage.values() if value is not None]
        if source_ts > target or (
            required_end and coverage_values and target > min(coverage_values)
        ):
            outside += 1
        side = str(row.get("side"))
        expected_exit = event.get("best_bid") if side == "LONG" else event.get("best_ask")
        if expected_exit is None:
            expected_exit = event.get("last_price", event.get("price"))
        price_ok, _ = _float_equal(
            row.get("executable_exit_price", row.get("future_price")), expected_exit
        )
        expected_mid = event.get("mid_price")
        if (
            expected_mid is None
            and event.get("best_bid") is not None
            and event.get("best_ask") is not None
        ):
            expected_mid = (float(event["best_bid"]) + float(event["best_ask"])) / 2
        metadata_ok = (
            row.get("price_source") == "raw_orderbook"
            and row.get("price_selection_method")
            == "last_executable_orderbook_quote_at_or_before_target"
            and _timestamp_equal(row.get("price_source_exchange_ts"), source_ts)
            and _timestamp_equal(row.get("price_source_receive_ts"), _utc(event["receive_ts"]))
            and _float_equal(row.get("future_mid_price"), expected_mid)[0]
            and _float_equal(row.get("exit_bid"), event.get("best_bid"))[0]
            and _float_equal(row.get("exit_ask"), event.get("best_ask"))[0]
            and _float_equal(row.get("future_price"), expected_exit)[0]
            and bool(row.get("support_complete"))
            and not bool(row.get("gap_detected"))
        )
        prices += int(price_ok and metadata_ok)
        entry = float(row["entry_price"])
        exit_price = float(expected_exit) if expected_exit is not None else float("nan")
        quantity = float(row.get("filled_quantity") or 1.0)
        direction = 1.0 if side == "LONG" else -1.0
        expected_gross = direction * (exit_price - entry) * quantity
        raw_ok, _ = _float_equal(row.get("raw_return"), expected_gross)
        percent_ok, _ = _float_equal(row.get("return_percent"), expected_gross / entry * 100)
        gross_ok, _ = _float_equal(row.get("gross_pnl"), expected_gross)
        # Net can be reconciled from the archive's explicit costs without sharing writer logic.
        costs = float(row.get("spread_cost") or 0.0) + float(row.get("slippage_cost") or 0.0)
        net_ok, _ = _float_equal(row.get("net_pnl"), expected_gross - costs)
        pnl += int(raw_ok and percent_ok and gross_ok and net_ok)
    missing = complete - source_present
    if missing or prices != complete or pnl != complete or outside:
        errors.append(
            f"outcomes: complete={complete}, missing_source={missing}, prices={prices}, "
            f"pnl={pnl}, outside_support={outside}"
        )
    support = {
        "max_actual_fill_ts": max_fill.isoformat() if max_fill else None,
        "max_theoretical_entry_ts": max_theoretical.isoformat() if max_theoretical else None,
        "max_required_entry_ts": max_entry.isoformat() if max_entry else None,
        "required_support_end": required_end.isoformat() if required_end else None,
        "actual_support_end": {
            name: value.isoformat() if value else None for name, value in coverage.items()
        },
    }
    return support, {
        "complete_count": complete,
        "source_ids_present": source_present,
        "reproduced_prices": prices,
        "reproduced_pnl": pnl,
        "outside_support_count": outside,
        "missing_source_id_count": missing,
    }


def _reconcile_stops(tables: dict[str, pa.Table], errors: list[str]) -> dict[str, Any]:
    books = _rows(tables, "raw_orderbook")
    by_id = {str(row.get("event_id")): row for row in books}
    ordered = sorted(books, key=lambda row: (_utc(row["exchange_ts"]), str(row["event_id"])))
    executions = {
        str(row.get("simulation_id")): row for row in _rows(tables, "execution_simulations")
    }
    triggered = missing = reproduced = 0
    rows = _rows(tables, "shadow_stop_results")
    required = {
        "stop_level",
        "trigger_event_id",
        "trigger_event_type",
        "trigger_ts",
        "trigger_price",
        "trigger_bid",
        "trigger_ask",
        "exit_source_event_id",
        "exit_source_ts",
        "gap_through_stop",
    }
    missing_columns = (
        sorted(required - set(tables["shadow_stop_results"].column_names))
        if "shadow_stop_results" in tables
        else sorted(required)
    )
    if rows and missing_columns:
        triggered = sum(bool(row.get("stop_triggered")) for row in rows)
        errors.append(f"stops: missing audit columns {missing_columns}")
        return {
            "triggered_count": triggered,
            "source_ids_present": 0,
            "reproduced_count": 0,
            "missing_trigger_id_count": triggered,
            "identical_result_count": 0,
            "gap_explained_identical_count": 0,
            "unexplained_identical_count": 0,
            "trigger_examples": [],
        }
    for row in rows:
        execution = executions.get(str(row.get("simulation_id")), {})
        fill_ts = _utc(row.get("fill_ts") or execution.get("fill_ts") or row["entry_ts"])
        stop_level = float(row["stop_level"])
        side = str(row["side"])
        segment = [
            event
            for event in ordered
            if fill_ts < _utc(event["exchange_ts"]) <= fill_ts + timedelta(seconds=1800)
        ]
        candidates = [
            item
            for item in segment
            if (
                (
                    side == "LONG"
                    and item.get("best_bid") is not None
                    and float(item["best_bid"]) <= stop_level
                )
                or (
                    side == "SHORT"
                    and item.get("best_ask") is not None
                    and float(item["best_ask"]) >= stop_level
                )
            )
        ]
        expected_trigger = candidates[0] if candidates else None
        hit = expected_trigger or (segment[-1] if segment else None)
        expected_trigger_id = str(expected_trigger["event_id"]) if expected_trigger else None
        expected_exit_id = str(hit["event_id"]) if hit else None
        expected_exit_ts = _utc(hit["exchange_ts"]) if hit else None
        expected_exit = (
            hit.get("best_bid" if side == "LONG" else "best_ask") if hit else row["entry_price"]
        )
        row_triggered = bool(row.get("stop_triggered"))
        triggered += int(row_triggered)
        event = by_id.get(str(row.get("trigger_event_id") or ""))
        if row_triggered and event is None:
            missing += 1
        exit_event = by_id.get(str(row.get("exit_source_event_id") or ""))
        source_exit = (
            exit_event.get("best_bid" if side == "LONG" else "best_ask") if exit_event else None
        )
        direction = 1.0 if side == "LONG" else -1.0
        expected_exit_value = (
            float(expected_exit) if expected_exit is not None else float(row["entry_price"])
        )
        gross = direction * (expected_exit_value - float(row["entry_price"]))
        net = gross - float(execution.get("spread_cost") or 0.0)
        event_ok = row_triggered == (expected_trigger is not None)
        if row_triggered and expected_trigger is not None:
            trigger_price = expected_trigger.get("best_bid" if side == "LONG" else "best_ask")
            event_ok &= (
                row.get("trigger_event_type") == "raw_orderbook"
                and row.get("trigger_event_id") == expected_trigger_id
                and _timestamp_equal(row.get("trigger_ts"), _utc(expected_trigger["exchange_ts"]))
                and _float_equal(row.get("trigger_price"), trigger_price)[0]
                and _float_equal(row.get("trigger_bid"), expected_trigger.get("best_bid"))[0]
                and _float_equal(row.get("trigger_ask"), expected_trigger.get("best_ask"))[0]
                and int(row.get("trigger_sequence") or 0)
                == int(expected_trigger.get("sequence") or expected_trigger.get("revision") or 0)
            )
        elif not row_triggered:
            event_ok &= not row.get("trigger_event_id")
        row_ok = (
            event_ok
            and expected_exit is not None
            and expected_exit_ts is not None
            and exit_event is not None
            and row.get("exit_source_event_id") == expected_exit_id
            and _timestamp_equal(row.get("exit_source_ts"), expected_exit_ts)
            and _float_equal(source_exit, expected_exit)[0]
            and _float_equal(row.get("exit_price"), expected_exit)[0]
            and _float_equal(row.get("gross_pnl"), gross)[0]
            and _float_equal(row.get("net_pnl"), net)[0]
            and int(row.get("holding_ms") or 0) >= 0
        )
        if row_ok:
            reproduced += 1
    if missing or reproduced != len(rows):
        errors.append(
            f"stops: rows={len(rows)}, triggered={triggered}, "
            f"missing_trigger={missing}, reproduced={reproduced}"
        )

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("simulation_id"))].append(row)
    identical = explained = unexplained = 0
    examples: list[str] = []
    for group in grouped.values():
        for index, left in enumerate(group):
            for right in group[index + 1 :]:
                same_result = (
                    left.get("exit_ts"),
                    left.get("exit_price"),
                    left.get("exit_reason"),
                ) == (right.get("exit_ts"), right.get("exit_price"), right.get("exit_reason"))
                if (
                    not same_result
                    or left.get("stop_ticks") == right.get("stop_ticks")
                    or not left.get("stop_triggered")
                    or not right.get("stop_triggered")
                ):
                    continue
                identical += 1
                same_trigger = left.get("trigger_event_id") and left.get(
                    "trigger_event_id"
                ) == right.get("trigger_event_id")
                levels_differ = left.get("stop_level") != right.get("stop_level")
                gap = bool(left.get("gap_through_stop")) and bool(right.get("gap_through_stop"))
                trigger_event = by_id.get(str(left.get("trigger_event_id") or ""))
                side = str(left.get("side"))
                executable = (
                    trigger_event.get("best_bid" if side == "LONG" else "best_ask")
                    if trigger_event
                    else None
                )
                crosses_both = executable is not None and all(
                    float(executable) <= float(item["stop_level"])
                    if side == "LONG"
                    else float(executable) >= float(item["stop_level"])
                    for item in (left, right)
                )
                if same_trigger and levels_differ and gap and crosses_both:
                    explained += 1
                    if len(examples) < 5:
                        examples.append(str(left.get("trigger_event_id")))
                else:
                    unexplained += 1
    if unexplained:
        errors.append(f"stops: {unexplained} unexplained identical results")
    return {
        "triggered_count": triggered,
        "source_ids_present": triggered - missing,
        "reproduced_count": reproduced,
        "missing_trigger_id_count": missing,
        "identical_result_count": identical,
        "gap_explained_identical_count": explained,
        "unexplained_identical_count": unexplained,
        "trigger_examples": examples,
    }


__all__ = ["AGE_SOURCES", "TRADE_WINDOWS", "reconcile_review_tables"]
