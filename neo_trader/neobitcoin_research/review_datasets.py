"""Typed, referentially-complete derived datasets for a review bundle.

This module is intentionally independent from the collector and archive writer.  It
turns an already captured feature stream and an order-book price path into a small
counterfactual research graph suitable for review.
"""

from __future__ import annotations

import hashlib
import json
from bisect import bisect_left, bisect_right
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, Final, cast

pa = cast(Any, __import__("pyarrow"))

HORIZONS: Final = (5, 10, 30, 60, 180, 300, 600, 900, 1800)
EXECUTION_MODELS: Final = (
    "aggressive_marketable",
    "ideal_touch",
    "passive_best",
    "passive_conservative",
    "passive_delayed",
)
STOP_TICKS: Final = (2, 3, 4, 5)
EXIT_VARIANTS: Final = (
    "take_profit",
    "time_exit",
    "breakeven",
    "dynamic_breakeven",
    "trailing",
    "microstructure",
    "orderbook",
)
TRADE_WINDOWS: Final = (5, 10, 30, 60, 180, 300, 900)

TS = pa.timestamp("us", tz="UTC")


def _common() -> list[Any]:
    return [
        pa.field("schema_version", pa.string(), nullable=False),
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("instrument_uid", pa.string(), nullable=False),
        pa.field("instrument_ticker", pa.string(), nullable=False),
        pa.field("exchange_ts", TS, nullable=False),
        pa.field("receive_ts", TS, nullable=False),
        pa.field("processing_ts", TS, nullable=False),
        pa.field("session_id", pa.string(), nullable=False),
        pa.field("collector_instance_id", pa.string(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("code_commit", pa.string(), nullable=False),
        pa.field("config_hash", pa.string(), nullable=False),
        pa.field("data_quality_flags", pa.list_(pa.string()), nullable=False),
    ]


def _schema(*fields: tuple[str, Any, bool]) -> Any:
    return pa.schema(
        _common() + [pa.field(name, typ, nullable=nullable) for name, typ, nullable in fields]
    )


FEATURE_SCHEMA = _schema(
    ("feature_snapshot_id", pa.string(), False),
    ("last_price", pa.float64(), True),
    ("best_bid", pa.float64(), False),
    ("best_ask", pa.float64(), False),
    ("mid_price", pa.float64(), False),
    ("spread_ticks", pa.float64(), False),
    ("spread_percent", pa.float64(), False),
    ("microprice", pa.float64(), True),
    ("ofi", pa.float64(), True),
    ("ofi_delta", pa.float64(), True),
    *[(f"imbalance_{n}", pa.float64(), True) for n in (1, 3, 5, 10, 20)],
    *[(f"bid_depth_{n}", pa.float64(), True) for n in (1, 3, 5, 10, 20)],
    *[(f"ask_depth_{n}", pa.float64(), True) for n in (1, 3, 5, 10, 20)],
    *[(f"trade_flow_{n}s", pa.float64(), False) for n in TRADE_WINDOWS],
    *[
        (f"{kind}_{n}s", typ, nullable)
        for n in TRADE_WINDOWS
        for kind, typ, nullable in (
            ("trade_count", pa.int64(), False),
            ("known_side_trade_count", pa.int64(), False),
            ("unknown_side_trade_count", pa.int64(), False),
            ("buy_volume", pa.float64(), False),
            ("sell_volume", pa.float64(), False),
            ("trade_window_first_event_id", pa.string(), True),
            ("trade_window_last_event_id", pa.string(), True),
        )
    ],
    ("signed_volume", pa.float64(), True),
    ("return_1m", pa.float64(), True),
    ("realized_volatility", pa.float64(), True),
    ("atr_1m", pa.float64(), True),
    ("atr_5m", pa.float64(), True),
    ("atr_15m", pa.float64(), True),
    ("candle_volume_1m", pa.float64(), True),
    ("candle_volume_5m", pa.float64(), True),
    ("candle_volume_15m", pa.float64(), True),
    ("trend", pa.float64(), True),
    ("regime", pa.string(), False),
    ("feed_latency_ms", pa.float64(), True),
    ("book_age_ms", pa.float64(), True),
    ("feature_ready", pa.bool_(), False),
    ("warmup_remaining", pa.int64(), False),
    ("missing_reason", pa.string(), True),
    ("source_status", pa.string(), False),
    ("raw_signal", pa.string(), True),
    ("data_age_ms", pa.float64(), True),
    ("orderbook_age_ms", pa.float64(), True),
    ("last_price_age_ms", pa.float64(), True),
    ("last_trade_age_ms", pa.float64(), True),
    ("candle_1m_age_ms", pa.float64(), True),
    ("candle_5m_age_ms", pa.float64(), True),
    ("candle_15m_age_ms", pa.float64(), True),
    ("oldest_required_source_age_ms", pa.float64(), True),
    ("newest_source_age_ms", pa.float64(), True),
    ("source_exchange_age_ms", pa.float64(), True),
    ("orderbook_source_event_id", pa.string(), True),
    ("last_price_source_event_id", pa.string(), True),
    ("last_trade_source_event_id", pa.string(), True),
    ("candle_1m_source_event_id", pa.string(), True),
    ("candle_5m_source_event_id", pa.string(), True),
    ("candle_15m_source_event_id", pa.string(), True),
    *[
        (f"{side}_{kind}_{level:02d}", pa.float64(), True)
        for level in range(1, 21)
        for side in ("bid", "ask")
        for kind in ("price", "quantity")
    ],
)

CANDIDATE_SCHEMA = _schema(
    ("candidate_id", pa.string(), False),
    ("feature_snapshot_id", pa.string(), False),
    ("candidate_ts", TS, False),
    ("side", pa.string(), False),
    ("reference_price", pa.float64(), False),
    ("best_bid", pa.float64(), False),
    ("best_ask", pa.float64(), False),
    ("spread_ticks", pa.float64(), False),
    ("allowed", pa.bool_(), False),
    ("final_decision", pa.string(), False),
    ("rejection_reason", pa.string(), True),
    ("strategy_version", pa.string(), False),
    ("model_version", pa.string(), False),
    ("experiment_id", pa.string(), False),
    ("feature_ready", pa.bool_(), False),
)

FILTER_SCHEMA = _schema(
    ("candidate_id", pa.string(), False),
    ("filter_name", pa.string(), False),
    ("passed", pa.bool_(), False),
    ("value", pa.float64(), True),
    ("threshold", pa.float64(), True),
    ("reason", pa.string(), False),
    ("decision_ts", TS, False),
)

EXECUTION_SCHEMA = _schema(
    ("simulation_id", pa.string(), False),
    ("candidate_id", pa.string(), False),
    ("feature_snapshot_id", pa.string(), False),
    ("side", pa.string(), False),
    ("execution_model", pa.string(), False),
    ("order_ts", TS, False),
    ("order_price", pa.float64(), False),
    ("requested_quantity", pa.float64(), False),
    ("filled_quantity", pa.float64(), False),
    ("remaining_quantity", pa.float64(), False),
    ("fill_status", pa.string(), False),
    ("fill_ts", TS, True),
    ("fill_price", pa.float64(), True),
    ("fill_delay_ms", pa.int64(), True),
    ("slippage_ticks", pa.float64(), True),
    ("slippage_percent", pa.float64(), True),
    ("spread_cost", pa.float64(), False),
    ("no_fill_reason", pa.string(), True),
)

OUTCOME_SCHEMA = _schema(
    ("outcome_id", pa.string(), False),
    ("candidate_id", pa.string(), False),
    ("simulation_id", pa.string(), False),
    ("side", pa.string(), False),
    ("horizon_seconds", pa.int64(), False),
    ("entry_ts", TS, False),
    ("entry_price", pa.float64(), False),
    ("future_ts", TS, False),
    ("target_ts", TS, False),
    ("future_price", pa.float64(), False),
    ("future_mid_price", pa.float64(), False),
    ("executable_exit_price", pa.float64(), False),
    ("exit_bid", pa.float64(), False),
    ("exit_ask", pa.float64(), False),
    ("price_source", pa.string(), False),
    ("price_source_event_id", pa.string(), False),
    ("price_source_exchange_ts", TS, False),
    ("price_source_receive_ts", TS, False),
    ("price_selection_method", pa.string(), False),
    ("source_age_ms_at_target", pa.float64(), False),
    ("support_complete", pa.bool_(), False),
    ("gap_detected", pa.bool_(), False),
    ("raw_return", pa.float64(), False),
    ("return_ticks", pa.float64(), False),
    ("return_percent", pa.float64(), False),
    ("mfe_ticks", pa.float64(), False),
    ("mae_ticks", pa.float64(), False),
    ("mfe_percent", pa.float64(), False),
    ("mae_percent", pa.float64(), False),
    ("time_to_mfe_ms", pa.int64(), False),
    ("time_to_mae_ms", pa.int64(), False),
    ("spread_cost", pa.float64(), False),
    ("slippage_cost", pa.float64(), False),
    ("gross_pnl", pa.float64(), False),
    ("net_pnl", pa.float64(), False),
    ("outcome_complete", pa.bool_(), False),
    ("missing_reason", pa.string(), True),
)

STOP_SCHEMA = _schema(
    ("result_id", pa.string(), False),
    ("candidate_id", pa.string(), False),
    ("simulation_id", pa.string(), False),
    ("side", pa.string(), False),
    ("stop_ticks", pa.int64(), False),
    ("stop_level", pa.float64(), False),
    ("entry_ts", TS, False),
    ("entry_price", pa.float64(), False),
    ("exit_ts", TS, False),
    ("exit_price", pa.float64(), False),
    ("exit_reason", pa.string(), False),
    ("stop_triggered", pa.bool_(), False),
    ("trigger_event_id", pa.string(), True),
    ("trigger_event_type", pa.string(), True),
    ("trigger_ts", TS, True),
    ("trigger_price", pa.float64(), True),
    ("trigger_bid", pa.float64(), True),
    ("trigger_ask", pa.float64(), True),
    ("trigger_sequence", pa.int64(), True),
    ("exit_source_event_id", pa.string(), False),
    ("exit_source_ts", TS, False),
    ("gap_through_stop", pa.bool_(), False),
    ("slippage_from_stop_ticks", pa.float64(), False),
    ("gross_pnl", pa.float64(), False),
    ("net_pnl", pa.float64(), False),
    ("mfe_before_exit", pa.float64(), False),
    ("mae_before_exit", pa.float64(), False),
    ("holding_ms", pa.int64(), False),
)

EXIT_SCHEMA = _schema(
    ("result_id", pa.string(), False),
    ("candidate_id", pa.string(), False),
    ("simulation_id", pa.string(), False),
    ("side", pa.string(), False),
    ("exit_variant", pa.string(), False),
    ("variant_parameters", pa.string(), False),
    ("entry_ts", TS, False),
    ("entry_price", pa.float64(), False),
    ("activation_ts", TS, True),
    ("exit_ts", TS, False),
    ("exit_price", pa.float64(), False),
    ("exit_reason", pa.string(), False),
    ("trigger_value", pa.float64(), True),
    ("trigger_threshold", pa.float64(), True),
    ("gross_pnl", pa.float64(), False),
    ("net_pnl", pa.float64(), False),
    ("mfe_before_exit", pa.float64(), False),
    ("mae_before_exit", pa.float64(), False),
    ("holding_ms", pa.int64(), False),
)

REVIEW_SCHEMAS: Final = {
    "feature_snapshots": FEATURE_SCHEMA,
    "candidate_events": CANDIDATE_SCHEMA,
    "filter_decisions": FILTER_SCHEMA,
    "execution_simulations": EXECUTION_SCHEMA,
    "future_outcomes": OUTCOME_SCHEMA,
    "shadow_stop_results": STOP_SCHEMA,
    "shadow_exit_results": EXIT_SCHEMA,
}


def build_review_datasets(
    feature_rows: Sequence[Mapping[str, Any]],
    price_path: Sequence[Mapping[str, Any]],
    *,
    candidate_start: datetime,
    candidate_end: datetime,
    tick_size: float = 0.1,
    market_events: Sequence[Mapping[str, Any]] | None = None,
    orderbook_rows: Sequence[Mapping[str, Any]] | None = None,
    trade_rows: Sequence[Mapping[str, Any]] | None = None,
    last_price_rows: Sequence[Mapping[str, Any]] | None = None,
    candle_rows_by_interval: Mapping[str, Sequence[Mapping[str, Any]]] | None = None,
    execution_latency_ms: int = 100,
    virtual_order_quantity: float = 10.0,
) -> dict[str, Any]:
    """Build a deterministic review graph from local data.

    ``price_path`` rows require ``exchange_ts`` and ``mid_price``. Feature rows
    may be promoted Parquet rows or their original record dictionaries.
    """

    start, end = _utc(candidate_start), _utc(candidate_end)
    path_source = list(market_events or orderbook_rows or price_path)
    path = sorted(
        (_normalize_market_point(p) for p in path_source),
        key=_market_sort_key,
    )
    if trade_rows:
        path = _merge_trade_events(path, trade_rows)
    path_ts = [p["exchange_ts"] for p in path]
    enriched_features = _enrich_feature_market_fields(
        feature_rows,
        path,
        trade_rows or (),
        last_price_rows or (),
        candle_rows_by_interval or {},
        trade_rows is not None,
        last_price_rows is not None or candle_rows_by_interval is not None,
    )
    normalized_features = [_feature_row(row) for row in enriched_features]
    features = [row for row in normalized_features if start <= row["exchange_ts"] <= end]
    candidates: list[dict[str, Any]] = []
    filters: list[dict[str, Any]] = []
    executions: list[dict[str, Any]] = []
    outcomes: list[dict[str, Any]] = []
    stops: list[dict[str, Any]] = []
    exits: list[dict[str, Any]] = []

    for feature in features:
        for side in ("LONG", "SHORT"):
            candidate = _candidate(feature, side)
            candidates.append(candidate)
            candidate_filters = _filters(candidate, feature)
            filters.extend(candidate_filters)
            side_signaled = str(feature.get("raw_signal") or "") == side
            candidate["allowed"] = side_signaled and all(row["passed"] for row in candidate_filters)
            candidate["final_decision"] = "ACCEPTED" if candidate["allowed"] else "REJECTED"
            candidate["rejection_reason"] = (
                None
                if candidate["allowed"]
                else (
                    "counterfactual_side_not_signaled" if not side_signaled else "filter_rejected"
                )
            )
            for model in EXECUTION_MODELS:
                simulation = _execution(
                    candidate,
                    feature,
                    model,
                    path,
                    path_ts,
                    tick_size,
                    execution_latency_ms,
                    virtual_order_quantity,
                )
                executions.append(simulation)
                if simulation["fill_status"] == "NO_FILL":
                    continue
                outcomes.extend(_outcomes(candidate, simulation, path, path_ts, tick_size))
                stops.extend(_stops(candidate, simulation, path, path_ts, tick_size))
                exits.extend(_exits(candidate, simulation, path, path_ts, tick_size))

    rows = {
        "feature_snapshots": features,
        "candidate_events": candidates,
        "filter_decisions": filters,
        "execution_simulations": executions,
        "future_outcomes": outcomes,
        "shadow_stop_results": stops,
        "shadow_exit_results": exits,
    }
    return {
        name: pa.Table.from_pylist(values, schema=REVIEW_SCHEMAS[name])
        for name, values in rows.items()
    }


def validate_review_links(tables: Mapping[str, Any]) -> list[str]:
    """Return human-readable referential-integrity errors."""
    rows = {name: table.to_pylist() for name, table in tables.items()}
    feature_ids = {r["feature_snapshot_id"] for r in rows["feature_snapshots"]}
    candidate_ids = {r["candidate_id"] for r in rows["candidate_events"]}
    simulation_ids = {r["simulation_id"] for r in rows["execution_simulations"]}
    errors: list[str] = []
    if len(feature_ids) != len(rows["feature_snapshots"]):
        errors.append("duplicate feature_snapshot_id")
    if len(candidate_ids) != len(rows["candidate_events"]):
        errors.append("duplicate candidate_id")
    if len(simulation_ids) != len(rows["execution_simulations"]):
        errors.append("duplicate simulation_id")
    for dataset in ("candidate_events", "execution_simulations"):
        if any(r["feature_snapshot_id"] not in feature_ids for r in rows[dataset]):
            errors.append(f"{dataset}: unknown feature_snapshot_id")
    for dataset in (
        "filter_decisions",
        "execution_simulations",
        "future_outcomes",
        "shadow_stop_results",
        "shadow_exit_results",
    ):
        if any(r["candidate_id"] not in candidate_ids for r in rows[dataset]):
            errors.append(f"{dataset}: unknown candidate_id")
    for dataset in ("future_outcomes", "shadow_stop_results", "shadow_exit_results"):
        if any(r["simulation_id"] not in simulation_ids for r in rows[dataset]):
            errors.append(f"{dataset}: unknown simulation_id")
    present = {r["horizon_seconds"] for r in rows["future_outcomes"]}
    if rows["future_outcomes"] and present != set(HORIZONS):
        errors.append("missing outcome horizons")
    return errors


def _base(row: Mapping[str, Any], event_id: str) -> dict[str, Any]:
    exchange = _utc(
        _dt(
            row.get("exchange_ts")
            or row.get("exchange_timestamp")
            or row.get("timestamp")
            or row.get("recorded_at")
        )
    )
    receive = _utc(
        _dt(
            row.get("receive_ts")
            or row.get("receive_timestamp")
            or row.get("recorded_at")
            or exchange
        )
    )
    return {
        "schema_version": "review-v3",
        "event_id": event_id,
        "instrument_uid": str(row.get("instrument_uid") or "4effa274-4e8f-422c-93ff-04aa34fe8e39"),
        "instrument_ticker": str(row.get("instrument_ticker") or row.get("ticker") or "NEOBITCOIN"),
        "exchange_ts": exchange,
        "receive_ts": receive,
        "processing_ts": _utc(_dt(row.get("processing_ts") or receive)),
        "session_id": str(row.get("session_id") or "local-review"),
        "collector_instance_id": str(row.get("collector_instance_id") or "local-review"),
        "source": "local_counterfactual_review",
        "code_commit": str(row.get("code_commit") or "unknown"),
        "config_hash": str(row.get("config_hash") or row.get("configuration_hash") or "unknown"),
        "data_quality_flags": list(row.get("data_quality_flags") or []),
    }


def _feature_row(source: Mapping[str, Any]) -> dict[str, Any]:
    record = source
    if source.get("record_json"):
        record = {
            **json.loads(str(source["record_json"])),
            **{k: v for k, v in source.items() if v is not None},
        }
    fid = str(
        record.get("feature_snapshot_id")
        or record.get("raw_event_id")
        or source.get("event_id")
        or _id("feature", record)
    )
    row = _base(record, str(source.get("event_id") or fid))
    row["feature_snapshot_id"] = fid
    aliases = {
        "mid_price": "midprice",
        "spread_percent": "spread_bps",
        "ofi": "multi_level_ofi",
        "ofi_delta": "ofi_change",
        "feed_latency_ms": "latency_ms",
    }
    for field in FEATURE_SCHEMA.names:
        if field in row or field in {"feature_snapshot_id"}:
            continue
        value = record.get(field, record.get(aliases.get(field, "")))
        if field.startswith("bid_depth_"):
            value = record.get("cumulative_" + field)
        if field.startswith("ask_depth_"):
            value = record.get("cumulative_" + field)
        if field.startswith("trade_flow_"):
            # Exact semantics: buy volume minus sell volume in the trailing
            # event-time window ending at (and never after) exchange_ts.
            window = field.removeprefix("trade_flow_")
            value = record.get(f"trades_{window}_signed_volume")
            if value is None:
                value = 0.0
        if (
            any(
                field.startswith(prefix)
                for prefix in (
                    "trade_count_",
                    "known_side_trade_count_",
                    "unknown_side_trade_count_",
                )
            )
            and value is None
        ):
            value = 0
        if field.startswith(("buy_volume_", "sell_volume_")) and value is None:
            value = 0.0
        if "_quantity_" in field:
            value = record.get(field.replace("_quantity_", "_volume_").replace("_0", "_"))
        if "_price_" in field:
            value = record.get(field.replace("_0", "_"))
        row[field] = value
    if row.get("data_age_ms") is None:
        row["data_age_ms"] = record.get("book_age_ms")
    if row.get("data_age_ms") is None:
        market_ts = record.get("last_market_event_ts") or record.get("exchange_ts")
        feature_ts = record.get("timestamp") or record.get("recorded_at")
        if market_ts is not None and feature_ts is not None:
            row["data_age_ms"] = max(
                0.0, (_utc(_dt(feature_ts)) - _utc(_dt(market_ts))).total_seconds() * 1000
            )
    row["regime"] = str(record.get("regime") or "unknown")
    row["source_status"] = str(
        record.get("source_status") or record.get("trading_status") or "NORMAL_TRADING"
    )
    required = (
        "trade_flow_10s",
        "trade_flow_180s",
        "trade_flow_300s",
        "trade_flow_900s",
        "data_age_ms",
    )
    missing = []
    for name in required:
        if name.startswith("trade_flow_"):
            window = name.removeprefix("trade_flow_")
            if record.get(f"trades_{window}_signed_volume") is None:
                missing.append(name)
        elif row.get(name) is None:
            missing.append(name)
    missing.extend(f"{name}_source_event_id" for name in record.get("_lineage_missing", ()))
    row["feature_ready"] = bool(record.get("feature_ready", True)) and not missing
    row["warmup_remaining"] = max(int(record.get("warmup_remaining") or 0), len(missing))
    row["missing_reason"] = record.get("missing_reason") or (
        "missing_required_features:" + ",".join(missing) if missing else None
    )
    return row


def _candidate(feature: Mapping[str, Any], side: str) -> dict[str, Any]:
    cid = _id("candidate", feature["feature_snapshot_id"], side)
    row = _base(feature, cid)
    row.update(
        candidate_id=cid,
        feature_snapshot_id=feature["feature_snapshot_id"],
        candidate_ts=feature["exchange_ts"],
        side=side,
        reference_price=feature["mid_price"],
        best_bid=feature["best_bid"],
        best_ask=feature["best_ask"],
        spread_ticks=feature["spread_ticks"],
        allowed=False,
        final_decision="PENDING",
        rejection_reason=None,
        strategy_version="review-counterfactual-v1",
        model_version="fixed-diagnostic-v1",
        experiment_id="review-bundle-v3",
        feature_ready=feature["feature_ready"],
    )
    return row


def _filters(candidate: Mapping[str, Any], feature: Mapping[str, Any]) -> list[dict[str, Any]]:
    checks = (
        ("session", True, None, None),
        ("spread", feature["spread_ticks"] <= 100, feature["spread_ticks"], 100.0),
        (
            "orderbook",
            feature["best_bid"] < feature["best_ask"],
            feature["best_ask"] - feature["best_bid"],
            0.0,
        ),
        ("microstructure", feature["microprice"] is not None, feature["microprice"], None),
        (
            "volatility_sanity",
            feature["realized_volatility"] is not None,
            feature["realized_volatility"],
            None,
        ),
        ("data_quality", feature["feature_ready"], 1.0 if feature["feature_ready"] else 0.0, 1.0),
    )
    result = []
    for name, passed, value, threshold in checks:
        eid = _id("filter", candidate["candidate_id"], name)
        row = _base(candidate, eid)
        row.update(
            candidate_id=candidate["candidate_id"],
            filter_name=name,
            passed=bool(passed),
            value=value,
            threshold=threshold,
            reason="passed" if passed else f"{name}_failed",
            decision_ts=candidate["candidate_ts"],
        )
        result.append(row)
    return result


def _execution(
    candidate: Mapping[str, Any],
    feature: Mapping[str, Any],
    model: str,
    path: list[dict[str, Any]],
    times: list[datetime],
    tick: float,
    execution_latency_ms: int,
    requested: float,
) -> dict[str, Any]:
    sid = _id("simulation", candidate["candidate_id"], model)
    row = _base(candidate, sid)
    long = candidate["side"] == "LONG"
    touch = float(candidate["best_ask"] if long else candidate["best_bid"])
    order_ts = candidate["candidate_ts"]
    if requested <= 0:
        raise ValueError("virtual_order_quantity must be positive")
    filled = 0.0
    fill_ts: datetime | None = None
    price: float | None = None
    no_fill_reason: str | None = None
    if model == "ideal_touch":
        filled, fill_ts, price = requested, order_ts, touch
    elif model == "aggressive_marketable":
        target = order_ts + timedelta(milliseconds=execution_latency_ms)
        snapshot = next((point for point in path if point["exchange_ts"] >= target), None)
        levels = _book_levels(snapshot, "asks" if long else "bids") if snapshot else []
        remaining = requested
        notional = 0.0
        for px, qty in levels:
            take = min(remaining, qty)
            notional += take * px
            filled += take
            remaining -= take
            if remaining <= 0:
                break
        fill_ts = snapshot["exchange_ts"] if snapshot is not None and filled else None
        price = notional / filled if filled else None
        no_fill_reason = None if filled else "insufficient_orderbook_depth"
    else:
        placement_delay = 500 if model == "passive_delayed" else 0
        order_ts = order_ts + timedelta(milliseconds=placement_delay)
        order_price = float(candidate["best_bid"] if long else candidate["best_ask"])
        queue_ahead = requested * (2.0 if model == "passive_conservative" else 0.5)
        for point in path[bisect_left(times, order_ts) :]:
            ts = point["exchange_ts"]
            if ts > order_ts + timedelta(seconds=30):
                break
            traded = point.get("sell_volume" if long else "buy_volume")
            executable = float(
                point.get(
                    "trade_price",
                    point.get("best_ask" if long else "best_bid", point["mid_price"]),
                )
            )
            crossed = executable <= order_price if long else executable >= order_price
            if not crossed:
                continue
            if traded is None:
                if model == "passive_conservative":
                    continue
                levels = _book_levels(point, "asks" if long else "bids")
                market_quantity = levels[0][1] if levels else 0.0
            else:
                market_quantity = float(traded)
            queue_consumed = min(queue_ahead, market_quantity)
            queue_ahead -= queue_consumed
            available = market_quantity - queue_consumed
            if available <= 0:
                continue
            filled += min(requested - filled, available)
            fill_ts, price = ts, order_price
            if filled >= requested:
                break
        no_fill_reason = None if filled else "no_market_confirmed_queue_fill"
    status = "NO_FILL" if filled == 0 else ("FULL_FILL" if filled >= requested else "PARTIAL_FILL")
    order_price = (
        touch
        if model in {"ideal_touch", "aggressive_marketable"}
        else float(candidate["best_bid"] if long else candidate["best_ask"])
    )
    slip = None if price is None else (price - touch) / tick * (1 if long else -1)
    delay = None if fill_ts is None else int((fill_ts - order_ts).total_seconds() * 1000)
    row.update(
        simulation_id=sid,
        candidate_id=candidate["candidate_id"],
        feature_snapshot_id=candidate["feature_snapshot_id"],
        side=candidate["side"],
        execution_model=model,
        order_ts=order_ts,
        order_price=order_price,
        requested_quantity=requested,
        filled_quantity=filled,
        remaining_quantity=max(0.0, requested - filled),
        fill_status=status,
        fill_ts=fill_ts,
        fill_price=price,
        fill_delay_ms=delay,
        slippage_ticks=slip,
        slippage_percent=(
            None if price is None else (price - touch) / touch * 100 * (1 if long else -1)
        ),
        spread_cost=abs(candidate["best_ask"] - candidate["best_bid"]) / 2,
        no_fill_reason=no_fill_reason,
    )
    return row


def _outcomes(
    candidate: Mapping[str, Any],
    sim: Mapping[str, Any],
    path: list[dict[str, Any]],
    times: list[datetime],
    tick: float,
) -> list[dict[str, Any]]:
    result = []
    sign = 1 if candidate["side"] == "LONG" else -1
    entry = sim["fill_ts"]
    for horizon in HORIZONS:
        future = entry + timedelta(seconds=horizon)
        segment = [p for p in _segment(path, times, entry, future) if _is_quote(p)]
        entry_quote = _asof_quote(path, times, entry)
        if entry_quote is not None and not any(
            _market_event_id(item) == _market_event_id(entry_quote) for item in segment
        ):
            segment.insert(0, entry_quote)
        point = _asof_quote(path, times, future)
        if point is None or not segment:
            continue
        mid_price = _point_price(point)
        exit_price = _exit_price(point, candidate["side"], tick)
        quote_support_end = max(
            (_point_ts(item) for item in path if _is_quote(item)),
            default=None,
        )
        support_complete = quote_support_end is not None and quote_support_end >= future
        gap_detected = _has_critical_gap(path, entry, future)
        if not support_complete or gap_detected:
            continue
        changes = [sign * (_point_price(p) - sim["fill_price"]) for p in segment]
        mfe = max(0.0, max(changes))
        mae = max(0.0, -min(changes))
        mfe_i = changes.index(max(changes))
        mae_i = changes.index(min(changes))
        raw = sign * (exit_price - sim["fill_price"])
        spread = sim["spread_cost"]
        oid = _id("outcome", sim["simulation_id"], horizon)
        row = _base(candidate, oid)
        row.update(
            outcome_id=oid,
            candidate_id=candidate["candidate_id"],
            simulation_id=sim["simulation_id"],
            side=candidate["side"],
            horizon_seconds=horizon,
            entry_ts=entry,
            entry_price=sim["fill_price"],
            future_ts=future,
            target_ts=future,
            future_price=exit_price,
            future_mid_price=mid_price,
            executable_exit_price=exit_price,
            exit_bid=float(point.get("best_bid", mid_price - tick)),
            exit_ask=float(point.get("best_ask", mid_price + tick)),
            price_source="raw_orderbook",
            price_source_event_id=_market_event_id(point),
            price_source_exchange_ts=_point_ts(point),
            price_source_receive_ts=_event_receive_ts(point),
            price_selection_method="last_executable_orderbook_quote_at_or_before_target",
            source_age_ms_at_target=max(0.0, (future - _point_ts(point)).total_seconds() * 1000),
            support_complete=True,
            gap_detected=False,
            raw_return=raw,
            return_ticks=raw / tick,
            return_percent=raw / sim["fill_price"] * 100,
            mfe_ticks=mfe / tick,
            mae_ticks=mae / tick,
            mfe_percent=mfe / sim["fill_price"] * 100,
            mae_percent=mae / sim["fill_price"] * 100,
            time_to_mfe_ms=max(0, int((_point_ts(segment[mfe_i]) - entry).total_seconds() * 1000)),
            time_to_mae_ms=max(0, int((_point_ts(segment[mae_i]) - entry).total_seconds() * 1000)),
            spread_cost=spread,
            slippage_cost=abs((sim["slippage_ticks"] or 0.0) * tick),
            gross_pnl=raw,
            net_pnl=raw - spread - abs((sim["slippage_ticks"] or 0.0) * tick),
            outcome_complete=True,
            missing_reason=None,
        )
        result.append(row)
    return result


def _stops(
    candidate: Mapping[str, Any],
    sim: Mapping[str, Any],
    path: list[dict[str, Any]],
    times: list[datetime],
    tick: float,
) -> list[dict[str, Any]]:
    result = []
    sign = 1 if candidate["side"] == "LONG" else -1
    segment = [
        p
        for p in _segment(path, times, sim["fill_ts"], sim["fill_ts"] + timedelta(seconds=1800))
        if _point_ts(p) > sim["fill_ts"] and _is_quote(p)
    ]
    for stop in STOP_TICKS:
        stop_level = sim["fill_price"] + (-stop * tick if sign == 1 else stop * tick)
        triggered = next(
            (
                p
                for p in segment
                if (
                    _exit_price(p, candidate["side"], tick) <= stop_level + 1e-9
                    if sign == 1
                    else _exit_price(p, candidate["side"], tick) >= stop_level - 1e-9
                )
            ),
            None,
        )
        hit = triggered or (segment[-1] if segment else None)
        hit_ts = _point_ts(hit) if hit is not None else sim["fill_ts"]
        hit_price = (
            _exit_price(hit, candidate["side"], tick) if hit is not None else sim["fill_price"]
        )
        changes = [
            sign * (_point_price(p) - sim["fill_price"]) for p in segment if _point_ts(p) <= hit_ts
        ] or [0.0]
        gross = sign * (hit_price - sim["fill_price"])
        triggered_flag = triggered is not None
        slippage_ticks = (
            (
                max(0.0, (stop_level - hit_price) / tick)
                if sign == 1
                else max(0.0, (hit_price - stop_level) / tick)
            )
            if triggered_flag
            else 0.0
        )
        trigger_bid = (
            float(triggered["best_bid"])
            if triggered is not None and triggered.get("best_bid") is not None
            else None
        )
        trigger_ask = (
            float(triggered["best_ask"])
            if triggered is not None and triggered.get("best_ask") is not None
            else None
        )
        trigger_sequence = (
            int(triggered.get("sequence") or triggered.get("revision") or 0)
            if triggered is not None
            else None
        )
        rid = _id("stop", sim["simulation_id"], stop)
        row = _base(candidate, rid)
        row.update(
            result_id=rid,
            candidate_id=candidate["candidate_id"],
            simulation_id=sim["simulation_id"],
            side=candidate["side"],
            stop_ticks=stop,
            stop_level=stop_level,
            entry_ts=sim["fill_ts"],
            entry_price=sim["fill_price"],
            exit_ts=hit_ts,
            exit_price=hit_price,
            exit_reason="stop_triggered" if triggered is not None else "horizon_end",
            stop_triggered=triggered_flag,
            trigger_event_id=_market_event_id(triggered) if triggered_flag else None,
            trigger_event_type="raw_orderbook" if triggered_flag else None,
            trigger_ts=_point_ts(triggered) if triggered_flag else None,
            trigger_price=hit_price if triggered_flag else None,
            trigger_bid=trigger_bid,
            trigger_ask=trigger_ask,
            trigger_sequence=trigger_sequence,
            exit_source_event_id=(
                _market_event_id(hit)
                if hit is not None
                else _market_event_id_at_entry(path, times, sim["fill_ts"])
            ),
            exit_source_ts=hit_ts,
            gap_through_stop=triggered_flag and slippage_ticks > 1e-9,
            slippage_from_stop_ticks=slippage_ticks,
            gross_pnl=gross,
            net_pnl=gross - sim["spread_cost"],
            mfe_before_exit=max(0.0, max(changes)) / tick,
            mae_before_exit=max(0.0, -min(changes)) / tick,
            holding_ms=max(0, int((hit_ts - sim["fill_ts"]).total_seconds() * 1000)),
        )
        result.append(row)
    # One executable quote can jump across several distinct stop levels.  In
    # that case every identical result is explicitly marked as gap-explained,
    # including a level that happens to equal the executable quote exactly.
    by_trigger: dict[str, list[dict[str, Any]]] = {}
    for row in result:
        trigger_id = row.get("trigger_event_id")
        if trigger_id:
            by_trigger.setdefault(str(trigger_id), []).append(row)
    for group in by_trigger.values():
        signatures = {(row.get("exit_ts"), row.get("exit_price")) for row in group}
        levels = {row.get("stop_level") for row in group}
        if len(group) > 1 and len(signatures) == 1 and len(levels) > 1:
            for row in group:
                row["gap_through_stop"] = True
    return result


def _exits(
    candidate: Mapping[str, Any],
    sim: Mapping[str, Any],
    path: list[dict[str, Any]],
    times: list[datetime],
    tick: float,
) -> list[dict[str, Any]]:
    result = []
    sign = 1 if candidate["side"] == "LONG" else -1
    segment = _segment(path, times, sim["fill_ts"], sim["fill_ts"] + timedelta(seconds=1800))
    for variant in EXIT_VARIANTS:
        hit: dict[str, Any] | None = None
        activation: datetime | None = None
        trigger_value: float | None = None
        threshold: float | None = None
        reason = f"{variant}_horizon_end"
        if variant == "time_exit":
            target = sim["fill_ts"] + timedelta(seconds=300)
            hit = next((p for p in segment if _point_ts(p) >= target), None)
            threshold, reason = 300.0, "time_exit"
        elif variant == "take_profit":
            threshold = 4.0
            hit = next(
                (
                    p
                    for p in segment
                    if sign * (_exit_price(p, candidate["side"], tick) - sim["fill_price"])
                    >= threshold * tick
                ),
                None,
            )
            reason = "take_profit" if hit else reason
        elif variant in {"breakeven", "dynamic_breakeven"}:
            activation_ticks = 3.0 if variant == "breakeven" else 4.0
            cost = (
                0.0
                if variant == "breakeven"
                else sim["spread_cost"] + abs((sim["slippage_ticks"] or 0.0) * tick)
            )
            protection = sim["fill_price"] + sign * cost
            threshold = protection
            active = False
            for point in segment:
                favourable = sign * (_point_price(point) - sim["fill_price"]) / tick
                if not active and favourable >= activation_ticks:
                    active, activation = True, _point_ts(point)
                if (
                    active
                    and sign * (_exit_price(point, candidate["side"], tick) - protection) <= 0
                ):
                    hit, trigger_value = point, _exit_price(point, candidate["side"], tick)
                    break
            reason = variant if hit else reason
        elif variant == "trailing":
            threshold = 2.0
            peak = sim["fill_price"]
            level: float | None = None
            for point in segment:
                px = _point_price(point)
                peak = max(peak, px) if sign > 0 else min(peak, px)
                favourable = sign * (peak - sim["fill_price"]) / tick
                if level is None and favourable >= 3.0:
                    activation = _point_ts(point)
                    level = peak - sign * threshold * tick
                elif level is not None:
                    proposed = peak - sign * threshold * tick
                    level = max(level, proposed) if sign > 0 else min(level, proposed)
                if (
                    level is not None
                    and sign * (_exit_price(point, candidate["side"], tick) - level) <= 0
                ):
                    hit, trigger_value = point, level
                    break
            reason = "trailing_stop" if hit else reason
        elif variant == "microstructure":
            threshold = -2.0
            for previous, point in zip(segment, segment[1:], strict=False):
                momentum = sign * (_point_price(point) - _point_price(previous)) / tick
                if momentum <= threshold:
                    hit, activation, trigger_value = point, _point_ts(point), momentum
                    break
            reason = "microstructure_deterioration" if hit else reason
        elif variant == "orderbook":
            initial_spread = _spread(segment[0], tick) if segment else 2 * tick
            threshold = initial_spread * 1.5
            for point in segment:
                spread = _spread(point, tick)
                if spread >= threshold:
                    hit, activation, trigger_value = point, _point_ts(point), spread
                    break
            reason = "orderbook_deterioration" if hit else reason
        hit = hit or (segment[-1] if segment else None)
        hit_ts = _point_ts(hit) if hit is not None else sim["fill_ts"]
        hit_price = (
            _exit_price(hit, candidate["side"], tick) if hit is not None else sim["fill_price"]
        )
        prefix = [
            sign * (_point_price(p) - sim["fill_price"]) for p in segment if _point_ts(p) <= hit_ts
        ] or [0.0]
        gross = sign * (hit_price - sim["fill_price"])
        rid = _id("exit", sim["simulation_id"], variant)
        row = _base(candidate, rid)
        row.update(
            result_id=rid,
            candidate_id=candidate["candidate_id"],
            simulation_id=sim["simulation_id"],
            side=candidate["side"],
            exit_variant=variant,
            variant_parameters=json.dumps(
                {"tick_size": tick, "horizon_seconds": 1800, "trigger_logic": variant},
                separators=(",", ":"),
            ),
            entry_ts=sim["fill_ts"],
            entry_price=sim["fill_price"],
            activation_ts=activation,
            exit_ts=hit_ts,
            exit_price=hit_price,
            exit_reason=reason,
            trigger_value=trigger_value,
            trigger_threshold=threshold,
            gross_pnl=gross,
            net_pnl=gross - sim["spread_cost"],
            mfe_before_exit=max(0.0, max(prefix)) / tick,
            mae_before_exit=max(0.0, -min(prefix)) / tick,
            holding_ms=max(0, int((hit_ts - sim["fill_ts"]).total_seconds() * 1000)),
        )
        result.append(row)
    return result


def _nearest(path: Sequence[Any], times: list[datetime], target: datetime) -> float | None:
    # Horizon prices are strict as-of values. Taking the first quote after the
    # horizon introduces look-ahead and can fall outside the support window.
    index = bisect_right(times, target) - 1
    return None if not path or index < 0 else _point_price(path[index])


def _nearest_pair(path: Sequence[Any], target: datetime) -> Any:
    times = [_point_ts(p) for p in path]
    i = min(bisect_left(times, target), len(path) - 1)
    return path[i]


def _segment(
    path: Sequence[Any], times: list[datetime], start: datetime, end: datetime
) -> list[Any]:
    if not path:
        return []
    # Carry the last known quote into the interval. An event-driven order book
    # may be unchanged for a short horizon; that means a flat as-of price, not
    # an unknown outcome and not permission to take the next future snapshot.
    start_index = max(0, bisect_right(times, start) - 1)
    return list(path[start_index : bisect_right(times, end)])


def _point_ts(point: Any) -> datetime:
    value = point[0] if not isinstance(point, Mapping) else point["exchange_ts"]
    return _utc(_dt(value))


def _point_price(point: Any) -> float:
    return float(point[1] if not isinstance(point, Mapping) else point["mid_price"])


def _spread(point: Mapping[str, Any], tick: float) -> float:
    bid, ask = point.get("best_bid"), point.get("best_ask")
    return float(ask) - float(bid) if bid is not None and ask is not None else 2 * tick


def _book_levels(point: Mapping[str, Any], side: str) -> list[tuple[float, float]]:
    nested = point.get(side) or []
    levels = [
        (float(level["price"]), float(level.get("quantity") or level.get("volume") or 0.0))
        for level in nested
        if level.get("price") is not None
        and float(level.get("quantity") or level.get("volume") or 0.0) > 0
    ]
    if levels:
        return levels
    prefix = "ask" if side == "asks" else "bid"
    result: list[tuple[float, float]] = []
    for level in range(1, 21):
        price = point.get(f"{prefix}_price_{level:02d}") or point.get(f"{prefix}_price_{level}")
        quantity = (
            point.get(f"{prefix}_quantity_{level:02d}")
            or point.get(f"{prefix}_volume_{level:02d}")
            or point.get(f"{prefix}_quantity_{level}")
            or point.get(f"{prefix}_volume_{level}")
        )
        if price is not None and quantity is not None and float(quantity) > 0:
            result.append((float(price), float(quantity)))
    return result


def _exit_price(point: Mapping[str, Any] | None, side: str, tick: float) -> float:
    if point is None:
        raise ValueError("exit price requires a market point")
    field = "best_bid" if side == "LONG" else "best_ask"
    if point.get(field) is not None:
        return float(point[field])
    return _point_price(point) + (-tick if side == "LONG" else tick)


def _is_quote(point: Mapping[str, Any]) -> bool:
    return point.get("_event_type") != "raw_trade"


def _market_event_id(point: Mapping[str, Any] | None) -> str:
    if point is None:
        return ""
    return str(
        point.get("event_id")
        or point.get("raw_event_id")
        or _id("market", _point_ts(point), point.get("best_bid"), point.get("best_ask"))
    )


def _event_receive_ts(point: Mapping[str, Any]) -> datetime:
    return _utc(_dt(point.get("receive_ts") or point.get("recorded_at") or _point_ts(point)))


def _asof_quote(
    path: Sequence[Mapping[str, Any]], times: list[datetime], target: datetime
) -> Mapping[str, Any] | None:
    index = bisect_right(times, target) - 1
    while index >= 0:
        if _is_quote(path[index]) and _event_receive_ts(path[index]) <= target:
            return path[index]
        index -= 1
    return None


def _market_event_id_at_entry(
    path: Sequence[Mapping[str, Any]], times: list[datetime], target: datetime
) -> str:
    return _market_event_id(_asof_quote(path, times, target))


def _has_critical_gap(path: Sequence[Mapping[str, Any]], start: datetime, end: datetime) -> bool:
    quote_times = (
        [start]
        + [_point_ts(p) for p in path if _is_quote(p) and start <= _point_ts(p) <= end]
        + [end]
    )
    return any(
        (right - left).total_seconds() > 180
        for left, right in zip(quote_times, quote_times[1:], strict=False)
    )


def _normalize_market_point(row: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result["_event_type"] = "raw_orderbook"
    result["exchange_ts"] = _utc(_dt(row["exchange_ts"]))
    bids, asks = list(row.get("bids") or []), list(row.get("asks") or [])
    if result.get("best_bid") is None and bids:
        result["best_bid"] = bids[0].get("price")
    if result.get("best_ask") is None and asks:
        result["best_ask"] = asks[0].get("price")
    if result.get("mid_price") is None:
        result["mid_price"] = (float(result["best_bid"]) + float(result["best_ask"])) / 2
    result["mid_price"] = float(result["mid_price"])
    return result


def _merge_trade_events(
    path: list[dict[str, Any]], trades: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    """Merge trade evidence without changing its event time or looking ahead."""
    if not path:
        return path
    times = [p["exchange_ts"] for p in path]
    merged = list(path)
    for trade in trades:
        ts = _utc(_dt(trade["exchange_ts"]))
        index = bisect_right(times, ts) - 1
        if index < 0:
            continue
        point = dict(path[index])
        point["exchange_ts"] = ts
        point["_event_type"] = "raw_trade"
        point["trade_event_id"] = str(trade.get("event_id") or trade.get("raw_event_id") or "")
        quantity = float(trade.get("quantity") or trade.get("volume") or 0.0)
        side = str(trade.get("side") or trade.get("direction") or "").upper()
        point["buy_volume" if side in {"BUY", "LONG"} else "sell_volume"] = quantity
        if trade.get("price") is not None:
            point["trade_price"] = float(trade["price"])
        merged.append(point)
    return sorted(merged, key=_market_sort_key)


def _market_sort_key(point: Mapping[str, Any]) -> tuple[datetime, int]:
    return (
        _utc(_dt(point["exchange_ts"])),
        int(point.get("sequence") or point.get("revision") or 0),
    )


def _enrich_feature_market_fields(
    features: Sequence[Mapping[str, Any]],
    market_path: Sequence[Mapping[str, Any]],
    trades: Sequence[Mapping[str, Any]],
    last_prices: Sequence[Mapping[str, Any]],
    candles: Mapping[str, Sequence[Mapping[str, Any]]],
    trade_stream_provided: bool,
    strict_lineage: bool,
) -> list[dict[str, Any]]:
    """Compute trailing signed flow and age strictly as-of each feature timestamp."""
    normalized_trades: list[dict[str, Any]] = []
    seen_trade_ids: set[str] = set()
    for trade in trades:
        ts = _utc(_dt(trade["exchange_ts"]))
        quantity = float(trade.get("quantity") or trade.get("volume") or 0.0)
        side = str(
            trade.get("aggressor_side") or trade.get("side") or trade.get("direction") or "UNKNOWN"
        ).upper()
        if side in {"LONG", "AGGRESSIVE_BUY"}:
            side = "BUY"
        elif side in {"SHORT", "AGGRESSIVE_SELL"}:
            side = "SELL"
        elif side not in {"BUY", "SELL"}:
            side = "UNKNOWN"
        event_id = str(
            trade.get("event_id")
            or trade.get("raw_event_id")
            or _id("trade", ts, trade.get("trade_id"), trade.get("price"), quantity, side)
        )
        if event_id in seen_trade_ids:
            continue
        seen_trade_ids.add(event_id)
        normalized_trades.append(
            {
                "exchange_ts": ts,
                "receive_ts": _event_receive_ts(trade),
                "event_id": event_id,
                "quantity": quantity,
                "side": side,
            }
        )
    normalized_trades.sort(key=lambda item: (item["exchange_ts"], item["event_id"]))

    source_groups: dict[str, Sequence[Mapping[str, Any]]] = {
        "orderbook": [p for p in market_path if _is_quote(p)],
        "last_price": last_prices,
        "last_trade": normalized_trades,
        "candle_1m": candles.get("1m", candles.get("1", candles.get("candle_1m", ()))),
        "candle_5m": candles.get("5m", candles.get("5", candles.get("candle_5m", ()))),
        "candle_15m": candles.get("15m", candles.get("15", candles.get("candle_15m", ()))),
    }
    result: list[dict[str, Any]] = []
    for source in features:
        row = dict(source)
        feature_ts = _utc(
            _dt(row.get("exchange_ts") or row.get("timestamp") or row.get("recorded_at"))
        )
        processing_ts = _utc(
            _dt(
                row.get("processing_ts")
                or row.get("receive_ts")
                or row.get("recorded_at")
                or feature_ts
            )
        )
        for seconds in TRADE_WINDOWS:
            lower = feature_ts - timedelta(seconds=seconds)
            window = [t for t in normalized_trades if lower < t["exchange_ts"] <= feature_ts]
            known = [t for t in window if t["side"] in {"BUY", "SELL"}]
            buys = sum(t["quantity"] for t in known if t["side"] == "BUY")
            sells = sum(t["quantity"] for t in known if t["side"] == "SELL")
            suffix = f"{seconds}s"
            row[f"trade_count_{suffix}"] = len(window)
            row[f"known_side_trade_count_{suffix}"] = len(known)
            row[f"unknown_side_trade_count_{suffix}"] = len(window) - len(known)
            row[f"buy_volume_{suffix}"] = buys
            row[f"sell_volume_{suffix}"] = sells
            if trade_stream_provided:
                row[f"trades_{suffix}_signed_volume"] = buys - sells
            row[f"trade_window_first_event_id_{suffix}"] = window[0]["event_id"] if window else None
            row[f"trade_window_last_event_id_{suffix}"] = window[-1]["event_id"] if window else None

        ages: list[float] = []
        exchange_ages: list[float] = []
        for name, events in source_groups.items():
            eligible = [
                e
                for e in events
                if _utc(_dt(e["exchange_ts"])) <= feature_ts
                and _event_receive_ts(e) <= processing_ts
            ]
            event = max(eligible, key=lambda e: _utc(_dt(e["exchange_ts"]))) if eligible else None
            age_name = f"{name}_age_ms"
            id_name = f"{name}_source_event_id"
            if event is None:
                row[age_name] = None
                row[id_name] = None
                continue
            receive_ts = _event_receive_ts(event)
            age = (processing_ts - receive_ts).total_seconds() * 1000
            exchange_age = (feature_ts - _utc(_dt(event["exchange_ts"]))).total_seconds() * 1000
            row[age_name] = age
            row[id_name] = str(event.get("event_id") or event.get("raw_event_id") or "") or None
            ages.append(age)
            exchange_ages.append(exchange_age)
        row["oldest_required_source_age_ms"] = max(ages) if ages else None
        row["newest_source_age_ms"] = min(ages) if ages else None
        row["source_exchange_age_ms"] = max(exchange_ages) if exchange_ages else None
        row["book_age_ms"] = row.get("orderbook_age_ms")
        row["data_age_ms"] = row.get("newest_source_age_ms")
        required_sources = tuple(source_groups) if strict_lineage else ()
        lineage_missing = [
            name for name in required_sources if row.get(f"{name}_source_event_id") is None
        ]
        if lineage_missing:
            row["_lineage_missing"] = lineage_missing
            row["source_status"] = "MISSING_REQUIRED_SOURCE"
        result.append(row)
    return result


def _id(*parts: Any) -> str:
    return hashlib.sha256("|".join(map(str, parts)).encode()).hexdigest()


def _dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


__all__ = ["HORIZONS", "REVIEW_SCHEMAS", "build_review_datasets", "validate_review_links"]
