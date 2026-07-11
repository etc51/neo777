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
    *[(f"trade_flow_{n}s", pa.float64(), True) for n in (5, 10, 30, 60, 180, 300, 900)],
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
    ("order_price", pa.float64(), False),
    ("fill_status", pa.string(), False),
    ("fill_ts", TS, False),
    ("fill_price", pa.float64(), False),
    ("fill_delay_ms", pa.int64(), False),
    ("slippage_ticks", pa.float64(), False),
    ("slippage_percent", pa.float64(), False),
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
    ("future_price", pa.float64(), False),
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
    ("entry_ts", TS, False),
    ("entry_price", pa.float64(), False),
    ("exit_ts", TS, False),
    ("exit_price", pa.float64(), False),
    ("exit_reason", pa.string(), False),
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
    ("exit_ts", TS, False),
    ("exit_price", pa.float64(), False),
    ("exit_reason", pa.string(), False),
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
) -> dict[str, Any]:
    """Build a deterministic review graph from local data.

    ``price_path`` rows require ``exchange_ts`` and ``mid_price``. Feature rows
    may be promoted Parquet rows or their original record dictionaries.
    """

    start, end = _utc(candidate_start), _utc(candidate_end)
    path = sorted((_utc(_dt(p["exchange_ts"])), float(p["mid_price"])) for p in price_path)
    path_ts = [p[0] for p in path]
    normalized_features = [_feature_row(row) for row in feature_rows]
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
                simulation = _execution(candidate, model, tick_size)
                executions.append(simulation)
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
        "processing_ts": receive,
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
            value = record.get("trades_" + field.removeprefix("trade_flow_") + "_imbalance")
        if "_quantity_" in field:
            value = record.get(field.replace("_quantity_", "_volume_").replace("_0", "_"))
        if "_price_" in field:
            value = record.get(field.replace("_0", "_"))
        row[field] = value
    row["regime"] = str(record.get("regime") or "unknown")
    row["source_status"] = str(record.get("trading_status") or "NORMAL_TRADING")
    row["feature_ready"] = bool(record.get("feature_ready", True))
    row["warmup_remaining"] = int(record.get("warmup_remaining") or 0)
    row["missing_reason"] = record.get("missing_reason")
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


def _execution(candidate: Mapping[str, Any], model: str, tick: float) -> dict[str, Any]:
    sid = _id("simulation", candidate["candidate_id"], model)
    row = _base(candidate, sid)
    long = candidate["side"] == "LONG"
    touch = candidate["best_ask"] if long else candidate["best_bid"]
    delay = {
        "aggressive_marketable": 0,
        "ideal_touch": 0,
        "passive_best": 100,
        "passive_conservative": 250,
        "passive_delayed": 500,
    }[model]
    offset = {
        "aggressive_marketable": 0,
        "ideal_touch": 0,
        "passive_best": -1,
        "passive_conservative": -2,
        "passive_delayed": -1,
    }[model]
    price = touch + (offset * tick if long else -offset * tick)
    slip = (price - touch) / tick * (1 if long else -1)
    row.update(
        simulation_id=sid,
        candidate_id=candidate["candidate_id"],
        feature_snapshot_id=candidate["feature_snapshot_id"],
        side=candidate["side"],
        execution_model=model,
        order_price=price,
        fill_status="FULL_FILL",
        fill_ts=candidate["candidate_ts"] + timedelta(milliseconds=delay),
        fill_price=price,
        fill_delay_ms=delay,
        slippage_ticks=slip,
        slippage_percent=(price - touch) / touch * 100 * (1 if long else -1),
        spread_cost=abs(candidate["best_ask"] - candidate["best_bid"]) / 2,
        no_fill_reason=None,
    )
    return row


def _outcomes(
    candidate: Mapping[str, Any],
    sim: Mapping[str, Any],
    path: list[tuple[datetime, float]],
    times: list[datetime],
    tick: float,
) -> list[dict[str, Any]]:
    result = []
    sign = 1 if candidate["side"] == "LONG" else -1
    entry = sim["fill_ts"]
    for horizon in HORIZONS:
        future = entry + timedelta(seconds=horizon)
        segment = _segment(path, times, entry, future)
        price = _nearest(path, times, future)
        if price is None or not segment:
            continue
        changes = [sign * (p - sim["fill_price"]) for _, p in segment]
        mfe = max(changes)
        mae = min(changes)
        mfe_i = changes.index(mfe)
        mae_i = changes.index(mae)
        raw = sign * (price - sim["fill_price"])
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
            future_price=price,
            raw_return=raw,
            return_ticks=raw / tick,
            return_percent=raw / sim["fill_price"] * 100,
            mfe_ticks=mfe / tick,
            mae_ticks=mae / tick,
            mfe_percent=mfe / sim["fill_price"] * 100,
            mae_percent=mae / sim["fill_price"] * 100,
            time_to_mfe_ms=int((segment[mfe_i][0] - entry).total_seconds() * 1000),
            time_to_mae_ms=int((segment[mae_i][0] - entry).total_seconds() * 1000),
            spread_cost=spread,
            slippage_cost=abs(sim["slippage_ticks"] * tick),
            gross_pnl=raw,
            net_pnl=raw - spread - abs(sim["slippage_ticks"] * tick),
            outcome_complete=True,
            missing_reason=None,
        )
        result.append(row)
    return result


def _stops(
    candidate: Mapping[str, Any],
    sim: Mapping[str, Any],
    path: list[tuple[datetime, float]],
    times: list[datetime],
    tick: float,
) -> list[dict[str, Any]]:
    result = []
    sign = 1 if candidate["side"] == "LONG" else -1
    segment = _segment(path, times, sim["fill_ts"], sim["fill_ts"] + timedelta(seconds=1800))
    for stop in STOP_TICKS:
        threshold = -stop * tick
        hit = next(
            ((ts, p) for ts, p in segment if sign * (p - sim["fill_price"]) <= threshold),
            segment[-1] if segment else (sim["fill_ts"], sim["fill_price"]),
        )
        changes = [sign * (p - sim["fill_price"]) for ts, p in segment if ts <= hit[0]] or [0.0]
        gross = sign * (hit[1] - sim["fill_price"])
        rid = _id("stop", sim["simulation_id"], stop)
        row = _base(candidate, rid)
        row.update(
            result_id=rid,
            candidate_id=candidate["candidate_id"],
            simulation_id=sim["simulation_id"],
            side=candidate["side"],
            stop_ticks=stop,
            entry_ts=sim["fill_ts"],
            entry_price=sim["fill_price"],
            exit_ts=hit[0],
            exit_price=hit[1],
            exit_reason="stop" if gross <= threshold else "horizon",
            gross_pnl=gross,
            net_pnl=gross - sim["spread_cost"],
            mfe_before_exit=max(changes) / tick,
            mae_before_exit=min(changes) / tick,
            holding_ms=int((hit[0] - sim["fill_ts"]).total_seconds() * 1000),
        )
        result.append(row)
    return result


def _exits(
    candidate: Mapping[str, Any],
    sim: Mapping[str, Any],
    path: list[tuple[datetime, float]],
    times: list[datetime],
    tick: float,
) -> list[dict[str, Any]]:
    result = []
    sign = 1 if candidate["side"] == "LONG" else -1
    segment = _segment(path, times, sim["fill_ts"], sim["fill_ts"] + timedelta(seconds=1800))
    for variant in EXIT_VARIANTS:
        if not segment:
            hit = (sim["fill_ts"], sim["fill_price"])
        elif variant == "take_profit":
            hit = next(
                ((ts, p) for ts, p in segment if sign * (p - sim["fill_price"]) >= 4 * tick),
                segment[-1],
            )
        elif variant == "time_exit":
            hit = _nearest_pair(segment, sim["fill_ts"] + timedelta(seconds=300))
        else:
            hit = segment[-1]
        prefix = [sign * (p - sim["fill_price"]) for ts, p in segment if ts <= hit[0]] or [0.0]
        gross = sign * (hit[1] - sim["fill_price"])
        rid = _id("exit", sim["simulation_id"], variant)
        row = _base(candidate, rid)
        row.update(
            result_id=rid,
            candidate_id=candidate["candidate_id"],
            simulation_id=sim["simulation_id"],
            side=candidate["side"],
            exit_variant=variant,
            variant_parameters=json.dumps(
                {"tick_size": tick, "horizon_seconds": 1800}, separators=(",", ":")
            ),
            entry_ts=sim["fill_ts"],
            entry_price=sim["fill_price"],
            exit_ts=hit[0],
            exit_price=hit[1],
            exit_reason=variant,
            gross_pnl=gross,
            net_pnl=gross - sim["spread_cost"],
            mfe_before_exit=max(prefix) / tick,
            mae_before_exit=min(prefix) / tick,
            holding_ms=int((hit[0] - sim["fill_ts"]).total_seconds() * 1000),
        )
        result.append(row)
    return result


def _nearest(
    path: list[tuple[datetime, float]], times: list[datetime], target: datetime
) -> float | None:
    # Horizon prices are strict as-of values. Taking the first quote after the
    # horizon introduces look-ahead and can fall outside the support window.
    index = bisect_right(times, target) - 1
    return None if not path or index < 0 else path[index][1]


def _nearest_pair(path: list[tuple[datetime, float]], target: datetime) -> tuple[datetime, float]:
    times = [p[0] for p in path]
    i = min(bisect_left(times, target), len(path) - 1)
    return path[i]


def _segment(
    path: list[tuple[datetime, float]], times: list[datetime], start: datetime, end: datetime
) -> list[tuple[datetime, float]]:
    if not path:
        return []
    # Carry the last known quote into the interval. An event-driven order book
    # may be unchanged for a short horizon; that means a flat as-of price, not
    # an unknown outcome and not permission to take the next future snapshot.
    start_index = max(0, bisect_right(times, start) - 1)
    return path[start_index : bisect_right(times, end)]


def _id(*parts: Any) -> str:
    return hashlib.sha256("|".join(map(str, parts)).encode()).hexdigest()


def _dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


__all__ = ["HORIZONS", "REVIEW_SCHEMAS", "build_review_datasets", "validate_review_links"]
