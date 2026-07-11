"""Render a human-reviewable provenance chain for five real local feature timestamps."""

from __future__ import annotations

import argparse
from bisect import bisect_right
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pyarrow.parquet as pq  # type: ignore[import-untyped]


def _read(data: Path, name: str) -> list[dict[str, Any]]:
    return cast(list[dict[str, Any]], pq.read_table(data / f"{name}.parquet").to_pylist())


def _ts(row: dict[str, Any], *names: str) -> datetime:
    return cast(datetime, next(row[name] for name in names if row.get(name) is not None))


def generate(data: Path, output: Path, count: int = 5) -> None:
    raw_names = (
        "raw_orderbook",
        "raw_trades",
        "raw_last_price",
        "candles_1m",
        "candles_5m",
        "candles_15m",
        "market_status_events",
    )
    raw = {name: _read(data, name) for name in raw_names}
    books = sorted(raw["raw_orderbook"], key=lambda r: _ts(r, "exchange_ts"))
    trades = sorted(raw["raw_trades"], key=lambda r: _ts(r, "exchange_ts"))
    candles = sorted(raw["candles_1m"], key=lambda r: _ts(r, "exchange_ts"))
    features = _read(data, "feature_snapshots")
    candidates = _read(data, "candidate_events")
    executions = _read(data, "execution_simulations")
    outcomes = _read(data, "future_outcomes")
    stops = _read(data, "shadow_stop_results")
    exits = _read(data, "shadow_exit_results")
    eligible = [row for row in features if bool(row.get("feature_ready"))]
    if len(eligible) < count:
        raise RuntimeError(f"need {count} ready feature timestamps, found {len(eligible)}")
    indexes = [round(i * (len(eligible) - 1) / (count - 1)) for i in range(count)]
    selected = [eligible[index] for index in indexes]
    book_times = [_ts(row, "exchange_ts") for row in books]
    lines = [
        "# Golden real-data slice",
        "",
        "This report is generated from captured local Parquet. It is an inspection "
        "fixture, not a source of expected synthetic values.",
        "",
        "- Source: archived schema-v4 raw and independently validated derived Parquet",
        f"- Feature timestamps: {len(selected)}",
        "- Candidate sides: LONG and SHORT",
        f"- Outcome horizons present: {sorted({row['horizon_seconds'] for row in outcomes})}",
        f"- Stop distances present: {sorted({row['stop_ticks'] for row in stops})}",
        "",
    ]
    for number, feature in enumerate(selected, 1):
        feature_id = feature["feature_snapshot_id"]
        timestamp = _ts(feature, "exchange_ts")
        index = bisect_right(book_times, timestamp) - 1
        book = books[index]
        recent_trades = [
            row
            for row in trades
            if timestamp - timedelta(seconds=60) < _ts(row, "exchange_ts") <= timestamp
        ]
        signed_flow = sum(
            float(row.get("quantity") or 0.0)
            * (1 if str(row.get("aggressor_side") or "").upper() == "BUY" else -1)
            for row in recent_trades
        )
        prior_candles = [row for row in candles if _ts(row, "exchange_ts") <= timestamp]
        candle = prior_candles[-1] if prior_candles else None
        linked_candidates = [row for row in candidates if row["feature_snapshot_id"] == feature_id]
        candidate_ids = {row["candidate_id"] for row in linked_candidates}
        linked_exec = [row for row in executions if row["candidate_id"] in candidate_ids]
        simulation_ids = {row["simulation_id"] for row in linked_exec}
        linked_outcomes = [row for row in outcomes if row["simulation_id"] in simulation_ids]
        linked_stops = [row for row in stops if row["simulation_id"] in simulation_ids]
        linked_exits = [row for row in exits if row["simulation_id"] in simulation_ids]
        bid_levels, ask_levels = book.get("bids") or [], book.get("asks") or []
        candidate_summary = [(row["side"], row["candidate_id"]) for row in linked_candidates]
        fill_summary = dict(Counter(row["fill_status"] for row in linked_exec))
        horizons = sorted({row["horizon_seconds"] for row in linked_outcomes})
        triggered_stops = sum(bool(row.get("stop_triggered")) for row in linked_stops)
        exit_variants = sorted({row["exit_model"] for row in linked_exits})
        candle_id = (candle.get("candle_id") or candle.get("event_id")) if candle else "none"
        candle_close = candle.get("close") if candle else "n/a"
        candle_volume = candle.get("volume") if candle else "n/a"
        lines.extend(
            [
                f"## {number}. {timestamp.isoformat()}",
                "",
                f"- Feature: `{feature_id}`; ready={feature['feature_ready']}; "
                f"regime={feature.get('regime')}",
                f"- Book: `{book['event_id']}`; levels={len(bid_levels)}/{len(ask_levels)}; "
                f"bid/ask={book['best_bid']}/{book['best_ask']}",
                f"- Trades/flow: {len(recent_trades)} events; independently summed "
                f"60s signed flow={signed_flow:.6f}",
                f"- Candle: `{candle_id}`; close={candle_close}; volume={candle_volume}",
                f"- Candidates: {candidate_summary}",
                f"- Fills: {fill_summary}",
                f"- Targets/outcomes: {len(linked_outcomes)} rows; horizons={horizons}",
                f"- Stops: {len(linked_stops)} rows; triggered={triggered_stops}",
                f"- Exits: {len(linked_exits)} rows; variants={exit_variants}",
                "",
            ]
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("data", type=Path)
    parser.add_argument("--output", type=Path, default=Path("reports/golden_real_slice.md"))
    parser.add_argument("--count", type=int, choices=range(5, 11), default=5)
    args = parser.parse_args()
    generate(args.data, args.output, args.count)


if __name__ == "__main__":
    main()
