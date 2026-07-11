"""Raw-event execution, outcome, stop, and exit simulations for schema-v4.1."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Final

from .timeline import CanonicalEvent, CanonicalTimeline

HORIZONS: Final = (5, 10, 30, 60, 180, 300, 600, 900, 1800)
STOP_TICKS: Final = (2, 3, 4, 5)
EXIT_MODELS: Final = (
    "fixed_take_profit",
    "time_exit",
    "breakeven",
    "dynamic_breakeven",
    "trailing",
    "microstructure",
    "orderbook",
)


def _id(prefix: str, *parts: object) -> str:
    return f"{prefix}_{hashlib.sha256('|'.join(map(str, parts)).encode()).hexdigest()[:24]}"


def _book_levels(event: CanonicalEvent, side: str) -> list[tuple[float, float]]:
    value = event.row.get(side, ())
    return [(float(level["price"]), float(level["quantity"])) for level in value]


def _first_book(timeline: CanonicalTimeline, target: datetime) -> CanonicalEvent | None:
    return timeline.first_at_or_after("raw_orderbook", target)


def _executable(event: CanonicalEvent, position_side: str) -> float:
    levels = _book_levels(event, "bids" if position_side == "LONG" else "asks")
    return levels[0][0]


@dataclass(frozen=True, slots=True)
class ExecutionConfig:
    tick_size: float
    order_quantity: float = 1.0
    aggressive_latency_ms: int = 0
    passive_timeout_ms: int = 5_000
    take_profit_ticks: int = 4

    def __post_init__(self) -> None:
        if self.tick_size <= 0 or self.order_quantity <= 0:
            raise ValueError("tick size and order quantity must be positive")


class ExecutionSimulator:
    def __init__(self, timeline: CanonicalTimeline, config: ExecutionConfig) -> None:
        self.timeline, self.config = timeline, config

    def materialize(self, candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for candidate in candidates:
            rows.append(self._aggressive(candidate))
            rows.append(self._ideal(candidate))
            rows.append(self._passive(candidate))
        return rows

    def _base(self, candidate: dict[str, Any], model: str) -> dict[str, Any]:
        simulation_id = _id("simulation", candidate["candidate_id"], model)
        return {
            "event_id": simulation_id,
            "schema_version": "schema-v4.1",
            "exchange_ts": candidate["exchange_ts"],
            "receive_ts": candidate["receive_ts"],
            "processing_ts": candidate["processing_ts"],
            "materialized_ts": candidate["materialized_ts"],
            "simulation_id": simulation_id,
            "candidate_id": candidate["candidate_id"],
            "feature_snapshot_id": candidate["feature_snapshot_id"],
            "side": candidate["side"],
            "execution_model": model,
            "order_ts": candidate["exchange_ts"],
            "requested_quantity": self.config.order_quantity,
            "is_theoretical": model == "ideal_touch",
        }

    def _aggressive(self, candidate: dict[str, Any]) -> dict[str, Any]:
        row = self._base(candidate, "aggressive")
        target = candidate["exchange_ts"] + timedelta(
            milliseconds=self.config.aggressive_latency_ms
        )
        book = _first_book(self.timeline, target)
        levels = (
            _book_levels(book, "asks" if candidate["side"] == "LONG" else "bids") if book else []
        )
        remaining, notional, fills = self.config.order_quantity, 0.0, []
        for price, available in levels:
            quantity = min(remaining, available)
            if quantity > 0:
                notional += price * quantity
                remaining -= quantity
                fills.append(
                    {
                        "source_event_id": book.event_id if book else None,
                        "price": price,
                        "quantity": quantity,
                    }
                )
            if remaining <= 0:
                break
        filled = self.config.order_quantity - remaining
        status = "FULL" if remaining <= 1e-12 else "PARTIAL" if filled else "NO_FILL"
        return {
            **row,
            "order_price": candidate["reference_price"],
            "filled_quantity": filled,
            "remaining_quantity": remaining,
            "fill_status": status,
            "fill_ts": book.exchange_ts if filled and book else None,
            "fill_price": notional / filled if filled else None,
            "source_orderbook_event_id": book.event_id if book else None,
            "fill_lineage": fills,
            "queue_ahead_quantity": None,
            "aggressive_traded_volume": None,
        }

    def _ideal(self, candidate: dict[str, Any]) -> dict[str, Any]:
        row = self._base(candidate, "ideal_touch")
        price = candidate["best_ask"] if candidate["side"] == "LONG" else candidate["best_bid"]
        return {
            **row,
            "order_price": price,
            "filled_quantity": self.config.order_quantity,
            "remaining_quantity": 0.0,
            "fill_status": "FULL",
            "fill_ts": candidate["exchange_ts"],
            "fill_price": price,
            "source_orderbook_event_id": None,
            "fill_lineage": [],
            "queue_ahead_quantity": None,
            "aggressive_traded_volume": None,
        }

    def _passive(self, candidate: dict[str, Any]) -> dict[str, Any]:
        row = self._base(candidate, "passive_queue")
        start = self.timeline.latest("raw_orderbook", candidate["exchange_ts"])
        side = candidate["side"]
        levels = _book_levels(start, "bids" if side == "LONG" else "asks") if start else []
        price = levels[0][0] if levels else candidate["reference_price"]
        queue = levels[0][1] if levels else 0.0
        deadline = candidate["exchange_ts"] + timedelta(milliseconds=self.config.passive_timeout_ms)
        trades = self.timeline.after("raw_trades", candidate["exchange_ts"], deadline)
        matched, lineage = 0.0, []
        wanted_aggressor = "SELL" if side == "LONG" else "BUY"
        for trade in trades:
            trade_price = float(trade.row["price"])
            crosses = trade_price <= price if side == "LONG" else trade_price >= price
            if (
                str(trade.row.get("aggressor_side", "UNKNOWN")).upper() != wanted_aggressor
                or not crosses
            ):
                continue
            volume = float(trade.row["quantity"])
            queue_consumed = min(queue, volume)
            queue -= queue_consumed
            available = volume - queue_consumed
            fill = min(self.config.order_quantity - matched, available)
            if fill > 0:
                matched += fill
                lineage.append(
                    {"source_event_id": trade.event_id, "price": price, "quantity": fill}
                )
            if matched >= self.config.order_quantity:
                break
        last = next(
            (
                trade
                for trade in reversed(trades)
                if any(x["source_event_id"] == trade.event_id for x in lineage)
            ),
            None,
        )
        return {
            **row,
            "order_price": price,
            "filled_quantity": matched,
            "remaining_quantity": self.config.order_quantity - matched,
            "fill_status": "FULL"
            if matched >= self.config.order_quantity
            else "PARTIAL"
            if matched
            else "NO_FILL",
            "fill_ts": last.exchange_ts if last else None,
            "fill_price": price if matched else None,
            "source_orderbook_event_id": start.event_id if start else None,
            "fill_lineage": lineage,
            "queue_ahead_quantity": levels[0][1] if levels else 0.0,
            "aggressive_traded_volume": sum(
                float(item.row["quantity"])
                for item in trades
                if str(item.row.get("aggressor_side", "")).upper() == wanted_aggressor
            ),
        }


class OutcomeMaterializer:
    def __init__(
        self, timeline: CanonicalTimeline, *, tick_size: float, critical_gap_seconds: float = 180.0
    ) -> None:
        self.timeline = timeline
        self.tick_size = tick_size
        self.critical_gap_seconds = critical_gap_seconds

    def materialize(self, simulations: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        support_end = max(
            (e.exchange_ts for e in self.timeline.events("raw_orderbook")), default=None
        )
        for simulation in simulations:
            entry_ts = simulation.get("fill_ts")
            entry_price = simulation.get("fill_price")
            if not entry_ts or not entry_price or not simulation.get("filled_quantity"):
                continue
            for horizon in HORIZONS:
                target = entry_ts + timedelta(seconds=horizon)
                source = self.timeline.latest("raw_orderbook", target)
                complete = bool(source and support_end and support_end >= target)
                path = self.timeline.after("raw_orderbook", entry_ts, target) if complete else ()
                side = simulation["side"]
                prices = [(_executable(event, side), event) for event in path]
                executable = _executable(source, side) if complete and source else None
                pnl = (
                    ((executable - entry_price) if side == "LONG" else (entry_price - executable))
                    * simulation["filled_quantity"]
                    if executable is not None
                    else None
                )
                favorable = (
                    max(prices, key=lambda x: x[0])
                    if side == "LONG" and prices
                    else min(prices, key=lambda x: x[0])
                    if prices
                    else None
                )
                adverse = (
                    min(prices, key=lambda x: x[0])
                    if side == "LONG" and prices
                    else max(prices, key=lambda x: x[0])
                    if prices
                    else None
                )
                mfe = (
                    max(
                        0.0,
                        favorable[0] - entry_price
                        if side == "LONG"
                        else entry_price - favorable[0],
                    )
                    if favorable
                    else 0.0 if complete else None
                )
                mae = (
                    max(
                        0.0,
                        entry_price - adverse[0] if side == "LONG" else adverse[0] - entry_price,
                    )
                    if adverse
                    else 0.0 if complete else None
                )
                outcome_id = _id("outcome", simulation["simulation_id"], horizon)
                rows.append(
                    {
                        "event_id": outcome_id,
                        "schema_version": "schema-v4.1",
                        "exchange_ts": target,
                        "receive_ts": source.receive_ts if source else simulation["receive_ts"],
                        "processing_ts": source.processing_ts
                        if source
                        else simulation["processing_ts"],
                        "materialized_ts": simulation["materialized_ts"],
                        "outcome_id": outcome_id,
                        "simulation_id": simulation["simulation_id"],
                        "candidate_id": simulation["candidate_id"],
                        "side": side,
                        "entry_ts": entry_ts,
                        "entry_price": entry_price,
                        "target_ts": target,
                        "horizon_seconds": horizon,
                        "price_source_event_id": source.event_id if complete and source else None,
                        "price_source_exchange_ts": source.exchange_ts
                        if complete and source
                        else None,
                        "price_source_receive_ts": source.receive_ts
                        if complete and source
                        else None,
                        "source_age_ms": (target - source.exchange_ts).total_seconds() * 1000
                        if complete and source
                        else None,
                        "future_mid": (
                            float(source.row["best_bid"]) + float(source.row["best_ask"])
                        )
                        / 2
                        if complete and source
                        else None,
                        "exit_bid": float(source.row["best_bid"]) if complete and source else None,
                        "exit_ask": float(source.row["best_ask"]) if complete and source else None,
                        "executable_exit_price": executable,
                        "future_price": executable,
                        "price_selection_method": "last_orderbook_at_or_before_target",
                        "gap_detected": bool(
                            source
                            and (target - source.exchange_ts).total_seconds()
                            > self.critical_gap_seconds
                        ),
                        "support_complete": complete,
                        "outcome_complete": complete,
                        "pnl": pnl,
                        "gross_pnl": pnl,
                        "mfe": mfe,
                        "mae": mae,
                        "mfe_ticks": mfe / self.tick_size if mfe is not None else None,
                        "mae_ticks": mae / self.tick_size if mae is not None else None,
                        "mfe_source_event_id": favorable[1].event_id if favorable else None,
                        "mae_source_event_id": adverse[1].event_id if adverse else None,
                    }
                )
        return rows


class StopAndExitSimulator:
    def __init__(self, timeline: CanonicalTimeline, config: ExecutionConfig) -> None:
        self.timeline, self.config = timeline, config

    def materialize(
        self, simulations: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        stops: list[dict[str, Any]] = []
        exits: list[dict[str, Any]] = []
        support_end = max(
            (e.exchange_ts for e in self.timeline.events("raw_orderbook")), default=None
        )
        for sim in simulations:
            if not sim.get("fill_ts") or not sim.get("fill_price") or not support_end:
                continue
            path = self.timeline.after("raw_orderbook", sim["fill_ts"], support_end)
            for ticks in STOP_TICKS:
                stops.append(self._stop(sim, path, ticks))
            for model in EXIT_MODELS:
                exits.append(self._exit(sim, path, model))
        grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in stops:
            trigger_id = row.get("trigger_event_id")
            if trigger_id:
                grouped.setdefault((str(row["simulation_id"]), str(trigger_id)), []).append(row)
        for group in grouped.values():
            signatures = {(row.get("exit_ts"), row.get("exit_price")) for row in group}
            if len(group) > 1 and len(signatures) == 1:
                for row in group:
                    row["gap_through"] = True
                    row["gap_through_stop"] = True
        return stops, exits

    def _stop(
        self, sim: dict[str, Any], path: tuple[CanonicalEvent, ...], ticks: int
    ) -> dict[str, Any]:
        long = sim["side"] == "LONG"
        level = sim["fill_price"] + (-ticks if long else ticks) * self.config.tick_size
        trigger = next(
            (
                e
                for e in path
                if (
                    _executable(e, sim["side"]) <= level
                    if long
                    else _executable(e, sim["side"]) >= level
                )
            ),
            None,
        )
        price = _executable(trigger, sim["side"]) if trigger else None
        result_id = _id("stop", sim["simulation_id"], ticks)
        return {
            "event_id": result_id,
            "schema_version": "schema-v4.1",
            "exchange_ts": trigger.exchange_ts if trigger else sim["fill_ts"],
            "receive_ts": trigger.receive_ts if trigger else sim["receive_ts"],
            "processing_ts": trigger.processing_ts if trigger else sim["processing_ts"],
            "materialized_ts": sim["materialized_ts"],
            "result_id": result_id,
            "stop_result_id": result_id,
            "simulation_id": sim["simulation_id"],
            "candidate_id": sim["candidate_id"],
            "side": sim["side"],
            "entry_ts": sim["fill_ts"],
            "entry_price": sim["fill_price"],
            "exit_ts": trigger.exchange_ts if trigger else sim["fill_ts"],
            "stop_ticks": ticks,
            "stop_level": level,
            "triggered": trigger is not None,
            "stop_triggered": trigger is not None,
            "trigger_event_id": trigger.event_id if trigger else None,
            "trigger_ts": trigger.exchange_ts if trigger else None,
            "trigger_bid": float(trigger.row["best_bid"]) if trigger else None,
            "trigger_ask": float(trigger.row["best_ask"]) if trigger else None,
            "gap_through": bool(price is not None and (price < level if long else price > level)),
            "gap_through_stop": bool(
                price is not None and (price < level if long else price > level)
            ),
            "executable_exit_price": price,
            "exit_price": price,
            "slippage": abs(price - level) if price is not None else None,
            "exit_source_event_id": trigger.event_id if trigger else None,
        }

    def _exit(
        self, sim: dict[str, Any], path: tuple[CanonicalEvent, ...], model: str
    ) -> dict[str, Any]:
        entry, long = float(sim["fill_price"]), sim["side"] == "LONG"
        trigger: CanonicalEvent | None = None
        reason = "support_end"
        peak = entry
        for event in path:
            price = _executable(event, sim["side"])
            peak = max(peak, price) if long else min(peak, price)
            age = (event.exchange_ts - sim["fill_ts"]).total_seconds()
            tp = (
                entry
                + (self.config.take_profit_ticks if long else -self.config.take_profit_ticks)
                * self.config.tick_size
            )
            conditions = {
                "fixed_take_profit": price >= tp if long else price <= tp,
                "time_exit": age >= 60,
                "breakeven": age >= 5
                and (peak >= tp if long else peak <= tp)
                and (price <= entry if long else price >= entry),
                "dynamic_breakeven": age >= 10
                and (
                    price <= entry + self.config.tick_size
                    if long
                    else price >= entry - self.config.tick_size
                ),
                "trailing": (
                    peak - price >= 2 * self.config.tick_size
                    if long
                    else price - peak >= 2 * self.config.tick_size
                ),
                "microstructure": (float(event.row["best_bid"]) - float(event.row["best_ask"]))
                / max(float(event.row["best_bid"]), 1e-12)
                < -0.01,
                "orderbook": sum(q for _, q in _book_levels(event, "bids" if long else "asks"))
                < sum(q for _, q in _book_levels(event, "asks" if long else "bids")) * 0.25,
            }
            if conditions[model]:
                trigger, reason = event, model
                break
        if trigger is None and path:
            trigger = path[-1]
        exit_price = _executable(trigger, sim["side"]) if trigger else None
        result_id = _id("exit", sim["simulation_id"], model)
        return {
            "event_id": result_id,
            "schema_version": "schema-v4.1",
            "exchange_ts": trigger.exchange_ts if trigger else sim["fill_ts"],
            "receive_ts": trigger.receive_ts if trigger else sim["receive_ts"],
            "processing_ts": trigger.processing_ts if trigger else sim["processing_ts"],
            "materialized_ts": sim["materialized_ts"],
            "result_id": result_id,
            "exit_result_id": result_id,
            "simulation_id": sim["simulation_id"],
            "candidate_id": sim["candidate_id"],
            "side": sim["side"],
            "entry_ts": sim["fill_ts"],
            "exit_ts": trigger.exchange_ts if trigger else sim["fill_ts"],
            "exit_model": model,
            "exit_variant": model,
            "triggered": reason != "support_end",
            "trigger_reason": reason,
            "exit_reason": reason,
            "trigger_event_id": trigger.event_id if trigger else None,
            "trigger_ts": trigger.exchange_ts if trigger else None,
            "exit_source_event_id": trigger.event_id if trigger else None,
            "executable_exit_price": exit_price,
            "pnl": ((exit_price - entry) if long else (entry - exit_price)) * sim["filled_quantity"]
            if exit_price is not None
            else None,
        }
