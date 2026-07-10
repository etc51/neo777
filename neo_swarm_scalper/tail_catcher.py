"""Shadow tail-catcher strategy for T-Bank neoassets.

The engine never places broker orders. It consumes read-only market snapshots,
creates paper/shadow trades from a single signal price, and compares stop,
protection, and trailing configurations side by side.
"""

from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from statistics import median
from typing import Any
from uuid import uuid4

from neo_swarm_scalper.config import NeoSwarmScalperConfig
from neo_swarm_scalper.entry_engine import EntryCandidate, EntryTypeEngine
from neo_swarm_scalper.expectancy import (
    POLICY_VERSION,
    adaptive_control_stop_ticks,
    evaluate_entry_candidate,
    protection_threshold_ticks,
)
from neo_swarm_scalper.storage import SQLiteJournal
from neo_swarm_scalper.types import FeatureSnapshot, MarketSnapshot, PositionSide

SPREAD_SHOCK_GRACE_CYCLES = 3


@dataclass
class _InstrumentState:
    prices: deque[tuple[datetime, Decimal]]
    pressure_history: deque[Decimal]
    previous_spread_ticks: Decimal | None = None
    previous_imbalance: Decimal | None = None
    previous_velocity: Decimal = Decimal("0")
    last_signal_time: datetime | None = None
    pending_side: PositionSide | None = None
    pending_confirmation_cycles: int = 0


@dataclass
class _ShadowTrade:
    trade_id: str
    signal_id: str
    instrument: str
    side: PositionSide
    stop_ticks: int
    protection_trigger_bps: Decimal
    trailing_mode: str
    entry_time: datetime
    entry_price: Decimal
    theoretical_bid: Decimal | None
    theoretical_ask: Decimal | None
    stop_price: Decimal
    best_price_after_entry: Decimal
    worst_price_after_entry: Decimal
    mfe_abs: Decimal = Decimal("0")
    mfe_pct: Decimal = Decimal("0")
    mfe_ticks: Decimal = Decimal("0")
    mae_abs: Decimal = Decimal("0")
    mae_pct: Decimal = Decimal("0")
    mae_ticks: Decimal = Decimal("0")
    time_to_mfe_sec: Decimal | None = None
    time_to_mae_sec: Decimal | None = None
    max_runup_before_exit: Decimal = Decimal("0")
    max_drawdown_before_exit: Decimal = Decimal("0")
    protection_activated: bool = False
    protection_price: Decimal | None = None
    protected_exit_reason: str | None = None
    entry_type: str | None = None
    direction_model: str | None = None
    direction_score: Decimal = Decimal("0")
    pressure_score: Decimal = Decimal("0")
    impulse_score: Decimal = Decimal("0")
    pullback_score: Decimal = Decimal("0")
    book_flip_score: Decimal = Decimal("0")
    reversal_score: Decimal = Decimal("0")
    lead_lag_score: Decimal = Decimal("0")
    expected_mfe_ticks: Decimal = Decimal("0")
    expected_stop_risk_ticks: Decimal = Decimal("0")
    reentry_series_id: str | None = None
    reentry_number: int = 0
    reason_reentry_allowed: str | None = None
    reason_reentry_blocked: str | None = None
    trailing_reason: str | None = None
    reentry_index: int = 0
    consecutive_stop_index: int = 0
    spread_bad_cycles: int = 0
    opportunity_id: str | None = None
    is_control: bool = False
    experiment_role: str = "research"
    policy_version: str = POLICY_VERSION
    trigger_entry_price: Decimal | None = None
    execution_cost_ticks: Decimal = Decimal("0")
    direction_margin: Decimal = Decimal("0")
    confirmation_cycles: int = 0
    max_hold_sec: int = 120
    stop_trigger_cycles: int = 0
    protection_trigger_cycles: int = 0
    trailing_bad_cycles: int = 0
    time_exit_bad_spread_cycles: int = 0
    exit_trigger_price: Decimal | None = None
    execution_shortfall_ticks: Decimal = Decimal("0")


class TailCatcherEngine:
    """Runs the NEOBITOK shadow strategy on each market-data cycle."""

    def __init__(self, config: NeoSwarmScalperConfig, storage: SQLiteJournal) -> None:
        self.config = config
        self.storage = storage
        self._states: dict[str, _InstrumentState] = {}
        self._open_trades: dict[str, _ShadowTrade] = {}
        self._entry_engine = EntryTypeEngine(
            min_direction_score=config.tail_catcher.control_min_direction_score,
            expected_mfe_atr_capture=config.tail_catcher.expected_mfe_atr_capture,
            stop_atr_fraction=config.tail_catcher.control_stop_atr_fraction,
            stop_ticks_min=config.tail_catcher.control_stop_ticks_min,
            stop_ticks_max=config.tail_catcher.control_stop_ticks_max,
        )
        self._latest_contexts: dict[str, dict[str, Any]] = {}
        self._load_open_trades()
        self._backfill_missing_mfe_mae()

    def process(
        self,
        *,
        timestamp_utc: datetime,
        snapshots: list[MarketSnapshot] | tuple[MarketSnapshot, ...],
        features: dict[str, FeatureSnapshot],
    ) -> None:
        self._record_health(timestamp_utc, "running", {"snapshots": len(snapshots)})
        for snapshot in snapshots:
            self._upsert_instrument(snapshot)
            state = self._state(snapshot.instrument)
            micro = self._microstructure(snapshot, state)
            volatility = self._volatility(snapshot, state)
            money_flow = self._money_flow(snapshot, micro, volatility)
            self._record_market_detail(snapshot, micro)
            self._record_feature_tables(snapshot, micro, volatility, money_flow)
            self._update_open_trades(snapshot, micro, volatility, timestamp_utc)
            peer_contexts = {
                **self._latest_contexts,
                snapshot.instrument: {
                    "micro": micro,
                    "volatility": volatility,
                    "timestamp_utc": timestamp_utc,
                },
            }
            allowed, side, confidence, reason, gates, entry = self._curator_decision(
                snapshot,
                micro,
                volatility,
                features.get(snapshot.instrument),
                state=state,
                peer_contexts=peer_contexts,
            )
            signal_id = self._record_signal(
                timestamp_utc=timestamp_utc,
                snapshot=snapshot,
                side=side,
                confidence=confidence,
                reason=reason,
                gates=gates,
                micro=micro,
                volatility=volatility,
                money_flow=money_flow,
                entry=entry,
            )
            if entry is not None:
                self._record_live_entry_score(timestamp_utc, snapshot, entry)
            self._record_curator(
                timestamp_utc=timestamp_utc,
                instrument=snapshot.instrument,
                allowed=allowed,
                side=side,
                confidence=confidence,
                reason=reason,
                gates=gates,
            )
            if (
                allowed
                and side is not None
                and entry is not None
                and not self._has_open_matrix(snapshot.instrument)
            ):
                opened = self._open_shadow_matrix(
                    signal_id=signal_id,
                    snapshot=snapshot,
                    side=side,
                    timestamp_utc=timestamp_utc,
                    micro=micro,
                    volatility=volatility,
                    entry=entry,
                    spread_entry=gates["spread_entry"],
                    entry_policy=gates["entry_policy"],
                    confirmation_cycles=int(gates["confirmation_cycles"]),
                )
                if opened:
                    state.last_signal_time = timestamp_utc
                    state.pending_side = None
                    state.pending_confirmation_cycles = 0
            self._record_experiment_metrics(timestamp_utc, snapshot.instrument)
            self._record_entry_type_performance(timestamp_utc, snapshot.instrument)
            self._latest_contexts[snapshot.instrument] = {
                "micro": micro,
                "volatility": volatility,
                "timestamp_utc": timestamp_utc,
            }

    def _state(self, instrument: str) -> _InstrumentState:
        return self._states.setdefault(
            instrument,
            _InstrumentState(
                prices=deque(maxlen=2000),
                pressure_history=deque(
                    maxlen=self.config.tail_catcher.pressure_confirmation_window
                ),
            ),
        )

    def _load_open_trades(self) -> None:
        if not self.storage.path.exists():
            return
        try:
            rows = self.storage.fetch_all(
                """
                SELECT *
                FROM shadow_trades
                WHERE status = 'OPEN'
                """
            )
        except Exception:  # noqa: BLE001 - schema may not exist before initialize().
            return
        for row in rows:
            side = PositionSide(str(row["side"]))
            trade = _ShadowTrade(
                trade_id=str(row["trade_id"]),
                signal_id=str(row["signal_id"]),
                instrument=str(row["instrument"]),
                side=side,
                stop_ticks=int(row["stop_ticks"]),
                protection_trigger_bps=Decimal(str(row["protection_trigger_bps"])),
                trailing_mode=str(row["trailing_mode"]),
                entry_time=_dt(row["entry_time"]),
                entry_price=Decimal(str(row["entry_price"])),
                theoretical_bid=_dec_or_none(row["theoretical_bid"]),
                theoretical_ask=_dec_or_none(row["theoretical_ask"]),
                stop_price=Decimal(str(row["stop_price"])),
                best_price_after_entry=Decimal(str(row["best_price_after_entry"])),
                worst_price_after_entry=Decimal(str(row["worst_price_after_entry"])),
                mfe_abs=Decimal(str(row["mfe_abs"])),
                mfe_pct=Decimal(str(row["mfe_pct"])),
                mfe_ticks=Decimal(str(row["mfe_ticks"])),
                mae_abs=Decimal(str(row["mae_abs"])),
                mae_pct=Decimal(str(row["mae_pct"])),
                mae_ticks=Decimal(str(row["mae_ticks"])),
                time_to_mfe_sec=_dec_or_none(row["time_to_mfe_sec"]),
                time_to_mae_sec=_dec_or_none(row["time_to_mae_sec"]),
                max_runup_before_exit=Decimal(str(row["max_runup_before_exit"])),
                max_drawdown_before_exit=Decimal(str(row["max_drawdown_before_exit"])),
                protection_activated=bool(row["protection_activated"]),
                protection_price=_dec_or_none(row["protection_price"]),
                protected_exit_reason=(
                    None
                    if row["protected_exit_reason"] is None
                    else str(row["protected_exit_reason"])
                ),
                entry_type=None if row["entry_type"] is None else str(row["entry_type"]),
                direction_model=(
                    None if row["direction_model"] is None else str(row["direction_model"])
                ),
                direction_score=Decimal(str(row["direction_score"])),
                pressure_score=Decimal(str(row["pressure_score"])),
                impulse_score=Decimal(str(row["impulse_score"])),
                pullback_score=Decimal(str(row["pullback_score"])),
                book_flip_score=Decimal(str(row["book_flip_score"])),
                reversal_score=Decimal(str(row["reversal_score"])),
                lead_lag_score=Decimal(str(row["lead_lag_score"])),
                expected_mfe_ticks=Decimal(str(row["expected_mfe_ticks"])),
                expected_stop_risk_ticks=Decimal(str(row["expected_stop_risk_ticks"])),
                reentry_series_id=(
                    None if row["reentry_series_id"] is None else str(row["reentry_series_id"])
                ),
                reentry_number=int(row["reentry_number"]),
                reason_reentry_allowed=(
                    None
                    if row["reason_reentry_allowed"] is None
                    else str(row["reason_reentry_allowed"])
                ),
                reason_reentry_blocked=(
                    None
                    if row["reason_reentry_blocked"] is None
                    else str(row["reason_reentry_blocked"])
                ),
                trailing_reason=(
                    None if row["trailing_reason"] is None else str(row["trailing_reason"])
                ),
                reentry_index=int(row["reentry_index"]),
                consecutive_stop_index=int(row["consecutive_stop_index"]),
                opportunity_id=(
                    str(row["opportunity_id"])
                    if row["opportunity_id"] is not None
                    else f"OPPORTUNITY_{row['signal_id']}"
                ),
                is_control=bool(row["is_control"]),
                experiment_role=str(row["experiment_role"]),
                policy_version=(
                    POLICY_VERSION if row["policy_version"] is None else str(row["policy_version"])
                ),
                trigger_entry_price=(
                    Decimal(str(row["trigger_entry_price"]))
                    if row["trigger_entry_price"] is not None
                    else Decimal(str(row["entry_price"]))
                ),
                execution_cost_ticks=Decimal(str(row["execution_cost_ticks"])),
                direction_margin=Decimal(str(row["direction_margin"])),
                confirmation_cycles=int(row["confirmation_cycles"]),
                max_hold_sec=int(row["max_hold_sec"]),
                spread_bad_cycles=int(row["spread_bad_cycles"]),
                stop_trigger_cycles=int(row["stop_trigger_cycles"]),
                protection_trigger_cycles=int(row["protection_trigger_cycles"]),
                trailing_bad_cycles=int(row["trailing_bad_cycles"]),
                time_exit_bad_spread_cycles=int(row["time_exit_bad_spread_cycles"]),
                exit_trigger_price=_dec_or_none(row["exit_trigger_price"]),
                execution_shortfall_ticks=Decimal(str(row["execution_shortfall_ticks"])),
            )
            self._open_trades[trade.trade_id] = trade

    def _backfill_missing_mfe_mae(self) -> None:
        if not self.storage.path.exists():
            return
        try:
            with self.storage.connect() as conn:
                conn.execute(
                    """
                    INSERT INTO mfe_mae_tracking(
                        trade_id, timestamp_utc, current_price, mfe_abs, mfe_pct,
                        mfe_ticks, mae_abs, mae_pct, mae_ticks, time_to_mfe_sec,
                        time_to_mae_sec
                    )
                    SELECT
                        st.trade_id, st.entry_time, st.entry_price,
                        0, 0, 0, 0, 0, 0, NULL, NULL
                    FROM shadow_trades st
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM mfe_mae_tracking mt
                        WHERE mt.trade_id = st.trade_id
                          AND mt.timestamp_utc = st.entry_time
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO mfe_mae_tracking(
                        trade_id, timestamp_utc, current_price, mfe_abs, mfe_pct,
                        mfe_ticks, mae_abs, mae_pct, mae_ticks, time_to_mfe_sec,
                        time_to_mae_sec
                    )
                    SELECT
                        st.trade_id, st.exit_time, COALESCE(st.exit_price, st.entry_price),
                        st.mfe_abs, st.mfe_pct, st.mfe_ticks,
                        st.mae_abs, st.mae_pct, st.mae_ticks,
                        st.time_to_mfe_sec, st.time_to_mae_sec
                    FROM shadow_trades st
                    WHERE st.exit_time IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1
                          FROM mfe_mae_tracking mt
                          WHERE mt.trade_id = st.trade_id
                            AND mt.timestamp_utc = st.exit_time
                      )
                    """
                )
                conn.commit()
        except Exception:  # noqa: BLE001 - tolerate partial historical schemas.
            return

    def _microstructure(
        self,
        snapshot: MarketSnapshot,
        state: _InstrumentState,
    ) -> dict[str, Any]:
        bid = snapshot.best_bid
        ask = snapshot.best_ask
        mid = snapshot.mid_price
        spread_abs = snapshot.spread_abs
        tick = snapshot.tick_size or Decimal("1")
        spread_ticks = snapshot.spread_ticks
        spread_bps = Decimal("0")
        if spread_abs is not None and mid and mid != 0:
            spread_bps = (spread_abs / mid) * Decimal("10000")

        top_bid = snapshot.bid_volume(1)
        top_ask = snapshot.ask_volume(1)
        depth_bid_3 = snapshot.bid_volume(3)
        depth_bid_5 = snapshot.bid_volume(5)
        depth_bid_10 = snapshot.bid_volume(10)
        depth_ask_3 = snapshot.ask_volume(3)
        depth_ask_5 = snapshot.ask_volume(5)
        depth_ask_10 = snapshot.ask_volume(10)
        imbalance_3 = _imbalance(depth_bid_3, depth_ask_3)
        imbalance_5 = _imbalance(depth_bid_5, depth_ask_5)
        imbalance_10 = _imbalance(depth_bid_10, depth_ask_10)
        microprice = mid
        if bid is not None and ask is not None and top_bid + top_ask > 0:
            microprice = ((ask * top_bid) + (bid * top_ask)) / (top_bid + top_ask)
        microprice_deviation = Decimal("0")
        if microprice is not None and mid is not None and tick:
            microprice_deviation = (microprice - mid) / tick
        pressure_score = (
            (imbalance_3 * Decimal("0.50"))
            + (imbalance_5 * Decimal("0.35"))
            + (imbalance_10 * Decimal("0.15"))
        )
        liquidity_score = depth_bid_5 + depth_ask_5
        book_slope = _book_slope(snapshot)
        wall_bid = _wall_detect(snapshot.bid_levels)
        wall_ask = _wall_detect(snapshot.ask_levels)
        spread_expansion = (
            state.previous_spread_ticks is not None
            and spread_ticks is not None
            and spread_ticks > state.previous_spread_ticks
        )
        spread_compression = (
            state.previous_spread_ticks is not None
            and spread_ticks is not None
            and spread_ticks < state.previous_spread_ticks
        )
        quote_stability = Decimal("1")
        if spread_ticks is not None:
            quote_stability = Decimal("1") / (Decimal("1") + abs(spread_ticks))
        orderbook_flip = (
            state.previous_imbalance is not None
            and state.previous_imbalance != 0
            and imbalance_3 != 0
            and (state.previous_imbalance > 0) != (imbalance_3 > 0)
        )
        thin_book = liquidity_score <= self.config.tail_catcher.thin_book_depth_top5
        state.previous_spread_ticks = spread_ticks
        state.previous_imbalance = imbalance_3
        state.pressure_history.append(pressure_score)
        return {
            "bid": bid,
            "ask": ask,
            "mid": mid,
            "spread_abs": spread_abs,
            "spread_ticks": spread_ticks,
            "spread_bps": spread_bps,
            "top_bid_qty": top_bid,
            "top_ask_qty": top_ask,
            "depth_bid_3": depth_bid_3,
            "depth_bid_5": depth_bid_5,
            "depth_bid_10": depth_bid_10,
            "depth_ask_3": depth_ask_3,
            "depth_ask_5": depth_ask_5,
            "depth_ask_10": depth_ask_10,
            "orderbook_imbalance_3": imbalance_3,
            "orderbook_imbalance_5": imbalance_5,
            "orderbook_imbalance_10": imbalance_10,
            "microprice": microprice,
            "microprice_deviation": microprice_deviation,
            "pressure_score": pressure_score,
            "liquidity_score": liquidity_score,
            "book_slope": book_slope,
            "wall_detect_bid": wall_bid,
            "wall_detect_ask": wall_ask,
            "spread_expansion_flag": spread_expansion,
            "spread_compression_flag": spread_compression,
            "quote_stability": quote_stability,
            "orderbook_flip_flag": orderbook_flip,
            "stale_orderbook_flag": snapshot.stale,
            "thin_book_flag": thin_book,
        }

    def _volatility(
        self,
        snapshot: MarketSnapshot,
        state: _InstrumentState,
    ) -> dict[str, Any]:
        price = snapshot.executable_price or snapshot.mid_price
        timestamp = snapshot.timestamp_utc.astimezone(UTC)
        if price is None:
            price = Decimal("0")
        state.prices.append((timestamp, price))
        rv = {
            "10s": _realized(state.prices, 10),
            "30s": _realized(state.prices, 30),
            "1m": _realized(state.prices, 60),
            "3m": _realized(state.prices, 180),
            "5m": _realized(state.prices, 300),
            "15m": _realized(state.prices, 900),
        }
        price_velocity = _velocity(state.prices, 30)
        acceleration = price_velocity - state.previous_velocity
        state.previous_velocity = price_velocity
        tick = snapshot.tick_size or Decimal("1")
        tick_velocity = price_velocity / tick if tick else Decimal("0")
        atr_range = _range(state.prices, 300)
        range_position = _range_position(state.prices, price, 300)
        impulse_score = abs(tick_velocity)
        chop_score = Decimal("1") / (Decimal("1") + impulse_score)
        regime = "normal"
        if rv["1m"] <= Decimal("0.00001"):
            regime = "low"
        elif rv["1m"] >= Decimal("0.0025") or impulse_score >= Decimal("25"):
            regime = "chaotic"
        elif rv["1m"] >= Decimal("0.0007") or impulse_score >= Decimal("5"):
            regime = "high"
        return {
            "realized_volatility_10s": rv["10s"],
            "realized_volatility_30s": rv["30s"],
            "realized_volatility_1m": rv["1m"],
            "realized_volatility_3m": rv["3m"],
            "realized_volatility_5m": rv["5m"],
            "realized_volatility_15m": rv["15m"],
            "atr_range": atr_range,
            "atr_range_ticks": atr_range / tick if tick else Decimal("0"),
            "tick_velocity": tick_velocity,
            "price_velocity": price_velocity,
            "acceleration": acceleration,
            "range_position": range_position,
            "breakout_flag": range_position >= Decimal("0.90") or range_position <= Decimal("0.10"),
            "range_flag": Decimal("0.25") <= range_position <= Decimal("0.75"),
            "impulse_score": impulse_score,
            "chop_score": chop_score,
            "volatility_regime": regime,
        }

    def _money_flow(
        self,
        snapshot: MarketSnapshot,
        micro: dict[str, Any],
        volatility: dict[str, Any],
    ) -> dict[str, Any]:
        volume_proxy = Decimal(str(micro["depth_bid_5"])) + Decimal(str(micro["depth_ask_5"]))
        pressure = Decimal(str(micro["pressure_score"]))
        velocity = Decimal(str(volatility["price_velocity"]))
        if velocity > 0:
            flow_bias = Decimal("1")
        elif velocity < 0:
            flow_bias = Decimal("-1")
        else:
            flow_bias = Decimal("0")
        flow = pressure + flow_bias
        direction = "neutral"
        if flow > Decimal("0.10"):
            direction = "buy"
        elif flow < Decimal("-0.10"):
            direction = "sell"
        mfi = Decimal("50") + max(min(flow * Decimal("25"), Decimal("50")), Decimal("-50"))
        if snapshot.orderbook_missing:
            mfi = Decimal("50")
        return {
            "mfi": mfi,
            "money_flow_direction": direction,
            "volume_proxy": volume_proxy,
        }

    def _curator_decision(
        self,
        snapshot: MarketSnapshot,
        micro: dict[str, Any],
        volatility: dict[str, Any],
        features: FeatureSnapshot | None,
        *,
        state: _InstrumentState,
        peer_contexts: dict[str, dict[str, Any]],
    ) -> tuple[bool, PositionSide | None, Decimal, str, dict[str, Any], EntryCandidate | None]:
        pressure = Decimal(str(micro["pressure_score"]))
        fallback_side = PositionSide.LONG if pressure >= 0 else PositionSide.SHORT
        spread_entry = _spread_entry_assessment(self.config, micro)
        gates = {
            "position_gate": not self._has_open_matrix(snapshot.instrument),
            "session_gate": _session_open(snapshot),
            "spread_gate": spread_entry["entry_ok"],
            "spread_entry": spread_entry,
            "spread_status": spread_entry["status"],
            "spread_no_entry_reason": spread_entry["no_entry_reason"],
            "spread_ticks": spread_entry["spread_ticks"],
            "spread_bps": spread_entry["spread_bps"],
            "orderbook_health_gate": not snapshot.orderbook_missing
            and not bool(micro["thin_book_flag"]),
            "microstructure_min_pressure": self.config.tail_catcher.min_pressure_score,
            "volatility_chaos_gate": volatility["volatility_regime"] != "chaotic",
            "data_freshness_gate": not snapshot.stale,
            "momentum_is_context_only": False,
            "external_btc_eth_is_context_only": True,
            "policy_version": POLICY_VERSION,
        }
        allowed = all(bool(value) for key, value in gates.items() if key.endswith("_gate"))
        if not allowed:
            state.pending_side = None
            state.pending_confirmation_cycles = 0
            blocked = [key for key, value in gates.items() if key.endswith("_gate") and not value]
            return (
                False,
                fallback_side,
                Decimal("0"),
                "no_trade: " + ",".join(blocked),
                gates,
                None,
            )
        candidates = self._entry_engine.score_candidates(
            snapshot=snapshot,
            micro=micro,
            volatility=volatility,
            price_history=tuple(state.prices),
            features=features,
            peer_contexts=peer_contexts,
        )
        control_candidates = [
            candidate
            for candidate in candidates
            if candidate.entry_type in self.config.tail_catcher.control_entry_types
        ]
        if not control_candidates:
            state.pending_side = None
            state.pending_confirmation_cycles = 0
            gates["current_regime"] = "no_trade"
            return (
                False,
                fallback_side,
                Decimal("0"),
                "no_trade: no_entry_type_active",
                gates,
                None,
            )
        entry = max(
            control_candidates,
            key=lambda item: (
                item.direction_score,
                item.expected_mfe_ticks,
                -item.expected_stop_risk_ticks,
            ),
        )
        opposite_score = max(
            (
                candidate.direction_score
                for candidate in candidates
                if candidate.side is not entry.side
            ),
            default=Decimal("0"),
        )
        policy = evaluate_entry_candidate(
            candidate=entry,
            opposite_score=opposite_score,
            pressure_history=tuple(state.pressure_history),
            tick_velocity=Decimal(str(volatility["tick_velocity"])),
            spread_ticks=_dec_or_none(micro.get("spread_ticks")),
            slippage_ticks=self.config.simulation.fallback_slippage_ticks,
            config=self.config.tail_catcher,
        )
        gates["entry_policy"] = {
            "allowed": policy.allowed,
            "reasons": list(policy.reasons),
            "direction_margin": policy.direction_margin,
            "pressure_persistence": policy.pressure_persistence,
            "execution_cost_ticks": policy.execution_cost_ticks,
            "required_mfe_ticks": policy.required_mfe_ticks,
        }
        gates["direction_margin"] = policy.direction_margin
        gates["pressure_persistence"] = policy.pressure_persistence
        gates["execution_cost_ticks"] = policy.execution_cost_ticks
        gates["required_mfe_ticks"] = policy.required_mfe_ticks
        if not policy.allowed:
            state.pending_side = None
            state.pending_confirmation_cycles = 0
            gates["current_regime"] = "no_trade"
            return (
                False,
                entry.side,
                Decimal("0"),
                "no_trade: " + ",".join(policy.reasons),
                gates,
                entry,
            )
        if state.pending_side is entry.side:
            state.pending_confirmation_cycles += 1
        else:
            state.pending_side = entry.side
            state.pending_confirmation_cycles = 1
        gates["confirmation_cycles"] = state.pending_confirmation_cycles
        if state.pending_confirmation_cycles < self.config.tail_catcher.entry_confirmation_cycles:
            gates["current_regime"] = "confirming"
            return (
                False,
                entry.side,
                entry.confidence,
                "no_trade: entry_confirmation_pending",
                gates,
                entry,
            )
        gates["current_regime"] = entry.entry_type
        gates["entry_reason"] = entry.reason
        gates["direction_model"] = entry.direction_model
        gates["direction_score"] = entry.direction_score
        gates["entry_diagnostics"] = entry.diagnostics
        return (
            True,
            entry.side,
            max(entry.confidence, Decimal("0.10")),
            f"entry_type:{entry.entry_type}",
            gates,
            entry,
        )

    def _open_shadow_matrix(
        self,
        *,
        signal_id: str,
        snapshot: MarketSnapshot,
        side: PositionSide,
        timestamp_utc: datetime,
        micro: dict[str, Any],
        volatility: dict[str, Any],
        entry: EntryCandidate,
        spread_entry: dict[str, Any],
        entry_policy: dict[str, Any],
        confirmation_cycles: int,
    ) -> bool:
        entry_price = self._entry_price(snapshot, side)
        if entry_price is None:
            self._record_error(
                timestamp_utc,
                "tail_catcher",
                snapshot.instrument,
                "paper_fill_unavailable",
                "No executable bid/ask or last price for shadow entry.",
                {"side": side.value},
            )
            return False
        tick = snapshot.tick_size or Decimal("1")
        bid = _dec_or_none(micro.get("bid"))
        ask = _dec_or_none(micro.get("ask"))
        trigger_entry_price = (
            snapshot.mid_price
            or snapshot.last_price
            or _dec_or_none(micro.get("microprice"))
            or entry_price
        )
        execution_cost_ticks = Decimal(str(entry_policy["execution_cost_ticks"]))
        direction_margin = Decimal(str(entry_policy["direction_margin"]))
        consecutive_stops = self._recent_consecutive_stops(snapshot.instrument, side)
        reentry_plan = self._reentry_plan(
            snapshot=snapshot,
            side=side,
            entry=entry,
            micro=micro,
            volatility=volatility,
            spread_entry=spread_entry,
            consecutive_stops=consecutive_stops,
        )
        if not reentry_plan["allowed"]:
            self._record_error(
                timestamp_utc,
                "tail_catcher",
                snapshot.instrument,
                "bad_stop_series_cooldown",
                "Re-entry rules blocked the new shadow matrix.",
                {
                    "side": side.value,
                    "entry_type": entry.entry_type,
                    "consecutive_stops": consecutive_stops,
                    "reason": reentry_plan["reason_reentry_blocked"],
                },
            )
            return False
        self._record_reentry_series_signal(
            timestamp_utc=timestamp_utc,
            series_id=str(reentry_plan["reentry_series_id"]),
            instrument=snapshot.instrument,
            side=side,
            entry_type=entry.entry_type,
            reason=str(reentry_plan["reason_reentry_allowed"]),
        )
        effective_consecutive_stops = int(reentry_plan["effective_consecutive_stops"])
        reentry_index = min(
            effective_consecutive_stops,
            self.config.tail_catcher.max_reentries_per_direction_per_instrument,
        )
        opportunity_id = f"OPPORTUNITY_{uuid4().hex}"
        control_stop_ticks = adaptive_control_stop_ticks(
            trigger_entry_price=trigger_entry_price,
            tick=tick,
            atr_range=Decimal(str(volatility["atr_range"])),
            execution_cost_ticks=execution_cost_ticks,
            config=self.config.tail_catcher,
        )
        configurations: list[tuple[int, Decimal, str, bool]] = [
            (
                control_stop_ticks,
                self.config.tail_catcher.default_protection_trigger_bps,
                self.config.tail_catcher.default_trailing_mode,
                True,
            )
        ]
        for stop_ticks in self.config.tail_catcher.stop_ticks:
            configurations.append(
                (
                    stop_ticks,
                    self.config.tail_catcher.default_protection_trigger_bps,
                    self.config.tail_catcher.default_trailing_mode,
                    False,
                )
            )

        control_trade_id = f"SHADOW_TRADE_{uuid4().hex}"
        for index, (stop_ticks, trigger, trailing_mode, is_control) in enumerate(configurations):
            stop_distance = Decimal(stop_ticks) * tick
            stop_price = (
                trigger_entry_price - stop_distance
                if side is PositionSide.LONG
                else trigger_entry_price + stop_distance
            )
            trade_id = control_trade_id if index == 0 else f"SHADOW_TRADE_{uuid4().hex}"
            trade = _ShadowTrade(
                trade_id=trade_id,
                signal_id=signal_id,
                instrument=snapshot.instrument,
                side=side,
                stop_ticks=stop_ticks,
                protection_trigger_bps=trigger,
                trailing_mode=trailing_mode,
                entry_time=timestamp_utc,
                entry_price=entry_price,
                theoretical_bid=bid,
                theoretical_ask=ask,
                stop_price=stop_price,
                best_price_after_entry=trigger_entry_price,
                worst_price_after_entry=trigger_entry_price,
                entry_type=entry.entry_type,
                direction_model=entry.direction_model,
                direction_score=entry.direction_score,
                pressure_score=entry.pressure_score,
                impulse_score=entry.impulse_score,
                pullback_score=entry.pullback_score,
                book_flip_score=entry.book_flip_score,
                reversal_score=entry.reversal_score,
                lead_lag_score=entry.lead_lag_score,
                expected_mfe_ticks=entry.expected_mfe_ticks,
                expected_stop_risk_ticks=entry.expected_stop_risk_ticks,
                reentry_series_id=str(reentry_plan["reentry_series_id"]),
                reentry_number=int(reentry_plan["reentry_number"]),
                reason_reentry_allowed=str(reentry_plan["reason_reentry_allowed"]),
                reason_reentry_blocked=reentry_plan["reason_reentry_blocked"],
                reentry_index=reentry_index,
                consecutive_stop_index=consecutive_stops,
                opportunity_id=opportunity_id,
                is_control=is_control,
                experiment_role="control" if is_control else "research",
                trigger_entry_price=trigger_entry_price,
                execution_cost_ticks=execution_cost_ticks,
                direction_margin=direction_margin,
                confirmation_cycles=confirmation_cycles,
                max_hold_sec=self.config.tail_catcher.control_time_exit_sec,
            )
            self._insert_shadow_trade(trade)
            self._record_mfe_mae(trade, timestamp_utc, trigger_entry_price)
            self._record_trade_event(
                trade.trade_id,
                timestamp_utc,
                "shadow_entry",
                entry_price,
                {
                    "opportunity_id": opportunity_id,
                    "experiment_role": trade.experiment_role,
                    "stop_ticks": stop_ticks,
                    "protection_trigger_bps": str(trigger),
                    "trailing_mode": trailing_mode,
                    "entry_type": entry.entry_type,
                    "direction_score": str(entry.direction_score),
                    "direction_margin": str(direction_margin),
                    "confirmation_cycles": confirmation_cycles,
                    "execution_cost_ticks": str(execution_cost_ticks),
                    "trigger_entry_price": str(trigger_entry_price),
                    "reentry_series_id": trade.reentry_series_id,
                    "reentry_number": trade.reentry_number,
                    "fill_model": "BUY=ask+slippage SELL=bid-slippage",
                },
            )
            self._open_trades[trade.trade_id] = trade
        self._record_opportunity(
            opportunity_id=opportunity_id,
            signal_id=signal_id,
            timestamp_utc=timestamp_utc,
            instrument=snapshot.instrument,
            side=side,
            entry=entry,
            control_trade_id=control_trade_id,
            direction_margin=direction_margin,
            confirmation_cycles=confirmation_cycles,
            pressure_persistence=Decimal(str(entry_policy["pressure_persistence"])),
            execution_cost_ticks=execution_cost_ticks,
            required_mfe_ticks=Decimal(str(entry_policy["required_mfe_ticks"])),
            control_stop_ticks=control_stop_ticks,
        )
        return True

    def _update_open_trades(
        self,
        snapshot: MarketSnapshot,
        micro: dict[str, Any],
        volatility: dict[str, Any],
        timestamp_utc: datetime,
    ) -> None:
        for trade in list(self._open_trades.values()):
            if trade.instrument != snapshot.instrument:
                continue
            exit_price = self._exit_price(snapshot, trade.side)
            if exit_price is None:
                continue
            tick = snapshot.tick_size or Decimal("1")
            trigger_price = (
                snapshot.mid_price
                or snapshot.last_price
                or _dec_or_none(micro.get("microprice"))
                or exit_price
            )
            trigger_entry = trade.trigger_entry_price or trade.entry_price
            trigger_pnl_abs = _pnl_abs(trade.side, trigger_entry, trigger_price)
            trigger_pnl_pct = trigger_pnl_abs / trigger_entry if trigger_entry else Decimal("0")
            trigger_pnl_ticks = trigger_pnl_abs / tick if tick else Decimal("0")
            executable_pnl_abs = _pnl_abs(trade.side, trade.entry_price, exit_price)
            executable_pnl_ticks = executable_pnl_abs / tick if tick else Decimal("0")
            self._update_trade_extremes(
                trade,
                trigger_price,
                trigger_pnl_abs,
                trigger_pnl_pct,
                trigger_pnl_ticks,
                timestamp_utc,
            )
            protection_ticks = protection_threshold_ticks(
                trigger_entry_price=trigger_entry,
                tick=tick,
                trigger_bps=trade.protection_trigger_bps,
                execution_cost_ticks=trade.execution_cost_ticks,
            )
            if not trade.protection_activated and trigger_pnl_ticks >= protection_ticks:
                trade.protection_activated = True
                trade.protection_price = _protection_price(
                    trade.side,
                    trigger_entry,
                    tick * max(trade.execution_cost_ticks, Decimal("1")),
                )
                self._record_trade_event(
                    trade.trade_id,
                    timestamp_utc,
                    "protection_activated",
                    trade.protection_price,
                    {
                        "trigger_bps": str(trade.protection_trigger_bps),
                        "trigger_pnl_ticks": str(trigger_pnl_ticks),
                        "required_ticks": str(protection_ticks),
                    },
                )

            reason: str | None = None
            spread_exit = _spread_exit_assessment(self.config, micro)
            if spread_exit["market_bad"]:
                trade.spread_bad_cycles += 1
            else:
                trade.spread_bad_cycles = 0
                trade.time_exit_bad_spread_cycles = 0
            stop_triggered = _stop_hit(trade, trigger_price)
            if stop_triggered:
                trade.stop_trigger_cycles += 1
            else:
                trade.stop_trigger_cycles = 0
            protection_triggered = (
                trade.protection_activated
                and trade.protection_price is not None
                and _protection_hit(trade, trigger_price)
            )
            if protection_triggered:
                trade.protection_trigger_cycles += 1
            else:
                trade.protection_trigger_cycles = 0
            time_due = _time_exit(trade, timestamp_utc, trade.max_hold_sec)
            trailing_reason = None
            if trade.protection_activated and trade.mfe_ticks > 0:
                trailing_reason = self._trailing_exit_reason(
                    trade,
                    trigger_price,
                    micro,
                    volatility,
                    tick,
                )
            panic_threshold = -(
                Decimal(trade.stop_ticks) * self.config.tail_catcher.panic_stop_multiple
            )
            normal_exit_reason: str | None = None
            if (
                stop_triggered
                and trade.stop_trigger_cycles >= self.config.tail_catcher.stop_confirmation_cycles
            ):
                normal_exit_reason = "protected_stop" if trade.protection_activated else "stop_loss"
            elif trailing_reason is not None:
                trade.trailing_reason = trailing_reason
                normal_exit_reason = "trailing_runner"
            elif (
                protection_triggered
                and trade.protection_trigger_cycles
                >= self.config.tail_catcher.protection_confirmation_cycles
            ):
                trade.protected_exit_reason = "protection_floor"
                normal_exit_reason = "protected_exit"
            elif time_due:
                normal_exit_reason = "time_exit"

            if snapshot.stale:
                reason = "stale_data_close"
            elif spread_exit["market_bad"] and spread_exit["exit_infeasible"]:
                grace_met = trade.spread_bad_cycles >= SPREAD_SHOCK_GRACE_CYCLES
                if grace_met and trigger_pnl_ticks <= panic_threshold:
                    reason = "spread_shock"
                    self._record_trade_event(
                        trade.trade_id,
                        timestamp_utc,
                        "real_spread_shock_exit_confirmed",
                        exit_price,
                        {
                            **spread_exit,
                            "spread_bad_cycles": trade.spread_bad_cycles,
                            "grace_cycles_required": SPREAD_SHOCK_GRACE_CYCLES,
                            "trigger_pnl_ticks": str(trigger_pnl_ticks),
                            "panic_threshold_ticks": str(panic_threshold),
                        },
                    )
                else:
                    pending_reasons = []
                    if normal_exit_reason is not None:
                        pending_reasons.append(normal_exit_reason)
                    self._record_trade_event(
                        trade.trade_id,
                        timestamp_utc,
                        "avoided_spread_shock_exit",
                        exit_price,
                        {
                            **spread_exit,
                            "spread_bad_cycles": trade.spread_bad_cycles,
                            "grace_cycles_required": SPREAD_SHOCK_GRACE_CYCLES,
                            "kept_open": True,
                            "pending_reasons": pending_reasons,
                            "trigger_pnl_ticks": str(trigger_pnl_ticks),
                        },
                    )
                    if time_due:
                        trade.time_exit_bad_spread_cycles += 1
                        if (
                            trade.time_exit_bad_spread_cycles
                            >= self.config.tail_catcher.time_exit_spread_grace_cycles
                        ):
                            reason = "time_exit_spread_timeout"
            else:
                reason = normal_exit_reason

            if (
                spread_exit["market_bad"]
                and not spread_exit["exit_infeasible"]
                and reason != "spread_shock"
            ):
                self._record_trade_event(
                    trade.trade_id,
                    timestamp_utc,
                    "avoided_spread_shock_exit",
                    exit_price,
                    {
                        **spread_exit,
                        "spread_bad_cycles": trade.spread_bad_cycles,
                        "kept_open": reason is None,
                        "normal_exit_reason": reason,
                        "trigger_pnl_ticks": str(trigger_pnl_ticks),
                    },
                )

            if spread_exit["market_bad"] and reason is not None and reason != "spread_shock":
                self._record_trade_event(
                    trade.trade_id,
                    timestamp_utc,
                    "market_bad_spread_warning",
                    exit_price,
                    {**spread_exit, "exit_reason": reason},
                )

            if reason is None:
                self._update_shadow_trade(trade, status="OPEN")
                self._record_mfe_mae(trade, timestamp_utc, trigger_price)
                continue

            trade.exit_trigger_price = trigger_price
            trade.execution_shortfall_ticks = trigger_pnl_ticks - executable_pnl_ticks
            self._close_trade(trade, timestamp_utc, exit_price, reason)

    def _update_trade_extremes(
        self,
        trade: _ShadowTrade,
        exit_price: Decimal,
        pnl_abs: Decimal,
        pnl_pct: Decimal,
        pnl_ticks: Decimal,
        timestamp_utc: datetime,
    ) -> None:
        if trade.side is PositionSide.LONG:
            trade.best_price_after_entry = max(trade.best_price_after_entry, exit_price)
            trade.worst_price_after_entry = min(trade.worst_price_after_entry, exit_price)
        else:
            trade.best_price_after_entry = min(trade.best_price_after_entry, exit_price)
            trade.worst_price_after_entry = max(trade.worst_price_after_entry, exit_price)
        if pnl_abs > trade.mfe_abs:
            trade.mfe_abs = pnl_abs
            trade.mfe_pct = pnl_pct
            trade.mfe_ticks = pnl_ticks
            trade.time_to_mfe_sec = Decimal(str((timestamp_utc - trade.entry_time).total_seconds()))
        if pnl_abs < trade.mae_abs:
            trade.mae_abs = pnl_abs
            trade.mae_pct = pnl_pct
            trade.mae_ticks = pnl_ticks
            trade.time_to_mae_sec = Decimal(str((timestamp_utc - trade.entry_time).total_seconds()))
        trade.max_runup_before_exit = max(trade.max_runup_before_exit, trade.mfe_abs)
        trade.max_drawdown_before_exit = min(trade.max_drawdown_before_exit, trade.mae_abs)

    def _trailing_exit_reason(
        self,
        trade: _ShadowTrade,
        trigger_price: Decimal,
        micro: dict[str, Any],
        volatility: dict[str, Any],
        tick: Decimal,
    ) -> str | None:
        if trade.trailing_mode == "expectancy_adaptive":
            trail_ticks = max(
                trade.execution_cost_ticks * Decimal("1.25"),
                min(
                    Decimal(trade.stop_ticks) * Decimal("0.50"),
                    max(Decimal("4"), trade.mfe_ticks * Decimal("0.30")),
                ),
            )
        else:
            trail_ticks = {
                "tight": Decimal("5"),
                "normal": Decimal("10"),
                "loose": Decimal("20"),
            }.get(trade.trailing_mode, Decimal(trade.stop_ticks))
        if trade.side is PositionSide.LONG:
            pullback = (trade.best_price_after_entry - trigger_price) / tick
            pressure_lost = Decimal(str(micro["pressure_score"])) < Decimal("0")
        else:
            pullback = (trigger_price - trade.best_price_after_entry) / tick
            pressure_lost = Decimal(str(micro["pressure_score"])) > Decimal("0")
        volatility_compressed = Decimal(str(volatility["chop_score"])) >= Decimal("0.85")
        book_flip_against = bool(micro["orderbook_flip_flag"]) and pressure_lost
        reason = None
        if pullback >= trail_ticks:
            reason = "MFE_pullback"
        elif book_flip_against:
            reason = "book_flip_against_runner"
        elif bool(micro["thin_book_flag"]):
            reason = "microstructure_deterioration"
        elif pressure_lost:
            reason = "pressure_loss"
        elif volatility_compressed:
            reason = "volatility_compression"
        if reason is None:
            trade.trailing_bad_cycles = 0
            return None
        if reason == "MFE_pullback":
            trade.trailing_bad_cycles = 0
            return reason
        trade.trailing_bad_cycles += 1
        if trade.trailing_bad_cycles < self.config.tail_catcher.trailing_confirmation_cycles:
            return None
        return reason

    def _entry_price(self, snapshot: MarketSnapshot, side: PositionSide) -> Decimal | None:
        tick = snapshot.tick_size or Decimal("1")
        slip = self.config.simulation.fallback_slippage_ticks * tick
        if side is PositionSide.LONG:
            if snapshot.best_ask is not None:
                return snapshot.best_ask + slip
            if snapshot.last_price is not None:
                return snapshot.last_price + slip
        else:
            if snapshot.best_bid is not None:
                return snapshot.best_bid - slip
            if snapshot.last_price is not None:
                return snapshot.last_price - slip
        return None

    def _exit_price(self, snapshot: MarketSnapshot, side: PositionSide) -> Decimal | None:
        tick = snapshot.tick_size or Decimal("1")
        slip = self.config.simulation.fallback_slippage_ticks * tick
        if side is PositionSide.LONG:
            if snapshot.best_bid is not None:
                return snapshot.best_bid - slip
            if snapshot.last_price is not None:
                return snapshot.last_price - slip
        else:
            if snapshot.best_ask is not None:
                return snapshot.best_ask + slip
            if snapshot.last_price is not None:
                return snapshot.last_price + slip
        return None

    def _has_open_matrix(self, instrument: str) -> bool:
        return any(
            trade.instrument == instrument and trade.is_control
            for trade in self._open_trades.values()
        )

    def _reentry_plan(
        self,
        *,
        snapshot: MarketSnapshot,
        side: PositionSide,
        entry: EntryCandidate,
        micro: dict[str, Any],
        volatility: dict[str, Any],
        spread_entry: dict[str, Any],
        consecutive_stops: int,
    ) -> dict[str, Any]:
        blocked: list[str] = []
        effective_consecutive_stops = consecutive_stops
        reset_after_cooldown = False
        signed_pressure = _side_signed(side, Decimal(str(micro["pressure_score"])))
        signed_micro = _side_signed(side, Decimal(str(micro["microprice_deviation"])))
        if consecutive_stops >= self.config.tail_catcher.max_consecutive_stops:
            latest_exit = self._latest_control_exit_time(snapshot.instrument, side)
            age_sec = (
                None
                if latest_exit is None
                else (snapshot.timestamp_utc - latest_exit).total_seconds()
            )
            if age_sec is None or age_sec < self.config.tail_catcher.cooldown_after_bad_series_sec:
                blocked.append("bad_series_cooldown")
            else:
                effective_consecutive_stops = 0
                reset_after_cooldown = True
        if (
            not blocked
            and effective_consecutive_stops
            > self.config.tail_catcher.max_reentries_per_direction_per_instrument
        ):
            blocked.append("max_reentries")
        if effective_consecutive_stops > 0:
            if entry.direction_score < self._entry_engine.min_direction_score:
                blocked.append("direction_score_deteriorated")
            if signed_micro < Decimal("-0.25"):
                blocked.append("microprice_flipped_against")
            if bool(micro["orderbook_flip_flag"]) and signed_pressure < Decimal("0.10"):
                blocked.append("orderbook_flip_against")
            if not bool(spread_entry["entry_ok"]):
                blocked.append("spread_not_normal")
            if volatility["volatility_regime"] == "chaotic":
                blocked.append("volatility_chaotic")
        series_id = (
            self._latest_reentry_series_id(snapshot.instrument, side, entry.entry_type)
            if effective_consecutive_stops > 0
            else None
        )
        if series_id is None:
            series_id = f"REENTRY_SERIES_{uuid4().hex}"
        if blocked:
            return {
                "allowed": False,
                "reentry_series_id": series_id,
                "reentry_number": effective_consecutive_stops,
                "effective_consecutive_stops": effective_consecutive_stops,
                "reason_reentry_allowed": None,
                "reason_reentry_blocked": ",".join(blocked),
            }
        reason = (
            "initial_entry_after_bad_series_cooldown"
            if reset_after_cooldown
            else "initial_entry"
            if effective_consecutive_stops == 0
            else "reentry_allowed: direction_score/book/spread still aligned"
        )
        return {
            "allowed": True,
            "reentry_series_id": series_id,
            "reentry_number": effective_consecutive_stops,
            "effective_consecutive_stops": effective_consecutive_stops,
            "reason_reentry_allowed": reason,
            "reason_reentry_blocked": None,
        }

    def _latest_reentry_series_id(
        self,
        instrument: str,
        side: PositionSide,
        entry_type: str,
    ) -> str | None:
        rows = self.storage.fetch_all(
            """
            SELECT reentry_series_id
            FROM shadow_trades
            WHERE instrument = ?
              AND side = ?
              AND entry_type = ?
              AND is_control = 1
              AND reentry_series_id IS NOT NULL
            ORDER BY COALESCE(exit_time, entry_time) DESC
            LIMIT 1
            """,
            (instrument, side.value, entry_type),
        )
        if not rows:
            return None
        return None if rows[0]["reentry_series_id"] is None else str(rows[0]["reentry_series_id"])

    def _latest_control_exit_time(
        self,
        instrument: str,
        side: PositionSide,
    ) -> datetime | None:
        rows = self.storage.fetch_all(
            """
            SELECT exit_time
            FROM shadow_trades
            WHERE instrument = ?
              AND side = ?
              AND status = 'CLOSED'
              AND is_control = 1
              AND exit_time IS NOT NULL
            ORDER BY exit_time DESC
            LIMIT 1
            """,
            (instrument, side.value),
        )
        return None if not rows else _dt(rows[0]["exit_time"])

    def _record_reentry_series_signal(
        self,
        *,
        timestamp_utc: datetime,
        series_id: str,
        instrument: str,
        side: PositionSide,
        entry_type: str,
        reason: str,
    ) -> None:
        with self.storage.connect() as conn:
            conn.execute(
                """
                INSERT INTO reentry_series(
                    series_id, instrument, side, entry_type, started_at, last_signal_at,
                    entries_count, stops_count, status, reason_json
                )
                VALUES (?, ?, ?, ?, ?, ?, 1, 0, 'OPEN', ?)
                ON CONFLICT(series_id) DO UPDATE SET
                    last_signal_at = excluded.last_signal_at,
                    entries_count = entries_count + 1,
                    status = 'OPEN',
                    reason_json = excluded.reason_json
                """,
                (
                    series_id,
                    instrument,
                    side.value,
                    entry_type,
                    timestamp_utc.isoformat(),
                    timestamp_utc.isoformat(),
                    _json({"reason": reason}),
                ),
            )
            conn.commit()

    def _update_reentry_series_on_close(self, trade: _ShadowTrade, reason: str) -> None:
        if trade.reentry_series_id is None or not trade.is_control:
            return
        trigger_entry = trade.trigger_entry_price or trade.entry_price
        exit_trigger = trade.exit_trigger_price or trigger_entry
        tick = abs(trigger_entry - trade.stop_price) / Decimal(trade.stop_ticks)
        trigger_pnl_ticks = _pnl_abs(trade.side, trigger_entry, exit_trigger) / tick
        loss = (trigger_pnl_ticks - trade.execution_shortfall_ticks) < 0
        status = "STOPPED" if loss else "CLOSED"
        with self.storage.connect() as conn:
            conn.execute(
                """
                UPDATE reentry_series
                SET stops_count = stops_count + ?,
                    status = ?
                WHERE series_id = ?
                """,
                (1 if loss else 0, status, trade.reentry_series_id),
            )
            conn.commit()

    def _record_opportunity(
        self,
        *,
        opportunity_id: str,
        signal_id: str,
        timestamp_utc: datetime,
        instrument: str,
        side: PositionSide,
        entry: EntryCandidate,
        control_trade_id: str,
        direction_margin: Decimal,
        confirmation_cycles: int,
        pressure_persistence: Decimal,
        execution_cost_ticks: Decimal,
        required_mfe_ticks: Decimal,
        control_stop_ticks: int,
    ) -> None:
        with self.storage.connect() as conn:
            conn.execute(
                """
                INSERT INTO market_opportunities(
                    opportunity_id, signal_id, timestamp_utc, instrument, side, entry_type,
                    status, control_trade_id, policy_version, direction_score,
                    direction_margin, confirmation_cycles, pressure_persistence,
                    execution_cost_ticks, required_mfe_ticks, details_json
                )
                VALUES (?, ?, ?, ?, ?, ?, 'OPEN', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    opportunity_id,
                    signal_id,
                    timestamp_utc.isoformat(),
                    instrument,
                    side.value,
                    entry.entry_type,
                    control_trade_id,
                    POLICY_VERSION,
                    _num(entry.direction_score),
                    _num(direction_margin),
                    confirmation_cycles,
                    _num(pressure_persistence),
                    _num(execution_cost_ticks),
                    _num(required_mfe_ticks),
                    _json({"control_stop_ticks": control_stop_ticks}),
                ),
            )
            conn.execute(
                "UPDATE shadow_signals SET opportunity_id = ? WHERE signal_id = ?",
                (opportunity_id, signal_id),
            )
            conn.commit()

    def _close_opportunity(
        self,
        trade: _ShadowTrade,
        timestamp_utc: datetime,
        exit_price: Decimal,
        reason: str,
    ) -> None:
        if trade.opportunity_id is None:
            return
        trigger_entry = trade.trigger_entry_price or trade.entry_price
        tick = abs(trigger_entry - trade.stop_price) / Decimal(trade.stop_ticks)
        pnl_ticks = _pnl_abs(trade.side, trade.entry_price, exit_price) / tick
        with self.storage.connect() as conn:
            conn.execute(
                """
                UPDATE market_opportunities
                SET status = 'CLOSED', exit_time = ?, exit_reason = ?, pnl_ticks = ?
                WHERE opportunity_id = ?
                """,
                (
                    timestamp_utc.isoformat(),
                    reason,
                    _num(pnl_ticks),
                    trade.opportunity_id,
                ),
            )
            conn.commit()

    def _insert_shadow_trade(self, trade: _ShadowTrade) -> None:
        with self.storage.connect() as conn:
            conn.execute(
                """
                INSERT INTO shadow_trades(
                    trade_id, signal_id, instrument, side, stop_ticks, protection_trigger_bps,
                    trailing_mode, status, entry_time, entry_price, theoretical_bid,
                    theoretical_ask, stop_price, best_price_after_entry, worst_price_after_entry,
                    entry_type, direction_model, direction_score, pressure_score, impulse_score,
                    pullback_score, book_flip_score, reversal_score, lead_lag_score,
                    expected_mfe_ticks, expected_stop_risk_ticks, reentry_series_id,
                    reentry_number, reason_reentry_allowed, reason_reentry_blocked,
                    reentry_index, consecutive_stop_index, opportunity_id, is_control,
                    experiment_role, policy_version, trigger_entry_price, execution_cost_ticks,
                    direction_margin, confirmation_cycles, max_hold_sec
                )
                VALUES (
                    :trade_id, :signal_id, :instrument, :side, :stop_ticks,
                    :protection_trigger_bps, :trailing_mode, 'OPEN', :entry_time,
                    :entry_price, :theoretical_bid, :theoretical_ask, :stop_price,
                    :best_price_after_entry, :worst_price_after_entry, :entry_type,
                    :direction_model, :direction_score, :pressure_score, :impulse_score,
                    :pullback_score, :book_flip_score, :reversal_score, :lead_lag_score,
                    :expected_mfe_ticks, :expected_stop_risk_ticks, :reentry_series_id,
                    :reentry_number, :reason_reentry_allowed, :reason_reentry_blocked,
                    :reentry_index, :consecutive_stop_index, :opportunity_id, :is_control,
                    :experiment_role, :policy_version, :trigger_entry_price,
                    :execution_cost_ticks, :direction_margin, :confirmation_cycles,
                    :max_hold_sec
                )
                """,
                {
                    "trade_id": trade.trade_id,
                    "signal_id": trade.signal_id,
                    "instrument": trade.instrument,
                    "side": trade.side.value,
                    "stop_ticks": trade.stop_ticks,
                    "protection_trigger_bps": _num(trade.protection_trigger_bps),
                    "trailing_mode": trade.trailing_mode,
                    "entry_time": trade.entry_time.isoformat(),
                    "entry_price": _num(trade.entry_price),
                    "theoretical_bid": _num(trade.theoretical_bid),
                    "theoretical_ask": _num(trade.theoretical_ask),
                    "stop_price": _num(trade.stop_price),
                    "best_price_after_entry": _num(trade.best_price_after_entry),
                    "worst_price_after_entry": _num(trade.worst_price_after_entry),
                    "entry_type": trade.entry_type,
                    "direction_model": trade.direction_model,
                    "direction_score": _num(trade.direction_score),
                    "pressure_score": _num(trade.pressure_score),
                    "impulse_score": _num(trade.impulse_score),
                    "pullback_score": _num(trade.pullback_score),
                    "book_flip_score": _num(trade.book_flip_score),
                    "reversal_score": _num(trade.reversal_score),
                    "lead_lag_score": _num(trade.lead_lag_score),
                    "expected_mfe_ticks": _num(trade.expected_mfe_ticks),
                    "expected_stop_risk_ticks": _num(trade.expected_stop_risk_ticks),
                    "reentry_series_id": trade.reentry_series_id,
                    "reentry_number": trade.reentry_number,
                    "reason_reentry_allowed": trade.reason_reentry_allowed,
                    "reason_reentry_blocked": trade.reason_reentry_blocked,
                    "reentry_index": trade.reentry_index,
                    "consecutive_stop_index": trade.consecutive_stop_index,
                    "opportunity_id": trade.opportunity_id,
                    "is_control": int(trade.is_control),
                    "experiment_role": trade.experiment_role,
                    "policy_version": trade.policy_version,
                    "trigger_entry_price": _num(trade.trigger_entry_price),
                    "execution_cost_ticks": _num(trade.execution_cost_ticks),
                    "direction_margin": _num(trade.direction_margin),
                    "confirmation_cycles": trade.confirmation_cycles,
                    "max_hold_sec": trade.max_hold_sec,
                },
            )
            conn.commit()

    def _update_shadow_trade(self, trade: _ShadowTrade, *, status: str) -> None:
        with self.storage.connect() as conn:
            conn.execute(
                """
                UPDATE shadow_trades
                SET status = ?,
                    best_price_after_entry = ?,
                    worst_price_after_entry = ?,
                    mfe_abs = ?,
                    mfe_pct = ?,
                    mfe_ticks = ?,
                    mae_abs = ?,
                    mae_pct = ?,
                    mae_ticks = ?,
                    time_to_mfe_sec = ?,
                    time_to_mae_sec = ?,
                    max_runup_before_exit = ?,
                    max_drawdown_before_exit = ?,
                    protection_activated = ?,
                    protection_price = ?,
                    protected_exit_reason = ?,
                    trailing_reason = ?,
                    spread_bad_cycles = ?,
                    stop_trigger_cycles = ?,
                    protection_trigger_cycles = ?,
                    trailing_bad_cycles = ?,
                    time_exit_bad_spread_cycles = ?,
                    exit_trigger_price = ?,
                    execution_shortfall_ticks = ?
                WHERE trade_id = ?
                """,
                (
                    status,
                    _num(trade.best_price_after_entry),
                    _num(trade.worst_price_after_entry),
                    _num(trade.mfe_abs),
                    _num(trade.mfe_pct),
                    _num(trade.mfe_ticks),
                    _num(trade.mae_abs),
                    _num(trade.mae_pct),
                    _num(trade.mae_ticks),
                    _num(trade.time_to_mfe_sec),
                    _num(trade.time_to_mae_sec),
                    _num(trade.max_runup_before_exit),
                    _num(trade.max_drawdown_before_exit),
                    int(trade.protection_activated),
                    _num(trade.protection_price),
                    trade.protected_exit_reason,
                    trade.trailing_reason,
                    trade.spread_bad_cycles,
                    trade.stop_trigger_cycles,
                    trade.protection_trigger_cycles,
                    trade.trailing_bad_cycles,
                    trade.time_exit_bad_spread_cycles,
                    _num(trade.exit_trigger_price),
                    _num(trade.execution_shortfall_ticks),
                    trade.trade_id,
                ),
            )
            conn.commit()

    def _close_trade(
        self,
        trade: _ShadowTrade,
        timestamp_utc: datetime,
        exit_price: Decimal,
        reason: str,
    ) -> None:
        self._record_mfe_mae(
            trade,
            timestamp_utc,
            trade.exit_trigger_price or exit_price,
        )
        self._update_shadow_trade(trade, status="CLOSED")
        with self.storage.connect() as conn:
            conn.execute(
                """
                UPDATE shadow_trades
                SET status = 'CLOSED',
                    exit_time = ?,
                    exit_price = ?,
                    exit_reason = ?,
                    protected_exit_reason = ?,
                    trailing_reason = ?,
                    exit_trigger_price = ?,
                    execution_shortfall_ticks = ?
                WHERE trade_id = ?
                """,
                (
                    timestamp_utc.isoformat(),
                    _num(exit_price),
                    reason,
                    trade.protected_exit_reason,
                    trade.trailing_reason,
                    _num(trade.exit_trigger_price),
                    _num(trade.execution_shortfall_ticks),
                    trade.trade_id,
                ),
            )
            conn.commit()
        self._update_reentry_series_on_close(trade, reason)
        self._record_trade_event(
            trade.trade_id,
            timestamp_utc,
            reason,
            exit_price,
            {
                "mfe_ticks": str(trade.mfe_ticks),
                "mae_ticks": str(trade.mae_ticks),
                "protection_activated": trade.protection_activated,
                "entry_type": trade.entry_type,
                "trailing_reason": trade.trailing_reason,
                "opportunity_id": trade.opportunity_id,
                "experiment_role": trade.experiment_role,
                "exit_trigger_price": str(trade.exit_trigger_price),
                "execution_shortfall_ticks": str(trade.execution_shortfall_ticks),
            },
        )
        if trade.is_control:
            self._close_opportunity(trade, timestamp_utc, exit_price, reason)
        self._open_trades.pop(trade.trade_id, None)

    def _record_signal(
        self,
        *,
        timestamp_utc: datetime,
        snapshot: MarketSnapshot,
        side: PositionSide | None,
        confidence: Decimal,
        reason: str,
        gates: dict[str, Any],
        micro: dict[str, Any],
        volatility: dict[str, Any],
        money_flow: dict[str, Any],
        entry: EntryCandidate | None,
    ) -> str:
        signal_id = f"SHADOW_SIGNAL_{uuid4().hex}"
        features = {
            "microstructure": _jsonable(micro),
            "volatility": _jsonable(volatility),
            "money_flow": _jsonable(money_flow),
            "entry": None if entry is None else _jsonable(entry.diagnostics),
        }
        with self.storage.connect() as conn:
            conn.execute(
                """
                INSERT INTO shadow_signals(
                    signal_id, timestamp_utc, instrument, side, confidence, reason,
                    entry_type, direction_model, direction_score, pressure_score,
                    impulse_score, pullback_score, book_flip_score, reversal_score,
                    lead_lag_score, expected_mfe_ticks, expected_stop_risk_ticks,
                    policy_version, direction_margin, confirmation_cycles,
                    pressure_persistence, execution_cost_ticks, required_mfe_ticks,
                    gate_status_json, features_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_id,
                    timestamp_utc.isoformat(),
                    snapshot.instrument,
                    "NONE" if side is None else side.value,
                    _num(confidence),
                    reason,
                    None if entry is None else entry.entry_type,
                    None if entry is None else entry.direction_model,
                    _num(Decimal("0") if entry is None else entry.direction_score),
                    _num(Decimal("0") if entry is None else entry.pressure_score),
                    _num(Decimal("0") if entry is None else entry.impulse_score),
                    _num(Decimal("0") if entry is None else entry.pullback_score),
                    _num(Decimal("0") if entry is None else entry.book_flip_score),
                    _num(Decimal("0") if entry is None else entry.reversal_score),
                    _num(Decimal("0") if entry is None else entry.lead_lag_score),
                    _num(Decimal("0") if entry is None else entry.expected_mfe_ticks),
                    _num(Decimal("0") if entry is None else entry.expected_stop_risk_ticks),
                    POLICY_VERSION,
                    _num(Decimal(str(gates.get("direction_margin", 0)))),
                    int(gates.get("confirmation_cycles", 0)),
                    _num(Decimal(str(gates.get("pressure_persistence", 0)))),
                    _num(Decimal(str(gates.get("execution_cost_ticks", 0)))),
                    _num(Decimal(str(gates.get("required_mfe_ticks", 0)))),
                    _json(gates),
                    _json(features),
                ),
            )
            conn.commit()
        return signal_id

    def _record_live_entry_score(
        self,
        timestamp_utc: datetime,
        snapshot: MarketSnapshot,
        entry: EntryCandidate,
    ) -> None:
        with self.storage.connect() as conn:
            conn.execute(
                """
                INSERT INTO entry_strategy_scores(
                    run_id, timestamp_utc, instrument, entry_type, side, direction_model,
                    direction_score, pressure_score, impulse_score, pullback_score,
                    book_flip_score, reversal_score, lead_lag_score, expected_mfe_ticks,
                    expected_stop_risk_ticks, diagnostics_json, created_at
                )
                VALUES ('LIVE', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    timestamp_utc.isoformat(),
                    snapshot.instrument,
                    entry.entry_type,
                    entry.side.value,
                    entry.direction_model,
                    _num(entry.direction_score),
                    _num(entry.pressure_score),
                    _num(entry.impulse_score),
                    _num(entry.pullback_score),
                    _num(entry.book_flip_score),
                    _num(entry.reversal_score),
                    _num(entry.lead_lag_score),
                    _num(entry.expected_mfe_ticks),
                    _num(entry.expected_stop_risk_ticks),
                    _json(entry.diagnostics),
                    timestamp_utc.isoformat(),
                ),
            )
            conn.commit()

    def _record_curator(
        self,
        *,
        timestamp_utc: datetime,
        instrument: str,
        allowed: bool,
        side: PositionSide | None,
        confidence: Decimal,
        reason: str,
        gates: dict[str, Any],
    ) -> None:
        self.storage.record_curator_decision(
            decision_id=f"CURATOR_DECISION_{uuid4().hex}",
            timestamp_utc=timestamp_utc,
            bot_id="tail_catcher_curator",
            account_ref="shadow",
            instrument=instrument,
            action="allow_trade" if allowed else "reject_trade",
            old_params=None,
            new_params={
                "side": None if side is None else side.value,
                "confidence": str(confidence),
                "stop_ticks": list(self.config.tail_catcher.stop_ticks),
            },
            reason=reason,
            metrics_snapshot=gates,
        )

    def _record_market_detail(self, snapshot: MarketSnapshot, micro: dict[str, Any]) -> None:
        with self.storage.connect() as conn:
            conn.execute(
                """
                INSERT INTO raw_orderbook_snapshots(
                    timestamp_utc, instrument, best_bid, best_ask, spread_bps,
                    depth_bid_3, depth_ask_3, bids_json, asks_json, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.timestamp_utc.isoformat(),
                    snapshot.instrument,
                    _num(snapshot.best_bid),
                    _num(snapshot.best_ask),
                    _num(micro["spread_bps"]),
                    _num(micro["depth_bid_3"]),
                    _num(micro["depth_ask_3"]),
                    _levels_json(snapshot.bid_levels),
                    _levels_json(snapshot.ask_levels),
                    _json(snapshot.raw),
                ),
            )
            conn.execute(
                """
                INSERT INTO raw_quotes(timestamp_utc, instrument, bid, ask, mid, spread_abs,
                                       spread_bps, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.timestamp_utc.isoformat(),
                    snapshot.instrument,
                    _num(snapshot.best_bid),
                    _num(snapshot.best_ask),
                    _num(snapshot.mid_price),
                    _num(snapshot.spread_abs),
                    _num(micro["spread_bps"]),
                    _json({"source": "orderbook_top"}),
                ),
            )
            conn.commit()

    def _record_feature_tables(
        self,
        snapshot: MarketSnapshot,
        micro: dict[str, Any],
        volatility: dict[str, Any],
        money_flow: dict[str, Any],
    ) -> None:
        with self.storage.connect() as conn:
            conn.execute(
                """
                INSERT INTO microstructure_features(
                    timestamp_utc, instrument, bid, ask, mid, spread_abs, spread_ticks,
                    spread_bps, top_bid_qty, top_ask_qty, depth_bid_3, depth_bid_5,
                    depth_bid_10, depth_ask_3, depth_ask_5, depth_ask_10,
                    orderbook_imbalance_3, orderbook_imbalance_5, orderbook_imbalance_10,
                    microprice, microprice_deviation, pressure_score, liquidity_score,
                    book_slope, wall_detect_bid, wall_detect_ask, spread_expansion_flag,
                    spread_compression_flag, quote_stability, orderbook_flip_flag,
                    stale_orderbook_flag, thin_book_flag, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.timestamp_utc.isoformat(),
                    snapshot.instrument,
                    _num(micro["bid"]),
                    _num(micro["ask"]),
                    _num(micro["mid"]),
                    _num(micro["spread_abs"]),
                    _num(micro["spread_ticks"]),
                    _num(micro["spread_bps"]),
                    _num(micro["top_bid_qty"]),
                    _num(micro["top_ask_qty"]),
                    _num(micro["depth_bid_3"]),
                    _num(micro["depth_bid_5"]),
                    _num(micro["depth_bid_10"]),
                    _num(micro["depth_ask_3"]),
                    _num(micro["depth_ask_5"]),
                    _num(micro["depth_ask_10"]),
                    _num(micro["orderbook_imbalance_3"]),
                    _num(micro["orderbook_imbalance_5"]),
                    _num(micro["orderbook_imbalance_10"]),
                    _num(micro["microprice"]),
                    _num(micro["microprice_deviation"]),
                    _num(micro["pressure_score"]),
                    _num(micro["liquidity_score"]),
                    _num(micro["book_slope"]),
                    int(bool(micro["wall_detect_bid"])),
                    int(bool(micro["wall_detect_ask"])),
                    int(bool(micro["spread_expansion_flag"])),
                    int(bool(micro["spread_compression_flag"])),
                    _num(micro["quote_stability"]),
                    int(bool(micro["orderbook_flip_flag"])),
                    int(bool(micro["stale_orderbook_flag"])),
                    int(bool(micro["thin_book_flag"])),
                    _json(_jsonable(micro)),
                ),
            )
            conn.execute(
                """
                INSERT INTO volatility_features(
                    timestamp_utc, instrument, realized_volatility_10s,
                    realized_volatility_30s, realized_volatility_1m,
                    realized_volatility_3m, realized_volatility_5m,
                    realized_volatility_15m, atr_range, tick_velocity, price_velocity,
                    acceleration, range_position, breakout_flag, range_flag, impulse_score,
                    chop_score, volatility_regime, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.timestamp_utc.isoformat(),
                    snapshot.instrument,
                    _num(volatility["realized_volatility_10s"]),
                    _num(volatility["realized_volatility_30s"]),
                    _num(volatility["realized_volatility_1m"]),
                    _num(volatility["realized_volatility_3m"]),
                    _num(volatility["realized_volatility_5m"]),
                    _num(volatility["realized_volatility_15m"]),
                    _num(volatility["atr_range"]),
                    _num(volatility["tick_velocity"]),
                    _num(volatility["price_velocity"]),
                    _num(volatility["acceleration"]),
                    _num(volatility["range_position"]),
                    int(bool(volatility["breakout_flag"])),
                    int(bool(volatility["range_flag"])),
                    _num(volatility["impulse_score"]),
                    _num(volatility["chop_score"]),
                    str(volatility["volatility_regime"]),
                    _json(_jsonable(volatility)),
                ),
            )
            conn.execute(
                """
                INSERT INTO money_flow_features(
                    timestamp_utc, instrument, mfi, money_flow_direction, volume_proxy, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.timestamp_utc.isoformat(),
                    snapshot.instrument,
                    _num(money_flow["mfi"]),
                    str(money_flow["money_flow_direction"]),
                    _num(money_flow["volume_proxy"]),
                    _json(_jsonable(money_flow)),
                ),
            )
            conn.commit()

    def _upsert_instrument(self, snapshot: MarketSnapshot) -> None:
        metadata = snapshot.metadata
        with self.storage.connect() as conn:
            conn.execute(
                """
                INSERT INTO instruments(
                    name, display_name, ticker, figi, class_code, uid, lot, tick_size,
                    trading_status, currency, exchange_code, instrument_type, raw_json, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    display_name = excluded.display_name,
                    ticker = excluded.ticker,
                    figi = excluded.figi,
                    class_code = excluded.class_code,
                    uid = excluded.uid,
                    lot = excluded.lot,
                    tick_size = excluded.tick_size,
                    trading_status = excluded.trading_status,
                    currency = excluded.currency,
                    exchange_code = excluded.exchange_code,
                    instrument_type = excluded.instrument_type,
                    raw_json = excluded.raw_json,
                    updated_at = excluded.updated_at
                """,
                (
                    metadata.name,
                    metadata.display_name,
                    metadata.ticker,
                    metadata.figi,
                    metadata.class_code,
                    metadata.uid,
                    metadata.lot,
                    _num(metadata.min_price_increment),
                    metadata.trading_status,
                    metadata.currency,
                    metadata.exchange,
                    metadata.instrument_type,
                    _json(snapshot.raw),
                    snapshot.timestamp_utc.isoformat(),
                ),
            )
            conn.commit()

    def _record_mfe_mae(
        self,
        trade: _ShadowTrade,
        timestamp_utc: datetime,
        current_price: Decimal,
    ) -> None:
        with self.storage.connect() as conn:
            conn.execute(
                """
                INSERT INTO mfe_mae_tracking(
                    trade_id, timestamp_utc, current_price, mfe_abs, mfe_pct, mfe_ticks,
                    mae_abs, mae_pct, mae_ticks, time_to_mfe_sec, time_to_mae_sec
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trade.trade_id,
                    timestamp_utc.isoformat(),
                    _num(current_price),
                    _num(trade.mfe_abs),
                    _num(trade.mfe_pct),
                    _num(trade.mfe_ticks),
                    _num(trade.mae_abs),
                    _num(trade.mae_pct),
                    _num(trade.mae_ticks),
                    _num(trade.time_to_mfe_sec),
                    _num(trade.time_to_mae_sec),
                ),
            )
            conn.commit()

    def _record_trade_event(
        self,
        trade_id: str,
        timestamp_utc: datetime,
        event_type: str,
        price: Decimal | None,
        details: dict[str, Any],
    ) -> None:
        with self.storage.connect() as conn:
            conn.execute(
                """
                INSERT INTO shadow_trade_events(trade_id, timestamp_utc, event_type, price,
                                                details_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (trade_id, timestamp_utc.isoformat(), event_type, _num(price), _json(details)),
            )
            conn.commit()

    def _record_experiment_metrics(self, timestamp_utc: datetime, instrument: str) -> None:
        with self.storage.connect() as conn:
            for stop_ticks in self.config.tail_catcher.stop_ticks:
                trigger = self.config.tail_catcher.default_protection_trigger_bps
                trailing_mode = self.config.tail_catcher.default_trailing_mode
                rows = list(
                    conn.execute(
                        """
                        SELECT *
                        FROM shadow_trades
                        WHERE instrument = ?
                          AND stop_ticks = ?
                          AND protection_trigger_bps = ?
                          AND trailing_mode = ?
                          AND is_control = 0
                          AND status = 'CLOSED'
                        """,
                        (instrument, stop_ticks, _num(trigger), trailing_mode),
                    )
                )
                metrics = _experiment_metrics(
                    rows,
                    big_runner_bps=self.config.tail_catcher.big_runner_mfe_bps,
                )
                conn.execute(
                    """
                    INSERT INTO shadow_stop_experiments(
                        timestamp_utc, instrument, stop_ticks, protection_trigger_bps,
                        trailing_mode, trades_count, stops_count, reentries_count,
                        winrate, avg_loss_ticks, avg_win_ticks, expectancy,
                        profit_factor, median_mfe, median_mae, p90_mfe, p95_mfe,
                        max_consecutive_stops, time_in_trade_avg, stop_efficiency,
                        protection_reached_rate, protection_saved_count,
                        big_runner_count, best_context_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?)
                    """,
                    (
                        timestamp_utc.isoformat(),
                        instrument,
                        stop_ticks,
                        _num(trigger),
                        trailing_mode,
                        metrics["trades_count"],
                        metrics["stops_count"],
                        metrics["reentries_count"],
                        _num(metrics["winrate"]),
                        _num(metrics["avg_loss_ticks"]),
                        _num(metrics["avg_win_ticks"]),
                        _num(metrics["expectancy"]),
                        _num(metrics["profit_factor"]),
                        _num(metrics["median_mfe"]),
                        _num(metrics["median_mae"]),
                        _num(metrics["p90_mfe"]),
                        _num(metrics["p95_mfe"]),
                        metrics["max_consecutive_stops"],
                        _num(metrics["time_in_trade_avg"]),
                        _num(metrics["stop_efficiency"]),
                        _num(metrics["protection_reached_rate"]),
                        metrics["protection_saved_count"],
                        metrics["big_runner_count"],
                        _json(metrics["best_context"]),
                    ),
                )
            conn.commit()

    def _record_entry_type_performance(self, timestamp_utc: datetime, instrument: str) -> None:
        with self.storage.connect() as conn:
            entry_types = [
                str(row["entry_type"])
                for row in conn.execute(
                    """
                    SELECT DISTINCT entry_type
                    FROM shadow_signals
                    WHERE instrument = ? AND entry_type IS NOT NULL
                    """,
                    (instrument,),
                )
            ]
            for entry_type in entry_types:
                signals_count = int(
                    conn.execute(
                        """
                        SELECT COUNT(*)
                        FROM shadow_signals
                        WHERE instrument = ? AND entry_type = ?
                        """,
                        (instrument, entry_type),
                    ).fetchone()[0]
                )
                rows = list(
                    conn.execute(
                        """
                        SELECT *
                        FROM shadow_trades
                        WHERE instrument = ? AND entry_type = ? AND is_control = 1
                        """,
                        (instrument, entry_type),
                    )
                )
                metrics = _entry_type_metrics(rows, signals_count)
                conn.execute(
                    """
                    INSERT INTO entry_type_performance(
                        timestamp_utc, instrument, entry_type, signals_count,
                        accepted_trades, stop_exits, protected_exits, trailing_exits,
                        avg_pnl_ticks, avg_pnl_bps, avg_mfe_ticks, avg_mae_ticks,
                        mfe3_rate, mfe5_rate, mfe10_rate, mfe_005pct_rate,
                        mfe_010pct_rate, mfe_015pct_rate, stop_hit_rate,
                        direction_correct_rate, best_stop_ticks, best_session_time,
                        max_consecutive_stops, profit_factor, details_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?)
                    """,
                    (
                        timestamp_utc.isoformat(),
                        instrument,
                        entry_type,
                        signals_count,
                        metrics["accepted_trades"],
                        metrics["stop_exits"],
                        metrics["protected_exits"],
                        metrics["trailing_exits"],
                        _num(metrics["avg_pnl_ticks"]),
                        _num(metrics["avg_pnl_bps"]),
                        _num(metrics["avg_mfe_ticks"]),
                        _num(metrics["avg_mae_ticks"]),
                        _num(metrics["mfe3_rate"]),
                        _num(metrics["mfe5_rate"]),
                        _num(metrics["mfe10_rate"]),
                        _num(metrics["mfe_005pct_rate"]),
                        _num(metrics["mfe_010pct_rate"]),
                        _num(metrics["mfe_015pct_rate"]),
                        _num(metrics["stop_hit_rate"]),
                        _num(metrics["direction_correct_rate"]),
                        metrics["best_stop_ticks"],
                        metrics["best_session_time"],
                        metrics["max_consecutive_stops"],
                        _num(metrics["profit_factor"]),
                        _json(metrics["details"]),
                    ),
                )
            conn.commit()

    def _record_health(self, timestamp_utc: datetime, status: str, details: dict[str, Any]) -> None:
        with self.storage.connect() as conn:
            conn.execute(
                """
                INSERT INTO system_health(timestamp_utc, component, status, heartbeat,
                                          details_json)
                VALUES (?, 'tail_catcher', ?, ?, ?)
                """,
                (timestamp_utc.isoformat(), status, timestamp_utc.isoformat(), _json(details)),
            )
            conn.commit()

    def _record_error(
        self,
        timestamp_utc: datetime,
        component: str,
        instrument: str | None,
        error_type: str,
        message: str,
        details: dict[str, Any],
    ) -> None:
        with self.storage.connect() as conn:
            conn.execute(
                """
                INSERT INTO errors(timestamp_utc, component, instrument, error_type, message,
                                   details_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    timestamp_utc.isoformat(),
                    component,
                    instrument,
                    error_type,
                    message,
                    _json(details),
                ),
            )
            conn.commit()

    def _recent_consecutive_stops(self, instrument: str, side: PositionSide) -> int:
        rows = self.storage.fetch_all(
            """
            SELECT entry_price, exit_price, side
            FROM shadow_trades
            WHERE instrument = ?
              AND side = ?
              AND status = 'CLOSED'
              AND is_control = 1
              AND exit_price IS NOT NULL
            ORDER BY exit_time DESC
            LIMIT 20
            """,
            (instrument, side.value),
        )
        count = 0
        for row in rows:
            entry_price = Decimal(str(row["entry_price"]))
            exit_price = Decimal(str(row["exit_price"]))
            pnl = _pnl_abs(PositionSide(str(row["side"])), entry_price, exit_price)
            if pnl < 0:
                count += 1
            else:
                break
        return count


def _session_open(snapshot: MarketSnapshot) -> bool:
    status = (snapshot.metadata.trading_status or "normal_trading").lower()
    return "normal" in status or "trading" in status or status in {"open", "unknown"}


def _spread_ok(config: NeoSwarmScalperConfig, micro: dict[str, Any]) -> bool:
    return bool(_spread_entry_assessment(config, micro)["entry_ok"])


def _spread_entry_assessment(
    config: NeoSwarmScalperConfig,
    micro: dict[str, Any],
) -> dict[str, Any]:
    spread_ticks = _dec_or_none(micro.get("spread_ticks"))
    spread_bps = Decimal(str(micro["spread_bps"]))
    tick_blocked = spread_ticks is not None and spread_ticks > config.tail_catcher.spread_max_ticks
    bps_blocked = spread_bps > config.tail_catcher.spread_hard_bps
    reasons = []
    if tick_blocked:
        reasons.append("spread_ticks")
    if bps_blocked:
        reasons.append("spread_bps")
    return {
        "entry_ok": not reasons,
        "status": "ok" if not reasons else "no_entry",
        "no_entry_reason": ",".join(reasons),
        "spread_ticks": spread_ticks,
        "spread_bps": spread_bps,
        "spread_max_ticks": config.tail_catcher.spread_max_ticks,
        "spread_hard_bps": config.tail_catcher.spread_hard_bps,
    }


def _spread_exit_assessment(
    config: NeoSwarmScalperConfig,
    micro: dict[str, Any],
) -> dict[str, Any]:
    entry = _spread_entry_assessment(config, micro)
    spread_ticks = _dec_or_none(micro.get("spread_ticks"))
    spread_bps = Decimal(str(micro["spread_bps"]))
    tick_extreme = (
        spread_ticks is not None
        and spread_ticks >= config.tail_catcher.spread_max_ticks * Decimal("3")
    )
    bps_extreme = spread_bps >= config.tail_catcher.spread_hard_bps * Decimal("2")
    exit_infeasible = bool(
        bps_extreme or (tick_extreme and spread_bps > config.tail_catcher.spread_hard_bps)
    )
    return {
        **entry,
        "status": "market_bad" if not entry["entry_ok"] else "ok",
        "market_bad": not entry["entry_ok"],
        "exit_infeasible": exit_infeasible,
        "tick_extreme": tick_extreme,
        "bps_extreme": bps_extreme,
    }


def _time_exit(trade: _ShadowTrade, timestamp_utc: datetime, max_age_sec: int) -> bool:
    return (timestamp_utc - trade.entry_time).total_seconds() >= max_age_sec


def _imbalance(bid: Decimal, ask: Decimal) -> Decimal:
    total = bid + ask
    if total == 0:
        return Decimal("0")
    return (bid - ask) / total


def _book_slope(snapshot: MarketSnapshot) -> Decimal:
    if not snapshot.bid_levels or not snapshot.ask_levels:
        return Decimal("0")
    bid_span = snapshot.bid_levels[0].price - snapshot.bid_levels[-1].price
    ask_span = snapshot.ask_levels[-1].price - snapshot.ask_levels[0].price
    depth = Decimal(max(len(snapshot.bid_levels), len(snapshot.ask_levels), 1))
    return (bid_span + ask_span) / depth


def _wall_detect(levels: tuple[Any, ...]) -> bool:
    if len(levels) < 3:
        return False
    quantities = [Decimal(str(level.quantity)) for level in levels[:10]]
    avg = sum(quantities, Decimal("0")) / Decimal(len(quantities))
    return quantities[0] >= avg * Decimal("2") if avg > 0 else False


def _realized(prices: deque[tuple[datetime, Decimal]], seconds: int) -> Decimal:
    values = _window_prices(prices, seconds)
    if len(values) < 3:
        return Decimal("0")
    returns: list[Decimal] = []
    for index in range(1, len(values)):
        previous = values[index - 1]
        current = values[index]
        if previous != 0:
            returns.append((current - previous) / previous)
    if len(returns) < 2:
        return Decimal("0")
    mean = sum(returns, Decimal("0")) / Decimal(len(returns))
    variance = sum(((item - mean) ** 2 for item in returns), Decimal("0")) / Decimal(len(returns))
    return variance.sqrt()


def _velocity(prices: deque[tuple[datetime, Decimal]], seconds: int) -> Decimal:
    values = list(prices)
    if len(values) < 2:
        return Decimal("0")
    latest_time, latest_price = values[-1]
    cutoff = latest_time.timestamp() - seconds
    candidate = values[0]
    for item in values:
        if item[0].timestamp() <= cutoff:
            candidate = item
    elapsed = Decimal(str(max((latest_time - candidate[0]).total_seconds(), 1)))
    return (latest_price - candidate[1]) / elapsed


def _range(prices: deque[tuple[datetime, Decimal]], seconds: int) -> Decimal:
    values = _window_prices(prices, seconds)
    if not values:
        return Decimal("0")
    return max(values) - min(values)


def _range_position(
    prices: deque[tuple[datetime, Decimal]],
    price: Decimal,
    seconds: int,
) -> Decimal:
    values = _window_prices(prices, seconds)
    if not values:
        return Decimal("0.5")
    low = min(values)
    high = max(values)
    if high == low:
        return Decimal("0.5")
    return (price - low) / (high - low)


def _window_prices(prices: deque[tuple[datetime, Decimal]], seconds: int) -> list[Decimal]:
    values = list(prices)
    if not values:
        return []
    cutoff = values[-1][0].timestamp() - seconds
    return [price for timestamp, price in values if timestamp.timestamp() >= cutoff]


def _stop_hit(trade: _ShadowTrade, exit_price: Decimal) -> bool:
    if trade.side is PositionSide.LONG:
        return exit_price <= trade.stop_price
    return exit_price >= trade.stop_price


def _protection_hit(trade: _ShadowTrade, trigger_price: Decimal) -> bool:
    if trade.protection_price is None:
        return False
    if trade.side is PositionSide.LONG:
        return trigger_price <= trade.protection_price
    return trigger_price >= trade.protection_price


def _pnl_abs(side: PositionSide, entry: Decimal, exit_price: Decimal) -> Decimal:
    if side is PositionSide.LONG:
        return exit_price - entry
    return entry - exit_price


def _side_signed(side: PositionSide, value: Decimal) -> Decimal:
    return value if side is PositionSide.LONG else -value


def _protection_price(side: PositionSide, entry: Decimal, tick: Decimal) -> Decimal:
    if side is PositionSide.LONG:
        return entry + tick
    return entry - tick


def _experiment_metrics(rows: list[Any], *, big_runner_bps: Decimal) -> dict[str, Any]:
    if not rows:
        return {
            "trades_count": 0,
            "stops_count": 0,
            "reentries_count": 0,
            "winrate": Decimal("0"),
            "avg_loss_ticks": Decimal("0"),
            "avg_win_ticks": Decimal("0"),
            "expectancy": Decimal("0"),
            "profit_factor": Decimal("0"),
            "median_mfe": Decimal("0"),
            "median_mae": Decimal("0"),
            "p90_mfe": Decimal("0"),
            "p95_mfe": Decimal("0"),
            "max_consecutive_stops": 0,
            "time_in_trade_avg": Decimal("0"),
            "stop_efficiency": Decimal("0"),
            "protection_reached_rate": Decimal("0"),
            "protection_saved_count": 0,
            "big_runner_count": 0,
            "best_context": {},
        }
    pnl_ticks = [_row_pnl_ticks(row) for row in rows]
    wins = [item for item in pnl_ticks if item > 0]
    losses = [item for item in pnl_ticks if item < 0]
    mfe = [Decimal(str(row["mfe_ticks"])) for row in rows]
    mae = [Decimal(str(row["mae_ticks"])) for row in rows]
    stops_count = sum(
        1 for row in rows if str(row["exit_reason"]) in {"stop_loss", "protected_stop"}
    )
    durations = [_duration_seconds(row) for row in rows]
    protection_reached = sum(1 for row in rows if int(row["protection_activated"]) == 1)
    protection_saved = sum(1 for row in rows if row["protected_exit_reason"] is not None)
    big_runner = sum(
        1 for row in rows if Decimal(str(row["mfe_pct"])) >= big_runner_bps / Decimal("10000")
    )
    gross_win = sum(wins, Decimal("0"))
    gross_loss = abs(sum(losses, Decimal("0")))
    return {
        "trades_count": len(rows),
        "stops_count": stops_count,
        "reentries_count": sum(1 for row in rows if int(row["reentry_index"]) > 0),
        "winrate": Decimal(len(wins)) / Decimal(len(rows)),
        "avg_loss_ticks": (
            sum(losses, Decimal("0")) / Decimal(len(losses)) if losses else Decimal("0")
        ),
        "avg_win_ticks": sum(wins, Decimal("0")) / Decimal(len(wins)) if wins else Decimal("0"),
        "expectancy": sum(pnl_ticks, Decimal("0")) / Decimal(len(rows)),
        "profit_factor": (
            gross_win / gross_loss
            if gross_loss
            else (Decimal("999") if gross_win else Decimal("0"))
        ),
        "median_mfe": _median(mfe),
        "median_mae": _median(mae),
        "p90_mfe": _percentile(mfe, Decimal("0.90")),
        "p95_mfe": _percentile(mfe, Decimal("0.95")),
        "max_consecutive_stops": _max_consecutive_stops(rows),
        "time_in_trade_avg": sum(durations, Decimal("0")) / Decimal(len(durations)),
        "stop_efficiency": abs(sum(losses, Decimal("0")))
        / (Decimal(stops_count) if stops_count else Decimal("1")),
        "protection_reached_rate": Decimal(protection_reached) / Decimal(len(rows)),
        "protection_saved_count": protection_saved,
        "big_runner_count": big_runner,
        "best_context": {"sample": "aggregate_by_stop_protection_trailing"},
    }


def _entry_type_metrics(rows: list[Any], signals_count: int) -> dict[str, Any]:
    if not rows:
        return {
            "accepted_trades": 0,
            "stop_exits": 0,
            "protected_exits": 0,
            "trailing_exits": 0,
            "avg_pnl_ticks": Decimal("0"),
            "avg_pnl_bps": Decimal("0"),
            "avg_mfe_ticks": Decimal("0"),
            "avg_mae_ticks": Decimal("0"),
            "mfe3_rate": Decimal("0"),
            "mfe5_rate": Decimal("0"),
            "mfe10_rate": Decimal("0"),
            "mfe_005pct_rate": Decimal("0"),
            "mfe_010pct_rate": Decimal("0"),
            "mfe_015pct_rate": Decimal("0"),
            "stop_hit_rate": Decimal("0"),
            "direction_correct_rate": Decimal("0"),
            "best_stop_ticks": None,
            "best_session_time": None,
            "max_consecutive_stops": 0,
            "profit_factor": Decimal("0"),
            "details": {"signals_count": signals_count},
        }
    closed = [row for row in rows if row["exit_price"] is not None]
    pnl_ticks = [_row_pnl_ticks(row) for row in closed]
    pnl_bps = [_row_pnl_bps(row) for row in closed]
    wins = [item for item in pnl_ticks if item > 0]
    losses = [item for item in pnl_ticks if item < 0]
    denominator = Decimal(len(rows))
    stop_exits = sum(
        1 for row in closed if str(row["exit_reason"]) in {"stop_loss", "protected_stop"}
    )
    protected_exits = sum(1 for row in closed if str(row["exit_reason"]) == "protected_exit")
    trailing_exits = sum(1 for row in closed if str(row["exit_reason"]) == "trailing_runner")
    gross_win = sum(wins, Decimal("0"))
    gross_loss = abs(sum(losses, Decimal("0")))
    return {
        "accepted_trades": len(rows),
        "stop_exits": stop_exits,
        "protected_exits": protected_exits,
        "trailing_exits": trailing_exits,
        "avg_pnl_ticks": _avg(pnl_ticks),
        "avg_pnl_bps": _avg(pnl_bps),
        "avg_mfe_ticks": _avg([Decimal(str(row["mfe_ticks"])) for row in rows]),
        "avg_mae_ticks": _avg([Decimal(str(row["mae_ticks"])) for row in rows]),
        "mfe3_rate": _rate(rows, lambda row: Decimal(str(row["mfe_ticks"])) >= Decimal("3")),
        "mfe5_rate": _rate(rows, lambda row: Decimal(str(row["mfe_ticks"])) >= Decimal("5")),
        "mfe10_rate": _rate(rows, lambda row: Decimal(str(row["mfe_ticks"])) >= Decimal("10")),
        "mfe_005pct_rate": _rate(
            rows,
            lambda row: Decimal(str(row["mfe_pct"])) >= Decimal("0.0005"),
        ),
        "mfe_010pct_rate": _rate(
            rows,
            lambda row: Decimal(str(row["mfe_pct"])) >= Decimal("0.0010"),
        ),
        "mfe_015pct_rate": _rate(
            rows,
            lambda row: Decimal(str(row["mfe_pct"])) >= Decimal("0.0015"),
        ),
        "stop_hit_rate": Decimal(stop_exits) / denominator,
        "direction_correct_rate": _rate(
            rows,
            lambda row: Decimal(str(row["mfe_ticks"])) > Decimal("0"),
        ),
        "best_stop_ticks": _best_stop_ticks(closed),
        "best_session_time": _best_session_time(rows),
        "max_consecutive_stops": _max_consecutive_stops(closed),
        "profit_factor": (
            gross_win / gross_loss
            if gross_loss
            else (Decimal("999") if gross_win else Decimal("0"))
        ),
        "details": {"signals_count": signals_count, "closed_trades": len(closed)},
    }


def _avg(values: list[Decimal]) -> Decimal:
    return sum(values, Decimal("0")) / Decimal(len(values)) if values else Decimal("0")


def _rate(rows: list[Any], predicate: Any) -> Decimal:
    if not rows:
        return Decimal("0")
    return Decimal(sum(1 for row in rows if predicate(row))) / Decimal(len(rows))


def _best_stop_ticks(rows: list[Any]) -> int | None:
    by_stop: dict[int, list[Decimal]] = {}
    for row in rows:
        if str(row["exit_reason"]) == "spread_shock":
            continue
        by_stop.setdefault(int(row["stop_ticks"]), []).append(_row_pnl_ticks(row))
    if not by_stop:
        return None
    return max(by_stop.items(), key=lambda item: _avg(item[1]))[0]


def _best_session_time(rows: list[Any]) -> str | None:
    if not rows:
        return None
    best = max(rows, key=lambda row: Decimal(str(row["mfe_ticks"])))
    raw = str(best["entry_time"])
    return raw[:13] + ":00" if len(raw) >= 13 else raw


def _row_pnl_ticks(row: Any) -> Decimal:
    entry = Decimal(str(row["entry_price"]))
    exit_price = Decimal(str(row["exit_price"]))
    stop_price = Decimal(str(row["stop_price"]))
    stop_ticks = Decimal(str(row["stop_ticks"]))
    trigger_entry_raw = row["trigger_entry_price"]
    trigger_entry = entry if trigger_entry_raw is None else Decimal(str(trigger_entry_raw))
    tick = abs(trigger_entry - stop_price) / stop_ticks if stop_ticks else Decimal("1")
    pnl = _pnl_abs(PositionSide(str(row["side"])), entry, exit_price)
    return pnl / tick if tick else Decimal("0")


def _row_pnl_bps(row: Any) -> Decimal:
    entry = Decimal(str(row["entry_price"]))
    exit_price = Decimal(str(row["exit_price"]))
    if entry == 0:
        return Decimal("0")
    pnl = _pnl_abs(PositionSide(str(row["side"])), entry, exit_price)
    return (pnl / entry) * Decimal("10000")


def _duration_seconds(row: Any) -> Decimal:
    try:
        return Decimal(str((_dt(row["exit_time"]) - _dt(row["entry_time"])).total_seconds()))
    except Exception:
        return Decimal("0")


def _max_consecutive_stops(rows: list[Any]) -> int:
    result = 0
    current = 0
    ordered = sorted(rows, key=lambda row: str(row["exit_time"]))
    for row in ordered:
        if str(row["exit_reason"]) in {"stop_loss", "protected_stop"}:
            current += 1
            result = max(result, current)
        else:
            current = 0
    return result


def _median(values: list[Decimal]) -> Decimal:
    if not values:
        return Decimal("0")
    return Decimal(str(median(values)))


def _percentile(values: list[Decimal], percentile: Decimal) -> Decimal:
    if not values:
        return Decimal("0")
    ordered = sorted(values)
    index = int((Decimal(len(ordered) - 1) * percentile).to_integral_value())
    return ordered[index]


def _levels_json(levels: tuple[Any, ...]) -> str:
    return _json([{"price": str(level.price), "quantity": str(level.quantity)} for level in levels])


def _json(value: Any) -> str:
    return json.dumps(_jsonable(value), ensure_ascii=False, sort_keys=True)


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _num(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bool):
        return float(int(value))
    return float(Decimal(str(value)))


def _dec_or_none(value: object) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def _dt(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


__all__ = ["TailCatcherEngine"]
