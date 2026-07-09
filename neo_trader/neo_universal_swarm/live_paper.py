"""Live-market paper loop for the Neo Universal Bot Swarm.

The loop polls T-Bank read-only order books and simulates hedge-pair lifecycle
locally. It never submits, cancels, or replaces broker orders.
"""

from __future__ import annotations

import csv
import json
import os
import time
import traceback
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Protocol
from uuid import uuid4

from neo_trader.broker.tbank import TBankClient, TBankOrderBookSnapshot
from neo_trader.neo_universal_swarm.bots import CuratorBot
from neo_trader.neo_universal_swarm.config import DEFAULT_ACCOUNTS_CONFIG, load_accounts_config
from neo_trader.neo_universal_swarm.dashboard import build_swarm_dashboard_state
from neo_trader.neo_universal_swarm.first_bot_logic_clone import (
    ARCHITECTURE,
    FIRST_BOT_LOGIC_CLONE_ENABLED,
    INSTRUMENT_ALLOCATION,
    LEGACY_UNIVERSAL_LOGIC_ENABLED,
    LOGIC_SOURCE,
    CloneGateDecision,
    FirstBotCloneGate,
    force_first_bot_prediction,
    instrument_allowed,
    label_enrichment,
)
from neo_trader.neo_universal_swarm.instruments import (
    SwarmInstrumentCatalog,
    load_swarm_instrument_catalog,
)
from neo_trader.neo_universal_swarm.model import (
    PairEVModel,
    PairEVModelConfig,
    PairEVPrediction,
)
from neo_trader.neo_universal_swarm.online_learning import (
    ExperimentalMode,
    OnlineLearningState,
)
from neo_trader.neo_universal_swarm.simulator import (
    HedgePairSimulationConfig,
    aggregate_pair_metrics,
)
from neo_trader.neo_universal_swarm.types import (
    ActivePair,
    BookSnapshot,
    LegSide,
    PairExitReason,
    PairLabel,
    RejectionReason,
    SwarmInstrument,
    SwarmMetrics,
    as_utc,
)
from neo_trader.runtime import get_runtime_commit_hash

ClockFunc = Callable[[], datetime]
SleepFunc = Callable[[float], None]
TRAIL_LOCK_4_TICKS = Decimal("4")
TRAIL_LOCK_6_TICKS = Decimal("6")
TRAIL_LOCK_MIN_PNL_TICKS = Decimal("1")
INSTRUMENT_ENTRY_ALLOCATION: dict[SwarmInstrument, Decimal] = dict(INSTRUMENT_ALLOCATION)


class OrderBookProvider(Protocol):
    """Read-only market-data subset used by the live paper loop."""

    def get_orderbook_snapshot(
        self,
        instrument_id: str,
        *,
        depth: int = 10,
        order_book_type: str | None = None,
    ) -> TBankOrderBookSnapshot:
        """Return a current order book snapshot."""


@dataclass(frozen=True)
class LivePaperSwarmConfig:
    """Runtime settings for real-data paper trading."""

    accounts_path: Path = DEFAULT_ACCOUNTS_CONFIG
    orderbook_depth: int = 10
    poll_interval_seconds: float = 5.0
    min_required_ev_ticks: Decimal = Decimal("1")
    slippage_stress_ticks: Decimal = Decimal("0")
    max_pair_age_seconds: float = 300.0
    reports_dir: Path = Path("data/reports/neo_universal_swarm_live_paper")
    dashboard_state_path: Path = Path("data/monitoring/neo_universal_swarm_dashboard_state.json")
    heartbeat_path: Path = Path("data/monitoring/neo_universal_swarm_heartbeat.txt")
    online_learning_state_path: Path | None = None
    instruments: tuple[SwarmInstrument, ...] = tuple(SwarmInstrument)
    max_cycles: int | None = None

    def __post_init__(self) -> None:
        if self.orderbook_depth <= 0:
            raise ValueError("orderbook_depth must be positive.")
        if self.poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive.")
        if self.min_required_ev_ticks < 0:
            raise ValueError("min_required_ev_ticks must be non-negative.")
        if self.slippage_stress_ticks < 0:
            raise ValueError("slippage_stress_ticks must be non-negative.")
        if self.max_pair_age_seconds <= 0:
            raise ValueError("max_pair_age_seconds must be positive.")
        if not self.instruments:
            raise ValueError("instruments must not be empty.")
        if self.max_cycles is not None and self.max_cycles <= 0:
            raise ValueError("max_cycles must be positive when set.")


@dataclass(frozen=True)
class LivePaperSwarmCycle:
    """One completed live-paper polling cycle."""

    cycle: int
    started_at: datetime
    finished_at: datetime
    status: str
    snapshots: int
    active_pairs: int
    closed_pairs: int
    total_pnl_ticks: Decimal
    error: str | None = None


@dataclass
class _LivePairState:
    pair: ActivePair
    mode: ExperimentalMode
    entry: BookSnapshot
    long_entry: Decimal
    short_entry: Decimal
    stop: Decimal
    slippage: Decimal
    loser_side: LegSide | None = None
    runner_side: LegSide | None = None
    loser_loss_ticks: Decimal = Decimal("0")
    runner_mfe_ticks: Decimal = Decimal("-999999")
    runner_mae_ticks: Decimal = Decimal("999999")
    breakeven_active: bool = False
    max_adverse_excursion: Decimal = Decimal("0")
    max_favorable_excursion: Decimal = Decimal("0")
    latest_snapshot: BookSnapshot | None = None
    wide_spread_panic_cycles: int = 0
    wide_spread_pnl_before_wait: Decimal | None = None
    wide_spread_last_spread_ticks: Decimal | None = None
    wide_spread_max_spread_ticks: Decimal = Decimal("0")
    panic_wait_helped: bool = False
    panic_wait_hurt: bool = False
    avoided_wide_spread_exits: int = 0
    trail_lock_4_active: bool = False
    missed_runner_profit_ticks: Decimal = Decimal("0")
    logged_avoided_wide_spread_exits: int = 0
    logged_missed_runner_profit_ticks: Decimal = Decimal("0")
    logged_loser_stop: bool = False
    logged_runner_breakeven: bool = False
    logged_runner_trail: bool = False
    gate_decision: CloneGateDecision | None = None


class _LivePaperRecorder:
    """Append-only detailed recorder for live-paper runtime data."""

    def __init__(self, reports_dir: Path | str) -> None:
        self.reports_dir = Path(reports_dir)
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        self._pair_labels_csv_path = self.reports_dir / "live_paper_pair_labels.csv"
        self._ensure_jsonl_files()
        self._ensure_pair_labels_csv()

    def record_orderbook(self, *, cycle: int, snapshot: BookSnapshot) -> None:
        payload = {
            "cycle": cycle,
            "recorded_at": datetime.now(UTC),
            "valid": True,
            "snapshot": _snapshot_payload(snapshot),
        }
        self._append("live_paper_orderbooks.jsonl", payload)
        self._append("orderbook_snapshots.jsonl", payload)

    def record_invalid_orderbook(
        self,
        *,
        cycle: int,
        instrument: SwarmInstrument,
        instrument_uid: str,
        reason: str,
        bids: int,
        asks: int,
    ) -> None:
        payload = {
            "cycle": cycle,
            "recorded_at": datetime.now(UTC),
            "valid": False,
            "instrument": instrument.value,
            "instrument_uid": instrument_uid,
            "reason": reason,
            "bids": bids,
            "asks": asks,
        }
        self._append("live_paper_orderbooks.jsonl", payload)
        self._append("orderbook_snapshots.jsonl", payload)

    def record_prediction(
        self,
        *,
        cycle: int,
        snapshot: BookSnapshot,
        mode: ExperimentalMode,
        prediction: PairEVPrediction,
        opened_pair_id: str | None = None,
        skipped_reason: str | None = None,
        gate_decision: CloneGateDecision | None = None,
    ) -> None:
        payload = {
            "cycle": cycle,
            "recorded_at": datetime.now(UTC),
            "logic_source": LOGIC_SOURCE,
            "architecture": ARCHITECTURE,
            "instrument": snapshot.instrument.value,
            "timestamp": snapshot.timestamp,
            "mode": mode.value,
            "opened_pair_id": opened_pair_id,
            "skipped_reason": skipped_reason,
            "prediction": _prediction_payload(prediction),
            "snapshot": _snapshot_payload(snapshot),
            "clone_diagnostic": None if gate_decision is None else gate_decision.to_payload(),
        }
        self._append("live_paper_predictions.jsonl", payload)
        self._append("model_predictions.jsonl", payload)

    def record_pair_opened(
        self,
        *,
        cycle: int,
        state: _LivePairState,
        snapshot: BookSnapshot,
    ) -> None:
        base_event = {
            "cycle": cycle,
            "recorded_at": datetime.now(UTC),
            "logic_source": LOGIC_SOURCE,
            "architecture": ARCHITECTURE,
            "mode": state.mode.value,
            "pair_id": state.pair.pair_id,
            "instrument": state.pair.instrument.value,
            "symbol": state.pair.instrument.value,
            "long_bot": state.pair.long_bot_id,
            "short_bot": state.pair.short_bot_id,
            "long_bot_id": state.pair.long_bot_id,
            "short_bot_id": state.pair.short_bot_id,
            "entry_bid": snapshot.best_bid,
            "entry_ask": snapshot.best_ask,
            "entry_spread": snapshot.spread_ticks,
            "long_entry_price": state.long_entry,
            "short_entry_price": state.short_entry,
            "stop_loss_ticks": state.pair.stop_loss_ticks,
            "breakeven_ticks": state.pair.breakeven_ticks,
            "paper": True,
            "live": False,
            "clone_diagnostic": None
            if state.gate_decision is None
            else state.gate_decision.to_payload(),
        }
        for event_name in (
            "PAIR_OPEN_REQUEST",
            "PAIR_OPEN",
            "LONG_LEG_OPEN",
            "SHORT_LEG_OPEN",
        ):
            event = {
                **base_event,
                "event": event_name,
                "event_type": event_name,
            }
            if event_name == "LONG_LEG_OPEN":
                event["bot_id"] = state.pair.long_bot_id
                event["side"] = LegSide.LONG.value
                event["entry_price"] = state.long_entry
            elif event_name == "SHORT_LEG_OPEN":
                event["bot_id"] = state.pair.short_bot_id
                event["side"] = LegSide.SHORT.value
                event["entry_price"] = state.short_entry
            self._append("pair_events.jsonl", event)
        payload = {
            "cycle": cycle,
            "recorded_at": datetime.now(UTC),
            "event_type": "PAIR_OPENED",
            "event": "PAIR_OPEN",
            "logic_source": LOGIC_SOURCE,
            "architecture": ARCHITECTURE,
            "mode": state.mode.value,
            "pair": _active_pair_payload(state.pair),
            "entry": _live_pair_state_payload(state, snapshot),
            "snapshot": _snapshot_payload(snapshot),
        }
        self._append("live_paper_pair_opened.jsonl", payload)
        self._append("pair_events.jsonl", payload)

    def record_pair_update(
        self,
        *,
        cycle: int,
        state: _LivePairState,
        snapshot: BookSnapshot,
        label: PairLabel | None,
    ) -> None:
        payload = {
            "cycle": cycle,
            "recorded_at": datetime.now(UTC),
            "event_type": "PAIR_UPDATE" if label is None else "PAIR_CLOSED",
            "event": "PAIR_UPDATE" if label is None else "PAIR_CLOSE",
            "logic_source": LOGIC_SOURCE,
            "architecture": ARCHITECTURE,
            "mode": state.mode.value,
            "pair": _active_pair_payload(state.pair),
            "state": _live_pair_state_payload(state, snapshot),
            "snapshot": _snapshot_payload(snapshot),
            "closed_label": None if label is None else _label_payload(label),
        }
        self._append("live_paper_pair_updates.jsonl", payload)
        self._append("pair_events.jsonl", payload)
        if label is not None:
            self._append(
                "pair_events.jsonl",
                {
                    "cycle": cycle,
                    "recorded_at": datetime.now(UTC),
                    "event": "PAIR_CLOSE",
                    "event_type": "PAIR_CLOSE",
                    "logic_source": LOGIC_SOURCE,
                    "architecture": ARCHITECTURE,
                    "mode": state.mode.value,
                    "pair_id": label.pair_id,
                    "instrument": label.instrument.value,
                    "symbol": label.instrument.value,
                    "long_bot": label.long_bot_id,
                    "short_bot": label.short_bot_id,
                    "runner_side": None if label.runner_side is None else label.runner_side.value,
                    "loser_side": None if label.loser_side is None else label.loser_side.value,
                    "pair_pnl_after_spread": label.pair_total_pnl_ticks,
                    "raw_pair_pnl": label.pair_total_pnl_ticks + label.entry_spread_cost_ticks,
                    "spread_cost": label.entry_spread_cost_ticks,
                    "exit_reason": label.exit_reason.value,
                    "paper": True,
                    "live": False,
                },
            )

    def record_control_event(
        self,
        *,
        cycle: int,
        event_type: str,
        state: _LivePairState,
        snapshot: BookSnapshot,
        payload: Mapping[str, object],
    ) -> None:
        normalized_payload = dict(payload)
        event_name = str(normalized_payload.pop("event", event_type))
        event = {
            "cycle": cycle,
            "recorded_at": datetime.now(UTC),
            "event": event_name,
            "event_type": event_type,
            "logic_source": LOGIC_SOURCE,
            "architecture": ARCHITECTURE,
            "mode": state.mode.value,
            "pair_id": state.pair.pair_id,
            "instrument": state.pair.instrument.value,
            "state": _live_pair_state_payload(state, snapshot),
            "payload": normalized_payload,
        }
        self._append("pair_events.jsonl", event)

    def record_pair_label(
        self,
        *,
        cycle: int,
        mode: ExperimentalMode,
        label: PairLabel,
        state: _LivePairState | None = None,
    ) -> None:
        label_payload = _label_payload(label, state=state)
        payload = {
            "cycle": cycle,
            "recorded_at": datetime.now(UTC),
            "mode": mode.value,
            "logic_source": LOGIC_SOURCE,
            "architecture": ARCHITECTURE,
            "label": label_payload,
        }
        self._append("live_paper_pair_labels.jsonl", payload)
        self._append(
            "pair_labels.jsonl",
            {
                "cycle": cycle,
                "recorded_at": datetime.now(UTC),
                **label_payload,
                "mode": mode.value,
            },
        )
        self._append_pair_label_csv(cycle=cycle, label=label)

    def record_bot_states(self, *, cycle: int, curator: CuratorBot) -> None:
        self._append(
            "live_paper_bot_states.jsonl",
            {
                "cycle": cycle,
                "recorded_at": datetime.now(UTC),
                "bots": [
                    {
                        "bot_id": bot.bot_id,
                        "account_ref": _masked_account_ref(bot.account_ref),
                        "account_kind": bot.config.account_kind.value,
                        "state": bot.state.value,
                        "assigned_pair_id": bot.assigned_pair_id,
                        "realized_pnl_ticks": bot.realized_pnl_ticks,
                        "last_command_id": bot.last_command_id,
                        "paper_enabled": bot.config.paper_enabled,
                        "live_enabled": bot.config.live_enabled,
                    }
                    for bot in curator.bots.values()
                ],
            },
        )

    def record_metrics(self, *, cycle: int, metrics: SwarmMetrics, curator: CuratorBot) -> None:
        self._append(
            "live_paper_metrics.jsonl",
            {
                "cycle": cycle,
                "recorded_at": datetime.now(UTC),
                "curator": {
                    "active_pairs": len(curator.active_pairs),
                    "closed_pairs": len(curator.closed_pairs),
                    "total_pnl_ticks": curator.total_pnl_ticks,
                    "rejections": {
                        reason.value: count for reason, count in curator.rejections.items()
                    },
                },
                "metrics": _metrics_payload(metrics),
            },
        )

    def record_cycle(self, cycle: LivePaperSwarmCycle) -> None:
        self._append(
            "live_paper_cycles.jsonl",
            {
                "cycle": cycle.cycle,
                "started_at": cycle.started_at,
                "finished_at": cycle.finished_at,
                "status": cycle.status,
                "snapshots": cycle.snapshots,
                "active_pairs": cycle.active_pairs,
                "closed_pairs": cycle.closed_pairs,
                "total_pnl_ticks": cycle.total_pnl_ticks,
                "error": cycle.error,
            },
        )

    def record_error(self, *, cycle: int, started_at: datetime, exc: Exception) -> None:
        self._append(
            "live_paper_errors.jsonl",
            {
                "cycle": cycle,
                "started_at": started_at,
                "recorded_at": datetime.now(UTC),
                "error_type": type(exc).__name__,
                "error": str(exc),
                "traceback": traceback.format_exc(),
            },
        )

    def _append(self, filename: str, payload: Mapping[str, object]) -> None:
        path = self.reports_dir / filename
        with path.open("a", encoding="utf-8") as file:
            file.write(json.dumps(_json_safe_mapping(payload), ensure_ascii=False, sort_keys=True))
            file.write("\n")

    def _ensure_jsonl_files(self) -> None:
        for filename in (
            "live_paper_orderbooks.jsonl",
            "orderbook_snapshots.jsonl",
            "live_paper_predictions.jsonl",
            "model_predictions.jsonl",
            "live_paper_pair_opened.jsonl",
            "live_paper_pair_updates.jsonl",
            "live_paper_pair_labels.jsonl",
            "pair_events.jsonl",
            "pair_labels.jsonl",
        ):
            (self.reports_dir / filename).touch(exist_ok=True)

    def _ensure_pair_labels_csv(self) -> None:
        if self._pair_labels_csv_path.exists():
            return
        with self._pair_labels_csv_path.open("w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(
                [
                    "cycle",
                    "pair_id",
                    "instrument",
                    "entry_timestamp",
                    "exit_timestamp",
                    "stop_loss_ticks",
                    "loser_side",
                    "loser_loss_ticks",
                    "runner_side",
                    "runner_mfe_ticks",
                    "runner_mae_ticks",
                    "runner_exit_ticks",
                    "pair_total_pnl_ticks",
                    "pair_total_pnl_rub",
                    "exit_reason",
                    "max_adverse_excursion",
                    "max_favorable_excursion",
                    "fakeout_flag",
                    "runner_success_flag",
                    "entry_spread_cost_ticks",
                    "avg_slippage_ticks",
                    "latency_ms",
                    "regime",
                    "long_bot_id",
                    "short_bot_id",
                ]
            )

    def _append_pair_label_csv(self, *, cycle: int, label: PairLabel) -> None:
        with self._pair_labels_csv_path.open("a", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(
                [
                    cycle,
                    label.pair_id,
                    label.instrument.value,
                    label.entry_timestamp.isoformat(),
                    label.exit_timestamp.isoformat(),
                    label.stop_loss_ticks,
                    None if label.loser_side is None else label.loser_side.value,
                    label.loser_loss_ticks,
                    None if label.runner_side is None else label.runner_side.value,
                    label.runner_mfe_ticks,
                    label.runner_mae_ticks,
                    label.runner_exit_ticks,
                    label.pair_total_pnl_ticks,
                    label.pair_total_pnl_rub,
                    label.exit_reason.value,
                    label.max_adverse_excursion,
                    label.max_favorable_excursion,
                    label.fakeout_flag,
                    label.runner_success_flag,
                    label.entry_spread_cost_ticks,
                    label.avg_slippage_ticks,
                    label.latency_ms,
                    label.regime,
                    label.long_bot_id,
                    label.short_bot_id,
                ]
            )


def run_live_paper_swarm(
    config: LivePaperSwarmConfig | None = None,
    *,
    provider: OrderBookProvider | None = None,
    sleep: SleepFunc = time.sleep,
    clock: ClockFunc | None = None,
) -> tuple[LivePaperSwarmCycle, ...]:
    """Run live-market paper trading until stopped or ``max_cycles`` is reached."""

    resolved_config = config or LivePaperSwarmConfig()
    _assert_safe_environment()
    now = clock or (lambda: datetime.now(UTC))
    accounts = load_accounts_config(resolved_config.accounts_path)
    catalog = load_swarm_instrument_catalog()
    base_model_config = PairEVModelConfig(
        min_required_ev_ticks=resolved_config.min_required_ev_ticks,
        slippage_stress_ticks=resolved_config.slippage_stress_ticks,
    )
    model = PairEVModel(base_model_config)
    curator = CuratorBot(accounts=accounts, model=model)
    client = provider or TBankClient()
    owns_client = provider is None
    recorder = _LivePaperRecorder(resolved_config.reports_dir)
    learning_state_path = (
        resolved_config.online_learning_state_path
        or resolved_config.reports_dir / "online_learning_state.json"
    )
    learning_state = _load_online_learning_state(resolved_config.reports_dir)
    clone_gate = FirstBotCloneGate(resolved_config.reports_dir / "first_bot_clone_state.json")
    latest_snapshots: dict[SwarmInstrument, BookSnapshot] = {}
    live_pairs: dict[str, _LivePairState] = {}
    labels: list[PairLabel] = []
    cycles: list[LivePaperSwarmCycle] = []
    cycle_number = 0

    try:
        while True:
            cycle_number += 1
            started_at = _as_utc(now())
            try:
                snapshots = _poll_orderbooks(
                    client,
                    catalog=catalog,
                    instruments=resolved_config.instruments,
                    previous=latest_snapshots,
                    depth=resolved_config.orderbook_depth,
                    clock=now,
                    recorder=recorder,
                    cycle=cycle_number,
                )
                for snapshot in snapshots:
                    latest_snapshots[snapshot.instrument] = snapshot
                    recorder.record_orderbook(cycle=cycle_number, snapshot=snapshot)
                    for state in tuple(live_pairs.values()):
                        if state.pair.instrument is not snapshot.instrument:
                            continue
                        label = _update_live_pair(
                            state,
                            snapshot,
                            config=resolved_config,
                            learning_state=learning_state,
                        )
                        _record_live_pair_control_events(
                            recorder=recorder,
                            cycle=cycle_number,
                            state=state,
                            snapshot=snapshot,
                        )
                        recorder.record_pair_update(
                            cycle=cycle_number,
                            state=state,
                            snapshot=snapshot,
                            label=label,
                        )
                        if label is None:
                            continue
                        learning_state.update_label(
                            instrument=label.instrument,
                            mode=state.mode,
                            label=label,
                        )
                        curator.settle_pair(label)
                        labels.append(label)
                        recorder.record_pair_label(
                            cycle=cycle_number,
                            mode=state.mode,
                            label=label,
                            state=state,
                        )
                        live_pairs.pop(label.pair_id, None)

                curator.release_cooldowns()
                predictions: dict[SwarmInstrument, PairEVPrediction] = {}
                for snapshot in snapshots:
                    if _has_active_pair_for_instrument(live_pairs, snapshot.instrument):
                        mode = learning_state.instruments[snapshot.instrument].active_mode
                        prediction = _predict_for_mode(
                            snapshot,
                            mode=mode,
                            base_config=base_model_config,
                        )
                        gate_decision = clone_gate.evaluate_pre_mode(
                            snapshot=snapshot,
                            cycle=cycle_number,
                            available_count=len(snapshots),
                            has_active_pair=True,
                        )
                        predictions[snapshot.instrument] = prediction
                        recorder.record_prediction(
                            cycle=cycle_number,
                            snapshot=snapshot,
                            mode=mode,
                            prediction=prediction,
                            skipped_reason="ACTIVE_PAIR_EXISTS",
                            gate_decision=gate_decision,
                        )
                        continue
                    pre_mode_gate = clone_gate.evaluate_pre_mode(
                        snapshot=snapshot,
                        cycle=cycle_number,
                        available_count=len(snapshots),
                        has_active_pair=False,
                    )
                    if not pre_mode_gate.allowed:
                        mode = learning_state.instruments[snapshot.instrument].active_mode
                        prediction = _predict_for_mode(
                            snapshot,
                            mode=mode,
                            base_config=base_model_config,
                        )
                        predictions[snapshot.instrument] = prediction
                        recorder.record_prediction(
                            cycle=cycle_number,
                            snapshot=snapshot,
                            mode=mode,
                            prediction=prediction,
                            skipped_reason=pre_mode_gate.rejection_reason,
                            gate_decision=pre_mode_gate,
                        )
                        continue
                    mode = learning_state.choose_mode(snapshot.instrument)
                    gate_decision = clone_gate.evaluate_mode(
                        snapshot=snapshot,
                        mode=mode.value,
                        pre_mode=pre_mode_gate,
                    )
                    if not gate_decision.allowed:
                        prediction = _predict_for_mode(
                            snapshot,
                            mode=mode,
                            base_config=base_model_config,
                        )
                        predictions[snapshot.instrument] = prediction
                        recorder.record_prediction(
                            cycle=cycle_number,
                            snapshot=snapshot,
                            mode=mode,
                            prediction=prediction,
                            skipped_reason=gate_decision.rejection_reason,
                            gate_decision=gate_decision,
                        )
                        continue
                    prediction = _predict_for_mode(
                        snapshot,
                        mode=mode,
                        base_config=base_model_config,
                    )
                    prediction = force_first_bot_prediction(
                        prediction,
                        snapshot=snapshot,
                        mode=mode.value,
                    )
                    prediction = _apply_online_entry_gates(
                        prediction,
                        snapshot=snapshot,
                        mode=mode,
                    )
                    predictions[snapshot.instrument] = prediction
                    active_pair = curator.maybe_open_pair(snapshot, prediction=prediction)
                    skipped_reason = None
                    if active_pair is None:
                        skipped_reason = (
                            prediction.rejection_reason.value
                            if prediction.rejection_reason is not None
                            else "NO_FREE_BOTS_OR_DISABLED"
                        )
                    recorder.record_prediction(
                        cycle=cycle_number,
                        snapshot=snapshot,
                        mode=mode,
                        prediction=prediction,
                        opened_pair_id=None if active_pair is None else active_pair.pair_id,
                        skipped_reason=skipped_reason,
                        gate_decision=gate_decision,
                    )
                    if active_pair is not None:
                        state = _new_live_pair_state(
                            active_pair,
                            mode,
                            snapshot,
                            slippage_stress_ticks=resolved_config.slippage_stress_ticks,
                            gate_decision=gate_decision,
                        )
                        live_pairs[active_pair.pair_id] = state
                        clone_gate.record_open(
                            snapshot=snapshot,
                            mode=mode.value,
                            decision=gate_decision,
                        )
                        learning_state.record_assignment(
                            active_pair.long_bot_id,
                            active_pair.short_bot_id,
                        )
                        recorder.record_pair_opened(
                            cycle=cycle_number,
                            state=state,
                            snapshot=snapshot,
                        )

                metrics = _metrics(labels, curator)
                recorder.record_metrics(cycle=cycle_number, metrics=metrics, curator=curator)
                recorder.record_bot_states(cycle=cycle_number, curator=curator)
                _write_live_dashboard_state(
                    resolved_config.dashboard_state_path,
                    curator=curator,
                    latest_snapshots=tuple(
                        latest_snapshots[instrument]
                        for instrument in sorted(latest_snapshots, key=lambda item: item.value)
                    ),
                    metrics=metrics,
                    updated_at=_as_utc(now()),
                    learning_state=learning_state,
                    predictions={
                        instrument.value: prediction
                        for instrument, prediction in predictions.items()
                    },
                )
                _write_online_learning_state(learning_state_path, learning_state)
                _write_summary_json(
                    resolved_config.reports_dir / "live_paper_summary.json",
                    cycle=cycle_number,
                    metrics=metrics,
                    active_pairs=live_pairs,
                    latest_snapshots=latest_snapshots,
                    learning_state=learning_state,
                )
                cycle = LivePaperSwarmCycle(
                    cycle=cycle_number,
                    started_at=started_at,
                    finished_at=_as_utc(now()),
                    status="OK",
                    snapshots=len(snapshots),
                    active_pairs=len(live_pairs),
                    closed_pairs=len(labels),
                    total_pnl_ticks=curator.total_pnl_ticks,
                )
            except Exception as exc:  # pragma: no cover - service resilience path
                recorder.record_error(cycle=cycle_number, started_at=started_at, exc=exc)
                cycle = LivePaperSwarmCycle(
                    cycle=cycle_number,
                    started_at=started_at,
                    finished_at=_as_utc(now()),
                    status="ERROR",
                    snapshots=0,
                    active_pairs=len(live_pairs),
                    closed_pairs=len(labels),
                    total_pnl_ticks=curator.total_pnl_ticks,
                    error=f"{type(exc).__name__}: {exc}",
                )
                _write_error_log(resolved_config.reports_dir, exc)

            cycles.append(cycle)
            recorder.record_cycle(cycle)
            _write_heartbeat(resolved_config.heartbeat_path, cycle)
            if (
                resolved_config.max_cycles is not None
                and cycle_number >= resolved_config.max_cycles
            ):
                return tuple(cycles)
            sleep(resolved_config.poll_interval_seconds)
    finally:
        close = getattr(client, "close", None)
        if owns_client and close is not None:
            close()


def orderbook_snapshot_to_book_snapshot(
    orderbook: TBankOrderBookSnapshot,
    *,
    instrument: SwarmInstrument,
    timestamp: datetime,
    previous: BookSnapshot | None = None,
    latency_ms: int = 0,
    tick_size: Decimal = Decimal("1"),
) -> BookSnapshot:
    """Convert a T-Bank order book snapshot into a swarm model snapshot."""

    if not orderbook.bids or not orderbook.asks:
        raise ValueError(f"{instrument.value} order book has no bid/ask levels.")
    event_time = _as_utc(timestamp)
    bids = [(level.price, Decimal(level.quantity)) for level in orderbook.bids]
    asks = [(level.price, Decimal(level.quantity)) for level in orderbook.asks]
    best_bid = bids[0][0]
    best_ask = asks[0][0]
    mid_price = (best_bid + best_ask) / Decimal("2")
    previous_mid = previous.mid_price if previous is not None else mid_price
    elapsed_seconds = (
        max((event_time - previous.timestamp).total_seconds(), 1e-9)
        if previous is not None
        else 1.0
    )
    tick_velocity = ((mid_price - previous_mid) / tick_size) / Decimal(str(elapsed_seconds))
    tick_direction = 1 if tick_velocity > 0 else -1 if tick_velocity < 0 else 0
    trade_side = (
        LegSide.LONG
        if tick_direction > 0
        else LegSide.SHORT
        if tick_direction < 0
        else None
    )
    top_bid_quantity = bids[0][1]
    top_ask_quantity = asks[0][1]
    volume_delta = top_bid_quantity - top_ask_quantity
    volatility = min(abs(tick_velocity), Decimal("5"))

    return BookSnapshot.from_levels(
        timestamp=event_time,
        instrument=instrument,
        bids=bids,
        asks=asks,
        tick_size=tick_size,
        last_price=orderbook.last_price or mid_price,
        last_trade_size=Decimal("0"),
        trade_side=trade_side,
        tick_direction=tick_direction,
        tick_velocity=tick_velocity,
        volume_delta_1s=volume_delta,
        volume_delta_5s=volume_delta,
        volume_delta_15s=volume_delta,
        volatility_5s=volatility,
        volatility_15s=volatility,
        volatility_60s=volatility,
        latency_ms=latency_ms,
    )


def _poll_orderbooks(
    provider: OrderBookProvider,
    *,
    catalog: SwarmInstrumentCatalog,
    instruments: Sequence[SwarmInstrument],
    previous: Mapping[SwarmInstrument, BookSnapshot],
    depth: int,
    clock: ClockFunc,
    recorder: _LivePaperRecorder,
    cycle: int,
) -> tuple[BookSnapshot, ...]:
    metadata = catalog.by_instrument()
    snapshots: list[BookSnapshot] = []
    for instrument in instruments:
        item = metadata[instrument]
        request_started_at = _as_utc(clock())
        try:
            orderbook = provider.get_orderbook_snapshot(item.uid, depth=depth)
        except Exception as exc:
            recorder.record_invalid_orderbook(
                cycle=cycle,
                instrument=instrument,
                instrument_uid=item.uid,
                reason=f"{type(exc).__name__}: {exc}",
                bids=0,
                asks=0,
            )
            continue
        received_at = _as_utc(clock())
        latency_ms = int(max((received_at - request_started_at).total_seconds() * 1000, 0))
        if not orderbook.bids or not orderbook.asks:
            recorder.record_invalid_orderbook(
                cycle=cycle,
                instrument=instrument,
                instrument_uid=item.uid,
                reason="EMPTY_ORDERBOOK",
                bids=len(orderbook.bids),
                asks=len(orderbook.asks),
            )
            continue
        snapshots.append(
            orderbook_snapshot_to_book_snapshot(
                orderbook,
                instrument=instrument,
                timestamp=received_at,
                previous=previous.get(instrument),
                latency_ms=latency_ms,
            )
        )
    return tuple(snapshots)


def _predict_for_mode(
    snapshot: BookSnapshot,
    *,
    mode: ExperimentalMode,
    base_config: PairEVModelConfig,
) -> PairEVPrediction:
    return PairEVModel(_model_config_for_mode(base_config, mode)).predict(snapshot)


def _apply_online_entry_gates(
    prediction: PairEVPrediction,
    *,
    snapshot: BookSnapshot,
    mode: ExperimentalMode,
) -> PairEVPrediction:
    del snapshot, mode
    return prediction


def _is_exploration_mode(mode: ExperimentalMode) -> bool:
    return mode is ExperimentalMode.TICK_VELOCITY_PAIR


def _instrument_entry_allowed(
    *,
    snapshots: Sequence[BookSnapshot],
    instrument: SwarmInstrument,
    cycle: int,
) -> bool:
    instruments = {snapshot.instrument for snapshot in snapshots}
    if len(instruments) <= 1:
        return True
    if instrument not in INSTRUMENT_ENTRY_ALLOCATION:
        return True
    return instrument_allowed(
        instrument=instrument,
        cycle=cycle,
        available_count=len(instruments),
    )


def _model_config_for_mode(
    base_config: PairEVModelConfig,
    mode: ExperimentalMode,
) -> PairEVModelConfig:
    if mode is ExperimentalMode.IMBALANCE_PAIR:
        return PairEVModelConfig(
            min_required_ev_ticks=base_config.min_required_ev_ticks,
            max_spread_ticks=base_config.max_spread_ticks,
            max_latency_ms=base_config.max_latency_ms,
            min_abs_imbalance_3=Decimal("0.12"),
            min_abs_microprice_edge_ticks=Decimal("0.03"),
            chop_velocity_ticks_per_second=base_config.chop_velocity_ticks_per_second,
            slippage_stress_ticks=base_config.slippage_stress_ticks,
            execution_error_per_250ms_ticks=base_config.execution_error_per_250ms_ticks,
        )
    if mode is ExperimentalMode.TICK_VELOCITY_PAIR:
        return PairEVModelConfig(
            min_required_ev_ticks=base_config.min_required_ev_ticks,
            max_spread_ticks=base_config.max_spread_ticks,
            max_latency_ms=base_config.max_latency_ms,
            min_abs_imbalance_3=Decimal("0.06"),
            min_abs_microprice_edge_ticks=base_config.min_abs_microprice_edge_ticks,
            chop_velocity_ticks_per_second=Decimal("0.005"),
            slippage_stress_ticks=base_config.slippage_stress_ticks,
            execution_error_per_250ms_ticks=base_config.execution_error_per_250ms_ticks,
        )
    if mode is ExperimentalMode.MICROPRICE_PAIR:
        return PairEVModelConfig(
            min_required_ev_ticks=base_config.min_required_ev_ticks,
            max_spread_ticks=base_config.max_spread_ticks,
            max_latency_ms=base_config.max_latency_ms,
            min_abs_imbalance_3=Decimal("0.08"),
            min_abs_microprice_edge_ticks=Decimal("0.04"),
            chop_velocity_ticks_per_second=base_config.chop_velocity_ticks_per_second,
            slippage_stress_ticks=base_config.slippage_stress_ticks,
            execution_error_per_250ms_ticks=base_config.execution_error_per_250ms_ticks,
        )
    if mode is ExperimentalMode.BOLLINGER_PAIR:
        return PairEVModelConfig(
            min_required_ev_ticks=base_config.min_required_ev_ticks,
            max_spread_ticks=Decimal("3"),
            max_latency_ms=base_config.max_latency_ms,
            min_abs_imbalance_3=base_config.min_abs_imbalance_3,
            min_abs_microprice_edge_ticks=base_config.min_abs_microprice_edge_ticks,
            chop_velocity_ticks_per_second=base_config.chop_velocity_ticks_per_second,
            slippage_stress_ticks=base_config.slippage_stress_ticks,
            execution_error_per_250ms_ticks=base_config.execution_error_per_250ms_ticks,
        )
    return base_config


def _trailing_retrace_ticks(mode: ExperimentalMode) -> Decimal:
    if mode is ExperimentalMode.BOLLINGER_PAIR:
        return Decimal("3")
    return Decimal("2")


def _new_live_pair_state(
    pair: ActivePair,
    mode: ExperimentalMode,
    entry: BookSnapshot,
    *,
    slippage_stress_ticks: Decimal,
    gate_decision: CloneGateDecision | None = None,
) -> _LivePairState:
    slippage = entry.slippage_ticks + slippage_stress_ticks
    return _LivePairState(
        pair=pair,
        mode=mode,
        entry=entry,
        long_entry=entry.best_ask + (slippage * entry.tick_size),
        short_entry=entry.best_bid - (slippage * entry.tick_size),
        stop=Decimal(pair.stop_loss_ticks),
        slippage=slippage,
        gate_decision=gate_decision,
    )


def _update_live_pair_for_snapshot(
    snapshot: BookSnapshot,
    *,
    live_pairs: Mapping[str, _LivePairState],
    config: LivePaperSwarmConfig,
    learning_state: OnlineLearningState | None = None,
) -> tuple[PairLabel, ...]:
    labels: list[PairLabel] = []
    for state in tuple(live_pairs.values()):
        if state.pair.instrument is not snapshot.instrument:
            continue
        label = _update_live_pair(
            state,
            snapshot,
            config=config,
            learning_state=learning_state,
        )
        if label is not None:
            labels.append(label)
    return tuple(labels)


def _update_live_pair(
    state: _LivePairState,
    snapshot: BookSnapshot,
    *,
    config: LivePaperSwarmConfig,
    learning_state: OnlineLearningState | None = None,
) -> PairLabel | None:
    state.latest_snapshot = snapshot
    raw_risk_reason = _risk_exit_reason(snapshot)
    age_seconds = (_as_utc(snapshot.timestamp) - state.entry.timestamp).total_seconds()

    long_pnl = _long_exit_ticks(state, snapshot)
    short_pnl = _short_exit_ticks(state, snapshot)
    pair_total = long_pnl + short_pnl
    state.max_favorable_excursion = max(state.max_favorable_excursion, pair_total)
    state.max_adverse_excursion = min(state.max_adverse_excursion, pair_total)
    risk_reason = _delayed_risk_exit_reason(
        state,
        snapshot,
        raw_risk_reason=raw_risk_reason,
        pair_total=pair_total,
        learning_state=learning_state,
    )
    if age_seconds >= config.max_pair_age_seconds:
        risk_reason = PairExitReason.END_OF_REPLAY

    if risk_reason is not None and state.runner_side is None:
        return _label(
            state,
            snapshot,
            loser_side=None,
            loser_loss_ticks=Decimal("0"),
            runner_side=None,
            runner_mfe_ticks=max(pair_total, Decimal("0")),
            runner_mae_ticks=min(pair_total, Decimal("0")),
            runner_exit_ticks=pair_total,
            pair_total_pnl_ticks=pair_total,
            exit_reason=risk_reason,
            fakeout_flag=pair_total < 0,
            runner_success_flag=False,
        )

    if state.runner_side is None:
        long_trigger = _long_trigger_ticks(state, snapshot)
        short_trigger = _short_trigger_ticks(state, snapshot)
        if long_trigger <= -state.stop and short_trigger <= -state.stop:
            return _label(
                state,
                snapshot,
                loser_side=None,
                loser_loss_ticks=abs(pair_total),
                runner_side=None,
                runner_mfe_ticks=Decimal("0"),
                runner_mae_ticks=pair_total,
                runner_exit_ticks=Decimal("0"),
                pair_total_pnl_ticks=pair_total,
                exit_reason=PairExitReason.FAILED_PAIR,
                fakeout_flag=True,
                runner_success_flag=False,
            )
        if short_trigger <= -state.stop:
            state.loser_side = LegSide.SHORT
            state.runner_side = LegSide.LONG
            state.loser_loss_ticks = abs(short_pnl)
            state.runner_mfe_ticks = max(long_pnl, Decimal("0"))
            state.runner_mae_ticks = min(long_pnl, Decimal("0"))
            return None
        if long_trigger <= -state.stop:
            state.loser_side = LegSide.LONG
            state.runner_side = LegSide.SHORT
            state.loser_loss_ticks = abs(long_pnl)
            state.runner_mfe_ticks = max(short_pnl, Decimal("0"))
            state.runner_mae_ticks = min(short_pnl, Decimal("0"))
            return None
        return None

    runner_pnl = _runner_exit_ticks(state, snapshot)
    state.runner_mfe_ticks = max(state.runner_mfe_ticks, runner_pnl)
    state.runner_mae_ticks = min(state.runner_mae_ticks, runner_pnl)
    if state.runner_mfe_ticks >= TRAIL_LOCK_4_TICKS:
        state.trail_lock_4_active = True
    pair_total = runner_pnl - state.loser_loss_ticks
    state.max_favorable_excursion = max(state.max_favorable_excursion, pair_total)
    state.max_adverse_excursion = min(state.max_adverse_excursion, pair_total)
    if runner_pnl >= Decimal(state.pair.breakeven_ticks):
        state.breakeven_active = True

    exit_reason: PairExitReason | None = risk_reason
    if exit_reason is None and state.breakeven_active and runner_pnl <= 0:
        exit_reason = PairExitReason.BREAKEVEN
        runner_pnl = Decimal("0")
        pair_total = -state.loser_loss_ticks
    if (
        exit_reason is None
        and state.trail_lock_4_active
        and state.runner_mfe_ticks >= TRAIL_LOCK_6_TICKS
        and runner_pnl < TRAIL_LOCK_MIN_PNL_TICKS
    ):
        exit_reason = PairExitReason.TRAILING_STOP
        state.missed_runner_profit_ticks += max(state.runner_mfe_ticks - runner_pnl, Decimal("0"))
    if exit_reason is None and state.runner_mfe_ticks - runner_pnl >= _trailing_retrace_ticks(
        state.mode
    ):
        exit_reason = PairExitReason.TRAILING_STOP
    if exit_reason is None and _microstructure_trailing_exit(snapshot, state.runner_side):
        exit_reason = PairExitReason.TRAILING_STOP

    if exit_reason is None:
        return None
    return _label(
        state,
        snapshot,
        loser_side=state.loser_side,
        loser_loss_ticks=state.loser_loss_ticks,
        runner_side=state.runner_side,
        runner_mfe_ticks=max(state.runner_mfe_ticks, Decimal("0")),
        runner_mae_ticks=min(state.runner_mae_ticks, Decimal("0")),
        runner_exit_ticks=runner_pnl,
        pair_total_pnl_ticks=pair_total,
        exit_reason=exit_reason,
        fakeout_flag=(
            pair_total < 0
            and state.runner_mfe_ticks < Decimal(state.pair.breakeven_ticks)
        ),
        runner_success_flag=runner_pnl >= Decimal(state.pair.breakeven_ticks),
    )


def _label(
    state: _LivePairState,
    snapshot: BookSnapshot,
    *,
    loser_side: LegSide | None,
    loser_loss_ticks: Decimal,
    runner_side: LegSide | None,
    runner_mfe_ticks: Decimal,
    runner_mae_ticks: Decimal,
    runner_exit_ticks: Decimal,
    pair_total_pnl_ticks: Decimal,
    exit_reason: PairExitReason,
    fakeout_flag: bool,
    runner_success_flag: bool,
) -> PairLabel:
    return PairLabel(
        pair_id=state.pair.pair_id,
        instrument=state.pair.instrument,
        entry_timestamp=state.entry.timestamp,
        exit_timestamp=snapshot.timestamp,
        stop_loss_ticks=state.pair.stop_loss_ticks,
        loser_side=loser_side,
        loser_loss_ticks=loser_loss_ticks,
        runner_side=runner_side,
        runner_mfe_ticks=runner_mfe_ticks,
        runner_mae_ticks=runner_mae_ticks,
        runner_exit_ticks=runner_exit_ticks,
        pair_total_pnl_ticks=pair_total_pnl_ticks,
        pair_total_pnl_rub=pair_total_pnl_ticks,
        exit_reason=exit_reason,
        max_adverse_excursion=min(state.max_adverse_excursion, pair_total_pnl_ticks),
        max_favorable_excursion=max(state.max_favorable_excursion, pair_total_pnl_ticks),
        fakeout_flag=fakeout_flag,
        runner_success_flag=runner_success_flag,
        entry_spread_cost_ticks=state.entry.spread_ticks,
        avg_slippage_ticks=state.slippage,
        latency_ms=snapshot.latency_ms,
        regime=state.mode.value,
        long_bot_id=state.pair.long_bot_id,
        short_bot_id=state.pair.short_bot_id,
        avoided_wide_spread_exits=state.avoided_wide_spread_exits,
        missed_runner_profit_ticks=state.missed_runner_profit_ticks,
        trail_lock_4_active=state.trail_lock_4_active,
        wide_spread_pnl_before_wait=state.wide_spread_pnl_before_wait or Decimal("0"),
        wide_spread_pnl_after_wait=pair_total_pnl_ticks,
        panic_wait_helped=state.panic_wait_helped,
        panic_wait_hurt=state.panic_wait_hurt,
    )


def _snapshot_payload(snapshot: BookSnapshot) -> dict[str, object]:
    return {
        "timestamp": snapshot.timestamp,
        "instrument": snapshot.instrument.value,
        "best_bid": snapshot.best_bid,
        "best_ask": snapshot.best_ask,
        "mid_price": snapshot.mid_price,
        "spread_price": snapshot.spread_price,
        "spread_ticks": snapshot.spread_ticks,
        "spread_bps": (snapshot.spread_price / snapshot.mid_price) * Decimal("10000"),
        "microprice": snapshot.microprice,
        "microprice_edge_ticks": snapshot.microprice_edge_ticks,
        "imbalance_1": snapshot.imbalance(1),
        "imbalance_3": snapshot.imbalance(3),
        "imbalance_5": snapshot.imbalance(5),
        "tick_size": snapshot.tick_size,
        "last_price": snapshot.last_price,
        "last_trade_size": snapshot.last_trade_size,
        "trade_side": None if snapshot.trade_side is None else snapshot.trade_side.value,
        "tick_direction": snapshot.tick_direction,
        "tick_velocity": snapshot.tick_velocity,
        "volume_delta_1s": snapshot.volume_delta_1s,
        "volume_delta_5s": snapshot.volume_delta_5s,
        "volume_delta_15s": snapshot.volume_delta_15s,
        "volatility_5s": snapshot.volatility_5s,
        "volatility_15s": snapshot.volatility_15s,
        "volatility_60s": snapshot.volatility_60s,
        "mfi": snapshot.mfi,
        "own_orders": snapshot.own_orders,
        "own_executions": snapshot.own_executions,
        "latency_ms": snapshot.latency_ms,
        "slippage_ticks": snapshot.slippage_ticks,
        "bid_levels": [
            {"price": level.price, "quantity": level.quantity}
            for level in snapshot.bid_levels
        ],
        "ask_levels": [
            {"price": level.price, "quantity": level.quantity}
            for level in snapshot.ask_levels
        ],
        "feature_row": snapshot.model_feature_row(),
    }


def _prediction_payload(prediction: PairEVPrediction) -> dict[str, object]:
    return {
        "instrument": prediction.instrument.value,
        "pair_ev_ticks": prediction.pair_ev_ticks,
        "probability_pair_profit": prediction.probability_pair_profit,
        "expected_runner_mfe_ticks": prediction.expected_runner_mfe_ticks,
        "probability_fakeout": prediction.probability_fakeout,
        "best_stop_loss_ticks": prediction.best_stop_loss_ticks,
        "best_instrument": prediction.best_instrument.value,
        "trade_allowed": prediction.trade_allowed,
        "p_up_runner": prediction.p_up_runner,
        "p_down_runner": prediction.p_down_runner,
        "avg_long_runner_profit_ticks": prediction.avg_long_runner_profit_ticks,
        "avg_short_runner_profit_ticks": prediction.avg_short_runner_profit_ticks,
        "avg_fakeout_loss_ticks": prediction.avg_fakeout_loss_ticks,
        "avg_spread_cost_ticks": prediction.avg_spread_cost_ticks,
        "avg_slippage_ticks": prediction.avg_slippage_ticks,
        "avg_execution_error_ticks": prediction.avg_execution_error_ticks,
        "reason_codes": list(prediction.reason_codes),
        "rejection_reason": None
        if prediction.rejection_reason is None
        else prediction.rejection_reason.value,
    }


def _active_pair_payload(pair: ActivePair) -> dict[str, object]:
    return {
        "logic_source": LOGIC_SOURCE,
        "architecture": ARCHITECTURE,
        "pair_id": pair.pair_id,
        "instrument": pair.instrument.value,
        "long_bot_id": pair.long_bot_id,
        "short_bot_id": pair.short_bot_id,
        "opened_at": pair.opened_at,
        "stop_loss_ticks": pair.stop_loss_ticks,
        "breakeven_ticks": pair.breakeven_ticks,
        "status": pair.status.value,
        "model_ev_ticks": pair.model_ev_ticks,
        "entry_reason_codes": list(pair.entry_reason_codes),
    }


def _live_pair_state_payload(
    state: _LivePairState,
    snapshot: BookSnapshot,
) -> dict[str, object]:
    long_pnl = _long_exit_ticks(state, snapshot)
    short_pnl = _short_exit_ticks(state, snapshot)
    runner_pnl: Decimal | None = None
    if state.runner_side is not None:
        runner_pnl = _runner_exit_ticks(state, snapshot)
    pair_total = (
        long_pnl + short_pnl
        if runner_pnl is None
        else runner_pnl - state.loser_loss_ticks
    )
    risk_exit_reason = _risk_exit_reason(snapshot)
    return {
        "pair_id": state.pair.pair_id,
        "instrument": state.pair.instrument.value,
        "mode": state.mode.value,
        "entry_timestamp": state.entry.timestamp,
        "current_timestamp": snapshot.timestamp,
        "age_seconds": (_as_utc(snapshot.timestamp) - state.entry.timestamp).total_seconds(),
        "long_bot_id": state.pair.long_bot_id,
        "short_bot_id": state.pair.short_bot_id,
        "long_entry": state.long_entry,
        "short_entry": state.short_entry,
        "current_best_bid": snapshot.best_bid,
        "current_best_ask": snapshot.best_ask,
        "long_pnl_ticks": long_pnl,
        "short_pnl_ticks": short_pnl,
        "pair_total_pnl_ticks": pair_total,
        "stop_loss_ticks": state.stop,
        "breakeven_ticks": state.pair.breakeven_ticks,
        "trailing_retrace_ticks": _trailing_retrace_ticks(state.mode),
        "slippage_ticks": state.slippage,
        "loser_side": None if state.loser_side is None else state.loser_side.value,
        "runner_side": None if state.runner_side is None else state.runner_side.value,
        "loser_loss_ticks": state.loser_loss_ticks,
        "runner_pnl_ticks": runner_pnl,
        "runner_mfe_ticks": _none_if_uninitialized(state.runner_mfe_ticks, Decimal("-999999")),
        "runner_mae_ticks": _none_if_uninitialized(state.runner_mae_ticks, Decimal("999999")),
        "breakeven_active": state.breakeven_active,
        "wide_spread_panic_cycles": state.wide_spread_panic_cycles,
        "wide_spread_pnl_before_wait": state.wide_spread_pnl_before_wait,
        "wide_spread_last_spread_ticks": state.wide_spread_last_spread_ticks,
        "wide_spread_max_spread_ticks": state.wide_spread_max_spread_ticks,
        "panic_wait_helped": state.panic_wait_helped,
        "panic_wait_hurt": state.panic_wait_hurt,
        "avoided_wide_spread_exits": state.avoided_wide_spread_exits,
        "trail_lock_4_active": state.trail_lock_4_active,
        "missed_runner_profit_ticks": state.missed_runner_profit_ticks,
        "max_adverse_excursion": state.max_adverse_excursion,
        "max_favorable_excursion": state.max_favorable_excursion,
        "risk_exit_reason": None if risk_exit_reason is None else risk_exit_reason.value,
        "microstructure_trailing_exit": _microstructure_trailing_exit(
            snapshot,
            state.runner_side,
        ),
    }


def _label_payload(
    label: PairLabel,
    *,
    state: _LivePairState | None = None,
) -> dict[str, object]:
    enrichment = label_enrichment(
        label=label,
        long_entry_price=None if state is None else state.long_entry,
        short_entry_price=None if state is None else state.short_entry,
        entry_bid=None if state is None else state.entry.best_bid,
        entry_ask=None if state is None else state.entry.best_ask,
        gate_decision=None if state is None else state.gate_decision,
    )
    return {
        **enrichment,
        "pair_id": label.pair_id,
        "instrument": label.instrument.value,
        "mode": label.regime,
        "entry_timestamp": label.entry_timestamp,
        "exit_timestamp": label.exit_timestamp,
        "stop_loss_ticks": label.stop_loss_ticks,
        "loser_side": None if label.loser_side is None else label.loser_side.value,
        "loser_loss_ticks": label.loser_loss_ticks,
        "runner_side": None if label.runner_side is None else label.runner_side.value,
        "runner_mfe_ticks": label.runner_mfe_ticks,
        "runner_mae_ticks": label.runner_mae_ticks,
        "runner_reached_breakeven": (
            label.runner_success_flag or label.runner_mfe_ticks >= Decimal(label.stop_loss_ticks)
        ),
        "runner_exit_ticks": label.runner_exit_ticks,
        "pair_pnl_ticks": label.pair_total_pnl_ticks,
        "pair_total_pnl_ticks": label.pair_total_pnl_ticks,
        "pair_total_pnl_rub": label.pair_total_pnl_rub,
        "exit_reason": label.exit_reason.value,
        "max_adverse_excursion": label.max_adverse_excursion,
        "max_favorable_excursion": label.max_favorable_excursion,
        "fakeout_flag": label.fakeout_flag,
        "runner_success_flag": label.runner_success_flag,
        "entry_spread_cost_ticks": label.entry_spread_cost_ticks,
        "avg_slippage_ticks": label.avg_slippage_ticks,
        "latency_ms": label.latency_ms,
        "regime": label.regime,
        "long_bot_id": label.long_bot_id,
        "short_bot_id": label.short_bot_id,
        "avoided_wide_spread_exits": label.avoided_wide_spread_exits,
        "missed_runner_profit_ticks": label.missed_runner_profit_ticks,
        "trail_lock_4_active": label.trail_lock_4_active,
        "wide_spread_pnl_before_wait": label.wide_spread_pnl_before_wait,
        "wide_spread_pnl_after_wait": label.wide_spread_pnl_after_wait,
        "panic_wait_helped": label.panic_wait_helped,
        "panic_wait_hurt": label.panic_wait_hurt,
    }


def _metrics_payload(metrics: SwarmMetrics) -> dict[str, object]:
    return {
        "total_pairs": metrics.total_pairs,
        "profitable_pairs": metrics.profitable_pairs,
        "losing_pairs": metrics.losing_pairs,
        "pair_winrate": metrics.pair_winrate,
        "pair_ev_ticks": metrics.pair_ev_ticks,
        "pair_ev_rub": metrics.pair_ev_rub,
        "avg_runner_profit_ticks": metrics.avg_runner_profit_ticks,
        "avg_loser_loss_ticks": metrics.avg_loser_loss_ticks,
        "avg_spread_cost_ticks": metrics.avg_spread_cost_ticks,
        "avg_slippage_ticks": metrics.avg_slippage_ticks,
        "fakeout_rate": metrics.fakeout_rate,
        "runner_to_breakeven_rate": metrics.runner_to_breakeven_rate,
        "runner_trailing_capture_ratio": metrics.runner_trailing_capture_ratio,
        "profit_factor": metrics.profit_factor,
        "max_drawdown": metrics.max_drawdown,
        "pnl_by_bot": metrics.pnl_by_bot,
        "pnl_by_account": {
            _masked_account_ref(key): value for key, value in metrics.pnl_by_account.items()
        },
        "pnl_by_instrument": metrics.pnl_by_instrument,
        "pnl_by_regime": metrics.pnl_by_regime,
        "rejected_by_model": metrics.rejected_by_model,
        "rejected_by_spread": metrics.rejected_by_spread,
        "rejected_by_spread_entry_gate": metrics.rejected_by_spread_entry_gate,
        "rejected_by_stale_book": metrics.rejected_by_stale_book,
        "rejected_by_chop": metrics.rejected_by_chop,
        "rejected_by_latency": metrics.rejected_by_latency,
    }


def _none_if_uninitialized(value: Decimal, sentinel: Decimal) -> Decimal | None:
    return None if value == sentinel else value


def _metrics(labels: Sequence[PairLabel], curator: CuratorBot) -> SwarmMetrics:
    return aggregate_pair_metrics(
        labels,
        bot_to_account=curator.bot_to_account,
        rejected_by_model=curator.rejections.get(RejectionReason.MODEL, 0),
        rejected_by_spread=curator.rejections.get(RejectionReason.SPREAD, 0),
        rejected_by_spread_entry_gate=curator.rejections.get(
            RejectionReason.SPREAD_ENTRY_GATE,
            0,
        ),
        rejected_by_stale_book=curator.rejections.get(RejectionReason.STALE_BOOK, 0),
        rejected_by_chop=curator.rejections.get(RejectionReason.CHOP, 0),
        rejected_by_latency=curator.rejections.get(RejectionReason.LATENCY, 0),
    )


def _has_active_pair_for_instrument(
    live_pairs: Mapping[str, _LivePairState],
    instrument: SwarmInstrument,
) -> bool:
    return any(state.pair.instrument is instrument for state in live_pairs.values())


def _delayed_risk_exit_reason(
    state: _LivePairState,
    snapshot: BookSnapshot,
    *,
    raw_risk_reason: PairExitReason | None,
    pair_total: Decimal,
    learning_state: OnlineLearningState | None,
) -> PairExitReason | None:
    if raw_risk_reason is not PairExitReason.WIDE_SPREAD:
        if state.wide_spread_panic_cycles:
            before = state.wide_spread_pnl_before_wait or pair_total
            if pair_total >= before:
                state.panic_wait_helped = True
            else:
                state.panic_wait_hurt = True
            state.avoided_wide_spread_exits += 1
        state.wide_spread_panic_cycles = 0
        state.wide_spread_pnl_before_wait = None
        state.wide_spread_last_spread_ticks = None
        state.wide_spread_max_spread_ticks = Decimal("0")
        return raw_risk_reason

    if learning_state is not None and learning_state.panic_wait_disabled:
        return PairExitReason.WIDE_SPREAD

    runner_pnl = _runner_exit_ticks(state, snapshot) if state.runner_side is not None else None
    current_pnl = runner_pnl if runner_pnl is not None else pair_total
    previous_spread = state.wide_spread_last_spread_ticks
    spread_expanded = previous_spread is not None and snapshot.spread_ticks > previous_spread
    spread_narrowed = previous_spread is not None and snapshot.spread_ticks < previous_spread

    if state.wide_spread_panic_cycles == 0:
        state.wide_spread_pnl_before_wait = current_pnl
        state.wide_spread_max_spread_ticks = snapshot.spread_ticks

    state.wide_spread_panic_cycles += 1
    state.wide_spread_last_spread_ticks = snapshot.spread_ticks

    if runner_pnl is not None and runner_pnl > 0 and spread_expanded:
        state.panic_wait_helped = (
            state.wide_spread_pnl_before_wait is None
            or runner_pnl >= state.wide_spread_pnl_before_wait
        )
        state.panic_wait_hurt = not state.panic_wait_helped
        return PairExitReason.WIDE_SPREAD
    if spread_expanded and snapshot.spread_ticks > state.wide_spread_max_spread_ticks:
        state.wide_spread_max_spread_ticks = snapshot.spread_ticks
        state.panic_wait_hurt = (
            state.wide_spread_pnl_before_wait is not None
            and current_pnl < state.wide_spread_pnl_before_wait
        )
        return PairExitReason.WIDE_SPREAD
    if _price_against_position(state, snapshot, pair_total=pair_total):
        state.panic_wait_hurt = (
            state.wide_spread_pnl_before_wait is not None
            and current_pnl < state.wide_spread_pnl_before_wait
        )
        return PairExitReason.WIDE_SPREAD
    if spread_narrowed:
        state.avoided_wide_spread_exits += 1
        state.panic_wait_helped = (
            state.wide_spread_pnl_before_wait is not None
            and current_pnl >= state.wide_spread_pnl_before_wait
        )
        return None
    return None


def _price_against_position(
    state: _LivePairState,
    snapshot: BookSnapshot,
    *,
    pair_total: Decimal,
) -> bool:
    if state.runner_side is None:
        return pair_total < 0
    return _runner_exit_ticks(state, snapshot) < 0


def _record_live_pair_control_events(
    *,
    recorder: _LivePaperRecorder,
    cycle: int,
    state: _LivePairState,
    snapshot: BookSnapshot,
) -> None:
    if state.loser_side is not None and not state.logged_loser_stop:
        loser_bot_id = (
            state.pair.long_bot_id
            if state.loser_side is LegSide.LONG
            else state.pair.short_bot_id
        )
        recorder.record_control_event(
            cycle=cycle,
            event_type="LOSER_STOP",
            state=state,
            snapshot=snapshot,
            payload={
                "event": "LOSER_STOP",
                "bot_id": loser_bot_id,
                "side": state.loser_side.value,
                "loser_loss_ticks": state.loser_loss_ticks,
                "paper": True,
                "live": False,
            },
        )
        state.logged_loser_stop = True
    if state.runner_side is not None and not state.logged_runner_breakeven:
        runner_bot_id = (
            state.pair.long_bot_id
            if state.runner_side is LegSide.LONG
            else state.pair.short_bot_id
        )
        recorder.record_control_event(
            cycle=cycle,
            event_type="RUNNER_BREAKEVEN",
            state=state,
            snapshot=snapshot,
            payload={
                "event": "RUNNER_BREAKEVEN",
                "bot_id": runner_bot_id,
                "side": state.runner_side.value,
                "breakeven_ticks": state.pair.breakeven_ticks,
                "paper": True,
                "live": False,
            },
        )
        state.logged_runner_breakeven = True
    if state.runner_side is not None and not state.logged_runner_trail:
        runner_bot_id = (
            state.pair.long_bot_id
            if state.runner_side is LegSide.LONG
            else state.pair.short_bot_id
        )
        recorder.record_control_event(
            cycle=cycle,
            event_type="RUNNER_TRAIL",
            state=state,
            snapshot=snapshot,
            payload={
                "event": "RUNNER_TRAIL",
                "bot_id": runner_bot_id,
                "side": state.runner_side.value,
                "runner_mfe_ticks": state.runner_mfe_ticks,
                "runner_mae_ticks": state.runner_mae_ticks,
                "trail_profile": "TRAIL_WIDE"
                if state.pair.instrument is SwarmInstrument.NEOEFIR
                else "DEFAULT",
                "paper": True,
                "live": False,
            },
        )
        state.logged_runner_trail = True
    if state.avoided_wide_spread_exits > state.logged_avoided_wide_spread_exits:
        recorder.record_control_event(
            cycle=cycle,
            event_type="AVOIDED_WIDE_SPREAD_EXIT",
            state=state,
            snapshot=snapshot,
            payload={
                "avoided_wide_spread_exit": True,
                "avoided_wide_spread_exits": state.avoided_wide_spread_exits,
                "spread_ticks": snapshot.spread_ticks,
            },
        )
        state.logged_avoided_wide_spread_exits = state.avoided_wide_spread_exits
    if state.missed_runner_profit_ticks > state.logged_missed_runner_profit_ticks:
        recorder.record_control_event(
            cycle=cycle,
            event_type="MISSED_RUNNER_PROFIT",
            state=state,
            snapshot=snapshot,
            payload={
                "missed_runner_profit": True,
                "missed_runner_profit_ticks": state.missed_runner_profit_ticks,
                "runner_mfe_ticks": state.runner_mfe_ticks,
                "runner_pnl_ticks": _runner_exit_ticks(state, snapshot),
            },
        )
        state.logged_missed_runner_profit_ticks = state.missed_runner_profit_ticks


def _risk_exit_reason(snapshot: BookSnapshot) -> PairExitReason | None:
    cfg = HedgePairSimulationConfig()
    if snapshot.spread_ticks > cfg.max_spread_ticks:
        return PairExitReason.WIDE_SPREAD
    if snapshot.latency_ms > cfg.max_latency_ms:
        return PairExitReason.STALE_BOOK
    if (
        abs(snapshot.imbalance(3)) < Decimal("0.01")
        and abs(snapshot.tick_velocity) < Decimal("0.01")
    ):
        return PairExitReason.CHOP
    return None


def _long_exit_ticks(state: _LivePairState, snapshot: BookSnapshot) -> Decimal:
    exit_price = snapshot.best_bid - (state.slippage * snapshot.tick_size)
    return (exit_price - state.long_entry) / snapshot.tick_size


def _long_trigger_ticks(state: _LivePairState, snapshot: BookSnapshot) -> Decimal:
    return (snapshot.best_bid - state.long_entry) / snapshot.tick_size


def _short_exit_ticks(state: _LivePairState, snapshot: BookSnapshot) -> Decimal:
    exit_price = snapshot.best_ask + (state.slippage * snapshot.tick_size)
    return (state.short_entry - exit_price) / snapshot.tick_size


def _short_trigger_ticks(state: _LivePairState, snapshot: BookSnapshot) -> Decimal:
    return (state.short_entry - snapshot.best_ask) / snapshot.tick_size


def _runner_exit_ticks(state: _LivePairState, snapshot: BookSnapshot) -> Decimal:
    if state.runner_side is LegSide.LONG:
        return _long_exit_ticks(state, snapshot)
    return _short_exit_ticks(state, snapshot)


def _microstructure_trailing_exit(snapshot: BookSnapshot, runner_side: LegSide | None) -> bool:
    if runner_side is LegSide.LONG:
        return snapshot.imbalance(1) < Decimal("-0.35") or snapshot.microprice < snapshot.mid_price
    if runner_side is LegSide.SHORT:
        return snapshot.imbalance(1) > Decimal("0.35") or snapshot.microprice > snapshot.mid_price
    return False


def _write_live_dashboard_state(
    path: Path,
    *,
    curator: CuratorBot,
    latest_snapshots: Sequence[BookSnapshot],
    metrics: SwarmMetrics,
    updated_at: datetime,
    learning_state: OnlineLearningState,
    predictions: Mapping[str, PairEVPrediction],
) -> Path:
    payload = build_swarm_dashboard_state(
        curator=curator,
        latest_snapshots=latest_snapshots,
        metrics=metrics,
        predictions=predictions,
        updated_at=updated_at,
        commit_hash=get_runtime_commit_hash(),
    )
    payload["runtime_mode"] = "live-paper"
    payload["market_data_source"] = "tbank-readonly-rest"
    payload["paper_enabled"] = True
    payload["live_enabled"] = False
    payload["logic_source"] = LOGIC_SOURCE
    payload["architecture"] = ARCHITECTURE
    payload["first_bot_logic_clone_enabled"] = FIRST_BOT_LOGIC_CLONE_ENABLED
    payload["legacy_universal_logic_enabled"] = LEGACY_UNIVERSAL_LOGIC_ENABLED
    learning_payload = learning_state.to_payload()
    payload["online_learning"] = learning_payload
    swarm = payload.get("swarm")
    if isinstance(swarm, dict):
        swarm["paper_enabled"] = True
        swarm["live_enabled"] = False
        swarm["logic_source"] = LOGIC_SOURCE
        swarm["architecture"] = ARCHITECTURE
        swarm["first_bot_logic_clone_enabled"] = FIRST_BOT_LOGIC_CLONE_ENABLED
        swarm["legacy_universal_logic_enabled"] = LEGACY_UNIVERSAL_LOGIC_ENABLED
        curator_payload = swarm.get("curator")
        if isinstance(curator_payload, dict):
            curator_payload["status"] = "LIVE_PAPER_COORDINATOR"
        swarm["online_learning"] = learning_payload
        swarm["active_mode"] = learning_payload["active_mode"]
        swarm["mode_allocation"] = learning_payload["mode_allocation"]
        swarm["ev_by_mode"] = learning_payload["ev_by_mode"]
        swarm["ev_by_instrument"] = learning_payload["ev_by_instrument"]
        swarm["fakeout_rate_by_mode"] = learning_payload["fakeout_rate_by_mode"]
        swarm["runner_to_breakeven_by_mode"] = learning_payload[
            "runner_to_breakeven_by_mode"
        ]
        swarm["bot_utilization"] = _bot_utilization_payload(curator, learning_state)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        tmp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()
    return path


def _write_summary_json(
    path: Path,
    *,
    cycle: int,
    metrics: SwarmMetrics,
    active_pairs: Mapping[str, _LivePairState],
    latest_snapshots: Mapping[SwarmInstrument, BookSnapshot],
    learning_state: OnlineLearningState,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "commit_hash": get_runtime_commit_hash(),
        "cycle": cycle,
        "runtime_mode": "live-paper",
        "market_data_source": "tbank-readonly-rest",
        "logic_source": LOGIC_SOURCE,
        "architecture": ARCHITECTURE,
        "first_bot_logic_clone_enabled": FIRST_BOT_LOGIC_CLONE_ENABLED,
        "legacy_universal_logic_enabled": LEGACY_UNIVERSAL_LOGIC_ENABLED,
        "paper_enabled": True,
        "live_enabled": False,
        "online_learning": learning_state.to_payload(),
        "active_pairs": len(active_pairs),
        "closed_pairs": metrics.total_pairs,
        "total_pnl_ticks": str(sum(metrics.pnl_by_bot.values(), Decimal("0"))),
        "latest_market": {
            instrument.value: {
                "timestamp": snapshot.timestamp.isoformat(),
                "best_bid": str(snapshot.best_bid),
                "best_ask": str(snapshot.best_ask),
                "spread_ticks": str(snapshot.spread_ticks),
                "imbalance_3": str(snapshot.imbalance(3)),
            }
            for instrument, snapshot in latest_snapshots.items()
        },
    }
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _load_online_learning_state(reports_dir: Path) -> OnlineLearningState:
    state = OnlineLearningState()
    labels_path = reports_dir / "pair_labels.jsonl"
    if not labels_path.exists():
        return state

    with labels_path.open("r", encoding="utf-8") as file:
        for line in file:
            raw_line = line.strip()
            if not raw_line:
                continue
            try:
                record = json.loads(raw_line)
                if not isinstance(record, dict):
                    continue
                label_payload = record.get("label")
                if not isinstance(label_payload, dict):
                    label_payload = record
                mode_value = (
                    record.get("mode")
                    or label_payload.get("mode")
                    or label_payload.get("regime")
                )
                mode = ExperimentalMode(str(mode_value))
                label = _label_from_record(label_payload)
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                continue
            state.update_label(instrument=label.instrument, mode=mode, label=label)
            if label.long_bot_id is not None and label.short_bot_id is not None:
                state.record_assignment(label.long_bot_id, label.short_bot_id)
    return state


def _label_from_record(payload: Mapping[str, object]) -> PairLabel:
    return PairLabel(
        pair_id=str(payload["pair_id"]),
        instrument=SwarmInstrument(str(payload["instrument"])),
        entry_timestamp=_record_datetime(payload, "entry_timestamp"),
        exit_timestamp=_record_datetime(payload, "exit_timestamp"),
        stop_loss_ticks=_record_int(payload, "stop_loss_ticks", default=2),
        loser_side=_record_side(payload.get("loser_side")),
        loser_loss_ticks=_record_decimal(payload, "loser_loss_ticks"),
        runner_side=_record_side(payload.get("runner_side")),
        runner_mfe_ticks=_record_decimal(payload, "runner_mfe_ticks"),
        runner_mae_ticks=_record_decimal(payload, "runner_mae_ticks"),
        runner_exit_ticks=_record_decimal(payload, "runner_exit_ticks"),
        pair_total_pnl_ticks=_record_decimal(
            payload,
            "pair_total_pnl_ticks",
            fallback_key="pair_pnl_ticks",
        ),
        pair_total_pnl_rub=_record_decimal(payload, "pair_total_pnl_rub"),
        exit_reason=PairExitReason(str(payload["exit_reason"])),
        max_adverse_excursion=_record_decimal(payload, "max_adverse_excursion"),
        max_favorable_excursion=_record_decimal(payload, "max_favorable_excursion"),
        fakeout_flag=_record_bool(payload.get("fakeout_flag")),
        runner_success_flag=_record_bool(
            payload.get("runner_success_flag", payload.get("runner_reached_breakeven"))
        ),
        entry_spread_cost_ticks=_record_decimal(payload, "entry_spread_cost_ticks"),
        avg_slippage_ticks=_record_decimal(payload, "avg_slippage_ticks"),
        latency_ms=_record_int(payload, "latency_ms", default=0),
        regime=str(payload.get("mode") or payload.get("regime") or "replayed"),
        long_bot_id=_record_optional_str(payload.get("long_bot_id")),
        short_bot_id=_record_optional_str(payload.get("short_bot_id")),
        avoided_wide_spread_exits=_record_int(
            payload,
            "avoided_wide_spread_exits",
            default=0,
        ),
        missed_runner_profit_ticks=_record_decimal(
            payload,
            "missed_runner_profit_ticks",
        ),
        trail_lock_4_active=_record_bool(payload.get("trail_lock_4_active")),
        wide_spread_pnl_before_wait=_record_decimal(
            payload,
            "wide_spread_pnl_before_wait",
        ),
        wide_spread_pnl_after_wait=_record_decimal(
            payload,
            "wide_spread_pnl_after_wait",
        ),
        panic_wait_helped=_record_bool(payload.get("panic_wait_helped")),
        panic_wait_hurt=_record_bool(payload.get("panic_wait_hurt")),
    )


def _record_decimal(
    payload: Mapping[str, object],
    key: str,
    *,
    fallback_key: str | None = None,
) -> Decimal:
    value = payload.get(key)
    if value is None and fallback_key is not None:
        value = payload.get(fallback_key)
    if value is None:
        return Decimal("0")
    return Decimal(str(value))


def _record_int(payload: Mapping[str, object], key: str, *, default: int) -> int:
    value = payload.get(key)
    if value is None:
        return default
    return int(str(value))


def _record_datetime(payload: Mapping[str, object], key: str) -> datetime:
    value = payload[key]
    if isinstance(value, datetime):
        return _as_utc(value)
    return _as_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))


def _record_side(value: object) -> LegSide | None:
    if value is None:
        return None
    return LegSide(str(value))


def _record_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes"}


def _record_optional_str(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _write_online_learning_state(
    path: Path,
    learning_state: OnlineLearningState,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        tmp_path.write_text(
            json.dumps(
                _json_safe_mapping(learning_state.to_payload()),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def _bot_utilization_payload(
    curator: CuratorBot,
    learning_state: OnlineLearningState,
) -> dict[str, object]:
    return {
        bot_id: {
            "state": bot.state.value,
            "assigned_pair_id": bot.assigned_pair_id,
            "assignment_count": learning_state.bot_assignment_counts.get(bot_id, 0),
            "realized_pnl_ticks": str(bot.realized_pnl_ticks),
            "paper_enabled": bot.config.paper_enabled,
            "live_enabled": bot.config.live_enabled,
        }
        for bot_id, bot in curator.bots.items()
    }


def _write_heartbeat(path: Path, cycle: LivePaperSwarmCycle) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "mode=live-paper",
                "market_data_source=tbank-readonly-rest",
                f"status={cycle.status}",
                f"cycle={cycle.cycle}",
                f"started_at={cycle.started_at.isoformat()}",
                f"finished_at={cycle.finished_at.isoformat()}",
                f"snapshots={cycle.snapshots}",
                f"active_pairs={cycle.active_pairs}",
                f"closed_pairs={cycle.closed_pairs}",
                f"total_pnl_ticks={cycle.total_pnl_ticks}",
                f"error={cycle.error or ''}",
            ]
        ),
        encoding="utf-8",
    )


def _write_error_log(reports_dir: Path, exc: Exception) -> None:
    reports_dir.mkdir(parents=True, exist_ok=True)
    error_path = reports_dir / "live_paper_error.log"
    with error_path.open("a", encoding="utf-8") as file:
        file.write(
            f"{datetime.now(UTC).isoformat()} {type(exc).__name__}: {exc}\n"
            f"{traceback.format_exc()}\n"
        )


def _assert_safe_environment() -> None:
    live_flags = {
        "LIVE_TRADING_ENABLED": os.environ.get("LIVE_TRADING_ENABLED", "false"),
        "NEO_TRADER_LIVE_TRADING_ENABLED": os.environ.get(
            "NEO_TRADER_LIVE_TRADING_ENABLED",
            "false",
        ),
    }
    enabled = [key for key, value in live_flags.items() if value.strip().lower() == "true"]
    if enabled:
        joined = ", ".join(enabled)
        raise RuntimeError(f"live paper swarm never submits orders; live flags enabled: {joined}")

    trading_mode = os.environ.get("TRADING_MODE", "readonly").strip().lower()
    neo_trading_mode = os.environ.get("NEO_TRADER_TRADING_MODE", "readonly").strip().lower()
    if trading_mode != "readonly" or neo_trading_mode != "readonly":
        raise RuntimeError(
            "live paper swarm requires TRADING_MODE and NEO_TRADER_TRADING_MODE readonly."
        )


def _as_utc(value: datetime) -> datetime:
    return as_utc(value)


def _masked_account_ref(value: str) -> str:
    if value.startswith("PAPER_ACCOUNT_"):
        return value
    if len(value) <= 8:
        return "***"
    return f"{value[:4]}...{value[-4:]}"


def _json_safe_mapping(raw: Mapping[str, object]) -> dict[str, object]:
    return {str(key): _json_safe_value(value) for key, value in raw.items()}


def _json_safe_value(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return _as_utc(value).isoformat()
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _json_safe_value(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [_json_safe_value(item) for item in value]
    return value


__all__ = [
    "LivePaperSwarmConfig",
    "LivePaperSwarmCycle",
    "OrderBookProvider",
    "orderbook_snapshot_to_book_snapshot",
    "run_live_paper_swarm",
]
