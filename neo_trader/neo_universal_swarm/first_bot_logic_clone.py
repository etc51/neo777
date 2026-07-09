"""First-bot trading logic adapter for the universal swarm.

The reference bot in ``etc51/pervii`` uses fixed long-only and short-only
account groups. This adapter carries over its pair modes, allocation schedule,
bid/ask accounting fields, and paper-only runtime flags while leaving bot
selection to the target swarm's ten universal bots.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Final

from neo_trader.neo_universal_swarm.model import PairEVPrediction
from neo_trader.neo_universal_swarm.types import (
    BookSnapshot,
    LegSide,
    PairLabel,
    SwarmInstrument,
)

FIRST_BOT_LOGIC_CLONE_ENABLED: Final = True
LEGACY_UNIVERSAL_LOGIC_ENABLED: Final = False
STRICT_FIRST_BOT_CADENCE_ENABLED: Final = True
LOGIC_SOURCE: Final = "FIRST_BOT_CLONE"
ARCHITECTURE: Final = "UNIVERSAL_10_BOTS"
DECISION_INTERVAL_MINUTES: Final = 15
MODE_INSTRUMENT_COOLDOWN_MINUTES: Final = 15
MAX_WIDE_SPREAD_RATE: Final = Decimal("0.20")
MAX_RUNNER_SIDE_NONE_RATE: Final = Decimal("0.20")

PAIR_EVENT_NAMES: Final[frozenset[str]] = frozenset(
    {
        "PAIR_OPEN_REQUEST",
        "PAIR_OPEN",
        "LONG_LEG_OPEN",
        "SHORT_LEG_OPEN",
        "LOSER_STOP",
        "RUNNER_BREAKEVEN",
        "RUNNER_TRAIL",
        "PAIR_CLOSE",
        "PAIR_FAILED",
    }
)

EXPERIMENTAL_MODES: Final[tuple[str, ...]] = (
    "BASELINE_PAIR",
    "BOLLINGER_PAIR",
    "IMBALANCE_PAIR",
    "MICROPRICE_PAIR",
    "TICK_VELOCITY_PAIR",
)

MODE_ALLOCATION_SLOTS: Final[tuple[str, ...]] = (
    "BASELINE_PAIR",
    "BASELINE_PAIR",
    "BASELINE_PAIR",
    "BASELINE_PAIR",
    "BASELINE_PAIR",
    "BOLLINGER_PAIR",
    "BOLLINGER_PAIR",
    "IMBALANCE_PAIR",
    "MICROPRICE_PAIR",
    "TICK_VELOCITY_PAIR",
)

MODE_ALLOCATION: Final[dict[str, Decimal]] = {
    "BASELINE_PAIR": Decimal("0.45"),
    "BOLLINGER_PAIR": Decimal("0.20"),
    "IMBALANCE_PAIR": Decimal("0.15"),
    "MICROPRICE_PAIR": Decimal("0.15"),
    "TICK_VELOCITY_PAIR": Decimal("0.05"),
}

INSTRUMENT_ALLOCATION_SLOTS: Final[tuple[SwarmInstrument, ...]] = (
    SwarmInstrument.NEOBITOK,
    SwarmInstrument.NEOBITOK,
    SwarmInstrument.NEOBITOK,
    SwarmInstrument.NEOBITOK,
    SwarmInstrument.NEOBITOK,
    SwarmInstrument.NEOBITOK,
    SwarmInstrument.NEOBITOK,
    SwarmInstrument.NEOBITOK,
    SwarmInstrument.NEOEFIR,
    SwarmInstrument.NEOEFIR,
)

INSTRUMENT_ALLOCATION: Final[dict[SwarmInstrument, Decimal]] = {
    SwarmInstrument.NEOBITOK: Decimal("0.80"),
    SwarmInstrument.NEOEFIR: Decimal("0.20"),
}
ENTRY_SPREAD_LIMIT_TICKS: Final[dict[SwarmInstrument, Decimal]] = {
    SwarmInstrument.NEOBITOK: Decimal("1"),
    SwarmInstrument.NEOEFIR: Decimal("1"),
}


@dataclass(frozen=True)
class CloneGateDecision:
    allowed: bool
    matched_first_bot_rule: bool
    clone_rule_missing: bool
    why_entry_allowed: str
    first_bot_rule: str
    entry_interval_id: str
    duplicate_guard_status: str
    cooldown_status: str
    spread_gate_status: str
    cadence_gate_status: str
    rejection_reason: str | None = None

    def to_payload(self) -> dict[str, object]:
        return {
            "strict_first_bot_cadence": STRICT_FIRST_BOT_CADENCE_ENABLED,
            "allowed": self.allowed,
            "why_entry_allowed": self.why_entry_allowed,
            "which_first_bot_rule_allowed_it": self.first_bot_rule,
            "matched_first_bot_rule": self.matched_first_bot_rule,
            "entry_interval_id": self.entry_interval_id,
            "duplicate_guard_status": self.duplicate_guard_status,
            "cooldown_status": self.cooldown_status,
            "spread_gate_status": self.spread_gate_status,
            "cadence_gate_status": self.cadence_gate_status,
            "clone_rule_missing": self.clone_rule_missing,
            "rejection_reason": self.rejection_reason,
        }


class FirstBotCloneGate:
    """Persistent entry gate that mirrors the first bot's completed-bar cadence."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.used_intervals: set[str] = set()
        self.last_mode_instrument_open: dict[str, datetime] = {}
        self._load()

    def evaluate_pre_mode(
        self,
        *,
        snapshot: BookSnapshot,
        cycle: int,
        available_count: int,
        has_active_pair: bool,
    ) -> CloneGateDecision:
        interval_id = entry_interval_id(snapshot.timestamp)
        if has_active_pair:
            return _blocked_decision(
                interval_id=interval_id,
                cadence="REJECT_ACTIVE_PAIR_EXISTS",
                duplicate="PASS",
                cooldown="PASS",
                spread="NOT_CHECKED",
                reason="ACTIVE_PAIR_EXISTS",
            )
        if interval_id in self.used_intervals:
            return _blocked_decision(
                interval_id=interval_id,
                cadence="REJECT_INTERVAL_ALREADY_USED",
                duplicate="REJECT_DUPLICATE_INTERVAL",
                cooldown="PASS",
                spread="NOT_CHECKED",
                reason="DUPLICATE_INTERVAL",
            )
        if not instrument_allowed(
            instrument=snapshot.instrument,
            cycle=cycle,
            available_count=available_count,
        ):
            return _blocked_decision(
                interval_id=interval_id,
                cadence="PASS",
                duplicate="PASS",
                cooldown="PASS",
                spread="NOT_CHECKED",
                reason="INSTRUMENT_ALLOCATION",
            )
        spread_limit = ENTRY_SPREAD_LIMIT_TICKS[snapshot.instrument]
        if snapshot.spread_ticks > spread_limit:
            return _blocked_decision(
                interval_id=interval_id,
                cadence="PASS",
                duplicate="PASS",
                cooldown="PASS",
                spread=f"REJECT_SPREAD_GT_{spread_limit}",
                reason="SPREAD_ENTRY_GATE",
            )
        return CloneGateDecision(
            allowed=True,
            matched_first_bot_rule=False,
            clone_rule_missing=False,
            why_entry_allowed="PRE_MODE_PASS",
            first_bot_rule="COMPLETE_15M_BAR_CANDIDATE_ONCE",
            entry_interval_id=interval_id,
            duplicate_guard_status="PASS",
            cooldown_status="PENDING_MODE",
            spread_gate_status=f"PASS_SPREAD_LTE_{spread_limit}",
            cadence_gate_status="PASS_NEW_15M_INTERVAL",
            rejection_reason=None,
        )

    def evaluate_mode(
        self,
        *,
        snapshot: BookSnapshot,
        mode: str,
        pre_mode: CloneGateDecision,
    ) -> CloneGateDecision:
        if mode not in EXPERIMENTAL_MODES:
            return _blocked_decision(
                interval_id=pre_mode.entry_interval_id,
                cadence=pre_mode.cadence_gate_status,
                duplicate=pre_mode.duplicate_guard_status,
                cooldown="REJECT_CLONE_RULE_MISSING",
                spread=pre_mode.spread_gate_status,
                reason="CLONE_RULE_MISSING",
                clone_rule_missing=True,
            )
        cooldown_key = f"{snapshot.instrument.value}:{mode}"
        cooldown_after = self.last_mode_instrument_open.get(cooldown_key)
        if cooldown_after is not None and snapshot.timestamp < cooldown_after:
            return _blocked_decision(
                interval_id=pre_mode.entry_interval_id,
                cadence=pre_mode.cadence_gate_status,
                duplicate=pre_mode.duplicate_guard_status,
                cooldown=f"REJECT_COOLDOWN_UNTIL_{cooldown_after.isoformat()}",
                spread=pre_mode.spread_gate_status,
                reason="MODE_INSTRUMENT_COOLDOWN",
            )
        return CloneGateDecision(
            allowed=True,
            matched_first_bot_rule=True,
            clone_rule_missing=False,
            why_entry_allowed=(
                "STRICT_FIRST_BOT_CADENCE: completed 15m interval, one selected "
                "instrument, no duplicate interval, mode/instrument cooldown clear, "
                "entry spread within first-bot clone limit"
            ),
            first_bot_rule="COMPLETE_15M_BAR_CANDIDATE_ONCE_WITH_ALLOCATED_MODE",
            entry_interval_id=pre_mode.entry_interval_id,
            duplicate_guard_status=pre_mode.duplicate_guard_status,
            cooldown_status="PASS",
            spread_gate_status=pre_mode.spread_gate_status,
            cadence_gate_status=pre_mode.cadence_gate_status,
            rejection_reason=None,
        )

    def record_open(
        self,
        *,
        snapshot: BookSnapshot,
        mode: str,
        decision: CloneGateDecision,
    ) -> None:
        self.used_intervals.add(decision.entry_interval_id)
        cooldown_until = snapshot.timestamp + timedelta(minutes=MODE_INSTRUMENT_COOLDOWN_MINUTES)
        self.last_mode_instrument_open[f"{snapshot.instrument.value}:{mode}"] = cooldown_until
        self._save()

    def _load(self) -> None:
        if not self.path.exists() or self.path.stat().st_size == 0:
            return
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        self.used_intervals = {str(item) for item in payload.get("used_intervals", [])}
        self.last_mode_instrument_open = {}
        for key, value in dict(payload.get("last_mode_instrument_open", {})).items():
            try:
                self.last_mode_instrument_open[str(key)] = datetime.fromisoformat(
                    str(value).replace("Z", "+00:00")
                )
            except ValueError:
                continue

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "strict_first_bot_cadence": STRICT_FIRST_BOT_CADENCE_ENABLED,
            "decision_interval_minutes": DECISION_INTERVAL_MINUTES,
            "mode_instrument_cooldown_minutes": MODE_INSTRUMENT_COOLDOWN_MINUTES,
            "used_intervals": sorted(self.used_intervals)[-500:],
            "last_mode_instrument_open": {
                key: value.isoformat()
                for key, value in sorted(self.last_mode_instrument_open.items())
            },
        }
        self.path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )


def force_first_bot_prediction(
    prediction: PairEVPrediction,
    *,
    snapshot: BookSnapshot,
    mode: str,
) -> PairEVPrediction:
    """Convert the existing model estimate into a first-bot pair-open request.

    The first bot uses allocated pair modes instead of the target bot's legacy
    universal EV gate. Hard runtime safety stays paper-only; invalid books are
    filtered before this function is called.
    """

    stop_loss_ticks = prediction.best_stop_loss_ticks
    if stop_loss_ticks not in {2, 3}:
        stop_loss_ticks = 2
    score = mode_score(mode=mode, snapshot=snapshot)
    reason_codes = (
        LOGIC_SOURCE,
        mode,
        "PAIR_OPEN_REQUEST",
        f"MODE_SCORE={score}",
    )
    return replace(
        prediction,
        trade_allowed=True,
        rejection_reason=None,
        best_stop_loss_ticks=stop_loss_ticks,
        best_instrument=snapshot.instrument,
        reason_codes=reason_codes,
    )


def mode_score(*, mode: str, snapshot: BookSnapshot) -> Decimal:
    """Mirror the reference bot's lightweight mode scoring."""

    if mode == "IMBALANCE_PAIR":
        return abs(snapshot.imbalance(3))
    if mode == "MICROPRICE_PAIR":
        return abs(snapshot.microprice_edge_ticks)
    if mode == "TICK_VELOCITY_PAIR":
        return abs(snapshot.tick_velocity)
    if mode == "BOLLINGER_PAIR":
        return Decimal("1")
    return Decimal("0.5")


def instrument_allowed(*, instrument: SwarmInstrument, cycle: int, available_count: int) -> bool:
    """Use the reference bot's 80/20 instrument rotation when both books exist."""

    if available_count <= 1:
        return True
    selected = INSTRUMENT_ALLOCATION_SLOTS[(cycle - 1) % len(INSTRUMENT_ALLOCATION_SLOTS)]
    return instrument is selected


def entry_interval_id(value: datetime) -> str:
    timestamp = value.astimezone(UTC)
    minute = (timestamp.minute // DECISION_INTERVAL_MINUTES) * DECISION_INTERVAL_MINUTES
    interval = timestamp.replace(minute=minute, second=0, microsecond=0)
    return interval.isoformat()


def label_enrichment(
    *,
    label: PairLabel,
    long_entry_price: Decimal | None,
    short_entry_price: Decimal | None,
    entry_bid: Decimal | None,
    entry_ask: Decimal | None,
    gate_decision: CloneGateDecision | None = None,
) -> dict[str, object]:
    """Return mandatory first-bot-clone fields for ``pair_labels.jsonl``."""

    spread = (entry_ask - entry_bid) if entry_bid is not None and entry_ask is not None else None
    long_pnl, short_pnl = split_leg_pnl(label)
    tick = Decimal("1")
    long_exit_price = (
        None
        if long_entry_price is None
        else long_entry_price + (long_pnl * tick)
    )
    short_exit_price = (
        None
        if short_entry_price is None
        else short_entry_price - (short_pnl * tick)
    )
    spread_cost = label.entry_spread_cost_ticks + (label.avg_slippage_ticks * Decimal("2"))
    pnl_before_spread = label.pair_total_pnl_ticks + spread_cost
    capture_ratio = (
        max(label.runner_exit_ticks, Decimal("0")) / label.runner_mfe_ticks
        if label.runner_mfe_ticks > 0
        else Decimal("0")
    )
    trail_profile = "TRAIL_WIDE" if label.instrument is SwarmInstrument.NEOEFIR else "DEFAULT"
    diagnostic = (
        gate_decision.to_payload()
        if gate_decision is not None
        else _blocked_decision(
            interval_id="UNKNOWN",
            cadence="UNKNOWN",
            duplicate="UNKNOWN",
            cooldown="UNKNOWN",
            spread="UNKNOWN",
            reason="MISSING_DIAGNOSTIC",
            clone_rule_missing=True,
        ).to_payload()
    )
    return {
        "logic_source": LOGIC_SOURCE,
        "architecture": ARCHITECTURE,
        "symbol": label.instrument.value,
        "long_bot": label.long_bot_id,
        "short_bot": label.short_bot_id,
        "long_entry_price": long_entry_price,
        "short_entry_price": short_entry_price,
        "long_exit_price": long_exit_price,
        "short_exit_price": short_exit_price,
        "entry_bid": entry_bid,
        "entry_ask": entry_ask,
        "entry_spread": spread,
        "spread_cost": spread_cost,
        "pnl_before_spread": pnl_before_spread,
        "raw_pair_pnl": pnl_before_spread,
        "pnl_after_spread": label.pair_total_pnl_ticks,
        "pair_pnl_after_spread": label.pair_total_pnl_ticks,
        "real_pair_pnl_ticks": label.pair_total_pnl_ticks,
        "loser_pnl": -label.loser_loss_ticks,
        "runner_pnl": label.runner_exit_ticks,
        "fakeout": label.fakeout_flag,
        "runner_to_breakeven": (
            label.runner_success_flag or label.runner_mfe_ticks >= Decimal(label.stop_loss_ticks)
        ),
        "capture_ratio": capture_ratio,
        "trail_profile": trail_profile,
        **diagnostic,
        "paper": True,
        "live": False,
    }


def split_leg_pnl(label: PairLabel) -> tuple[Decimal, Decimal]:
    """Split pair PnL into long and short leg PnL from label semantics."""

    if label.loser_side is LegSide.LONG:
        return -label.loser_loss_ticks, label.runner_exit_ticks
    if label.loser_side is LegSide.SHORT:
        return label.runner_exit_ticks, -label.loser_loss_ticks
    half = label.pair_total_pnl_ticks / Decimal("2")
    return half, half


def legacy_mode_value(value: object) -> bool:
    return str(value) not in EXPERIMENTAL_MODES


def _blocked_decision(
    *,
    interval_id: str,
    cadence: str,
    duplicate: str,
    cooldown: str,
    spread: str,
    reason: str,
    clone_rule_missing: bool = False,
) -> CloneGateDecision:
    return CloneGateDecision(
        allowed=False,
        matched_first_bot_rule=False,
        clone_rule_missing=clone_rule_missing,
        why_entry_allowed="BLOCKED",
        first_bot_rule="NONE" if clone_rule_missing else "STRICT_FIRST_BOT_CADENCE",
        entry_interval_id=interval_id,
        duplicate_guard_status=duplicate,
        cooldown_status=cooldown,
        spread_gate_status=spread,
        cadence_gate_status=cadence,
        rejection_reason=reason,
    )


__all__ = [
    "ARCHITECTURE",
    "CloneGateDecision",
    "DECISION_INTERVAL_MINUTES",
    "EXPERIMENTAL_MODES",
    "ENTRY_SPREAD_LIMIT_TICKS",
    "FIRST_BOT_LOGIC_CLONE_ENABLED",
    "FirstBotCloneGate",
    "INSTRUMENT_ALLOCATION",
    "INSTRUMENT_ALLOCATION_SLOTS",
    "LEGACY_UNIVERSAL_LOGIC_ENABLED",
    "LOGIC_SOURCE",
    "MAX_RUNNER_SIDE_NONE_RATE",
    "MAX_WIDE_SPREAD_RATE",
    "MODE_ALLOCATION",
    "MODE_ALLOCATION_SLOTS",
    "PAIR_EVENT_NAMES",
    "STRICT_FIRST_BOT_CADENCE_ENABLED",
    "entry_interval_id",
    "force_first_bot_prediction",
    "instrument_allowed",
    "label_enrichment",
    "legacy_mode_value",
    "mode_score",
    "split_leg_pnl",
]
