"""First-bot trading logic adapter for the universal swarm.

The reference bot in ``etc51/pervii`` uses fixed long-only and short-only
account groups. This adapter carries over its pair modes, allocation schedule,
bid/ask accounting fields, and paper-only runtime flags while leaving bot
selection to the target swarm's ten universal bots.
"""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
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
LOGIC_SOURCE: Final = "FIRST_BOT_CLONE"
ARCHITECTURE: Final = "UNIVERSAL_10_BOTS"

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


def label_enrichment(
    *,
    label: PairLabel,
    long_entry_price: Decimal | None,
    short_entry_price: Decimal | None,
    entry_bid: Decimal | None,
    entry_ask: Decimal | None,
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


__all__ = [
    "ARCHITECTURE",
    "EXPERIMENTAL_MODES",
    "FIRST_BOT_LOGIC_CLONE_ENABLED",
    "INSTRUMENT_ALLOCATION",
    "INSTRUMENT_ALLOCATION_SLOTS",
    "LEGACY_UNIVERSAL_LOGIC_ENABLED",
    "LOGIC_SOURCE",
    "MODE_ALLOCATION",
    "MODE_ALLOCATION_SLOTS",
    "PAIR_EVENT_NAMES",
    "force_first_bot_prediction",
    "instrument_allowed",
    "label_enrichment",
    "legacy_mode_value",
    "mode_score",
    "split_leg_pnl",
]
