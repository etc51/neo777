"""Curator and universal account bots for the paper swarm."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from neo_trader.neo_universal_swarm.config import SwarmAccountsConfig, UniversalBotConfig
from neo_trader.neo_universal_swarm.data import JsonlEventStore, SwarmEventType
from neo_trader.neo_universal_swarm.model import PairEVModel, PairEVPrediction
from neo_trader.neo_universal_swarm.types import (
    AccountBotState,
    ActivePair,
    BookSnapshot,
    LegSide,
    PairLabel,
    PairStatus,
    RejectionReason,
)


@dataclass
class UniversalAccountBot:
    """One paper account bot controlled by the curator."""

    config: UniversalBotConfig
    state: AccountBotState = AccountBotState.IDLE
    assigned_pair_id: str | None = None
    realized_pnl_ticks: Decimal = Decimal("0")
    last_command_id: str | None = None

    @property
    def bot_id(self) -> str:
        return self.config.bot_id

    @property
    def account_ref(self) -> str:
        return self.config.account_ref

    @property
    def is_free(self) -> bool:
        return self.state is AccountBotState.IDLE and self.assigned_pair_id is None

    def assign_leg(self, *, command_id: str, pair_id: str, side: LegSide) -> None:
        """Assign the bot to a leg by curator command."""

        self._ensure_usable()
        if not self.is_free:
            raise RuntimeError(f"{self.bot_id} is not free.")
        self.assigned_pair_id = pair_id
        self.state = AccountBotState.LONG_LEG if side is LegSide.LONG else AccountBotState.SHORT_LEG
        self.last_command_id = command_id

    def promote_runner(self, *, command_id: str, side: LegSide) -> None:
        """Promote the surviving leg to a runner by curator command."""

        self._ensure_usable()
        if self.assigned_pair_id is None:
            raise RuntimeError(f"{self.bot_id} has no assigned pair.")
        self.state = (
            AccountBotState.RUNNER_LONG if side is LegSide.LONG else AccountBotState.RUNNER_SHORT
        )
        self.last_command_id = command_id

    def close_position(self, *, command_id: str, pnl_ticks: Decimal, cooldown: bool) -> None:
        """Close the bot's paper leg and optionally move it to cooldown."""

        self.realized_pnl_ticks += pnl_ticks
        self.assigned_pair_id = None
        self.state = AccountBotState.COOLDOWN if cooldown else AccountBotState.IDLE
        self.last_command_id = command_id

    def release_cooldown(self) -> None:
        if self.state is AccountBotState.COOLDOWN:
            self.state = AccountBotState.IDLE

    def disable(self, *, command_id: str) -> None:
        self.assigned_pair_id = None
        self.state = AccountBotState.DISABLED
        self.last_command_id = command_id

    def _ensure_usable(self) -> None:
        if self.state is AccountBotState.DISABLED:
            raise RuntimeError(f"{self.bot_id} is disabled.")
        if self.config.live_enabled:
            raise RuntimeError(f"{self.bot_id} live mode is not allowed.")


@dataclass
class CuratorBot:
    """Neo swarm coordinator.

    The curator never trades directly. It reads model decisions, assigns two
    free universal bots, and settles paper labels.
    """

    accounts: SwarmAccountsConfig
    model: PairEVModel = field(default_factory=PairEVModel)
    event_store: JsonlEventStore | None = None
    bots: dict[str, UniversalAccountBot] = field(init=False)
    active_pairs: dict[str, ActivePair] = field(default_factory=dict)
    closed_pairs: list[PairLabel] = field(default_factory=list)
    rejections: dict[RejectionReason, int] = field(default_factory=dict)
    _pair_sequence: int = 0
    _bot_rotation_index: int = 0

    def __post_init__(self) -> None:
        self.bots = {
            config.bot_id: UniversalAccountBot(config)
            for config in self.accounts.universal_bots
        }

    @property
    def bot_to_account(self) -> dict[str, str]:
        return {bot_id: bot.account_ref for bot_id, bot in self.bots.items()}

    @property
    def total_pnl_ticks(self) -> Decimal:
        return sum((bot.realized_pnl_ticks for bot in self.bots.values()), Decimal("0"))

    def free_bots(self) -> tuple[UniversalAccountBot, ...]:
        bots = tuple(self.bots.values())
        if not bots:
            return ()
        offset = self._bot_rotation_index % len(bots)
        rotated = bots[offset:] + bots[:offset]
        return tuple(bot for bot in rotated if bot.is_free)

    def evaluate_entry(self, snapshot: BookSnapshot) -> PairEVPrediction:
        """Evaluate the model and record the decision event."""

        prediction = self.model.predict(snapshot)
        if self.event_store is not None:
            self.event_store.append(
                SwarmEventType.MODEL_DECISION,
                timestamp=snapshot.timestamp,
                instrument=snapshot.instrument,
                payload={
                    "pair_ev_ticks": str(prediction.pair_ev_ticks),
                    "trade_allowed": prediction.trade_allowed,
                    "best_stop_loss_ticks": prediction.best_stop_loss_ticks,
                    "probability_fakeout": str(prediction.probability_fakeout),
                    "reason_codes": list(prediction.reason_codes),
                    "rejection_reason": None
                    if prediction.rejection_reason is None
                    else prediction.rejection_reason.value,
                },
            )
        return prediction

    def maybe_open_pair(
        self,
        snapshot: BookSnapshot,
        *,
        prediction: PairEVPrediction | None = None,
    ) -> ActivePair | None:
        """Open a pair only if the model gate passes and two bots are free."""

        resolved_prediction = prediction or self.evaluate_entry(snapshot)
        if not resolved_prediction.trade_allowed:
            self._reject(resolved_prediction.rejection_reason or RejectionReason.MODEL)
            return None

        free_bots = self.free_bots()
        if len(free_bots) < 2:
            self._reject(RejectionReason.NO_FREE_BOTS)
            return None

        self._pair_sequence += 1
        pair_id = f"PAIR_{self._pair_sequence:06d}"
        command_id = f"CURATOR_OPEN_{pair_id}"
        long_bot, short_bot = free_bots[0], free_bots[1]
        self._bot_rotation_index = (
            self._bot_index(short_bot.bot_id) + 1
        ) % max(len(self.bots), 1)
        long_bot.assign_leg(command_id=command_id, pair_id=pair_id, side=LegSide.LONG)
        short_bot.assign_leg(command_id=command_id, pair_id=pair_id, side=LegSide.SHORT)
        active_pair = ActivePair(
            pair_id=pair_id,
            instrument=snapshot.instrument,
            long_bot_id=long_bot.bot_id,
            short_bot_id=short_bot.bot_id,
            opened_at=snapshot.timestamp,
            stop_loss_ticks=resolved_prediction.best_stop_loss_ticks,
            breakeven_ticks=resolved_prediction.best_stop_loss_ticks,
            model_ev_ticks=resolved_prediction.pair_ev_ticks,
            entry_reason_codes=resolved_prediction.reason_codes,
        )
        self.active_pairs[pair_id] = active_pair
        if self.event_store is not None:
            self.event_store.append(
                SwarmEventType.PAIR_OPENED,
                timestamp=snapshot.timestamp,
                instrument=snapshot.instrument,
                payload={
                    "pair_id": pair_id,
                    "long_bot_id": long_bot.bot_id,
                    "short_bot_id": short_bot.bot_id,
                    "model_ev_ticks": str(resolved_prediction.pair_ev_ticks),
                    "stop_loss_ticks": resolved_prediction.best_stop_loss_ticks,
                    "breakeven_ticks": resolved_prediction.best_stop_loss_ticks,
                    "entry_reason_codes": list(resolved_prediction.reason_codes),
                },
            )
        return active_pair

    def settle_pair(self, label: PairLabel) -> None:
        """Apply a simulator label to the assigned bots."""

        active_pair = self.active_pairs.pop(label.pair_id, None)
        if active_pair is None:
            raise KeyError(f"unknown active pair: {label.pair_id}")
        command_id = f"CURATOR_CLOSE_{label.pair_id}"
        long_bot = self.bots[active_pair.long_bot_id]
        short_bot = self.bots[active_pair.short_bot_id]

        if label.runner_side is LegSide.LONG:
            long_bot.promote_runner(command_id=f"CURATOR_RUNNER_{label.pair_id}", side=LegSide.LONG)
        elif label.runner_side is LegSide.SHORT:
            short_bot.promote_runner(
                command_id=f"CURATOR_RUNNER_{label.pair_id}",
                side=LegSide.SHORT,
            )

        long_pnl, short_pnl = _split_label_pnl(label)
        cooldown = label.fakeout_flag or label.exit_reason.value in {"FAILED_PAIR", "CHOP"}
        long_bot.close_position(command_id=command_id, pnl_ticks=long_pnl, cooldown=cooldown)
        short_bot.close_position(command_id=command_id, pnl_ticks=short_pnl, cooldown=cooldown)
        self.closed_pairs.append(label)
        if self.event_store is not None:
            self.event_store.append(
                SwarmEventType.PAIR_CLOSED,
                timestamp=label.exit_timestamp,
                instrument=label.instrument,
                payload={
                    "pair_id": label.pair_id,
                    "status": (
                        PairStatus.FAILED.value
                        if label.fakeout_flag
                        else PairStatus.CLOSED.value
                    ),
                    "pair_total_pnl_ticks": str(label.pair_total_pnl_ticks),
                    "pair_total_pnl_rub": str(label.pair_total_pnl_rub),
                    "exit_reason": label.exit_reason.value,
                    "runner_side": None if label.runner_side is None else label.runner_side.value,
                    "fakeout_flag": label.fakeout_flag,
                },
            )

    def release_cooldowns(self) -> None:
        """Return cooled-down bots to IDLE in paper simulation."""

        for bot in self.bots.values():
            bot.release_cooldown()

    def _reject(self, reason: RejectionReason) -> None:
        self.rejections[reason] = self.rejections.get(reason, 0) + 1

    def _bot_index(self, bot_id: str) -> int:
        for index, current_bot_id in enumerate(self.bots):
            if current_bot_id == bot_id:
                return index
        return 0


def _split_label_pnl(label: PairLabel) -> tuple[Decimal, Decimal]:
    if label.long_bot_id is None or label.short_bot_id is None:
        half = label.pair_total_pnl_ticks / Decimal("2")
        return half, half
    if label.loser_side is LegSide.LONG:
        return -label.loser_loss_ticks, label.runner_exit_ticks
    if label.loser_side is LegSide.SHORT:
        return label.runner_exit_ticks, -label.loser_loss_ticks
    half = label.pair_total_pnl_ticks / Decimal("2")
    return half, half


__all__ = [
    "CuratorBot",
    "UniversalAccountBot",
]
