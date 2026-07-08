"""Online curator for the paper-only neo swarm."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from uuid import uuid4

from neo_swarm_scalper.config import NeoSwarmScalperConfig
from neo_swarm_scalper.storage import SQLiteJournal
from neo_swarm_scalper.types import (
    BotAction,
    BotDecision,
    BotParams,
    CuratorAction,
    FeatureSnapshot,
    VirtualAccount,
)


class NeoSwarmCurator:
    """Supervisor that can approve paper trades and tune bot parameters."""

    def __init__(
        self,
        config: NeoSwarmScalperConfig,
        *,
        storage: SQLiteJournal,
        bots: list[BotParams],
    ) -> None:
        self.config = config
        self.storage = storage
        self.bots = {bot.bot_id: bot for bot in bots}

    def approve_trade(
        self,
        *,
        decision: BotDecision,
        account: VirtualAccount,
        features: FeatureSnapshot | None,
        timestamp_utc: datetime,
    ) -> bool:
        bot = self.bots[decision.bot_id]
        allowed, reason = self._approval_reason(decision, account, features, bot)
        action = CuratorAction.ALLOW_TRADE if allowed else CuratorAction.REJECT_TRADE
        self.storage.record_curator_decision(
            decision_id=f"CURATOR_DECISION_{uuid4().hex}",
            timestamp_utc=timestamp_utc,
            bot_id=decision.bot_id,
            account_ref=decision.account_ref,
            instrument=decision.instrument,
            action=action.value,
            old_params=None,
            new_params=_bot_params(bot),
            reason=reason,
            metrics_snapshot=self.bot_metrics(decision.bot_id),
        )
        return allowed

    def close_decision(
        self,
        *,
        bot_id: str,
        account_ref: str,
        instrument: str,
        reason: str,
        timestamp_utc: datetime,
    ) -> None:
        self.storage.record_curator_decision(
            decision_id=f"CURATOR_DECISION_{uuid4().hex}",
            timestamp_utc=timestamp_utc,
            bot_id=bot_id,
            account_ref=account_ref,
            instrument=instrument,
            action=CuratorAction.CLOSE_POSITION.value,
            old_params=None,
            new_params=None,
            reason=reason,
            metrics_snapshot=self.bot_metrics(bot_id),
        )

    def update_bot_parameters(self, *, timestamp_utc: datetime) -> None:
        if not self.config.curator.enabled:
            return
        for bot in self.bots.values():
            metrics = self.bot_metrics(bot.bot_id)
            self.storage.record_bot_metrics(
                timestamp_utc=timestamp_utc,
                bot_id=bot.bot_id,
                account_ref=bot.account_ref,
                instrument="ALL",
                metrics=metrics,
            )
            trades = int(metrics["trades"])
            if trades < self.config.curator.min_trades_before_weight_change:
                continue
            old = _bot_params(bot)
            expectancy = Decimal(str(metrics["expectancy"]))
            stop_loss_rate = Decimal(str(metrics["stop_loss_rate"]))
            time_stop_rate = Decimal(str(metrics["time_stop_rate"]))
            if expectancy > 0 and Decimal(str(metrics["max_drawdown"])) > Decimal("-1000"):
                bot.weight = min(bot.weight + Decimal("0.05"), self.config.curator.max_weight)
                self._record_update(
                    timestamp_utc,
                    bot,
                    old,
                    CuratorAction.UPDATE_WEIGHT,
                    "positive expectancy and controlled drawdown",
                    metrics,
                )
            elif expectancy < 0:
                bot.weight = max(bot.weight - Decimal("0.05"), self.config.curator.min_weight)
                self._record_update(
                    timestamp_utc,
                    bot,
                    old,
                    CuratorAction.UPDATE_WEIGHT,
                    "negative expectancy",
                    metrics,
                )
                if (
                    self.config.curator.shadow_mode_for_bad_bots
                    and bot.weight <= self.config.curator.min_weight
                ):
                    bot.shadow_mode = True
                    self._record_update(
                        timestamp_utc,
                        bot,
                        old,
                        CuratorAction.SWITCH_TO_SHADOW,
                        "bad bot moved to shadow, not deleted",
                        metrics,
                    )
            if stop_loss_rate > Decimal("0.55") and self.config.curator.adaptive_tp_sl:
                bot.tp_ticks = max(self.config.scalping.take_profit_ticks_min, bot.tp_ticks - 1)
                bot.sl_ticks = max(self.config.scalping.stop_loss_ticks_min, bot.sl_ticks - 1)
                self._record_update(
                    timestamp_utc,
                    bot,
                    old,
                    CuratorAction.UPDATE_SL,
                    "high stop loss rate reduced TP/SL aggression",
                    metrics,
                )
            if time_stop_rate > Decimal("0.55") and self.config.curator.adaptive_cooldown:
                bot.cooldown_sec = min(30, bot.cooldown_sec + 3)
                self._record_update(
                    timestamp_utc,
                    bot,
                    old,
                    CuratorAction.UPDATE_COOLDOWN,
                    "high time-stop rate increased cooldown",
                    metrics,
                )

    def bot_metrics(self, bot_id: str) -> dict[str, object]:
        rows = self.storage.fetch_all(
            """
            SELECT net_pnl, gross_pnl, mfe, mae, duration_sec, exit_reason, instrument,
                   slippage_cost
            FROM trades
            WHERE bot_id = ?
            ORDER BY rowid DESC
            LIMIT ?
            """,
            (bot_id, self.config.curator.scoring_window_trades),
        )
        trades = len(rows)
        if trades == 0:
            return _empty_metrics()
        pnls = [Decimal(str(row["net_pnl"])) for row in rows]
        gross = [Decimal(str(row["gross_pnl"])) for row in rows]
        wins = [pnl for pnl in pnls if pnl > 0]
        losses = [pnl for pnl in pnls if pnl < 0]
        net_pnl = sum(pnls, Decimal("0"))
        gross_pnl = sum(gross, Decimal("0"))
        stop_loss = sum(1 for row in rows if row["exit_reason"] == "STOP_LOSS")
        take_profit = sum(1 for row in rows if row["exit_reason"] == "TAKE_PROFIT")
        time_stop = sum(1 for row in rows if row["exit_reason"] == "TIME_STOP")
        durations = [Decimal(str(row["duration_sec"])) for row in rows]
        mfe_values = [Decimal(str(row["mfe"])) for row in rows]
        mae_values = [Decimal(str(row["mae"])) for row in rows]
        return {
            "trades": trades,
            "net_pnl": str(net_pnl),
            "gross_pnl": str(gross_pnl),
            "average_trade_pnl": str(net_pnl / Decimal(trades)),
            "expectancy": str(net_pnl / Decimal(trades)),
            "winrate": str(Decimal(len(wins)) / Decimal(trades)),
            "profit_factor": str(_profit_factor(wins, losses)),
            "max_drawdown": str(_max_drawdown(list(reversed(pnls)))),
            "last_10_trades_pnl": str(sum(pnls[:10], Decimal("0"))),
            "last_30_trades_pnl": str(sum(pnls[:30], Decimal("0"))),
            "MFE": str(sum(mfe_values, Decimal("0")) / Decimal(trades)),
            "MAE": str(sum(mae_values, Decimal("0")) / Decimal(trades)),
            "avg_duration_sec": str(sum(durations, Decimal("0")) / Decimal(trades)),
            "take_profit_rate": str(Decimal(take_profit) / Decimal(trades)),
            "stop_loss_rate": str(Decimal(stop_loss) / Decimal(trades)),
            "time_stop_rate": str(Decimal(time_stop) / Decimal(trades)),
            "avg_spread_at_entry": "0",
            "avg_slippage_ticks": "0",
            "performance_neobitcoin": str(_instrument_pnl(rows, "neobitcoin")),
            "performance_neoether": str(_instrument_pnl(rows, "neoether")),
        }

    def _approval_reason(
        self,
        decision: BotDecision,
        account: VirtualAccount,
        features: FeatureSnapshot | None,
        bot: BotParams,
    ) -> tuple[bool, str]:
        if decision.action not in {BotAction.OPEN_LONG, BotAction.OPEN_SHORT}:
            return False, "not an entry signal"
        if account.open_position is not None:
            return False, "account already has an open paper position"
        if decision.instrument is None or features is None:
            return False, "missing instrument features"
        if not bot.enabled or bot.shadow_mode:
            return False, "bot disabled or in shadow"
        if features.values.get("stale"):
            return False, "stale data"
        tick_size = features.values.get("tick_size")
        if tick_size in {None, "", "None"}:
            return False, "tick_size unknown"
        spread = Decimal(str(features.values.get("spread_ticks") or "0"))
        if spread > self.config.scalping.spread_max_ticks:
            return False, "spread too wide"
        if features.values.get("market_regime") == "chaotic":
            return False, "chaotic regime"
        return True, "paper trade allowed by curator"

    def _record_update(
        self,
        timestamp_utc: datetime,
        bot: BotParams,
        old: Mapping[str, object],
        action: CuratorAction,
        reason: str,
        metrics: Mapping[str, object],
    ) -> None:
        self.storage.record_curator_decision(
            decision_id=f"CURATOR_DECISION_{uuid4().hex}",
            timestamp_utc=timestamp_utc,
            bot_id=bot.bot_id,
            account_ref=bot.account_ref,
            instrument=None,
            action=action.value,
            old_params=old,
            new_params=_bot_params(bot),
            reason=reason,
            metrics_snapshot=metrics,
        )


def _bot_params(bot: BotParams) -> dict[str, object]:
    return {
        "enabled": bot.enabled,
        "weight": str(bot.weight),
        "tp_ticks": bot.tp_ticks,
        "sl_ticks": bot.sl_ticks,
        "time_stop_sec": bot.time_stop_sec,
        "cooldown_sec": bot.cooldown_sec,
        "shadow_mode": bot.shadow_mode,
    }


def _empty_metrics() -> dict[str, object]:
    return {
        "trades": 0,
        "net_pnl": "0",
        "gross_pnl": "0",
        "average_trade_pnl": "0",
        "expectancy": "0",
        "winrate": "0",
        "profit_factor": "0",
        "max_drawdown": "0",
        "last_10_trades_pnl": "0",
        "last_30_trades_pnl": "0",
        "MFE": "0",
        "MAE": "0",
        "avg_duration_sec": "0",
        "take_profit_rate": "0",
        "stop_loss_rate": "0",
        "time_stop_rate": "0",
        "avg_spread_at_entry": "0",
        "avg_slippage_ticks": "0",
        "performance_neobitcoin": "0",
        "performance_neoether": "0",
    }


def _profit_factor(wins: list[Decimal], losses: list[Decimal]) -> Decimal:
    gross_loss = abs(sum(losses, Decimal("0")))
    if gross_loss == 0:
        return Decimal("0") if not wins else Decimal("999")
    return sum(wins, Decimal("0")) / gross_loss


def _max_drawdown(pnls: list[Decimal]) -> Decimal:
    equity = Decimal("0")
    peak = Decimal("0")
    max_dd = Decimal("0")
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)
    return max_dd


def _instrument_pnl(rows: list[Mapping[str, object]], instrument: str) -> Decimal:
    total = Decimal("0")
    for row in rows:
        if row["instrument"] == instrument:
            total += Decimal(str(row["net_pnl"]))
    return total


__all__ = ["NeoSwarmCurator"]
