"""Dashboard state writer for the Neo Universal Bot Swarm."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from neo_trader.neo_universal_swarm.bots import CuratorBot, UniversalAccountBot
from neo_trader.neo_universal_swarm.instruments import (
    SwarmInstrumentMetadata,
    load_swarm_instrument_catalog,
)
from neo_trader.neo_universal_swarm.model import PairEVModel, PairEVPrediction
from neo_trader.neo_universal_swarm.types import (
    ActivePair,
    BookSnapshot,
    SwarmInstrument,
    SwarmMetrics,
    as_utc,
)
from neo_trader.runtime import get_runtime_commit_hash


def build_swarm_dashboard_state(
    *,
    curator: CuratorBot,
    latest_snapshots: Sequence[BookSnapshot],
    metrics: SwarmMetrics | None = None,
    predictions: Mapping[str, PairEVPrediction] | None = None,
    instrument_metadata: Mapping[SwarmInstrument, SwarmInstrumentMetadata] | None = None,
    updated_at: datetime | None = None,
    commit_hash: str | None = None,
) -> dict[str, object]:
    """Build a JSON-serializable dashboard state payload."""

    now = as_utc(updated_at or datetime.now(UTC))
    model = curator.model if isinstance(curator.model, PairEVModel) else PairEVModel()
    prediction_map = predictions or {
        snapshot.instrument.value: model.predict(snapshot) for snapshot in latest_snapshots
    }
    metadata = dict(instrument_metadata or _load_default_instrument_metadata())
    realized_pnl = metrics.pair_ev_rub * Decimal(metrics.total_pairs) if metrics else Decimal("0")
    return {
        "updated_at": now.isoformat(),
        "commit_hash": commit_hash or get_runtime_commit_hash(),
        "force_flatten_at": "20:45:00",
        "kill_switch_enabled": False,
        "realized_pnl": str(realized_pnl),
        "instruments": [
            _instrument_payload(snapshot, metadata.get(snapshot.instrument))
            for snapshot in latest_snapshots
        ],
        "signals": [_signal_payload(snapshot, prediction_map) for snapshot in latest_snapshots],
        "positions": [],
        "orders": [],
        "swarm": {
            "curator": {
                "bot_id": curator.accounts.curator.bot_id,
                "trading_enabled": curator.accounts.curator.trading_enabled,
                "status": "PAPER_COORDINATOR",
                "active_pairs": len(curator.active_pairs),
                "closed_pairs": len(curator.closed_pairs),
                "total_pnl_ticks": str(curator.total_pnl_ticks),
            },
            "bots": [_bot_payload(bot) for bot in curator.bots.values()],
            "active_pairs": [
                _active_pair_payload(pair) for pair in curator.active_pairs.values()
            ],
            "latest_market": [
                _market_payload(snapshot, prediction_map, metadata.get(snapshot.instrument))
                for snapshot in latest_snapshots
            ],
            "metrics": {} if metrics is None else _metrics_payload(metrics),
            "model_quality": {} if metrics is None else _model_quality_payload(metrics),
            "readiness": {} if metrics is None else _readiness_payload(metrics),
        },
    }


def write_swarm_dashboard_state(
    path: Path | str,
    *,
    curator: CuratorBot,
    latest_snapshots: Sequence[BookSnapshot],
    metrics: SwarmMetrics | None = None,
    predictions: Mapping[str, PairEVPrediction] | None = None,
    instrument_metadata: Mapping[SwarmInstrument, SwarmInstrumentMetadata] | None = None,
    updated_at: datetime | None = None,
    commit_hash: str | None = None,
) -> Path:
    """Atomically write the swarm dashboard state JSON."""

    payload = build_swarm_dashboard_state(
        curator=curator,
        latest_snapshots=latest_snapshots,
        metrics=metrics,
        predictions=predictions,
        instrument_metadata=instrument_metadata,
        updated_at=updated_at,
        commit_hash=commit_hash,
    )
    resolved_path = Path(path)
    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = resolved_path.with_name(f".{resolved_path.name}.{uuid4().hex}.tmp")
    try:
        tmp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        os.replace(tmp_path, resolved_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()
    return resolved_path


def _instrument_payload(
    snapshot: BookSnapshot,
    metadata: SwarmInstrumentMetadata | None,
) -> dict[str, object]:
    uid = metadata.uid if metadata is not None else snapshot.instrument.value
    ticker = metadata.ticker if metadata is not None else snapshot.instrument.value
    name = metadata.tbank_query if metadata is not None else snapshot.instrument.value
    return {
        "instrument_uid": uid,
        "ticker": ticker,
        "name": name,
        "spread_bps": str((snapshot.spread_price / snapshot.mid_price) * Decimal("10000")),
        "imbalance": str(snapshot.imbalance(3)),
        "volatility_regime": _regime_from_snapshot(snapshot),
        "last_event_at": snapshot.timestamp.isoformat(),
    }


def _signal_payload(
    snapshot: BookSnapshot,
    predictions: Mapping[str, PairEVPrediction],
) -> dict[str, object]:
    prediction = predictions.get(snapshot.instrument.value)
    if prediction is None:
        action = "HOLD"
        confidence = Decimal("0")
        reason_codes: tuple[str, ...] = ()
    else:
        action = "BUY" if prediction.trade_allowed else "HOLD"
        confidence = prediction.probability_pair_profit
        reason_codes = prediction.reason_codes
    return {
        "instrument_uid": snapshot.instrument.value,
        "ticker": snapshot.instrument.value,
        "action": action,
        "confidence_score": str(confidence),
        "reason_codes": list(reason_codes),
        "suggested_stop": None,
        "suggested_take_profits": [],
        "timestamp": snapshot.timestamp.isoformat(),
    }


def _bot_payload(bot: UniversalAccountBot) -> dict[str, object]:
    return {
        "bot_id": bot.bot_id,
        "account_ref": bot.account_ref,
        "state": bot.state.value,
        "assigned_pair_id": bot.assigned_pair_id,
        "allowed_instruments": [
            instrument.value for instrument in bot.config.allowed_instruments
        ],
        "max_lot": bot.config.max_lot,
        "paper_enabled": bot.config.paper_enabled,
        "live_enabled": bot.config.live_enabled,
        "realized_pnl_ticks": str(bot.realized_pnl_ticks),
        "last_command_id": bot.last_command_id,
    }


def _active_pair_payload(pair: ActivePair) -> dict[str, object]:
    return {
        "pair_id": pair.pair_id,
        "instrument": pair.instrument.value,
        "long_bot_id": pair.long_bot_id,
        "short_bot_id": pair.short_bot_id,
        "opened_at": pair.opened_at.isoformat(),
        "stop_loss_ticks": pair.stop_loss_ticks,
        "breakeven_ticks": pair.breakeven_ticks,
        "status": pair.status.value,
        "model_ev_ticks": str(pair.model_ev_ticks),
        "entry_reason_codes": list(pair.entry_reason_codes),
    }


def _market_payload(
    snapshot: BookSnapshot,
    predictions: Mapping[str, PairEVPrediction],
    metadata: SwarmInstrumentMetadata | None,
) -> dict[str, object]:
    prediction = predictions.get(snapshot.instrument.value)
    return {
        "instrument": snapshot.instrument.value,
        "ticker": None if metadata is None else metadata.ticker,
        "uid": None if metadata is None else metadata.uid,
        "figi": None if metadata is None else metadata.figi,
        "class_code": None if metadata is None else metadata.class_code,
        "position_uid": None if metadata is None else metadata.position_uid,
        "best_bid": str(snapshot.best_bid),
        "best_ask": str(snapshot.best_ask),
        "spread_ticks": str(snapshot.spread_ticks),
        "microprice": str(snapshot.microprice),
        "imbalance_1": str(snapshot.imbalance(1)),
        "imbalance_3": str(snapshot.imbalance(3)),
        "model_ev_ticks": None if prediction is None else str(prediction.pair_ev_ticks),
        "trade_allowed": False if prediction is None else prediction.trade_allowed,
        "entry_reasons": [] if prediction is None else list(prediction.reason_codes),
        "rejection_reason": None
        if prediction is None or prediction.rejection_reason is None
        else prediction.rejection_reason.value,
    }


def _metrics_payload(metrics: SwarmMetrics) -> dict[str, object]:
    return {
        "total_pairs": metrics.total_pairs,
        "profitable_pairs": metrics.profitable_pairs,
        "losing_pairs": metrics.losing_pairs,
        "pair_winrate": str(metrics.pair_winrate),
        "pair_ev_ticks": str(metrics.pair_ev_ticks),
        "pair_ev_rub": str(metrics.pair_ev_rub),
        "avg_runner_profit_ticks": str(metrics.avg_runner_profit_ticks),
        "avg_loser_loss_ticks": str(metrics.avg_loser_loss_ticks),
        "avg_spread_cost_ticks": str(metrics.avg_spread_cost_ticks),
        "avg_slippage_ticks": str(metrics.avg_slippage_ticks),
        "fakeout_rate": str(metrics.fakeout_rate),
        "runner_to_breakeven_rate": str(metrics.runner_to_breakeven_rate),
        "runner_trailing_capture_ratio": str(metrics.runner_trailing_capture_ratio),
        "profit_factor": str(metrics.profit_factor),
        "max_drawdown": str(metrics.max_drawdown),
        "pnl_by_bot": _decimal_dict(metrics.pnl_by_bot),
        "pnl_by_account": _decimal_dict(metrics.pnl_by_account),
        "pnl_by_instrument": _decimal_dict(metrics.pnl_by_instrument),
        "pnl_by_regime": _decimal_dict(metrics.pnl_by_regime),
        "rejected_by_model": metrics.rejected_by_model,
        "rejected_by_spread": metrics.rejected_by_spread,
        "rejected_by_stale_book": metrics.rejected_by_stale_book,
        "rejected_by_chop": metrics.rejected_by_chop,
        "rejected_by_latency": metrics.rejected_by_latency,
    }


def _model_quality_payload(metrics: SwarmMetrics) -> dict[str, object]:
    return {
        "pair_ev_ticks": str(metrics.pair_ev_ticks),
        "profit_factor": str(metrics.profit_factor),
        "fakeout_rate": str(metrics.fakeout_rate),
        "runner_to_breakeven_rate": str(metrics.runner_to_breakeven_rate),
        "capture_ratio": str(metrics.runner_trailing_capture_ratio),
    }


def _readiness_payload(metrics: SwarmMetrics) -> dict[str, object]:
    return {
        "min_3000_pairs": metrics.total_pairs >= 3000,
        "positive_pair_ev": metrics.pair_ev_ticks > 0,
        "profit_factor_gt_1_10": metrics.profit_factor > Decimal("1.10"),
        "positive_on_at_least_one_instrument": any(
            pnl > 0 for pnl in metrics.pnl_by_instrument.values()
        ),
        "no_overnight_technical_guard": True,
        "live_enabled_requires_manual_approval": True,
    }


def _decimal_dict(values: Mapping[str, Decimal]) -> dict[str, str]:
    return {key: str(value) for key, value in values.items()}


def _regime_from_snapshot(snapshot: BookSnapshot) -> str:
    if snapshot.volatility_15s >= Decimal("3"):
        return "high"
    if snapshot.volatility_15s <= Decimal("0.5"):
        return "low"
    return "normal"


def _load_default_instrument_metadata() -> dict[SwarmInstrument, SwarmInstrumentMetadata]:
    try:
        return load_swarm_instrument_catalog().by_instrument()
    except FileNotFoundError:
        return {}


__all__ = [
    "build_swarm_dashboard_state",
    "write_swarm_dashboard_state",
]
