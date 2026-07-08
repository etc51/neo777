"""Tests for the neoether tail-catcher heartbeat component."""

from __future__ import annotations

from datetime import UTC, datetime

from neo_swarm_scalper.bots import NeoScalperBot, build_default_bots
from neo_swarm_scalper.config import load_config
from neo_swarm_scalper.types import BotAction, FeatureSnapshot, PositionSide


def test_tail_neoether_opens_on_negative_book_pressure() -> None:
    config = load_config()
    params = next(bot for bot in build_default_bots(config) if bot.bot_id == "tail_neoether")
    bot = NeoScalperBot(params, config)
    timestamp = datetime(2026, 7, 8, 12, tzinfo=UTC)

    decision = bot.decide(
        features={"neoether": _neoether_features(timestamp)},
        has_open_position=False,
        timestamp_utc=timestamp,
    )

    assert decision.action == BotAction.OPEN_SHORT
    assert decision.side == PositionSide.SHORT
    assert decision.instrument == "neoether"
    assert decision.features_snapshot["orderbook_imbalance_top3"] == "-0.42"


def test_tail_neoether_blocks_missing_or_unsafe_features() -> None:
    config = load_config()
    params = next(bot for bot in build_default_bots(config) if bot.bot_id == "tail_neoether")
    bot = NeoScalperBot(params, config)
    timestamp = datetime(2026, 7, 8, 12, tzinfo=UTC)

    for override in (
        {"stale": True},
        {"orderbook_missing": True},
    ):
        feature = _neoether_features(timestamp, **override)
        decision = bot.decide(
            features={"neoether": feature},
            has_open_position=False,
            timestamp_utc=timestamp,
        )

        assert decision.action == BotAction.WAIT

    decision = bot.decide(features={}, has_open_position=False, timestamp_utc=timestamp)
    assert decision.action == BotAction.WAIT


def _neoether_features(timestamp: datetime, **overrides: object) -> FeatureSnapshot:
    values: dict[str, object] = {
        "last_price": "2999",
        "mid_price": "2999",
        "impulse_up_score": "0.2",
        "impulse_down_score": "3.2",
        "orderbook_imbalance_top3": "-0.42",
        "spread_ticks": "1",
        "spread_bps": "4",
        "orderbook_missing": False,
        "stale": False,
    }
    values.update(overrides)
    return FeatureSnapshot(timestamp_utc=timestamp, instrument="neoether", values=values)
