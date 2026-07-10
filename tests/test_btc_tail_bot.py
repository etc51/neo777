"""Tests for the neobitcoin tail-catcher heartbeat component."""

from __future__ import annotations

from datetime import UTC, datetime

from neo_swarm_scalper.bots import NeoScalperBot, build_default_bots
from neo_swarm_scalper.config import load_config
from neo_swarm_scalper.types import BotAction, FeatureSnapshot, PositionSide


def test_tail_neobitcoin_opens_on_positive_book_pressure() -> None:
    config = load_config()
    params = build_default_bots(config)[0]
    bot = NeoScalperBot(params, config)
    timestamp = datetime(2026, 7, 10, 12, tzinfo=UTC)

    decision = bot.decide(
        features={"neobitcoin": _neobitcoin_features(timestamp)},
        has_open_position=False,
        timestamp_utc=timestamp,
    )

    assert decision.action == BotAction.OPEN_LONG
    assert decision.side == PositionSide.LONG
    assert decision.instrument == "neobitcoin"
    assert decision.features_snapshot["orderbook_imbalance_top3"] == "0.42"


def test_tail_neobitcoin_blocks_missing_or_unsafe_features() -> None:
    config = load_config()
    bot = NeoScalperBot(build_default_bots(config)[0], config)
    timestamp = datetime(2026, 7, 10, 12, tzinfo=UTC)

    for override in ({"stale": True}, {"orderbook_missing": True}):
        feature = _neobitcoin_features(timestamp, **override)
        decision = bot.decide(
            features={"neobitcoin": feature},
            has_open_position=False,
            timestamp_utc=timestamp,
        )
        assert decision.action == BotAction.WAIT

    decision = bot.decide(features={}, has_open_position=False, timestamp_utc=timestamp)
    assert decision.action == BotAction.WAIT


def _neobitcoin_features(timestamp: datetime, **overrides: object) -> FeatureSnapshot:
    values: dict[str, object] = {
        "last_price": "119000",
        "mid_price": "119000",
        "impulse_up_score": "3.2",
        "impulse_down_score": "0.2",
        "orderbook_imbalance_top3": "0.42",
        "spread_ticks": "60",
        "spread_bps": "0.94",
        "orderbook_missing": False,
        "stale": False,
    }
    values.update(overrides)
    return FeatureSnapshot(timestamp_utc=timestamp, instrument="neobitcoin", values=values)
