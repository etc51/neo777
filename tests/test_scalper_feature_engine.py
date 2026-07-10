from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from neo_swarm_scalper.feature_engine import NeoFeatureEngine
from neo_swarm_scalper.types import BookLevel, InstrumentMetadata, MarketSnapshot


def test_normal_bitcoin_spread_is_not_classified_as_chaotic() -> None:
    snapshot = MarketSnapshot(
        timestamp_utc=datetime(2026, 7, 10, tzinfo=UTC),
        instrument="neobitcoin",
        metadata=InstrumentMetadata(
            name="neobitcoin",
            display_name="Neobitcoin",
            ticker="BTCUSDperpA",
            figi="BTCUSDPERP00",
            class_code="SPBDMFUT",
            min_price_increment=Decimal("0.1"),
            trading_status="normal_trading",
        ),
        last_price=Decimal("64000"),
        bid_levels=(BookLevel(Decimal("63997"), Decimal("100")),),
        ask_levels=(BookLevel(Decimal("64003"), Decimal("100")),),
    )

    features = NeoFeatureEngine().update(snapshot)

    assert features.values["spread_ticks"] == "6E+1"
    assert features.values["market_regime"] != "chaotic"
