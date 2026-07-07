"""Tests for offline universe selection scoring."""

from decimal import Decimal
from pathlib import Path

import yaml

from neo_trader.research.universe_selector import (
    InstrumentQualityMetrics,
    rank_universe,
    write_active_universe,
)


def test_universe_selector_ranks_liquid_instrument_first(tmp_path: Path) -> None:
    liquid = InstrumentQualityMetrics(
        instrument_uid="UID_LIQUID",
        ticker="SBER",
        rows_total=1_000,
        events_per_minute=Decimal("120"),
        trade_notional=Decimal("50000000"),
        candle_volume=Decimal("100000"),
        volatility_bps=Decimal("35"),
        spread_bps_p90=Decimal("2"),
        top10_depth=Decimal("100000"),
        expected_slippage_bps={"100": {"buy_p90": Decimal("3"), "sell_p90": Decimal("4")}},
        stale_seconds=Decimal("1"),
        gap_count=0,
        usable_data_ratio=Decimal("1"),
    )
    weak = InstrumentQualityMetrics(
        instrument_uid="UID_WEAK",
        ticker="THIN",
        rows_total=10,
        events_per_minute=Decimal("1"),
        trade_notional=Decimal("1000"),
        candle_volume=Decimal("100"),
        volatility_bps=Decimal("3"),
        spread_bps_p90=Decimal("80"),
        top10_depth=Decimal("100"),
        expected_slippage_bps={"100": {"buy_p90": Decimal("100"), "sell_p90": Decimal("100")}},
        stale_seconds=Decimal("120"),
        gap_count=5,
        usable_data_ratio=Decimal("0.5"),
    )

    scores = rank_universe([weak, liquid])

    assert scores[0].instrument_uid == "UID_LIQUID"
    assert scores[0].score > scores[1].score
    path = write_active_universe(tmp_path / "active_universe.yaml", scores, max_instruments=1)
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert payload["instruments"] == [
        {
            "ticker": "SBER",
            "uid": "UID_LIQUID",
            "enabled": True,
            "score": str(scores[0].score),
        }
    ]

