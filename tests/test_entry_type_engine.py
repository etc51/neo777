from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from neo_swarm_scalper.config import load_config
from neo_swarm_scalper.entry_engine import ENTRY_TYPES, EntryCandidate, EntryTypeEngine
from neo_swarm_scalper.entry_research import run_entry_research
from neo_swarm_scalper.run import MockNeoMarketDataProvider, run_swarm
from neo_swarm_scalper.storage import SQLiteJournal
from neo_swarm_scalper.tail_catcher import TailCatcherEngine
from neo_swarm_scalper.types import BookLevel, InstrumentMetadata, MarketSnapshot, PositionSide


def test_impulse_continuation_long_short() -> None:
    engine = EntryTypeEngine()
    long = _types(
        engine.score_candidates(
            snapshot=_snapshot(Decimal("100")),
            micro=_micro(pressure="0.35", micro_dev="1.2"),
            volatility=_vol(tick_velocity="3", range_position="0.90", breakout=True),
            price_history=_history("99.95", "100.00"),
        )
    )
    short = _types(
        engine.score_candidates(
            snapshot=_snapshot(Decimal("100")),
            micro=_micro(pressure="-0.35", micro_dev="-1.2"),
            volatility=_vol(tick_velocity="-3", range_position="0.10", breakout=True),
            price_history=_history("100.05", "100.00"),
        )
    )
    assert ("impulse_continuation", PositionSide.LONG) in long
    assert ("impulse_continuation", PositionSide.SHORT) in short


def test_pullback_continuation_long_short() -> None:
    engine = EntryTypeEngine()
    long = _types(
        engine.score_candidates(
            snapshot=_snapshot(Decimal("100.07")),
            micro=_micro(pressure="0.25", micro_dev="0.8", wall_bid=True),
            volatility=_vol(tick_velocity="1.0", range_position="0.70"),
            price_history=_history("100.00", "100.05", "100.10", "100.07"),
        )
    )
    short = _types(
        engine.score_candidates(
            snapshot=_snapshot(Decimal("99.93")),
            micro=_micro(pressure="-0.25", micro_dev="-0.8", wall_ask=True),
            volatility=_vol(tick_velocity="-1.0", range_position="0.30"),
            price_history=_history("100.00", "99.95", "99.90", "99.93"),
        )
    )
    assert ("pullback_continuation", PositionSide.LONG) in long
    assert ("pullback_continuation", PositionSide.SHORT) in short


def test_book_flip_entry_long_short() -> None:
    engine = EntryTypeEngine()
    long = _types(
        engine.score_candidates(
            snapshot=_snapshot(Decimal("100")),
            micro=_micro(pressure="0.30", micro_dev="1.0", flip=True),
            volatility=_vol(tick_velocity="1"),
            price_history=_history("99.99", "100.00"),
        )
    )
    short = _types(
        engine.score_candidates(
            snapshot=_snapshot(Decimal("100")),
            micro=_micro(pressure="-0.30", micro_dev="-1.0", flip=True),
            volatility=_vol(tick_velocity="-1"),
            price_history=_history("100.01", "100.00"),
        )
    )
    assert ("book_flip_entry", PositionSide.LONG) in long
    assert ("book_flip_entry", PositionSide.SHORT) in short


def test_failed_push_reversal_long_short() -> None:
    engine = EntryTypeEngine()
    long = _types(
        engine.score_candidates(
            snapshot=_snapshot(Decimal("100")),
            micro=_micro(pressure="0.22", micro_dev="0.7"),
            volatility=_vol(tick_velocity="-2", acceleration="1", range_position="0.10"),
            price_history=_history("100.05", "100.00"),
        )
    )
    short = _types(
        engine.score_candidates(
            snapshot=_snapshot(Decimal("100")),
            micro=_micro(pressure="-0.22", micro_dev="-0.7"),
            volatility=_vol(tick_velocity="2", acceleration="-1", range_position="0.90"),
            price_history=_history("99.95", "100.00"),
        )
    )
    assert ("failed_push_reversal", PositionSide.LONG) in long
    assert ("failed_push_reversal", PositionSide.SHORT) in short


def test_lead_lag_confirmed() -> None:
    engine = EntryTypeEngine()
    candidates = engine.score_candidates(
        snapshot=_snapshot(Decimal("100")),
        micro=_micro(pressure="0.25", micro_dev="0.8"),
        volatility=_vol(tick_velocity="1"),
        price_history=_history("99.99", "100.00"),
        peer_contexts={"neobitcoin": {"volatility": {"tick_velocity": Decimal("5")}}},
    )
    assert ("lead_lag_confirmed", PositionSide.LONG) in _types(candidates)


def test_live_shadow_writes_entry_fields_and_matrix(tmp_path: Path) -> None:
    result = run_swarm(
        load_config(),
        max_cycles=4,
        poll_interval_sec=0,
        provider=MockNeoMarketDataProvider(),
        db_path=tmp_path / "entry_live.sqlite",
        reports_dir=tmp_path / "reports",
        sleep=lambda _: None,
    )
    storage = SQLiteJournal(result.db_path)
    rows = storage.fetch_all(
        """
        SELECT DISTINCT entry_type
        FROM shadow_trades
        WHERE entry_type IS NOT NULL
        """
    )
    stop_ticks = {
        row["stop_ticks"]
        for row in storage.fetch_all("SELECT DISTINCT stop_ticks FROM shadow_trades")
    }
    counts = storage.table_counts()
    assert {row["entry_type"] for row in rows} <= set(ENTRY_TYPES)
    assert rows
    assert stop_ticks == {2, 3, 4, 5, 7, 10}
    assert counts["entry_strategy_scores"] > 0
    assert counts["entry_type_performance"] > 0
    assert counts["reentry_series"] > 0


def test_forward_labeler_is_offline_only_and_writes_labels(tmp_path: Path) -> None:
    result = run_swarm(
        load_config(),
        max_cycles=6,
        poll_interval_sec=0,
        provider=MockNeoMarketDataProvider(),
        db_path=tmp_path / "entry_research.sqlite",
        reports_dir=tmp_path / "reports",
        sleep=lambda _: None,
    )
    summary = run_entry_research(SQLiteJournal(result.db_path), limit_per_instrument=20)
    package_root = Path(__file__).resolve().parents[1] / "neo_swarm_scalper"
    live_sources = (
        (package_root / "tail_catcher.py").read_text(encoding="utf-8")
        + (package_root / "run.py").read_text(encoding="utf-8")
    )
    assert "entry_research" not in live_sources
    assert summary.candidates_scored > 0
    assert summary.labels_written > 0
    assert summary.best_entry_type in set(ENTRY_TYPES)


def test_reentry_blocks_when_direction_flips(tmp_path: Path) -> None:
    storage = SQLiteJournal(tmp_path / "reentry_rules.sqlite")
    storage.initialize()
    engine = TailCatcherEngine(load_config(), storage)
    candidate = EntryCandidate(
        entry_type="impulse_continuation",
        side=PositionSide.LONG,
        direction_model="unit",
        direction_score=Decimal("0.55"),
        pressure_score=Decimal("0.25"),
        impulse_score=Decimal("0.55"),
        pullback_score=Decimal("0"),
        book_flip_score=Decimal("0"),
        reversal_score=Decimal("0"),
        lead_lag_score=Decimal("0"),
        expected_mfe_ticks=Decimal("6"),
        expected_stop_risk_ticks=Decimal("3"),
        reason="unit",
        diagnostics={},
    )
    good = engine._reentry_plan(
        snapshot=_snapshot(Decimal("100")),
        side=PositionSide.LONG,
        entry=candidate,
        micro=_micro(pressure="0.20", micro_dev="0.5"),
        volatility=_vol(tick_velocity="1"),
        spread_entry={"entry_ok": True},
        consecutive_stops=1,
    )
    bad = engine._reentry_plan(
        snapshot=_snapshot(Decimal("100")),
        side=PositionSide.LONG,
        entry=candidate,
        micro=_micro(pressure="-0.20", micro_dev="-0.7", flip=True),
        volatility=_vol(tick_velocity="1"),
        spread_entry={"entry_ok": True},
        consecutive_stops=1,
    )
    assert good["allowed"] is True
    assert bad["allowed"] is False
    assert "microprice_flipped_against" in bad["reason_reentry_blocked"]


def _snapshot(price: Decimal) -> MarketSnapshot:
    tick = Decimal("0.01")
    return MarketSnapshot(
        timestamp_utc=datetime(2026, 7, 9, tzinfo=UTC),
        instrument="neoether",
        metadata=InstrumentMetadata(
            name="neoether",
            display_name="Neo Ethereum",
            ticker="ETHUSDperpA",
            figi="ETHUSDPERP00",
            class_code="SPBDMFUT",
            min_price_increment=tick,
            trading_status="normal_trading",
        ),
        last_price=price,
        bid_levels=(BookLevel(price - tick, Decimal("200")),),
        ask_levels=(BookLevel(price + tick, Decimal("80")),),
    )


def _micro(
    *,
    pressure: str,
    micro_dev: str,
    flip: bool = False,
    wall_bid: bool = False,
    wall_ask: bool = False,
) -> dict[str, object]:
    return {
        "pressure_score": Decimal(pressure),
        "microprice_deviation": Decimal(micro_dev),
        "orderbook_flip_flag": flip,
        "wall_detect_bid": wall_bid,
        "wall_detect_ask": wall_ask,
        "spread_expansion_flag": False,
        "thin_book_flag": False,
    }


def _vol(
    *,
    tick_velocity: str,
    acceleration: str = "0",
    range_position: str = "0.5",
    breakout: bool = False,
) -> dict[str, object]:
    return {
        "tick_velocity": Decimal(tick_velocity),
        "price_velocity": Decimal(tick_velocity) * Decimal("0.01"),
        "acceleration": Decimal(acceleration),
        "range_position": Decimal(range_position),
        "breakout_flag": breakout,
        "impulse_score": abs(Decimal(tick_velocity)),
        "chop_score": Decimal("0.4"),
        "volatility_regime": "normal",
    }


def _history(*prices: str) -> tuple[tuple[datetime, Decimal], ...]:
    start = datetime(2026, 7, 9, tzinfo=UTC)
    return tuple(
        (start + timedelta(seconds=index), Decimal(price))
        for index, price in enumerate(prices)
    )


def _types(candidates: list[EntryCandidate]) -> set[tuple[str, PositionSide]]:
    return {(candidate.entry_type, candidate.side) for candidate in candidates}
