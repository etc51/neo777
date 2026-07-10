from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from neo_swarm_scalper.config import load_config
from neo_swarm_scalper.entry_engine import EntryCandidate
from neo_swarm_scalper.expectancy import (
    adaptive_control_stop_ticks,
    evaluate_entry_candidate,
    evaluate_promotion,
)
from neo_swarm_scalper.storage import SQLiteJournal
from neo_swarm_scalper.tail_catcher import TailCatcherEngine
from neo_swarm_scalper.types import PositionSide


def test_entry_policy_requires_directional_edge_after_costs() -> None:
    config = load_config().tail_catcher
    decision = evaluate_entry_candidate(
        candidate=_candidate(expected_mfe_ticks="4", expected_stop_risk_ticks="2"),
        opposite_score=Decimal("0.20"),
        pressure_history=(Decimal("0.3"),) * 3,
        tick_velocity=Decimal("1"),
        spread_ticks=Decimal("2"),
        slippage_ticks=Decimal("1"),
        config=config,
    )
    assert decision.allowed is False
    assert decision.execution_cost_ticks == Decimal("4")
    assert decision.required_mfe_ticks == Decimal("5.00")
    assert "edge_below_execution_cost" in decision.reasons


def test_entry_policy_rejects_conflicting_or_transient_pressure() -> None:
    config = load_config().tail_catcher
    decision = evaluate_entry_candidate(
        candidate=_candidate(),
        opposite_score=Decimal("0.55"),
        pressure_history=(Decimal("0.3"), Decimal("-0.2"), Decimal("-0.1")),
        tick_velocity=Decimal("1"),
        spread_ticks=Decimal("2"),
        slippage_ticks=Decimal("1"),
        config=config,
    )
    assert decision.allowed is False
    assert "direction_conflict" in decision.reasons
    assert "pressure_not_persistent" in decision.reasons


def test_short_stop_experiment_is_not_blocked_by_uncalibrated_risk_prior() -> None:
    config = load_config().tail_catcher
    decision = evaluate_entry_candidate(
        candidate=_candidate(expected_mfe_ticks="8", expected_stop_risk_ticks="10"),
        opposite_score=Decimal("0.20"),
        pressure_history=(Decimal("0.3"),) * 3,
        tick_velocity=Decimal("1"),
        spread_ticks=Decimal("2"),
        slippage_ticks=Decimal("1"),
        config=config,
    )
    assert decision.allowed is True


def test_adaptive_stop_covers_execution_cost() -> None:
    stop_ticks = adaptive_control_stop_ticks(
        trigger_entry_price=Decimal("100"),
        tick=Decimal("0.01"),
        atr_range=Decimal("0.20"),
        execution_cost_ticks=Decimal("4"),
        config=load_config().tail_catcher,
    )
    assert stop_ticks == 10


def test_adaptive_stop_is_not_widened_by_bitcoin_spread_cost() -> None:
    stop_ticks = adaptive_control_stop_ticks(
        trigger_entry_price=Decimal("64000"),
        tick=Decimal("0.1"),
        atr_range=Decimal("20"),
        execution_cost_ticks=Decimal("70"),
        config=load_config().tail_catcher,
    )
    assert stop_ticks == 20


def test_promotion_requires_forward_sample_and_positive_confidence_bound() -> None:
    small = evaluate_promotion([Decimal("1")] * 199)
    validated = evaluate_promotion([Decimal("1")] * 200)
    mixed = evaluate_promotion([Decimal("2"), Decimal("-2")] * 100)
    assert small.promoted is False
    assert "sample_too_small" in small.reason
    assert validated.promoted is True
    assert validated.lower_confidence_ticks == Decimal("1")
    assert mixed.promoted is False
    assert "expectancy_not_positive" in mixed.reason


def test_old_shadow_schema_is_migrated_in_place(tmp_path: Path) -> None:
    path = tmp_path / "old.sqlite"
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE shadow_signals (
                signal_id TEXT PRIMARY KEY,
                timestamp_utc TEXT NOT NULL,
                instrument TEXT NOT NULL,
                side TEXT NOT NULL,
                confidence REAL NOT NULL,
                reason TEXT NOT NULL,
                gate_status_json TEXT NOT NULL,
                features_json TEXT NOT NULL
            );
            CREATE TABLE shadow_trades (
                trade_id TEXT PRIMARY KEY,
                signal_id TEXT NOT NULL,
                instrument TEXT NOT NULL,
                side TEXT NOT NULL,
                stop_ticks INTEGER NOT NULL,
                protection_trigger_bps REAL NOT NULL,
                trailing_mode TEXT NOT NULL,
                status TEXT NOT NULL,
                entry_time TEXT NOT NULL,
                entry_price REAL NOT NULL,
                stop_price REAL NOT NULL,
                exit_time TEXT,
                exit_price REAL,
                exit_reason TEXT,
                reentry_index INTEGER NOT NULL DEFAULT 0,
                consecutive_stop_index INTEGER NOT NULL DEFAULT 0
            );
            """
        )
    storage = SQLiteJournal(path)
    storage.initialize()
    with storage.connect() as conn:
        trade_columns = {row[1] for row in conn.execute("PRAGMA table_info(shadow_trades)")}
        signal_columns = {row[1] for row in conn.execute("PRAGMA table_info(shadow_signals)")}
    assert {"opportunity_id", "is_control", "trigger_entry_price"} <= trade_columns
    assert {"policy_version", "execution_cost_ticks"} <= signal_columns
    assert storage.table_counts()["market_opportunities"] == 0


def test_research_losses_do_not_consume_control_cooldown(tmp_path: Path) -> None:
    storage = SQLiteJournal(tmp_path / "cooldown.sqlite")
    storage.initialize()
    _insert_closed_trades(storage, is_control=False, count=12)
    engine = TailCatcherEngine(load_config(), storage)
    assert engine._recent_consecutive_stops("neobitcoin", PositionSide.LONG) == 0
    _insert_closed_trades(storage, is_control=True, count=2)
    assert engine._recent_consecutive_stops("neobitcoin", PositionSide.LONG) == 2


def test_bad_series_cooldown_expires_into_a_new_series(tmp_path: Path) -> None:
    storage = SQLiteJournal(tmp_path / "series_cooldown.sqlite")
    storage.initialize()
    _insert_closed_trades(storage, is_control=True, count=3)
    engine = TailCatcherEngine(load_config(), storage)
    common = {
        "side": PositionSide.LONG,
        "entry": _candidate(),
        "micro": {
            "pressure_score": Decimal("0.3"),
            "microprice_deviation": Decimal("0.5"),
            "orderbook_flip_flag": False,
        },
        "volatility": {"volatility_regime": "normal"},
        "spread_entry": {"entry_ok": True},
        "consecutive_stops": 3,
    }
    blocked = engine._reentry_plan(
        snapshot=SimpleNamespace(
            instrument="neobitcoin",
            timestamp_utc=datetime(2026, 7, 10, 0, 1, 3, tzinfo=UTC),
        ),
        **common,
    )
    allowed = engine._reentry_plan(
        snapshot=SimpleNamespace(
            instrument="neobitcoin",
            timestamp_utc=datetime(2026, 7, 10, 0, 6, 3, tzinfo=UTC),
        ),
        **common,
    )
    assert blocked["allowed"] is False
    assert blocked["reason_reentry_blocked"] == "bad_series_cooldown"
    assert allowed["allowed"] is True
    assert allowed["effective_consecutive_stops"] == 0
    assert allowed["reason_reentry_allowed"] == "initial_entry_after_bad_series_cooldown"


def _candidate(
    *,
    expected_mfe_ticks: str = "8",
    expected_stop_risk_ticks: str = "4",
) -> EntryCandidate:
    return EntryCandidate(
        entry_type="impulse_continuation",
        side=PositionSide.LONG,
        direction_model="test",
        direction_score=Decimal("0.60"),
        pressure_score=Decimal("0.30"),
        impulse_score=Decimal("0.60"),
        pullback_score=Decimal("0"),
        book_flip_score=Decimal("0"),
        reversal_score=Decimal("0"),
        lead_lag_score=Decimal("0"),
        expected_mfe_ticks=Decimal(expected_mfe_ticks),
        expected_stop_risk_ticks=Decimal(expected_stop_risk_ticks),
        reason="test",
        diagnostics={},
    )


def _insert_closed_trades(
    storage: SQLiteJournal,
    *,
    is_control: bool,
    count: int,
) -> None:
    with storage.connect() as conn:
        for index in range(count):
            conn.execute(
                """
                INSERT INTO shadow_trades(
                    trade_id, signal_id, instrument, side, stop_ticks,
                    protection_trigger_bps, trailing_mode, status, entry_time,
                    entry_price, stop_price, exit_time, exit_price, exit_reason,
                    best_price_after_entry, worst_price_after_entry,
                    is_control, experiment_role, reentry_index, consecutive_stop_index
                )
                VALUES (?, ?, 'neobitcoin', 'LONG', 10, 2, 'expectancy_adaptive',
                        'CLOSED', ?, 100, 99, ?, 99, 'stop_loss', 100, 99, ?, ?, 0, 0)
                """,
                (
                    f"trade-{int(is_control)}-{index}",
                    f"signal-{int(is_control)}-{index}",
                    f"2026-07-10T00:00:{index:02d}+00:00",
                    f"2026-07-10T00:01:{index:02d}+00:00",
                    int(is_control),
                    "control" if is_control else "research",
                ),
            )
        conn.commit()
