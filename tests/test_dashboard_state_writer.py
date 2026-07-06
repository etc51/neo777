"""Tests for atomic readonly dashboard state writing."""

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from neo_trader.monitoring.dashboard_state_writer import (
    DashboardInstrumentState,
    DashboardSignalState,
    write_readonly_dashboard_state,
)
from neo_trader.monitoring.streamlit_dashboard import load_dashboard_state


def test_dashboard_state_writer_writes_valid_readonly_snapshot(tmp_path: Path) -> None:
    path = tmp_path / "dashboard_state.json"

    written = write_readonly_dashboard_state(
        path,
        instruments=[
            DashboardInstrumentState(
                instrument_uid="MOCK-SBER-TQBR",
                ticker="SBER",
                name="SBER.TQBR",
                spread_bps=Decimal("2.5"),
                imbalance=Decimal("0.12"),
                volatility_regime="normal",
                last_event_at=datetime(2026, 7, 6, 10, 0, tzinfo=UTC),
                last_price=Decimal("100.10"),
            )
        ],
        signals=[
            DashboardSignalState(
                instrument_uid="MOCK-SBER-TQBR",
                ticker="SBER",
                action="HOLD",
                reason_codes=("NO_BREAKOUT",),
            )
        ],
        kill_switch_enabled=False,
        commit_hash="abc1234",
        updated_at=datetime(2026, 7, 6, 10, 1, tzinfo=UTC),
    )

    assert written == path
    state = load_dashboard_state(path)
    assert state.commit_hash == "abc1234"
    assert state.instruments[0].instrument_uid == "MOCK-SBER-TQBR"
    assert state.positions[0].side.value == "FLAT"
    assert state.orders == ()
    assert not list(tmp_path.glob("*.tmp"))
