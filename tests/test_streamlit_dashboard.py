"""Tests for the read-only Streamlit monitoring dashboard state model."""

from datetime import UTC, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

from neo_trader.monitoring.streamlit_dashboard import (
    DashboardSignalAction,
    countdown_to_force_flatten,
    dashboard_state_from_mapping,
    dashboard_state_to_tables,
    load_dashboard_state,
)


def test_dashboard_state_loads_snapshot_and_calculates_pnl(tmp_path: Path) -> None:
    path = tmp_path / "dashboard_state.json"
    path.write_text(
        """
        {
          "updated_at": "2026-07-06T10:15:00Z",
          "force_flatten_at": "18:40:00",
          "kill_switch_enabled": false,
          "realized_pnl": "125.50",
          "instruments": [
            {
              "instrument_uid": "UID1",
              "ticker": "SBER",
              "name": "Sber",
              "spread_bps": "2.5",
              "imbalance": "0.35",
              "volatility_regime": "normal",
              "last_event_at": "2026-07-06T10:14:55Z"
            }
          ],
          "signals": [
            {
              "instrument_uid": "UID1",
              "ticker": "SBER",
              "action": "BUY",
              "confidence_score": "0.72",
              "reason_codes": ["LONG_BREAKOUT", "ORDERBOOK_CONFIRMATION"],
              "suggested_stop": "99",
              "suggested_take_profits": ["103", "105"],
              "timestamp": "2026-07-06T10:15:00Z"
            },
            {
              "instrument_uid": "UID2",
              "ticker": "GAZP",
              "action": "HOLD",
              "confidence_score": "0"
            }
          ],
          "positions": [
            {
              "instrument_uid": "UID1",
              "ticker": "SBER",
              "side": "LONG",
              "quantity": "10",
              "avg_price": "100",
              "last_price": "101.25",
              "realized_pnl": "25"
            }
          ],
          "orders": [
            {
              "order_id": "sim-1",
              "instrument_uid": "UID1",
              "ticker": "SBER",
              "side": "BUY",
              "order_type": "LIMIT",
              "status": "PARTIALLY_FILLED",
              "quantity": "10",
              "filled_quantity": "4",
              "price": "101",
              "created_at": "2026-07-06T10:15:01Z"
            }
          ]
        }
        """,
        encoding="utf-8",
    )

    state = load_dashboard_state(path)

    assert state.updated_at == datetime(2026, 7, 6, 10, 15, tzinfo=UTC)
    assert state.force_flatten_at == time(18, 40)
    assert state.realized_pnl == Decimal("125.50")
    assert state.unrealized_pnl == Decimal("12.50")
    assert state.total_pnl == Decimal("138.00")
    assert state.active_signals[0].action is DashboardSignalAction.BUY
    assert len(state.active_signals) == 1


def test_dashboard_state_to_tables_contains_requested_sections() -> None:
    state = dashboard_state_from_mapping(
        {
            "updated_at": "2026-07-06T10:15:00Z",
            "force_flatten_at": "18:40:00",
            "kill_switch_enabled": True,
            "realized_pnl": "-10",
            "instruments": [
                {
                    "instrument_uid": "UID1",
                    "ticker": "SBER",
                    "spread_bps": "3",
                    "imbalance": "-0.2",
                    "volatility_regime": "high",
                    "last_event_at": "2026-07-06T10:14:55Z",
                }
            ],
            "signals": [
                {
                    "instrument_uid": "UID1",
                    "ticker": "SBER",
                    "action": "EXIT",
                    "confidence_score": "1",
                    "reason_codes": ["KILL_SWITCH"],
                }
            ],
            "positions": [
                {
                    "instrument_uid": "UID1",
                    "ticker": "SBER",
                    "side": "SHORT",
                    "quantity": "5",
                    "avg_price": "100",
                    "last_price": "98",
                }
            ],
            "orders": [],
        }
    )

    tables = dashboard_state_to_tables(
        state,
        now=datetime(2026, 7, 6, 10, 15, tzinfo=UTC),
    )

    assert tables["instruments"][0] == {
        "ticker": "SBER",
        "instrument": "SBER",
        "uid": "UID1",
        "spread_bps": "3",
        "imbalance": "-0.2",
        "volatility_regime": "high",
        "stale_seconds": "5",
    }
    assert tables["signals"][0]["action"] == "EXIT"
    assert tables["positions"][0]["unrealized_pnl"] == "10"
    assert tables["orders"] == []


def test_countdown_to_force_flatten_returns_zero_after_deadline() -> None:
    before = countdown_to_force_flatten(
        now=datetime(2026, 7, 6, 18, 30, tzinfo=UTC),
        force_flatten_at=time(18, 40),
    )
    after = countdown_to_force_flatten(
        now=datetime(2026, 7, 6, 18, 41, tzinfo=UTC),
        force_flatten_at=time(18, 40),
    )

    assert before == timedelta(minutes=10)
    assert after == timedelta(0)


def test_missing_dashboard_state_file_returns_empty_state(tmp_path: Path) -> None:
    state = load_dashboard_state(tmp_path / "missing.json")

    assert state.instruments == ()
    assert state.active_signals == ()
    assert state.realized_pnl == Decimal("0")
