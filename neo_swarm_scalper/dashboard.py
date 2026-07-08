"""Streamlit dashboard for the paper/live-data swarm."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from neo_swarm_scalper.config import DEFAULT_CONFIG_PATH, load_config
from neo_swarm_scalper.storage import ACCT_COL, SQLiteJournal


def load_dashboard_state(db_path: Path | str) -> dict[str, Any]:
    storage = SQLiteJournal(db_path)
    if not Path(db_path).exists():
        return {
            "connection": {"tbank": "unknown", "stale": True},
            "market": [],
            "bots": [],
            "positions": [],
            "trades": [],
            "curator": [],
            "data_quality": [],
        }
    market = storage.fetch_all(
        """
        SELECT instrument, MAX(timestamp_utc) AS last_timestamp, last_price
        FROM market_events
        GROUP BY instrument
        ORDER BY instrument
        """
    )
    latest_books = storage.fetch_all(
        """
        SELECT instrument, best_bid, best_ask, spread_ticks
        FROM orderbook_snapshots
        WHERE id IN (SELECT MAX(id) FROM orderbook_snapshots GROUP BY instrument)
        ORDER BY instrument
        """
    )
    bot_rows = storage.fetch_all(
        f"""
        SELECT va.bot_id, va.{ACCT_COL}, va.cash, va.equity, va.realized_pnl,
               va.unrealized_pnl, va.open_position_side, va.open_position_instrument,
               va.open_position_qty,
               COALESCE(t.trades, 0) AS trades,
               COALESCE(t.net_pnl, 0) AS net_pnl
        FROM virtual_accounts va
        LEFT JOIN (
            SELECT bot_id, COUNT(*) AS trades, SUM(net_pnl) AS net_pnl
            FROM trades GROUP BY bot_id
        ) t ON t.bot_id = va.bot_id
        ORDER BY va.{ACCT_COL}
        """
    )
    return {
        "connection": {"tbank": "read-only or unavailable", "stale": False},
        "market": _market_rows(market, latest_books),
        "bots": [_row_dict(row) for row in bot_rows],
        "positions": [
            _row_dict(row)
            for row in storage.fetch_all("SELECT * FROM positions WHERE status = 'OPEN'")
        ],
        "trades": [
            _row_dict(row)
            for row in storage.fetch_all("SELECT * FROM trades ORDER BY rowid DESC LIMIT 50")
        ],
        "curator": [
            _row_dict(row)
            for row in storage.fetch_all(
                "SELECT * FROM curator_decisions ORDER BY rowid DESC LIMIT 50"
            )
        ],
        "data_quality": [
            _row_dict(row)
            for row in storage.fetch_all("SELECT * FROM data_quality ORDER BY rowid DESC LIMIT 50")
        ],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="neo_swarm_scalper Streamlit dashboard")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--db", default=None)
    args = parser.parse_args(argv)
    config = load_config(args.config)
    db_path = Path(args.db) if args.db else config.storage.sqlite_path
    st = _streamlit()
    state = load_dashboard_state(db_path)
    st.set_page_config(page_title="neo_swarm_scalper", layout="wide")
    st.title("neo_swarm_scalper")
    st.caption("paper/live-data only; real orders disabled; token hidden")
    cols = st.columns(4)
    counts = SQLiteJournal(db_path).table_counts() if db_path.exists() else {}
    cols[0].metric("Market events", counts.get("market_events", 0))
    cols[1].metric("Trades", counts.get("trades", 0))
    cols[2].metric("Bot decisions", counts.get("bot_decisions", 0))
    cols[3].metric("Curator decisions", counts.get("curator_decisions", 0))
    st.subheader("Market")
    st.dataframe(state["market"], use_container_width=True)
    st.subheader("10 Bots")
    st.dataframe(state["bots"], use_container_width=True)
    st.subheader("Open Positions")
    st.dataframe(state["positions"], use_container_width=True)
    st.subheader("Last 50 Trades")
    st.dataframe(state["trades"], use_container_width=True)
    st.subheader("Curator Decisions")
    st.dataframe(state["curator"], use_container_width=True)
    st.subheader("Data Quality")
    st.dataframe(state["data_quality"], use_container_width=True)
    return 0


def _streamlit() -> Any:
    import streamlit as st

    return st


def _market_rows(market: list[Any], books: list[Any]) -> list[dict[str, Any]]:
    by_book = {row["instrument"]: row for row in books}
    rows: list[dict[str, Any]] = []
    for row in market:
        book = by_book.get(row["instrument"])
        rows.append(
            {
                "instrument": row["instrument"],
                "last_timestamp": row["last_timestamp"],
                "last_price": row["last_price"],
                "best_bid": None if book is None else book["best_bid"],
                "best_ask": None if book is None else book["best_ask"],
                "spread_ticks": None if book is None else book["spread_ticks"],
            }
        )
    return rows


def _row_dict(row: Any) -> dict[str, Any]:
    return dict(row)


if __name__ == "__main__":
    raise SystemExit(main())
