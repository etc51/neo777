"""Streamlit dashboard for the paper/live-data swarm."""

from __future__ import annotations

import argparse
import json
from decimal import Decimal
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
            "equity_curve": [],
            "swarm_equity": 0,
            "best_bot": None,
            "worst_bot": None,
            "instrument_comparison": [],
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
    latest_decisions = {
        row["bot_id"]: row
        for row in storage.fetch_all(
            """
            SELECT bot_id, instrument, action, timestamp_utc, bot_params_json
            FROM bot_decisions
            WHERE rowid IN (SELECT MAX(rowid) FROM bot_decisions GROUP BY bot_id)
            """
        )
    }
    latest_metrics = {
        row["bot_id"]: row
        for row in storage.fetch_all(
            """
            SELECT bot_id, trades, winrate, expectancy, net_pnl, profit_factor, max_drawdown,
                   metrics_json
            FROM bot_metrics
            WHERE id IN (SELECT MAX(id) FROM bot_metrics GROUP BY bot_id)
            """
        )
    }
    bots = _bot_rows(bot_rows, latest_decisions, latest_metrics)
    instrument_comparison = _instrument_comparison(storage, market)
    equity_curve = _equity_curve(storage, bot_rows)
    return {
        "connection": {"tbank": "read-only or unavailable", "stale": False},
        "market": _market_rows(market, latest_books),
        "bots": bots,
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
        "equity_curve": equity_curve,
        "swarm_equity": _swarm_equity(bot_rows),
        "best_bot": _best_or_worst_bot(bots, reverse=True),
        "worst_bot": _best_or_worst_bot(bots, reverse=False),
        "instrument_comparison": instrument_comparison,
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
    st.metric("Swarm equity", state["swarm_equity"])
    st.subheader("Market")
    st.dataframe(state["market"], use_container_width=True)
    st.subheader("10 Bots")
    st.dataframe(state["bots"], use_container_width=True)
    st.subheader("Instrument Comparison")
    st.dataframe(state["instrument_comparison"], use_container_width=True)
    st.subheader("Equity Curve")
    st.dataframe(state["equity_curve"], use_container_width=True)
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


def _bot_rows(
    bot_rows: list[Any],
    latest_decisions: dict[str, Any],
    latest_metrics: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in bot_rows:
        payload = _row_dict(row)
        decision = latest_decisions.get(row["bot_id"])
        metrics = latest_metrics.get(row["bot_id"])
        params = _json_dict(None if decision is None else decision["bot_params_json"])
        payload.update(
            {
                "enabled": params.get("enabled"),
                "weight": params.get("weight"),
                "TP": params.get("tp_ticks"),
                "SL": params.get("sl_ticks"),
                "time_stop": params.get("time_stop_sec"),
                "cooldown": params.get("cooldown_sec"),
                "shadow_mode": params.get("shadow_mode"),
                "instrument": (
                    payload.get("open_position_instrument")
                    or (None if decision is None else decision["instrument"])
                ),
                "position": payload.get("open_position_side"),
                "last_action": None if decision is None else decision["action"],
                "last_decision_at": None if decision is None else decision["timestamp_utc"],
                "winrate": None if metrics is None else metrics["winrate"],
                "expectancy": None if metrics is None else metrics["expectancy"],
                "profit_factor": None if metrics is None else metrics["profit_factor"],
                "max_drawdown": None if metrics is None else metrics["max_drawdown"],
            }
        )
        rows.append(payload)
    return rows


def _instrument_comparison(storage: SQLiteJournal, market: list[Any]) -> list[dict[str, Any]]:
    trade_rows = {
        row["instrument"]: row
        for row in storage.fetch_all(
            """
            SELECT instrument, COUNT(*) AS trades, COALESCE(SUM(net_pnl), 0) AS net_pnl
            FROM trades
            GROUP BY instrument
            """
        )
    }
    result: list[dict[str, Any]] = []
    for row in market:
        trades = trade_rows.get(row["instrument"])
        result.append(
            {
                "instrument": row["instrument"],
                "last_timestamp": row["last_timestamp"],
                "last_price": row["last_price"],
                "trades": 0 if trades is None else trades["trades"],
                "net_pnl": 0 if trades is None else trades["net_pnl"],
            }
        )
    return result


def _equity_curve(storage: SQLiteJournal, bot_rows: list[Any]) -> list[dict[str, Any]]:
    trade_rows = storage.fetch_all(
        """
        SELECT t.bot_id, p.exit_time, t.net_pnl
        FROM trades t
        LEFT JOIN positions p ON p.position_id = t.position_id
        WHERE p.exit_time IS NOT NULL
        ORDER BY p.exit_time, t.rowid
        """
    )
    cumulative: dict[str, Decimal] = {}
    curve: list[dict[str, Any]] = []
    for row in trade_rows:
        bot_id = row["bot_id"]
        cumulative[bot_id] = cumulative.get(bot_id, Decimal("0")) + Decimal(str(row["net_pnl"]))
        curve.append(
            {
                "bot_id": bot_id,
                "timestamp_utc": row["exit_time"],
                "cumulative_pnl": str(cumulative[bot_id]),
            }
        )
    if curve:
        return curve
    return [
        {
            "bot_id": row["bot_id"],
            "timestamp_utc": row["updated_at"],
            "equity": row["equity"],
            "cumulative_pnl": row["net_pnl"],
        }
        for row in bot_rows
    ]


def _swarm_equity(bot_rows: list[Any]) -> float:
    return float(sum((Decimal(str(row["equity"])) for row in bot_rows), Decimal("0")))


def _best_or_worst_bot(bots: list[dict[str, Any]], *, reverse: bool) -> dict[str, Any] | None:
    if not bots:
        return None
    return sorted(bots, key=lambda row: Decimal(str(row.get("net_pnl") or 0)), reverse=reverse)[0]


def _json_dict(value: object) -> dict[str, Any]:
    if not isinstance(value, str):
        return {}
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _row_dict(row: Any) -> dict[str, Any]:
    return dict(row)


if __name__ == "__main__":
    raise SystemExit(main())
