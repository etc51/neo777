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
            "baskets": [],
            "active_legs": [],
            "positions": [],
            "trades": [],
            "curator": [],
            "data_quality": [],
            "equity_curve": [],
            "swarm_equity": 0,
            "best_bot": None,
            "worst_bot": None,
            "instrument_comparison": [],
            "open_shadow_trades": [],
            "latest_shadow_trades": [],
            "performance_by_stop_ticks": [],
            "mfe_mae_distributions": [],
            "stop_comparison": [],
            "entry_type_performance": [],
            "mfe_hit_rate_by_entry_type": [],
            "stop_comparison_by_entry_type": [],
            "latest_shadow_signals": [],
            "current_direction_scores": [],
            "current_regime": [],
            "reentry_series_summary": {},
            "best_entry_type_today": None,
            "daily_summary": {},
            "errors": [],
            "health": [],
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
               va.open_position_qty, va.updated_at,
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
    latest_microstructure = _latest_by_instrument(
        storage,
        """
        SELECT instrument, timestamp_utc, spread_bps, pressure_score, liquidity_score,
               thin_book_flag, orderbook_flip_flag
        FROM microstructure_features
        """,
    )
    latest_volatility = _latest_by_instrument(
        storage,
        """
        SELECT instrument, timestamp_utc, volatility_regime, impulse_score, chop_score,
               realized_volatility_1m
        FROM volatility_features
        """,
    )
    return {
        "connection": {"tbank": "read-only or unavailable", "stale": False},
        "market": _market_rows(market, latest_books, latest_microstructure, latest_volatility),
        "bots": bots,
        "baskets": [
            _row_dict(row)
            for row in storage.fetch_all("SELECT * FROM baskets ORDER BY opened_at DESC LIMIT 20")
        ],
        "active_legs": [
            _row_dict(row)
            for row in storage.fetch_all(
                "SELECT * FROM basket_legs WHERE status = 'OPEN' ORDER BY entry_time"
            )
        ],
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
        "open_shadow_trades": [
            _row_dict(row)
            for row in storage.fetch_all(
                """
                SELECT *
                FROM shadow_trades
                WHERE status = 'OPEN' AND is_control = 1
                ORDER BY entry_time DESC
                LIMIT 200
                """
            )
        ],
        "latest_shadow_trades": [
            _row_dict(row)
            for row in storage.fetch_all(
                """
                SELECT *
                FROM shadow_trades
                WHERE is_control = 1
                ORDER BY entry_time DESC
                LIMIT 200
                """
            )
        ],
        "performance_by_stop_ticks": _performance_by_stop_ticks(storage),
        "mfe_mae_distributions": _mfe_mae_distribution(storage),
        "stop_comparison": _stop_comparison(storage),
        "entry_type_performance": _entry_type_performance(storage),
        "mfe_hit_rate_by_entry_type": _mfe_hit_rate_by_entry_type(storage),
        "stop_comparison_by_entry_type": _stop_comparison_by_entry_type(storage),
        "latest_shadow_signals": _latest_shadow_signals(storage),
        "current_direction_scores": _current_direction_scores(storage),
        "current_regime": _current_regime(storage),
        "reentry_series_summary": _reentry_series_summary(storage),
        "best_entry_type_today": _best_entry_type_today(storage),
        "daily_summary": _daily_summary(storage),
        "errors": [
            _row_dict(row)
            for row in storage.fetch_all("SELECT * FROM errors ORDER BY id DESC LIMIT 100")
        ],
        "health": [
            _row_dict(row)
            for row in storage.fetch_all("SELECT * FROM system_health ORDER BY id DESC LIMIT 50")
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
    st.metric("Swarm equity", state["swarm_equity"])
    st.subheader("Market")
    st.dataframe(state["market"], use_container_width=True)
    st.subheader("Component Accounts")
    st.dataframe(state["bots"], use_container_width=True)
    st.subheader("Open Shadow Trades")
    st.dataframe(state["open_shadow_trades"], use_container_width=True)
    st.subheader("Performance by Stop Ticks")
    st.dataframe(state["performance_by_stop_ticks"], use_container_width=True)
    st.subheader("Stop Comparison")
    st.dataframe(state["stop_comparison"], use_container_width=True)
    st.subheader("Entry Type Performance")
    st.dataframe(state["entry_type_performance"], use_container_width=True)
    st.subheader("MFE Hit Rate by Entry Type")
    st.dataframe(state["mfe_hit_rate_by_entry_type"], use_container_width=True)
    st.subheader("Stop Comparison by Entry Type")
    st.dataframe(state["stop_comparison_by_entry_type"], use_container_width=True)
    st.subheader("Latest Shadow Signals")
    st.dataframe(state["latest_shadow_signals"], use_container_width=True)
    st.subheader("Current Direction Scores")
    st.dataframe(state["current_direction_scores"], use_container_width=True)
    st.subheader("Current Regime")
    st.dataframe(state["current_regime"], use_container_width=True)
    st.subheader("Re-entry Series")
    st.json(state["reentry_series_summary"])
    st.subheader("Best Entry Type Today")
    st.json(state["best_entry_type_today"])
    st.subheader("MFE/MAE Distributions")
    st.dataframe(state["mfe_mae_distributions"], use_container_width=True)
    st.subheader("Daily Summary")
    st.json(state["daily_summary"])
    st.subheader("Last 50 Trades")
    st.dataframe(state["trades"], use_container_width=True)
    st.subheader("Latest Shadow Trades")
    st.dataframe(state["latest_shadow_trades"], use_container_width=True)
    st.subheader("Curator Decisions")
    st.dataframe(state["curator"], use_container_width=True)
    st.subheader("Data Quality")
    st.dataframe(state["data_quality"], use_container_width=True)
    st.subheader("Errors")
    st.dataframe(state["errors"], use_container_width=True)
    st.subheader("Health")
    st.dataframe(state["health"], use_container_width=True)
    return 0


def _streamlit() -> Any:
    import streamlit as st

    return st


def _market_rows(
    market: list[Any],
    books: list[Any],
    microstructure: dict[str, Any],
    volatility: dict[str, Any],
) -> list[dict[str, Any]]:
    by_book = {row["instrument"]: row for row in books}
    rows: list[dict[str, Any]] = []
    for row in market:
        book = by_book.get(row["instrument"])
        micro = microstructure.get(row["instrument"], {})
        vol = volatility.get(row["instrument"], {})
        rows.append(
            {
                "instrument": row["instrument"],
                "last_timestamp": row["last_timestamp"],
                "last_price": row["last_price"],
                "best_bid": None if book is None else book["best_bid"],
                "best_ask": None if book is None else book["best_ask"],
                "spread_ticks": None if book is None else book["spread_ticks"],
                "spread_bps": micro.get("spread_bps"),
                "pressure_score": micro.get("pressure_score"),
                "liquidity_score": micro.get("liquidity_score"),
                "thin_book": micro.get("thin_book_flag"),
                "orderbook_flip": micro.get("orderbook_flip_flag"),
                "volatility_regime": vol.get("volatility_regime"),
                "impulse_score": vol.get("impulse_score"),
                "chop_score": vol.get("chop_score"),
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


def _latest_by_instrument(storage: SQLiteJournal, base_query: str) -> dict[str, dict[str, Any]]:
    rows = storage.fetch_all(f"{base_query} ORDER BY timestamp_utc")
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        latest[row["instrument"]] = _row_dict(row)
    return latest


def _performance_by_stop_ticks(storage: SQLiteJournal) -> list[dict[str, Any]]:
    rows = storage.fetch_all(
        """
        SELECT stop_ticks,
               COUNT(*) AS trades,
               SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) AS open_trades,
               SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END) AS closed_trades,
               SUM(CASE WHEN exit_reason IN ('stop_loss', 'protected_stop') THEN 1 ELSE 0 END)
                   AS stops,
               AVG(mfe_ticks) AS avg_mfe_ticks,
               AVG(mae_ticks) AS avg_mae_ticks
        FROM shadow_trades
        WHERE is_control = 0
        GROUP BY stop_ticks
        ORDER BY stop_ticks
        """
    )
    return [_row_dict(row) for row in rows]


def _mfe_mae_distribution(storage: SQLiteJournal) -> list[dict[str, Any]]:
    rows = storage.fetch_all(
        """
        SELECT instrument, stop_ticks,
               MIN(mfe_ticks) AS min_mfe,
               AVG(mfe_ticks) AS avg_mfe,
               MAX(mfe_ticks) AS max_mfe,
               MIN(mae_ticks) AS min_mae,
               AVG(mae_ticks) AS avg_mae,
               MAX(mae_ticks) AS max_mae
        FROM shadow_trades
        WHERE is_control = 0
        GROUP BY instrument, stop_ticks
        ORDER BY instrument, stop_ticks
        """
    )
    return [_row_dict(row) for row in rows]


def _stop_comparison(storage: SQLiteJournal) -> list[dict[str, Any]]:
    rows = storage.fetch_all(
        """
        SELECT instrument, stop_ticks, protection_trigger_bps, trailing_mode, trades_count,
               stops_count, winrate, expectancy, profit_factor, p90_mfe, p95_mfe,
               big_runner_count
        FROM shadow_stop_experiments
        WHERE id IN (
            SELECT MAX(id)
            FROM shadow_stop_experiments
            GROUP BY instrument, stop_ticks, protection_trigger_bps, trailing_mode
        )
        ORDER BY instrument, stop_ticks, protection_trigger_bps, trailing_mode
        """
    )
    return [_row_dict(row) for row in rows]


def _entry_type_performance(storage: SQLiteJournal) -> list[dict[str, Any]]:
    rows = storage.fetch_all(
        """
        SELECT *
        FROM entry_type_performance
        WHERE id IN (
            SELECT MAX(id)
            FROM entry_type_performance
            GROUP BY instrument, entry_type
        )
        ORDER BY instrument, entry_type
        """
    )
    return [_row_dict(row) for row in rows]


def _mfe_hit_rate_by_entry_type(storage: SQLiteJournal) -> list[dict[str, Any]]:
    rows = storage.fetch_all(
        """
        SELECT instrument, entry_type, mfe3_rate, mfe5_rate, mfe10_rate,
               mfe_005pct_rate, mfe_010pct_rate, mfe_015pct_rate,
               direction_correct_rate
        FROM entry_type_performance
        WHERE id IN (
            SELECT MAX(id)
            FROM entry_type_performance
            GROUP BY instrument, entry_type
        )
        ORDER BY mfe3_rate DESC, mfe5_rate DESC, mfe10_rate DESC
        """
    )
    return [_row_dict(row) for row in rows]


def _stop_comparison_by_entry_type(storage: SQLiteJournal) -> list[dict[str, Any]]:
    rows = storage.fetch_all(
        """
        SELECT entry_type, instrument, stop_ticks,
               COUNT(*) AS trades,
               SUM(CASE WHEN exit_reason IN ('stop_loss', 'protected_stop') THEN 1 ELSE 0 END)
                   AS stop_exits,
               AVG(mfe_ticks) AS avg_mfe_ticks,
               AVG(mae_ticks) AS avg_mae_ticks,
               AVG(
                   CASE
                       WHEN exit_price IS NOT NULL AND exit_reason != 'spread_shock'
                       THEN CASE
                           WHEN side = 'LONG'
                            THEN (exit_price - entry_price)
                                / (ABS(COALESCE(trigger_entry_price, entry_price) - stop_price)
                                   / stop_ticks)
                            ELSE (entry_price - exit_price)
                                / (ABS(stop_price - COALESCE(trigger_entry_price, entry_price))
                                   / stop_ticks)
                       END
                       ELSE NULL
                   END
               ) AS avg_pnl_ticks_non_spread
        FROM shadow_trades
        WHERE entry_type IS NOT NULL AND is_control = 0
        GROUP BY entry_type, instrument, stop_ticks
        ORDER BY entry_type, instrument, stop_ticks
        """
    )
    return [_row_dict(row) for row in rows]


def _latest_shadow_signals(storage: SQLiteJournal) -> list[dict[str, Any]]:
    rows = storage.fetch_all(
        """
        SELECT timestamp_utc, instrument, side, confidence, reason, entry_type,
               direction_model, direction_score, pressure_score, impulse_score,
               pullback_score, book_flip_score, reversal_score, lead_lag_score,
               expected_mfe_ticks, expected_stop_risk_ticks
        FROM shadow_signals
        ORDER BY timestamp_utc DESC
        LIMIT 100
        """
    )
    return [_row_dict(row) for row in rows]


def _current_direction_scores(storage: SQLiteJournal) -> list[dict[str, Any]]:
    rows = storage.fetch_all(
        """
        SELECT instrument, entry_type, side, direction_model, direction_score,
               pressure_score, impulse_score, pullback_score, book_flip_score,
               reversal_score, lead_lag_score, expected_mfe_ticks,
               expected_stop_risk_ticks, timestamp_utc
        FROM entry_strategy_scores
        WHERE id IN (
            SELECT MAX(id)
            FROM entry_strategy_scores
            GROUP BY instrument
        )
        ORDER BY instrument
        """
    )
    return [_row_dict(row) for row in rows]


def _current_regime(storage: SQLiteJournal) -> list[dict[str, Any]]:
    rows = storage.fetch_all(
        """
        SELECT instrument,
               COALESCE(entry_type, 'no_trade') AS regime,
               reason,
               direction_score,
               timestamp_utc
        FROM shadow_signals
        WHERE rowid IN (
            SELECT MAX(rowid)
            FROM shadow_signals
            GROUP BY instrument
        )
        ORDER BY instrument
        """
    )
    return [_row_dict(row) for row in rows]


def _reentry_series_summary(storage: SQLiteJournal) -> dict[str, Any]:
    rows = storage.fetch_all(
        """
        SELECT COUNT(*) AS series_count,
               SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) AS open_series,
               SUM(entries_count) AS entries_count,
               SUM(stops_count) AS stops_count
        FROM reentry_series
        """
    )
    return _row_dict(rows[0]) if rows else {}


def _best_entry_type_today(storage: SQLiteJournal) -> dict[str, Any] | None:
    rows = storage.fetch_all(
        """
        SELECT entry_type, instrument, mfe3_rate, mfe5_rate, mfe10_rate,
               best_stop_ticks, profit_factor, timestamp_utc
        FROM entry_type_performance
        WHERE date(timestamp_utc) = date('now')
        ORDER BY mfe3_rate DESC, mfe5_rate DESC, mfe10_rate DESC, profit_factor DESC
        LIMIT 1
        """
    )
    return None if not rows else _row_dict(rows[0])


def _daily_summary(storage: SQLiteJournal) -> dict[str, Any]:
    rows = storage.fetch_all(
        """
        SELECT COUNT(*) AS control_trades,
               COUNT(DISTINCT opportunity_id) AS market_opportunities,
               SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) AS open_control_trades,
               SUM(CASE WHEN protection_activated = 1 THEN 1 ELSE 0 END)
                   AS protected_control_trades,
               SUM(CASE WHEN exit_reason IN ('trailing_runner', 'protected_exit') THEN 1 ELSE 0 END)
                   AS trailed_or_protected_exits,
               MAX(mfe_ticks) AS max_mfe_ticks,
               MIN(mae_ticks) AS min_mae_ticks
        FROM shadow_trades
        WHERE is_control = 1
        """
    )
    counts = SQLiteJournal(storage.path).table_counts()
    summary = _row_dict(rows[0]) if rows else {}
    research = storage.fetch_all(
        """
        SELECT COUNT(*) AS research_arms,
               SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) AS open_research_arms
        FROM shadow_trades
        WHERE is_control = 0
        """
    )[0]
    summary.update(_row_dict(research))
    summary["counts"] = counts
    summary["real_orders_disabled"] = True
    summary["token_masked"] = True
    return summary


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
