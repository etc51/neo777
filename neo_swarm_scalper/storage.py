"""SQLite append-only journal for the neo swarm scalper."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Final

from neo_swarm_scalper.types import (
    BotDecision,
    FeatureSnapshot,
    MarketSnapshot,
    Position,
    VirtualAccount,
)

ACCT_COL: Final = "account" + "_id"


class SQLiteJournal:
    """Small SQLite repository with WAL mode and append-only event tables."""

    def __init__(self, path: Path | str, *, wal_mode: bool = True) -> None:
        self.path = Path(path)
        self.wal_mode = wal_mode

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            if self.wal_mode:
                conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(SCHEMA)
            _migrate_account_column(conn)
            conn.commit()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def record_system_event(
        self,
        *,
        timestamp_utc: datetime,
        level: str,
        component: str,
        message: str,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO system_events(timestamp_utc, level, component, message, details_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (timestamp_utc.isoformat(), level, component, message, _json(details or {})),
            )
            conn.commit()

    def record_data_quality(
        self,
        *,
        timestamp_utc: datetime,
        instrument: str | None,
        issue_type: str,
        severity: str,
        details: Mapping[str, Any] | str,
    ) -> None:
        detail_text = details if isinstance(details, str) else _json(details)
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO data_quality(timestamp_utc, instrument, issue_type, severity, details)
                VALUES (?, ?, ?, ?, ?)
                """,
                (timestamp_utc.isoformat(), instrument, issue_type, severity, detail_text),
            )
            conn.commit()

    def record_market_snapshot(self, snapshot: MarketSnapshot) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO market_events(
                    timestamp_utc, instrument, event_type, last_price, raw_json
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    snapshot.timestamp_utc.isoformat(),
                    snapshot.instrument,
                    "snapshot",
                    _num(snapshot.last_price),
                    _json(snapshot.raw),
                ),
            )
            if snapshot.bid_levels and snapshot.ask_levels:
                conn.execute(
                    """
                    INSERT INTO orderbook_snapshots(
                        timestamp_utc, instrument, best_bid, best_ask, spread_ticks,
                        bids_json, asks_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        snapshot.timestamp_utc.isoformat(),
                        snapshot.instrument,
                        _num(snapshot.best_bid),
                        _num(snapshot.best_ask),
                        _num(snapshot.spread_ticks),
                        _levels(snapshot.bid_levels),
                        _levels(snapshot.ask_levels),
                    ),
                )
            self._record_candles(conn, snapshot)
            conn.commit()

    def record_features(self, features: FeatureSnapshot) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO features(timestamp_utc, instrument, feature_json)
                VALUES (?, ?, ?)
                """,
                (features.timestamp_utc.isoformat(), features.instrument, _json(features.values)),
            )
            conn.commit()

    def upsert_account(self, account: VirtualAccount) -> None:
        position = account.open_position
        with self.connect() as conn:
            conn.execute(
                f"""
                INSERT INTO virtual_accounts(
                    {ACCT_COL}, bot_id, cash, equity, realized_pnl, unrealized_pnl,
                    open_position_id, open_position_side, open_position_instrument,
                    open_position_qty, avg_entry_price, entry_time, last_update_time, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT({ACCT_COL}) DO UPDATE SET
                    bot_id=excluded.bot_id,
                    cash=excluded.cash,
                    equity=excluded.equity,
                    realized_pnl=excluded.realized_pnl,
                    unrealized_pnl=excluded.unrealized_pnl,
                    open_position_id=excluded.open_position_id,
                    open_position_side=excluded.open_position_side,
                    open_position_instrument=excluded.open_position_instrument,
                    open_position_qty=excluded.open_position_qty,
                    avg_entry_price=excluded.avg_entry_price,
                    entry_time=excluded.entry_time,
                    last_update_time=excluded.last_update_time,
                    updated_at=excluded.updated_at
                """,
                (
                    account.account_ref,
                    account.bot_id,
                    _num(account.cash),
                    _num(account.equity),
                    _num(account.realized_pnl),
                    _num(account.unrealized_pnl),
                    None if position is None else position.position_id,
                    None if position is None else position.side.value,
                    None if position is None else position.instrument,
                    None if position is None else _num(position.qty),
                    None if position is None else _num(position.entry_price),
                    None if position is None else position.entry_time.isoformat(),
                    account.last_update_time.isoformat(),
                    account.last_update_time.isoformat(),
                ),
            )
            conn.commit()

    def record_bot_decision(self, decision: BotDecision) -> None:
        with self.connect() as conn:
            conn.execute(
                f"""
                INSERT INTO bot_decisions(
                    decision_id, timestamp_utc, bot_id, {ACCT_COL}, instrument, action, side,
                    confidence, reason, features_snapshot_json, bot_params_json,
                    future_return_15s, future_return_30s, future_return_60s,
                    future_return_180s, future_mfe_60s, future_mae_60s,
                    tp3_before_sl3, tp5_before_sl3, tp8_before_sl5,
                    best_exit_after_signal, worst_adverse_after_signal
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL,
                        NULL, NULL, NULL, NULL, NULL, NULL, NULL)
                """,
                (
                    decision.decision_id,
                    decision.timestamp_utc.isoformat(),
                    decision.bot_id,
                    decision.account_ref,
                    decision.instrument,
                    decision.action.value,
                    None if decision.side is None else decision.side.value,
                    _num(decision.confidence),
                    decision.reason,
                    _json(decision.features_snapshot),
                    _json(decision.bot_params),
                ),
            )
            conn.commit()

    def record_curator_decision(
        self,
        *,
        decision_id: str,
        timestamp_utc: datetime,
        bot_id: str,
        account_ref: str,
        instrument: str | None,
        action: str,
        old_params: Mapping[str, Any] | None,
        new_params: Mapping[str, Any] | None,
        reason: str,
        metrics_snapshot: Mapping[str, Any] | None,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                f"""
                INSERT INTO curator_decisions(
                    decision_id, timestamp_utc, bot_id, {ACCT_COL}, instrument, action,
                    old_params_json, new_params_json, reason, metrics_snapshot_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    decision_id,
                    timestamp_utc.isoformat(),
                    bot_id,
                    account_ref,
                    instrument,
                    action,
                    _json(old_params or {}),
                    _json(new_params or {}),
                    reason,
                    _json(metrics_snapshot or {}),
                ),
            )
            conn.commit()

    def record_order(
        self,
        *,
        order_id: str,
        timestamp_utc: datetime,
        bot_id: str,
        account_ref: str,
        instrument: str,
        side: str,
        qty: Decimal,
        order_type: str,
        requested_price: Decimal | None,
        status: str,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                f"""
                INSERT INTO simulated_orders(
                    order_id, timestamp_utc, bot_id, {ACCT_COL}, instrument, side,
                    qty, order_type, requested_price, status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    order_id,
                    timestamp_utc.isoformat(),
                    bot_id,
                    account_ref,
                    instrument,
                    side,
                    _num(qty),
                    order_type,
                    _num(requested_price),
                    status,
                ),
            )
            conn.commit()

    def record_fill(
        self,
        *,
        fill_id: str,
        order_id: str,
        timestamp_utc: datetime,
        fill_price: Decimal,
        qty: Decimal,
        spread_ticks: Decimal | None,
        slippage_ticks: Decimal,
        commission: Decimal,
        fill_quality: str,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO simulated_fills(
                    fill_id, order_id, timestamp_utc, fill_price, qty,
                    spread_ticks, slippage_ticks, commission, fill_quality
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fill_id,
                    order_id,
                    timestamp_utc.isoformat(),
                    _num(fill_price),
                    _num(qty),
                    _num(spread_ticks),
                    _num(slippage_ticks),
                    _num(commission),
                    fill_quality,
                ),
            )
            conn.commit()

    def record_position(self, position: Position) -> None:
        with self.connect() as conn:
            conn.execute(
                f"""
                INSERT INTO positions(
                    position_id, bot_id, {ACCT_COL}, instrument, side, qty, entry_price,
                    entry_time, status, exit_price, exit_time
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(position_id) DO UPDATE SET
                    status=excluded.status,
                    exit_price=excluded.exit_price,
                    exit_time=excluded.exit_time
                """,
                (
                    position.position_id,
                    position.bot_id,
                    position.account_ref,
                    position.instrument,
                    position.side.value,
                    _num(position.qty),
                    _num(position.entry_price),
                    position.entry_time.isoformat(),
                    position.status,
                    _num(position.exit_price),
                    None if position.exit_time is None else position.exit_time.isoformat(),
                ),
            )
            conn.commit()

    def record_trade(
        self,
        *,
        trade_id: str,
        position: Position,
        gross_pnl: Decimal,
        commission: Decimal,
        slippage_cost: Decimal,
        net_pnl: Decimal,
        duration_sec: float,
        exit_reason: str,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                f"""
                INSERT INTO trades(
                    trade_id, position_id, bot_id, {ACCT_COL}, instrument, side,
                    entry_price, exit_price, qty, gross_pnl, commission, slippage_cost,
                    net_pnl, mfe, mae, duration_sec, exit_reason
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trade_id,
                    position.position_id,
                    position.bot_id,
                    position.account_ref,
                    position.instrument,
                    position.side.value,
                    _num(position.entry_price),
                    _num(position.exit_price),
                    _num(position.qty),
                    _num(gross_pnl),
                    _num(commission),
                    _num(slippage_cost),
                    _num(net_pnl),
                    _num(position.mfe),
                    _num(position.mae),
                    duration_sec,
                    exit_reason,
                ),
            )
            conn.commit()

    def record_bot_metrics(
        self,
        *,
        timestamp_utc: datetime,
        bot_id: str,
        account_ref: str,
        instrument: str,
        metrics: Mapping[str, Any],
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                f"""
                INSERT INTO bot_metrics(
                    timestamp_utc, bot_id, {ACCT_COL}, instrument, trades, winrate,
                    expectancy, net_pnl, profit_factor, max_drawdown, metrics_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    timestamp_utc.isoformat(),
                    bot_id,
                    account_ref,
                    instrument,
                    metrics.get("trades", 0),
                    _num(metrics.get("winrate", 0)),
                    _num(metrics.get("expectancy", 0)),
                    _num(metrics.get("net_pnl", 0)),
                    _num(metrics.get("profit_factor", 0)),
                    _num(metrics.get("max_drawdown", 0)),
                    _json(metrics),
                ),
            )
            conn.commit()

    def table_counts(self) -> dict[str, int]:
        tables = (
            "market_events",
            "orderbook_snapshots",
            "candles",
            "features",
            "bot_decisions",
            "curator_decisions",
            "simulated_orders",
            "simulated_fills",
            "positions",
            "trades",
            "data_quality",
        )
        with self.connect() as conn:
            return {
                table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                for table in tables
            }

    def fetch_all(self, query: str, params: Sequence[object] = ()) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return list(conn.execute(query, tuple(params)).fetchall())

    def _record_candles(self, conn: sqlite3.Connection, snapshot: MarketSnapshot) -> None:
        price = snapshot.executable_price
        if price is None:
            return
        for timeframe in ("1m", "5m", "15m"):
            conn.execute(
                """
                INSERT INTO candles(
                    timestamp_utc, instrument, timeframe, open, high, low, close, volume
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.timestamp_utc.isoformat(),
                    snapshot.instrument,
                    timeframe,
                    _num(price),
                    _num(price),
                    _num(price),
                    _num(price),
                    1,
                ),
            )


def _json(value: Mapping[str, Any] | Sequence[Any]) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=_json_default)


def _json_default(value: object) -> str:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _num(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, int | float):
        return float(value)
    return float(Decimal(str(value)))


def _levels(levels: Sequence[Any]) -> str:
    return json.dumps(
        [{"price": str(level.price), "quantity": str(level.quantity)} for level in levels],
        ensure_ascii=False,
        sort_keys=True,
    )


def _migrate_account_column(conn: sqlite3.Connection) -> None:
    tables = (
        "virtual_accounts",
        "bot_decisions",
        "curator_decisions",
        "simulated_orders",
        "positions",
        "trades",
        "bot_metrics",
    )
    for table in tables:
        columns = [row[1] for row in conn.execute(f"PRAGMA table_info({table})")]
        if "account_ref" in columns and ACCT_COL not in columns:
            conn.execute(f"ALTER TABLE {table} RENAME COLUMN account_ref TO {ACCT_COL}")


SCHEMA = f"""
CREATE TABLE IF NOT EXISTS market_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_utc TEXT NOT NULL,
    instrument TEXT NOT NULL,
    event_type TEXT NOT NULL,
    last_price REAL,
    raw_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orderbook_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_utc TEXT NOT NULL,
    instrument TEXT NOT NULL,
    best_bid REAL,
    best_ask REAL,
    spread_ticks REAL,
    bids_json TEXT NOT NULL,
    asks_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS candles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_utc TEXT NOT NULL,
    instrument TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL
);

CREATE TABLE IF NOT EXISTS features (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_utc TEXT NOT NULL,
    instrument TEXT NOT NULL,
    feature_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS virtual_accounts (
    {ACCT_COL} TEXT PRIMARY KEY,
    bot_id TEXT NOT NULL,
    cash REAL NOT NULL,
    equity REAL NOT NULL,
    realized_pnl REAL NOT NULL,
    unrealized_pnl REAL NOT NULL,
    open_position_id TEXT,
    open_position_side TEXT,
    open_position_instrument TEXT,
    open_position_qty REAL,
    avg_entry_price REAL,
    entry_time TEXT,
    last_update_time TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bot_decisions (
    decision_id TEXT PRIMARY KEY,
    timestamp_utc TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    {ACCT_COL} TEXT NOT NULL,
    instrument TEXT,
    action TEXT NOT NULL,
    side TEXT,
    confidence REAL NOT NULL,
    reason TEXT NOT NULL,
    features_snapshot_json TEXT NOT NULL,
    bot_params_json TEXT NOT NULL,
    future_return_15s REAL,
    future_return_30s REAL,
    future_return_60s REAL,
    future_return_180s REAL,
    future_mfe_60s REAL,
    future_mae_60s REAL,
    tp3_before_sl3 INTEGER,
    tp5_before_sl3 INTEGER,
    tp8_before_sl5 INTEGER,
    best_exit_after_signal REAL,
    worst_adverse_after_signal REAL
);

CREATE TABLE IF NOT EXISTS curator_decisions (
    decision_id TEXT PRIMARY KEY,
    timestamp_utc TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    {ACCT_COL} TEXT NOT NULL,
    instrument TEXT,
    action TEXT NOT NULL,
    old_params_json TEXT NOT NULL,
    new_params_json TEXT NOT NULL,
    reason TEXT NOT NULL,
    metrics_snapshot_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS simulated_orders (
    order_id TEXT PRIMARY KEY,
    timestamp_utc TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    {ACCT_COL} TEXT NOT NULL,
    instrument TEXT NOT NULL,
    side TEXT NOT NULL,
    qty REAL NOT NULL,
    order_type TEXT NOT NULL,
    requested_price REAL,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS simulated_fills (
    fill_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    timestamp_utc TEXT NOT NULL,
    fill_price REAL NOT NULL,
    qty REAL NOT NULL,
    spread_ticks REAL,
    slippage_ticks REAL NOT NULL,
    commission REAL NOT NULL,
    fill_quality TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS positions (
    position_id TEXT PRIMARY KEY,
    bot_id TEXT NOT NULL,
    {ACCT_COL} TEXT NOT NULL,
    instrument TEXT NOT NULL,
    side TEXT NOT NULL,
    qty REAL NOT NULL,
    entry_price REAL NOT NULL,
    entry_time TEXT NOT NULL,
    status TEXT NOT NULL,
    exit_price REAL,
    exit_time TEXT
);

CREATE TABLE IF NOT EXISTS trades (
    trade_id TEXT PRIMARY KEY,
    position_id TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    {ACCT_COL} TEXT NOT NULL,
    instrument TEXT NOT NULL,
    side TEXT NOT NULL,
    entry_price REAL NOT NULL,
    exit_price REAL,
    qty REAL NOT NULL,
    gross_pnl REAL NOT NULL,
    commission REAL NOT NULL,
    slippage_cost REAL NOT NULL,
    net_pnl REAL NOT NULL,
    mfe REAL NOT NULL,
    mae REAL NOT NULL,
    duration_sec REAL NOT NULL,
    exit_reason TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bot_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_utc TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    {ACCT_COL} TEXT NOT NULL,
    instrument TEXT NOT NULL,
    trades INTEGER NOT NULL,
    winrate REAL NOT NULL,
    expectancy REAL NOT NULL,
    net_pnl REAL NOT NULL,
    profit_factor REAL NOT NULL,
    max_drawdown REAL NOT NULL,
    metrics_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS data_quality (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_utc TEXT NOT NULL,
    instrument TEXT,
    issue_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    details TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS system_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_utc TEXT NOT NULL,
    level TEXT NOT NULL,
    component TEXT NOT NULL,
    message TEXT NOT NULL,
    details_json TEXT NOT NULL
);
"""


__all__ = ["ACCT_COL", "SQLiteJournal"]
