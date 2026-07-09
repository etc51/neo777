"""SQLite journal for the Dual-Bot Neobitcoin Resolver."""

from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from neo_trader.neobitcoin_resolver.types import (
    GateResult,
    GateSnapshot,
    PairState,
    PositionLeg,
    PositionSide,
)


class ResolverJournal:
    """Append-only local journal with the tables required by the spec."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(SCHEMA)
            self._migrate(conn)
            conn.commit()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def fetch_all(self, query: str, params: tuple[object, ...] = ()) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return list(conn.execute(query, params))

    def active_pair_count(self) -> int:
        """Return number of pairs whose latest leg state still has open risk."""

        with self.connect() as conn:
            rows = list(conn.execute(ACTIVE_PAIR_IDS_SQL))
        return len(rows)

    def latest_active_pair_id(self) -> str | None:
        """Return the most recently updated active pair id, if any."""

        active_pair_ids = [str(row["pair_id"]) for row in self.fetch_all(ACTIVE_PAIR_IDS_SQL)]
        if not active_pair_ids:
            return None
        placeholders = ",".join("?" for _ in active_pair_ids)
        with self.connect() as conn:
            row = conn.execute(
                f"""
                SELECT pair_id
                FROM positions
                WHERE pair_id IN ({placeholders})
                GROUP BY pair_id
                ORDER BY MAX(id) DESC
                LIMIT 1
                """,
                tuple(active_pair_ids),
            ).fetchone()
        return None if row is None else str(row["pair_id"])

    def close_stale_active_pairs(self, *, keep_pair_id: str | None) -> int:
        """Close active pairs other than the selected one using their latest PnL marks."""

        active_pair_ids = [str(row["pair_id"]) for row in self.fetch_all(ACTIVE_PAIR_IDS_SQL)]
        stale_pair_ids = [pair_id for pair_id in active_pair_ids if pair_id != keep_pair_id]
        return self.close_active_pairs(stale_pair_ids)

    def close_legacy_active_pairs(self) -> int:
        """Close pre-UUID active pairs created by the old restart-local sequence."""

        active_pair_ids = [str(row["pair_id"]) for row in self.fetch_all(ACTIVE_PAIR_IDS_SQL)]
        legacy_pair_ids = [
            pair_id for pair_id in active_pair_ids if LEGACY_PAIR_ID_RE.fullmatch(pair_id)
        ]
        return self.close_active_pairs(legacy_pair_ids)

    def close_active_pairs(self, pair_ids: list[str]) -> int:
        """Append CLOSED position rows for supplied active pair ids."""

        active_pair_ids = list(dict.fromkeys(pair_ids))
        if not active_pair_ids:
            return 0
        placeholders = ",".join("?" for _ in active_pair_ids)
        now = datetime.now().isoformat()
        closed_rows = 0
        with self.connect() as conn:
            rows = list(
                conn.execute(
                    f"""
                    WITH latest AS (
                        SELECT pair_id, bot_id, MAX(id) AS max_id
                        FROM positions
                        WHERE pair_id IN ({placeholders})
                        GROUP BY pair_id, bot_id
                    )
                    SELECT p.*
                    FROM positions p
                    JOIN latest l ON p.id = l.max_id
                    WHERE p.state != 'CLOSED'
                    """,
                    tuple(active_pair_ids),
                )
            )
            for row in rows:
                conn.execute(
                    """
                    INSERT INTO positions(
                        pair_id, bot_id, side, entry, exit, state, gross_pnl,
                        estimated_net_pnl, mfe, mae, mfi_context, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, 'CLOSED', ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["pair_id"],
                        row["bot_id"],
                        row["side"],
                        row["entry"],
                        row["exit"] or row["entry"],
                        row["gross_pnl"],
                        row["estimated_net_pnl"],
                        row["mfe"],
                        row["mae"],
                        row["mfi_context"],
                        now,
                    ),
                )
                closed_rows += 1
            conn.commit()
        return closed_rows

    def load_active_pair(self, pair_id: str | None = None) -> PairState | None:
        """Rehydrate the latest active pair from the append-only journal."""

        resolved_pair_id = pair_id or self.latest_active_pair_id()
        if resolved_pair_id is None:
            return None
        with self.connect() as conn:
            entry = conn.execute(
                "SELECT * FROM pair_entries WHERE pair_id = ? ORDER BY id DESC LIMIT 1",
                (resolved_pair_id,),
            ).fetchone()
            latest_positions = list(
                conn.execute(
                    """
                    WITH latest AS (
                        SELECT bot_id, MAX(id) AS max_id
                        FROM positions
                        WHERE pair_id = ?
                        GROUP BY bot_id
                    )
                    SELECT p.*
                    FROM positions p
                    JOIN latest l ON p.id = l.max_id
                    ORDER BY p.bot_id
                    """,
                    (resolved_pair_id,),
                )
            )
            decision = conn.execute(
                """
                SELECT *
                FROM resolver_decisions
                WHERE pair_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (resolved_pair_id,),
            ).fetchone()
            protection = conn.execute(
                """
                SELECT *
                FROM protection_events
                WHERE pair_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (resolved_pair_id,),
            ).fetchone()
        if entry is None or len(latest_positions) < 2:
            return None

        by_side = {str(row["side"]): row for row in latest_positions}
        long_row = by_side.get(PositionSide.LONG.value)
        short_row = by_side.get(PositionSide.SHORT.value)
        if long_row is None or short_row is None:
            return None
        if long_row["state"] == "CLOSED" and short_row["state"] == "CLOSED":
            return None

        winner_side = None
        if decision is not None and decision["winner_selected"] is not None:
            winner_side = PositionSide(str(decision["winner_selected"]))
        spread_at_loser_close = (
            None
            if decision is None or "spread_at_loser_close" not in decision
            else (
                None
                if decision["spread_at_loser_close"] is None
                else Decimal(str(decision["spread_at_loser_close"]))
            )
        )
        protection_active = (
            protection is not None
            and int(protection["no_loss_mode_active"]) == 1
            and not (long_row["state"] == "CLOSED" and short_row["state"] == "CLOSED")
        )
        safe_exit_price = (
            None
            if protection is None or protection["safe_exit_price"] is None
            else Decimal(str(protection["safe_exit_price"]))
        )
        return PairState(
            pair_id=resolved_pair_id,
            opened_at=datetime.fromisoformat(str(entry["timestamp"])),
            entry_mid_price=(
                Decimal(str(entry["long_entry_price"])) + Decimal(str(entry["short_entry_price"]))
            )
            / Decimal("2"),
            entry_spread_ticks=Decimal(str(entry["spread_at_entry"])),
            expected_slippage_ticks=Decimal(str(entry["expected_slippage"])),
            long_leg=_leg_from_row(long_row),
            short_leg=_leg_from_row(short_row),
            gate_results=(),
            state="NO_LOSS_OR_PROFIT_ONLY"
            if protection_active
            else ("WINNER_TRAILING" if winner_side is not None else "BOTH_OPEN"),
            winner_side=winner_side,
            loser_closed=decision is not None and decision["loser_closed"] is not None,
            protection_active=protection_active,
            protection_trigger_reason=None
            if protection is None
            else protection["dynamic_trigger_reason"],
            safe_exit_price=safe_exit_price,
            protection_audit=None
            if protection is None
            else _json_loads_optional(protection["protection_audit_json"]),
            spread_at_loser_close=spread_at_loser_close,
        )

    def record_orderbook(self, snapshot: GateSnapshot) -> None:
        book = snapshot.book
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO raw_orderbook_snapshots(
                    timestamp, instrument, best_bid, best_ask, spread_ticks,
                    top1_json, top2_json, top3_json, top5_json, top10_json,
                    bid_volume, ask_volume, imbalance, walls_json, liquidity_score,
                    spoof_pull_risk
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    book.timestamp.isoformat(),
                    book.instrument.value,
                    _num(book.best_bid),
                    _num(book.best_ask),
                    _num(book.spread_ticks),
                    _levels(book, 1),
                    _levels(book, 2),
                    _levels(book, 3),
                    _levels(book, 5),
                    _levels(book, 10),
                    _num(book.bid_volume(10)),
                    _num(book.ask_volume(10)),
                    _num(book.imbalance(10)),
                    _json(_walls(book)),
                    _num(book.bid_volume(10) + book.ask_volume(10)),
                    _num(_spoof_pull_risk(book)),
                ),
            )
            conn.commit()

    def record_trade(
        self,
        *,
        timestamp: datetime,
        price: Decimal,
        volume: Decimal,
        side: str | None,
        trade_speed: Decimal,
        pressure: Decimal,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO raw_trades(
                    timestamp, price, volume, side, trade_speed, buy_sell_pressure
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    timestamp.isoformat(),
                    _num(price),
                    _num(volume),
                    side,
                    _num(trade_speed),
                    _num(pressure),
                ),
            )
            conn.commit()

    def record_microstructure(
        self,
        snapshot: GateSnapshot,
        metrics: Mapping[str, object],
    ) -> None:
        book = snapshot.book
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO microstructure_features(
                    timestamp, microprice, order_flow_imbalance, pressure_score,
                    update_speed, print_speed, volatility_short_term,
                    spread_dynamics, depth_dynamics, metrics_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    book.timestamp.isoformat(),
                    _num(book.microprice),
                    _num(book.imbalance(3)),
                    _num(metrics.get("pressure_score", Decimal("0"))),
                    _num(snapshot.update_speed_hz),
                    _num(snapshot.trade_speed_hz),
                    _num(book.volatility_60s),
                    _num(book.spread_ticks),
                    _num(book.bid_volume(10) + book.ask_volume(10)),
                    _json(metrics),
                ),
            )
            conn.commit()

    def record_pair_entry(self, pair: PairState) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO pair_entries(
                    pair_id, timestamp, long_bot_id, short_bot_id,
                    long_entry_price, short_entry_price, spread_at_entry,
                    expected_slippage, actual_fill_json, gates_json, reason_if_blocked
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    pair.pair_id,
                    pair.opened_at.isoformat(),
                    pair.long_leg.bot_id,
                    pair.short_leg.bot_id,
                    _num(pair.long_leg.entry_price),
                    _num(pair.short_leg.entry_price),
                    _num(pair.entry_spread_ticks),
                    _num(pair.expected_slippage_ticks),
                    _json(
                        {
                            "long": str(pair.long_leg.entry_price),
                            "short": str(pair.short_leg.entry_price),
                        }
                    ),
                    _json([_gate_payload(gate) for gate in pair.gate_results]),
                    None,
                ),
            )
            conn.commit()

    def record_resolver_decision(
        self,
        *,
        timestamp: datetime,
        pair: PairState,
        movement_percent: Decimal,
        decision_zone: str | None,
        trend_score: Decimal,
        orderbook_score: Decimal,
        microstructure_score: Decimal,
        continuation_up: bool,
        continuation_down: bool,
        loser_closed: str | None,
        winner_selected: str | None,
        reason: str,
        spread_at_loser_close: Decimal | None = None,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO resolver_decisions(
                    pair_id, timestamp, movement_percent, decision_zone,
                    trend_score, orderbook_score, microstructure_score,
                    continuation_up, continuation_down, loser_closed,
                    winner_selected, reason, spread_at_loser_close
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    pair.pair_id,
                    timestamp.isoformat(),
                    _num(movement_percent),
                    decision_zone,
                    _num(trend_score),
                    _num(orderbook_score),
                    _num(microstructure_score),
                    int(continuation_up),
                    int(continuation_down),
                    loser_closed,
                    winner_selected,
                    reason,
                    _num(spread_at_loser_close),
                ),
            )
            conn.commit()

    def record_position(self, pair_id: str, leg: PositionLeg) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO positions(
                    pair_id, bot_id, side, entry, exit, state, gross_pnl,
                    estimated_net_pnl, mfe, mae, mfi_context, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    pair_id,
                    leg.bot_id,
                    leg.side.value,
                    _num(leg.entry_price),
                    _num(leg.exit_price),
                    leg.state,
                    _num(leg.gross_pnl),
                    _num(leg.estimated_net_pnl),
                    _num(leg.mfe),
                    _num(leg.mae),
                    _num(leg.mfi_context),
                    datetime.now().isoformat(),
                ),
            )
            conn.commit()

    def record_protection_event(
        self,
        *,
        timestamp: datetime,
        pair: PairState,
        trigger_percent: Decimal,
        spread_at_protection: Decimal,
        expected_slippage: Decimal,
        buffer: Decimal,
        final_result: Decimal | None = None,
        protection_audit: Mapping[str, object] | None = None,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO protection_events(
                    pair_id, timestamp, winner_side, trigger_percent,
                    dynamic_trigger_reason, safe_exit_price, spread_at_protection,
                    expected_slippage, buffer, no_loss_mode_active, final_result,
                    protection_audit_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    pair.pair_id,
                    timestamp.isoformat(),
                    None if pair.winner_side is None else pair.winner_side.value,
                    _num(trigger_percent),
                    pair.protection_trigger_reason,
                    _num(pair.safe_exit_price),
                    _num(spread_at_protection),
                    _num(expected_slippage),
                    _num(buffer),
                    int(pair.protection_active),
                    _num(final_result),
                    _json(protection_audit or pair.protection_audit or {}),
                ),
            )
            conn.commit()

    def record_blocked_entry(
        self,
        *,
        timestamp: datetime,
        reason: str,
        snapshot: GateSnapshot,
        gates: tuple[GateResult, ...],
    ) -> None:
        book = snapshot.book
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO blocked_entries(
                    timestamp, reason, spread, orderbook_metrics_json,
                    microstructure_metrics_json, volatility_metrics_json,
                    pair_execution_metrics_json, gates_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    timestamp.isoformat(),
                    reason,
                    _num(book.spread_ticks),
                    _json(_gate_metrics(gates, "orderbook")),
                    _json(_gate_metrics(gates, "microstructure")),
                    _json(_gate_metrics(gates, "volatility")),
                    _json(_gate_metrics(gates, "pair_execution")),
                    _json([_gate_payload(gate) for gate in gates]),
                ),
            )
            conn.commit()

    def _migrate(self, conn: sqlite3.Connection) -> None:
        protection_columns = {
            str(row["name"]) for row in conn.execute("PRAGMA table_info(protection_events)")
        }
        if "protection_audit_json" not in protection_columns:
            conn.execute(
                "ALTER TABLE protection_events "
                "ADD COLUMN protection_audit_json TEXT NOT NULL DEFAULT '{}'"
            )
        pair_columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(pair_entries)")}
        if "pair_total_pnl" not in pair_columns:
            conn.execute("ALTER TABLE pair_entries ADD COLUMN pair_total_pnl TEXT")
        decision_columns = {
            str(row["name"]) for row in conn.execute("PRAGMA table_info(resolver_decisions)")
        }
        if "spread_at_loser_close" not in decision_columns:
            conn.execute("ALTER TABLE resolver_decisions ADD COLUMN spread_at_loser_close TEXT")

    def record_heartbeat(
        self,
        *,
        timestamp: datetime,
        bot_status: str,
        api_status: str,
        data_freshness: str,
        last_orderbook_time: datetime | None,
        last_trade_time: datetime | None,
        current_state: str,
        current_open_pair: str | None,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO heartbeat(
                    timestamp, bot_status, api_status, data_freshness,
                    last_orderbook_time, last_trade_time, current_state,
                    current_open_pair
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    timestamp.isoformat(),
                    bot_status,
                    api_status,
                    data_freshness,
                    None if last_orderbook_time is None else last_orderbook_time.isoformat(),
                    None if last_trade_time is None else last_trade_time.isoformat(),
                    current_state,
                    current_open_pair,
                ),
            )
            conn.commit()


def _num(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def _json_loads_optional(value: object) -> dict[str, object] | None:
    if not isinstance(value, str) or not value:
        return None
    loaded = json.loads(value)
    return loaded if isinstance(loaded, dict) else None


def _leg_from_row(row: sqlite3.Row) -> PositionLeg:
    leg = PositionLeg(
        bot_id=str(row["bot_id"]),
        side=PositionSide(str(row["side"])),
        entry_price=Decimal(str(row["entry"])),
        entry_time=datetime.fromisoformat(str(row["updated_at"])),
        state=str(row["state"]),
        exit_price=None if row["exit"] is None else Decimal(str(row["exit"])),
        gross_pnl=Decimal(str(row["gross_pnl"])),
        estimated_net_pnl=Decimal(str(row["estimated_net_pnl"])),
        mfe=Decimal(str(row["mfe"])),
        mae=Decimal(str(row["mae"])),
        mfi_context=None if row["mfi_context"] is None else Decimal(str(row["mfi_context"])),
    )
    return leg


def _json(value: object) -> str:
    return json.dumps(_json_safe(value), ensure_ascii=False, sort_keys=True)


def _json_safe(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _levels(book: Any, depth: int) -> str:
    return _json(
        {
            "bids": [
                {"price": level.price, "quantity": level.quantity}
                for level in book.bid_levels[:depth]
            ],
            "asks": [
                {"price": level.price, "quantity": level.quantity}
                for level in book.ask_levels[:depth]
            ],
        }
    )


def _walls(book: Any) -> dict[str, object]:
    bid_top = book.bid_volume(1)
    ask_top = book.ask_volume(1)
    bid_rest = max(book.bid_volume(10) - bid_top, Decimal("0"))
    ask_rest = max(book.ask_volume(10) - ask_top, Decimal("0"))
    return {
        "bid_wall_top1": str(bid_top) if bid_top > bid_rest else None,
        "ask_wall_top1": str(ask_top) if ask_top > ask_rest else None,
    }


def _spoof_pull_risk(book: Any) -> Decimal:
    top = book.bid_volume(1) + book.ask_volume(1)
    depth = book.bid_volume(10) + book.ask_volume(10)
    if depth == 0:
        return Decimal("1")
    return Decimal(str(min(top / depth, Decimal("1"))))


def _gate_payload(gate: GateResult) -> dict[str, object]:
    return {
        "name": gate.name.value,
        "passed": gate.passed,
        "reason": gate.reason.value,
        "metrics": gate.metrics,
    }


def _gate_metrics(gates: tuple[GateResult, ...], name: str) -> dict[str, object]:
    for gate in gates:
        if gate.name.value == name:
            return dict(gate.metrics)
    return {}


SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_orderbook_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    instrument TEXT NOT NULL,
    best_bid TEXT NOT NULL,
    best_ask TEXT NOT NULL,
    spread_ticks TEXT NOT NULL,
    top1_json TEXT NOT NULL,
    top2_json TEXT NOT NULL,
    top3_json TEXT NOT NULL,
    top5_json TEXT NOT NULL,
    top10_json TEXT NOT NULL,
    bid_volume TEXT NOT NULL,
    ask_volume TEXT NOT NULL,
    imbalance TEXT NOT NULL,
    walls_json TEXT NOT NULL,
    liquidity_score TEXT NOT NULL,
    spoof_pull_risk TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    price TEXT NOT NULL,
    volume TEXT NOT NULL,
    side TEXT,
    trade_speed TEXT NOT NULL,
    buy_sell_pressure TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS microstructure_features (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    microprice TEXT NOT NULL,
    order_flow_imbalance TEXT NOT NULL,
    pressure_score TEXT NOT NULL,
    update_speed TEXT NOT NULL,
    print_speed TEXT NOT NULL,
    volatility_short_term TEXT NOT NULL,
    spread_dynamics TEXT NOT NULL,
    depth_dynamics TEXT NOT NULL,
    metrics_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pair_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pair_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    long_bot_id TEXT NOT NULL,
    short_bot_id TEXT NOT NULL,
    long_entry_price TEXT NOT NULL,
    short_entry_price TEXT NOT NULL,
    spread_at_entry TEXT NOT NULL,
    expected_slippage TEXT NOT NULL,
    actual_fill_json TEXT NOT NULL,
    gates_json TEXT NOT NULL,
    reason_if_blocked TEXT,
    pair_total_pnl TEXT
);

CREATE TABLE IF NOT EXISTS resolver_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pair_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    movement_percent TEXT NOT NULL,
    decision_zone TEXT,
    trend_score TEXT NOT NULL,
    orderbook_score TEXT NOT NULL,
    microstructure_score TEXT NOT NULL,
    continuation_up INTEGER NOT NULL,
    continuation_down INTEGER NOT NULL,
    loser_closed TEXT,
    winner_selected TEXT,
    reason TEXT NOT NULL,
    spread_at_loser_close TEXT
);

CREATE TABLE IF NOT EXISTS positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pair_id TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    side TEXT NOT NULL,
    entry TEXT NOT NULL,
    exit TEXT,
    state TEXT NOT NULL,
    gross_pnl TEXT NOT NULL,
    estimated_net_pnl TEXT NOT NULL,
    mfe TEXT NOT NULL,
    mae TEXT NOT NULL,
    mfi_context TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS protection_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pair_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    winner_side TEXT,
    trigger_percent TEXT NOT NULL,
    dynamic_trigger_reason TEXT,
    safe_exit_price TEXT,
    spread_at_protection TEXT NOT NULL,
    expected_slippage TEXT NOT NULL,
    buffer TEXT NOT NULL,
    no_loss_mode_active INTEGER NOT NULL,
    final_result TEXT,
    protection_audit_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS blocked_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    reason TEXT NOT NULL,
    spread TEXT NOT NULL,
    orderbook_metrics_json TEXT NOT NULL,
    microstructure_metrics_json TEXT NOT NULL,
    volatility_metrics_json TEXT NOT NULL,
    pair_execution_metrics_json TEXT NOT NULL,
    gates_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS heartbeat (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    bot_status TEXT NOT NULL,
    api_status TEXT NOT NULL,
    data_freshness TEXT NOT NULL,
    last_orderbook_time TEXT,
    last_trade_time TEXT,
    current_state TEXT NOT NULL,
    current_open_pair TEXT
);
"""


ACTIVE_PAIR_IDS_SQL = """
WITH latest AS (
    SELECT pair_id, bot_id, MAX(id) AS max_id
    FROM positions
    GROUP BY pair_id, bot_id
),
latest_positions AS (
    SELECT p.pair_id, p.state
    FROM positions p
    JOIN latest l ON p.id = l.max_id
)
SELECT pair_id
FROM latest_positions
GROUP BY pair_id
HAVING SUM(CASE WHEN state != 'CLOSED' THEN 1 ELSE 0 END) > 0
"""

LEGACY_PAIR_ID_RE = re.compile(r"NBPAIR_\d{6}_[0-9a-f]{8}")


__all__ = ["ResolverJournal"]
