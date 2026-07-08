"""Dashboard-state writer for the Neobitcoin resolver."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

from neo_trader.neobitcoin_resolver.config import ResolverConfig
from neo_trader.neobitcoin_resolver.storage import ResolverJournal


def write_dashboard_state(
    *,
    config: ResolverConfig,
    journal: ResolverJournal,
    path: Path | str | None = None,
    updated_at: datetime | None = None,
) -> Path:
    target = Path(path or config.dashboard_state_path)
    payload = build_dashboard_state(config=config, journal=journal, updated_at=updated_at)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target.with_name(f".{target.name}.{uuid4().hex}.tmp")
    try:
        tmp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        os.replace(tmp_path, target)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()
    return target


def build_dashboard_state(
    *,
    config: ResolverConfig,
    journal: ResolverJournal,
    updated_at: datetime | None = None,
) -> dict[str, Any]:
    latest_heartbeat = _latest(journal, "heartbeat")
    latest_orderbook = _latest(journal, "raw_orderbook_snapshots")
    latest_features = _latest(journal, "microstructure_features")
    open_positions = [
        dict(row)
        for row in journal.fetch_all(
            """
            SELECT pair_id, bot_id, side, entry, exit, state, gross_pnl,
                   estimated_net_pnl, mfe, mae, mfi_context
            FROM positions
            WHERE state != 'CLOSED'
            ORDER BY id DESC
            """
        )
    ]
    active_pair_count = journal.active_pair_count()
    pair_metrics = _pair_metrics(journal)
    return {
        "runtime_mode": "paper-live-data",
        "paper_mode": config.paper_mode,
        "live_trading": config.live_trading,
        "multi_pair_mode": config.multi_pair_mode,
        "instrument": config.instrument.value,
        "ticker": config.ticker,
        "display_name": config.display_name,
        "commission": str(config.commission),
        "round_trip_commission": str(config.round_trip_commission),
        "updated_at": (updated_at or datetime.now(UTC)).isoformat(),
        "status": latest_heartbeat,
        "market": latest_orderbook,
        "microstructure": latest_features,
        "active_pair_count": active_pair_count,
        "entry_blocked_by_active_pair": active_pair_count > 0 and not config.multi_pair_mode,
        "open_pair_positions": open_positions,
        "pair_metrics": pair_metrics,
        "pair_total_pnl": pair_metrics[0]["pair_total_pnl"] if pair_metrics else None,
        "latest_entries": _recent(journal, "pair_entries", limit=5),
        "latest_blocked_entries": _recent(journal, "blocked_entries", limit=10),
        "resolver_decisions": _recent(journal, "resolver_decisions", limit=10),
        "protection_events": _recent(journal, "protection_events", limit=10),
        "protection_audit": _latest_protection_audit(journal),
        "pnl": _pnl(journal),
        "data_freshness": None
        if latest_heartbeat is None
        else latest_heartbeat.get("data_freshness"),
        "api_status": None if latest_heartbeat is None else latest_heartbeat.get("api_status"),
    }


def write_heartbeat_file(
    *,
    config: ResolverConfig,
    journal: ResolverJournal,
    path: Path | str | None = None,
) -> Path:
    target = Path(path or config.heartbeat_path)
    latest = _latest(journal, "heartbeat") or {}
    lines = [
        "service=neobitcoin-resolver",
        "mode=paper-live-data",
        f"instrument={config.instrument.value}",
        f"status={latest.get('bot_status', 'unknown')}",
        f"api_status={latest.get('api_status', 'unknown')}",
        f"data_freshness={latest.get('data_freshness', 'unknown')}",
        f"current_state={latest.get('current_state', 'unknown')}",
        f"current_open_pair={latest.get('current_open_pair', '') or ''}",
        f"updated_at={latest.get('timestamp', '')}",
    ]
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return target


def _latest(journal: ResolverJournal, table: str) -> dict[str, Any] | None:
    rows = journal.fetch_all(f"SELECT * FROM {table} ORDER BY id DESC LIMIT 1")
    return None if not rows else dict(rows[0])


def _recent(journal: ResolverJournal, table: str, *, limit: int) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in journal.fetch_all(f"SELECT * FROM {table} ORDER BY id DESC LIMIT ?", (limit,))
    ]


def _pnl(journal: ResolverJournal) -> dict[str, str]:
    rows = journal.fetch_all("SELECT estimated_net_pnl FROM positions WHERE state = 'CLOSED'")
    total = sum((Decimal(str(row["estimated_net_pnl"])) for row in rows), Decimal("0"))
    return {"closed_positions": str(len(rows)), "estimated_net_pnl": str(total)}


def _pair_metrics(journal: ResolverJournal) -> list[dict[str, Any]]:
    latest_positions = [
        dict(row)
        for row in journal.fetch_all(
            """
            WITH latest AS (
                SELECT pair_id, bot_id, MAX(id) AS max_id
                FROM positions
                GROUP BY pair_id, bot_id
            )
            SELECT p.*
            FROM positions p
            JOIN latest l ON p.id = l.max_id
            ORDER BY p.pair_id, p.bot_id
            """
        )
    ]
    entries = {
        row["pair_id"]: dict(row)
        for row in journal.fetch_all("SELECT * FROM pair_entries ORDER BY id")
    }
    decisions = {
        row["pair_id"]: dict(row)
        for row in journal.fetch_all(
            """
            SELECT *
            FROM resolver_decisions
            WHERE id IN (
                SELECT MAX(id)
                FROM resolver_decisions
                GROUP BY pair_id
            )
            """
        )
    }
    protection = {
        row["pair_id"]: dict(row)
        for row in journal.fetch_all(
            """
            SELECT *
            FROM protection_events
            WHERE id IN (
                SELECT MAX(id)
                FROM protection_events
                GROUP BY pair_id
            )
            """
        )
    }
    by_pair: dict[str, list[dict[str, Any]]] = {}
    for row in latest_positions:
        by_pair.setdefault(str(row["pair_id"]), []).append(row)

    metrics: list[dict[str, Any]] = []
    for pair_id, rows in sorted(by_pair.items()):
        long_row = next((row for row in rows if row["side"] == "LONG"), None)
        short_row = next((row for row in rows if row["side"] == "SHORT"), None)
        long_pnl = _decimal_field(long_row, "estimated_net_pnl")
        short_pnl = _decimal_field(short_row, "estimated_net_pnl")
        pair_total = long_pnl + short_pnl
        decision = decisions.get(pair_id, {})
        winner_side = decision.get("winner_selected")
        loser_side = decision.get("loser_closed")
        winner_pnl = _side_pnl(winner_side, long_pnl, short_pnl)
        loser_pnl = _side_pnl(loser_side, long_pnl, short_pnl)
        entry = entries.get(pair_id, {})
        protection_event = protection.get(pair_id, {})
        metrics.append(
            {
                "pair_id": pair_id,
                "opened_at": entry.get("timestamp"),
                "long_pnl": str(long_pnl),
                "short_pnl": str(short_pnl),
                "pair_total_pnl": str(pair_total),
                "winner_side": winner_side,
                "winner_pnl": None if winner_pnl is None else str(winner_pnl),
                "loser_side": loser_side,
                "loser_pnl": None if loser_pnl is None else str(loser_pnl),
                "total_after_loser_close": str(pair_total) if loser_side else None,
                "total_after_protection": str(pair_total)
                if protection_event.get("no_loss_mode_active") is not None
                else None,
                "max_pair_drawdown": str(min(_decimal_field(row, "mae") for row in rows)),
                "mfe_before_resolver": str(max(_decimal_field(row, "mfe") for row in rows)),
                "mae_before_resolver": str(min(_decimal_field(row, "mae") for row in rows)),
                "states": {
                    str(row["side"]): str(row["state"])
                    for row in rows
                },
            }
        )
    return sorted(metrics, key=lambda item: str(item.get("opened_at") or ""), reverse=True)


def _latest_protection_audit(journal: ResolverJournal) -> dict[str, Any] | None:
    row = _latest(journal, "protection_events")
    if row is None:
        return None
    raw = row.get("protection_audit_json")
    if not isinstance(raw, str) or not raw:
        return None
    loaded = json.loads(raw)
    return loaded if isinstance(loaded, dict) else None


def _decimal_field(row: dict[str, Any] | None, field: str) -> Decimal:
    if row is None or row.get(field) is None:
        return Decimal("0")
    return Decimal(str(row[field]))


def _side_pnl(
    side: object,
    long_pnl: Decimal,
    short_pnl: Decimal,
) -> Decimal | None:
    if side == "LONG":
        return long_pnl
    if side == "SHORT":
        return short_pnl
    return None


__all__ = ["build_dashboard_state", "write_dashboard_state", "write_heartbeat_file"]
