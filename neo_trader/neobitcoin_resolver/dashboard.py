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
    return {
        "runtime_mode": "paper-live-data",
        "paper_mode": config.paper_mode,
        "live_trading": config.live_trading,
        "instrument": config.instrument.value,
        "ticker": config.ticker,
        "display_name": config.display_name,
        "commission": str(config.commission),
        "round_trip_commission": str(config.round_trip_commission),
        "updated_at": (updated_at or datetime.now(UTC)).isoformat(),
        "status": latest_heartbeat,
        "market": latest_orderbook,
        "microstructure": latest_features,
        "open_pair_positions": open_positions,
        "latest_entries": _recent(journal, "pair_entries", limit=5),
        "latest_blocked_entries": _recent(journal, "blocked_entries", limit=10),
        "resolver_decisions": _recent(journal, "resolver_decisions", limit=10),
        "protection_events": _recent(journal, "protection_events", limit=10),
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


__all__ = ["build_dashboard_state", "write_dashboard_state", "write_heartbeat_file"]
