"""Markdown and JSON reports for `neo_swarm_scalper`."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from neo_swarm_scalper.config import NeoSwarmScalperConfig
from neo_swarm_scalper.storage import SQLiteJournal


def build_report_payload(
    *,
    storage: SQLiteJournal,
    config: NeoSwarmScalperConfig,
    runtime_start: datetime,
    generated_at: datetime | None = None,
    commit_hash: str = "unknown",
    final: bool = False,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    counts = storage.table_counts()
    bot_rows = storage.fetch_all(
        """
        SELECT bot_id, COUNT(*) AS trades, COALESCE(SUM(net_pnl), 0) AS net_pnl
        FROM trades
        GROUP BY bot_id
        ORDER BY bot_id
        """
    )
    instrument_rows = storage.fetch_all(
        """
        SELECT instrument, COUNT(*) AS trades, COALESCE(SUM(net_pnl), 0) AS net_pnl
        FROM trades
        GROUP BY instrument
        ORDER BY instrument
        """
    )
    curator_rows = storage.fetch_all(
        """
        SELECT action, reason, COUNT(*) AS count
        FROM curator_decisions
        GROUP BY action, reason
        ORDER BY count DESC, action
        LIMIT 20
        """
    )
    data_quality_rows = storage.fetch_all(
        """
        SELECT issue_type, severity, COUNT(*) AS count
        FROM data_quality
        GROUP BY issue_type, severity
        ORDER BY count DESC
        """
    )
    total_pnl = sum((Decimal(str(row["net_pnl"])) for row in bot_rows), Decimal("0"))
    best_bot = max(bot_rows, key=lambda row: Decimal(str(row["net_pnl"])), default=None)
    worst_bot = min(bot_rows, key=lambda row: Decimal(str(row["net_pnl"])), default=None)
    return {
        "generated_at": generated.isoformat(),
        "runtime_start": runtime_start.isoformat(),
        "commit_hash": commit_hash,
        "final": final,
        "mode": config.mode,
        "real_orders_disabled": not config.real_orders_enabled,
        "paper_trading_enabled": config.paper_trading_enabled,
        "token_masked": True,
        "instruments": [
            {"name": item.name, "display_name": item.display_name}
            for item in config.enabled_instruments
        ],
        "freshness": _freshness(storage, config),
        "counts": counts,
        "total_paper_pnl": str(total_pnl),
        "bots": [_row_dict(row) for row in bot_rows],
        "pnl_by_instrument": [_row_dict(row) for row in instrument_rows],
        "best_bot": None if best_bot is None else _row_dict(best_bot),
        "worst_bot": None if worst_bot is None else _row_dict(worst_bot),
        "curator_changes": [_row_dict(row) for row in curator_rows],
        "data_quality": [_row_dict(row) for row in data_quality_rows],
        "next_steps": [
            "Watch data freshness and orderbook_missing warnings.",
            "Compare expectancy after at least 30 closed trades per bot.",
            "Review spread and slippage before increasing bot weights.",
        ],
    }


def write_report(
    *,
    storage: SQLiteJournal,
    config: NeoSwarmScalperConfig,
    runtime_start: datetime,
    reports_dir: Path | str | None = None,
    generated_at: datetime | None = None,
    commit_hash: str = "unknown",
    final: bool = False,
) -> Path:
    directory = Path(reports_dir) if reports_dir is not None else config.reports.markdown_dir
    directory.mkdir(parents=True, exist_ok=True)
    generated = generated_at or datetime.now(UTC)
    suffix = generated.strftime("%Y%m%d_%H%M%S")
    path = directory / f"neo_swarm_report_{suffix}.md"
    payload = build_report_payload(
        storage=storage,
        config=config,
        runtime_start=runtime_start,
        generated_at=generated,
        commit_hash=commit_hash,
        final=final,
    )
    json_path = path.with_suffix(".json")
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    path.write_text(render_markdown(payload), encoding="utf-8")
    return path


def render_markdown(payload: dict[str, Any]) -> str:
    counts = payload["counts"]
    lines = [
        "# neo_swarm_scalper report",
        "",
        f"- runtime start time: {payload['runtime_start']}",
        f"- generated at: {payload['generated_at']}",
        f"- commit hash: {payload['commit_hash']}",
        f"- mode: {payload['mode']}",
        f"- real orders disabled: {payload['real_orders_disabled']}",
        f"- token masked: {payload['token_masked']}",
        f"- final report: {payload['final']}",
        "",
        "## Instruments",
        "",
    ]
    for item in payload["instruments"]:
        lines.append(f"- {item['display_name']} (`{item['name']}`)")
    lines.extend(
        [
            "",
            "## Counts",
            "",
            f"- market events: {counts['market_events']}",
            f"- orderbook snapshots: {counts['orderbook_snapshots']}",
            f"- candles: {counts['candles']}",
            f"- trades total: {counts['trades']}",
            f"- bot decisions: {counts['bot_decisions']}",
            f"- curator decisions: {counts['curator_decisions']}",
            f"- total paper PnL: {payload['total_paper_pnl']}",
            "",
            "## Bots",
            "",
            "| bot | trades | net_pnl |",
            "| --- | ---: | ---: |",
        ]
    )
    for row in payload["bots"]:
        lines.append(f"| {row['bot_id']} | {row['trades']} | {row['net_pnl']} |")
    lines.extend(
        [
            "",
            "## Instruments PnL",
            "",
            "| instrument | trades | net_pnl |",
            "| --- | ---: | ---: |",
        ]
    )
    for row in payload["pnl_by_instrument"]:
        lines.append(f"| {row['instrument']} | {row['trades']} | {row['net_pnl']} |")
    lines.extend(["", "## Curator Changes", ""])
    for row in payload["curator_changes"]:
        lines.append(f"- {row['action']}: {row['count']} ({row['reason']})")
    lines.extend(["", "## Data Quality", ""])
    for row in payload["data_quality"]:
        lines.append(f"- {row['severity']} {row['issue_type']}: {row['count']}")
    lines.extend(["", "## Next Steps", ""])
    lines.extend(f"- {item}" for item in payload["next_steps"])
    lines.append("")
    return "\n".join(lines)


def _freshness(storage: SQLiteJournal, config: NeoSwarmScalperConfig) -> dict[str, Any]:
    rows = storage.fetch_all(
        """
        SELECT instrument, MAX(timestamp_utc) AS last_timestamp
        FROM market_events
        GROUP BY instrument
        """
    )
    now = datetime.now(UTC)
    result: dict[str, Any] = {"stale_data_sec": config.data.stale_data_sec, "instruments": []}
    for row in rows:
        last_raw = row["last_timestamp"]
        try:
            last = datetime.fromisoformat(str(last_raw))
            age = (now - last).total_seconds()
        except ValueError:
            age = None
        result["instruments"].append(
            {
                "instrument": row["instrument"],
                "last_timestamp": last_raw,
                "stale": age is None or age > config.data.stale_data_sec,
            }
        )
    return result


def _row_dict(row: Any) -> dict[str, Any]:
    return dict(row)


__all__ = ["build_report_payload", "render_markdown", "write_report"]
