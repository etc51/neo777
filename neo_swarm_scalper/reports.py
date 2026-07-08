"""Markdown and JSON reports for `neo_swarm_scalper`."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from neo_swarm_scalper.bots import ACTIVE_BOT_IDS
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
    basket_rows = storage.fetch_all(
        """
        SELECT name, status, anchor_price, close_reason, realized_pnl, unrealized_pnl
        FROM baskets
        ORDER BY opened_at DESC
        LIMIT 10
        """
    )
    spread_rows = storage.fetch_all(
        """
        SELECT instrument, best_bid, best_ask, spread_ticks
        FROM orderbook_snapshots
        ORDER BY id DESC
        LIMIT 5
        """
    )
    shadow_rows = storage.fetch_all(
        """
        SELECT instrument, stop_ticks, protection_trigger_bps, trailing_mode,
               COUNT(*) AS trades,
               SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) AS open_trades,
               SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END) AS closed_trades,
               SUM(CASE WHEN exit_reason IN ('stop_loss', 'protected_stop') THEN 1 ELSE 0 END)
                   AS stops,
               SUM(CASE WHEN protection_activated = 1 THEN 1 ELSE 0 END) AS protected,
               MAX(mfe_ticks) AS max_mfe_ticks,
               MIN(mae_ticks) AS min_mae_ticks
        FROM shadow_trades
        GROUP BY instrument, stop_ticks, protection_trigger_bps, trailing_mode
        ORDER BY instrument, stop_ticks, protection_trigger_bps, trailing_mode
        """
    )
    latest_experiments = storage.fetch_all(
        """
        SELECT instrument, stop_ticks, protection_trigger_bps, trailing_mode, trades_count,
               stops_count, winrate, expectancy, profit_factor, median_mfe, median_mae,
               p90_mfe, p95_mfe, big_runner_count
        FROM shadow_stop_experiments
        WHERE id IN (
            SELECT MAX(id)
            FROM shadow_stop_experiments
            GROUP BY instrument, stop_ticks, protection_trigger_bps, trailing_mode
        )
        ORDER BY instrument, stop_ticks, protection_trigger_bps, trailing_mode
        """
    )
    shadow_exit_summary = _shadow_exit_summary(storage)
    stop_comparison = _shadow_stop_comparison(storage)
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
        "capital_rub": str(config.simulation.total_capital),
        "reserve_rub": str(config.simulation.reserve_cash),
        "working_capital_rub": str(config.simulation.working_capital),
        "bots_count": config.simulation.virtual_accounts_count,
        "active_bots": list(ACTIVE_BOT_IDS),
        "stop_ticks": list(config.tail_catcher.stop_ticks),
        "protection_trigger_bps": [
            str(item) for item in config.tail_catcher.protection_trigger_bps
        ],
        "trailing_modes": list(config.tail_catcher.trailing_modes),
        "writes": [
            "instruments",
            "raw_orderbook_snapshots",
            "raw_trades",
            "raw_quotes",
            "candles",
            "microstructure_features",
            "volatility_features",
            "money_flow_features",
            "shadow_signals",
            "shadow_trades",
            "shadow_trade_events",
            "mfe_mae_tracking",
            "shadow_stop_experiments",
            "curator_decisions",
            "system_health",
            "errors",
        ],
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
        "baskets": [_row_dict(row) for row in basket_rows],
        "spread_slippage": [_row_dict(row) for row in spread_rows],
        "shadow_trades": [_row_dict(row) for row in shadow_rows],
        "shadow_experiments": [_row_dict(row) for row in latest_experiments],
        "shadow_exit_summary": shadow_exit_summary,
        "shadow_stop_comparison": stop_comparison,
        "next_steps": [
            "Watch data freshness and orderbook_missing warnings.",
            "Compare stop_ticks after at least 30 closed shadow trades per instrument.",
            "Review spread, protection saves, and MFE pullbacks before changing defaults.",
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
        f"- capital: {payload['capital_rub']} RUB",
        f"- reserve: {payload['reserve_rub']} RUB",
        f"- working capital: {payload['working_capital_rub']} RUB",
        f"- bots: {payload['bots_count']}",
        f"- active bots: {', '.join(payload['active_bots'])}",
        f"- stop ticks: {', '.join(str(item) for item in payload['stop_ticks'])}",
        f"- protection triggers bps: {', '.join(payload['protection_trigger_bps'])}",
        f"- trailing modes: {', '.join(payload['trailing_modes'])}",
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
            f"- simulated orders: {counts['simulated_orders']}",
            f"- simulated fills: {counts['simulated_fills']}",
            "",
            "## Bot Writes",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in payload["writes"])
    lines.extend(
        [
            "",
            "## Shadow Trades",
            "",
            "| instrument | stop_ticks | protection_bps | trailing | trades | open | "
            "closed | stops | protected | max_mfe_ticks | min_mae_ticks |",
            "| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in payload["shadow_trades"]:
        lines.append(
            f"| {row['instrument']} | {row['stop_ticks']} | {row['protection_trigger_bps']} | "
            f"{row['trailing_mode']} | {row['trades']} | {row['open_trades']} | "
            f"{row['closed_trades']} | {row['stops']} | {row['protected']} | "
            f"{row['max_mfe_ticks']} | {row['min_mae_ticks']} |"
        )
    exit_summary = payload["shadow_exit_summary"]
    stop_comparison = payload["shadow_stop_comparison"]
    lines.extend(
        [
            "",
            "## Shadow Exit Summary",
            "",
            f"- stop exits: {exit_summary['stop_exits']}",
            f"- protection exits: {exit_summary['protection_exits']}",
            f"- trailing exits: {exit_summary['trailing_exits']}",
            f"- time exits: {exit_summary['time_exits']}",
            f"- real spread_shock exits: {exit_summary['real_spread_shock_exits']}",
            f"- avoided spread_shock exits: {exit_summary['avoided_spread_shock_exits']}",
            f"- market_bad spread warnings: {exit_summary['market_bad_spread_warnings']}",
            "",
            "## Stop Tick Comparison",
            "",
            f"- status: {stop_comparison['status']}",
            f"- reason: {stop_comparison['reason']}",
            f"- best stop_ticks excluding spread_shock: {stop_comparison['best_stop_ticks']}",
            "",
            "| stop_ticks | trades | non_spread_trades | avg_pnl_ticks_non_spread | stops | "
            "protection | trailing | time | spread_shock |",
            "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in stop_comparison["rows"]:
        lines.append(
            f"| {row['stop_ticks']} | {row['trades']} | {row['non_spread_trades']} | "
            f"{row['avg_pnl_ticks_non_spread']} | {row['stop_exits']} | "
            f"{row['protection_exits']} | {row['trailing_exits']} | {row['time_exits']} | "
            f"{row['spread_shock_exits']} |"
        )
    lines.extend(
        [
            "",
            "## Stop Experiments",
            "",
            "| instrument | stop_ticks | protection_bps | trailing | trades | stops | "
            "winrate | expectancy | profit_factor | p90_mfe | p95_mfe | big_runners |",
            "| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in payload["shadow_experiments"]:
        lines.append(
            f"| {row['instrument']} | {row['stop_ticks']} | {row['protection_trigger_bps']} | "
            f"{row['trailing_mode']} | {row['trades_count']} | {row['stops_count']} | "
            f"{row['winrate']} | {row['expectancy']} | {row['profit_factor']} | "
            f"{row['p90_mfe']} | {row['p95_mfe']} | {row['big_runner_count']} |"
        )
    lines.extend(
        [
            "",
            "## Baskets",
            "",
            "| basket | status | anchor | realized_pnl | unrealized_pnl | close_reason |",
            "| --- | --- | ---: | ---: | ---: | --- |",
        ]
    )
    for row in payload["baskets"]:
        lines.append(
            f"| {row['name']} | {row['status']} | {row['anchor_price']} | "
            f"{row['realized_pnl']} | {row['unrealized_pnl']} | {row['close_reason']} |"
        )
    lines.extend(
        [
            "",
            "## Spread Slippage",
            "",
            "| instrument | best_bid | best_ask | spread_ticks |",
            "| --- | ---: | ---: | ---: |",
        ]
    )
    for row in payload["spread_slippage"]:
        lines.append(
            f"| {row['instrument']} | {row['best_bid']} | "
            f"{row['best_ask']} | {row['spread_ticks']} |"
        )
    lines.extend(
        [
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


def _shadow_exit_summary(storage: SQLiteJournal) -> dict[str, Any]:
    row = storage.fetch_all(
        """
        SELECT
            SUM(CASE WHEN exit_reason IN ('stop_loss', 'protected_stop') THEN 1 ELSE 0 END)
                AS stop_exits,
            SUM(CASE WHEN exit_reason = 'protected_exit' THEN 1 ELSE 0 END)
                AS protection_exits,
            SUM(CASE WHEN exit_reason = 'trailing_runner' THEN 1 ELSE 0 END)
                AS trailing_exits,
            SUM(CASE WHEN exit_reason = 'time_exit' THEN 1 ELSE 0 END)
                AS time_exits,
            SUM(CASE WHEN exit_reason = 'spread_shock' THEN 1 ELSE 0 END)
                AS real_spread_shock_exits
        FROM shadow_trades
        WHERE status = 'CLOSED'
        """
    )[0]
    events = storage.fetch_all(
        """
        SELECT event_type, COUNT(*) AS count
        FROM shadow_trade_events
        WHERE event_type IN ('avoided_spread_shock_exit', 'market_bad_spread_warning')
        GROUP BY event_type
        """
    )
    event_counts = {str(item["event_type"]): int(item["count"]) for item in events}
    return {
        "stop_exits": int(row["stop_exits"] or 0),
        "protection_exits": int(row["protection_exits"] or 0),
        "trailing_exits": int(row["trailing_exits"] or 0),
        "time_exits": int(row["time_exits"] or 0),
        "real_spread_shock_exits": int(row["real_spread_shock_exits"] or 0),
        "avoided_spread_shock_exits": event_counts.get("avoided_spread_shock_exit", 0),
        "market_bad_spread_warnings": event_counts.get("market_bad_spread_warning", 0),
    }


def _shadow_stop_comparison(storage: SQLiteJournal) -> dict[str, Any]:
    rows = storage.fetch_all(
        """
        SELECT
            stop_ticks,
            COUNT(*) AS trades,
            SUM(CASE WHEN exit_reason != 'spread_shock' THEN 1 ELSE 0 END)
                AS non_spread_trades,
            AVG(
                CASE
                    WHEN exit_reason != 'spread_shock'
                    THEN CASE
                        WHEN side = 'LONG'
                        THEN (exit_price - entry_price) / ((entry_price - stop_price) / stop_ticks)
                        ELSE (entry_price - exit_price) / ((stop_price - entry_price) / stop_ticks)
                    END
                    ELSE NULL
                END
            ) AS avg_pnl_ticks_non_spread,
            SUM(CASE WHEN exit_reason IN ('stop_loss', 'protected_stop') THEN 1 ELSE 0 END)
                AS stop_exits,
            SUM(CASE WHEN exit_reason = 'protected_exit' THEN 1 ELSE 0 END)
                AS protection_exits,
            SUM(CASE WHEN exit_reason = 'trailing_runner' THEN 1 ELSE 0 END)
                AS trailing_exits,
            SUM(CASE WHEN exit_reason = 'time_exit' THEN 1 ELSE 0 END)
                AS time_exits,
            SUM(CASE WHEN exit_reason = 'spread_shock' THEN 1 ELSE 0 END)
                AS spread_shock_exits
        FROM shadow_trades
        WHERE status = 'CLOSED' AND exit_price IS NOT NULL
        GROUP BY stop_ticks
        ORDER BY stop_ticks
        """
    )
    reason_rows = storage.fetch_all(
        """
        SELECT DISTINCT exit_reason
        FROM shadow_trades
        WHERE status = 'CLOSED' AND exit_reason IS NOT NULL
        """
    )
    result_rows = [_row_dict(row) for row in rows]
    non_spread_total = sum(int(row["non_spread_trades"] or 0) for row in result_rows)
    distinct_reasons = {str(row["exit_reason"]) for row in reason_rows}
    status = "VALID"
    reason = "non-spread exit sample is available"
    if not result_rows:
        status = "INVALID"
        reason = "no closed shadow trades"
    elif len(distinct_reasons) <= 1:
        status = "INVALID"
        reason = "all stop_ticks closed with one exit_reason"
    elif non_spread_total == 0:
        status = "INVALID"
        reason = "no closed trades after excluding spread_shock"
    best_stop_ticks: int | None = None
    if status == "VALID":
        candidates = [
            row
            for row in result_rows
            if row["avg_pnl_ticks_non_spread"] is not None
            and int(row["non_spread_trades"] or 0) > 0
        ]
        if candidates:
            best = max(candidates, key=lambda row: float(row["avg_pnl_ticks_non_spread"]))
            best_stop_ticks = int(best["stop_ticks"])
    return {
        "status": status,
        "reason": reason,
        "best_stop_ticks": best_stop_ticks,
        "rows": result_rows,
    }


__all__ = ["build_report_payload", "render_markdown", "write_report"]
