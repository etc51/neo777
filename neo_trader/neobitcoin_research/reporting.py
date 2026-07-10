"""Daily quality and execution reporting for Neobitcoin research storage."""

from __future__ import annotations

import json
import math
import os
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from statistics import fmean, median
from typing import Any, TypeAlias
from uuid import uuid4

from neo_trader.neobitcoin_research.storage import ResearchStorage

JsonMapping: TypeAlias = Mapping[str, Any]


@dataclass(frozen=True)
class DailyResearchReport:
    """Serializable daily data-quality and execution summary."""

    report_date: date
    generated_at: datetime
    quality: Mapping[str, Any]
    execution: Mapping[str, Any]

    def to_json_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""

        return {
            "report_date": self.report_date.isoformat(),
            "generated_at": _as_utc(self.generated_at).isoformat(),
            "quality": dict(self.quality),
            "execution": dict(self.execution),
        }


@dataclass(frozen=True)
class DailyReportPaths:
    """Paths atomically written for one daily report."""

    json_path: Path
    markdown_path: Path


def build_daily_research_report(
    storage: ResearchStorage,
    report_date: date | str,
    *,
    generated_at: datetime | None = None,
) -> DailyResearchReport:
    """Aggregate raw WAL quality and derived execution simulations."""

    day = _parse_date(report_date)
    raw_events = list(storage.iter_raw_events(day))
    execution_records = list(storage.iter_derived("execution_simulations", day))
    control = storage.daily_control_metrics(day)
    return DailyResearchReport(
        report_date=day,
        generated_at=_as_utc(generated_at or datetime.now(UTC)),
        quality=_build_quality_metrics(raw_events, control),
        execution=_build_execution_metrics(execution_records),
    )


def write_daily_research_report(
    report: DailyResearchReport,
    *,
    reports_dir: Path | str,
) -> DailyReportPaths:
    """Atomically write matching JSON and Markdown daily report files."""

    directory = Path(reports_dir)
    directory.mkdir(parents=True, exist_ok=True)
    stem = f"neobitcoin_research_{report.report_date.isoformat()}"
    json_path = directory / f"{stem}.json"
    markdown_path = directory / f"{stem}.md"
    json_tmp = directory / f".{json_path.name}.{uuid4().hex}.tmp"
    markdown_tmp = directory / f".{markdown_path.name}.{uuid4().hex}.tmp"
    try:
        json_tmp.write_text(
            json.dumps(
                report.to_json_dict(),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        markdown_tmp.write_text(render_daily_report_markdown(report), encoding="utf-8")
        os.replace(json_tmp, json_path)
        os.replace(markdown_tmp, markdown_path)
    finally:
        if json_tmp.exists():
            json_tmp.unlink()
        if markdown_tmp.exists():
            markdown_tmp.unlink()
    return DailyReportPaths(json_path=json_path, markdown_path=markdown_path)


def generate_daily_research_report(
    storage: ResearchStorage,
    report_date: date | str,
    *,
    reports_dir: Path | str,
    generated_at: datetime | None = None,
) -> DailyReportPaths:
    """Build and atomically write one daily Markdown+JSON report pair."""

    report = build_daily_research_report(storage, report_date, generated_at=generated_at)
    return write_daily_research_report(report, reports_dir=reports_dir)


def render_daily_report_markdown(report: DailyResearchReport) -> str:
    """Render a compact human-readable daily report."""

    quality = report.quality
    execution = report.execution
    event_counts = _mapping(quality.get("event_counts"))
    lines = [
        f"# Neobitcoin research daily report — {report.report_date.isoformat()}",
        "",
        f"Generated at: `{_as_utc(report.generated_at).isoformat()}`",
        "",
        "## Data quality",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Uptime seconds | {_format_number(quality.get('uptime_seconds'))} |",
        f"| Total messages | {_format_number(quality.get('events_total'))} |",
        f"| Order books | {_format_number(event_counts.get('orderbook', 0))} |",
        f"| Trades | {_format_number(event_counts.get('trades', 0))} |",
        f"| Candles | {_format_number(event_counts.get('candles', 0))} |",
        f"| Reconnects | {_format_number(quality.get('reconnects'))} |",
        f"| Data gaps | {_format_number(quality.get('data_gaps'))} |",
        f"| Gap duration seconds | {_format_number(quality.get('gap_duration_seconds'))} |",
        f"| Consistent fraction | {_format_number(quality.get('consistent_fraction'))} |",
        f"| Stale fraction | {_format_number(quality.get('stale_fraction'))} |",
        f"| Latency p50 ms | {_format_number(_nested(quality, 'latency_ms', 'p50'))} |",
        f"| Latency p95 ms | {_format_number(_nested(quality, 'latency_ms', 'p95'))} |",
        f"| Latency p99 ms | {_format_number(_nested(quality, 'latency_ms', 'p99'))} |",
        f"| Excluded from analysis | {_format_number(quality.get('excluded_events'))} |",
        "",
        "### Exclusion reasons",
        "",
    ]
    exclusion_reasons = _mapping(quality.get("exclusion_reasons"))
    if exclusion_reasons:
        lines.extend(
            f"- `{reason}`: {count}" for reason, count in sorted(exclusion_reasons.items())
        )
    else:
        lines.append("- none")

    lines.extend(
        [
            "",
            "## Execution simulation",
            "",
            "| Model | Samples | Fill rate | Partial rate | No-fill rate | "
            "Median fill ms | P95 fill ms | Avg levels | Avg slippage | Net PnL |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    models = _mapping(execution.get("models"))
    if models:
        for model, raw_metrics in sorted(models.items()):
            metrics = _mapping(raw_metrics)
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(model),
                        _format_number(metrics.get("samples")),
                        _format_number(metrics.get("fill_rate")),
                        _format_number(metrics.get("partial_fill_rate")),
                        _format_number(metrics.get("no_fill_rate")),
                        _format_number(metrics.get("median_fill_time_ms")),
                        _format_number(metrics.get("p95_fill_time_ms")),
                        _format_number(metrics.get("average_levels_consumed")),
                        _format_number(metrics.get("average_slippage")),
                        _format_number(metrics.get("net_pnl")),
                    ]
                )
                + " |"
            )
    else:
        lines.append("| no samples | 0 | — | — | — | — | — | — | — | — |")
    lines.extend(
        [
            "",
            f"Total simulations: **{_format_number(execution.get('simulations_total'))}**",
            "",
        ]
    )
    return "\n".join(lines)


def _build_quality_metrics(
    events: Sequence[JsonMapping],
    control: JsonMapping,
) -> dict[str, Any]:
    counts: Counter[str] = Counter()
    timestamps: list[datetime] = []
    latencies: list[float] = []
    consistent_values: list[bool] = []
    stale_values: list[bool] = []
    exclusion_reasons: Counter[str] = Counter()
    depth_values: list[float] = []

    for event in events:
        event_type = _event_type_bucket(str(event.get("event_type", "unknown")))
        counts[event_type] += 1
        timestamp = _optional_timestamp(
            event.get("exchange_timestamp") or event.get("receive_timestamp")
        )
        if timestamp is not None:
            timestamps.append(timestamp)
        latency = _number(event.get("latency_ms"))
        if latency is not None:
            latencies.append(latency)
        payload = _mapping(event.get("payload"))
        consistent = _optional_boolean(event.get("is_consistent"))
        if consistent is None:
            consistent = _optional_boolean(payload.get("is_consistent"))
        if consistent is not None:
            consistent_values.append(consistent)
        stale = _optional_boolean(event.get("is_stale"))
        if stale is None:
            stale = _optional_boolean(event.get("stale"))
        if stale is None:
            stale = _optional_boolean(payload.get("is_stale"))
        if stale is not None:
            stale_values.append(stale)
        depth = _number(event.get("orderbook_depth"))
        if depth is None:
            depth = _number(payload.get("depth"))
        if depth is not None:
            depth_values.append(depth)

        explicit_reason = event.get("exclusion_reason") or payload.get("exclusion_reason")
        explicitly_excluded = _optional_boolean(event.get("excluded_from_analysis")) is True
        if explicit_reason:
            exclusion_reasons[str(explicit_reason)] += 1
        elif explicitly_excluded:
            exclusion_reasons["explicit_exclusion"] += 1
        elif consistent is False:
            exclusion_reasons["inconsistent"] += 1
        elif stale is True:
            exclusion_reasons["stale"] += 1

    uptime_seconds = 0.0
    if len(timestamps) >= 2:
        uptime_seconds = max((max(timestamps) - min(timestamps)).total_seconds(), 0.0)
    consistent_fraction = (
        sum(consistent_values) / len(consistent_values) if consistent_values else None
    )
    stale_fraction = sum(stale_values) / len(stale_values) if stale_values else None
    return {
        "events_total": len(events),
        "event_counts": dict(sorted(counts.items())),
        "uptime_seconds": uptime_seconds,
        "stream_sessions": int(control.get("stream_sessions", 0)),
        "reconnects": int(control.get("reconnects", 0)),
        "subscription_events": int(control.get("subscription_events", 0)),
        "data_gaps": int(control.get("data_gaps", 0)),
        "gap_duration_seconds": float(control.get("gap_duration_seconds", 0.0)),
        "consistent_fraction": consistent_fraction,
        "stale_fraction": stale_fraction,
        "latency_ms": _percentiles(latencies),
        "orderbook_depth": {
            "minimum": min(depth_values) if depth_values else None,
            "maximum": max(depth_values) if depth_values else None,
            "average": fmean(depth_values) if depth_values else None,
        },
        "excluded_events": sum(exclusion_reasons.values()),
        "exclusion_reasons": dict(sorted(exclusion_reasons.items())),
    }


def _build_execution_metrics(records: Sequence[JsonMapping]) -> dict[str, Any]:
    grouped: dict[str, list[JsonMapping]] = defaultdict(list)
    for record in records:
        model = record.get("model") or record.get("execution_model") or "unknown"
        grouped[_enum_value(model)].append(record)

    model_metrics: dict[str, dict[str, Any]] = {}
    for model, samples in grouped.items():
        statuses = [_execution_status(sample) for sample in samples]
        full = sum(status == "filled" for status in statuses)
        partial = sum(status == "partial" for status in statuses)
        no_fill = sum(status == "no_fill" for status in statuses)
        fill_times = _numbers(
            _first(sample, "fill_time_ms", "elapsed_ms", "time_to_fill_ms")
            for sample in samples
            if _execution_status(sample) != "no_fill"
        )
        levels = _numbers(_first(sample, "levels_consumed", "levels") for sample in samples)
        sweep_costs = _numbers(
            _first(sample, "sweep_cost", "sweep_cost_ticks") for sample in samples
        )
        slippages = _numbers(_first(sample, "slippage", "slippage_ticks") for sample in samples)
        adverse = _numbers(
            _first(sample, "adverse_selection", "adverse_selection_ticks") for sample in samples
        )
        pnl_values = _numbers(
            _first(sample, "net_pnl", "pnl", "realized_pnl") for sample in samples
        )
        count = len(samples)
        model_metrics[model] = {
            "samples": count,
            "fill_rate": full / count if count else None,
            "partial_fill_rate": partial / count if count else None,
            "no_fill_rate": no_fill / count if count else None,
            "average_fill_time_ms": fmean(fill_times) if fill_times else None,
            "median_fill_time_ms": median(fill_times) if fill_times else None,
            "p95_fill_time_ms": _percentile(fill_times, 0.95),
            "average_levels_consumed": fmean(levels) if levels else None,
            "average_sweep_cost": fmean(sweep_costs) if sweep_costs else None,
            "average_slippage": fmean(slippages) if slippages else None,
            "average_adverse_selection": fmean(adverse) if adverse else None,
            "net_pnl": sum(pnl_values) if pnl_values else 0.0,
            "average_pnl": fmean(pnl_values) if pnl_values else None,
        }
    return {
        "simulations_total": len(records),
        "models": dict(sorted(model_metrics.items())),
    }


def _execution_status(record: JsonMapping) -> str:
    raw_status = _enum_value(record.get("status") or record.get("fill_status") or "")
    filled = _number(record.get("filled_quantity"))
    requested = _number(record.get("requested_quantity"))
    if raw_status in {"filled", "full", "fully_filled"}:
        return "filled"
    if raw_status in {"partial", "partially_filled"}:
        return "partial"
    if raw_status in {"no_fill", "unfilled", "timeout", "rejected"}:
        return "no_fill"
    if filled is not None and requested is not None and requested > 0:
        if filled >= requested:
            return "filled"
        if filled > 0:
            return "partial"
        return "no_fill"
    return "no_fill"


def _event_type_bucket(value: str) -> str:
    lowered = _enum_value(value)
    if "orderbook" in lowered or "order_book" in lowered:
        return "orderbook"
    if "trade" in lowered:
        return "trades"
    if "candle" in lowered:
        return "candles"
    if "last_price" in lowered:
        return "last_prices"
    if "status" in lowered:
        return "trading_status"
    return lowered or "unknown"


def _percentiles(values: Sequence[float]) -> dict[str, float | None]:
    return {
        "p50": _percentile(values, 0.50),
        "p95": _percentile(values, 0.95),
        "p99": _percentile(values, 0.99),
    }


def _percentile(values: Sequence[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _numbers(values: Iterable[Any]) -> list[float]:
    return [number for value in values if (number := _number(value)) is not None]


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _optional_boolean(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
    return None


def _optional_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _as_utc(value)
    if isinstance(value, str):
        try:
            return _as_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
        except ValueError:
            return None
    return None


def _parse_date(value: date | str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _nested(mapping: Mapping[str, Any], key: str, nested_key: str) -> Any:
    return _mapping(mapping.get(key)).get(nested_key)


def _first(mapping: JsonMapping, *keys: str) -> Any:
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def _enum_value(value: Any) -> str:
    raw = getattr(value, "value", value)
    normalized = str(raw).strip().lower()
    return normalized.rsplit(".", 1)[-1]


def _format_number(value: Any) -> str:
    if value is None:
        return "—"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"{value:.6g}"
    return str(value)


__all__ = [
    "DailyReportPaths",
    "DailyResearchReport",
    "build_daily_research_report",
    "generate_daily_research_report",
    "render_daily_report_markdown",
    "write_daily_research_report",
]
