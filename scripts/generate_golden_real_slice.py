"""Render the schema-v4.1 point-in-time candle/OFI/spread audit."""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import pyarrow.parquet as pq  # type: ignore[import-untyped]


def _read(data: Path, name: str) -> list[dict[str, Any]]:
    return cast(list[dict[str, Any]], pq.read_table(data / f"{name}.parquet").to_pylist())


def _ts(row: dict[str, Any]) -> datetime:
    return cast(datetime, row.get("feature_ts") or row["exchange_ts"])


def _show(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if value is None:
        return "null"
    if isinstance(value, list):
        return ", ".join(map(str, value)) or "[]"
    return str(value)


def _pick_evenly(rows: list[dict[str, Any]], count: int) -> list[dict[str, Any]]:
    if len(rows) < count:
        raise RuntimeError(f"need {count} feature timestamps, found {len(rows)}")
    if count == 1:
        return [rows[0]]
    indexes = [round(index * (len(rows) - 1) / (count - 1)) for index in range(count)]
    return [rows[index] for index in indexes]


def generate(data: Path, output: Path, count: int = 10) -> None:
    """Write an audit whose candle details come from each feature's exact source IDs."""

    if count < 10:
        raise ValueError("schema-v4.1 golden real audit requires at least 10 timestamps")
    features = sorted(_read(data, "feature_snapshots"), key=_ts)
    ready = [row for row in features if bool(row.get("feature_ready"))]
    selected = _pick_evenly(ready or features, count)
    candles = {
        minutes: {row["event_id"]: row for row in _read(data, f"candles_{minutes}m")}
        for minutes in (1, 5, 15)
    }
    lines = [
        "# Golden real candle audit (schema-v4.1)",
        "",
        "Generated only from captured local Parquet. Every candle block follows the exact "
        "source event ID recorded by the feature row; it does not select a candle again.",
        "",
        f"- Feature timestamps: {len(selected)}",
        f"- Ready timestamps available: {len(ready)}",
        "- Timeframes audited: 1m, 5m, 15m",
        "",
    ]
    for number, feature in enumerate(selected, 1):
        lines.extend(
            [
                f"## {number}. {_show(_ts(feature))}",
                "",
                f"- Feature ID: `{_show(feature.get('feature_snapshot_id'))}`",
                f"- Readiness: {_show(feature.get('feature_ready'))}; "
                f"checks={_show(feature.get('readiness_checks_passed'))}/"
                f"{_show(feature.get('readiness_checks_total'))}; "
                f"version={_show(feature.get('readiness_version'))}",
                f"- Missing fields: {_show(feature.get('missing_fields'))}",
                f"- Trend/regime: {_show(feature.get('trend'))} / "
                f"{_show(feature.get('regime'))}",
                f"- OFI: {_show(feature.get('ofi'))}; delta={_show(feature.get('ofi_delta'))}; "
                f"source=`{_show(feature.get('ofi_source_event_id'))}`; "
                f"previous=`{_show(feature.get('ofi_previous_event_id'))}`; "
                f"continuity={_show(feature.get('ofi_continuity_valid'))}",
                f"- Spread: price={_show(feature.get('spread_price', feature.get('spread')))}; "
                f"tick={_show(feature.get('tick_size'))}; "
                f"decimal={_show(feature.get('spread_ticks_decimal', feature.get('spread_ticks')))}; "  # noqa: E501
                f"integer={_show(feature.get('spread_ticks_int'))}; "
                f"grid_error={_show(feature.get('tick_grid_error'))}; "
                f"threshold={_show(feature.get('spread_threshold_ticks'))}; "
                f"gate={_show(feature.get('spread_gate_passed'))}",
                "",
            ]
        )
        for minutes in (1, 5, 15):
            prefix = f"candle_{minutes}m"
            source_id = feature.get(f"{prefix}_source_event_id")
            candle = candles[minutes].get(source_id, {})
            lines.extend(
                [
                    f"### {minutes}m candle",
                    "",
                    f"- Source event ID: `{_show(source_id)}`",
                    f"- Source/canonical complete: "
                    f"{_show(feature.get(f'{prefix}_source_is_complete', candle.get('is_complete')))} / "  # noqa: E501
                    f"{_show(feature.get(f'{prefix}_canonical_is_complete'))}",
                    f"- Start/end: "
                    f"{_show(feature.get(f'{prefix}_start', candle.get('candle_start')))} / "
                    f"{_show(feature.get(f'{prefix}_end', candle.get('candle_end')))}",
                    f"- Revision/receive: "
                    f"{_show(feature.get(f'{prefix}_revision', candle.get('revision')))} / "
                    f"{_show(feature.get(f'{prefix}_revision_receive_ts', candle.get('receive_ts')))}",  # noqa: E501
                    f"- Interval/receive age ms: "
                    f"{_show(feature.get(f'{prefix}_interval_age_ms'))} / "
                    f"{_show(feature.get(f'{prefix}_receive_age_ms'))}",
                    f"- Stale: {_show(feature.get(f'{prefix}_stale'))}",
                    f"- Volume/return/ATR: {_show(feature.get(f'candle_volume_{minutes}m'))} / "
                    f"{_show(feature.get(f'return_{minutes}m'))} / "
                    f"{_show(feature.get(f'atr_{minutes}m'))}",
                    "",
                ]
            )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("data", type=Path)
    parser.add_argument(
        "--output", type=Path, default=Path("reports/golden_real_candle_audit.md")
    )
    parser.add_argument("--count", type=int, default=10)
    args = parser.parse_args()
    generate(args.data, args.output, args.count)


if __name__ == "__main__":
    main()
