"""Analyze readonly market-data recording quality and select active universe."""

from __future__ import annotations

import argparse
import csv
import importlib
import json
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Final, cast
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from neo_trader.config_loader import load_instrument_universe_config  # noqa: E402
from neo_trader.features.orderbook import (  # noqa: E402
    depth_sum,
    expected_slippage_bps,
    spread_bps,
)
from neo_trader.features.volatility import realized_volatility  # noqa: E402
from neo_trader.research.universe_selector import (  # noqa: E402
    InstrumentQualityMetrics,
    UniverseScore,
    rank_universe,
    scores_to_csv_rows,
    write_active_universe,
)
from neo_trader.runtime import get_runtime_commit_hash  # noqa: E402

DEFAULT_TEST_SIZES: Final = (Decimal("10"), Decimal("100"), Decimal("1000"))
EVENT_TYPES: Final = ("orderbook", "trades", "candles")


@dataclass
class InstrumentAccumulator:
    """Mutable per-instrument aggregation state."""

    instrument_uid: str
    ticker: str = ""
    rows_by_type: dict[str, int] = field(default_factory=dict)
    rows_total: int = 0
    usable_rows: int = 0
    first_timestamp: datetime | None = None
    last_timestamp: datetime | None = None
    last_timestamp_by_type: dict[str, datetime] = field(default_factory=dict)
    max_stale_seconds_by_type: dict[str, Decimal] = field(default_factory=dict)
    gap_count_by_type: dict[str, int] = field(default_factory=dict)
    spreads: list[Decimal] = field(default_factory=list)
    top5_depths: list[Decimal] = field(default_factory=list)
    top10_depths: list[Decimal] = field(default_factory=list)
    slippage_values: dict[str, dict[str, list[Decimal]]] = field(default_factory=dict)
    trade_notional: Decimal = Decimal("0")
    candle_volume: Decimal = Decimal("0")
    candle_closes: list[Decimal] = field(default_factory=list)

    def observe_timestamp(
        self,
        event_type: str,
        timestamp: datetime,
        gap_threshold: Decimal,
    ) -> None:
        """Observe event timestamp, stale seconds, and gaps."""

        normalized = _as_utc(timestamp)
        if self.first_timestamp is None or normalized < self.first_timestamp:
            self.first_timestamp = normalized
        if self.last_timestamp is None or normalized > self.last_timestamp:
            self.last_timestamp = normalized

        previous = self.last_timestamp_by_type.get(event_type)
        if previous is not None:
            stale_seconds = Decimal(str(max((normalized - previous).total_seconds(), 0.0)))
            current_max = self.max_stale_seconds_by_type.get(event_type, Decimal("0"))
            self.max_stale_seconds_by_type[event_type] = max(current_max, stale_seconds)
            if stale_seconds > gap_threshold:
                self.gap_count_by_type[event_type] = self.gap_count_by_type.get(event_type, 0) + 1
        self.last_timestamp_by_type[event_type] = normalized

    def add_slippage(self, size: Decimal, side: str, value: Decimal) -> None:
        """Store one slippage observation."""

        size_key = str(size)
        self.slippage_values.setdefault(size_key, {}).setdefault(side, []).append(value)

    def to_metrics(self) -> InstrumentQualityMetrics:
        """Convert aggregation state to selector metrics."""

        return InstrumentQualityMetrics(
            instrument_uid=self.instrument_uid,
            ticker=self.ticker,
            rows_total=self.rows_total,
            events_per_minute=self.events_per_minute(),
            trade_notional=self.trade_notional,
            candle_volume=self.candle_volume,
            volatility_bps=self.volatility_bps(),
            spread_bps_p50=_quantile(self.spreads, Decimal("0.50")),
            spread_bps_p90=_quantile(self.spreads, Decimal("0.90")),
            spread_bps_p99=_quantile(self.spreads, Decimal("0.99")),
            top5_depth=_mean(self.top5_depths),
            top10_depth=_mean(self.top10_depths),
            expected_slippage_bps=self.slippage_summary(),
            stale_seconds=self.stale_seconds(),
            gap_count=sum(self.gap_count_by_type.values()),
            usable_data_ratio=self.usable_data_ratio(),
        )

    def events_per_minute(self) -> Decimal:
        """Return total events per recording minute."""

        duration = self.duration_seconds()
        if duration <= 0:
            return Decimal(self.rows_total)
        return Decimal(self.rows_total) / (duration / Decimal("60"))

    def duration_seconds(self) -> Decimal:
        """Return instrument recording duration in seconds."""

        if self.first_timestamp is None or self.last_timestamp is None:
            return Decimal("0")
        return Decimal(str(max((self.last_timestamp - self.first_timestamp).total_seconds(), 0.0)))

    def stale_seconds(self) -> Decimal:
        """Return max observed stale seconds across event types."""

        if not self.max_stale_seconds_by_type:
            return Decimal("0")
        return max(self.max_stale_seconds_by_type.values())

    def usable_data_ratio(self) -> Decimal:
        """Return usable rows over total rows."""

        if self.rows_total == 0:
            return Decimal("0")
        return Decimal(self.usable_rows) / Decimal(self.rows_total)

    def volatility_bps(self) -> Decimal:
        """Return realized close volatility in basis points."""

        if len(self.candle_closes) < 2:
            return Decimal("0")
        return realized_volatility(self.candle_closes) * Decimal("10000")

    def slippage_summary(self) -> dict[str, dict[str, Decimal | None]]:
        """Return per-size buy/sell slippage quantiles."""

        summary: dict[str, dict[str, Decimal | None]] = {}
        for size, by_side in sorted(
            self.slippage_values.items(),
            key=lambda item: Decimal(item[0]),
        ):
            row: dict[str, Decimal | None] = {}
            for side in ("buy", "sell"):
                values = by_side.get(side, [])
                row[f"{side}_p50"] = _quantile(values, Decimal("0.50"))
                row[f"{side}_p90"] = _quantile(values, Decimal("0.90"))
                row[f"{side}_p99"] = _quantile(values, Decimal("0.99"))
            summary[size] = row
        return summary


def main(argv: Sequence[str] | None = None) -> int:
    """Run recording quality analyzer."""

    parser = _build_parser()
    args = parser.parse_args(argv)
    report = analyze_recording_quality(
        raw_path=args.raw,
        reports_dir=args.reports_dir,
        active_universe_path=args.active_universe,
        instruments_config=args.instruments_config,
        test_sizes=_parse_test_sizes(args.test_sizes),
        gap_threshold_seconds=Decimal(str(args.gap_threshold_seconds)),
        top_n=args.top_n,
    )
    print(f"liquidity_report_json={report['json_path']}")
    print(f"liquidity_report_csv={report['csv_path']}")
    print(f"active_universe={report['active_universe_path']}")
    return 0


def analyze_recording_quality(
    *,
    raw_path: Path,
    reports_dir: Path,
    active_universe_path: Path,
    instruments_config: Path,
    test_sizes: Sequence[Decimal] = DEFAULT_TEST_SIZES,
    gap_threshold_seconds: Decimal = Decimal("30"),
    top_n: int = 10,
) -> dict[str, object]:
    """Analyze parquet recording data and write liquidity reports."""

    ticker_by_uid = _ticker_by_uid(instruments_config)
    allowed_uids = set(ticker_by_uid) or None
    accumulators = _load_accumulators(
        raw_path=raw_path,
        ticker_by_uid=ticker_by_uid,
        allowed_uids=allowed_uids,
        test_sizes=test_sizes,
        gap_threshold_seconds=gap_threshold_seconds,
    )
    metrics = [accumulator.to_metrics() for accumulator in accumulators.values()]
    scores = rank_universe(metrics)

    generated_at = datetime.now(UTC)
    reports_dir.mkdir(parents=True, exist_ok=True)
    timestamp = generated_at.strftime("%Y%m%d_%H%M%S")
    json_path = reports_dir / f"liquidity_report_{timestamp}.json"
    csv_path = reports_dir / f"liquidity_report_{timestamp}.csv"

    payload = _report_payload(
        generated_at=generated_at,
        raw_path=raw_path,
        test_sizes=test_sizes,
        accumulators=accumulators,
        metrics=metrics,
        scores=scores,
    )
    _atomic_write_json(json_path, payload)
    _atomic_write_csv(csv_path, _csv_rows(metrics, scores))
    write_active_universe(active_universe_path, scores, max_instruments=top_n)

    return {
        "json_path": str(json_path),
        "csv_path": str(csv_path),
        "active_universe_path": str(active_universe_path),
        "scores": scores,
        "payload": payload,
    }


def _load_accumulators(
    *,
    raw_path: Path,
    ticker_by_uid: Mapping[str, str],
    allowed_uids: set[str] | None,
    test_sizes: Sequence[Decimal],
    gap_threshold_seconds: Decimal,
) -> dict[str, InstrumentAccumulator]:
    accumulators: dict[str, InstrumentAccumulator] = {}
    for path in sorted(raw_path.rglob("*.parquet")):
        for row in _read_parquet_rows(path):
            instrument_uid = _string(row.get("instrument_uid"))
            event_type = _string(row.get("event_type"))
            if not instrument_uid or event_type not in EVENT_TYPES:
                continue
            if allowed_uids is not None and instrument_uid not in allowed_uids:
                continue
            accumulator = accumulators.setdefault(
                instrument_uid,
                InstrumentAccumulator(
                    instrument_uid=instrument_uid,
                    ticker=ticker_by_uid.get(instrument_uid, instrument_uid),
                ),
            )
            accumulator.rows_total += 1
            accumulator.rows_by_type[event_type] = accumulator.rows_by_type.get(event_type, 0) + 1
            timestamp = _row_timestamp(row)
            payload = _payload(row)
            if timestamp is None or payload is None:
                continue
            accumulator.usable_rows += 1
            accumulator.observe_timestamp(event_type, timestamp, gap_threshold_seconds)
            _observe_payload(
                accumulator=accumulator,
                event_type=event_type,
                payload=payload,
                test_sizes=test_sizes,
            )
    return accumulators


def _observe_payload(
    *,
    accumulator: InstrumentAccumulator,
    event_type: str,
    payload: Mapping[str, object],
    test_sizes: Sequence[Decimal],
) -> None:
    if event_type == "orderbook":
        _observe_orderbook(accumulator, payload, test_sizes)
    elif event_type == "trades":
        _observe_trade(accumulator, payload)
    elif event_type == "candles":
        _observe_candle(accumulator, payload)


def _observe_orderbook(
    accumulator: InstrumentAccumulator,
    payload: Mapping[str, object],
    test_sizes: Sequence[Decimal],
) -> None:
    try:
        accumulator.spreads.append(spread_bps(payload))
        accumulator.top5_depths.append(depth_sum(payload, "bid", 5) + depth_sum(payload, "ask", 5))
        accumulator.top10_depths.append(
            depth_sum(payload, "bid", 10) + depth_sum(payload, "ask", 10)
        )
    except (ValueError, TypeError, InvalidOperation):
        return

    for size in test_sizes:
        for side in ("buy", "sell"):
            try:
                accumulator.add_slippage(size, side, expected_slippage_bps(payload, side, size))
            except (ValueError, TypeError, InvalidOperation):
                continue


def _observe_trade(accumulator: InstrumentAccumulator, payload: Mapping[str, object]) -> None:
    price = _decimal_or_none(payload.get("price"))
    quantity = _decimal_or_none(payload.get("quantity"))
    if price is not None and quantity is not None:
        accumulator.trade_notional += price * quantity


def _observe_candle(accumulator: InstrumentAccumulator, payload: Mapping[str, object]) -> None:
    close = _decimal_or_none(payload.get("close"))
    volume = _decimal_or_none(payload.get("volume"))
    if close is not None:
        accumulator.candle_closes.append(close)
    if volume is not None:
        accumulator.candle_volume += volume


def _report_payload(
    *,
    generated_at: datetime,
    raw_path: Path,
    test_sizes: Sequence[Decimal],
    accumulators: Mapping[str, InstrumentAccumulator],
    metrics: Sequence[InstrumentQualityMetrics],
    scores: Sequence[UniverseScore],
) -> dict[str, object]:
    first_timestamp = _first_timestamp(accumulators.values())
    last_timestamp = _last_timestamp(accumulators.values())
    duration_seconds = Decimal("0")
    if first_timestamp is not None and last_timestamp is not None:
        duration_seconds = Decimal(
            str(max((last_timestamp - first_timestamp).total_seconds(), 0.0))
        )

    metrics_by_uid = {metric.instrument_uid: metric for metric in metrics}
    return {
        "generated_at": generated_at.isoformat(),
        "commit_hash": get_runtime_commit_hash(),
        "raw_path": str(raw_path),
        "test_sizes": [str(size) for size in test_sizes],
        "recording_duration_seconds": str(duration_seconds),
        "first_timestamp": None if first_timestamp is None else first_timestamp.isoformat(),
        "last_timestamp": None if last_timestamp is None else last_timestamp.isoformat(),
        "instruments": {
            uid: _instrument_payload(accumulators[uid], metrics_by_uid[uid])
            for uid in sorted(metrics_by_uid)
        },
        "ranked_universe": [score.to_json_dict() for score in scores],
    }


def _instrument_payload(
    accumulator: InstrumentAccumulator,
    metric: InstrumentQualityMetrics,
) -> dict[str, object]:
    return {
        "ticker": metric.ticker,
        "events_per_minute": str(metric.events_per_minute),
        "rows_total": metric.rows_total,
        "rows_by_type": {
            event_type: accumulator.rows_by_type.get(event_type, 0)
            for event_type in EVENT_TYPES
        },
        "spread_bps": {
            "p50": _decimal_to_string(metric.spread_bps_p50),
            "p90": _decimal_to_string(metric.spread_bps_p90),
            "p99": _decimal_to_string(metric.spread_bps_p99),
        },
        "top5_depth": str(metric.top5_depth),
        "top10_depth": str(metric.top10_depth),
        "expected_slippage_bps": _nested_decimal_strings(metric.expected_slippage_bps),
        "stale_seconds": str(metric.stale_seconds),
        "gap_count": metric.gap_count,
        "usable_data_ratio": str(metric.usable_data_ratio),
        "recording_duration_seconds": str(accumulator.duration_seconds()),
        "first_timestamp": (
            None if accumulator.first_timestamp is None else accumulator.first_timestamp.isoformat()
        ),
        "last_timestamp": (
            None if accumulator.last_timestamp is None else accumulator.last_timestamp.isoformat()
        ),
    }


def _csv_rows(
    metrics: Sequence[InstrumentQualityMetrics],
    scores: Sequence[UniverseScore],
) -> list[dict[str, object]]:
    metrics_by_uid = {metric.instrument_uid: metric for metric in metrics}
    rows: list[dict[str, object]] = []
    for row in scores_to_csv_rows(scores):
        metric = metrics_by_uid[_string(row["instrument_uid"])]
        row.update(
            {
                "rows_total": metric.rows_total,
                "events_per_minute": str(metric.events_per_minute),
                "spread_bps_p50": _decimal_to_string(metric.spread_bps_p50),
                "spread_bps_p90": _decimal_to_string(metric.spread_bps_p90),
                "spread_bps_p99": _decimal_to_string(metric.spread_bps_p99),
                "top5_depth": str(metric.top5_depth),
                "top10_depth": str(metric.top10_depth),
                "stale_seconds": str(metric.stale_seconds),
                "gap_count": metric.gap_count,
            }
        )
        rows.append(row)
    return rows


def _read_parquet_rows(path: Path) -> list[Mapping[str, object]]:
    try:
        pq = cast(Any, importlib.import_module("pyarrow.parquet"))
        rows = pq.read_table(path).to_pylist()
    except Exception:
        return []
    return [cast(Mapping[str, object], row) for row in rows if isinstance(row, Mapping)]


def _payload(row: Mapping[str, object]) -> Mapping[str, object] | None:
    raw = row.get("payload_json")
    if not isinstance(raw, str):
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, Mapping):
        return None
    return cast(Mapping[str, object], payload)


def _row_timestamp(row: Mapping[str, object]) -> datetime | None:
    for key in ("received_at", "event_time"):
        value = row.get(key)
        if isinstance(value, str):
            return _parse_datetime(value)
        if isinstance(value, datetime):
            return _as_utc(value)
    return None


def _ticker_by_uid(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    try:
        config = load_instrument_universe_config(path)
    except Exception:
        return {}
    return {
        instrument.uid: instrument.ticker
        for instrument in config.instruments
        if instrument.uid.strip()
    }


def _parse_test_sizes(value: str) -> tuple[Decimal, ...]:
    sizes = tuple(Decimal(item.strip()) for item in value.split(",") if item.strip())
    if not sizes:
        raise ValueError("--test-sizes must contain at least one quantity.")
    if any(size <= 0 for size in sizes):
        raise ValueError("--test-sizes must contain positive quantities.")
    return sizes


def _parse_datetime(value: str) -> datetime | None:
    try:
        return _as_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        return None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _decimal_or_none(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        if isinstance(value, Decimal):
            return value
        if isinstance(value, int | str):
            return Decimal(value)
        if isinstance(value, float):
            return Decimal(str(value))
    except InvalidOperation:
        return None
    return None


def _quantile(values: Sequence[Decimal], quantile: Decimal) -> Decimal | None:
    if not values:
        return None
    if not (Decimal("0") <= quantile <= Decimal("1")):
        raise ValueError("quantile must be between 0 and 1.")
    ordered = sorted(values)
    index = int((Decimal(len(ordered) - 1) * quantile).to_integral_value())
    return ordered[index]


def _mean(values: Sequence[Decimal]) -> Decimal:
    if not values:
        return Decimal("0")
    return sum(values, Decimal("0")) / Decimal(len(values))


def _first_timestamp(accumulators: Sequence[InstrumentAccumulator]) -> datetime | None:
    values = [item.first_timestamp for item in accumulators if item.first_timestamp is not None]
    return min(values) if values else None


def _last_timestamp(accumulators: Sequence[InstrumentAccumulator]) -> datetime | None:
    values = [item.last_timestamp for item in accumulators if item.last_timestamp is not None]
    return max(values) if values else None


def _nested_decimal_strings(
    value: Mapping[str, Mapping[str, Decimal | None]],
) -> dict[str, dict[str, str | None]]:
    return {
        outer_key: {
            inner_key: _decimal_to_string(inner_value)
            for inner_key, inner_value in inner.items()
        }
        for outer_key, inner in value.items()
    }


def _decimal_to_string(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _string(value: object) -> str:
    return "" if value is None else str(value)


def _atomic_write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        tmp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def _atomic_write_csv(path: Path, rows: Sequence[Mapping[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    fieldnames = sorted({key for row in rows for key in row})
    try:
        with tmp_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze readonly recording quality")
    parser.add_argument("--raw", type=Path, default=Path("data/raw"))
    parser.add_argument("--reports-dir", type=Path, default=Path("data/reports"))
    parser.add_argument(
        "--active-universe",
        type=Path,
        default=Path("configs/active_universe.yaml"),
    )
    parser.add_argument(
        "--instruments-config",
        type=Path,
        default=Path("configs/instruments.yaml"),
    )
    parser.add_argument("--test-sizes", default="10,100,1000")
    parser.add_argument("--gap-threshold-seconds", type=float, default=30.0)
    parser.add_argument("--top-n", type=int, default=10)
    return parser


if __name__ == "__main__":
    raise SystemExit(main())
