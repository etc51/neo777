"""Rank instruments from readonly recording quality metrics."""

from __future__ import annotations

import json
import math
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import yaml


@dataclass(frozen=True)
class InstrumentQualityMetrics:
    """Per-instrument metrics used for universe ranking."""

    instrument_uid: str
    ticker: str = ""
    rows_total: int = 0
    events_per_minute: Decimal = Decimal("0")
    trade_notional: Decimal = Decimal("0")
    candle_volume: Decimal = Decimal("0")
    volatility_bps: Decimal = Decimal("0")
    spread_bps_p50: Decimal | None = None
    spread_bps_p90: Decimal | None = None
    spread_bps_p99: Decimal | None = None
    top5_depth: Decimal = Decimal("0")
    top10_depth: Decimal = Decimal("0")
    expected_slippage_bps: Mapping[str, Mapping[str, Decimal | None]] = field(
        default_factory=dict
    )
    stale_seconds: Decimal = Decimal("0")
    gap_count: int = 0
    usable_data_ratio: Decimal = Decimal("0")


@dataclass(frozen=True)
class UniverseScore:
    """Ranked instrument with score components."""

    instrument_uid: str
    ticker: str
    score: Decimal
    liquidity_score: Decimal
    volume_score: Decimal
    volatility_score: Decimal
    spread_penalty: Decimal
    slippage_penalty: Decimal
    stale_penalty: Decimal
    usable_data_ratio: Decimal

    def to_json_dict(self) -> dict[str, object]:
        """Return JSON/YAML-compatible score payload."""

        return {
            "instrument_uid": self.instrument_uid,
            "ticker": self.ticker,
            "score": str(self.score),
            "liquidity_score": str(self.liquidity_score),
            "volume_score": str(self.volume_score),
            "volatility_score": str(self.volatility_score),
            "spread_penalty": str(self.spread_penalty),
            "slippage_penalty": str(self.slippage_penalty),
            "stale_penalty": str(self.stale_penalty),
            "usable_data_ratio": str(self.usable_data_ratio),
        }


def rank_universe(metrics: Sequence[InstrumentQualityMetrics]) -> tuple[UniverseScore, ...]:
    """Rank instruments from best to worst using the configured score formula."""

    scores = tuple(score_instrument(metric) for metric in metrics)
    return tuple(sorted(scores, key=lambda item: (item.score, item.instrument_uid), reverse=True))


def score_instrument(metric: InstrumentQualityMetrics) -> UniverseScore:
    """Compute liquidity + volume + volatility - penalties for one instrument."""

    liquidity_score = _bounded_log_score(
        metric.top10_depth,
        multiplier=Decimal("12"),
        cap=Decimal("60"),
    )
    volume_score = _bounded_log_score(
        max(metric.trade_notional, metric.candle_volume),
        multiplier=Decimal("8"),
        cap=Decimal("50"),
    )
    volatility_score = min(metric.volatility_bps / Decimal("2"), Decimal("40"))
    spread_penalty = min(_none_to_zero(metric.spread_bps_p90), Decimal("120"))
    slippage_penalty = min(_average_slippage_penalty(metric.expected_slippage_bps), Decimal("120"))
    stale_penalty = min(
        (metric.stale_seconds / Decimal("10"))
        + (Decimal(metric.gap_count) * Decimal("2"))
        + ((Decimal("1") - metric.usable_data_ratio) * Decimal("50")),
        Decimal("120"),
    )
    score = (
        liquidity_score
        + volume_score
        + volatility_score
        - spread_penalty
        - slippage_penalty
        - stale_penalty
    )
    return UniverseScore(
        instrument_uid=metric.instrument_uid,
        ticker=metric.ticker or metric.instrument_uid,
        score=score,
        liquidity_score=liquidity_score,
        volume_score=volume_score,
        volatility_score=volatility_score,
        spread_penalty=spread_penalty,
        slippage_penalty=slippage_penalty,
        stale_penalty=stale_penalty,
        usable_data_ratio=metric.usable_data_ratio,
    )


def write_active_universe(
    path: Path | str,
    scores: Sequence[UniverseScore],
    *,
    max_instruments: int = 10,
    min_score: Decimal | None = None,
) -> Path:
    """Write ``configs/active_universe.yaml`` from ranked scores."""

    selected = _selected_scores(scores, max_instruments=max_instruments, min_score=min_score)
    payload = {
        "generated_by": "neo_trader.research.universe_selector",
        "instruments": [
            {
                "ticker": score.ticker,
                "uid": score.instrument_uid,
                "enabled": True,
                "score": str(score.score),
            }
            for score in selected
        ],
    }
    resolved_path = Path(path)
    _atomic_write_text(
        resolved_path,
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=True),
    )
    return resolved_path


def scores_to_csv_rows(scores: Sequence[UniverseScore]) -> list[dict[str, object]]:
    """Return flat CSV rows for score output."""

    rows: list[dict[str, object]] = []
    for rank, score in enumerate(scores, start=1):
        row = score.to_json_dict()
        row["rank"] = rank
        rows.append(row)
    return rows


def _selected_scores(
    scores: Sequence[UniverseScore],
    *,
    max_instruments: int,
    min_score: Decimal | None,
) -> tuple[UniverseScore, ...]:
    if max_instruments <= 0:
        raise ValueError("max_instruments must be positive.")
    filtered = [score for score in scores if min_score is None or score.score >= min_score]
    return tuple(filtered[:max_instruments])


def _bounded_log_score(value: Decimal, *, multiplier: Decimal, cap: Decimal) -> Decimal:
    numeric = max(value, Decimal("0"))
    if numeric == 0:
        return Decimal("0")
    score = Decimal(str(math.log10(float(numeric) + 1.0))) * multiplier
    return min(score, cap)


def _average_slippage_penalty(
    slippage: Mapping[str, Mapping[str, Decimal | None]],
) -> Decimal:
    values: list[Decimal] = []
    for side_values in slippage.values():
        for key in ("buy_p90", "sell_p90", "buy_p50", "sell_p50"):
            value = side_values.get(key)
            if value is not None:
                values.append(value)
                break
    if not values:
        return Decimal("0")
    return sum(values, Decimal("0")) / Decimal(len(values))


def _none_to_zero(value: Decimal | None) -> Decimal:
    return Decimal("0") if value is None else value


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        tmp_path.write_text(text, encoding="utf-8")
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def _json_default(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Mapping):
        return dict(value)
    return str(value)


def scores_to_json(scores: Sequence[UniverseScore]) -> str:
    """Serialize scores for reports."""

    return json.dumps(
        [score.to_json_dict() for score in scores],
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        default=_json_default,
    )


__all__ = [
    "InstrumentQualityMetrics",
    "UniverseScore",
    "rank_universe",
    "score_instrument",
    "scores_to_csv_rows",
    "scores_to_json",
    "write_active_universe",
]
