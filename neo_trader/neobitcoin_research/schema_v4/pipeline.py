"""Public raw-tables-only schema-v4.1 materialization orchestration."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .candidates import CandidateConfig, CandidateMaterializer
from .features import FeatureConfig, FeatureMaterializer
from .simulation import (
    ExecutionConfig,
    ExecutionSimulator,
    OutcomeMaterializer,
    StopAndExitSimulator,
)
from .timeline import CanonicalTimeline, TimelineError


@dataclass(frozen=True, slots=True)
class MaterializationResult:
    feature_snapshots: list[dict[str, Any]]
    candidate_events: list[dict[str, Any]]
    filter_decisions: list[dict[str, Any]]
    execution_simulations: list[dict[str, Any]]
    future_outcomes: list[dict[str, Any]]
    shadow_stop_results: list[dict[str, Any]]
    shadow_exit_results: list[dict[str, Any]]

    @property
    def tables(self) -> dict[str, list[dict[str, Any]]]:
        return {
            "feature_snapshots": self.feature_snapshots,
            "candidate_events": self.candidate_events,
            "filter_decisions": self.filter_decisions,
            "execution_simulations": self.execution_simulations,
            "future_outcomes": self.future_outcomes,
            "shadow_stop_results": self.shadow_stop_results,
            "shadow_exit_results": self.shadow_exit_results,
        }


class RawOnlyPipeline:
    """Materialize exactly seven derived datasets from raw tables and explicit config."""

    def __init__(
        self,
        *,
        tick_size: float,
        feature_config: FeatureConfig | None = None,
        candidate_config: CandidateConfig | None = None,
        execution_config: ExecutionConfig | None = None,
        feature_interval_seconds: int = 5,
    ) -> None:
        self.feature_config = feature_config or FeatureConfig(tick_size=tick_size)
        self.candidate_config = candidate_config or CandidateConfig()
        self.execution_config = execution_config or ExecutionConfig(tick_size=tick_size)
        if (
            self.feature_config.tick_size != tick_size
            or self.execution_config.tick_size != tick_size
        ):
            raise ValueError("all pipeline tick sizes must match")
        if self.feature_config.spread_threshold_ticks != self.candidate_config.spread_gate_ticks:
            raise ValueError("feature and candidate spread thresholds must match")
        if feature_interval_seconds <= 0:
            raise ValueError("feature_interval_seconds must be positive")
        self.feature_interval_seconds = feature_interval_seconds

    def materialize(
        self,
        raw_tables: Mapping[str, Sequence[Mapping[str, Any]]],
        *,
        candidate_start: datetime,
        candidate_end: datetime,
        materialized_ts: datetime,
    ) -> MaterializationResult:
        if (
            candidate_start.tzinfo is None
            or candidate_end.tzinfo is None
            or materialized_ts.tzinfo is None
        ):
            raise TimelineError("pipeline timestamps must be timezone-aware")
        if candidate_start >= candidate_end:
            raise TimelineError("candidate_start must be before candidate_end")
        timeline = CanonicalTimeline(raw_tables)
        eligible_events = [
            event
            for event in timeline.events("raw_orderbook")
            if candidate_start <= event.exchange_ts <= candidate_end
        ]
        buckets: dict[int, Any] = {}
        for event in eligible_events:
            bucket = int(event.exchange_ts.timestamp()) // self.feature_interval_seconds
            buckets[bucket] = event
        feature_events = [buckets[key] for key in sorted(buckets)]
        features = FeatureMaterializer(
            timeline, self.feature_config, materialized_ts=materialized_ts
        ).materialize(feature_events)
        candidates, filters = CandidateMaterializer(self.candidate_config).materialize(features)
        executions = ExecutionSimulator(timeline, self.execution_config).materialize(candidates)
        outcomes = OutcomeMaterializer(
            timeline, tick_size=self.execution_config.tick_size
        ).materialize(executions)
        stops, exits = StopAndExitSimulator(timeline, self.execution_config).materialize(executions)
        def canonical_key(row: Mapping[str, Any]) -> tuple[datetime, str]:
            return row["exchange_ts"], str(row.get("event_id") or "")

        for rows in (features, candidates, filters, executions, outcomes, stops, exits):
            rows.sort(key=canonical_key)
        return MaterializationResult(
            features, candidates, filters, executions, outcomes, stops, exits
        )

    run = materialize
