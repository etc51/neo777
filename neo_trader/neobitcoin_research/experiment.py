"""Sealed time-series directional-edge experiment primitives."""

from __future__ import annotations

import hashlib
import json
import math
import random
import statistics
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

from .validation import TimedSample, clustered_mean_confidence_interval, purged_walk_forward_folds


@dataclass(frozen=True)
class ExperimentRow:
    event_id: str
    timestamp: datetime
    horizon_end: datetime
    cluster_id: str
    features: Mapping[str, float]
    aggressive_long_pnl_rub: float
    aggressive_short_pnl_rub: float


@dataclass(frozen=True)
class FoldMetric:
    model: str
    fold: int
    samples: int
    independent_clusters: int
    net_pnl_rub: float
    mean_pnl_rub: float
    profitable: bool


@dataclass(frozen=True)
class ExperimentResult:
    experiment_id: str
    configuration_hash: str
    feature_names: tuple[str, ...]
    metrics: tuple[FoldMetric, ...]
    holdout_fingerprint: str
    holdout_used_for_training: bool = False

    @property
    def profitable_fold_ratio(self) -> float:
        if not self.metrics:
            return 0.0
        return sum(metric.profitable for metric in self.metrics) / len(self.metrics)


class LinearSgd:
    """Tiny deterministic linear model suitable for transparent baselines."""

    def __init__(self, feature_names: Sequence[str], *, logistic: bool) -> None:
        self.feature_names = tuple(feature_names)
        self.logistic = logistic
        self.weights = [0.0] * (len(self.feature_names) + 1)
        self.means = [0.0] * len(self.feature_names)
        self.scales = [1.0] * len(self.feature_names)

    def fit(
        self,
        rows: Sequence[ExperimentRow],
        targets: Sequence[float],
        *,
        epochs: int = 200,
        learning_rate: float = 0.03,
        l2: float = 0.001,
    ) -> None:
        if not rows or len(rows) != len(targets):
            raise ValueError("rows and targets must be non-empty and aligned")
        columns = [[row.features.get(name, 0.0) for row in rows] for name in self.feature_names]
        self.means = [statistics.fmean(column) for column in columns]
        self.scales = [statistics.pstdev(column) or 1.0 for column in columns]
        for _ in range(epochs):
            gradient = [0.0] * len(self.weights)
            for row, target in zip(rows, targets, strict=True):
                vector = self._vector(row)
                raw = sum(
                    weight * value for weight, value in zip(self.weights, vector, strict=True)
                )
                prediction = _sigmoid(raw) if self.logistic else raw
                error = prediction - target
                for index, value in enumerate(vector):
                    gradient[index] += error * value
            scale = 1.0 / len(rows)
            for index in range(len(self.weights)):
                penalty = 0.0 if index == 0 else l2 * self.weights[index]
                self.weights[index] -= learning_rate * (gradient[index] * scale + penalty)

    def predict(self, row: ExperimentRow) -> float:
        raw = sum(
            weight * value for weight, value in zip(self.weights, self._vector(row), strict=True)
        )
        return _sigmoid(raw) if self.logistic else raw

    def _vector(self, row: ExperimentRow) -> list[float]:
        normalized = [
            (row.features.get(name, 0.0) - mean) / scale
            for name, mean, scale in zip(self.feature_names, self.means, self.scales, strict=True)
        ]
        return [1.0, *normalized]


def run_walk_forward_experiment(
    rows: Sequence[ExperimentRow],
    *,
    experiment_id: str,
    feature_names: Sequence[str],
    folds: int = 3,
    embargo: timedelta = timedelta(minutes=15),
    seed: int = 777,
) -> ExperimentResult:
    """Evaluate fixed baselines and transparent linear models out of sample."""

    ordered = sorted(rows, key=lambda row: row.timestamp)
    samples = tuple(
        TimedSample(row.event_id, row.timestamp, row.horizon_end, row.cluster_id) for row in ordered
    )
    split = purged_walk_forward_folds(samples, folds=folds, embargo=embargo)
    config = {
        "experiment_id": experiment_id,
        "features": list(feature_names),
        "folds": folds,
        "embargo_seconds": embargo.total_seconds(),
        "seed": seed,
    }
    config_hash = hashlib.sha256(json.dumps(config, sort_keys=True).encode("utf-8")).hexdigest()
    holdout_ids = [ordered[index].event_id for index in split[-1].test_indices]
    holdout_fingerprint = hashlib.sha256("\n".join(holdout_ids).encode()).hexdigest()
    metrics: list[FoldMetric] = []

    for fold_number, fold in enumerate(split):
        train = [ordered[index] for index in fold.train_indices]
        test = [ordered[index] for index in fold.test_indices]
        if not train or not test:
            continue
        majority_long = statistics.fmean(
            row.aggressive_long_pnl_rub for row in train
        ) >= statistics.fmean(row.aggressive_short_pnl_rub for row in train)
        rng = random.Random(seed + fold_number)

        long_regression = LinearSgd(feature_names, logistic=False)
        short_regression = LinearSgd(feature_names, logistic=False)
        long_classifier = LinearSgd(feature_names, logistic=True)
        short_classifier = LinearSgd(feature_names, logistic=True)
        long_regression.fit(train, [row.aggressive_long_pnl_rub for row in train])
        short_regression.fit(train, [row.aggressive_short_pnl_rub for row in train])
        long_classifier.fit(train, [float(row.aggressive_long_pnl_rub > 0) for row in train])
        short_classifier.fit(train, [float(row.aggressive_short_pnl_rub > 0) for row in train])

        def random_predict(_row: ExperimentRow, local_rng: random.Random = rng) -> str:
            return "LONG" if local_rng.random() >= 0.5 else "SHORT"

        def majority_predict(_row: ExperimentRow, use_long: bool = majority_long) -> str:
            return "LONG" if use_long else "SHORT"

        def logistic_predict(
            row: ExperimentRow,
            long_model: LinearSgd = long_classifier,
            short_model: LinearSgd = short_classifier,
        ) -> str:
            return _probability_direction(long_model.predict(row), short_model.predict(row))

        def linear_predict(
            row: ExperimentRow,
            long_model: LinearSgd = long_regression,
            short_model: LinearSgd = short_regression,
        ) -> str:
            return _pnl_direction(long_model.predict(row), short_model.predict(row))

        predictors: dict[str, Callable[[ExperimentRow], str]] = {
            "random_baseline": random_predict,
            "majority_baseline": majority_predict,
            "queue_imbalance": lambda row: _feature_direction(row, "imbalance_5"),
            "ofi": lambda row: _feature_direction(row, "multi_level_ofi"),
            "logistic_regression": logistic_predict,
            "linear_expected_pnl": linear_predict,
            "transparent_rule": _transparent_rule,
        }
        for model_name, predictor in predictors.items():
            pnl = [_decision_pnl(predictor(row), row) for row in test]
            metrics.append(
                FoldMetric(
                    model=model_name,
                    fold=fold_number,
                    samples=len(test),
                    independent_clusters=len({row.cluster_id for row in test}),
                    net_pnl_rub=sum(pnl),
                    mean_pnl_rub=statistics.fmean(pnl),
                    profitable=sum(pnl) > 0,
                )
            )

    return ExperimentResult(
        experiment_id=experiment_id,
        configuration_hash=config_hash,
        feature_names=tuple(feature_names),
        metrics=tuple(metrics),
        holdout_fingerprint=holdout_fingerprint,
    )


def edge_criteria_status(
    *,
    independent_pnl: Sequence[float],
    cluster_ids: Sequence[str],
    profitable_fold_ratio: float,
    latency_500ms_net_pnl: float,
    aggressive_net_pnl: float,
    gaps: int,
) -> dict[str, object]:
    """Evaluate only the minimum pre-confirmation gates; never promise profit."""

    mean, lower, upper = clustered_mean_confidence_interval(independent_pnl, cluster_ids)
    checks = {
        "minimum_1000_independent_signals": len(set(cluster_ids)) >= 1_000,
        "aggressive_net_pnl_positive": aggressive_net_pnl > 0,
        "latency_500ms_positive": latency_500ms_net_pnl > 0,
        "at_least_60pct_folds_profitable": profitable_fold_ratio >= 0.60,
        "clustered_ci_lower_positive": lower > 0,
        "no_critical_gaps": gaps == 0,
    }
    return {
        "edge_preliminarily_confirmed": all(checks.values()),
        "checks": checks,
        "clustered_mean_pnl": mean,
        "clustered_ci_95": [lower, upper],
        "disclaimer": "Preliminary evidence only; not a guarantee of future profit.",
    }


def _feature_direction(row: ExperimentRow, name: str) -> str:
    value = row.features.get(name, 0.0)
    if abs(value) < 1e-12:
        return "NO_TRADE"
    return "LONG" if value > 0 else "SHORT"


def _probability_direction(long_probability: float, short_probability: float) -> str:
    if max(long_probability, short_probability) < 0.55:
        return "NO_TRADE"
    return "LONG" if long_probability > short_probability else "SHORT"


def _pnl_direction(long_pnl: float, short_pnl: float) -> str:
    if max(long_pnl, short_pnl) <= 0:
        return "NO_TRADE"
    return "LONG" if long_pnl > short_pnl else "SHORT"


def _transparent_rule(row: ExperimentRow) -> str:
    imbalance = row.features.get("imbalance_5", 0.0)
    ofi = row.features.get("multi_level_ofi", 0.0)
    if imbalance > 0.15 and ofi > 0:
        return "LONG"
    if imbalance < -0.15 and ofi < 0:
        return "SHORT"
    return "NO_TRADE"


def _decision_pnl(decision: str, row: ExperimentRow) -> float:
    if decision == "LONG":
        return row.aggressive_long_pnl_rub
    if decision == "SHORT":
        return row.aggressive_short_pnl_rub
    return 0.0


def _sigmoid(value: float) -> float:
    if value >= 0:
        inverse = math.exp(-value)
        return 1 / (1 + inverse)
    exponent = math.exp(value)
    return exponent / (1 + exponent)
