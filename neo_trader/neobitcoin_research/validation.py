"""Time-only validation, purging, embargo, and clustered uncertainty."""

from __future__ import annotations

import math
import statistics
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True)
class TimedSample:
    sample_id: str
    timestamp: datetime
    horizon_end: datetime
    cluster_id: str


@dataclass(frozen=True)
class TimeFold:
    train_indices: tuple[int, ...]
    test_indices: tuple[int, ...]


def purged_walk_forward_folds(
    samples: Sequence[TimedSample],
    *,
    folds: int,
    embargo: timedelta,
) -> tuple[TimeFold, ...]:
    """Build expanding-window folds without overlapping label horizons."""

    if folds < 1 or embargo < timedelta(0):
        raise ValueError("folds must be positive and embargo non-negative")
    ordered = sorted(enumerate(samples), key=lambda pair: pair[1].timestamp)
    if len(ordered) < folds + 1:
        raise ValueError("not enough samples for requested folds")
    block = max(1, len(ordered) // (folds + 1))
    result: list[TimeFold] = []
    for fold in range(folds):
        test_start_position = block * (fold + 1)
        test_end_position = block * (fold + 2) if fold < folds - 1 else len(ordered)
        test_pairs = ordered[test_start_position:test_end_position]
        if not test_pairs:
            continue
        test_start = test_pairs[0][1].timestamp
        cutoff = test_start - embargo
        train = tuple(
            index for index, sample in ordered[:test_start_position] if sample.horizon_end < cutoff
        )
        test = tuple(index for index, _ in test_pairs)
        result.append(TimeFold(train_indices=train, test_indices=test))
    return tuple(result)


def cluster_events(timestamps: Iterable[datetime], *, horizon: timedelta) -> tuple[str, ...]:
    """Assign overlapping opportunities to one cluster."""

    if horizon <= timedelta(0):
        raise ValueError("horizon must be positive")
    cluster = -1
    active_until: datetime | None = None
    labels: list[str] = []
    for timestamp in timestamps:
        if active_until is None or timestamp > active_until:
            cluster += 1
            active_until = timestamp + horizon
        else:
            active_until = max(active_until, timestamp + horizon)
        labels.append(f"cluster-{cluster:08d}")
    return tuple(labels)


def clustered_mean_confidence_interval(
    values: Sequence[float],
    cluster_ids: Sequence[str],
    *,
    z_score: float = 1.96,
) -> tuple[float, float, float]:
    """Return mean and normal cluster-robust confidence interval."""

    if len(values) != len(cluster_ids) or not values:
        raise ValueError("values and cluster_ids must be non-empty and aligned")
    groups: dict[str, list[float]] = defaultdict(list)
    for value, cluster_id in zip(values, cluster_ids, strict=True):
        groups[cluster_id].append(value)
    cluster_means = [statistics.fmean(group) for group in groups.values()]
    mean = statistics.fmean(values)
    if len(cluster_means) < 2:
        return mean, mean, mean
    standard_error = statistics.stdev(cluster_means) / math.sqrt(len(cluster_means))
    return mean, mean - z_score * standard_error, mean + z_score * standard_error
