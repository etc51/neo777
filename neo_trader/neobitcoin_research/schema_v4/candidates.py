"""Deterministic research candidates and typed filter decisions."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any


def _id(prefix: str, *parts: object) -> str:
    return f"{prefix}_{hashlib.sha256('|'.join(map(str, parts)).encode()).hexdigest()[:24]}"


@dataclass(frozen=True, slots=True)
class CandidateConfig:
    spread_gate_ticks: int = 3
    max_abs_imbalance: float = 0.98
    max_abs_return_1m: float = 0.10


class CandidateMaterializer:
    FILTERS = (
        "session",
        "spread",
        "orderbook",
        "microstructure",
        "volatility_sanity",
        "data_quality",
        "curator_model",
    )

    def __init__(self, config: CandidateConfig) -> None:
        self.config = config

    def materialize(
        self, features: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        candidates: list[dict[str, Any]] = []
        decisions: list[dict[str, Any]] = []
        for feature in features:
            for side in ("LONG", "SHORT"):
                candidate_id = _id("candidate", feature["feature_snapshot_id"], side)
                checks = self._checks(feature)
                allowed = all(item[1] for item in checks)
                rejection = next((name for name, passed, _, _ in checks if not passed), None)
                common = {
                    "event_id": candidate_id,
                    "schema_version": "schema-v4.1",
                    "exchange_ts": feature["exchange_ts"],
                    "receive_ts": feature["receive_ts"],
                    "processing_ts": feature["processing_ts"],
                    "materialized_ts": feature["materialized_ts"],
                    "candidate_id": candidate_id,
                    "feature_snapshot_id": feature["feature_snapshot_id"],
                    "side": side,
                    "candidate_ts": feature["feature_ts"],
                }
                candidates.append(
                    {
                        **common,
                        "reference_price": feature["best_ask"]
                        if side == "LONG"
                        else feature["best_bid"],
                        "best_bid": feature["best_bid"],
                        "best_ask": feature["best_ask"],
                        "spread_ticks": feature["spread_ticks_decimal"],
                        "spread_ticks_decimal": feature["spread_ticks_decimal"],
                        "spread_ticks_int": feature["spread_ticks_int"],
                        "spread_price": feature["spread_price"],
                        "tick_size": feature["tick_size"],
                        "tick_grid_error": feature["tick_grid_error"],
                        "spread_threshold_ticks": self.config.spread_gate_ticks,
                        "allowed": allowed,
                        "final_decision": "ALLOW" if allowed else "REJECT",
                        "rejection_reason": f"{rejection}_gate" if rejection else None,
                    }
                )
                for name, passed, value, threshold in checks:
                    decision_id = _id("filter", candidate_id, name)
                    decisions.append(
                        {
                            **common,
                            "event_id": decision_id,
                            "filter_decision_id": decision_id,
                            "filter_name": name,
                            "passed": passed,
                            "value": value,
                            "threshold": threshold,
                            "reason": "pass" if passed else f"{name}_gate",
                        }
                    )
        return candidates, decisions

    def _checks(
        self, feature: dict[str, Any]
    ) -> list[tuple[str, bool, float | None, float | None]]:
        spread = feature.get("spread_ticks_int")
        imbalance = feature.get("imbalance_5")
        return_1m = feature.get("return_1m")
        flags = feature.get("data_quality_flags") or []
        return [
            ("session", True, 1.0, 1.0),
            (
                "spread",
                spread is not None and int(spread) <= self.config.spread_gate_ticks,
                float(spread) if spread is not None else None,
                float(self.config.spread_gate_ticks),
            ),
            (
                "orderbook",
                feature.get("best_bid") is not None and feature.get("best_ask") is not None,
                1.0,
                1.0,
            ),
            (
                "microstructure",
                imbalance is not None and abs(imbalance) <= self.config.max_abs_imbalance,
                imbalance,
                self.config.max_abs_imbalance,
            ),
            (
                "volatility_sanity",
                return_1m is not None and abs(return_1m) <= self.config.max_abs_return_1m,
                return_1m,
                self.config.max_abs_return_1m,
            ),
            ("data_quality", not flags, float(len(flags)), 0.0),
            (
                "curator_model",
                bool(feature.get("feature_ready")),
                float(bool(feature.get("feature_ready"))),
                1.0,
            ),
        ]
