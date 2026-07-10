"""Configuration and experiment lineage for the read-only research service."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Final

SAFE_FALSE: Final = frozenset({"0", "false", "no", "off"})


class UnsafeResearchConfiguration(ValueError):
    """Raised before startup when a setting could permit live execution."""


@dataclass(frozen=True)
class ResearchConfig:
    """Immutable runtime settings.

    The monetary sizes are ruble notionals. Quantities are derived only after
    instrument discovery confirms the current lot size and price.
    """

    data_root: Path = Path("data/neobitcoin_research")
    reports_root: Path = Path("reports/neobitcoin_research")
    token_path: Path | None = None
    instrument_query: str = "Neo Bitcoin"
    requested_depth: int = 50
    minimum_usable_depth: int = 20
    snapshot_interval_seconds: float = 1.0
    large_trade_lots: int = 10
    stale_after_seconds: float = 10.0
    reconnect_initial_seconds: float = 1.0
    reconnect_max_seconds: float = 60.0
    subscription_check_seconds: float = 60.0
    raw_schema_version: str = "neobitcoin-raw-v1"
    feature_schema_version: str = "neobitcoin-features-v1"
    target_schema_version: str = "neobitcoin-targets-v1"
    experiment_id: str = "directional-edge-v1"
    random_seed: int = 777
    position_sizes_rub: tuple[int, ...] = (10_000, 50_000, 100_000)
    latencies_ms: tuple[int, ...] = (0, 100, 250, 500, 1_000)
    horizons_seconds: tuple[int, ...] = (5, 30, 60, 180, 300, 900)
    adverse_slippage_ticks: float = 0.0
    adverse_slippage_bps: float = 0.0
    adverse_slippage_rub: float = 0.0
    daily_holding_fee_annual_rate: float | None = None
    signal_cooldown_seconds: int = 900
    wal_fsync: bool = True
    compact_closed_hours: bool = True
    real_orders_enabled: bool = False
    trading_mode: str = "readonly"
    extra: Mapping[str, object] = field(default_factory=dict)

    def validate(self) -> None:
        if self.real_orders_enabled:
            raise UnsafeResearchConfiguration("real_orders_enabled must remain false")
        if self.trading_mode.lower() not in {"readonly", "shadow", "research"}:
            raise UnsafeResearchConfiguration("trading_mode must be readonly/shadow/research")
        if self.requested_depth not in {20, 30, 40, 50}:
            raise ValueError("requested_depth must be one of 20, 30, 40, 50")
        if self.minimum_usable_depth < 20:
            raise ValueError("minimum_usable_depth cannot be below 20")
        if self.snapshot_interval_seconds <= 0 or self.stale_after_seconds <= 0:
            raise ValueError("snapshot and stale intervals must be positive")
        if self.reconnect_initial_seconds <= 0:
            raise ValueError("reconnect_initial_seconds must be positive")
        if self.reconnect_max_seconds < self.reconnect_initial_seconds:
            raise ValueError("reconnect_max_seconds must be >= reconnect_initial_seconds")
        if any(value <= 0 for value in self.position_sizes_rub):
            raise ValueError("position sizes must be positive")
        if any(value < 0 for value in self.latencies_ms):
            raise ValueError("latencies must be non-negative")
        if any(value <= 0 for value in self.horizons_seconds):
            raise ValueError("horizons must be positive")
        if (
            self.daily_holding_fee_annual_rate is not None
            and self.daily_holding_fee_annual_rate < 0
        ):
            raise ValueError("daily holding fee rate must be non-negative")

    @property
    def configuration_hash(self) -> str:
        payload = asdict(self)
        payload["data_root"] = str(self.data_root)
        payload["reports_root"] = str(self.reports_root)
        payload["token_path"] = str(self.token_path) if self.token_path else None
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    @property
    def feature_schema_hash(self) -> str:
        return hashlib.sha256(self.feature_schema_version.encode()).hexdigest()

    @property
    def target_schema_hash(self) -> str:
        return hashlib.sha256(self.target_schema_version.encode()).hexdigest()

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> ResearchConfig:
        source = os.environ if env is None else env
        token_path = source.get("NEOBITCOIN_RESEARCH_TOKEN_FILE", "").strip()
        holding_fee = source.get("NEOBITCOIN_RESEARCH_DAILY_HOLDING_FEE_RATE", "").strip()
        config = cls(
            data_root=Path(source.get("NEOBITCOIN_RESEARCH_DATA", "data/neobitcoin_research")),
            reports_root=Path(
                source.get("NEOBITCOIN_RESEARCH_REPORTS", "reports/neobitcoin_research")
            ),
            token_path=Path(token_path) if token_path else None,
            instrument_query=source.get("NEOBITCOIN_RESEARCH_INSTRUMENT", "Neo Bitcoin"),
            requested_depth=int(source.get("NEOBITCOIN_RESEARCH_DEPTH", "50")),
            experiment_id=source.get("NEOBITCOIN_RESEARCH_EXPERIMENT_ID", "directional-edge-v1"),
            daily_holding_fee_annual_rate=float(holding_fee) if holding_fee else None,
            real_orders_enabled=source.get("REAL_ORDERS_ENABLED", "false").lower()
            not in SAFE_FALSE,
            trading_mode=source.get("TRADING_MODE", "readonly"),
        )
        config.validate()
        return config
