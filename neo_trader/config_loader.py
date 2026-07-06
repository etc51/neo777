"""Typed YAML configuration loader for runtime, strategy, risk, and instruments."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import time
from decimal import Decimal
from pathlib import Path
from typing import Any, Final, Literal, cast

import yaml

from neo_trader.risk.manager import RiskConfig
from neo_trader.strategy.opening_range_book_momentum import OpeningRangeBookMomentumConfig

ROOT: Final = Path(__file__).resolve().parents[1]
CONFIG_DIR: Final = ROOT / "configs"


@dataclass(frozen=True)
class DashboardConfig:
    """Dashboard runtime settings loaded from YAML."""

    state_path: Path
    host: str
    port: int


@dataclass(frozen=True)
class RuntimeConfig:
    """Global runtime settings loaded from YAML."""

    environment: Literal["development", "test", "production"]
    log_level: str
    dry_run: bool
    trading_mode: Literal["readonly", "sandbox", "live"]
    live_trading_enabled: bool
    broker_name: str
    tbank_mode: Literal["readonly", "sandbox", "live"]
    dashboard: DashboardConfig


@dataclass(frozen=True)
class StrategyConfig:
    """Opening range strategy settings loaded from YAML."""

    session_start: time
    session_end: time
    opening_range_minutes: int
    entry_window_minutes: int
    force_exit_minutes_before_close: int
    breakout_buffer_bps: Decimal
    max_spread_bps: Decimal
    min_volatility_percentile: Decimal | None
    max_volatility_percentile: Decimal | None
    low_volatility_regimes: tuple[str, ...]
    high_volatility_regimes: tuple[str, ...]
    max_expected_slippage_bps: Decimal | None
    min_imbalance: Decimal
    min_weighted_imbalance: Decimal
    min_microprice_edge_bps: Decimal
    max_opposing_wall_score: Decimal
    require_ofi_confirmation: bool
    min_ofi_confirmation: Decimal
    min_confidence: Decimal
    atr_window: int
    stop_atr_multiple: Decimal
    take_profit_r_multiples: tuple[Decimal, ...]

    def __post_init__(self) -> None:
        self.to_opening_range_config()

    def to_opening_range_config(self) -> OpeningRangeBookMomentumConfig:
        """Convert to the strategy component config."""

        return OpeningRangeBookMomentumConfig(
            session_start=self.session_start,
            session_end=self.session_end,
            opening_range_minutes=self.opening_range_minutes,
            entry_window_minutes=self.entry_window_minutes,
            force_exit_minutes_before_close=self.force_exit_minutes_before_close,
            breakout_buffer_bps=self.breakout_buffer_bps,
            max_spread_bps=self.max_spread_bps,
            min_volatility_percentile=self.min_volatility_percentile,
            max_volatility_percentile=self.max_volatility_percentile,
            low_volatility_regimes=self.low_volatility_regimes,
            high_volatility_regimes=self.high_volatility_regimes,
            max_expected_slippage_bps=self.max_expected_slippage_bps,
            min_imbalance=self.min_imbalance,
            min_weighted_imbalance=self.min_weighted_imbalance,
            min_microprice_edge_bps=self.min_microprice_edge_bps,
            max_opposing_wall_score=self.max_opposing_wall_score,
            require_ofi_confirmation=self.require_ofi_confirmation,
            min_ofi_confirmation=self.min_ofi_confirmation,
            min_confidence=self.min_confidence,
            atr_window=self.atr_window,
            stop_atr_multiple=self.stop_atr_multiple,
            take_profit_r_multiples=self.take_profit_r_multiples,
        )


@dataclass(frozen=True)
class InstrumentConfig:
    """One configured instrument."""

    ticker: str
    class_code: str
    uid: str
    enabled: bool


@dataclass(frozen=True)
class InstrumentUniverseConfig:
    """Configured instrument universe."""

    instruments: tuple[InstrumentConfig, ...]


@dataclass(frozen=True)
class ProjectConfig:
    """All YAML-backed configs loaded together."""

    runtime: RuntimeConfig
    strategy: StrategyConfig
    risk: RiskConfig
    instruments: InstrumentUniverseConfig


def load_runtime_config(path: Path | str | None = None) -> RuntimeConfig:
    """Load ``configs/runtime.yaml`` into a typed dataclass."""

    raw = _load_yaml_mapping(_config_path(path, "runtime.yaml"))
    dashboard_raw = _mapping(_required(raw, "dashboard"), "dashboard")
    return RuntimeConfig(
        environment=_literal(
            _string(_required(raw, "environment"), "environment"),
            "environment",
            ("development", "test", "production"),
        ),
        log_level=_string(_required(raw, "log_level"), "log_level"),
        dry_run=_bool(_required(raw, "dry_run"), "dry_run"),
        trading_mode=_literal(
            _string(_required(raw, "trading_mode"), "trading_mode"),
            "trading_mode",
            ("readonly", "sandbox", "live"),
        ),
        live_trading_enabled=_bool(
            _required(raw, "live_trading_enabled"),
            "live_trading_enabled",
        ),
        broker_name=_string(_required(raw, "broker_name"), "broker_name"),
        tbank_mode=_literal(
            _string(_required(raw, "tbank_mode"), "tbank_mode"),
            "tbank_mode",
            ("readonly", "sandbox", "live"),
        ),
        dashboard=DashboardConfig(
            state_path=Path(_string(_required(dashboard_raw, "state_path"), "state_path")),
            host=_string(_required(dashboard_raw, "host"), "host"),
            port=_int(_required(dashboard_raw, "port"), "port"),
        ),
    )


def load_strategy_config(path: Path | str | None = None) -> StrategyConfig:
    """Load ``configs/strategy.yaml`` into a typed dataclass."""

    raw = _load_yaml_mapping(_config_path(path, "strategy.yaml"))
    return StrategyConfig(
        session_start=_time(_required(raw, "session_start"), "session_start"),
        session_end=_time(_required(raw, "session_end"), "session_end"),
        opening_range_minutes=_int(
            _required(raw, "opening_range_minutes"),
            "opening_range_minutes",
        ),
        entry_window_minutes=_int(_required(raw, "entry_window_minutes"), "entry_window_minutes"),
        force_exit_minutes_before_close=_int(
            _required(raw, "force_exit_minutes_before_close"),
            "force_exit_minutes_before_close",
        ),
        breakout_buffer_bps=_decimal(_required(raw, "breakout_buffer_bps"), "breakout_buffer_bps"),
        max_spread_bps=_decimal(_required(raw, "max_spread_bps"), "max_spread_bps"),
        min_volatility_percentile=_optional_decimal(
            raw,
            "min_volatility_percentile",
        ),
        max_volatility_percentile=_optional_decimal(
            raw,
            "max_volatility_percentile",
        ),
        low_volatility_regimes=_string_tuple(
            _required(raw, "low_volatility_regimes"),
            "low_volatility_regimes",
        ),
        high_volatility_regimes=_string_tuple(
            _required(raw, "high_volatility_regimes"),
            "high_volatility_regimes",
        ),
        max_expected_slippage_bps=_optional_decimal(raw, "max_expected_slippage_bps"),
        min_imbalance=_decimal(_required(raw, "min_imbalance"), "min_imbalance"),
        min_weighted_imbalance=_decimal(
            _required(raw, "min_weighted_imbalance"),
            "min_weighted_imbalance",
        ),
        min_microprice_edge_bps=_decimal(
            _required(raw, "min_microprice_edge_bps"),
            "min_microprice_edge_bps",
        ),
        max_opposing_wall_score=_decimal(
            _required(raw, "max_opposing_wall_score"),
            "max_opposing_wall_score",
        ),
        require_ofi_confirmation=_bool(
            _required(raw, "require_ofi_confirmation"),
            "require_ofi_confirmation",
        ),
        min_ofi_confirmation=_decimal(
            _required(raw, "min_ofi_confirmation"),
            "min_ofi_confirmation",
        ),
        min_confidence=_decimal(_required(raw, "min_confidence"), "min_confidence"),
        atr_window=_int(_required(raw, "atr_window"), "atr_window"),
        stop_atr_multiple=_decimal(_required(raw, "stop_atr_multiple"), "stop_atr_multiple"),
        take_profit_r_multiples=_decimal_tuple(
            _required(raw, "take_profit_r_multiples"),
            "take_profit_r_multiples",
        ),
    )


def load_risk_config(path: Path | str | None = None) -> RiskConfig:
    """Load ``configs/risk.yaml`` into ``RiskConfig``."""

    raw = _load_yaml_mapping(_config_path(path, "risk.yaml"))
    return RiskConfig(
        max_daily_loss=_decimal(_required(raw, "max_daily_loss"), "max_daily_loss"),
        max_trades_per_day=_int(_required(raw, "max_trades_per_day"), "max_trades_per_day"),
        no_new_entries_after=_time(
            _required(raw, "no_new_entries_after"),
            "no_new_entries_after",
        ),
        force_flatten_at=_time(_required(raw, "force_flatten_at"), "force_flatten_at"),
        max_market_data_stale_seconds=_decimal(
            _required(raw, "max_market_data_stale_seconds"),
            "max_market_data_stale_seconds",
        ),
        max_spread_bps=_decimal(_required(raw, "max_spread_bps"), "max_spread_bps"),
        max_slippage_bps=_decimal(_required(raw, "max_slippage_bps"), "max_slippage_bps"),
        risk_per_trade_fraction=_decimal(
            _required(raw, "risk_per_trade_fraction"),
            "risk_per_trade_fraction",
        ),
        max_position_notional_fraction=_decimal(
            _required(raw, "max_position_notional_fraction"),
            "max_position_notional_fraction",
        ),
        quantity_step=_decimal(_required(raw, "quantity_step"), "quantity_step"),
        min_quantity=_decimal(_required(raw, "min_quantity"), "min_quantity"),
        kill_switch=_bool(_required(raw, "kill_switch"), "kill_switch"),
        flatten_on_daily_loss=_bool(
            _required(raw, "flatten_on_daily_loss"),
            "flatten_on_daily_loss",
        ),
    )


def load_instrument_universe_config(path: Path | str | None = None) -> InstrumentUniverseConfig:
    """Load ``configs/instruments.yaml`` into a typed instrument universe."""

    raw = _load_yaml_mapping(_config_path(path, "instruments.yaml"))
    instruments_value = _required(raw, "instruments")
    if not isinstance(instruments_value, Sequence) or isinstance(instruments_value, str):
        raise ValueError("instruments must be a YAML sequence.")
    instruments = tuple(
        _instrument_config(_mapping(item, f"instruments[{index}]"))
        for index, item in enumerate(instruments_value)
    )
    return InstrumentUniverseConfig(instruments=instruments)


def load_project_config(config_dir: Path | str | None = None) -> ProjectConfig:
    """Load all project YAML configs."""

    base_dir = CONFIG_DIR if config_dir is None else Path(config_dir)
    return ProjectConfig(
        runtime=load_runtime_config(base_dir / "runtime.yaml"),
        strategy=load_strategy_config(base_dir / "strategy.yaml"),
        risk=load_risk_config(base_dir / "risk.yaml"),
        instruments=load_instrument_universe_config(base_dir / "instruments.yaml"),
    )


def _instrument_config(raw: Mapping[str, object]) -> InstrumentConfig:
    return InstrumentConfig(
        ticker=_non_empty_string(_required(raw, "ticker"), "ticker"),
        class_code=_non_empty_string(_required(raw, "class_code"), "class_code"),
        uid=_string(_required(raw, "uid"), "uid"),
        enabled=_bool(_required(raw, "enabled"), "enabled"),
    )


def _config_path(path: Path | str | None, default_name: str) -> Path:
    return CONFIG_DIR / default_name if path is None else Path(path)


def _load_yaml_mapping(path: Path) -> dict[str, object]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise ValueError(f"YAML root must be a mapping: {path}.")
    return {str(key): value for key, value in raw.items()}


def _required(raw: Mapping[str, object], key: str) -> object:
    if key not in raw:
        raise ValueError(f"missing required config key: {key}.")
    return raw[key]


def _mapping(value: object, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be a mapping.")
    return cast(Mapping[str, object], value)


def _literal(
    value: str,
    field_name: str,
    allowed: tuple[str, ...],
) -> Any:
    if value not in allowed:
        joined = ", ".join(allowed)
        raise ValueError(f"{field_name} must be one of: {joined}.")
    return value


def _string(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string.")
    return value


def _non_empty_string(value: object, field_name: str) -> str:
    raw = _string(value, field_name)
    if not raw:
        raise ValueError(f"{field_name} must not be empty.")
    return raw


def _bool(value: object, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    raise TypeError(f"{field_name} must be a boolean.")


def _int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer.")
    return value


def _decimal(value: object, field_name: str) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        raise TypeError(f"{field_name} must be a decimal-compatible value.")
    if isinstance(value, int | str):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))
    raise TypeError(f"{field_name} must be a decimal-compatible value.")


def _optional_decimal(raw: Mapping[str, object], key: str) -> Decimal | None:
    value = raw.get(key)
    if value is None:
        return None
    return _decimal(value, key)


def _time(value: object, field_name: str) -> time:
    if isinstance(value, time):
        return value
    if isinstance(value, str):
        return time.fromisoformat(value)
    raise TypeError(f"{field_name} must be an ISO time string.")


def _string_tuple(value: object, field_name: str) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, str):
        raise TypeError(f"{field_name} must be a sequence of strings.")
    return tuple(_non_empty_string(item, field_name) for item in value)


def _decimal_tuple(value: object, field_name: str) -> tuple[Decimal, ...]:
    if not isinstance(value, Sequence) or isinstance(value, str):
        raise TypeError(f"{field_name} must be a sequence of decimals.")
    return tuple(_decimal(item, field_name) for item in value)


__all__ = [
    "DashboardConfig",
    "InstrumentConfig",
    "InstrumentUniverseConfig",
    "ProjectConfig",
    "RuntimeConfig",
    "StrategyConfig",
    "load_instrument_universe_config",
    "load_project_config",
    "load_risk_config",
    "load_runtime_config",
    "load_strategy_config",
]
