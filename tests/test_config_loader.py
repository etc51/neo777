"""Tests for typed YAML configuration loading."""

from datetime import time
from decimal import Decimal
from pathlib import Path

from neo_trader.config_loader import (
    InstrumentUniverseConfig,
    RuntimeConfig,
    StrategyConfig,
    load_instrument_universe_config,
    load_project_config,
    load_risk_config,
    load_runtime_config,
    load_strategy_config,
)
from neo_trader.risk.manager import RiskConfig


def test_strategy_yaml_loads_into_strategy_config() -> None:
    config = load_strategy_config()

    assert isinstance(config, StrategyConfig)
    assert config.session_start == time(10, 0)
    assert config.breakout_buffer_bps == Decimal("2")
    assert config.take_profit_r_multiples == (Decimal("1"), Decimal("2"))
    assert config.to_opening_range_config().max_expected_slippage_bps == Decimal("15")


def test_risk_yaml_loads_into_risk_config() -> None:
    config = load_risk_config()

    assert isinstance(config, RiskConfig)
    assert config.max_daily_loss == Decimal("1000")
    assert config.force_flatten_at == time(18, 40)
    assert config.kill_switch is False


def test_runtime_yaml_loads_into_runtime_config() -> None:
    config = load_runtime_config()

    assert isinstance(config, RuntimeConfig)
    assert config.trading_mode == "readonly"
    assert config.live_trading_enabled is False
    assert config.dashboard.state_path == Path("data/monitoring/dashboard_state.example.json")


def test_instruments_yaml_loads_into_instrument_universe_config() -> None:
    config = load_instrument_universe_config()

    assert isinstance(config, InstrumentUniverseConfig)
    assert config.instruments[0].ticker == "SBER"
    assert config.instruments[0].enabled is True
    assert config.instruments[1].ticker == "T"


def test_project_config_loads_all_yaml_configs() -> None:
    config = load_project_config()

    assert config.runtime.trading_mode == "readonly"
    assert config.strategy.min_confidence == Decimal("0.55")
    assert config.risk.max_trades_per_day == 10
    assert len(config.instruments.instruments) == 2
