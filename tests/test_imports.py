"""Import smoke tests for the project scaffold."""

import importlib

MODULES = [
    "neo_trader",
    "neo_trader.backtest",
    "neo_trader.backtest.event_driven",
    "neo_trader.broker",
    "neo_trader.broker.tbank",
    "neo_trader.config",
    "neo_trader.config_loader",
    "neo_trader.data",
    "neo_trader.data.market_data_recorder",
    "neo_trader.data.recording_quality_report",
    "neo_trader.execution",
    "neo_trader.execution.smart_limit",
    "neo_trader.features",
    "neo_trader.features.orderbook",
    "neo_trader.features.volatility",
    "neo_trader.logging_config",
    "neo_trader.monitoring",
    "neo_trader.monitoring.dashboard_state_writer",
    "neo_trader.monitoring.streamlit_dashboard",
    "neo_trader.neo_universal_swarm",
    "neo_trader.neo_universal_swarm.bots",
    "neo_trader.neo_universal_swarm.config",
    "neo_trader.neo_universal_swarm.daemon",
    "neo_trader.neo_universal_swarm.dashboard",
    "neo_trader.neo_universal_swarm.data",
    "neo_trader.neo_universal_swarm.model",
    "neo_trader.neo_universal_swarm.paper",
    "neo_trader.neo_universal_swarm.server_dashboard",
    "neo_trader.neo_universal_swarm.simulator",
    "neo_trader.neo_universal_swarm.types",
    "neo_trader.risk",
    "neo_trader.risk.manager",
    "neo_trader.research",
    "neo_trader.research.backtest_runner",
    "neo_trader.research.feature_store",
    "neo_trader.research.universe_selector",
    "neo_trader.runtime",
    "neo_trader.strategy",
    "neo_trader.strategy.opening_range_book_momentum",
]


def test_project_modules_import() -> None:
    for module_name in MODULES:
        assert importlib.import_module(module_name)
