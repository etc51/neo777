# neo_trader Package

This is the main Python package for NeoIntraday Engine.

## Contents

- `backtest/` - event-driven simulation and report generation.
- `broker/` - broker adapters and read-only broker client helpers.
- `data/` - market data recording and recording quality reports.
- `execution/` - gateway abstraction for simulated/safe order intent handling.
- `features/` - pure feature calculations from candles and order books.
- `monitoring/` - Streamlit dashboard and dashboard state writer.
- `risk/` - risk gates, position sizing, kill switch, and flatten timing.
- `strategy/` - signal generation only.
- `config.py` - pydantic settings and environment defaults.
- `config_loader.py` - typed YAML config loader.
- `logging_config.py` - logging setup.
- `runtime.py` - runtime metadata helpers, including commit hash.

## Rules

- Real trading is prohibited until manual review.
- Live trading must remain disabled by default.
- Strategy code must not import broker or execution modules.
- Execution code must always pass through risk checks.
- Tokens, account ids, and secrets must never be logged or committed.

