# Runtime Config Files

This folder contains YAML configuration used by typed config loaders.

## Contents

- `runtime.yaml` - runtime mode, dashboard settings, and process-level safety defaults.
- `strategy.yaml` - strategy thresholds and filters.
- `risk.yaml` - risk limits and kill-switch related settings.
- `instruments.yaml` - instrument universe used by recorder, strategy, and dashboard paths.

## Rules

- YAML is the source of truth for runtime, strategy, risk, and instruments.
- Keep defaults safe: `TRADING_MODE=readonly` and live trading disabled.
- Do not store tokens, account ids, or other secrets here.
- If a config schema changes, update `neo_trader/config_loader.py` and its tests.

