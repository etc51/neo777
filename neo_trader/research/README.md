# Research

This folder contains offline research helpers.

## Contents

- `universe_selector.py` - ranks instruments from recording quality and liquidity metrics.
- `feature_store.py` - builds offline feature parquet from readonly raw market data.
- `backtest_runner.py` - runs session-aware offline research backtests and writes reports.

## Rules

- Research code must stay offline/read-only.
- Do not import broker, execution, or order placement modules.
- Do not store tokens, secrets, or raw market data here.
- Generated outputs belong under `data/features/`, `data/reports/`, or safe YAML configs.
- `simple_book_momentum_research` is diagnostic-only and must stay marked `RESEARCH_ONLY_NOT_FOR_LIVE`.
