# Backtest

This folder contains event-driven backtesting logic.

## Contents

- `event_driven.py` - reads parquet market data, computes features, calls strategy, simulates fills, and exports reports.

## Rules

- Backtests simulate execution only; they must not call broker or live execution APIs.
- Inputs should come from local parquet or synthetic fixtures.
- Keep slippage, fills, PnL, MAE, and MFE calculations deterministic and testable.
- Add focused tests in `tests/test_event_driven_backtester.py` when behavior changes.

