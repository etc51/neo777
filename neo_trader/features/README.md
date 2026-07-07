# Features

This folder contains pure feature functions.

## Contents

- `orderbook.py` - best bid/ask, mid, spread, depth, imbalance, microprice, VWAP-to-fill, slippage, and wall score.
- `volatility.py` - log returns, realized volatility, ATR, volatility percentiles, and regime classification.

## Rules

- Feature functions must be deterministic and side-effect free.
- Do not import broker, execution, risk manager, or strategy modules here.
- Prefer `Decimal` where market price precision matters.
- Add synthetic fixture tests when adding or changing calculations.

