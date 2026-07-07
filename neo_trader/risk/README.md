# Risk

This folder contains risk controls.

## Contents

- `manager.py` - RiskManager, RiskConfig, RiskState, risk decisions, reason codes, position checks, kill switch, stale data, spread/slippage gates, daily loss, trade limits, and flatten timing.

## Rules

- Risk checks are mandatory before any execution intent.
- Default behavior blocks new BUY/SELL entries while a position is open.
- EXIT flow must remain possible when risk reduction is required.
- Keep forced flatten, kill switch, and stale market data checks covered by tests.

