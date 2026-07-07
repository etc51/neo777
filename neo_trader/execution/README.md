# Execution

This folder contains safe execution gateway abstractions.

## Contents

- `smart_limit.py` - SmartLimitExecutor, marketable limit entry path, cancel/replace, cancel remaining, and emergency exit handling.

## Rules

- This module is not a live trading adapter.
- All order intents must pass through `RiskManager`.
- Public entry points must remain limited to approved safe methods.
- Market orders are forbidden except approved emergency exit flow.
- Live behavior must stay disabled unless `LIVE_TRADING_ENABLED=true` and manual review has approved it.

