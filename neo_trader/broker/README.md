# Broker

This folder contains broker-facing adapters and conversions.

## Contents

- `tbank.py` - T-Bank client helpers, mode handling, read-only market/account operations, and quotation conversion.

## Rules

- Do not log tokens, account ids, or raw credentials.
- Broker code must respect `readonly`, `sandbox`, and `live` modes.
- This project currently has no approved live trading adapter.
- Order placement must not be added here without an explicit manual review task.
- Keep quotation and decimal conversion covered by tests.

