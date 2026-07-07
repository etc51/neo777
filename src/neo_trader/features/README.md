# Mirrored Features

This folder mirrors selected feature modules.

## Contents

- `orderbook.py` - mirrored order book feature functions.
- `volatility.py` - mirrored volatility feature functions.

## Rules

- Canonical feature code lives in `neo_trader/features/`.
- Keep mirrored behavior aligned with canonical feature tests.
- Feature code must stay pure and must not import broker or execution modules.

