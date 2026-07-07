# Strategy

This folder contains signal generation.

## Contents

- `opening_range_book_momentum.py` - OpeningRangeBookMomentumStrategy and signal/reason-code models.

## Rules

- Strategy must never submit orders.
- Strategy must not import `neo_trader.broker` or `neo_trader.execution`.
- Output is only signal intent: BUY, SELL, EXIT, or HOLD with reasons, stops, take profits, and confidence.
- Keep filter rejections explicit with reason codes.

