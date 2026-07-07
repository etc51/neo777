# Data

This folder contains market data ingestion and recording support.

## Contents

- `market_data_recorder.py` - records raw orderbook, trades, and candles to partitioned parquet.
- `recording_quality_report.py` - builds JSON quality reports for recording runs.

## Rules

- Data recording is read-only and must never submit, cancel, or replace orders.
- Raw events should be written under `data/raw/date=YYYY-MM-DD/instrument=UID/type=*.parquet`.
- Keep heartbeat, reconnect, and quality counters observable.
- Do not import execution modules into recorder code.

