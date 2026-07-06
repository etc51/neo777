# neo_trader

Safe Python project scaffold for experimenting with trading-system architecture.

This repository intentionally contains no live-trading integration and no order
submission code. Current scope is package structure, configuration, logging, and
import tests only.

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
python -m pytest
python -m ruff check .
python -m mypy neo_trader
```

## T-Bank read client

`TBankClient` loads `NEO_TRADER_TBANK_TOKEN` from `.env` and supports three
connection modes:

- `readonly`: production endpoint, read methods only.
- `sandbox`: sandbox endpoint, read methods only.
- `live`: production endpoint, read methods only.

Implemented methods:

- `get_accounts()`
- `get_instruments(["SBER", "T"])`
- `get_trading_status("SBER_TQBR")`
- `get_trading_statuses(["SBER_TQBR", "T_TQBR"])`
- `get_orderbook_snapshot("SBER_TQBR", depth=10)`

No order placement or cancellation methods are implemented.

## Market data recorder

`MarketDataRecorder` records raw market-data events only:

- order book snapshots
- anonymous trades
- candles

Events are written to parquet partitions:

```text
data/raw/date=YYYY-MM-DD/instrument=UID/type=orderbook.parquet
data/raw/date=YYYY-MM-DD/instrument=UID/type=trades.parquet
data/raw/date=YYYY-MM-DD/instrument=UID/type=candles.parquet
```

The recorder includes heartbeat tracking, reconnect with exponential backoff,
and quality counters for events/sec, stale seconds, and gaps. Real trading and
order submission remain intentionally absent.
