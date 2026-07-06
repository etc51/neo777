# neo_trader

`neo_trader` is a safe research engine for intraday trading-system architecture.
It contains read-only broker access, market-data recording, feature
calculation, strategy decisions, risk gates, simulated execution, backtesting,
and a Streamlit monitoring dashboard.

Live trading is disabled by default. The live broker adapter is not
implemented. The execution module is a gateway abstraction only, and real
trading is prohibited until a manual architecture and safety review explicitly
approves it.

## Safety Model

- Default runtime mode is `readonly`.
- `LIVE_TRADING_ENABLED` and `NEO_TRADER_LIVE_TRADING_ENABLED` default to
  `false`.
- `TBankClient` exposes read-only market/account methods and does not place
  orders.
- `OpeningRangeBookMomentumStrategy` returns `BUY`, `SELL`, `EXIT`, or `HOLD`
  signals only.
- `SmartLimitExecutor` exposes only `enter_marketable_limit`, `cancel_replace`,
  and `emergency_exit`.
- All executor order paths require a `RiskManager` decision.
- Entry quantity cannot exceed the risk-approved position size.
- Emergency exit quantity is capped by the current position size.
- Market orders are rejected except for `emergency_exit`.

## Install

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev,dashboard]"
```

## Quality Gates

```powershell
python -m ruff check .
python -m mypy neo_trader
python -m pytest -q
python scripts\audit.py
```

The same checks are available through the Makefile:

```bash
make lint
make typecheck
make test
make audit
```

## Configuration

Runtime secrets stay in `.env`; `.env.example` documents safe defaults. Do not
commit `.env`.

Reference parameter files live under `configs/`:

- `configs/runtime.yaml`: global runtime mode, logging, broker mode, dashboard
  path/host/port.
- `configs/risk.yaml`: daily loss, position sizing, kill switch, stale-data,
  spread/slippage, forced flatten, and trade-count limits.
- `configs/strategy.yaml`: opening range, order-book confirmation,
  volatility/VWAP/EMA/slippage/OFI filters, stops, take profits, and
  confidence thresholds.
- `configs/instruments.yaml`: enabled instrument universe and placeholder
  market identifiers.

The YAML files are operational references. The typed Python defaults in
`neo_trader.config`, `neo_trader.risk.manager`, and
`neo_trader.strategy.opening_range_book_momentum` remain the source of truth
until a dedicated config loader is introduced.

## Components

### Broker

`neo_trader.broker.tbank.TBankClient` loads the token from environment
variables and supports `readonly`, `sandbox`, and `live` modes for read
methods:

- accounts;
- instruments by ticker;
- trading status;
- order-book snapshot.

The token is never logged.

### Data Recorder

`MarketDataRecorder` subscribes to order book, trades, and candles through an
injected stream source and writes raw events to parquet:

```text
data/raw/date=YYYY-MM-DD/instrument=UID/type=orderbook.parquet
data/raw/date=YYYY-MM-DD/instrument=UID/type=trades.parquet
data/raw/date=YYYY-MM-DD/instrument=UID/type=candles.parquet
```

It tracks heartbeat, reconnect backoff, events/sec, stale seconds, and gaps.

### Features

Order-book features include best bid/ask, mid, spread, depth, imbalance,
weighted imbalance, microprice, expected VWAP/slippage, and wall score.

Volatility features include log returns, realized volatility, ATR, percentile,
and regime classification.

### Strategy

`OpeningRangeBookMomentumStrategy` is a pure decision component. Entry filters
include realized-volatility percentile/regime, VWAP, EMA fast/slow trend,
expected slippage, and optional OFI confirmation.

### Risk

`RiskManager` enforces daily loss, trade count, no-new-entry time, forced
flatten time, stale market data, spread/slippage caps, open-position entry
blocks, and kill switch behavior.

### Execution

`SmartLimitExecutor` is not a broker adapter. It prepares guarded gateway
requests only after RiskManager approval and trading-status checks.

### Backtest

The event-driven backtester reads parquet order book/trades/candles, computes
features, calls the strategy, simulates fills from the book, and exports
HTML/CSV reports with slippage, fills, PnL, MAE, and MFE.

### Dashboard

Run the read-only Streamlit dashboard:

```bash
make run-dashboard
```

or directly:

```bash
python -m streamlit run neo_trader/monitoring/streamlit_dashboard.py -- --state data/monitoring/dashboard_state.example.json
```

The dashboard shows instruments, spread, imbalance, volatility regime, active
signals, positions, orders, realized/unrealized PnL, kill switch status, forced
flatten countdown, and runtime commit hash.
