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
  `cancel_remaining`, and `emergency_exit`.
- All executor order paths require a `RiskManager` decision.
- Entry quantity cannot exceed the risk-approved position size.
- Emergency exit side is derived from the current position: `LONG -> SELL`,
  `SHORT -> BUY`, `FLAT -> no-op`.
- Emergency exit quantity is capped by the current position size and cannot
  increase exposure.
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

The YAML files are the runtime source of truth. Load them through
`neo_trader.config_loader`:

- `load_runtime_config()`
- `load_strategy_config()`
- `load_risk_config()`
- `load_instrument_universe_config()`
- `load_project_config()`

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

`cancel_remaining(order_id)` is a cancel-only risk reduction path. It does not
replace the order and does not increase exposure.

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

## Data Recording Sprint 1

The recorder is readonly. It records market data snapshots only: no orders, no
live trading, no paper execution, and no use of execution modules. Keep `.env`
out of git; raw market data is excluded from git.

Mock pipeline check without T-Bank API:

```powershell
python scripts\run_data_recorder.py --mode mock --duration-seconds 60 --output data\raw --dashboard-state data\monitoring\dashboard_state.json
```

Short readonly T-Bank stream smoke test:

```powershell
python scripts\run_data_recorder.py --mode tbank-readonly --duration-seconds 60 --max-events 10 --output data\raw --dashboard-state data\monitoring\dashboard_state.json
```

Read the generated live-state dashboard snapshot:

```powershell
python -m streamlit run neo_trader\monitoring\streamlit_dashboard.py -- --state data\monitoring\dashboard_state.json
```

Makefile shortcuts:

```bash
make record-mock
make record-readonly
make run-dashboard-live-state
make analyze-recording
```

`record-readonly` is intentionally guarded by readonly safety flags and requires
local `T_INVEST_TOKEN` or `NEO_TRADER_TBANK_TOKEN`, configured instrument UIDs,
and the T-Invest Python SDK import path `t_tech.invest` or `tinkoff.invest`.
It uses only the T-Bank market-data stream path and must remain read-only.

Analyze recorded market data and generate an active universe:

```powershell
python scripts\analyze_recording_quality.py --raw data\raw --reports-dir data\reports --active-universe configs\active_universe.yaml
```

The analyzer writes liquidity reports to `data/reports/` and generates
`configs/active_universe.yaml` from read-only parquet data.
