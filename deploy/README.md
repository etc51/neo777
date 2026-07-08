# Neo Universal Swarm Server Deploy

This deploy scaffold runs the swarm as a 24/7 live-data paper/read-only Linux service.
It does not enable live trading.

## Neo Tail-Catcher Services

`neo_swarm_scalper` runs the NEOBITOK/NEOEFIR tail-catcher in paper/shadow mode.
Use `scripts/deploy_neo_swarm_scalper.ps1` from Windows to install:

- `neo-swarm-bot.service`
- `neo-swarm-dashboard.service`

The server env file is `/etc/neo-trader/neo-swarm-scalper.env`; set
`TBANK_TOKEN` there and keep all safety flags false/read-only. The bot writes
SQLite data to `/opt/neo_trader/data/neo_swarm_scalper.sqlite` and reports to
`/opt/neo_trader/reports/neo_swarm_scalper`. The dashboard listens on port
`8025`.

## Services

- `neo-universal-swarm.service` polls T-Bank read-only order books, runs the
  live-paper swarm loop, and continuously updates
  `data/monitoring/neo_universal_swarm_dashboard_state.json`.
- `neo-universal-swarm-dashboard.service` serves a minimal HTTP dashboard on
  port `8765`.

## Server Environment

Create `/etc/neo-trader/neo-universal-swarm.env` from
`deploy/neo-universal-swarm.env.example` and set the token on the server only.

Required safety values:

```env
LIVE_TRADING_ENABLED=false
NEO_TRADER_LIVE_TRADING_ENABLED=false
TRADING_MODE=readonly
NEO_TRADER_TRADING_MODE=readonly
NEO_TRADER_TBANK_MODE=readonly
```

## Manual Install

```bash
sudo useradd --system --home /opt/neo_trader --shell /usr/sbin/nologin neo-trader || true
sudo mkdir -p /opt/neo_trader /etc/neo-trader
sudo cp deploy/neo-universal-swarm.env.example /etc/neo-trader/neo-universal-swarm.env
sudo chmod 600 /etc/neo-trader/neo-universal-swarm.env

python3 -m venv /opt/neo_trader/.venv
/opt/neo_trader/.venv/bin/python -m pip install -U pip
/opt/neo_trader/.venv/bin/python -m pip install -e "/opt/neo_trader[dashboard]"

sudo cp deploy/neo-universal-swarm.service /etc/systemd/system/
sudo cp deploy/neo-universal-swarm-dashboard.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now neo-universal-swarm.service
sudo systemctl enable --now neo-universal-swarm-dashboard.service
```

Dashboard:

```text
http://SERVER_IP:8765/
```

Health:

```bash
curl http://127.0.0.1:8765/healthz
systemctl status neo-universal-swarm.service
systemctl status neo-universal-swarm-dashboard.service
```

Live trading remains blocked until a separate manual approval changes the
architecture and configuration.
