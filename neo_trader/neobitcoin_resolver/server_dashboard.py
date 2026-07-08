"""Small HTTP dashboard for the Neobitcoin resolver state file."""

from __future__ import annotations

import argparse
import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from neo_trader.neobitcoin_resolver.config import DEFAULT_DASHBOARD_PATH


def load_dashboard_state(path: Path | str = DEFAULT_DASHBOARD_PATH) -> dict[str, Any]:
    target = Path(path)
    if not target.exists():
        return {"status": "missing", "path": str(target)}
    loaded = json.loads(target.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        return {"status": "invalid", "path": str(target)}
    return loaded


def render_dashboard_html(state: dict[str, Any]) -> str:
    status = _mapping(state.get("status"))
    market = _mapping(state.get("market"))
    pnl = _mapping(state.get("pnl"))
    positions = state.get("open_pair_positions")
    position_rows = positions if isinstance(positions, list) else []
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Neobitcoin Resolver</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #17202a; }}
    main {{ max-width: 1120px; margin: 0 auto; }}
    h1 {{ font-size: 28px; margin: 0 0 18px; }}
    section {{ border-top: 1px solid #d7dde5; padding: 16px 0; }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 12px;
    }}
    .metric {{ border: 1px solid #d7dde5; border-radius: 6px; padding: 12px; }}
    .label {{ color: #5d6d7e; font-size: 12px; text-transform: uppercase; }}
    .value {{ font-size: 18px; margin-top: 6px; overflow-wrap: anywhere; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border-bottom: 1px solid #e5e9ef; padding: 8px; text-align: left; }}
    code {{ background: #f4f6f8; padding: 2px 4px; border-radius: 4px; }}
  </style>
</head>
<body>
<main>
  <h1>Dual-Bot Neobitcoin Resolver</h1>
  <section class="grid">
    {_metric("Instrument", state.get("instrument"))}
    {_metric("Mode", state.get("runtime_mode"))}
    {_metric("Bot Status", status.get("bot_status"))}
    {_metric("API", status.get("api_status"))}
    {_metric("Data", status.get("data_freshness"))}
    {_metric("Open Pair", status.get("current_open_pair") or "none")}
  </section>
  <section class="grid">
    {_metric("Spread Ticks", market.get("spread_ticks"))}
    {_metric("Best Bid", market.get("best_bid"))}
    {_metric("Best Ask", market.get("best_ask"))}
    {_metric("Closed PnL", pnl.get("estimated_net_pnl"))}
  </section>
  <section>
    <h2>Open Positions</h2>
    <table>
      <thead><tr><th>Pair</th><th>Bot</th><th>Side</th><th>State</th><th>MFE</th><th>MAE</th></tr></thead>
      <tbody>{_position_rows(position_rows)}</tbody>
    </table>
  </section>
  <section>
    <a href="/state.json">state.json</a> <code>/health</code>
  </section>
</main>
</body>
</html>"""


def serve_dashboard(
    *,
    state_path: Path | str = DEFAULT_DASHBOARD_PATH,
    host: str = "127.0.0.1",
    port: int = 8036,
) -> None:
    handler = _handler(Path(state_path))
    server = ThreadingHTTPServer((host, port), handler)
    try:
        server.serve_forever()
    finally:
        server.server_close()


def _handler(state_path: Path) -> type[BaseHTTPRequestHandler]:
    class DashboardHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/health":
                self._send(HTTPStatus.OK, "text/plain; charset=utf-8", b"ok\n")
                return
            state = load_dashboard_state(state_path)
            if self.path == "/state.json":
                payload = json.dumps(state, ensure_ascii=False, indent=2).encode("utf-8")
                self._send(HTTPStatus.OK, "application/json; charset=utf-8", payload)
                return
            if self.path not in {"/", "/index.html"}:
                self._send(HTTPStatus.NOT_FOUND, "text/plain; charset=utf-8", b"not found\n")
                return
            self._send(
                HTTPStatus.OK,
                "text/html; charset=utf-8",
                render_dashboard_html(state).encode("utf-8"),
            )

        def log_message(self, format: str, *args: object) -> None:
            return

        def _send(self, status: HTTPStatus, content_type: str, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return DashboardHandler


def _mapping(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _metric(label: str, value: object) -> str:
    safe_value = "" if value is None else str(value)
    return (
        f'<div class="metric"><div class="label">{label}</div>'
        f'<div class="value">{safe_value}</div></div>'
    )


def _position_rows(rows: list[object]) -> str:
    html_rows: list[str] = []
    for item in rows:
        row = _mapping(item)
        html_rows.append(
            "<tr>"
            f"<td>{row.get('pair_id', '')}</td>"
            f"<td>{row.get('bot_id', '')}</td>"
            f"<td>{row.get('side', '')}</td>"
            f"<td>{row.get('state', '')}</td>"
            f"<td>{row.get('mfe', '')}</td>"
            f"<td>{row.get('mae', '')}</td>"
            "</tr>"
        )
    return "".join(html_rows) or '<tr><td colspan="6">No open positions</td></tr>'


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Serve Neobitcoin resolver dashboard")
    parser.add_argument("--state", type=Path, default=DEFAULT_DASHBOARD_PATH)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8036)
    args = parser.parse_args(argv)
    serve_dashboard(state_path=args.state, host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["load_dashboard_state", "render_dashboard_html", "serve_dashboard"]
