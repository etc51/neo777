"""Minimal HTTP dashboard for the Neo Universal Bot Swarm."""

from __future__ import annotations

import argparse
import html
import json
from collections.abc import Mapping
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Final, TypeAlias, cast

JsonMapping: TypeAlias = Mapping[str, Any]
DEFAULT_STATE_PATH: Final = Path("data/monitoring/neo_universal_swarm_dashboard_state.json")


def load_swarm_state(path: Path | str = DEFAULT_STATE_PATH) -> JsonMapping:
    """Load dashboard state as a mapping."""

    resolved = Path(path)
    if not resolved.exists():
        return {"status": "missing_state", "state_path": str(resolved)}
    raw = json.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise ValueError("swarm dashboard state must be a JSON object.")
    return cast(JsonMapping, raw)


def render_swarm_dashboard_html(state: JsonMapping) -> str:
    """Render a compact auto-refreshing HTML dashboard."""

    swarm = _mapping(state.get("swarm"))
    curator = _mapping(swarm.get("curator"))
    metrics = _mapping(swarm.get("metrics"))
    readiness = _mapping(swarm.get("readiness"))
    latest_market = _sequence(swarm.get("latest_market"))
    bots = _sequence(swarm.get("bots"))
    market_table = render_sequence_table(
        latest_market,
        (
            "instrument",
            "ticker",
            "uid",
            "position_uid",
            "spread_ticks",
            "microprice",
            "imbalance_3",
            "model_ev_ticks",
            "trade_allowed",
            "rejection_reason",
        ),
    )
    bots_table = render_sequence_table(
        bots,
        (
            "bot_id",
            "account_ref",
            "state",
            "assigned_pair_id",
            "realized_pnl_ticks",
            "live_enabled",
        ),
    )
    updated_at = _text(state.get("updated_at", "unknown"))
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="10">
  <title>Neo Swarm Monitor</title>
  <style>
    body {{
      font-family: system-ui, sans-serif;
      margin: 24px;
      background: #f7f8fa;
      color: #14171a;
    }}
    h1 {{ font-size: 22px; margin: 0 0 4px; }}
    h2 {{ font-size: 16px; margin: 24px 0 8px; }}
    .muted {{ color: #5c6670; font-size: 13px; }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 12px;
    }}
    .metric {{ background: white; border: 1px solid #d8dee6; border-radius: 6px; padding: 12px; }}
    .metric b {{ display: block; font-size: 13px; color: #5c6670; font-weight: 600; }}
    .metric span {{ display: block; font-size: 20px; margin-top: 6px; }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: white;
      border: 1px solid #d8dee6;
    }}
    th, td {{ text-align: left; padding: 8px; border-bottom: 1px solid #edf0f2; font-size: 13px; }}
    th {{ background: #eef2f6; color: #343a40; }}
    .ok {{ color: #12733b; font-weight: 700; }}
    .bad {{ color: #9b1c1c; font-weight: 700; }}
  </style>
</head>
<body>
  <h1>Neo Universal Bot Swarm</h1>
  <div class="muted">
    Updated: {html.escape(updated_at)} | Auto-refresh: 10s | <a href="/state.json">state.json</a>
  </div>
  <h2>Curator</h2>
  <div class="grid">
    {_metric("Status", curator.get("status", "unknown"))}
    {_metric("Active pairs", curator.get("active_pairs", 0))}
    {_metric("Closed pairs", curator.get("closed_pairs", 0))}
    {_metric("Total PnL ticks", curator.get("total_pnl_ticks", "0"))}
    {_metric("Live enabled", str(curator.get("trading_enabled", False)))}
  </div>
  <h2>Metrics</h2>
  <div class="grid">
    {_metric("Total pairs", metrics.get("total_pairs", 0))}
    {_metric("EV ticks", metrics.get("pair_ev_ticks", "0"))}
    {_metric("Winrate", metrics.get("pair_winrate", "0"))}
    {_metric("Profit factor", metrics.get("profit_factor", "0"))}
    {_metric("Max drawdown", metrics.get("max_drawdown", "0"))}
    {_metric("Fakeout rate", metrics.get("fakeout_rate", "0"))}
  </div>
  <h2>Readiness</h2>
  {render_key_value_table(readiness)}
  <h2>Market</h2>
  {market_table}
  <h2>Bots</h2>
  {bots_table}
</body>
</html>"""


def render_key_value_table(values: JsonMapping) -> str:
    """Render mapping values as a two-column table."""

    rows = "\n".join(
        f"<tr><td>{html.escape(str(key))}</td><td>{_status_value(value)}</td></tr>"
        for key, value in values.items()
    )
    return f"<table><tbody>{rows}</tbody></table>"


def render_sequence_table(items: tuple[object, ...], columns: tuple[str, ...]) -> str:
    """Render a list of mappings with fixed columns."""

    header = "".join(f"<th>{html.escape(column)}</th>" for column in columns)
    rows: list[str] = []
    for item in items:
        mapping = _mapping(item)
        cells = "".join(f"<td>{_status_value(mapping.get(column, ''))}</td>" for column in columns)
        rows.append(f"<tr>{cells}</tr>")
    return f"<table><thead><tr>{header}</tr></thead><tbody>{''.join(rows)}</tbody></table>"


def serve_swarm_dashboard(
    *,
    state_path: Path | str = DEFAULT_STATE_PATH,
    host: str = "0.0.0.0",
    port: int = 8765,
) -> None:
    """Serve the dashboard until the process is stopped."""

    handler = _make_handler(Path(state_path))
    server = ThreadingHTTPServer((host, port), handler)
    server.serve_forever()


def _make_handler(state_path: Path) -> type[BaseHTTPRequestHandler]:
    class SwarmDashboardHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path in {"/", "/index.html"}:
                state = load_swarm_state(state_path)
                self._send_text(
                    render_swarm_dashboard_html(state),
                    content_type="text/html; charset=utf-8",
                )
                return
            if self.path == "/state.json":
                state = load_swarm_state(state_path)
                self._send_text(
                    json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True),
                    content_type="application/json; charset=utf-8",
                )
                return
            if self.path == "/healthz":
                status = HTTPStatus.OK if state_path.exists() else HTTPStatus.SERVICE_UNAVAILABLE
                self.send_response(status)
                self.end_headers()
                return
            self.send_error(HTTPStatus.NOT_FOUND)

        def log_message(self, format: str, *args: object) -> None:
            return

        def _send_text(self, body: str, *, content_type: str) -> None:
            encoded = body.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

    return SwarmDashboardHandler


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Serve Neo Swarm dashboard")
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE_PATH)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args(argv)
    serve_swarm_dashboard(state_path=args.state, host=args.host, port=args.port)
    return 0


def _metric(label: str, value: object) -> str:
    return (
        f'<div class="metric"><b>{html.escape(label)}</b>'
        f"<span>{_status_value(value)}</span></div>"
    )


def _status_value(value: object) -> str:
    if isinstance(value, bool):
        css_class = "ok" if value else "bad"
        return f'<span class="{css_class}">{html.escape(str(value))}</span>'
    return html.escape(str(value))


def _mapping(value: object) -> JsonMapping:
    if isinstance(value, Mapping):
        return cast(JsonMapping, value)
    return {}


def _sequence(value: object) -> tuple[object, ...]:
    if isinstance(value, list | tuple):
        return tuple(value)
    return ()


def _text(value: object) -> str:
    return str(value)


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "DEFAULT_STATE_PATH",
    "load_swarm_state",
    "render_key_value_table",
    "render_sequence_table",
    "render_swarm_dashboard_html",
    "serve_swarm_dashboard",
]
