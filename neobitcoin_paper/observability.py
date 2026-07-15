"""Local health endpoints, metrics, and secret-safe structured logging."""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Final

_TOKEN_PATTERN: Final = re.compile(r"(?<![A-Za-z0-9_.=-])t\.[A-Za-z0-9_.=-]{20,}")
_SECRET_ASSIGNMENT: Final = re.compile(
    r"(?i)(token|authorization|api[_-]?key|secret)(\s*[:=]\s*)([^\s,;]+)"
)


def redact(value: object) -> str:
    text = str(value)
    text = _TOKEN_PATTERN.sub("***", text)
    return _SECRET_ASSIGNMENT.sub(lambda match: f"{match.group(1)}{match.group(2)}***", text)


class JsonLogFormatter(logging.Formatter):
    """Compact structured logs whose rendered values are always redacted."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "ts": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": redact(record.getMessage()),
        }
        for field in (
            "correlation_id",
            "strategy_id",
            "strategy_version",
            "session_date",
            "event_id",
        ):
            item = getattr(record, field, None)
            if item is not None:
                payload[field] = redact(item)
        if record.exc_info:
            payload["exception"] = redact(self.formatException(record.exc_info))
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def configure_json_logging(level: int = logging.INFO) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonLogFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)


@dataclass(frozen=True, slots=True)
class HealthSnapshot:
    healthy: bool
    ready: bool
    generated_at: str
    uptime_seconds: float
    components: dict[str, dict[str, object]]
    gauges: dict[str, float]
    counters: dict[str, float]


class HealthRegistry:
    """Thread-safe state shared by runtime workers and local probes."""

    def __init__(self) -> None:
        self._started = time.monotonic()
        self._lock = threading.RLock()
        self._components: dict[str, dict[str, object]] = {}
        self._gauges: dict[str, float] = {}
        self._counters: defaultdict[str, float] = defaultdict(float)

    def heartbeat(
        self,
        component: str,
        *,
        healthy: bool = True,
        ready: bool | None = None,
        detail: str = "",
    ) -> None:
        now = datetime.now(UTC).isoformat()
        with self._lock:
            self._components[component] = {
                "healthy": healthy,
                "ready": healthy if ready is None else ready,
                "detail": redact(detail),
                "last_heartbeat": now,
                "monotonic": time.monotonic(),
            }

    def stale_component(self, component: str, max_age_seconds: float) -> bool:
        with self._lock:
            item = self._components.get(component)
            if item is None:
                return True
            monotonic = item.get("monotonic")
            return (
                not isinstance(monotonic, float)
                or time.monotonic() - monotonic > max_age_seconds
            )

    def set_gauge(self, name: str, value: float | int) -> None:
        with self._lock:
            self._gauges[name] = float(value)

    def increment(self, name: str, amount: float | int = 1) -> None:
        with self._lock:
            self._counters[name] += float(amount)

    def snapshot(self) -> HealthSnapshot:
        with self._lock:
            components = {
                name: {key: value for key, value in item.items() if key != "monotonic"}
                for name, item in self._components.items()
            }
            healthy = bool(components) and all(
                bool(item.get("healthy")) for item in components.values()
            )
            ready = healthy and all(bool(item.get("ready")) for item in components.values())
            return HealthSnapshot(
                healthy=healthy,
                ready=ready,
                generated_at=datetime.now(UTC).isoformat(),
                uptime_seconds=max(0.0, time.monotonic() - self._started),
                components=components,
                gauges=dict(self._gauges),
                counters=dict(self._counters),
            )

    def prometheus(self) -> str:
        snapshot = self.snapshot()
        lines = [
            "# TYPE neobitcoin_paper_uptime_seconds gauge",
            f"neobitcoin_paper_uptime_seconds {snapshot.uptime_seconds:.6f}",
            f"neobitcoin_paper_health {1 if snapshot.healthy else 0}",
            f"neobitcoin_paper_ready {1 if snapshot.ready else 0}",
        ]
        for name, value in sorted(snapshot.gauges.items()):
            lines.append(f"neobitcoin_paper_{_metric_name(name)} {value}")
        for name, value in sorted(snapshot.counters.items()):
            lines.append(f"neobitcoin_paper_{_metric_name(name)}_total {value}")
        return "\n".join(lines) + "\n"


def _metric_name(value: str) -> str:
    return "".join(character if character.isalnum() else "_" for character in value.casefold())


class HealthServer:
    """Loopback-only HTTP server for ``/healthz``, ``/readyz``, and metrics."""

    def __init__(self, registry: HealthRegistry, host: str, port: int) -> None:
        if host not in {"127.0.0.1", "::1", "localhost"}:
            raise ValueError("health server must bind to loopback")
        self._registry = registry
        self._server = ThreadingHTTPServer((host, port), self._handler_type())
        self._thread: threading.Thread | None = None

    def _handler_type(self) -> type[BaseHTTPRequestHandler]:
        registry = self._registry

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                snapshot = registry.snapshot()
                if self.path == "/healthz":
                    self._json(snapshot.healthy, snapshot)
                elif self.path == "/readyz":
                    self._json(snapshot.ready, snapshot)
                elif self.path == "/metrics":
                    body = registry.prometheus().encode("utf-8")
                    self.send_response(HTTPStatus.OK)
                    self.send_header("Content-Type", "text/plain; version=0.0.4")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                else:
                    self.send_error(HTTPStatus.NOT_FOUND)

            def _json(self, ok: bool, snapshot: HealthSnapshot) -> None:
                body = json.dumps(
                    {
                        "status": "ok" if ok else "unavailable",
                        "generated_at": snapshot.generated_at,
                        "uptime_seconds": snapshot.uptime_seconds,
                        "components": snapshot.components,
                    },
                    ensure_ascii=False,
                ).encode("utf-8")
                self.send_response(HTTPStatus.OK if ok else HTTPStatus.SERVICE_UNAVAILABLE)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: object) -> None:
                return

        return Handler

    @property
    def address(self) -> tuple[str, int]:
        host, port = self._server.server_address[:2]
        return str(host), int(port)

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="neobitcoin-paper-health",
            daemon=True,
        )
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None


__all__ = [
    "HealthRegistry",
    "HealthServer",
    "HealthSnapshot",
    "JsonLogFormatter",
    "configure_json_logging",
    "redact",
]
