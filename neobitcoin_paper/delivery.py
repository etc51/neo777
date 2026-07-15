"""Idempotent same-thread delivery over the official Codex App Server."""

from __future__ import annotations

import hashlib
import json
import os
import queue
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Protocol


class DeliveryError(RuntimeError):
    """A retryable or permanent artifact-delivery failure."""


@dataclass(frozen=True, slots=True)
class ArtifactDescriptor:
    archive_id: str
    session_date: str
    sha256: str
    size_bytes: int
    strategies: int
    signals: int
    paper_trades: int
    pnl_summary: str
    restart_gap_summary: str
    local_path: Path | None = None
    resource_uri: str | None = None

    def verify_accessible(self) -> str:
        if self.local_path is not None:
            path = self.local_path.expanduser().resolve(strict=True)
            if not path.is_file():
                raise DeliveryError("artifact path is not a regular file")
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            if digest != self.sha256:
                raise DeliveryError("artifact SHA-256 does not match")
            if path.stat().st_size != self.size_bytes:
                raise DeliveryError("artifact size does not match")
            return str(path)
        uri = (self.resource_uri or "").strip()
        if not uri or "://" not in uri:
            raise DeliveryError("artifact has no accessible path or resource URI")
        required = (self.session_date, self.sha256, self.archive_id)
        if not all(item in uri for item in required):
            raise DeliveryError("artifact resource URI is not content-addressed")
        return uri


@dataclass(frozen=True, slots=True)
class DeliveryAcknowledgement:
    thread_id: str
    turn_id: str
    status: str
    artifact_reference: str


def build_delivery_message(artifact: ArtifactDescriptor) -> str:
    reference = artifact.verify_accessible()
    return (
        f"Готов проверенный paper-архив Необиткоина за session date "
        f"{artifact.session_date}.\n"
        f"Archive ID: {artifact.archive_id}\n"
        f"SHA-256: {artifact.sha256}\n"
        f"Strategies: {artifact.strategies}\n"
        f"Signals: {artifact.signals}\n"
        f"Paper trades: {artifact.paper_trades}\n"
        f"Net PnL by strategy: {artifact.pnl_summary}\n"
        f"Restarts/gaps: {artifact.restart_gap_summary}\n"
        f"Artifact: {reference}\n\n"
        "Проведи независимый анализ:\n"
        "1. обнови накопительную OOS-статистику всех замороженных стратегий;\n"
        "2. не отдавай приоритет существующим идеям;\n"
        "3. ищи любые воспроизводимые закономерности с положительным матожиданием;\n"
        "4. учитывай исполнимость, spread, slippage, latency и независимость эпизодов;\n"
        "5. отделяй discovery от OOS;\n"
        "6. отклонённые гипотезы сохраняй в реестре;\n"
        "7. новые правила оформляй только как новые immutable strategy versions."
    )


class JsonRpcChannel(Protocol):
    def request(self, method: str, params: dict[str, object]) -> dict[str, object]: ...

    def notify(self, method: str, params: dict[str, object]) -> None: ...

    def wait_notification(self, method: str, timeout_seconds: float) -> dict[str, object]: ...

    def close(self) -> None: ...


class AppServerChannel:
    """Small stdio JSON-RPC client for ``codex app-server``."""

    def __init__(
        self,
        command: tuple[str, ...] = ("codex", "app-server"),
        *,
        timeout_seconds: float = 60.0,
        process: subprocess.Popen[str] | None = None,
    ) -> None:
        self._timeout_seconds = timeout_seconds
        self._process = process or subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            encoding="utf-8",
            bufsize=1,
            env=dict(os.environ),
        )
        if self._process.stdin is None or self._process.stdout is None:
            raise DeliveryError("Codex App Server stdio is unavailable")
        self._stdin: IO[str] = self._process.stdin
        self._stdout: IO[str] = self._process.stdout
        self._messages: queue.Queue[dict[str, object]] = queue.Queue()
        self._pending: list[dict[str, object]] = []
        self._next_id = 1
        self._reader = threading.Thread(target=self._read, daemon=True)
        self._reader.start()

    def _read(self) -> None:
        for line in self._stdout:
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                self._messages.put(payload)

    def _send(self, payload: dict[str, object]) -> None:
        self._stdin.write(json.dumps(payload, separators=(",", ":")) + "\n")
        self._stdin.flush()

    def request(self, method: str, params: dict[str, object]) -> dict[str, object]:
        request_id = self._next_id
        self._next_id += 1
        self._send({"method": method, "id": request_id, "params": params})
        deadline = time.monotonic() + self._timeout_seconds
        while time.monotonic() < deadline:
            message = self._take(deadline - time.monotonic())
            if message.get("id") == request_id:
                error = message.get("error")
                if error is not None:
                    raise DeliveryError(f"Codex App Server rejected {method}")
                result = message.get("result")
                return result if isinstance(result, dict) else {}
            self._pending.append(message)
        raise DeliveryError(f"Codex App Server timed out during {method}")

    def notify(self, method: str, params: dict[str, object]) -> None:
        self._send({"method": method, "params": params})

    def wait_notification(self, method: str, timeout_seconds: float) -> dict[str, object]:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            for index, message in enumerate(self._pending):
                if message.get("method") == method:
                    return self._pending.pop(index)
            message = self._take(deadline - time.monotonic())
            if message.get("method") == method:
                return message
            self._pending.append(message)
        raise DeliveryError(f"Codex App Server did not emit {method}")

    def _take(self, timeout_seconds: float) -> dict[str, object]:
        try:
            return self._messages.get(timeout=max(0.01, timeout_seconds))
        except queue.Empty as exc:
            raise DeliveryError("Codex App Server response timed out") from exc

    def close(self) -> None:
        if self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._process.kill()
        self._reader.join(timeout=1)


class CodexSameThreadTransport:
    """Resume one existing thread and create exactly one new turn in it."""

    def __init__(self, channel_factory: type[AppServerChannel] = AppServerChannel) -> None:
        self._channel_factory = channel_factory

    def deliver(
        self,
        *,
        thread_id: str,
        artifact: ArtifactDescriptor,
        timeout_seconds: float = 900.0,
    ) -> DeliveryAcknowledgement:
        normalized_thread = thread_id.strip()
        if not normalized_thread:
            raise DeliveryError("Codex thread ID is missing")
        reference = artifact.verify_accessible()
        channel: JsonRpcChannel = self._channel_factory()
        try:
            channel.request(
                "initialize",
                {
                    "clientInfo": {
                        "name": "neobitcoin_paper_delivery",
                        "title": "Neobitcoin Paper Archive Delivery",
                        "version": "1.0.0",
                    }
                },
            )
            channel.notify("initialized", {})
            resumed = channel.request("thread/resume", {"threadId": normalized_thread})
            resumed_thread = resumed.get("thread")
            if isinstance(resumed_thread, dict):
                actual_id = str(resumed_thread.get("id", normalized_thread))
                if actual_id != normalized_thread:
                    raise DeliveryError("Codex resumed a different thread")
            started = channel.request(
                "turn/start",
                {
                    "threadId": normalized_thread,
                    "input": [{"type": "text", "text": build_delivery_message(artifact)}],
                },
            )
            turn = started.get("turn")
            turn_id = str(turn.get("id", "")) if isinstance(turn, dict) else ""
            notification = channel.wait_notification("turn/completed", timeout_seconds)
            params = notification.get("params")
            completed_turn = params.get("turn") if isinstance(params, dict) else None
            completed_id = (
                str(completed_turn.get("id", "")) if isinstance(completed_turn, dict) else ""
            )
            status = (
                str(completed_turn.get("status", "completed"))
                if isinstance(completed_turn, dict)
                else "completed"
            )
            if turn_id and completed_id and turn_id != completed_id:
                raise DeliveryError("Codex completed an unexpected turn")
            if status.casefold() not in {"completed", "success"}:
                raise DeliveryError("Codex turn did not complete successfully")
            return DeliveryAcknowledgement(
                thread_id=normalized_thread,
                turn_id=completed_id or turn_id,
                status=status,
                artifact_reference=reference,
            )
        finally:
            channel.close()


__all__ = [
    "AppServerChannel",
    "ArtifactDescriptor",
    "CodexSameThreadTransport",
    "DeliveryAcknowledgement",
    "DeliveryError",
    "JsonRpcChannel",
    "build_delivery_message",
]
