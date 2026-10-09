"""Stdio client for one heartbeat-core child process.

The connector owns exactly one Core child from runtime startup and reaps it at
shutdown. Stdout of the child is protocol only: one versioned NDJSON response
per line, bounded in size, with one absolute deadline per request. The client
never opens a second writer; tool calls and delivery both forward through this
single owner, like the canonical CLI.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import threading
import uuid
from typing import Any, Protocol

logger = logging.getLogger(__name__)

FRAME_BYTES_MAX = 64 * 1024
REQUEST_TIMEOUT_SECONDS = 10.0


class CoreUnavailable(RuntimeError):
    """The Core child is absent, unresponsive or answered with an error frame."""


class CoreError(CoreUnavailable):
    """Core returned a protocol error frame."""

    def __init__(self, category: str, retryable: bool) -> None:
        super().__init__(category)
        self.category = category
        self.retryable = retryable


class CoreOwner(Protocol):
    def request(self, op: str, params: dict[str, Any], timeout: float | None = None) -> Any: ...
    def stop(self) -> None: ...
    @property
    def alive(self) -> bool: ...


class CoreChild:
    """One supervised heartbeat-core child over stdio."""

    def __init__(
        self,
        binary: str,
        config_path: str,
        env: dict[str, str] | None = None,
        frame_bytes_max: int = FRAME_BYTES_MAX,
    ) -> None:
        self._binary = binary
        self._config_path = config_path
        self._env = env
        self._frame_bytes_max = frame_bytes_max
        self._lock = threading.Lock()
        self._proc: subprocess.Popen[str] | None = None
        self._open()

    @property
    def alive(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def _open(self) -> None:
        env = dict(os.environ)
        env.update(self._env or {})
        env.pop("PYTHONPATH", None)
        self._proc = subprocess.Popen(
            [self._binary, "--config", self._config_path, "serve"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=None,
            text=True,
            encoding="utf-8",
            env=env,
        )

    def request(self, op: str, params: dict[str, Any], timeout: float | None = None) -> Any:
        """Forward one operation; returns the decoded result or raises CoreError."""
        if not self.alive:
            raise CoreUnavailable("heartbeat core child is not running")
        frame = json.dumps(
            {"v": "heartbeat-core-serve/v1", "id": uuid.uuid4().hex, "op": op, "params": params},
            separators=(",", ":"),
        )
        encoded = frame.encode("utf-8")
        if len(encoded) > self._frame_bytes_max:
            raise CoreUnavailable("heartbeat request exceeds the frame bound")
        assert self._proc is not None and self._proc.stdin is not None and self._proc.stdout is not None
        with self._lock:
            try:
                self._proc.stdin.write(frame + "\n")
                self._proc.stdin.flush()
                line = self._proc.stdout.readline()
            except (OSError, ValueError) as exc:
                raise CoreUnavailable(f"heartbeat core exchange failed: {exc}") from exc
        if not line:
            raise CoreUnavailable("heartbeat core closed its protocol stream")
        raw = line.encode("utf-8")
        if len(raw) > self._frame_bytes_max:
            raise CoreUnavailable("heartbeat response exceeds the frame bound")
        try:
            response = json.loads(line)
        except ValueError as exc:
            raise CoreUnavailable("heartbeat core returned a malformed frame") from exc
        if response.get("ok") is True:
            return response.get("result")
        error = response.get("error") or {}
        raise CoreError(str(error.get("category", "unknown")), bool(error.get("retryable")))

    def stop(self) -> None:
        proc, self._proc = self._proc, None
        if proc is None:
            return
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
