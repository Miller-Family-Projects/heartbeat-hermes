"""Bounded readiness polling before the gateway publishes its running loop."""

from __future__ import annotations

import importlib
import logging
import math
import threading
import time
from collections.abc import Callable
from typing import Final, Protocol, TypedDict, runtime_checkable

logger = logging.getLogger(__name__)
DEFAULT_STARTUP_TIMEOUT_SECONDS: Final = 120.0


class StartupEntry(TypedDict, total=False):
    startup_timeout_seconds: float | str | bool | None


class PluginConfig(TypedDict, total=False):
    entries: dict[str, StartupEntry]


class HermesConfig(TypedDict, total=False):
    plugins: PluginConfig


@runtime_checkable
class ConfigModule(Protocol):
    def load_config(self) -> HermesConfig: ...


def load_startup_timeout() -> float:
    """Read the optional positive, finite startup timeout from Hermes config."""
    try:
        module = importlib.import_module("hermes_cli.config")
        if not isinstance(module, ConfigModule):
            logger.warning("heartbeat: config loader unavailable, using default startup timeout")
            return DEFAULT_STARTUP_TIMEOUT_SECONDS
        config = module.load_config()
        value = config.get("plugins", {}).get("entries", {}).get("heartbeat-hermes", {}).get(
            "startup_timeout_seconds", DEFAULT_STARTUP_TIMEOUT_SECONDS
        )
        if value is None or isinstance(value, bool):
            logger.warning("heartbeat: invalid startup timeout, using default")
            return DEFAULT_STARTUP_TIMEOUT_SECONDS
        timeout = float(value)
        if not math.isfinite(timeout) or timeout <= 0:
            logger.warning("heartbeat: invalid startup timeout, using default")
            return DEFAULT_STARTUP_TIMEOUT_SECONDS
        return timeout
    except ImportError:
        return DEFAULT_STARTUP_TIMEOUT_SECONDS
    except (AttributeError, TypeError, ValueError, OSError):
        logger.warning("heartbeat: startup timeout unavailable, using default")
        return DEFAULT_STARTUP_TIMEOUT_SECONDS


class GatewayWaiter:
    """One cancellable worker; registration owns creation under its start lock."""

    def __init__(self, timeout_seconds: float, poll_seconds: float) -> None:
        self.timeout_seconds: float = timeout_seconds
        self.poll_seconds: float = poll_seconds
        self.stop: threading.Event = threading.Event()
        self.thread: threading.Thread | None = None

    def start(self, ready: Callable[[], bool], claim: Callable[[], bool]) -> None:
        if ready():
            _ = claim()
            return
        self.thread = threading.Thread(
            target=self._wait, args=(ready, claim), name="heartbeat-gateway-waiter", daemon=True
        )
        self.thread.start()

    def _wait(self, ready: Callable[[], bool], claim: Callable[[], bool]) -> None:
        deadline = time.monotonic() + self.timeout_seconds
        while not self.stop.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                logger.warning("heartbeat: gateway startup timed out, capture fallback remains available")
                return
            if ready():
                _ = claim()
                return
            if self.stop.wait(min(self.poll_seconds, remaining)):
                return
