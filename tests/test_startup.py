"""Startup capture and bounded readiness for the thin adapter.

The adapter must start at plugin initialization, without any inbound message:
the runner is captured through the pinned private seam and readiness waiting
is bounded. Non-gateway CLI surfaces never start the waiter.
"""

from __future__ import annotations

import sys
import threading
import weakref
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import Any

import fakes
import pytest

from heartbeat_hermes import plugin


class Context:
    def __init__(self) -> None:
        self.tools: list[dict[str, Any]] = []
        self.hooks: list[str] = []

    def register_tool(self, **kwargs: Any) -> None:
        self.tools.append(kwargs)

    def register_hook(self, name: str, callback: Any) -> None:
        self.hooks.append(name)


class FakeCoreChild:
    instances: list[FakeCoreChild] = []  # noqa: RUF012 - per-test registry reset by the isolated fixture

    def __init__(self, binary: str, config_path: str, env: Any = None) -> None:
        self.binary = binary
        self.config_path = config_path
        self.requests: list[tuple[str, dict[str, Any]]] = []
        self.stopped = False
        FakeCoreChild.instances.append(self)

    def request(self, op: str, params: dict[str, Any], timeout: float | None = None) -> Any:
        self.requests.append((op, params))
        if op == "tool_schema":
            return {
                "version": "heartbeat-tools/v1",
                "tools": [
                    {
                        "name": "heartbeat_watch",
                        "description": "watch",
                        "parameters": {"type": "object", "properties": {}},
                    }
                ],
            }
        if op == "offer":
            return None
        return {}

    @property
    def alive(self) -> bool:
        return not self.stopped

    def stop(self) -> None:
        self.stopped = True


@pytest.fixture(autouse=True)
def isolated_plugin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[None]:
    FakeCoreChild.instances = []
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("HEARTBEAT_CORE_BINARY", "/nix/store/fake/bin/heartbeat-core")
    monkeypatch.setenv("HEARTBEAT_CORE_CONFIG", str(tmp_path / "core.json"))
    monkeypatch.setenv("HEARTBEAT_DELIVER_PLATFORM", "matrix")
    monkeypatch.setenv("HEARTBEAT_DELIVER_CHAT_ID", "room")
    monkeypatch.setattr(plugin, "CoreChild", FakeCoreChild)
    for name in ("_gateway_runner", "_gateway_loop", "_routing", "_pinned_routing", "_startup_waiter", "_adapter", "_schema"):
        monkeypatch.setattr(plugin, name, None)
    yield
    if plugin._adapter is not None:
        plugin._adapter.stop()
        plugin._adapter = None


@pytest.fixture
def runner(monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    fakes.install_fake_gateway_modules(monkeypatch)
    module = ModuleType("gateway.run")
    monkeypatch.setattr(module, "GatewayRunner", fakes.FakeRunner, raising=False)
    monkeypatch.setitem(sys.modules, "gateway.run", module)
    loop, close = fakes.run_loop()
    gateway = fakes.FakeRunner(loop)
    monkeypatch.setattr(module, "_gateway_runner_ref", weakref.ref(gateway), raising=False)
    try:
        yield gateway
    finally:
        close()


def test_adapter_starts_when_loop_becomes_ready_without_inbound(
    runner: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given: discovery happens before the runner's loop is bound.
    loop = runner._gateway_loop
    runner._gateway_loop = None
    monkeypatch.setattr(plugin, "POLL_SECONDS", 0.01)
    # When: the plugin registers with no inbound dispatch.
    plugin.register(Context())
    assert plugin._adapter is None
    assert plugin._startup_waiter is not None
    # When: startup binds the running loop.
    runner._gateway_loop = loop
    deadline = threading.Event()
    result: list[bool] = []

    def poll() -> None:
        while not deadline.wait(0.01):
            if plugin._adapter is not None:
                result.append(True)
                return
        result.append(False)

    thread = threading.Thread(target=poll)
    thread.start()
    thread.join(timeout=3)
    deadline.set()
    assert result == [True]
    assert plugin._gateway_runner is None


def test_immediate_registration_starts_adapter_and_registers_schema_tools(
    runner: Any,
) -> None:
    # Given: a running gateway before registration.
    context = Context()
    plugin.register(context)
    # Then: the adapter is live and tools come from the Core schema.
    assert plugin._adapter is not None
    assert [tool["name"] for tool in context.tools] == ["heartbeat_watch"]
    assert context.hooks == ["pre_gateway_dispatch", "pre_llm_call", "post_gateway_admission"]
    # And: the Core child was asked for its schema exactly once.
    child = FakeCoreChild.instances[0]
    assert child.requests[0][0] == "tool_schema"


def test_missing_core_configuration_keeps_adapter_inactive(
    runner: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("HEARTBEAT_CORE_BINARY")
    monkeypatch.delenv("HEARTBEAT_CORE_CONFIG")
    context = Context()
    plugin.register(context)
    assert plugin._adapter is None
    assert context.tools == []


def test_cli_registration_never_starts_waiter(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given: a non-gateway process has no gateway.run module.
    monkeypatch.delitem(sys.modules, "gateway.run", raising=False)
    monkeypatch.setattr(sys, "argv", ["hermes", "plugins", "list"])
    # When: plugin management registers it repeatedly.
    for _ in range(4):
        plugin.register(Context())
    # Then: no readiness worker was claimed.
    assert plugin._startup_waiter is None
    assert plugin._adapter is None


def test_repeated_registrations_start_one_waiter_and_adapter(
    runner: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given: the runner exists but its loop is not bound during discovery.
    loop = runner._gateway_loop
    runner._gateway_loop = None
    monkeypatch.setattr(plugin, "POLL_SECONDS", 0.01)
    threads = [threading.Thread(target=plugin.register, args=(Context(),)) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)
        assert not thread.is_alive()
    waiter = plugin._startup_waiter
    assert waiter is not None and waiter.thread is not None
    assert plugin._adapter is None
    # When: startup binds the loop.
    runner._gateway_loop = loop
    waiter.thread.join(timeout=3)
    assert plugin._adapter is not None
    assert len(FakeCoreChild.instances) == 1
