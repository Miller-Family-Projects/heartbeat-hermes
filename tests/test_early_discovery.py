"""Discovery-before-import ordering: the waiter must still start the adapter."""

from __future__ import annotations

import sys
import threading
from types import ModuleType
from typing import Any

import fakes
import pytest
import test_startup
from test_startup import Context, FakeCoreChild

from heartbeat_hermes import plugin

isolated_plugin = test_startup.isolated_plugin


@pytest.mark.parametrize(
    "argv",
    [
        ["hermes", "gateway", "run", "--replace"],
        ["hermes", "--profile", "test", "gateway", "run"],
    ],
)
def test_waiter_starts_when_gateway_import_follows_registration(
    argv: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given: the CLI discovery worker finishes before gateway.run is imported.
    fakes.install_fake_gateway_modules(monkeypatch)
    monkeypatch.delitem(sys.modules, "gateway.run", raising=False)
    monkeypatch.setattr(sys, "argv", argv)
    monkeypatch.setattr(plugin, "POLL_SECONDS", 0.01)
    plugin.register(Context())
    # When: no gateway module exists yet.
    # Then: the bounded waiter was claimed anyway.
    assert plugin._startup_waiter is not None
    assert plugin._startup_waiter.thread is not None
    assert plugin._adapter is None


@pytest.mark.parametrize(
    "argv",
    [
        ["hermes", "plugins", "list"],
        ["hermes", "gateway", "status"],
        ["hermes", "dashboard"],
    ],
)
def test_cli_surfaces_never_start_the_waiter(
    argv: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given: a non-running CLI surface shares the configured plugin.
    monkeypatch.delitem(sys.modules, "gateway.run", raising=False)
    monkeypatch.setattr(sys, "argv", argv)
    plugin.register(Context())
    # Then: it neither waits for a gateway nor starts an adapter.
    assert plugin._startup_waiter is None
    assert plugin._adapter is None


def test_envelope_reaches_runner_after_late_import_without_inbound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: registration happened before the import; the loop binds late.
    fakes.install_fake_gateway_modules(monkeypatch)
    module = ModuleType("gateway.run")
    monkeypatch.setattr(module, "GatewayRunner", fakes.FakeRunner, raising=False)
    monkeypatch.setitem(sys.modules, "gateway.run", module)
    monkeypatch.setattr(sys, "argv", ["hermes", "gateway", "run"])
    monkeypatch.setattr(plugin, "POLL_SECONDS", 0.01)
    delivered = threading.Event()

    loop, close = fakes.run_loop()
    runner = fakes.FakeRunner(loop)
    import weakref

    monkeypatch.setattr(module, "_gateway_runner_ref", weakref.ref(runner), raising=False)

    async def record(event: Any) -> str:
        runner.events.append(event)
        delivered.set()
        return ""

    monkeypatch.setattr(runner, "_handle_message", record)
    monkeypatch.setattr(plugin, "POLL_SECONDS", 0.01)
    plugin.register(Context())
    # When: the gateway publishes its runner and an envelope is offered.
    assert plugin._adapter is not None
    envelope = {
        "tag": "heartbeat-data",
        "version": 1,
        "batch": 3,
        "data_not_instructions": True,
        "guidance": "",
        "backlog": False,
        "unhandled": 0,
        "dead_letter": 0,
        "members": [],
    }

    offered = {"done": False}

    def fake_request(self: FakeCoreChild, op: str, params: dict[str, Any], timeout: Any = None) -> Any:
        self.requests.append((op, params))
        if op == "tool_schema":
            return {"version": "heartbeat-tools/v1", "tools": []}
        if op == "offer" and not offered["done"]:
            offered["done"] = True
            return envelope
        return {}

    monkeypatch.setattr(FakeCoreChild, "request", fake_request)
    try:
        # Then: the envelope reaches the runner as one internal event, no inbound.
        assert delivered.wait(timeout=4)
        assert len(runner.events) == 1
        assert runner.events[0].internal is True
        assert plugin._gateway_runner is None
        deadline = threading.Event()
        found: list[Any] = []

        def collect() -> None:
            while not deadline.wait(0.01):
                rows = [
                    r for r in FakeCoreChild.instances[0].requests if r[0] == "admission"
                ]
                if rows:
                    found.extend(rows)
                    return

        collector = threading.Thread(target=collect)
        collector.start()
        collector.join(timeout=4)
        deadline.set()
        assert found == [
            ("admission", {"batch": 3, "outcome": {"kind": "admitted", "native_id": "evt-1"}})
        ]
    finally:
        close()
        if plugin._adapter is not None:
            plugin._adapter.stop()
            plugin._adapter = None
