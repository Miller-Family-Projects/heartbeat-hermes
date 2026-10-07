from __future__ import annotations

import sys
import threading

import pytest
import test_startup
from test_contract import _FakeMessageEvent
from test_startup import Context, Runner

from heartbeat_hermes import plugin

isolated_plugin = test_startup.isolated_plugin
runner = test_startup.runner


@pytest.mark.parametrize("argv", [
    ["hermes", "gateway", "run", "--replace"],
    ["hermes", "--profile", "test", "gateway", "run"],
])
def test_timer_reaches_runner_when_discovery_precedes_gateway_import(
    argv: list[str], runner: Runner, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: the CLI discovery worker finishes before gateway.run is imported.
    module = sys.modules["gateway.run"]
    monkeypatch.delitem(sys.modules, "gateway.run")
    monkeypatch.setattr(sys, "argv", argv)
    monkeypatch.setattr(plugin, "POLL_SECONDS", 0.01)
    delivered = threading.Event()

    async def record(event: _FakeMessageEvent) -> str:
        runner.events.append(event)
        delivered.set()
        return ""

    monkeypatch.setattr(runner, "_handle_message", record)
    plugin.heartbeat_watch_tool("timer", seconds=1)
    with plugin._state_lock:
        watches = plugin._load_watches()
        watches["timer"]["deadline"] = 0
        plugin._save_watches_locked(watches)
    plugin.register(Context())
    # When: the gateway publishes its real loop without any inbound dispatch.
    monkeypatch.setitem(sys.modules, "gateway.run", module)
    # Then: the saved timer reaches the runner as an internal event.
    assert delivered.wait(timeout=2)
    assert len(runner.events) == 1
    assert runner.events[0].internal
    assert plugin._gateway_runner is None


@pytest.mark.parametrize("argv", [
    ["hermes", "plugins", "list"],
    ["hermes", "gateway", "status"],
    ["hermes", "dashboard"],
])
def test_cli_leaves_scheduler_lock_available_when_gateway_is_not_imported(
    argv: list[str], monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: a non-running CLI surface shares the configured plugin.
    monkeypatch.delitem(sys.modules, "gateway.run", raising=False)
    monkeypatch.setattr(sys, "argv", argv)
    # When: the CLI registers the plugin.
    plugin.register(Context())
    # Then: it neither waits for a gateway nor takes scheduling ownership.
    assert plugin._startup_waiter is None
    assert not plugin._owns_scheduler_lock
    assert plugin._scheduler_lock_fd is None
