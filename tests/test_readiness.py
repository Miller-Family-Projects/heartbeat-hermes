from __future__ import annotations

import logging
import sys
import threading
from functools import partial
from types import ModuleType

import pytest
import test_startup
from test_startup import Context, Runner

from heartbeat_hermes import plugin, startup

isolated_plugin = test_startup.isolated_plugin
runner = test_startup.runner


def test_scheduler_starts_when_loop_becomes_ready_after_registration(
    runner: Runner, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: discovery happens before the runner's loop is bound.
    loop = runner._gateway_loop
    monkeypatch.setattr(runner, "_gateway_loop", None)
    evaluated = threading.Event()
    monkeypatch.setattr(plugin, "evaluate_watch", lambda *args: evaluated.set() or "pending")
    monkeypatch.setattr(plugin, "POLL_SECONDS", 0.01)
    plugin.heartbeat_watch_tool("timer", seconds=1)
    plugin.register(Context())
    assert not plugin._owns_scheduler_lock
    assert plugin._scheduler_thread is None
    # When: startup binds the running loop, with no inbound dispatch.
    runner._gateway_loop = loop
    # Then: the waiter starts the real scheduler and the saved watch is evaluated.
    assert evaluated.wait(timeout=2)
    assert plugin._owns_scheduler_lock
    assert plugin._gateway_runner is None


def test_cli_registration_never_starts_waiter(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given: a non-gateway process has no gateway.run module.
    monkeypatch.delitem(sys.modules, "gateway.run", raising=False)
    # When: plugin management registers it repeatedly.
    for _ in range(4):
        plugin.register(Context())
    # Then: no readiness worker or scheduler lock is claimed.
    assert plugin._startup_waiter is None
    assert not plugin._owns_scheduler_lock


def test_repeated_registrations_before_readiness_start_one_waiter_and_scheduler(
    runner: Runner, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: the runner exists but its loop is not bound during repeated discovery.
    loop = runner._gateway_loop
    monkeypatch.setattr(runner, "_gateway_loop", None)
    monkeypatch.setattr(plugin, "POLL_SECONDS", 0.01)
    acquire = plugin._try_acquire_scheduler_lock
    attempts: list[bool] = []

    def counted_acquire() -> bool:
        result = acquire()
        attempts.append(result)
        return result

    monkeypatch.setattr(plugin, "_try_acquire_scheduler_lock", counted_acquire)
    threads = [threading.Thread(target=plugin.register, args=(Context(),)) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)
        assert not thread.is_alive()
    waiter = plugin._startup_waiter
    assert waiter is not None and waiter.thread is not None
    assert waiter.thread.is_alive()
    assert attempts == []
    # When: startup binds the loop and discovery runs again after readiness.
    runner._gateway_loop = loop
    waiter.thread.join(timeout=2)
    scheduler = plugin._scheduler_thread
    plugin.register(Context())
    # Then: one worker made one lock claim and started one real scheduler.
    assert not waiter.thread.is_alive()
    assert plugin._startup_waiter is waiter
    assert attempts == [True]
    assert scheduler is not None and scheduler.is_alive()
    assert plugin._scheduler_thread is scheduler


def test_waiter_times_out_at_configured_deadline_without_claim(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    # Given: readiness never arrives and a deterministic clock advances on waits.
    waiter = startup.GatewayWaiter(timeout_seconds=7, poll_seconds=5)
    now = 0.0
    waits: list[float] = []
    claims: list[bool] = []

    def advance(seconds: float) -> bool:
        nonlocal now
        waits.append(seconds)
        now += seconds
        return False

    monkeypatch.setattr(startup.time, "monotonic", lambda: now)
    monkeypatch.setattr(waiter.stop, "wait", advance)
    # When: the readiness worker reaches its deadline.
    with caplog.at_level(logging.WARNING, logger=startup.__name__):
        waiter._wait(lambda: False, lambda: claims.append(True) or True)
    # Then: the final wait is bounded by the deadline and fallback is logged once.
    assert waits == [5, 2]
    assert claims == []
    assert len(caplog.records) == 1


def test_capture_still_claims_after_waiter_timeout(
    runner: Runner, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    # Given: the startup worker expires without a published runner.
    monkeypatch.setattr(sys.modules["gateway.run"], "_gateway_runner_ref", lambda: None)
    monkeypatch.setattr(plugin, "load_startup_timeout", lambda: 0.0)
    plugin.register(Context())
    waiter = plugin._startup_waiter
    assert waiter is not None and waiter.thread is not None
    waiter.thread.join(timeout=2)
    assert not waiter.thread.is_alive()
    assert not plugin._owns_scheduler_lock
    plugin.register(Context())
    assert plugin._startup_waiter is waiter
    # When: the original capture path sees its first inbound dispatch.
    captured = threading.Event()
    runner._gateway_loop.call_soon_threadsafe(partial(plugin._capture_gateway, gateway=runner))
    runner._gateway_loop.call_soon_threadsafe(captured.set)
    assert captured.wait(timeout=2)
    # Then: capture starts the real scheduler, with no second timeout warning.
    assert plugin._owns_scheduler_lock
    assert plugin._scheduler_thread is not None and plugin._scheduler_thread.is_alive()
    assert len([record for record in caplog.records if record.name == startup.__name__]) == 1


@pytest.mark.parametrize("value", [17.5, "42", 0, -1, float("nan"), float("inf"), None, True, "bad"])
def test_startup_timeout_resolves_config_with_invalid_value_fallback(
    value: float | str | bool | None, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: Hermes exposes a configured timeout through its normal config loader.
    module = ModuleType("hermes_cli.config")
    monkeypatch.setattr(module, "load_config", lambda: {
        "plugins": {"entries": {"heartbeat-hermes": {"startup_timeout_seconds": value}}}
    }, raising=False)
    monkeypatch.setitem(sys.modules, "hermes_cli.config", module)
    expected = {17.5: 17.5, "42": 42.0}.get(value, startup.DEFAULT_STARTUP_TIMEOUT_SECONDS)
    # When: startup reads its timeout.
    timeout = startup.load_startup_timeout()
    # Then: valid configured values win; invalid values retain a bounded default.
    assert timeout == expected
