from __future__ import annotations

import asyncio  # Gateway interoperability requires its native event loop.
import logging
import sys
import threading
import weakref
from collections.abc import Iterator
from functools import partial
from pathlib import Path
from types import ModuleType

import pytest
from test_contract import (
    _FakeMessageEvent,
    _FakePlatform,
    _install_fake_gateway_modules,
)

from heartbeat_hermes import plugin


class Context:
    def register_tool(self, **kwargs) -> None:
        pass

    def register_hook(self, name, callback) -> None:
        pass


class Runner:
    """Record synthetic events on a real gateway-compatible loop."""

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._gateway_loop = loop
        self.adapters = {_FakePlatform.MATRIX: self}
        self.events: list[_FakeMessageEvent] = []

    async def _handle_message(self, event: _FakeMessageEvent) -> str:
        self.events.append(event)
        return ""


@pytest.fixture(autouse=True)
def isolated_plugin(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    for name in (
        "_gateway_runner",
        "_gateway_loop",
        "_routing",
        "_pinned_routing",
        "_scheduler_thread",
        "_scheduler_lock_fd",
    ):
        monkeypatch.setattr(plugin, name, None)
    monkeypatch.setattr(plugin, "_owns_scheduler_lock", False)
    monkeypatch.setattr(plugin, "_scheduler_stop", threading.Event())
    monkeypatch.setattr(plugin, "_runner_warning_active", False, raising=False)
    yield
    plugin._scheduler_stop.set()
    if plugin._scheduler_thread is not None:
        plugin._scheduler_thread.join(timeout=2)
        assert not plugin._scheduler_thread.is_alive()
    if plugin._scheduler_lock_fd is not None:
        plugin._scheduler_lock_fd.close()


@pytest.fixture
def runner(monkeypatch: pytest.MonkeyPatch) -> Iterator[Runner]:
    _install_fake_gateway_modules(monkeypatch)
    module = ModuleType("gateway.run")
    monkeypatch.setattr(module, "GatewayRunner", Runner, raising=False)
    monkeypatch.setitem(sys.modules, "gateway.run", module)
    with asyncio.Runner() as owner:
        loop = owner.get_loop()
        ready = threading.Event()
        loop.call_soon(ready.set)
        thread = threading.Thread(target=loop.run_forever)
        thread.start()
        assert ready.wait(timeout=2)
        gateway = Runner(loop)
        monkeypatch.setattr(
            module, "_gateway_runner_ref", weakref.ref(gateway), raising=False
        )
        plugin._routing = {
            "platform": "matrix",
            "chat_id": "example-room",
            "chat_type": "dm",
        }
        try:
            yield gateway
        finally:
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=2)
            assert not thread.is_alive()


def test_scheduler_evaluates_when_registered_without_inbound(
    runner: Runner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: a saved, due watch and no captured runner.
    plugin.heartbeat_watch_tool("timer", seconds=1)
    evaluated = threading.Event()

    def evaluate(name, watch, now) -> str:
        evaluated.set()
        return "pending"

    monkeypatch.setattr(plugin, "evaluate_watch", evaluate)
    # When: the plugin loads, without dispatching any inbound event.
    plugin.register(Context())
    # Then: its real scheduler evaluates the saved watch.
    assert evaluated.wait(timeout=2)
    assert plugin._gateway_runner is None


def test_repeated_registration_keeps_one_scheduler_and_lock(
    runner: Runner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: simultaneous initial registrations against the real file lock.
    acquire = plugin._try_acquire_scheduler_lock
    attempts: list[bool] = []

    def counted_acquire() -> bool:
        result = acquire()
        attempts.append(result)
        return result

    monkeypatch.setattr(plugin, "_try_acquire_scheduler_lock", counted_acquire)
    threads = [
        threading.Thread(target=plugin.register, args=(Context(),)) for _ in range(8)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)
    scheduler = plugin._scheduler_thread
    lock = plugin._scheduler_lock_fd
    # When: Hermes registers the plugin again.
    plugin.register(Context())
    # Then: neither the scheduler nor the held lock is replaced.
    assert scheduler is not None and scheduler.is_alive()
    assert lock is not None and not lock.closed
    assert plugin._scheduler_thread is scheduler
    assert plugin._scheduler_lock_fd is lock
    assert attempts == [True]


@pytest.mark.parametrize("module_loaded", [False, True])
def test_registration_leaves_lock_available_when_gateway_is_not_running(
    module_loaded: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: a CLI surface, possibly with gateway.run imported for shared helpers.
    _install_fake_gateway_modules(monkeypatch)
    monkeypatch.delitem(sys.modules, "gateway.run", raising=False)
    if module_loaded:
        module = ModuleType("gateway.run")
        monkeypatch.setattr(module, "GatewayRunner", Runner, raising=False)
        monkeypatch.setattr(module, "_gateway_runner_ref", lambda: None, raising=False)
        monkeypatch.setitem(sys.modules, "gateway.run", module)
    # When: that surface registers the plugin.
    plugin.register(Context())
    # Then: it neither starts scheduling nor takes the gateway's file lock.
    assert not plugin._owns_scheduler_lock
    assert plugin._scheduler_thread is None
    assert plugin._scheduler_lock_fd is None
    assert plugin._try_acquire_scheduler_lock()


def test_registration_leaves_lock_available_when_gateway_loop_is_not_running(
    runner: Runner,
) -> None:
    # Given: an imported runner whose loop is not running, not a started gateway.
    with asyncio.Runner() as owner:
        runner._gateway_loop = owner.get_loop()
        # When: the plugin registers before gateway startup.
        plugin.register(Context())
    # Then: the process leaves scheduling ownership available.
    assert not plugin._owns_scheduler_lock
    assert plugin._scheduler_thread is None
    assert plugin._try_acquire_scheduler_lock()


def test_capture_claims_scheduler_when_registration_had_no_gateway(
    runner: Runner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: registration without a live runner reference leaves the lock unclaimed.
    monkeypatch.setattr(sys.modules["gateway.run"], "_gateway_runner_ref", lambda: None)
    plugin.register(Context())
    assert not plugin._owns_scheduler_lock
    # When: pre_gateway_dispatch captures the gateway on its running loop.
    runner._gateway_loop.call_soon_threadsafe(
        partial(plugin._capture_gateway, gateway=runner)
    )
    # Then: capture owns the real scheduler lock and starts scheduling.
    captured = threading.Event()
    runner._gateway_loop.call_soon_threadsafe(captured.set)
    assert captured.wait(timeout=2)
    assert plugin._owns_scheduler_lock
    assert plugin._scheduler_thread is not None
    assert plugin._scheduler_thread.is_alive()


def test_wake_uses_weakref_when_runner_not_captured(runner: Runner) -> None:
    # Given: a live gateway weakref and an explicit route, but no capture.
    assert plugin._gateway_runner is None
    # When: a wake is injected from outside the event loop.
    delivered = plugin._inject_wake("finding")
    # Then: the same internal dispatch reaches the runner exactly once.
    assert delivered
    assert len(runner.events) == 1
    assert runner.events[0].internal
    assert runner.events[0].source.user_id == "system:heartbeat"


def test_wake_prefers_captured_runner_when_weakref_differs(runner: Runner) -> None:
    # Given: a captured runner different from the weakref target.
    captured = Runner(runner._gateway_loop)
    plugin._gateway_runner = captured
    plugin._gateway_loop = captured._gateway_loop
    # When: a wake is injected.
    assert plugin._inject_wake("finding")
    # Then: capture wins and the weakref target receives nothing.
    assert len(captured.events) == 1
    assert runner.events == []


def test_scheduler_persists_finding_and_retries_on_next_tick(
    runner: Runner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: a due one-shot command with a long interval and no available runner.
    module = sys.modules["gateway.run"]
    monkeypatch.setattr(module, "_gateway_runner_ref", lambda: None)
    plugin.heartbeat_watch_tool("job", command="check", interval=3600, once=True)
    watches = plugin._load_watches()
    watches["job"]["next_run"] = 0
    with plugin._state_lock:
        plugin._save_watches_locked(watches)
    now = 0.0
    checks: list[str] = []
    snapshots = []
    evaluate = plugin.evaluate_watch

    def check(command: str, timeout: float) -> str:
        checks.append(command)
        return "transient" if len(checks) == 1 else ""

    def evaluate_with_check(name, watch, timestamp) -> str:
        return evaluate(name, watch, timestamp, runner=check)

    def advance_tick(timeout: float) -> bool:
        nonlocal now
        snapshots.append(plugin._load_watches())
        now += timeout
        monkeypatch.setattr(module, "_gateway_runner_ref", weakref.ref(runner))
        if len(snapshots) == 3:
            plugin._scheduler_stop.set()
        return False

    monkeypatch.setattr(plugin.time, "time", lambda: now)
    monkeypatch.setattr(plugin, "evaluate_watch", evaluate_with_check)
    monkeypatch.setattr(plugin._scheduler_stop, "wait", advance_tick)
    # When: the real scheduler evaluates three deterministic ticks.
    plugin._scheduler_loop()
    # Then: the finding is persisted, retried next tick, and removed after one delivery.
    assert snapshots[0]["job"]["pending_finding"] == "[heartbeat: job] transient"
    assert snapshots[0]["job"]["next_run"] == plugin.POLL_SECONDS
    assert snapshots[1:] == [{}, {}]
    assert checks == ["check"]
    assert len(runner.events) == 1


def test_warning_rearms_when_runner_becomes_unavailable_again(
    runner: Runner,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Given: one unavailable episode followed by a successful delivery.
    module = sys.modules["gateway.run"]
    monkeypatch.setattr(module, "_gateway_runner_ref", lambda: None)
    with caplog.at_level(logging.WARNING, logger=plugin.__name__):
        assert not plugin._inject_wake("finding")
        assert not plugin._inject_wake("finding")
        monkeypatch.setattr(module, "_gateway_runner_ref", weakref.ref(runner))
        assert plugin._inject_wake("finding")
        monkeypatch.setattr(module, "_gateway_runner_ref", lambda: None)
        # When: a new unavailable episode starts.
        assert not plugin._inject_wake("next finding")
        assert not plugin._inject_wake("next finding")
    # Then: each episode emits just one warning.
    assert len(caplog.records) == 2


def test_wake_stays_pending_when_gateway_module_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: delivery runs in a surface without the gateway module.
    monkeypatch.setitem(sys.modules, "gateway", None)
    # When: a timer is due.
    outcome = plugin.evaluate_watch("timer", {"type": "timer", "deadline": 0}, 1)
    # Then: no gateway import failure escapes and the timer stays pending.
    assert outcome == "pending"


def test_finding_survives_missing_runner_until_delivered_once(
    runner: Runner,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Given: a transient command finding while no runner is available.
    module = sys.modules["gateway.run"]
    monkeypatch.setattr(module, "_gateway_runner_ref", lambda: None)
    checks: list[str] = []

    def check(command: str, timeout: float) -> str:
        checks.append(command)
        return "transient finding" if len(checks) == 1 else ""

    watch = {"type": "command", "command": "check", "once": True}
    with caplog.at_level(logging.WARNING, logger=plugin.__name__):
        assert plugin.evaluate_watch("job", watch, 1, runner=check) == "pending"
        assert plugin.evaluate_watch("job", watch, 2, runner=check) == "pending"
    monkeypatch.setattr(module, "_gateway_runner_ref", weakref.ref(runner))
    # When: the runner appears on the following tick.
    outcome = plugin.evaluate_watch("job", watch, 3, runner=check)
    # Then: the original finding is delivered once without re-running the check.
    assert outcome == "remove"
    assert len(checks) == 1
    assert len(runner.events) == 1
    assert runner.events[0].text == "[heartbeat: job] transient finding"
    assert len(caplog.records) == 1


@pytest.mark.parametrize("reference", [None, 7, lambda: None])
def test_wake_stays_pending_when_compatibility_reference_unavailable(
    reference,
    runner: Runner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: a missing, malformed, or empty compatibility reference.
    monkeypatch.setattr(sys.modules["gateway.run"], "_gateway_runner_ref", reference)
    # When: a due timer attempts delivery.
    watch = {"type": "timer", "deadline": 0, "note": "done"}
    outcome = plugin.evaluate_watch("timer", watch, 1)
    # Then: it stays armed and no event reaches the gateway.
    assert outcome == "pending"
    assert runner.events == []
