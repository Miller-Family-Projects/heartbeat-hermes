from __future__ import annotations

from collections.abc import Iterator

import pytest

from heartbeat_hermes import plugin


@pytest.fixture(autouse=True)
def readiness_state(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(plugin, "_startup_waiter", None)
    yield
    waiter = plugin._startup_waiter
    if waiter is not None:
        waiter.stop.set()
        if waiter.thread is not None:
            waiter.thread.join(timeout=2)
            assert not waiter.thread.is_alive()
