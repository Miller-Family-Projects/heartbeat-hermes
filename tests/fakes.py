"""Shared synthetic gateway/core seams for the adapter tests."""

from __future__ import annotations

import asyncio
import sys
import threading
from collections.abc import Callable
from types import ModuleType, SimpleNamespace
from typing import Any


class FakePlatformValue:
    value = "matrix"


class FakePlatform:
    MATRIX = FakePlatformValue()

    def __new__(cls, value: str) -> FakePlatformValue:
        if value != "matrix":
            raise ValueError(value)
        return cls.MATRIX


class FakeSessionSource:
    def __init__(
        self,
        *,
        platform: FakePlatformValue | None = None,
        chat_id: str = "room",
        chat_name: str | None = None,
        chat_type: str = "dm",
        user_id: str = "user",
        user_name: str = "user",
        thread_id: str | None = None,
    ) -> None:
        self.platform = platform or FakePlatform.MATRIX
        self.chat_id = chat_id
        self.chat_name = chat_name
        self.chat_type = chat_type
        self.user_id = user_id
        self.user_name = user_name
        self.thread_id = thread_id


class FakeReceipt:
    def __init__(self, outcome: Any = None) -> None:
        self._outcome = outcome or FakeOutcome("delivered")
        self.completed: list[Any] = []

    async def wait(self) -> Any:
        return self._outcome

    def complete(self, outcome: Any) -> bool:
        self.completed.append(outcome)
        return True


class FakeOutcome:
    def __init__(self, status: str) -> None:
        self.status = status


class FakeMessageEvent:
    def __init__(
        self,
        *,
        text: str = "",
        message_type: str = "text",
        source: FakeSessionSource | None = None,
        internal: bool = False,
    ) -> None:
        self.text = text
        self.message_type = message_type
        self.source = source or FakeSessionSource()
        self.internal = internal
        self.allow_gateway_control = True
        self.metadata: dict[str, Any] = {}
        self.receipt: FakeReceipt | None = None
        self.message_id = "evt-1"


def install_fake_gateway_modules(monkeypatch: Any) -> None:
    monkeypatch.setitem(sys.modules, "gateway", ModuleType("gateway"))
    monkeypatch.setitem(
        sys.modules, "gateway.config", SimpleNamespace(Platform=FakePlatform)
    )
    monkeypatch.setitem(
        sys.modules,
        "gateway.platforms.base",
        SimpleNamespace(MessageEvent=FakeMessageEvent, MessageType=SimpleNamespace(TEXT="text")),
    )
    monkeypatch.setitem(
        sys.modules, "gateway.session", SimpleNamespace(SessionSource=FakeSessionSource)
    )
    monkeypatch.setitem(
        sys.modules, "gateway.receipts", SimpleNamespace(DeliveryReceipt=FakeReceipt)
    )


class FakeRunner:
    """Record injected events on a real gateway-compatible loop."""

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._gateway_loop = loop
        self.adapters = {FakePlatform.MATRIX: self}
        self.events: list[FakeMessageEvent] = []
        self.sent: list[tuple[str, str]] = []
        self.receipt_outcome = FakeOutcome("delivered")
        self._running_agents: dict[str, Any] = {}
        self.session_store = SimpleNamespace(
            lookup_by_session_key=lambda key: SimpleNamespace(session_id="session-1")
        )

    def _session_key_for_source(self, source: FakeSessionSource) -> str:
        return f"matrix:{source.chat_type}:{source.chat_id}"

    async def _handle_message(self, event: FakeMessageEvent) -> str:
        self.events.append(event)
        return ""

    async def send(self, chat_id: str, content: str, **_: Any) -> dict[str, str]:
        self.sent.append((chat_id, content))
        return {"message_id": "out-1"}


class FakeCore:
    """Scripted Core owner recording every forwarded operation."""

    def __init__(self, offers: list[dict[str, Any] | None] | None = None) -> None:
        self.offers = list(offers or [])
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.stopped = False

    def request(self, op: str, params: dict[str, Any], timeout: float | None = None) -> Any:
        self.calls.append((op, params))
        if op == "offer":
            return self.offers.pop(0) if self.offers else None
        if op == "tool_schema":
            return {"version": "heartbeat-tools/v1", "tools": []}
        return {}

    def stop(self) -> None:
        self.stopped = True


def run_loop() -> tuple[asyncio.AbstractEventLoop, Callable[[], None]]:
    owner = asyncio.Runner()
    loop = owner.get_loop()
    ready = threading.Event()
    loop.call_soon(ready.set)
    thread = threading.Thread(target=loop.run_forever)
    thread.start()
    assert ready.wait(timeout=2)

    def close() -> None:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=2)
        owner.close()

    return loop, close


def make_adapter(
    core: FakeCore,
    runner_holder: dict[str, Any],
    offers: list[dict[str, Any] | None],
    build_event: Callable[..., Any] | None = None,
    receipt_status: str = "delivered",
    routing: dict[str, Any] | None = None,
) -> tuple[Any, Any]:
    from heartbeat_hermes.adapter import HeartbeatAdapter

    core.offers = offers
    routing = routing or {"platform": "matrix", "chat_id": "room", "chat_type": "dm"}

    def resolve() -> Any:
        return runner_holder.get("gateway")

    def default_build_event(*, text: str, routing: dict, session_key: str, session_id: str):
        event = FakeMessageEvent(text=text, internal=True)
        event.allow_gateway_control = False
        event.metadata = {
            "gateway_session_key": session_key,
            "gateway_session_id": session_id,
            "gateway_session_strict": True,
            "hermes_plugin_injection": "heartbeat-hermes",
        }
        return event

    admissions: list[dict[str, Any]] = []

    adapter = HeartbeatAdapter(
        core=core,
        routing=routing,
        resolve_gateway=resolve,
        build_event=build_event or default_build_event,
        session_binding=lambda runner, routing: ("matrix:dm:room", "session-1"),
        send_admission=lambda outcome, batch: admissions.append({"outcome": outcome, "batch": batch}),
        wrap_egress=lambda runner, on_outbound: (setattr(runner, "_on_outbound", on_outbound) or (lambda: None)),
        receipt_factory=lambda: FakeReceipt(FakeOutcome(receipt_status)),
        poll_seconds=0.01,
    )
    return adapter, admissions
