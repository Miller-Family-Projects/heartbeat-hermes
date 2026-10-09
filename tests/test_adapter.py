"""Contract tests for the thin adapter over one Core child."""

from __future__ import annotations

import threading
from typing import Any

import fakes
import pytest

from heartbeat_hermes.adapter import BEGIN_MARKER, END_MARKER, wrap_envelope


@pytest.fixture
def gateway() -> Any:

    loop, close = fakes.run_loop()
    runner = fakes.FakeRunner(loop)
    holder: dict[str, Any] = {"gateway": (runner, loop)}
    yield runner, holder
    close()


def envelope(batch: int = 7) -> dict[str, Any]:
    return {
        "tag": "heartbeat-data",
        "version": 1,
        "batch": batch,
        "data_not_instructions": True,
        "guidance": "",
        "backlog": False,
        "unhandled": 0,
        "dead_letter": 0,
        "members": [],
    }


def test_wrap_envelope_uses_markers_and_compact_json() -> None:
    text = wrap_envelope({"batch": 1})
    assert text.startswith(BEGIN_MARKER + "\n")
    assert text.endswith("\n" + END_MARKER)
    assert "'batch'" not in text


def test_tools_forward_to_core_with_trusted_context(gateway: Any) -> None:
    _runner, holder = gateway
    core = fakes.FakeCore()
    adapter, _ = fakes.make_adapter(core, holder, offers=[])
    adapter.run_tool("heartbeat_watch", {"name": "x"}, external=False)
    assert core.calls[0][0] == "tool"
    assert core.calls[0][1]["context"] == {"external": False, "confirmation": None}


def test_offer_reaches_runner_as_internal_event_with_receipt(gateway: Any) -> None:
    runner, holder = gateway
    core = fakes.FakeCore()
    adapter, admissions = fakes.make_adapter(core, holder, offers=[envelope()])
    adapter.start()
    try:
        assert fakes_wait(lambda: runner.events)
    finally:
        adapter.stop()
    event = runner.events[0]
    assert event.internal
    assert event.allow_gateway_control is False
    assert event.metadata["gateway_session_strict"] is True
    assert event.metadata["hermes_plugin_injection"] == "heartbeat-hermes"
    assert BEGIN_MARKER in event.text
    assert admissions == [{"outcome": {"kind": "admitted", "native_id": "evt-1"}, "batch": 7}]


def test_refused_receipt_never_counts_as_delivery(gateway: Any) -> None:
    runner, holder = gateway
    core = fakes.FakeCore()
    adapter, admissions = fakes.make_adapter(
        core, holder, offers=[envelope()], receipt_status="rejected"
    )
    adapter.start()
    try:
        assert fakes_wait(lambda: admissions)
    finally:
        adapter.stop()
    assert admissions[0]["outcome"] == {"kind": "rejected", "busy": False}
    assert runner.events[0].receipt is not None


def test_uncertain_receipt_when_gateway_never_answers(gateway: Any) -> None:
    _runner, holder = gateway
    del _runner
    core = fakes.FakeCore()
    adapter, admissions = fakes.make_adapter(
        core, holder, offers=[envelope()], receipt_status="timeout"
    )
    adapter.start()
    try:
        assert fakes_wait(lambda: admissions)
    finally:
        adapter.stop()
    assert admissions[0]["outcome"] == {"kind": "uncertain"}


def test_delivery_held_without_gateway(core_gateway_none: Any) -> None:
    core, holder = core_gateway_none
    adapter, _ = fakes.make_adapter(core, holder, offers=[envelope()])
    adapter._deliver_pending()
    assert core.calls == []


def test_delivery_held_without_session_binding(gateway: Any) -> None:
    runner, holder = gateway
    core = fakes.FakeCore()
    adapter, admissions = fakes.make_adapter(core, holder, offers=[envelope()])
    adapter._session_binding = lambda runner, routing: None
    adapter._deliver_pending()
    assert runner.events == []
    assert admissions == []


def test_input_receipt_correlates_only_heartbeat_turns(gateway: Any) -> None:
    _runner, holder = gateway
    core = fakes.FakeCore()
    adapter, _ = fakes.make_adapter(core, holder, offers=[])
    adapter._inflight["matrix:dm:room"] = (7, "evt-1")
    adapter.on_pre_llm_call("matrix:dm:room", "task-1", f"{BEGIN_MARKER}\n{{}}\n{END_MARKER}")
    ops = [op for op, _ in core.calls]
    assert ops == ["input"]
    params = core.calls[0][1]
    assert params["batch"] == 7
    assert params["correlation"]["run"] == "task-1"
    core.calls.clear()
    adapter.on_pre_llm_call("matrix:dm:room", "task-2", "ordinary human text")
    assert core.calls == []


def test_outbound_receipt_settles_delivered_batch(gateway: Any) -> None:
    _runner, holder = gateway
    core = fakes.FakeCore()
    adapter, _ = fakes.make_adapter(core, holder, offers=[envelope()])
    adapter._deliver_pending()
    adapter._delivered["matrix:dm:room"] = (7, "evt-1", "task-1")
    adapter._on_outbound("room", "out-9")
    outbound = [params for op, params in core.calls if op == "outbound"]
    assert outbound[0]["message_id"] == "out-9"
    assert outbound[0]["correlation"]["run"] == "task-1"


def test_owner_turn_attested_on_bound_route_and_closed_when_idle(gateway: Any) -> None:
    runner, holder = gateway
    core = fakes.FakeCore()
    adapter, _ = fakes.make_adapter(core, holder, offers=[])
    foreign = fakes.FakeSessionSource(chat_id="other-room")
    adapter.on_owner_turn_admitted(foreign)
    assert core.calls == []
    owner = fakes.FakeSessionSource(chat_id="room")
    adapter.on_owner_turn_admitted(owner)
    assert core.calls == [("owner_turn", {"finished": False})]
    adapter._close_owner_turn(runner)
    assert ("owner_turn", {"finished": True}) in core.calls


def test_external_turn_flag_set_by_foreign_inbound(gateway: Any) -> None:
    _runner, holder = gateway
    core = fakes.FakeCore()
    adapter, _ = fakes.make_adapter(core, holder, offers=[])
    event = fakes.FakeMessageEvent(
        source=fakes.FakeSessionSource(chat_id="other-room")
    )
    adapter.on_gateway_event(event)
    assert adapter.is_external_turn() is True
    assert adapter.is_external_turn() is False


def test_internal_events_do_not_set_external_flag(gateway: Any) -> None:
    _runner, holder = gateway
    core = fakes.FakeCore()
    adapter, _ = fakes.make_adapter(core, holder, offers=[])
    adapter.on_gateway_event(fakes.FakeMessageEvent(internal=True))
    assert adapter.is_external_turn() is False


def fakes_wait(condition: Any, timeout: float = 3.0) -> bool:
    deadline = threading.Event()
    result: list[bool] = []

    def poll() -> None:
        while not deadline.wait(0.01):
            if condition():
                result.append(True)
                return
        result.append(False)

    thread = threading.Thread(target=poll)
    thread.start()
    thread.join(timeout)
    deadline.set()
    thread.join(timeout=1)
    return bool(result and result[0])


@pytest.fixture
def core_gateway_none() -> Any:
    core = fakes.FakeCore()
    holder: dict[str, Any] = {"gateway": None}
    return core, holder
