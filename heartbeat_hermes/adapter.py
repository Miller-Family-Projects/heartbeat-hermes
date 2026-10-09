"""Thin Hermes adapter over one heartbeat-core child.

The adapter owns registration, trusted route binding, native delivery and
receipts. Core owns scheduling, durable policy and the tool semantics. The
adapter never composes chat text: the agent's own final reply leaves through
the normal egress path, and delivery is an internal synthetic event with a
synthetic author, no gateway control and a strict current-session fence.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from collections.abc import Callable
from typing import Any

from .core_client import CoreError, CoreOwner

logger = logging.getLogger(__name__)

BEGIN_MARKER = "<<<BEGIN HEARTBEAT DATA>>>"
END_MARKER = "<<<END HEARTBEAT DATA>>>"
PLUGIN_INJECTION_TAG = "heartbeat-hermes"
ADMISSION_TIMEOUT_SECONDS = 15.0


def wrap_envelope(envelope: dict[str, Any]) -> str:
    return f"{BEGIN_MARKER}\n{json.dumps(envelope, separators=(',', ':'))}\n{END_MARKER}"


class HeartbeatAdapter:
    """Bridge between the captured gateway runner and the Core owner.

    Gateway-facing dependencies are injected so the contract is testable
    without a live runtime; ``plugin`` wires the real seams at registration.
    """

    def __init__(
        self,
        core: CoreOwner,
        routing: dict[str, Any],
        resolve_gateway: Callable[[], Any],
        build_event: Callable[..., Any],
        session_binding: Callable[[Any, Any], tuple[str, str] | None],
        send_admission: Callable[[Any, dict[str, Any]], None],
        wrap_egress: Callable[[Any, Callable[[str, str], None] | None], Callable[[], None]],
        poll_seconds: float = 5.0,
    ) -> None:
        self._core = core
        self._routing = routing
        self._resolve_gateway = resolve_gateway
        self._build_event = build_event
        self._session_binding = session_binding
        self._send_admission = send_admission
        self._wrap_egress = wrap_egress
        self._poll_seconds = poll_seconds
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._inflight: dict[str, tuple[Any, str]] = {}
        self._delivered: dict[str, tuple[Any, str, str]] = {}
        self._last_external: bool | None = None
        self._owner_turn_open = False
        self._unwrap_egress: Callable[[], None] | None = None

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._deliver_loop, name="heartbeat-delivery", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
        if self._unwrap_egress is not None:
            self._unwrap_egress()
            self._unwrap_egress = None
        self._core.stop()

    # -- tool projection ---------------------------------------------------

    def tool_schema(self) -> dict[str, Any]:
        return self._core.request("tool_schema", {})

    def run_tool(self, tool: str, arguments: dict[str, Any], external: bool) -> Any:
        return self._core.request(
            "tool",
            {"tool": tool, "arguments": arguments, "context": {"external": external, "confirmation": None}},
        )

    # -- turn evidence -----------------------------------------------------

    def on_gateway_event(self, event: Any) -> None:
        """Bookkeeping for one admitted inbound event (pre_gateway_dispatch).

        The external flag is single-slot: it describes the turn the agent is
        about to run. Per-session refinement is connector parity work shared
        with the OpenClaw/OpenCode adapters.
        """
        source = getattr(event, "source", None)
        if source is None or getattr(event, "internal", False):
            return
        self._last_external = not self._is_owner_route(source)

    def on_owner_turn_admitted(self, source: Any) -> None:
        """One authenticated human turn landed in the bound Gate 0 route."""
        if not self._is_owner_route(source):
            return
        try:
            self._core.request("owner_turn", {"finished": False})
            self._owner_turn_open = True
        except Exception as exc:  # noqa: BLE001 - evidence must never break admission
            logger.warning("heartbeat: owner_turn attestation failed: %s", exc)

    def on_pre_llm_call(self, session_id: str, task_id: str, user_message: Any) -> None:
        """Correlate a delivered envelope at the qualified model-input boundary."""
        batch, native_id = self._inflight.pop(session_id, (None, ""))
        if batch is None or BEGIN_MARKER not in str(user_message):
            return
        try:
            self._core.request(
                "input",
                {
                    "batch": batch,
                    "native_id": native_id,
                    "correlation": {"route": self._route_key(), "session": session_id, "run": task_id},
                },
            )
            self._delivered[session_id] = (batch, native_id, task_id)
        except Exception as exc:  # noqa: BLE001 - receipts are best-effort evidence
            logger.warning("heartbeat: input receipt failed: %s", exc)

    def is_external_turn(self) -> bool:
        external = self._last_external
        self._last_external = None
        return bool(external)

    # -- delivery ----------------------------------------------------------

    def _is_owner_route(self, source: Any) -> bool:
        platform = getattr(source, "platform", None)
        platform_value = getattr(platform, "value", platform)
        return (
            str(platform_value) == str(self._routing.get("platform", ""))
            and str(getattr(source, "chat_id", "") or "") == str(self._routing.get("chat_id", ""))
        )

    def _route_key(self) -> str:
        return f"agent:main:{self._routing.get('platform')}:{self._routing.get('chat_type', 'dm')}:{self._routing.get('chat_id')}"

    def _deliver_loop(self) -> None:
        logger.info("heartbeat: delivery loop started")
        while not self._stop.wait(self._poll_seconds):
            try:
                self._deliver_pending()
            except Exception as exc:  # noqa: BLE001 - the loop must survive one bad tick
                logger.warning("heartbeat: delivery tick failed: %s", exc)
        logger.info("heartbeat: delivery loop stopped")

    def _deliver_pending(self) -> None:
        gateway = self._resolve_gateway()
        if gateway is None:
            return
        runner, loop = gateway
        envelope = self._core.request(
            "offer", {"idle": True, "pointers_readable": True}
        )
        if not envelope:
            self._close_owner_turn(runner)
            return
        binding = self._session_binding(runner, self._routing)
        if binding is None:
            logger.warning("heartbeat: no current session for the bound route; delivery held")
            return
        session_key, session_id = binding
        text = wrap_envelope(envelope)
        event, await_receipt = self._build_event(
            text=text,
            routing=self._routing,
            session_key=session_key,
            session_id=session_id,
        )
        batch = envelope["batch"]
        native_id = str(getattr(event, "message_id", None) or f"heartbeat-{batch}")
        self._inflight[session_key] = (batch, native_id)
        accepted = self._inject(runner, loop, event, await_receipt, session_key, batch, native_id)
        if accepted is not True:
            self._inflight.pop(session_key, None)
            return
        if self._unwrap_egress is None:
            self._unwrap_egress = self._wrap_egress(runner, self._on_outbound)

    def _on_outbound(self, chat_id: str, message_id: str) -> None:
        """One native outbound on the bound route settles the delivered batch."""
        if str(chat_id) != str(self._routing.get("chat_id", "")):
            return
        delivered = next(iter(self._delivered.values()), None)
        if delivered is None:
            return
        batch, native_id, run = delivered
        try:
            self._core.request(
                "outbound",
                {
                    "batch": batch,
                    "message_id": message_id,
                    "correlation": {
                        "route": self._route_key(),
                        "session": native_id,
                        "run": run,
                    },
                },
            )
        except Exception as exc:  # noqa: BLE001 - receipts are best-effort evidence
            logger.warning("heartbeat: outbound receipt failed: %s", exc)

    def _close_owner_turn(self, runner: Any) -> None:
        if not self._owner_turn_open:
            return
        running = getattr(runner, "_running_agents", {}) or {}
        if running:
            return
        try:
            self._core.request("owner_turn", {"finished": True})
        except CoreError as exc:
            logger.warning("heartbeat: owner_turn finish failed: %s", exc.category)
        self._owner_turn_open = False

    def _inject(
        self,
        runner: Any,
        loop: asyncio.AbstractEventLoop,
        event: Any,
        await_receipt: Callable[[], Any],
        session_key: str,
        batch: Any,
        native_id: str,
    ) -> bool | None:
        """Inject one internal event; the receipt, not None, decides acceptance."""

        async def _deliver() -> Any:
            response = await runner._handle_message(event)
            outcome = await asyncio.wait_for(
                await_receipt(), timeout=ADMISSION_TIMEOUT_SECONDS
            )
            return response, outcome

        try:
            future = asyncio.run_coroutine_threadsafe(_deliver(), loop)
            _, outcome = future.result(timeout=ADMISSION_TIMEOUT_SECONDS + 5)
        except Exception as exc:  # noqa: BLE001 - uncertain beats silent success
            logger.warning("heartbeat: wake injection uncertain: %s", exc)
            self._send_admission({"kind": "uncertain"}, batch)
            return None
        status = str(getattr(outcome, "status", None))
        if status in ("delivered", "no_response"):
            self._send_admission({"kind": "admitted", "native_id": native_id}, batch)
            return True
        if status == "rejected":
            logger.info("heartbeat: wake refused by the gateway")
            self._send_admission({"kind": "rejected", "busy": False}, batch)
        else:
            logger.warning("heartbeat: wake outcome %s treated as uncertain", status)
            self._send_admission({"kind": "uncertain"}, batch)
        return False
