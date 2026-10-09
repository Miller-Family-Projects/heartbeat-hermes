"""Thin heartbeat adapter plugin for Hermes.

The plugin owns registration and trusted binding only. One heartbeat-core
child over stdio owns scheduling, durable policy and the tool semantics; the
agent's tools are exactly Core's `tool_schema`, forwarded unchanged.

Delivery is one internal synthetic event per offered envelope through the
captured gateway runner, with a synthetic author, no gateway control, a
strict current-session fence and an explicit delivery receipt. The plugin
never composes or relays chat text; the agent's own reply leaves through the
normal egress path. Startup needs no inbound message: the runner is captured
at initialization through the pinned private seam and readiness is bounded.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import threading
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from .adapter import PLUGIN_INJECTION_TAG, HeartbeatAdapter
from .core_client import CoreChild

if TYPE_CHECKING:
    from gateway.run import GatewayRunner

logger = logging.getLogger(__name__)

POLL_SECONDS = 5.0
STATE_DIRNAME = "heartbeat"

_gateway_runner: Any = None
_gateway_loop: Any = None
_routing: dict[str, Any] | None = None
_pinned_routing: dict[str, Any] | None = None
_startup_waiter: Any = None
_adapter: HeartbeatAdapter | None = None
_schema: dict[str, Any] = {}
_registration_lock = threading.Lock()
_runner_warning_active = False


# ---------------------------------------------------------------------------
# Gateway capture (pinned private seam, contract-tested)
# ---------------------------------------------------------------------------


def _resolve_adapter(runner: Any, platform_value: str) -> Any:
    for platform, adapter in getattr(runner, "adapters", {}).items():
        value = platform.value if hasattr(platform, "value") else str(platform)
        if value == platform_value:
            return adapter
    return None


def _resolve_gateway() -> tuple[GatewayRunner, asyncio.AbstractEventLoop] | None:
    """Prefer hook capture; fallback uses Hermes' private runner ref/loop."""
    if _gateway_runner is not None and _gateway_loop is not None:
        return _gateway_runner, _gateway_loop
    try:
        run = sys.modules.get("gateway.run")
        reference: Callable[[], GatewayRunner | None] | None = getattr(run, "_gateway_runner_ref", None)
        runner = reference() if callable(reference) else None
        if run is None or runner is None or not isinstance(runner, run.GatewayRunner):
            return None
        loop = getattr(runner, "_gateway_loop", None)
        if loop is None or not loop.is_running():
            return None
        return runner, loop
    except (ImportError, AttributeError, TypeError):
        return None


def _capture_gateway(**kwargs: Any) -> None:
    global _gateway_runner, _gateway_loop, _routing

    gateway = kwargs.get("gateway")
    if gateway is not None and _gateway_runner is None:
        _gateway_runner = gateway
        try:
            _gateway_loop = asyncio.get_running_loop()
        except RuntimeError:
            try:
                _gateway_loop = asyncio.get_event_loop()
            except RuntimeError:
                _gateway_loop = None
        logger.info("heartbeat: captured gateway runner")

    if _pinned_routing is not None:
        _routing = _pinned_routing
    elif _routing is None:
        source = getattr(kwargs.get("event"), "source", None)
        if source is not None:
            platform = getattr(source, "platform", None)
            _routing = {
                "platform": platform.value if platform is not None and hasattr(platform, "value") else str(platform or ""),
                "chat_id": getattr(source, "chat_id", "") or "",
                "chat_name": getattr(source, "chat_name", None),
                "chat_type": getattr(source, "chat_type", "dm") or "dm",
                "thread_id": getattr(source, "thread_id", None),
            }

    if _adapter is not None and kwargs.get("event") is not None:
        _adapter.on_gateway_event(kwargs["event"])


# ---------------------------------------------------------------------------
# Real gateway seams for the adapter
# ---------------------------------------------------------------------------


def _build_event(
    *, text: str, routing: dict[str, Any], session_key: str, session_id: str
) -> tuple[Any, Callable[[], Any]]:
    from gateway.platforms.base import MessageEvent, MessageType
    from gateway.receipts import DeliveryReceipt
    from gateway.session import SessionSource

    try:
        from gateway.config import Platform

        platform = Platform(str(routing.get("platform", "")))
    except (ImportError, ValueError) as exc:
        raise RuntimeError(f"heartbeat: unknown platform {routing.get('platform')!r}") from exc

    source = SessionSource(
        platform=platform,
        chat_id=str(routing.get("chat_id", "")),
        chat_name=routing.get("chat_name"),
        chat_type=routing.get("chat_type", "dm"),
        user_id="system:heartbeat",
        user_name="heartbeat",
        thread_id=routing.get("thread_id"),
    )
    receipt = DeliveryReceipt()
    event = MessageEvent(
        text=text,
        message_type=MessageType.TEXT,
        source=source,
        internal=True,
    )
    event.allow_gateway_control = False
    event.receipt = receipt
    metadata = dict(event.metadata or {}) if hasattr(event, "metadata") else {}
    metadata.update(
        {
            "hermes_plugin_injection": PLUGIN_INJECTION_TAG,
            "gateway_session_key": session_key,
            "gateway_session_id": session_id,
            "gateway_session_strict": True,
            "notification_category": "heartbeat",
        }
    )
    event.metadata = metadata
    return event, receipt.wait


def _session_binding(runner: Any, routing: dict[str, Any]) -> tuple[str, str] | None:
    adapter = _resolve_adapter(runner, str(routing.get("platform", "")))
    if adapter is None:
        return None
    probe = getattr(runner, "_session_key_for_source", None)
    store = getattr(runner, "session_store", None)
    if not callable(probe) or store is None:
        return None
    try:
        from gateway.config import Platform
        from gateway.session import SessionSource

        source = SessionSource(
            platform=Platform(str(routing.get("platform", ""))),
            chat_id=str(routing.get("chat_id", "")),
            chat_name=routing.get("chat_name"),
            chat_type=routing.get("chat_type", "dm"),
            user_id="system:heartbeat",
            user_name="heartbeat",
            thread_id=routing.get("thread_id"),
        )
        session_key = probe(source)
        if not isinstance(session_key, str) or not session_key:
            return None
        entry = store.lookup_by_session_key(session_key)
        session_id = getattr(entry, "session_id", None) if entry is not None else None
        if not isinstance(session_id, str) or not session_id:
            return None
        return session_key, session_id
    except Exception as exc:  # noqa: BLE001 - held delivery beats a wrong binding
        logger.warning("heartbeat: session binding unresolved: %s", exc)
        return None


def _wrap_egress(
    runner: Any, on_outbound: Callable[[str, str], None] | None
) -> Callable[[], None]:
    """Capture native outbound message ids for the bound route; returns unwrap."""
    adapter = _resolve_adapter(runner, str((_routing or {}).get("platform", "")))
    if adapter is None or on_outbound is None or "send" not in vars(type(adapter)):
        return lambda: None
    original = adapter.send

    async def send_with_receipt(*args: Any, **kwargs: Any) -> Any:
        result = await original(*args, **kwargs)
        chat_id = kwargs.get("chat_id", args[0] if args else None)
        message_id = getattr(result, "message_id", None) or (result or {}) if isinstance(result, dict) else None
        if chat_id is not None and message_id:
            on_outbound(str(chat_id), str(message_id))
        return result

    adapter.send = send_with_receipt

    def unwrap() -> None:
        if getattr(adapter, "send", None) is send_with_receipt:
            adapter.send = original

    return unwrap


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def _plugin_config() -> dict[str, Any]:
    try:
        from hermes_cli.config import load_config

        raw = (load_config() or {}).get("plugins", {}).get("entries", {}).get("heartbeat-hermes", {})
        return raw if isinstance(raw, dict) else {}
    except Exception:  # noqa: BLE001 - config absence disables, never crashes
        return {}


def _load_pinned_routing() -> dict[str, Any] | None:
    """Resolve an explicit wake target from config.yaml or env, if any."""
    cfg = _plugin_config()
    platform = os.environ.get("HEARTBEAT_DELIVER_PLATFORM") or cfg.get("deliver_platform")
    chat_id = os.environ.get("HEARTBEAT_DELIVER_CHAT_ID") or cfg.get("deliver_chat_id")
    if not platform or not chat_id:
        return None
    return {
        "platform": str(platform),
        "chat_id": str(chat_id),
        "chat_name": cfg.get("deliver_chat_name"),
        "chat_type": str(cfg.get("deliver_chat_type", "dm")),
        "thread_id": cfg.get("deliver_thread_id"),
    }


def _start_adapter() -> bool:
    """Start the Core child and the delivery loop after gateway readiness."""
    global _adapter, _schema
    if _adapter is not None:
        return True
    cfg = _plugin_config()
    binary = os.environ.get("HEARTBEAT_CORE_BINARY") or cfg.get("core_binary")
    config_path = os.environ.get("HEARTBEAT_CORE_CONFIG") or cfg.get("core_config")
    if not binary or not config_path:
        logger.warning(
            "heartbeat: core_binary/core_config unset; adapter stays inactive "
            "(set plugins.entries.heartbeat-hermes.core_binary and .core_config)"
        )
        return False
    if _routing is None:
        logger.warning("heartbeat: no bound route; adapter stays inactive")
        return False
    try:
        core = CoreChild(str(binary), str(config_path))
        _schema = core.request("tool_schema", {})
    except Exception as exc:  # noqa: BLE001 - visible failure, no half-owner
        logger.error("heartbeat: core child failed to start: %s", exc)
        return False

    _adapter = HeartbeatAdapter(
        core=core,
        routing=_routing,
        resolve_gateway=_resolve_gateway,
        build_event=_build_event,
        session_binding=_session_binding,
        send_admission=lambda outcome, batch: core.request(
            "admission", {"batch": batch, "outcome": outcome}
        ),
        wrap_egress=_wrap_egress,
        poll_seconds=POLL_SECONDS,
    )
    _adapter.start()
    logger.info("heartbeat: adapter started over the core child")
    return True


def _register_tools(register_tool: Callable[..., Any]) -> None:
    for tool in (_schema or {}).get("tools", []):
        name = str(tool.get("name", ""))
        if not name:
            continue

        def handler(args: dict[str, Any], _name: str = name, **_: Any) -> str:
            assert _adapter is not None
            external = _adapter.is_external_turn()
            try:
                result = _adapter.run_tool(_name, args, external)
            except Exception as exc:  # noqa: BLE001 - tool surface returns errors as data
                return json.dumps({"error": str(exc)})
            return json.dumps(result)

        register_tool(
            name=name,
            handler=handler,
            schema=tool,
            toolset="heartbeat",
            description=str(tool.get("description", name)),
            check_fn=lambda: True,
        )


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


def _on_pre_llm_call(**kwargs: Any) -> None:
    if _adapter is None:
        return
    _adapter.on_pre_llm_call(
        session_id=str(kwargs.get("session_id") or ""),
        task_id=str(kwargs.get("task_id") or kwargs.get("turn_id") or ""),
        user_message=kwargs.get("user_message"),
    )
    return


def _on_post_gateway_admission(**kwargs: Any) -> None:
    if _adapter is None:
        return
    source = kwargs.get("source") or {}
    if not isinstance(source, dict):
        return
    _adapter.on_owner_turn_admitted(type("Source", (), source)())
    return


def register(ctx: Any) -> None:
    """Register heartbeat tools and gateway hooks; start needs no inbound."""
    global _pinned_routing, _routing, _startup_waiter
    _pinned_routing = _load_pinned_routing()
    if _pinned_routing is not None:
        _routing = _pinned_routing

    from .startup import GatewayWaiter, gateway_start_requested, load_startup_timeout

    def ready() -> bool:
        return _resolve_gateway() is not None

    def claim() -> bool:
        return _start_adapter()

    if sys.modules.get("gateway.run") is not None or gateway_start_requested(sys.argv[1:]):
        with _registration_lock:
            if _startup_waiter is None:
                if ready():
                    claim()
                else:
                    _startup_waiter = GatewayWaiter(load_startup_timeout(), POLL_SECONDS)
                    _startup_waiter.start(ready, claim)

    ctx.register_hook("pre_gateway_dispatch", _capture_gateway)
    ctx.register_hook("pre_llm_call", _on_pre_llm_call)
    ctx.register_hook("post_gateway_admission", _on_post_gateway_admission)
    _register_tools(ctx.register_tool)
    logger.info("heartbeat plugin registered")
