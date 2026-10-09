"""Protected import of a legacy heartbeat watches.json into Core.

Lab/synthetic scope: this module is the import path for an owner-produced
export of the retired plugin's watches. It never reads live state by itself;
the operator (or the owner agent at the approved cutover) hands it a JSON
document. The original file stays unchanged as evidence. The import is
all-or-nothing per watch, idempotent, and dry-run by default.

Legacy shapes (retired plugin):

- timer: ``{type: timer, deadline, seconds, interval, note, repeat, enabled}``
- command: ``{type: command, command, interval, once, note, enabled}``

Deadlines are interpreted relative to a supplied reference time, so a stale
export fires one catch-up occurrence instead of silently arming a past timer.
"""

from __future__ import annotations

import json
import time
from typing import Any, Protocol

TIMER_KIND = "timer"
COMMAND_KIND = "command"


class MigrationError(ValueError):
    """One watch is malformed; the whole import refuses to run."""


class CoreLike(Protocol):
    def request(self, op: str, params: dict[str, Any], timeout: float | None = None) -> Any: ...


def parse_legacy(document: str, *, now: float | None = None) -> list[dict[str, Any]]:
    """Validate a legacy export and map it to Core watch operations.

    Returns one descriptor per watch, in document order. Raises MigrationError
    on any malformed entry, before anything is forwarded.
    """
    try:
        raw = json.loads(document)
    except ValueError as exc:
        raise MigrationError(f"legacy export is not valid JSON: {exc}") from exc
    if not isinstance(raw, dict):
        raise MigrationError("legacy export must be a JSON object keyed by watch name")
    reference = time.time() if now is None else now
    watches = []
    for name, entry in raw.items():
        if not isinstance(name, str) or not name.strip():
            raise MigrationError("watch names must be non-empty strings")
        if not isinstance(entry, dict):
            raise MigrationError(f"watch {name!r} must be an object")
        watches.append(_map_watch(name.strip(), entry, reference))
    return watches


def _map_watch(name: str, entry: dict[str, Any], reference: float) -> dict[str, Any]:
    kind = entry.get("type", COMMAND_KIND)
    note = str(entry.get("note") or "")
    enabled = bool(entry.get("enabled", True))
    if kind == TIMER_KIND:
        seconds = _finite_positive(entry.get("seconds"), name)
        interval_ms = max(5000, min(60_000, int(seconds * 1000 / 10)))
        deadline = float(entry.get("deadline", 0.0))
        remaining = max(1, int(deadline - reference))
        return {
            "id": name,
            "name": name,
            "kind": TIMER_KIND,
            "interval_ms": interval_ms,
            "start_at": int(reference * 1000) + remaining * 1000,
            "remaining_seconds": remaining,
            "repeat": bool(entry.get("repeat", False)),
            "note": note,
            "enabled": enabled,
        }
    if kind == COMMAND_KIND:
        command = str(entry.get("command") or "")
        if not command.strip():
            raise MigrationError(f"watch {name!r} has no command")
        if command.strip().split(maxsplit=1)[0].startswith("mcp__"):
            raise MigrationError(f"watch {name!r} names an MCP tool, not a shell command")
        interval = _finite_positive(entry.get("interval", 60), name)
        return {
            "id": name,
            "name": name,
            "kind": COMMAND_KIND,
            "command": command,
            "interval_ms": int(interval * 1000),
            "start_at": int(reference * 1000),
            "once": bool(entry.get("once", False)),
            "note": note,
            "enabled": enabled,
        }
    raise MigrationError(f"watch {name!r} has unknown type {kind!r}")


def _finite_positive(value: Any, name: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise MigrationError(f"watch {name!r} has a non-numeric duration") from exc
    if number <= 0:
        raise MigrationError(f"watch {name!r} has a non-positive duration")
    return number


def import_watches(
    core: CoreLike,
    document: str,
    *,
    now: float | None = None,
    dry_run: bool = True,
) -> list[dict[str, Any]]:
    """Translate and forward one legacy export; returns the applied plan."""
    watches = parse_legacy(document, now=now)
    if dry_run:
        return watches
    for watch in watches:
        _apply(core, watch)
    return watches


def _apply(core: CoreLike, watch: dict[str, Any]) -> None:
    if watch["kind"] == TIMER_KIND:
        arguments = {
            "name": watch["name"],
            "seconds": watch["remaining_seconds"],
            "note": watch["note"] or None,
            "repeat": watch["repeat"],
            "enabled": watch["enabled"],
        }
    else:
        arguments = {
            "name": watch["name"],
            "command": watch["command"],
            "interval": watch["interval_ms"] // 1000,
            "note": watch["note"] or None,
            "once": watch["once"],
            "enabled": watch["enabled"],
        }
    arguments = {key: value for key, value in arguments.items() if value is not None}
    core.request(
        "tool",
        {
            "tool": "heartbeat_watch",
            "arguments": arguments,
            "context": {"external": False, "confirmation": None},
        },
    )
