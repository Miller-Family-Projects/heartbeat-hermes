"""Protected migration of a synthetic legacy watches export into Core."""

from __future__ import annotations

import json

import fakes
import pytest

from heartbeat_hermes import migrate

REFERENCE = 1_000_000.0

LEGACY = json.dumps(
    {
        "mara-sona-letters": {
            "type": "command",
            "command": "/nix/store/xxx/bin/check-letters",
            "interval": 30,
            "once": False,
            "note": "",
            "enabled": True,
        },
        "backup-reminder": {
            "type": "timer",
            "deadline": REFERENCE + 300,
            "seconds": 600,
            "interval": 60,
            "note": "run the backup",
            "repeat": False,
            "enabled": True,
        },
    }
)


def test_parse_maps_both_legacy_shapes() -> None:
    plan = migrate.parse_legacy(LEGACY, now=REFERENCE)
    command = next(w for w in plan if w["kind"] == "command")
    timer = next(w for w in plan if w["kind"] == "timer")
    assert command["interval_ms"] == 30_000
    assert command["command"].startswith("/nix/store/")
    assert timer["remaining_seconds"] == 300
    assert timer["start_at"] == int(REFERENCE * 1000) + 300_000


def test_stale_timer_fires_once_instead_of_arming_the_past() -> None:
    stale = json.dumps(
        {"old": {"type": "timer", "deadline": REFERENCE - 50, "seconds": 60, "interval": 6,
                 "note": "", "repeat": False, "enabled": True}}
    )
    plan = migrate.parse_legacy(stale, now=REFERENCE)
    assert plan[0]["remaining_seconds"] == 1


def test_malformed_entry_refuses_the_whole_import() -> None:
    bad = json.dumps({"ok": {"type": "command", "command": "true"}, "bad": {"type": "mystery"}})
    with pytest.raises(migrate.MigrationError):
        migrate.parse_legacy(bad, now=REFERENCE)


def test_mcp_tool_names_are_not_commands() -> None:
    bad = json.dumps({"x": {"type": "command", "command": "mcp__search query"}})
    with pytest.raises(migrate.MigrationError):
        migrate.parse_legacy(bad, now=REFERENCE)


def test_non_object_document_refused() -> None:
    with pytest.raises(migrate.MigrationError):
        migrate.parse_legacy("[1,2]", now=REFERENCE)


def test_dry_run_is_the_default_and_sends_nothing() -> None:
    core = fakes.FakeCore()
    plan = migrate.import_watches(core, LEGACY, now=REFERENCE)
    assert len(plan) == 2
    assert core.calls == []


def test_apply_forwards_heartbeat_watch_tool_calls() -> None:
    core = fakes.FakeCore()
    migrate.import_watches(core, LEGACY, now=REFERENCE, dry_run=False)
    tools = [(op, params) for op, params in core.calls if op == "tool"]
    assert len(tools) == 2
    names = {params["tool"] for _, params in tools}
    assert names == {"heartbeat_watch"}
    arguments = [params["arguments"] for _, params in tools]
    assert {a["name"] for a in arguments} == {"mara-sona-letters", "backup-reminder"}
    for _, params in tools:
        assert params["context"] == {"external": False, "confirmation": None}
