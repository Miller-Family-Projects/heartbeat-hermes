"""Stdio protocol client against a scripted fake Core binary."""

from __future__ import annotations

from typing import Any

import pytest

from heartbeat_hermes import core_client

FAKE_CORE = r'''#!/usr/bin/env python3
import json
import sys

for line in sys.stdin:
    request = json.loads(line)
    if request["op"] == "echo":
        print(json.dumps({"v": request["v"], "id": request["id"], "ok": True,
                          "result": request["params"]}), flush=True)
    elif request["op"] == "boom":
        print(json.dumps({"v": request["v"], "id": request["id"], "ok": False,
                          "error": {"category": "invalid_request", "retryable": False}}), flush=True)
    elif request["op"] == "garbage":
        print("not-json", flush=True)
    elif request["op"] == "fat":
        print("x" * 100_000, flush=True)
sys.exit(0)
'''


@pytest.fixture
def fake_binary(tmp_path: Any) -> str:
    script = tmp_path / "fake-core"
    script.write_text(FAKE_CORE)
    script.chmod(0o755)
    return str(script)


def test_request_roundtrip(fake_binary: str) -> None:
    child = core_client.CoreChild(fake_binary, "unused-config.json")
    try:
        result = child.request("echo", {"a": 1})
        assert result == {"a": 1}
    finally:
        child.stop()
    assert not child.alive


def test_error_frame_raises_typed_category(fake_binary: str) -> None:
    child = core_client.CoreChild(fake_binary, "unused-config.json")
    try:
        with pytest.raises(core_client.CoreError) as info:
            child.request("boom", {})
        assert info.value.category == "invalid_request"
        assert info.value.retryable is False
    finally:
        child.stop()


def test_malformed_and_oversized_frames_fail_visibly(fake_binary: str) -> None:
    child = core_client.CoreChild(fake_binary, "unused-config.json")
    try:
        with pytest.raises(core_client.CoreUnavailable):
            child.request("garbage", {})
        with pytest.raises(core_client.CoreUnavailable):
            child.request("fat", {})
    finally:
        child.stop()


def test_dead_child_refuses_requests(fake_binary: str) -> None:
    child = core_client.CoreChild(fake_binary, "unused-config.json")
    child.stop()
    with pytest.raises(core_client.CoreUnavailable):
        child.request("echo", {})


def test_oversized_request_refused_before_writing(fake_binary: str) -> None:
    child = core_client.CoreChild(fake_binary, "unused-config.json", frame_bytes_max=64)
    try:
        with pytest.raises(core_client.CoreUnavailable):
            child.request("echo", {"pad": "x" * 10_000})
    finally:
        child.stop()
