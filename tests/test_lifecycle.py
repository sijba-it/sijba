"""Persistent handler processes: NDJSON transport, restart policy, shutdown."""
from __future__ import annotations
import asyncio
import stat

import core.lifecycle as lifecycle
import core.state as state
from conftest import handler


def script(tmp_path, name: str, body: str) -> str:
    p = tmp_path / name
    p.write_text("#!/usr/bin/env python3\n" + body)
    p.chmod(p.stat().st_mode | stat.S_IEXEC)
    return str(p)


# A persistent plugin: answers NDJSON requests, and emits one unsolicited
# source event at startup.
NDJSON_PLUGIN = """
import json, sys
print(json.dumps({"source": "testsrc", "text": "von der quelle"}), flush=True)
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    req = json.loads(line)
    print(json.dumps({
        "req_id": req["req_id"],
        "continue": True,
        "pipeline_text": req["message"]["pipeline_text"].upper(),
    }), flush=True)
"""


async def test_ndjson_roundtrip(tmp_path):
    h = handler("worker", lifecycle="persistent",
                exec=script(tmp_path, "p.py", NDJSON_PLUGIN))
    ph = lifecycle.PersistentHandler(h, tmp_path)
    state.persistent["worker"] = ph

    assert await ph.start() is True
    try:
        resp = await ph.invoke({"id": "e1", "pipeline_text": "hallo"})
        assert resp["pipeline_text"] == "HALLO"
        assert "req_id" not in resp, "transport detail must not leak to the pipeline"
    finally:
        await ph.stop()


async def test_source_events_reach_the_callback(tmp_path):
    seen = []

    async def on_source(raw):
        seen.append(raw)

    h = handler("src", direction="source", lifecycle="persistent",
                exec=script(tmp_path, "p.py", NDJSON_PLUGIN))
    ph = lifecycle.PersistentHandler(h, tmp_path)
    state.persistent["src"] = ph

    assert await ph.start(source_callback=on_source) is True
    try:
        await asyncio.sleep(0.3)
        assert [s["text"] for s in seen] == ["von der quelle"]
    finally:
        await ph.stop()


async def test_invoke_times_out_without_stalling_the_pipeline(tmp_path):
    silent = "import sys\nfor line in sys.stdin: pass\n"
    h = handler("mute", lifecycle="persistent", timeout_seconds=0.3,
                exec=script(tmp_path, "p.py", silent))
    ph = lifecycle.PersistentHandler(h, tmp_path)
    state.persistent["mute"] = ph
    await ph.start()

    try:
        assert await ph.invoke({"id": "e1", "pipeline_text": "x"}) == {"continue": True}
    finally:
        await ph.stop()


async def test_unhealthy_handler_is_not_invoked(tmp_path):
    h = handler("worker", lifecycle="persistent",
                exec=script(tmp_path, "p.py", NDJSON_PLUGIN))
    ph = lifecycle.PersistentHandler(h, tmp_path)
    state.handler_states["worker"] = "degraded"

    assert await ph.invoke({"id": "e1", "pipeline_text": "x"}) == {"continue": True}


async def test_persistent_http_is_rejected(tmp_path):
    """Remote handlers are per_message (registry.register) — a persistent
    http handler has no transport and must not silently look healthy."""
    h = handler("remote", lifecycle="persistent", type="http",
                startup={"url": "http://localhost:9999"})
    ph = lifecycle.PersistentHandler(h, tmp_path)

    assert await ph.start() is False
    assert state.handler_states["remote"] == "degraded"


async def test_failed_start_marks_degraded_when_not_required(tmp_path):
    h = handler("gone", lifecycle="persistent", exec=str(tmp_path / "nope"))
    ph = lifecycle.PersistentHandler(h, tmp_path)

    assert await ph.start() is False
    assert state.handler_states["gone"] == "degraded"


async def test_failed_start_of_a_required_handler_raises(tmp_path):
    h = handler("critical", lifecycle="persistent", required=True,
                exec=str(tmp_path / "nope"))
    ph = lifecycle.PersistentHandler(h, tmp_path)

    try:
        await ph.start()
        assert False, "a required handler failing to start must abort startup"
    except RuntimeError as exc:
        assert "critical" in str(exc)


async def test_stopped_handler_does_not_restart(tmp_path):
    h = handler("worker", lifecycle="persistent",
                exec=script(tmp_path, "p.py", NDJSON_PLUGIN))
    ph = lifecycle.PersistentHandler(h, tmp_path)
    state.persistent["worker"] = ph
    await ph.start()
    await ph.stop()

    assert state.handler_states["worker"] == "stopped"
    assert await ph.start() is False, "an explicitly stopped handler stays stopped"


async def test_shared_process_reuses_the_owner(tmp_path):
    owner_cfg = handler("owner", direction="source", lifecycle="persistent",
                        exec=script(tmp_path, "p.py", NDJSON_PLUGIN))
    owner = lifecycle.PersistentHandler(owner_cfg, tmp_path)
    state.persistent["owner"] = owner
    await owner.start()

    shared_cfg = handler("sender", direction="outbound", lifecycle="persistent",
                         shared_process="owner")
    shared = lifecycle.PersistentHandler(shared_cfg, tmp_path)
    state.persistent["sender"] = shared

    try:
        assert await shared.start() is True
        assert shared.process is owner.process
        resp = await shared.invoke({"id": "e1", "pipeline_text": "hallo"})
        assert resp["pipeline_text"] == "HALLO", "reply routed to the right handler"
    finally:
        await shared.stop()
        await owner.stop()


async def test_shared_process_without_owner_is_degraded(tmp_path):
    h = handler("sender", lifecycle="persistent", shared_process="missing")
    ph = lifecycle.PersistentHandler(h, tmp_path)

    assert await ph.start() is False
    assert state.handler_states["sender"] == "degraded"


async def test_stopping_a_shared_handler_leaves_the_owner_running(tmp_path):
    owner_cfg = handler("owner", direction="source", lifecycle="persistent",
                        exec=script(tmp_path, "p.py", NDJSON_PLUGIN))
    owner = lifecycle.PersistentHandler(owner_cfg, tmp_path)
    state.persistent["owner"] = owner
    await owner.start()

    shared = lifecycle.PersistentHandler(
        handler("sender", lifecycle="persistent", shared_process="owner"), tmp_path
    )
    state.persistent["sender"] = shared
    await shared.start()

    await shared.stop()
    try:
        assert owner.process.returncode is None, "must not kill a process it does not own"
    finally:
        await owner.stop()
