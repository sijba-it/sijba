"""POST /api/admin/reload: apply edited config without restarting Core."""
from __future__ import annotations
import json
import stat

import httpx
import pytest

import core.config as config
import core.reload as reload
import core.state as state
import core.web.api as api

PLUGIN = """#!/usr/bin/env python3
import json, sys
for line in sys.stdin:
    req = json.loads(line)
    print(json.dumps({"req_id": req["req_id"], "continue": True}), flush=True)
"""


def write_handler(cfg_dir, name, **h):
    (cfg_dir / "handlers" / name).write_text(json.dumps(h))


@pytest.fixture
def cfg_dir(tmp_path, monkeypatch, db):
    d = tmp_path / "cfg"
    (d / "handlers").mkdir(parents=True)
    (d / "core.yaml").write_text("port: 8080\nwebhooks:\n  voice: tok-voice\n")
    write_handler(d, "99.0-echo.json", id="echo", direction="inbound",
                  type="exec_json", exec="/bin/true")
    monkeypatch.setenv("SIJBA_CONFIG_DIR", str(d))
    monkeypatch.delenv("SIJBA_PORT", raising=False)
    api.configure(auth_token="tok", webhook_token=None, config_dir=d,
                  upload_dir=tmp_path, webhooks={})
    reload.configure(config.load(), source_callback=None)
    return d


@pytest.fixture
async def stop_persistent():
    yield
    for ph in list(state.persistent.values()):
        await ph.stop()


async def test_reload_picks_up_handlers_and_webhooks(cfg_dir):
    res = await reload.reload()
    assert res["handlers"] == ["echo"] and res["webhooks"] == ["voice"]
    assert api._webhooks == {"tok-voice": "voice"}

    write_handler(cfg_dir, "50.0-new.json", id="new", direction="inbound",
                  type="exec_json", exec="/bin/true")
    (cfg_dir / "core.yaml").write_text("port: 8080\nwebhooks:\n  pebble: tok-pebble\n")
    res = await reload.reload()
    assert [h["id"] for h in state.handlers] == ["new", "echo"]
    assert state.handler_states["new"] == "healthy"
    assert api._webhooks == {"tok-pebble": "pebble"}
    assert res["restart_required"] == [] and res["restarted"] == []


async def test_invalid_handler_is_reported_not_loaded(cfg_dir):
    (cfg_dir / "handlers" / "60.0-broken.json").write_text("{nope")
    write_handler(cfg_dir, "61.0-bad.json", id="bad")
    res = await reload.reload()
    assert res["handlers"] == ["echo"]
    assert len(res["errors"]) == 2 and "60.0-broken.json" in res["errors"][0]


async def test_port_change_needs_restart(cfg_dir):
    (cfg_dir / "core.yaml").write_text("port: 9090\n")
    res = await reload.reload()
    assert res["restart_required"] == ["port"]


async def test_persistent_handlers_restart_only_when_changed(cfg_dir, stop_persistent):
    exe = cfg_dir / "p.py"
    exe.write_text(PLUGIN)
    exe.chmod(exe.stat().st_mode | stat.S_IEXEC)
    write_handler(cfg_dir, "20.0-worker.json", id="worker", direction="inbound",
                  type="exec_json", lifecycle="persistent", exec=str(exe))

    res = await reload.reload()
    assert res["restarted"] == ["worker"]
    first = state.persistent["worker"]
    assert state.handler_states["worker"] == "healthy"

    res = await reload.reload()
    assert res["restarted"] == [] and state.persistent["worker"] is first

    write_handler(cfg_dir, "20.0-worker.json", id="worker", direction="inbound",
                  type="exec_json", lifecycle="persistent", exec=str(exe), timeout_seconds=5)
    res = await reload.reload()
    assert res["restarted"] == ["worker"] and state.persistent["worker"] is not first

    (cfg_dir / "handlers" / "20.0-worker.json").unlink()
    res = await reload.reload()
    assert "worker" not in state.persistent and "worker" not in state.handler_states


async def test_endpoint_requires_auth(cfg_dir):
    transport = httpx.ASGITransport(app=api.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        assert (await c.post("/api/admin/reload")).status_code == 401
        r = await c.post("/api/admin/reload", headers={"Authorization": "Bearer tok"})
        assert r.status_code == 200 and r.json()["handlers"] == ["echo"]
