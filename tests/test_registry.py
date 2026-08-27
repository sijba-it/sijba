"""Plugin WebSocket registry + remote HTTP handler registration."""
from __future__ import annotations

import pytest

import core.registry as registry
import core.state as state
from conftest import FakeWS, handler, install, register_channel


# ------------------------------------------------------------------
# Plugin channels
# ------------------------------------------------------------------

async def test_register_and_list_channel():
    register_channel("cursor", "projekt-a", triggers=["@projekt-a"])

    channels = registry.list_plugin_channels()

    assert len(channels) == 1
    assert channels[0]["channel_id"] == "projekt-a"
    assert channels[0]["plugin_id"] == "cursor"
    assert channels[0]["status"] == "healthy"
    assert channels[0]["triggers"] == ["@projekt-a"]


async def test_unregister_removes_channels():
    register_channel("cursor", "projekt-a")

    assert registry.plugin_unregister("cursor") is True
    assert registry.list_plugin_channels() == []


async def test_reconnect_race_does_not_drop_the_fresh_connection():
    """Regression: a plugin reconnects under the same deterministic id; the
    *old* connection's disconnect handler must not evict the *new* one."""
    old_ws = FakeWS()
    register_channel("cursor-Recherche", "recherche", ws=old_ws)
    new_ws = FakeWS()
    register_channel("cursor-Recherche", "recherche", ws=new_ws)

    # Late disconnect of the stale socket.
    assert registry.plugin_unregister("cursor-Recherche", old_ws) is False
    assert registry.list_plugin_channels(), "fresh connection was wiped out"

    # The current socket may still unregister itself.
    assert registry.plugin_unregister("cursor-Recherche", new_ws) is True
    assert registry.list_plugin_channels() == []


async def test_channel_metadata_patch_merges():
    register_channel("cursor", "projekt-a")
    registry.plugin_update_channel("cursor", "projekt-a", {"model": "sonnet"})
    registry.plugin_update_channel("cursor", "projekt-a", {"context": 1234})

    meta = registry.list_plugin_channels()[0]["metadata"]
    assert meta == {"model": "sonnet", "context": 1234}


async def test_send_to_channel_delivers_payload():
    ws = register_channel("cursor", "projekt-a")

    ok = await registry.send_to_channel("projekt-a", {"envelope_id": "e1", "text": "hi"})

    assert ok is True
    assert ws.sent[0]["type"] == "message"
    assert ws.sent[0]["channel"] == "projekt-a"


async def test_send_to_unknown_channel_reports_failure():
    assert await registry.send_to_channel("nope", {"envelope_id": "e1"}) is False


# ------------------------------------------------------------------
# Pending replies / redelivery
# ------------------------------------------------------------------

async def test_pending_reply_context_roundtrip():
    register_channel("cursor", "projekt-a")
    ctx = {"source": "web", "parent_id": "e1"}

    await registry.send_to_channel("projekt-a", {"envelope_id": "e1"}, ctx)

    assert registry.peek_pending_reply("e1") == ctx
    assert registry.peek_pending_reply("e1") == ctx, "peek must not consume"
    assert registry.pop_pending_reply("e1") == ctx
    assert registry.pop_pending_reply("e1") is None, "pop must consume"


async def test_unanswered_envelope_is_redelivered_on_reconnect():
    """Regression: a WS drop mid-stream must not lose the turn."""
    dead = FakeWS(fail=True)
    register_channel("cursor", "projekt-a", ws=dead)
    await registry.send_to_channel(
        "projekt-a", {"envelope_id": "e1", "text": "hi"}, {"source": "web"}
    )

    fresh = FakeWS()
    register_channel("cursor", "projekt-a", ws=fresh)
    sent = await registry.redeliver_pending_for_channel("projekt-a")

    assert sent == 1
    assert fresh.sent[0]["payload"]["envelope_id"] == "e1"


async def test_answered_envelope_is_not_redelivered():
    register_channel("cursor", "projekt-a")
    await registry.send_to_channel(
        "projekt-a", {"envelope_id": "e1", "text": "hi"}, {"source": "web"}
    )
    registry.pop_pending_reply("e1")  # plugin answered

    assert await registry.redeliver_pending_for_channel("projekt-a") == 0


async def test_redelivery_gives_up_after_max_attempts():
    register_channel("cursor", "projekt-a")
    await registry.send_to_channel(
        "projekt-a", {"envelope_id": "e1", "text": "hi"}, {"source": "web"}
    )

    for _ in range(registry._pending_max_redeliveries):
        await registry.redeliver_pending_for_channel("projekt-a")

    assert await registry.redeliver_pending_for_channel("projekt-a") == 0
    assert registry.peek_pending_reply("e1") is None, "must be dropped, not looped"


async def test_expired_pending_is_dropped(monkeypatch):
    register_channel("cursor", "projekt-a")
    await registry.send_to_channel(
        "projekt-a", {"envelope_id": "e1", "text": "hi"}, {"source": "web"}
    )
    registry._pending_replies["e1"]["sent_at"] -= registry._pending_ttl_seconds + 1

    assert await registry.redeliver_pending_for_channel("projekt-a") == 0
    assert registry.peek_pending_reply("e1") is None


async def test_retry_of_same_envelope_keeps_redelivery_count():
    register_channel("cursor", "projekt-a")
    payload = {"envelope_id": "e1", "text": "hi"}
    await registry.send_to_channel("projekt-a", payload, {"source": "web"})
    await registry.redeliver_pending_for_channel("projekt-a")

    await registry.send_to_channel("projekt-a", payload, {"source": "web"})

    assert registry._pending_replies["e1"]["redeliveries"] == 1


# ------------------------------------------------------------------
# Remote HTTP handler registration (used by sijba-kubectl)
# ------------------------------------------------------------------

def test_register_remote_handler_appears_in_handlers():
    result = registry.register({"id": "pods", "url": "http://kubectl:9000"})

    assert result["status"] == "registered"
    assert [h["id"] for h in state.handlers] == ["pods"]
    assert state.handler_states["pods"] == "healthy"
    h = state.handlers[0]
    assert h["type"] == "http"
    assert h["startup"]["url"] == "http://kubectl:9000"
    assert h["_dynamic"] is True


def test_register_defaults_match_regex_to_the_id():
    registry.register({"id": "pods", "url": "http://x"})
    assert state.handlers[0]["match"] == {"regex": r"(?i)^pods\b"}


def test_register_requires_id_and_url():
    with pytest.raises(ValueError, match="id required"):
        registry.register({"url": "http://x"})
    with pytest.raises(ValueError, match="url required"):
        registry.register({"id": "pods"})


def test_register_cannot_shadow_a_file_handler():
    install(handler("pods"))

    with pytest.raises(ValueError, match="defined in config files"):
        registry.register({"id": "pods", "url": "http://x"})


def test_unregister_removes_the_handler():
    registry.register({"id": "pods", "url": "http://x"})

    assert registry.unregister("pods") is True
    assert state.handlers == []
    assert registry.unregister("pods") is False


def test_heartbeat_extends_the_ttl():
    registry.register({"id": "pods", "url": "http://x", "ttl_seconds": 60})
    before = state.registered_handlers["pods"]["expires_at"]

    assert registry.heartbeat("pods", 600) is True
    assert state.registered_handlers["pods"]["expires_at"] > before
    assert registry.heartbeat("unknown") is False


def test_expire_stale_removes_lapsed_registrations():
    registry.register({"id": "pods", "url": "http://x", "ttl_seconds": 60})
    state.registered_handlers["pods"]["expires_at"] = 0  # long past

    assert registry.expire_stale() == 1
    assert state.handlers == []
    assert "pods" not in state.handler_states


def test_expire_stale_keeps_ttl_less_registrations():
    registry.register({"id": "pods", "url": "http://x"})

    assert registry.expire_stale() == 0
    assert [h["id"] for h in state.handlers] == ["pods"]


def test_rebuild_sorts_by_priority():
    install(handler("echo", priority=99.0), handler("audit", priority=0.0))
    registry.register({"id": "pods", "url": "http://x", "priority": 30.0})

    assert [h["id"] for h in state.handlers] == ["audit", "pods", "echo"]
