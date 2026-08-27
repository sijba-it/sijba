"""Dynamic remote handler registration (external pipelines / agents).

File-based handlers from handlers/*.json stay the source of truth.
Remote clients can POST /api/register to attach an HTTP handler at runtime.

Plugin WebSocket registry (persistent external processes like Claude).
"""
from __future__ import annotations
import asyncio
import time
from typing import Any, Dict, List, Optional

import core.log as log
import core.state as state

_logger = log.get("registry")


def _now() -> float:
    return time.time()


# ------------------------------------------------------------------
# Plugin WebSocket Registry
# ------------------------------------------------------------------

class PluginConnection:
    """Active WebSocket connection from an external plugin."""
    __slots__ = ("plugin_id", "ws", "channels", "last_seen", "capabilities")

    def __init__(self, plugin_id: str, ws: Any, capabilities: Dict[str, Any] = None):
        self.plugin_id = plugin_id
        self.ws = ws
        self.channels: Dict[str, Dict[str, Any]] = {}
        self.last_seen = _now()
        self.capabilities = capabilities or {}


_plugins: Dict[str, PluginConnection] = {}
_channel_to_plugin: Dict[str, str] = {}
# envelope_id -> {
#   reply_context, channel_id, payload, sent_at, redeliveries
# }
# Kept until the plugin's final {"type":"reply"} so a reconnect can
# redeliver turns that never got an answer (WS drop mid-stream).
_pending_replies: Dict[str, Dict[str, Any]] = {}
_plugin_cleanup_task: Optional[asyncio.Task] = None
_cleanup_interval = 30.0  # seconds
_plugin_timeout = 60.0    # seconds
_pending_max_redeliveries = 3
_pending_ttl_seconds = 3600.0


async def _broadcast_channel_status(
    channel_id: str, status: str, plugin_id: str = None, metadata: Dict[str, Any] = None
) -> None:
    """Broadcast channel status change to all SSE reply clients."""
    event = {
        "type": "channel_status",
        "channel_id": channel_id,
        "status": status,
    }
    if plugin_id:
        event["plugin_id"] = plugin_id
    if metadata is not None:
        event["metadata"] = metadata
    # Reuse existing reply broadcast mechanism
    state.broadcast_reply(event)


def plugin_register(plugin_id: str, ws: Any, capabilities: Dict[str, Any] = None) -> PluginConnection:
    """Register a new plugin WebSocket connection."""
    conn = PluginConnection(plugin_id, ws, capabilities)
    _plugins[plugin_id] = conn
    _logger.info(f"Plugin registered: {plugin_id}")
    return conn


def plugin_unregister(plugin_id: str, ws: Any = None) -> bool:
    """Unregister a plugin and all its channels.

    `ws` (when given) must match the currently-registered connection's own
    websocket, or this is a no-op. Without it: a reconnecting plugin uses
    the same deterministic plugin_id (e.g. "cursor-Recherche") every time,
    so the *old* connection's disconnect handler racing after the *new*
    one has already re-registered would otherwise pop the new entry out
    from under it by id alone — verified live, a burst of reconnects (a
    controller restart cycling all its children) silently dropped an
    otherwise-healthy, freshly (re-)registered channel from the registry.
    """
    conn = _plugins.get(plugin_id)
    if not conn:
        return False
    if ws is not None and conn.ws is not ws:
        return False
    del _plugins[plugin_id]
    for ch_id in list(conn.channels.keys()):
        _channel_to_plugin.pop(ch_id, None)
        asyncio.create_task(_broadcast_channel_status(ch_id, "unavailable", plugin_id))
    _logger.info(f"Plugin unregistered: {plugin_id}")
    return True


def plugin_register_channel(plugin_id: str, channel_id: str, config: Dict[str, Any]) -> bool:
    """Register a channel for a plugin."""
    conn = _plugins.get(plugin_id)
    if not conn:
        return False
    conn.channels[channel_id] = config
    _channel_to_plugin[channel_id] = plugin_id
    conn.last_seen = _now()
    asyncio.create_task(_broadcast_channel_status(channel_id, "healthy", plugin_id))
    _logger.info(f"Channel registered: {channel_id} (plugin={plugin_id})")
    # Re-send any unanswered turns for this channel (WS drop mid-stream,
    # or messages that arrived while the plugin was offline).
    asyncio.create_task(redeliver_pending_for_channel(channel_id))
    return True


def plugin_update_channel(plugin_id: str, channel_id: str, metadata_patch: Dict[str, Any]) -> bool:
    """Merge a metadata patch into a registered channel (e.g. current model,
    context size after a turn) and notify listeners — without a full
    re-registration."""
    conn = _plugins.get(plugin_id)
    if not conn or channel_id not in conn.channels:
        return False
    cfg = conn.channels[channel_id]
    cfg["metadata"] = {**cfg.get("metadata", {}), **metadata_patch}
    conn.last_seen = _now()
    asyncio.create_task(_broadcast_channel_status(channel_id, "healthy", plugin_id, cfg["metadata"]))
    return True


def plugin_unregister_channel(plugin_id: str, channel_id: str) -> bool:
    """Unregister a specific channel."""
    conn = _plugins.get(plugin_id)
    if not conn:
        return False
    if channel_id not in conn.channels:
        return False
    del conn.channels[channel_id]
    _channel_to_plugin.pop(channel_id, None)
    asyncio.create_task(_broadcast_channel_status(channel_id, "unavailable", plugin_id))
    return True


def plugin_heartbeat(plugin_id: str) -> bool:
    """Update last_seen timestamp for a plugin."""
    conn = _plugins.get(plugin_id)
    if not conn:
        return False
    conn.last_seen = _now()
    return True


def channel_triggers(cfg: Dict[str, Any], channel_id: str = "") -> List[str]:
    """The @-mentions a channel answers to, in registration order.

    Routing out of plain message text requires an explicit @mention. A bare
    project name would otherwise hijack any sentence starting with that word —
    "Recherche zu Thema X" is a message *about* research, not one addressed to
    the Recherche agent.

    Triggers registered without an @ are normalised rather than ignored, so
    plugins do not have to be redeployed in lockstep with core.
    """
    out: List[str] = []
    for raw in cfg.get("triggers") or []:
        trigger = str(raw).strip()
        if not trigger:
            continue
        mention = trigger if trigger.startswith("@") else f"@{trigger}"
        if mention not in out:
            out.append(mention)
    if not out and channel_id:
        out.append(f"@{channel_id}")  # always addressable by its own id
    return out


def list_plugin_channels() -> List[Dict[str, Any]]:
    """List all registered plugin channels. Being in `_plugins` *is* being
    connected — a dropped socket is unregistered, not left in place."""
    return [
        {
            "channel_id": ch_id,
            "plugin_id": pid,
            "label": cfg.get("label", ch_id),
            "hint": cfg.get("hint", ""),
            "triggers": channel_triggers(cfg, ch_id),
            "capabilities": cfg.get("capabilities", []),
            "metadata": cfg.get("metadata", {}),
            "status": "healthy",
        }
        for pid, conn in _plugins.items()
        for ch_id, cfg in conn.channels.items()
    ]


def channel_routing_targets() -> List[Dict[str, Any]]:
    """Plugin channels described the way a router has to address them.

    `id` is the channel's @mention, not its channel_id, because that is what a
    router must emit: the reply is re-injected as a normal message and picked
    up by the trigger match in pipeline.process_inbound, which only accepts
    @mentions.
    """
    targets = []
    for conn in _plugins.values():
        for ch_id, cfg in conn.channels.items():
            mentions = channel_triggers(cfg, ch_id)
            targets.append({
                "id": mentions[0],
                "router_hint": cfg.get("hint") or cfg.get("label") or ch_id,
            })
    return targets


def _stash_pending(
    channel_id: str,
    payload: Dict[str, Any],
    reply_context: Dict[str, Any],
) -> None:
    eid = payload.get("envelope_id")
    if not eid:
        return
    # Don't reset redelivery count if we're refreshing an existing entry
    # (e.g. pipeline retried the same envelope).
    existing = _pending_replies.get(eid)
    _pending_replies[eid] = {
        "reply_context": reply_context,
        "channel_id": channel_id,
        "payload": payload,
        "sent_at": (existing or {}).get("sent_at", _now()),
        "redeliveries": (existing or {}).get("redeliveries", 0),
    }


async def send_to_channel(
    channel_id: str,
    payload: Dict[str, Any],
    reply_context: Optional[Dict[str, Any]] = None,
) -> bool:
    """Send a message to a plugin channel via WebSocket.

    `reply_context` (source/source_meta/transcript channel/parent_id) is
    stashed keyed by payload["envelope_id"] until the plugin's final reply,
    so a reconnect can redeliver turns that never completed.
    """
    if reply_context is not None:
        _stash_pending(channel_id, payload, reply_context)

    plugin_id = _channel_to_plugin.get(channel_id)
    if not plugin_id or plugin_id not in _plugins:
        _logger.warning(
            f"Channel {channel_id} offline — envelope="
            f"{payload.get('envelope_id')} queued for redelivery"
        )
        return False
    conn = _plugins[plugin_id]
    try:
        await conn.ws.send_json({
            "type": "message",
            "channel": channel_id,
            "payload": payload,
        })
        conn.last_seen = _now()
        return True
    except Exception as exc:
        _logger.warning(f"Failed to send to channel {channel_id}: {exc}")
        return False


async def redeliver_pending_for_channel(channel_id: str) -> int:
    """Re-send unanswered envelopes for `channel_id` (after reconnect)."""
    plugin_id = _channel_to_plugin.get(channel_id)
    if not plugin_id or plugin_id not in _plugins:
        return 0
    conn = _plugins[plugin_id]
    now = _now()
    sent = 0
    for eid, entry in list(_pending_replies.items()):
        if entry["channel_id"] != channel_id:
            continue
        age = now - float(entry.get("sent_at") or now)
        if age > _pending_ttl_seconds:
            _logger.warning(f"Dropping expired pending envelope={eid} ({age:.0f}s old)")
            _pending_replies.pop(eid, None)
            continue
        attempts = int(entry.get("redeliveries") or 0)
        if attempts >= _pending_max_redeliveries:
            _logger.warning(
                f"Dropping pending envelope={eid} after {attempts} redeliveries"
            )
            _pending_replies.pop(eid, None)
            continue
        try:
            await conn.ws.send_json({
                "type": "message",
                "channel": channel_id,
                "payload": entry["payload"],
            })
            entry["redeliveries"] = attempts + 1
            sent += 1
            _logger.info(
                f"Redelivered envelope={eid} to {channel_id} "
                f"(attempt {entry['redeliveries']})"
            )
        except Exception as exc:
            _logger.warning(f"Redeliver envelope={eid} to {channel_id} failed: {exc}")
            break
    return sent


def pop_pending_reply(envelope_id: str) -> Optional[Dict[str, Any]]:
    """Retrieve and clear the reply context stashed by send_to_channel().
    Clearing it is what stops the envelope being redelivered."""
    entry = _pending_replies.pop(envelope_id, None)
    return entry["reply_context"] if entry else None


def peek_pending_reply(envelope_id: str) -> Optional[Dict[str, Any]]:
    """Like pop_pending_reply() but leaves it in place — for streamed
    reply_chunk messages, where more chunks (and the final reply) are
    still to come for the same envelope_id."""
    entry = _pending_replies.get(envelope_id)
    return entry["reply_context"] if entry else None


def discard_pending(envelope_id: str) -> bool:
    """Give up on delivering this envelope to its channel.

    A pending entry means "this channel still owes an answer". When the
    caller reacts to a failed send by handling the message some other way
    (the pipeline fallback), the channel no longer owes anything — leaving
    the entry would have the plugin redeliver and answer it a second time
    after it reconnects.
    """
    return _pending_replies.pop(envelope_id, None) is not None


async def _cleanup_dead_plugins() -> None:
    """Background task: remove plugins that missed heartbeats; expire stale pendings."""
    while True:
        await asyncio.sleep(_cleanup_interval)
        now = _now()
        dead = []
        for pid, conn in list(_plugins.items()):
            if now - conn.last_seen > _plugin_timeout:
                dead.append((pid, conn.ws))
        for pid, ws in dead:
            plugin_unregister(pid, ws)
        # Drop unanswered envelopes that will never be redelivered usefully.
        for eid, entry in list(_pending_replies.items()):
            sent_at = float(entry.get("sent_at") or now)
            if now - sent_at > _pending_ttl_seconds:
                _pending_replies.pop(eid, None)
                _logger.warning(f"Expired pending envelope={eid}")


def start_plugin_cleanup_task() -> None:
    """Start the background cleanup task."""
    global _plugin_cleanup_task
    if _plugin_cleanup_task is None or _plugin_cleanup_task.done():
        _plugin_cleanup_task = asyncio.create_task(_cleanup_dead_plugins())
        _logger.info("Plugin cleanup task started")


def stop_plugin_cleanup_task() -> None:
    """Stop the background cleanup task."""
    if _plugin_cleanup_task and not _plugin_cleanup_task.done():
        _plugin_cleanup_task.cancel()
        _logger.info("Plugin cleanup task stopped")


def rebuild() -> None:
    """Merge file handlers + registered remotes into state.handlers (sorted)."""
    file_ids = {h["id"] for h in state.file_handlers}
    merged: List[Dict[str, Any]] = list(state.file_handlers)

    for hid, entry in list(state.registered_handlers.items()):
        handler = entry["handler"]
        if hid in file_ids:
            _logger.warning(f"Registered handler '{hid}' conflicts with file handler — ignoring remote")
            continue
        # Drop expired
        expires = entry.get("expires_at")
        if expires is not None and expires < _now():
            _logger.info(f"Registered handler '{hid}' expired — removing")
            del state.registered_handlers[hid]
            state.handler_states.pop(hid, None)
            continue
        merged.append(handler)
        if hid not in state.handler_states:
            state.handler_states[hid] = "healthy"

    # Drop states for remotes that disappeared
    live_ids = {h["id"] for h in merged}
    for hid in list(state.handler_states.keys()):
        if hid not in live_ids and hid in state.registered_handlers:
            del state.handler_states[hid]

    merged.sort(key=lambda h: h.get("priority", 50.0))
    state.handlers = merged


def register(body: dict) -> dict:
    """Validate and register (or refresh) a remote HTTP handler."""
    hid = (body.get("id") or "").strip()
    if not hid:
        raise ValueError("id required")

    url = (body.get("url") or "").strip()
    if not url:
        raise ValueError("url required (advertise URL Core can reach)")

    # Conflict with on-disk handlers
    if any(h["id"] == hid for h in state.file_handlers):
        raise ValueError(f"handler id '{hid}' is defined in config files — cannot override")

    priority = float(body.get("priority", 30.0))
    match = body.get("match") or {"regex": f"(?i)^{hid}\\b"}
    ttl = body.get("ttl_seconds")
    ttl_f = float(ttl) if ttl is not None else None

    handler: Dict[str, Any] = {
        "id": hid,
        "priority": priority,
        "direction": body.get("direction", "inbound"),
        "lifecycle": "per_message",
        "type": "http",
        "category": body.get("category") or [],
        "required": False,
        "await_response": True,
        "allow_direct_inject": True,
        "continue_on_match": body.get("continue_on_match", False),
        "match": match,
        "router_hint": body.get("router_hint") or f"Remote handler '{hid}'",
        "timeout_seconds": float(body.get("timeout_seconds", 30)),
        "startup": {
            "url": url,
            "invoke_path": body.get("invoke_path", "/invoke"),
        },
        "_dynamic": True,
    }

    expires_at = (_now() + ttl_f) if ttl_f else None
    state.registered_handlers[hid] = {
        "handler": handler,
        "registered_at": _now(),
        "expires_at": expires_at,
        "ttl_seconds": ttl_f,
    }
    state.handler_states[hid] = "healthy"
    rebuild()
    _logger.info(f"Registered remote handler '{hid}' → {url} (ttl={ttl_f})")
    return {
        "id": hid,
        "status": "registered",
        "url": url,
        "ttl_seconds": ttl_f,
        "expires_at": expires_at,
    }


def unregister(hid: str) -> bool:
    if hid not in state.registered_handlers:
        return False
    del state.registered_handlers[hid]
    state.handler_states.pop(hid, None)
    rebuild()
    _logger.info(f"Unregistered remote handler '{hid}'")
    return True


def heartbeat(hid: str, ttl_seconds: Optional[float] = None) -> bool:
    entry = state.registered_handlers.get(hid)
    if not entry:
        return False
    ttl = ttl_seconds if ttl_seconds is not None else entry.get("ttl_seconds")
    if ttl is not None:
        entry["expires_at"] = _now() + float(ttl)
        entry["ttl_seconds"] = float(ttl)
    else:
        entry["expires_at"] = None
    entry["registered_at"] = _now()
    return True


def expire_stale() -> int:
    """Remove expired registrations. Returns count removed."""
    removed = 0
    now = _now()
    for hid, entry in list(state.registered_handlers.items()):
        exp = entry.get("expires_at")
        if exp is not None and exp < now:
            del state.registered_handlers[hid]
            state.handler_states.pop(hid, None)
            removed += 1
            _logger.info(f"Expired remote handler '{hid}'")
    if removed:
        rebuild()
    return removed
