"""Dynamic remote handler registration (external pipelines / agents).

File-based handlers from handlers/*.json stay the source of truth.
Remote clients can POST /api/register to attach an HTTP handler at runtime.

Plugin WebSocket registry (persistent external processes like Claude).
"""
from __future__ import annotations
import asyncio
import time
from typing import Any, Dict, List, Optional, Set

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
_plugin_cleanup_task: Optional[asyncio.Task] = None
_cleanup_interval = 30.0  # seconds
_plugin_timeout = 60.0    # seconds


async def _broadcast_channel_status(channel_id: str, status: str, plugin_id: str = None) -> None:
    """Broadcast channel status change to all SSE reply clients."""
    event = {
        "type": "channel_status",
        "channel_id": channel_id,
        "status": status,
    }
    if plugin_id:
        event["plugin_id"] = plugin_id
    # Reuse existing reply broadcast mechanism
    state.broadcast_reply(event)


def plugin_register(plugin_id: str, ws: Any, capabilities: Dict[str, Any] = None) -> PluginConnection:
    """Register a new plugin WebSocket connection."""
    global _plugins
    conn = PluginConnection(plugin_id, ws, capabilities)
    _plugins[plugin_id] = conn
    _logger.info(f"Plugin registered: {plugin_id}")
    return conn


def plugin_unregister(plugin_id: str) -> bool:
    """Unregister a plugin and all its channels."""
    global _plugins, _channel_to_plugin
    conn = _plugins.pop(plugin_id, None)
    if not conn:
        return False
    for ch_id in list(conn.channels.keys()):
        _channel_to_plugin.pop(ch_id, None)
        asyncio.create_task(_broadcast_channel_status(ch_id, "unavailable", plugin_id))
    _logger.info(f"Plugin unregistered: {plugin_id}")
    return True


def plugin_register_channel(plugin_id: str, channel_id: str, config: Dict[str, Any]) -> bool:
    """Register a channel for a plugin."""
    global _channel_to_plugin
    conn = _plugins.get(plugin_id)
    if not conn:
        return False
    conn.channels[channel_id] = config
    _channel_to_plugin[channel_id] = plugin_id
    conn.last_seen = _now()
    asyncio.create_task(_broadcast_channel_status(channel_id, "healthy", plugin_id))
    _logger.info(f"Channel registered: {channel_id} (plugin={plugin_id})")
    return True


def plugin_unregister_channel(plugin_id: str, channel_id: str) -> bool:
    """Unregister a specific channel."""
    global _channel_to_plugin
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


def list_plugin_channels() -> List[Dict[str, Any]]:
    """List all registered plugin channels with their status."""
    result = []
    for pid, conn in _plugins.items():
        ws_connected = True  # If in dict, it's connected
        for ch_id, cfg in conn.channels.items():
            result.append({
                "channel_id": ch_id,
                "plugin_id": pid,
                "label": cfg.get("label", ch_id),
                "hint": cfg.get("hint", ""),
                "triggers": cfg.get("triggers", []),
                "capabilities": cfg.get("capabilities", []),
                "metadata": cfg.get("metadata", {}),
                "status": "healthy" if ws_connected else "unavailable",
            })
    return result


async def send_to_channel(channel_id: str, payload: Dict[str, Any]) -> bool:
    """Send a message to a plugin channel via WebSocket."""
    plugin_id = _channel_to_plugin.get(channel_id)
    if not plugin_id or plugin_id not in _plugins:
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


async def _cleanup_dead_plugins() -> None:
    """Background task: remove plugins that missed heartbeats."""
    while True:
        await asyncio.sleep(_cleanup_interval)
        now = _now()
        dead = []
        for pid, conn in list(_plugins.items()):
            if now - conn.last_seen > _plugin_timeout:
                dead.append(pid)
        for pid in dead:
            plugin_unregister(pid)


def start_plugin_cleanup_task() -> None:
    """Start the background cleanup task."""
    global _plugin_cleanup_task
    if _plugin_cleanup_task is None or _plugin_cleanup_task.done():
        _plugin_cleanup_task = asyncio.create_task(_cleanup_dead_plugins())
        _logger.info("Plugin cleanup task started")


def stop_plugin_cleanup_task() -> None:
    """Stop the background cleanup task."""
    global _plugin_cleanup_task
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
            "health_path": body.get("health_path", "/health"),
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
