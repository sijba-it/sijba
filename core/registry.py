"""Dynamic remote handler registration (external pipelines / agents).

File-based handlers from handlers/*.json stay the source of truth.
Remote clients can POST /api/register to attach an HTTP handler at runtime.
"""
from __future__ import annotations
import time
from typing import Any, Dict, List, Optional

import core.log as log
import core.state as state

_logger = log.get("registry")


def _now() -> float:
    return time.time()


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
