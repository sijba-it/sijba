"""Shared mutable runtime state."""
from __future__ import annotations
from typing import Any, Dict, List, Set
import asyncio

# Sorted list of active handlers (file + dynamically registered)
handlers: List[Dict[str, Any]] = []

# Handlers loaded from handlers/*.json only (before remote merge)
file_handlers: List[Dict[str, Any]] = []

# Dynamically registered remote HTTP handlers: id -> {handler, expires_at, ...}
registered_handlers: Dict[str, Dict[str, Any]] = {}

# handler_id -> state string (pending/starting/healthy/degraded/restarting/stopped)
handler_states: Dict[str, str] = {}

# handler_id -> PersistentHandler instance (lifecycle.py)
persistent: Dict[str, Any] = {}

# Circular log buffer
log_buffer: List[Dict[str, Any]] = []
log_buffer_max: int = 1000

# Active SSE asyncio.Queue instances (one per connected browser)
sse_queues: Set[asyncio.Queue] = set()

# Active SSE queues for outbound replies (web/app clients)
reply_queues: Set[asyncio.Queue] = set()


def broadcast_reply(event: Dict[str, Any]) -> None:
    """Fan-out a reply event to all connected /api/replies SSE clients.

    On a full queue, drop the oldest pending event and keep the client
    subscribed rather than discarding the queue outright — the /api/replies
    generator (web/api.py) holds its own reference to this same queue object
    and keeps awaiting it regardless, so discarding it here only stops future
    broadcasts from reaching that client while its SSE connection stays open
    (ping-only) until the browser/app is manually reloaded. Dropping the
    oldest instead means a slow/backgrounded client loses stale messages but
    stays live for new ones, with no silent black hole.
    """
    for q in reply_queues:
        try:
            q.put_nowait(event)
        except asyncio.QueueFull:
            try:
                q.get_nowait()
            except asyncio.QueueEmpty:
                pass
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                pass
