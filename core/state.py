"""Shared mutable runtime state."""
from __future__ import annotations
from typing import Any, Dict, List, Set
import asyncio

# Sorted list of loaded handler dicts
handlers: List[Dict[str, Any]] = []

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
    """Fan-out a reply event to all connected /api/replies SSE clients."""
    dead = []
    for q in reply_queues:
        try:
            q.put_nowait(event)
        except asyncio.QueueFull:
            dead.append(q)
    for q in dead:
        reply_queues.discard(q)
