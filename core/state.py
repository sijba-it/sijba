"""Shared mutable runtime state."""
from __future__ import annotations
from collections import deque
from typing import Any, Deque, Dict, List, Set
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

# Circular log buffer, replayed to each newly connected log viewer
log_buffer: Deque[Dict[str, Any]] = deque(maxlen=1000)

# Active SSE asyncio.Queue instances (one per connected log viewer)
sse_queues: Set[asyncio.Queue] = set()

# Active queues for outbound replies (web/app clients, SSE and WebSocket)
reply_queues: Set[asyncio.Queue] = set()


def configure_log_buffer(maxlen: int) -> None:
    """Resize the log buffer, keeping whatever already fits."""
    global log_buffer
    log_buffer = deque(log_buffer, maxlen=maxlen)


def _fan_out(queues: Set[asyncio.Queue], event: Dict[str, Any]) -> None:
    """Push `event` to every subscriber.

    On a full queue, drop the oldest pending event and keep the subscriber
    rather than discarding its queue. The endpoint generators hold their own
    reference to the same queue object and keep awaiting it regardless, so
    dropping the queue here only stops future events from reaching that client
    while its connection stays open (ping-only) until the browser/app is
    manually reloaded. Dropping the oldest instead means a slow or
    backgrounded client loses stale events but stays live for new ones, with
    no silent black hole.
    """
    for q in queues:
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


def broadcast_reply(event: Dict[str, Any]) -> None:
    """Fan-out a reply event to all connected reply clients."""
    _fan_out(reply_queues, event)


def broadcast_log(entry: Dict[str, Any]) -> None:
    """Buffer a log entry and fan it out to all connected log viewers."""
    log_buffer.append(entry)
    _fan_out(sse_queues, entry)
