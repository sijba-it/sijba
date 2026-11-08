"""The message envelope — the one shape that travels through the pipeline.

Every entry point (source plugins, /api/message, /webhook, handler `respond`
and `inject`) builds its messages here so the field set stays in one place.

An envelope stays a plain dict on purpose: it is serialized as-is into the
SQLite queue and handed to plugins as JSON. `Envelope` only spells out which
keys exist, so a type checker catches a misspelled one.
"""
from __future__ import annotations
import uuid
from typing import Any, Literal, NotRequired, Optional, TypedDict


class Envelope(TypedDict):
    # Wire fields — what every plugin receives.
    id: str
    parent_id: Optional[str]
    direction: Literal["inbound", "outbound"]
    source: str
    source_meta: dict[str, Any]
    text: str
    pipeline_text: str  # `text` as rewritten by handlers along the way
    depth: int  # inject nesting, capped by max_inject_depth
    metadata: dict[str, Any]

    # Added by plugin_runner just before a handler sees it.
    context: NotRequired[dict[str, Any]]
    handlers: NotRequired[list[dict[str, Any]]]
    handlers_visited: NotRequired[list[str]]

    # Core-internal routing. Only core sets and reads these; plugins still
    # see them on the wire but must not rely on them.
    _target: NotRequired[str]  # direct target (handler id, category or plugin channel)
    _respond_target: NotRequired[str]  # outbound: "source", handler id or category
    _origin_source: NotRequired[str]  # external source an `internal` inject descends from
    _origin_source_meta: NotRequired[dict[str, Any]]
    _transcript_channel: NotRequired[str]
    _activity_broadcast: NotRequired[bool]  # push the inbound message live to web/app clients


# Sources that carry their own live return path (web/app SSE/WS) instead of
# going out via an outbound handler. Shared by pipeline + queue so msg_queue
# can surface delivery failures without importing pipeline.
_SELF_DELIVERING_SOURCES = ("web", "app", "internal")


def expects_outbound_delivery(envelope: Envelope) -> bool:
    """True when a missing outbound handler is a real delivery failure."""
    target = envelope.get("_respond_target", "source")
    if target and target != "source":
        return True
    return envelope.get("source", "") not in _SELF_DELIVERING_SOURCES


def new_envelope(
    *,
    source: str,
    text: str,
    direction: Literal["inbound", "outbound"] = "inbound",
    source_meta: Optional[dict[str, Any]] = None,
    metadata: Optional[dict[str, Any]] = None,
    parent_id: Optional[str] = None,
    depth: int = 0,
    target: Optional[str] = None,
    respond_target: Optional[str] = None,
    origin_source: Optional[str] = None,
    origin_source_meta: Optional[dict[str, Any]] = None,
    activity_broadcast: bool = False,
) -> Envelope:
    """Build an envelope. The internal routing keys are only set when given,
    so plugins see the same wire shape as before."""
    env: Envelope = {
        "id": str(uuid.uuid4()),
        "parent_id": parent_id,
        "direction": direction,
        "source": source,
        "source_meta": source_meta or {},
        "text": text,
        "pipeline_text": text,
        "depth": depth,
        "metadata": metadata or {},
    }
    if target:
        env["_target"] = target
    if respond_target is not None:
        env["_respond_target"] = respond_target
    if origin_source is not None:
        env["_origin_source"] = origin_source
    if origin_source_meta is not None:
        env["_origin_source_meta"] = origin_source_meta
    if activity_broadcast:
        env["_activity_broadcast"] = True
    return env
