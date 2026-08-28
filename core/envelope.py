"""The message envelope — the one shape that travels through the pipeline.

Every entry point (source plugins, /api/message, /webhook, handler `respond`
and `inject`) builds its messages here so the field set stays in one place.
"""
from __future__ import annotations
import uuid
from typing import Any, Dict, Optional

# Sources that carry their own live return path (web/app SSE/WS) instead of
# going out via an outbound handler. Shared by pipeline + queue so msg_queue
# can surface delivery failures without importing pipeline.
_SELF_DELIVERING_SOURCES = ("web", "app", "internal")


def expects_outbound_delivery(envelope: dict) -> bool:
    """True when a missing outbound handler is a real delivery failure."""
    target = envelope.get("_respond_target", "source")
    if target and target != "source":
        return True
    return envelope.get("source", "") not in _SELF_DELIVERING_SOURCES


def new_envelope(
    *,
    source: str,
    text: str,
    direction: str = "inbound",
    source_meta: Optional[dict] = None,
    metadata: Optional[dict] = None,
    parent_id: Optional[str] = None,
    depth: int = 0,
    **extra: Any,
) -> Dict[str, Any]:
    """Build an envelope. `extra` carries internal routing keys (`_target`,
    `_respond_target`) that only the core sets and reads."""
    env: Dict[str, Any] = {
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
    env.update(extra)
    return env
