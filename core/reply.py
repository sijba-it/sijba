"""Deliver a bot reply to whoever sent the original message.

Two paths produce replies — a handler returning `respond` (pipeline.py) and a
plugin sending {"type": "reply"} over its WebSocket (web/api.py) — and both
must fan out to live web/app clients *and* persist to the transcript. Keeping
that in one place stops the two from drifting apart.
"""
from __future__ import annotations
from typing import Optional

import core.log as log
import core.state as state
import core.transcript as transcript

_logger = log.get("reply")


def deliver(
    *,
    text: str,
    source: str,
    source_meta: Optional[dict],
    channel: str,
    parent_id: Optional[str],
    handler_id: str,
    envelope_id: Optional[str] = None,
    event: str = "reply",
    persist: bool = True,
) -> None:
    """Fan out to connected clients, then store it.

    `persist=False` is for streamed `reply_chunk` deltas — the final `reply`
    carries the authoritative full text into the transcript.
    """
    source_meta = source_meta or {}

    # Web/app clients have no outbound sink of their own; they listen on SSE/WS.
    if source in ("web", "app"):
        state.broadcast_reply({
            "type": event,
            "text": text,
            "parent_id": parent_id,
            "handler_id": handler_id,
            "source_meta": source_meta,
            "channel": channel,
        })

    if not persist:
        return

    # Cross-device replay. Never let a storage hiccup break delivery.
    chat_id = source_meta.get("chat_id")
    if not (chat_id and text):
        return
    try:
        transcript.append(
            chat_id=str(chat_id),
            channel=channel,
            role="bot",
            text=text,
            handler_id=handler_id,
            envelope_id=envelope_id,
            source=source,
        )
    except Exception as exc:
        _logger.warning(f"transcript append (bot) failed: {exc}")
