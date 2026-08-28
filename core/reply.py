"""Deliver a bot reply to whoever sent the original message.

Two paths produce replies — a handler returning `respond` (pipeline.py) and a
plugin sending {"type": "reply"} over its WebSocket (web/api.py) — and both
have to reach the same three places: the outbound pipeline (Telegram,
Pushover, ...), any live web/app client, and the transcript. Keeping that in
one place stops the two from drifting apart, which is how plugin replies ended
up skipping the outbound pipeline entirely.
"""
from __future__ import annotations
from typing import Optional

import core.log as log
import core.state as state
import core.transcript as transcript
from core.envelope import new_envelope

_logger = log.get("reply")

# Injected at startup by main.py to avoid a circular import with msg_queue.
_enqueue_outbound = None


def configure(enqueue_outbound) -> None:
    global _enqueue_outbound
    _enqueue_outbound = enqueue_outbound


def deliver(
    *,
    text: str,
    source: str,
    source_meta: Optional[dict],
    channel: str,
    parent_id: Optional[str],
    handler_id: str,
    envelope_id: Optional[str] = None,
    respond_target: str = "source",
    depth: int = 0,
    event: str = "reply",
    final: bool = True,
) -> None:
    """Send a reply out every way it needs to go.

    `final=False` is for streamed `reply_chunk` deltas: they go to live
    clients only, since the final reply carries the authoritative full text
    into both the transcript and the outbound pipeline.
    """
    source_meta = source_meta or {}

    # Outbound handlers scoped with match.source — this is how a reply gets
    # back out to the channel it came from (Pushover for a voice webhook,
    # Telegram for a Telegram message, ...).
    if final and _enqueue_outbound is not None:
        _enqueue_outbound(new_envelope(
            source=source,
            text=text,
            direction="outbound",
            source_meta=source_meta,
            parent_id=parent_id,
            depth=depth,
            _respond_target=respond_target,
        ))

    # Web/app clients have no outbound sink of their own; they listen on SSE/WS.
    # Webhooks are in the same boat: unlike Telegram, whose bot notifies the
    # user over its own transport, a webhook reply only ever becomes visible
    # in the app — so it has to be pushed live too. Deliberately not widened
    # beyond that; the other sources carry their own return path.
    if source in ("web", "app") or source.startswith("webhook"):
        state.broadcast_reply({
            "type": event,
            "text": text,
            "parent_id": parent_id,
            "handler_id": handler_id,
            "source_meta": source_meta,
            "channel": channel,
        })

    if not final:
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
