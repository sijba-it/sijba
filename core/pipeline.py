"""Inbound and outbound pipeline execution."""
from __future__ import annotations
import asyncio
import re
from pathlib import Path
from typing import Optional

import core.log as log
import core.msg_queue as msg_queue
import core.state as state
import core.plugin_runner as plugin_runner
import core.registry as registry
import core.reply as reply
import core.transcript as transcript
from core.envelope import expects_outbound_delivery, new_envelope

_logger = log.get("pipeline")

# Injected at startup by main.py to avoid circular imports
_enqueue_inbound = None
_enqueue_outbound = None
_config_dir: Optional[Path] = None
_max_inject_depth: int = 5


def configure(enqueue_inbound, enqueue_outbound, config_dir: Path, max_inject_depth: int) -> None:
    global _enqueue_inbound, _enqueue_outbound, _config_dir, _max_inject_depth
    _enqueue_inbound = enqueue_inbound
    _enqueue_outbound = enqueue_outbound
    _config_dir = config_dir
    _max_inject_depth = max_inject_depth


# ------------------------------------------------------------------
# Match logic
# ------------------------------------------------------------------

def _matches(handler: dict, envelope: dict) -> bool:
    match = handler.get("match") or {}
    if not match:
        return True

    allowed_sources = match.get("source")
    if allowed_sources and envelope.get("source") not in allowed_sources:
        return False

    regex = match.get("regex")
    if regex:
        text = envelope.get("pipeline_text") or envelope.get("text", "")
        if not re.search(regex, text):
            return False

    return True


# ------------------------------------------------------------------
# Inject target resolution
# ------------------------------------------------------------------

def _resolve_inject_target(target: Optional[str], *, require_allow_direct: bool = True):
    """Resolve a handler id or category to a handler list.

    Returns:
      None  — invalid / not allowed (caller should discard)
      []    — no target (full pipeline)
      [...] — run only these handlers
    """
    if not target:
        return []

    for h in state.handlers:
        if h["id"] == target:
            if require_allow_direct and not h.get("allow_direct_inject", False):
                _logger.warning(
                    f"Inject target '{target}' found but allow_direct_inject=false — discarding"
                )
                return None
            return [h]

    matched = [h for h in state.handlers if target in h.get("category", [])]
    if matched:
        return matched

    _logger.error(f"Inject target '{target}' not found — discarding")
    return None


# ------------------------------------------------------------------
# Core pipeline runner
# ------------------------------------------------------------------

def _outbound_handlers_for(envelope: dict) -> list:
    """Filter outbound handlers based on respond_target.

    - target == "source"  → handlers whose match.source includes envelope.source
                            (or handlers with no source filter)
    - target == handler ID → only that handler
    - target == category  → handlers with that category
    - no target           → all outbound handlers (normal full pipeline)
    """
    all_out = [h for h in state.handlers if h.get("direction") == "outbound"]
    target = envelope.get("_respond_target", "source")

    if not target or target == "source":
        src = envelope.get("source")
        if not src:
            return all_out
        result = []
        for h in all_out:
            allowed = (h.get("match") or {}).get("source")
            if allowed is None or src in allowed:
                result.append(h)
        return result

    # Exact handler ID
    by_id = [h for h in all_out if h["id"] == target]
    if by_id:
        return by_id

    # Category
    by_cat = [h for h in all_out if target in h.get("category", [])]
    if by_cat:
        return by_cat

    _logger.warning(f"respond target '{target}' not found in outbound handlers")
    return []


async def _run(
    direction: str,
    envelope: dict,
    handler_override: list = None,
    *,
    skip_match: bool = False,
) -> int:
    """Run the matching handlers. Returns how many of them actually took the
    message — which is how process_outbound tells "nobody delivered this"
    apart from "delivered fine"."""
    delivered = 0
    if handler_override is not None:
        handlers = handler_override
    elif direction == "outbound":
        handlers = _outbound_handlers_for(envelope)
    else:
        handlers = [h for h in state.handlers if h.get("direction") == direction]

    for handler in handlers:
        hid = handler["id"]

        if state.handler_states.get(hid, "healthy") != "healthy":
            continue

        # Direct API/plugin targets skip match so the user can talk to a part
        # without needing its trigger-word prefix.
        if not skip_match and not _matches(handler, envelope):
            continue

        # Fire-and-forget
        if not handler.get("await_response", True):
            asyncio.create_task(
                plugin_runner.invoke(handler, envelope, _config_dir)
            )
            delivered += 1  # handed off; there is no result to judge it by
            continue

        response = await plugin_runner.invoke(handler, envelope, _config_dir)

        if response.get("_error"):
            # invoke() never raises — a dead remote handler looks like an
            # ordinary empty response unless we check for this.
            _logger.warning(f"[{hid}] invoke failed: {response['_error']}")
        else:
            delivered += 1

        # pipeline_text update
        if "pipeline_text" in response:
            envelope["pipeline_text"] = response["pipeline_text"]

        # respond → outbound queue
        if "respond" in response and response["respond"]:
            resp = response["respond"]
            respond_text = resp.get("text", "")
            respond_target = resp.get("target", "source")
            src = envelope.get("source", "internal")
            _logger.info(
                f"respond [{src}→{respond_target}]: {respond_text[:120]!r}"
            )
            reply.deliver(
                text=respond_text,
                source=src,
                source_meta=envelope.get("source_meta"),
                channel=envelope.get("_transcript_channel") or "pipeline",
                parent_id=envelope["id"],
                handler_id=hid,
                envelope_id=envelope.get("id"),
                respond_target=respond_target,
                depth=envelope.get("depth", 0),
            )

        # inject → new inbound message
        if "inject" in response and response["inject"]:
            inj = response["inject"]
            depth = envelope.get("depth", 0) + 1
            if depth > _max_inject_depth:
                _logger.warning(
                    f"[{hid}] inject depth {depth} exceeds max_inject_depth={_max_inject_depth} — discarding"
                )
            else:
                child_env = new_envelope(
                    source="internal",
                    text=inj.get("text", ""),
                    parent_id=envelope["id"],
                    depth=depth,
                    metadata=inj.get("metadata") or {},
                    # Injected messages run as `internal` so match.source can't
                    # loop them back through source-scoped handlers — but the
                    # real external source is remembered so a reply that
                    # eventually comes back from a plugin channel still reaches
                    # that source's outbound handler (e.g. Pushover for voice).
                    _origin_source=envelope.get("_origin_source") or envelope.get("source"),
                    _origin_source_meta=(
                        envelope.get("_origin_source_meta")
                        or envelope.get("source_meta") or {}
                    ),
                )
                inj_target = inj.get("target")
                target_handlers = _resolve_inject_target(inj_target)
                if target_handlers is None:
                    pass
                elif target_handlers:
                    asyncio.create_task(
                        _run("inbound", child_env, target_handlers, skip_match=True)
                    )
                else:
                    _enqueue_inbound(child_env)

        default_continue = handler.get("continue_on_match", True)
        if not response.get("continue", default_continue):
            _logger.info(f"[{hid}] stopped pipeline")
            break

    return delivered


# ------------------------------------------------------------------
# Public entry points
# ------------------------------------------------------------------

async def _route_to_plugin_channel(ch: dict, envelope: dict) -> bool:
    """Send `envelope` to a registered plugin channel and stash reply context
    so the eventual {"type": "reply"} from the plugin can be routed back to
    the originating web/app client and the right transcript channel."""
    pipeline_text = envelope.get("pipeline_text") or envelope.get("text", "")
    payload = {
        "envelope_id": envelope["id"],
        "text": pipeline_text,
        "source": envelope.get("source"),
        "source_meta": envelope.get("source_meta", {}),
        "context": envelope.get("context", {}),
        "channel_metadata": ch.get("metadata", {}),
        "attachments": (envelope.get("metadata") or {}).get("attachments", []),
    }
    # Thread the whole exchange under the channel's own id so it reads like a
    # conversation you had directly with the channel — whatever routed it here.
    channel_id = ch["channel_id"]
    reply_meta = dict(
        envelope.get("_origin_source_meta") or envelope.get("source_meta") or {}
    )
    reply_meta.setdefault("chat_id", channel_id)
    reply_context = {
        # For an injected message `source` is "internal"; `_origin_source` is
        # the external source it descends from, which is where a reply has to
        # go back out (Pushover, Telegram, the web client, ...).
        "source": envelope.get("_origin_source") or envelope.get("source"),
        "source_meta": reply_meta,
        "channel": channel_id,
        "parent_id": envelope["id"],
    }
    sent = await registry.send_to_channel(channel_id, payload, reply_context)
    if sent:
        envelope["handlers_visited"] = envelope.get("handlers_visited", [])
        envelope["handlers_visited"].append(f"plugin:{channel_id}")
        _logger.info(
            f"Routed to plugin channel: {channel_id} "
            f"envelope={envelope['id']} text={pipeline_text[:120]!r}"
        )
        try:
            transcript.append(
                chat_id=str(reply_meta["chat_id"]),
                channel=channel_id,
                role="user",
                text=pipeline_text,
                envelope_id=envelope["id"],
                source=envelope.get("_origin_source") or envelope.get("source"),
            )
        except Exception as exc:
            _logger.warning(f"transcript append (channel in) failed: {exc}")
    else:
        _logger.warning(f"Plugin channel {channel_id} unreachable — send_to_channel failed")
    return sent


def _record_user_message(envelope: dict, channel: str, src: str) -> None:
    """Persist the incoming message for cross-device replay. Never fatal."""
    chat_id = (envelope.get("source_meta") or {}).get("chat_id")
    body = envelope.get("pipeline_text") or envelope.get("text") or ""
    attachments = (envelope.get("metadata") or {}).get("attachments") or None
    if not (chat_id and (body or attachments)):
        return
    try:
        transcript.append(
            chat_id=str(chat_id),
            channel=channel,
            role="user",
            text=body,
            envelope_id=envelope.get("id"),
            source=src,
            attachments=attachments,
        )
    except Exception as exc:
        _logger.warning(f"transcript append (user) failed: {exc}")


async def process_inbound(envelope: dict) -> None:
    src = envelope.get("source", "?")
    text = (envelope.get("pipeline_text") or "")[:80]
    target = envelope.pop("_target", None)
    channel = target or "pipeline"
    envelope["_transcript_channel"] = channel
    _logger.info(f"inbound [{src}]: {text!r}" + (f" target={target}" if target else ""))

    # Record user message (depth 0 only — no inject echoes). Skipped when the
    # message routes to a plugin channel: _route_to_plugin_channel records it
    # under the channel's own transcript thread instead.
    record_user = envelope.get("depth", 0) == 0

    if target:
        # API / web direct target — no allow_direct_inject gate (explicit user choice).
        # Plugin channels (Cursor, Claude, ...) take priority since their ids live in
        # the registry, not state.handlers.
        plugin_ch = next(
            (c for c in registry.list_plugin_channels() if c["channel_id"] == target),
            None,
        )
        if plugin_ch:
            if not await _route_to_plugin_channel(plugin_ch, envelope):
                _logger.error(f"Inject target '{target}' (plugin channel) send failed — discarding")
            return

        if record_user:
            _record_user_message(envelope, channel, src)
        target_handlers = _resolve_inject_target(target, require_allow_direct=False)
        if target_handlers is None:
            return
        if target_handlers:
            await _run("inbound", envelope, target_handlers, skip_match=True)
            return

    # Check plugin channels for trigger match
    pipeline_text = envelope.get("pipeline_text") or envelope.get("text", "")
    for ch in registry.list_plugin_channels():
        triggers = ch.get("triggers", [])
        if any(pipeline_text.lower().startswith(t.lower()) for t in triggers):
            if await _route_to_plugin_channel(ch, envelope):
                return  # Plugin takes over
            # Send failed and we are about to handle it in the pipeline
            # instead, so the channel must not also answer it on reconnect.
            registry.discard_pending(envelope["id"])

    if record_user:
        _record_user_message(envelope, channel, src)
    await _run("inbound", envelope)


async def process_outbound(envelope: dict) -> None:
    src = envelope.get("source", "?")
    text = (envelope.get("pipeline_text") or "")[:80]
    _logger.info(f"outbound [{src}]: {text!r}")
    delivered = await _run("outbound", envelope)

    if delivered or not expects_outbound_delivery(envelope):
        return

    # Nobody took it. Hand it back to the queue instead of letting the worker
    # mark it done: the handler may just be restarting, or not have
    # re-registered yet after a Core restart (registrations are restored from
    # disk now, but a remote that moved still needs a heartbeat to come back).
    target = envelope.get("_respond_target", "source")
    raise msg_queue.Retry(f"no outbound handler took [{src}→{target}]")
