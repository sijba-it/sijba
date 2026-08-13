"""Inbound and outbound pipeline execution."""
from __future__ import annotations
import asyncio
import re
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

import core.log as log
import core.state as state
import core.plugin_runner as plugin_runner
import core.registry as registry

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
# Build envelope
# ------------------------------------------------------------------

def _new_envelope(
    source: str,
    text: str,
    source_meta: dict = None,
    parent_id: str = None,
    depth: int = 0,
    metadata: dict = None,
) -> dict:
    return {
        "id": str(uuid.uuid4()),
        "parent_id": parent_id,
        "direction": "inbound",
        "source": source,
        "source_meta": source_meta or {},
        "text": text,
        "pipeline_text": text,
        "depth": depth,
        "metadata": metadata or {},
    }


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
) -> None:
    if handler_override is not None:
        handlers = handler_override
    elif direction == "outbound":
        handlers = _outbound_handlers_for(envelope)
    else:
        handlers = [h for h in state.handlers if h.get("direction") == direction]

    for handler in handlers:
        hid = handler["id"]

        hstate = state.handler_states.get(hid, "healthy")
        if hstate not in ("healthy",):
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
            continue

        response = await plugin_runner.invoke(handler, envelope, _config_dir)

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
            out_env = {
                "id": str(uuid.uuid4()),
                "parent_id": envelope["id"],
                "direction": "outbound",
                "source": src,
                "source_meta": envelope.get("source_meta", {}),
                "text": respond_text,
                "pipeline_text": respond_text,
                "depth": envelope.get("depth", 0),
                "metadata": {},
                "_respond_target": respond_target,
            }
            _enqueue_outbound(out_env)
            # Let web/app clients hear replies without a dedicated outbound sink
            if src in ("web", "app"):
                state.broadcast_reply({
                    "type": "reply",
                    "text": respond_text,
                    "parent_id": envelope["id"],
                    "handler_id": hid,
                    "source_meta": envelope.get("source_meta") or {},
                    "channel": envelope.get("_transcript_channel") or "pipeline",
                })
            # Persist bot reply for cross-device replay (bounded)
            try:
                import core.transcript as transcript
                chat_id = (envelope.get("source_meta") or {}).get("chat_id")
                if chat_id and respond_text:
                    transcript.append(
                        chat_id=str(chat_id),
                        channel=envelope.get("_transcript_channel") or "pipeline",
                        role="bot",
                        text=respond_text,
                        handler_id=hid,
                        envelope_id=envelope.get("id"),
                        source=src,
                    )
            except Exception as exc:
                _logger.warning(f"transcript append (bot) failed: {exc}")

        # inject → new inbound message
        if "inject" in response and response["inject"]:
            inj = response["inject"]
            depth = envelope.get("depth", 0) + 1
            if depth > _max_inject_depth:
                _logger.warning(
                    f"[{hid}] inject depth {depth} exceeds max_inject_depth={_max_inject_depth} — discarding"
                )
            else:
                child_env = _new_envelope(
                    source="internal",
                    text=inj.get("text", ""),
                    parent_id=envelope["id"],
                    depth=depth,
                    metadata=inj.get("metadata") or {},
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
    }
    reply_context = {
        "source": envelope.get("source"),
        "source_meta": envelope.get("source_meta", {}),
        "channel": envelope.get("_transcript_channel") or "pipeline",
        "parent_id": envelope["id"],
    }
    sent = await registry.send_to_channel(ch["channel_id"], payload, reply_context)
    if sent:
        envelope["handlers_visited"] = envelope.get("handlers_visited", [])
        envelope["handlers_visited"].append(f"plugin:{ch['channel_id']}")
        _logger.info(f"Routed to plugin channel: {ch['channel_id']}")
    return sent


async def process_inbound(envelope: dict) -> None:
    src = envelope.get("source", "?")
    text = (envelope.get("pipeline_text") or "")[:80]
    target = envelope.pop("_target", None)
    channel = target or "pipeline"
    envelope["_transcript_channel"] = channel
    _logger.info(f"inbound [{src}]: {text!r}" + (f" target={target}" if target else ""))

    # Record user message (depth 0 only — no inject echoes)
    if envelope.get("depth", 0) == 0:
        try:
            import core.transcript as transcript
            chat_id = (envelope.get("source_meta") or {}).get("chat_id")
            body = envelope.get("pipeline_text") or envelope.get("text") or ""
            if chat_id and body:
                transcript.append(
                    chat_id=str(chat_id),
                    channel=channel,
                    role="user",
                    text=body,
                    envelope_id=envelope.get("id"),
                    source=src,
                )
        except Exception as exc:
            _logger.warning(f"transcript append (user) failed: {exc}")

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

    await _run("inbound", envelope)


async def process_outbound(envelope: dict) -> None:
    src = envelope.get("source", "?")
    text = (envelope.get("pipeline_text") or "")[:80]
    _logger.info(f"outbound [{src}]: {text!r}")
    await _run("outbound", envelope)
