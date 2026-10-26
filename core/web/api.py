"""FastAPI application: REST endpoints + SSE log stream."""
from __future__ import annotations
import asyncio
import json
import uuid
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, File, HTTPException, Request, UploadFile, status, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sse_starlette.sse import EventSourceResponse

import core.log as log
import core.channel_permissions as channel_permissions
import core.msg_queue as queue
import core.registry as registry
import core.reply as reply
import core.state as state
import core.store as store
import core.transcript as transcript
from core.envelope import new_envelope

_logger = log.get("api")
app = FastAPI(title="Sjba", docs_url=None, redoc_url=None)

# Separate frontend (sijba-web) talks cross-origin over Tailscale/LAN.
# Auth remains Bearer — CORS is not a security boundary here.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

_auth_token: Optional[str] = None
_webhook_token: Optional[str] = None
_webhooks: dict = {}  # token -> name, from the `webhooks:` config block
_config_dir: Optional[Path] = None
_upload_dir: Optional[Path] = None

# PVC is small (1Gi) and this is a single-user instance — keep uploads modest.
MAX_UPLOAD_BYTES = 25 * 1024 * 1024


def configure(auth_token: str, webhook_token: str, config_dir: Path,
              upload_dir: Path, webhooks: dict = None) -> None:
    global _auth_token, _webhook_token, _webhooks, _config_dir, _upload_dir
    _auth_token = auth_token
    _webhook_token = webhook_token
    _webhooks = webhooks or {}
    _config_dir = config_dir
    _upload_dir = upload_dir


# ------------------------------------------------------------------
# Auth
# ------------------------------------------------------------------

_bearer = HTTPBearer(auto_error=False)


def _check_auth(
    request: Request,
    creds: Optional[HTTPAuthorizationCredentials] = Depends(_bearer),
) -> None:
    if not _auth_token:
        return  # no auth configured
    # EventSource cannot set Authorization headers — allow ?token= for SSE.
    query_token = request.query_params.get("token")
    if creds and creds.credentials == _auth_token:
        return
    if query_token and query_token == _auth_token:
        return
    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")


# ------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def ui():
    import os
    config_ui = os.environ.get("SIJBA_CONFIG_UI_URL", "").strip()
    if config_ui:
        return RedirectResponse(config_ui, status_code=307)
    ui_path = Path(__file__).parent / "ui.html"
    return HTMLResponse(ui_path.read_text())


@app.get("/api/version")
async def version():
    """Build/deploy identity (set at image build via Jenkins). No auth."""
    import os
    return {
        "service": "sijba",
        "version": os.environ.get("SIJBA_VERSION", "dev"),
        "commit": os.environ.get("GIT_COMMIT", "unknown"),
    }


@app.get("/api/handlers", dependencies=[Depends(_check_auth)])
async def list_handlers():
    result = []
    for h in state.handlers:
        result.append({
            "id": h["id"],
            "priority": h["priority"],
            "direction": h["direction"],
            "lifecycle": h.get("lifecycle", "per_message"),
            "type": h.get("type"),
            "state": state.handler_states.get(h["id"], "unknown"),
            "category": h.get("category", []),
            "router_hint": h.get("router_hint"),
            "match": h.get("match") or {},
            "dynamic": bool(h.get("_dynamic")),
        })
    return result


@app.post("/api/register", dependencies=[Depends(_check_auth)])
async def register_handler(request: Request):
    """Register (or refresh) a remote HTTP handler / pipeline.

    Body:
      id, url (required)
      match, router_hint, priority, invoke_path,
      timeout_seconds, ttl_seconds, continue_on_match, category
    """
    body = await request.json()
    try:
        return registry.register(body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/register/{handler_id}/heartbeat", dependencies=[Depends(_check_auth)])
async def register_heartbeat(handler_id: str, request: Request):
    """Refresh TTL for a registered remote handler."""
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    ttl = body.get("ttl_seconds") if isinstance(body, dict) else None
    if not registry.heartbeat(handler_id, ttl):
        raise HTTPException(status_code=404, detail="not registered")
    return {"id": handler_id, "status": "ok"}


@app.delete("/api/register/{handler_id}", dependencies=[Depends(_check_auth)])
async def unregister_handler(handler_id: str):
    if not registry.unregister(handler_id):
        raise HTTPException(status_code=404, detail="not registered")
    return {"id": handler_id, "status": "unregistered"}


@app.post("/api/handlers/{handler_id}/toggle", dependencies=[Depends(_check_auth)])
async def toggle_handler(handler_id: str):
    from core import lifecycle as lc

    handler = next((h for h in state.handlers if h["id"] == handler_id), None)
    if handler is None:
        raise HTTPException(status_code=404, detail="Handler not found")

    current = state.handler_states.get(handler_id, "healthy")

    if current == "stopped":
        # Re-enable
        if handler.get("lifecycle") == "persistent":
            ph = state.persistent.get(handler_id)
            if ph is None:
                ph = lc.PersistentHandler(handler, _config_dir)
                state.persistent[handler_id] = ph
            asyncio.create_task(ph.start())
        else:
            state.handler_states[handler_id] = "healthy"
        new_state = state.handler_states.get(handler_id, "starting")
    else:
        # Disable
        state.handler_states[handler_id] = "stopped"
        if handler.get("lifecycle") == "persistent":
            ph = state.persistent.get(handler_id)
            if ph:
                asyncio.create_task(ph.stop())
        new_state = "stopped"

    return {"id": handler_id, "state": new_state}


@app.post("/api/admin/reload", dependencies=[Depends(_check_auth)])
async def reload_config():
    """Re-read core.yaml and handlers/ and apply them without a restart."""
    from core import reload as rl

    return await rl.reload()


@app.get("/api/queue", dependencies=[Depends(_check_auth)])
async def get_queue():
    return queue.depths()


@app.get("/api/plugin/channels", dependencies=[Depends(_check_auth)])
async def list_plugin_channels():
    return registry.list_plugin_channels()


@app.get("/api/admin/channel-permissions", dependencies=[Depends(_check_auth)])
async def get_channel_permissions():
    return channel_permissions.all_permissions()


@app.patch("/api/admin/channel-permissions/{channel_id:path}", dependencies=[Depends(_check_auth)])
async def set_channel_permission(channel_id: str, request: Request):
    """Set a channel's permission flags. Called by the Agent Controller
    (operator-side, trusted the same way registry.json's own `plugin`
    field already is) — never by the plugin process itself, see
    core/channel_permissions.py's docstring for why that distinction
    matters. `channel_id` is path-encoded since it contains a literal
    colon (e.g. "cursor:obsidian").
    """
    body = await request.json()
    if "can_inject" in body:
        channel_permissions.set_can_inject(channel_id, bool(body["can_inject"]))
    return {"channel_id": channel_id, **channel_permissions.all_permissions().get(channel_id, {})}


@app.get("/api/webhooks", dependencies=[Depends(_check_auth)])
async def list_webhooks():
    """Configured webhooks as {name, source} — never the tokens.

    Lets the app offer a "send as this webhook" affordance (a message tagged
    with the webhook's source, routed like a real POST /webhook/<token> call).
    """
    result = [
        {"name": name, "source": f"webhook:{name}"}
        for name in sorted(set(_webhooks.values()))
    ]
    if _webhook_token:
        result.append({"name": "webhook", "source": "webhook"})
    return result


async def _handle_plugin_reply(msg: dict) -> None:
    """Route a {"type": "reply"} message from a plugin WebSocket back to
    whoever sent the original message — the same delivery the pipeline does
    for a handler's `respond`."""
    envelope_id = msg.get("envelope_id")
    channel_id = msg.get("channel")
    text = msg.get("text", "")

    ctx = registry.pop_pending_reply(envelope_id) if envelope_id else None
    if ctx is None:
        _logger.warning(f"plugin reply for unknown envelope_id={envelope_id!r} (channel={channel_id})")
        ctx = {}

    source = ctx.get("source") or "plugin"
    channel = ctx.get("channel") or channel_id or "pipeline"
    _logger.info(
        f"[plugin:{channel_id}] reply ({len(text)} chars) for envelope={envelope_id} "
        f"source={source} channel={channel}"
    )

    reply.deliver(
        text=text,
        source=source,
        source_meta=ctx.get("source_meta"),
        channel=channel,
        parent_id=ctx.get("parent_id") or envelope_id,
        handler_id=f"plugin:{channel_id}",
        envelope_id=envelope_id,
    )


async def _handle_plugin_inject(msg: dict) -> None:
    """Route a {"type": "inject"} message: a plugin channel sending a
    brand-new message into some other target, not a reply to its own turn
    (see plugin_ws's protocol docstring). Gated by
    channel_permissions.can_inject(source channel) — the *sending* plugin
    process never gets to decide this for itself (see that module's
    docstring for why); Core is the sole authority, checked fresh on every
    inject rather than trusted from whatever the plugin claimed at
    `register` time.
    """
    source_channel = msg.get("channel")
    target = msg.get("target")
    text = msg.get("text", "")
    if not source_channel or not target or not text:
        _logger.warning(
            f"inject from {source_channel!r} missing channel/target/text — dropped"
        )
        return
    if not channel_permissions.can_inject(source_channel):
        _logger.warning(
            f"inject from {source_channel!r} to {target!r} rejected — can_inject not set"
        )
        return
    envelope = new_envelope(
        source="internal",
        text=text,
        source_meta={"injected_by": source_channel},
    )
    envelope["_target"] = target
    queue.enqueue_inbound(envelope)
    _logger.info(f"inject: {source_channel!r} -> {target!r} ({len(text)} chars)")


async def _handle_plugin_reply_chunk(msg: dict) -> None:
    """Route a {"type": "reply_chunk"} streaming delta straight to any
    connected web/app client — never persisted, the final {"type": "reply"}
    carries the authoritative full text into the transcript."""
    envelope_id = msg.get("envelope_id")
    channel_id = msg.get("channel")
    text = msg.get("text", "")
    if not envelope_id or not text:
        return

    ctx = registry.peek_pending_reply(envelope_id)
    if ctx is None:
        return

    _logger.info(f"[plugin:{channel_id}] chunk +{len(text)} chars for envelope={envelope_id}")

    reply.deliver(
        text=text,
        source=ctx.get("source") or "plugin",
        source_meta=ctx.get("source_meta"),
        channel=ctx.get("channel") or channel_id or "pipeline",
        parent_id=ctx.get("parent_id") or envelope_id,
        handler_id=f"plugin:{channel_id}",
        event="reply_chunk",
        final=False,
    )


@app.websocket("/api/plugin/ws")
async def plugin_ws(websocket: WebSocket):
    """Persistent connection for external plugins (Cursor, Claude, ...).

    Protocol (JSON messages, both directions):
      client -> server: {"type": "register", "plugin_id", "capabilities",
                          "channels": {channel_id: {label, hint, triggers,
                                                     capabilities, metadata}, ...}}
      server -> client: {"type": "registered", "plugin_id"}
      server -> client: {"type": "channel_registered", "channel_id", "ok"}  (per channel)
      server -> client: {"type": "message", "channel", "payload": {...}}    (routed message;
                          re-sent on channel re-register if no final reply yet)
      client -> server: {"type": "reply_chunk", "channel", "envelope_id", "text"} (0+, streamed)
      client -> server: {"type": "reply", "channel", "envelope_id", "text", "continue"} (final, once;
                          clears Core's pending redelivery entry)
      client -> server: {"type": "channel_update", "channel_id", "metadata"}  (merged into channel metadata)
      client -> server: {"type": "inject", "channel", "target", "text"}     (new message *from*
                          `channel` *into* `target`, not a reply to this turn; requires
                          channel_permissions.can_inject(channel) — see core/channel_permissions.py)
      either direction:  {"type": "ping"} / {"type": "pong"}                (heartbeat)
    """
    token = websocket.query_params.get("token")
    if _auth_token and token != _auth_token:
        _logger.warning(f"plugin_ws: rejected connection from {websocket.client} — bad/missing token")
        await websocket.close(code=4401)
        return

    await websocket.accept()
    _logger.info(f"plugin_ws: connected from {websocket.client}, awaiting registration")
    plugin_id: Optional[str] = None

    async def _ping_loop() -> None:
        while True:
            await asyncio.sleep(20)
            await websocket.send_json({"type": "ping"})

    ping_task = asyncio.create_task(_ping_loop())
    try:
        while True:
            msg = await websocket.receive_json()
            mtype = msg.get("type")

            if mtype == "register":
                plugin_id = (msg.get("plugin_id") or "").strip()
                if not plugin_id:
                    await websocket.send_json({"type": "error", "message": "plugin_id required"})
                    continue
                registry.plugin_register(plugin_id, websocket, msg.get("capabilities"))
                await websocket.send_json({"type": "registered", "plugin_id": plugin_id})
                for ch_id, ch_cfg in (msg.get("channels") or {}).items():
                    ok = registry.plugin_register_channel(plugin_id, ch_id, ch_cfg or {})
                    await websocket.send_json({"type": "channel_registered", "channel_id": ch_id, "ok": ok})

            elif mtype == "reply":
                if plugin_id:
                    registry.plugin_heartbeat(plugin_id)
                await _handle_plugin_reply(msg)

            elif mtype == "reply_chunk":
                if plugin_id:
                    registry.plugin_heartbeat(plugin_id)
                await _handle_plugin_reply_chunk(msg)

            elif mtype == "inject":
                if plugin_id:
                    registry.plugin_heartbeat(plugin_id)
                await _handle_plugin_inject(msg)

            elif mtype == "channel_update":
                if plugin_id:
                    registry.plugin_heartbeat(plugin_id)
                    ch_id = msg.get("channel_id")
                    patch = msg.get("metadata") or {}
                    if ch_id:
                        registry.plugin_update_channel(plugin_id, ch_id, patch)

            elif mtype in ("ping", "pong"):
                if plugin_id:
                    registry.plugin_heartbeat(plugin_id)
                if mtype == "ping":
                    await websocket.send_json({"type": "pong"})

            else:
                _logger.warning(f"plugin_ws: unknown message type {mtype!r} from {plugin_id}")
    except WebSocketDisconnect:
        _logger.info(f"plugin_ws: disconnected (plugin_id={plugin_id})")
    except Exception as exc:
        _logger.warning(f"plugin_ws error (plugin_id={plugin_id}): {exc}")
    finally:
        ping_task.cancel()
        if plugin_id:
            registry.plugin_unregister(plugin_id, websocket)


# ------------------------------------------------------------------
# SSE / WebSocket streams
# ------------------------------------------------------------------

# Idle gap after which a keepalive goes out so proxies don't drop the stream.
_STREAM_PING_SECONDS = 15.0


async def _sse_stream(request: Request, q: asyncio.Queue, registry_set: set):
    """Drain `q` as SSE until the client goes away, then unsubscribe."""
    try:
        while True:
            if await request.is_disconnected():
                break
            try:
                entry = await asyncio.wait_for(q.get(), timeout=_STREAM_PING_SECONDS)
            except asyncio.TimeoutError:
                entry = {"ping": True}
            yield {"data": json.dumps(entry)}
    finally:
        registry_set.discard(q)


@app.get("/api/logs", dependencies=[Depends(_check_auth)])
async def stream_logs(request: Request):
    """SSE endpoint: streams log entries as JSON."""
    q: asyncio.Queue = asyncio.Queue(maxsize=500)
    state.sse_queues.add(q)

    # Send recent buffer first
    for entry in list(state.log_buffer):
        await q.put(entry)

    return EventSourceResponse(_sse_stream(request, q, state.sse_queues))


@app.get("/api/replies", dependencies=[Depends(_check_auth)])
async def stream_replies(request: Request):
    """SSE endpoint: outbound replies for web/app clients."""
    q: asyncio.Queue = asyncio.Queue(maxsize=200)
    state.reply_queues.add(q)

    return EventSourceResponse(_sse_stream(request, q, state.reply_queues))


@app.websocket("/api/ws/replies")
async def ws_replies(websocket: WebSocket):
    """WebSocket variant of /api/replies for web/app clients.

    Same fan-out (state.reply_queues) and same JSON event shapes as the SSE
    endpoint — this is an additional transport, not a replacement. A native
    WebSocket gets protocol-level close/error signaling that a hand-rolled
    HTTP streaming client can't rely on (no silent-stale-connection class of
    bug), at the cost of needing a WS-capable client library instead of
    plain http.
    """
    token = websocket.query_params.get("token")
    if _auth_token and token != _auth_token:
        await websocket.close(code=4401)
        return

    await websocket.accept()
    q: asyncio.Queue = asyncio.Queue(maxsize=200)
    state.reply_queues.add(q)
    try:
        while True:
            try:
                entry = await asyncio.wait_for(q.get(), timeout=_STREAM_PING_SECONDS)
            except asyncio.TimeoutError:
                entry = {"ping": True}
            await websocket.send_text(json.dumps(entry))
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        _logger.warning(f"ws_replies error: {exc}")
    finally:
        state.reply_queues.discard(q)


@app.get("/api/transcript", dependencies=[Depends(_check_auth)])
async def get_transcript(
    chat_id: str,
    channel: str = "pipeline",
    limit: int = 100,
):
    """Return recent messages for a chat/channel (oldest → newest).

    Hard-capped in storage (200/channel) and by `limit` (max 200).
    """
    if not chat_id:
        raise HTTPException(status_code=400, detail="chat_id required")
    msgs = transcript.list_messages(chat_id, channel=channel or "pipeline", limit=limit)
    return {
        "chat_id": chat_id,
        "channel": channel or "pipeline",
        "messages": msgs,
    }


@app.post("/api/upload", dependencies=[Depends(_check_auth)])
async def upload_file(file: UploadFile = File(...)):
    file_id = str(uuid.uuid4())
    dest_dir = _upload_dir / file_id
    dest_dir.mkdir(parents=True)
    filename = Path(file.filename or "file").name  # strip any path components
    dest = dest_dir / filename
    size = 0
    try:
        with dest.open("wb") as f:
            while chunk := await file.read(1024 * 1024):
                size += len(chunk)
                if size > MAX_UPLOAD_BYTES:
                    raise HTTPException(
                        status_code=413,
                        detail=f"file too large (max {MAX_UPLOAD_BYTES // (1024 * 1024)}MB)",
                    )
                f.write(chunk)
    except HTTPException:
        import shutil
        shutil.rmtree(dest_dir, ignore_errors=True)
        raise
    _logger.info(f"upload: {file_id} {filename!r} ({size} bytes)")
    return {
        "id": file_id,
        "filename": filename,
        "content_type": file.content_type,
        "size": size,
        "url": f"/api/upload/{file_id}",
    }


@app.get("/api/upload/{file_id}", dependencies=[Depends(_check_auth)])
async def download_file(file_id: str):
    try:
        uuid.UUID(file_id)
    except ValueError:
        raise HTTPException(status_code=404, detail="not found")
    dest_dir = _upload_dir / file_id
    matches = [p for p in dest_dir.iterdir() if p.is_file()] if dest_dir.is_dir() else []
    if not matches:
        raise HTTPException(status_code=404, detail="not found")
    return FileResponse(matches[0], filename=matches[0].name)


@app.post("/api/message", dependencies=[Depends(_check_auth)])
async def inject_message(request: Request):
    """Inject a message into the inbound pipeline.

    Body:
      text        message text (required unless attachments given)
      target      (optional) handler id or category — skip full pipeline / match
      chat_id     (optional) stored in source_meta for context store
      metadata    (optional) free-form dict
      source      (optional) defaults to "web"
      attachments (optional) list of {id, filename, content_type, size, url}
                  from /api/upload — stashed in metadata.attachments
    """
    body = await request.json()
    text = body.get("text", "")
    attachments = body.get("attachments") or []
    if not text and not attachments:
        raise HTTPException(status_code=400, detail="text or attachments required")

    target = body.get("target") or None
    if target is not None:
        target = str(target).strip() or None

    source = body.get("source") or "web"
    source_meta = dict(body.get("source_meta") or {})
    if body.get("chat_id") is not None:
        source_meta["chat_id"] = str(body["chat_id"])
    source_meta.setdefault("user", "admin")
    # Stable default so browsers share one transcript
    source_meta.setdefault("chat_id", "web")

    metadata = dict(body.get("metadata") or {})
    if attachments:
        metadata["attachments"] = attachments

    envelope = new_envelope(
        source=source,
        text=text,
        source_meta=source_meta,
        metadata=metadata,
    )
    if target:
        # Validated when the inbound worker runs (handler may not be loaded yet)
        envelope["_target"] = target

    mid = queue.enqueue_inbound(envelope)
    return {"id": mid, "status": "queued", "target": target}


def _webhook_source(token: str) -> Optional[str]:
    """Resolve a webhook token to the source it speaks as, or None to reject.

    The token *is* the identity, so the source cannot be spoofed by the
    caller. Named webhooks become "webhook:<name>"; the single legacy
    `webhook_token` stays plain "webhook".
    """
    name = _webhooks.get(token)
    if name:
        return f"webhook:{name}"
    if _webhook_token and token == _webhook_token:
        return "webhook"
    if not _webhook_token and not _webhooks:
        return "webhook"  # no webhook auth configured at all
    return None


def _form_str(form, *keys: str) -> Optional[str]:
    """First non-empty string value among `keys` in a starlette FormData.
    Skips file parts (UploadFile) so a stray `audio` upload can't sneak in."""
    for key in keys:
        val = form.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return None


@app.post("/webhook/{token}")
async def webhook(token: str, request: Request):
    """Inject a message from an external system (voice shortcut, IoT, Pebble Index, ...).

    Which webhook the token belongs to decides the envelope source, so each
    one can be routed — and answered — differently via match.source.

    Accepts JSON or multipart/form-data:

      JSON:  {"text"|"message": ..., "chat_id"?: ..., "target"?: ...}
      form:  transcription|text=...   (Pebble Index sends `transcription`,
             `recordedAt` in ms, `client`; an `audio` file part is ignored)

    `chat_id` decides which transcript bucket the message and its reply land in
    (how the app surfaces it). A form POST that can't set it — Pebble — is
    threaded under the source name.
    """
    source = _webhook_source(token)
    if source is None:
        _logger.warning(f"webhook: rejected unknown token from {request.client}")
        raise HTTPException(status_code=403, detail="Invalid token")

    source_meta = {"remote": str(request.client)}
    ctype = request.headers.get("content-type", "").split(";", 1)[0].strip()

    if ctype in ("multipart/form-data", "application/x-www-form-urlencoded"):
        form = await request.form()
        text = _form_str(form, "transcription", "text", "message")
        if not text:
            raise HTTPException(status_code=400, detail="no transcription/text in form")
        chat_id = _form_str(form, "chat_id")
        recorded_at = _form_str(form, "recordedAt")
        if recorded_at:
            source_meta["recorded_at"] = recorded_at
        client = _form_str(form, "client")
        if client:
            source_meta["client"] = client
        target = _form_str(form, "target")
        # No chat_id on the wire (Pebble can't set one) → thread every note
        # from this webhook under its source, not the shared "web" bucket.
        source_meta["chat_id"] = chat_id or source
    else:
        body = await request.json()
        text = body.get("text", body.get("message", str(body)))
        if body.get("chat_id") is not None:
            source_meta["chat_id"] = str(body["chat_id"])
        source_meta.setdefault("chat_id", "web")
        target = (str(body.get("target") or "").strip()) or None

    envelope = new_envelope(source=source, text=text, source_meta=source_meta)
    # External entry point with no live client of its own — ask the pipeline to
    # push this message to web/app clients so it shows up live in the tile.
    envelope["_activity_broadcast"] = True
    if target:
        envelope["_target"] = target

    mid = queue.enqueue_inbound(envelope)
    _logger.info(f"webhook [{source}]: {text[:120]!r}")
    return {"id": mid, "status": "queued", "source": source}


# ------------------------------------------------------------------
# Store endpoints
# ------------------------------------------------------------------

@app.get("/api/store/{namespace}", dependencies=[Depends(_check_auth)])
async def store_list(namespace: str):
    return store.list_ns(namespace)


@app.get("/api/store/{namespace}/{key}", dependencies=[Depends(_check_auth)])
async def store_get(namespace: str, key: str):
    val = store.get(namespace, key)
    if val is None:
        raise HTTPException(status_code=404, detail="Not found")
    return {"namespace": namespace, "key": key, "value": val}


@app.put("/api/store/{namespace}/{key}", dependencies=[Depends(_check_auth)])
async def store_set(namespace: str, key: str, request: Request):
    body = await request.json()
    store.set(namespace, key, body.get("value", body))
    return {"namespace": namespace, "key": key, "status": "ok"}


@app.delete("/api/store/{namespace}/{key}", dependencies=[Depends(_check_auth)])
async def store_delete(namespace: str, key: str):
    store.delete(namespace, key)
    return {"namespace": namespace, "key": key, "status": "deleted"}


@app.delete("/api/store/{namespace}", dependencies=[Depends(_check_auth)])
async def store_clear(namespace: str):
    n = store.clear_ns(namespace)
    return {"namespace": namespace, "deleted": n}
