"""FastAPI application: REST endpoints + SSE log stream."""
from __future__ import annotations
import asyncio
import json
import uuid
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Request, status, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sse_starlette.sse import EventSourceResponse

import core.log as log
import core.state as state
import core.msg_queue as queue
import core.store as store

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
_config_dir: Optional[Path] = None
_reload_fn = None  # injected by main.py


def configure(auth_token: str, webhook_token: str, config_dir: Path, reload_fn) -> None:
    global _auth_token, _webhook_token, _config_dir, _reload_fn
    _auth_token = auth_token
    _webhook_token = webhook_token
    _config_dir = config_dir
    _reload_fn = reload_fn


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
async def ui() -> HTMLResponse:
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
      match, router_hint, priority, invoke_path, health_path,
      timeout_seconds, ttl_seconds, continue_on_match, category
    """
    import core.registry as registry

    body = await request.json()
    try:
        return registry.register(body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/register/{handler_id}/heartbeat", dependencies=[Depends(_check_auth)])
async def register_heartbeat(handler_id: str, request: Request):
    """Refresh TTL for a registered remote handler."""
    import core.registry as registry

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
    import core.registry as registry

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


@app.post("/api/handlers/reload", dependencies=[Depends(_check_auth)])
async def reload_handlers():
    if _reload_fn:
        _reload_fn()
    return {"reloaded": len(state.handlers)}


@app.get("/api/queue", dependencies=[Depends(_check_auth)])
async def get_queue():
    return queue.depths()


@app.get("/api/plugin/channels", dependencies=[Depends(_check_auth)])
async def list_plugin_channels():
    import core.registry as registry

    return registry.list_plugin_channels()


async def _handle_plugin_reply(msg: dict) -> None:
    """Route a {"type": "reply"} message from a plugin WebSocket back to
    whoever sent the original message — mirrors the `respond` handling in
    pipeline._run() for regular handlers."""
    import core.registry as registry

    envelope_id = msg.get("envelope_id")
    channel_id = msg.get("channel")
    text = msg.get("text", "")
    handler_id = f"plugin:{channel_id}"

    ctx = registry.pop_pending_reply(envelope_id) if envelope_id else None
    if ctx is None:
        _logger.warning(f"plugin reply for unknown envelope_id={envelope_id!r} (channel={channel_id})")
        ctx = {}
    source = ctx.get("source") or "plugin"
    source_meta = ctx.get("source_meta") or {}
    transcript_channel = ctx.get("channel") or channel_id or "pipeline"
    parent_id = ctx.get("parent_id") or envelope_id

    _logger.info(
        f"[plugin:{channel_id}] reply ({len(text)} chars) for envelope={envelope_id} "
        f"source={source} channel={transcript_channel}"
    )

    if source in ("web", "app"):
        state.broadcast_reply({
            "type": "reply",
            "text": text,
            "parent_id": parent_id,
            "handler_id": handler_id,
            "source_meta": source_meta,
            "channel": transcript_channel,
        })

    try:
        import core.transcript as transcript

        chat_id = source_meta.get("chat_id")
        if chat_id and text:
            transcript.append(
                chat_id=str(chat_id),
                channel=transcript_channel,
                role="bot",
                text=text,
                handler_id=handler_id,
                envelope_id=envelope_id,
                source=source,
            )
    except Exception as exc:
        _logger.warning(f"transcript append (plugin reply) failed: {exc}")


async def _handle_plugin_reply_chunk(msg: dict) -> None:
    """Route a {"type": "reply_chunk"} streaming delta straight to any
    connected web/app client via SSE — never persisted, the final {"type":
    "reply"} carries the authoritative full text into the transcript."""
    import core.registry as registry

    envelope_id = msg.get("envelope_id")
    channel_id = msg.get("channel")
    text = msg.get("text", "")
    if not envelope_id or not text:
        return

    ctx = registry.peek_pending_reply(envelope_id)
    if ctx is None:
        return
    source = ctx.get("source") or "plugin"
    if source not in ("web", "app"):
        return

    _logger.info(f"[plugin:{channel_id}] chunk +{len(text)} chars for envelope={envelope_id}")

    state.broadcast_reply({
        "type": "reply_chunk",
        "text": text,
        "parent_id": ctx.get("parent_id") or envelope_id,
        "handler_id": f"plugin:{channel_id}",
        "source_meta": ctx.get("source_meta") or {},
        "channel": ctx.get("channel") or channel_id or "pipeline",
    })


@app.websocket("/api/plugin/ws")
async def plugin_ws(websocket: WebSocket):
    """Persistent connection for external plugins (Cursor, Claude, ...).

    Protocol (JSON messages, both directions):
      client -> server: {"type": "register", "plugin_id", "capabilities",
                          "channels": {channel_id: {label, hint, triggers,
                                                     capabilities, metadata}, ...}}
      server -> client: {"type": "registered", "plugin_id"}
      server -> client: {"type": "channel_registered", "channel_id", "ok"}  (per channel)
      server -> client: {"type": "message", "channel", "payload": {...}}    (routed message)
      client -> server: {"type": "reply_chunk", "channel", "envelope_id", "text"} (0+, streamed)
      client -> server: {"type": "reply", "channel", "envelope_id", "text", "continue"} (final, once)
      client -> server: {"type": "channel_update", "channel_id", "metadata"}  (merged into channel metadata)
      either direction:  {"type": "ping"} / {"type": "pong"}                (heartbeat)
    """
    import core.registry as registry

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
            registry.plugin_unregister(plugin_id)


@app.get("/api/logs", dependencies=[Depends(_check_auth)])
async def stream_logs(request: Request):
    """SSE endpoint: streams log entries as JSON."""
    q: asyncio.Queue = asyncio.Queue(maxsize=500)
    state.sse_queues.add(q)

    # Send recent buffer first
    for entry in list(state.log_buffer):
        await q.put(entry)

    async def _generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    entry = await asyncio.wait_for(q.get(), timeout=15.0)
                    yield {"data": json.dumps(entry)}
                except asyncio.TimeoutError:
                    yield {"data": json.dumps({"ping": True})}
        finally:
            state.sse_queues.discard(q)

    return EventSourceResponse(_generator())


@app.get("/api/replies", dependencies=[Depends(_check_auth)])
async def stream_replies(request: Request):
    """SSE endpoint: outbound replies for web/app clients."""
    q: asyncio.Queue = asyncio.Queue(maxsize=200)
    state.reply_queues.add(q)

    async def _generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    entry = await asyncio.wait_for(q.get(), timeout=15.0)
                    yield {"data": json.dumps(entry)}
                except asyncio.TimeoutError:
                    yield {"data": json.dumps({"ping": True})}
        finally:
            state.reply_queues.discard(q)

    return EventSourceResponse(_generator())


@app.get("/api/transcript", dependencies=[Depends(_check_auth)])
async def get_transcript(
    chat_id: str,
    channel: str = "pipeline",
    limit: int = 100,
):
    """Return recent messages for a chat/channel (oldest → newest).

    Hard-capped in storage (200/channel) and by `limit` (max 200).
    """
    import core.transcript as transcript

    if not chat_id:
        raise HTTPException(status_code=400, detail="chat_id required")
    msgs = transcript.list_messages(chat_id, channel=channel or "pipeline", limit=limit)
    return {
        "chat_id": chat_id,
        "channel": channel or "pipeline",
        "messages": msgs,
    }


@app.post("/api/message", dependencies=[Depends(_check_auth)])
async def inject_message(request: Request):
    """Inject a message into the inbound pipeline.

    Body:
      text       (required) message text
      target     (optional) handler id or category — skip full pipeline / match
      chat_id    (optional) stored in source_meta for context store
      metadata   (optional) free-form dict
      source     (optional) defaults to "web"
    """
    body = await request.json()
    text = body.get("text", "")
    if not text:
        raise HTTPException(status_code=400, detail="text required")

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

    envelope = {
        "id": str(uuid.uuid4()),
        "parent_id": None,
        "direction": "inbound",
        "source": source,
        "source_meta": source_meta,
        "text": text,
        "pipeline_text": text,
        "depth": 0,
        "metadata": body.get("metadata") or {},
    }
    if target:
        # Validated when the inbound worker runs (handler may not be loaded yet)
        envelope["_target"] = target

    mid = queue.enqueue_inbound(envelope)
    return {"id": mid, "status": "queued", "target": target}


@app.post("/webhook/{token}")
async def webhook(token: str, request: Request):
    if _webhook_token and token != _webhook_token:
        raise HTTPException(status_code=403, detail="Invalid token")

    body = await request.json()
    text = body.get("text", body.get("message", str(body)))
    envelope = {
        "id": str(uuid.uuid4()),
        "parent_id": None,
        "direction": "inbound",
        "source": "webhook",
        "source_meta": {"remote": str(request.client)},
        "text": text,
        "pipeline_text": text,
        "depth": 0,
        "metadata": {},
    }
    mid = queue.enqueue_inbound(envelope)
    return {"id": mid, "status": "queued"}


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
