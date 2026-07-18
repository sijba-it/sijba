"""FastAPI application: REST endpoints + SSE log stream."""
from __future__ import annotations
import asyncio
import json
import uuid
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Request, status
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
        })
    return result


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
