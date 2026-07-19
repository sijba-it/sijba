# REST API

All endpoints require `Authorization: Bearer <token>` when `auth_token` is set in `core.yaml`.

## Contents

- [Handlers](#handlers)
- [Messages](#messages)
- [Logs](#logs)
- [Queue](#queue)
- [Context store](#context-store)
- [Web UI](#web-ui)

---

## Handlers

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/handlers` | List all handlers with their states |
| `POST` | `/api/handlers/{id}/toggle` | Enable / disable a handler |
| `POST` | `/api/handlers/reload` | Hot-reload handler configs (no restart needed) |
| `POST` | `/api/register` | Register a remote HTTP handler / pipeline |
| `POST` | `/api/register/{id}/heartbeat` | Refresh TTL for a registered handler |
| `DELETE` | `/api/register/{id}` | Unregister a remote handler |

### Register a remote pipeline

External processes (e.g. a kubectl agent on the k8s master) can attach at runtime.
Core will `POST` the message envelope to `{url}{invoke_path}` and expect a normal plugin JSON response.

```bash
curl -X POST https://sijba.edv-bartl.at/api/register \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "kubectl",
    "url": "http://10.5.191.10:9099",
    "match": { "regex": "(?i)^pods\\b" },
    "router_hint": "Kubernetes pods. Trigger: pods / pods top",
    "ttl_seconds": 90,
    "priority": 25
  }'
```

`url` must be reachable **from the Core pod**. Use a node IP + hostPort, Tailscale IP, or in-cluster Service URL. Optional `ttl_seconds` — renew via heartbeat or re-register.

---

## Messages

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/message` | Inject a message into the inbound pipeline |
| `POST` | `/webhook/{token}` | External webhook → inbound pipeline |

### Inject a message

```bash
curl -X POST http://localhost:8080/api/message \
  -H "Authorization: Bearer secret" \
  -H "Content-Type: application/json" \
  -d '{"text": "iobroker get Bubble"}'
```

Optional body fields:

| Field | Description |
|-------|-------------|
| `target` | Handler id or category — run only that part (skips match / full pipeline). Omit for the general inbound pipeline (router, LLM, …). |
| `chat_id` | Stored in `source_meta` for the context store |
| `source` | Defaults to `web` |

```bash
# Direct to one handler (no trigger prefix required)
curl -X POST http://localhost:8080/api/message \
  -H "Authorization: Bearer secret" \
  -H "Content-Type: application/json" \
  -d '{"text": "get Bubble", "target": "iobroker", "chat_id": "web-1"}'
```

### Webhook

```bash
curl -X POST http://localhost:8080/webhook/mytoken \
  -H "Content-Type: application/json" \
  -d '{"text": "hello"}'
```

---

## Logs & replies

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/logs` | SSE live log stream |
| `GET` | `/api/replies` | SSE outbound replies for `web` / `app` sources |

SSE clients that cannot set `Authorization` may pass `?token=<auth_token>`.

```bash
curl -N http://localhost:8080/api/logs \
  -H "Authorization: Bearer secret"

curl -N "http://localhost:8080/api/replies?token=secret"
```

---

## Queue

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/queue` | Queue status (processed in/out counts) |

---

## Context store

Stores per-chat conversation history and arbitrary handler state.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/store/{namespace}` | List all keys in namespace |
| `GET` | `/api/store/{namespace}/{key}` | Get value |
| `PUT` | `/api/store/{namespace}/{key}` | Set value |
| `DELETE` | `/api/store/{namespace}/{key}` | Delete entry |
| `DELETE` | `/api/store/{namespace}` | Clear entire namespace |

```bash
# Inspect conversation history for a Telegram chat
curl http://localhost:8080/api/store/llm/123456789 \
  -H "Authorization: Bearer secret"

# Reset conversation (clears LLM history for this chat)
curl -X DELETE http://localhost:8080/api/store/llm/123456789 \
  -H "Authorization: Bearer secret"
```

Namespace is the handler's `context.namespace` (defaults to handler `id`).
Key is derived from `source_meta` using `context.key` (defaults to `chat_id`).

---

## Web UI

`GET /` — serves the web UI at `http://localhost:8080`.
