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

### Webhook

```bash
curl -X POST http://localhost:8080/webhook/mytoken \
  -H "Content-Type: application/json" \
  -d '{"text": "hello"}'
```

---

## Logs

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/logs` | SSE live log stream |

```bash
curl -N http://localhost:8080/api/logs \
  -H "Authorization: Bearer secret"
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
