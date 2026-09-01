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
| `GET` | `/api/version` | Build version + git commit (no auth) |
| `GET` | `/api/handlers` | List all handlers with their states |
| `GET` | `/api/webhooks` | Configured webhooks as `{name, source}` (never the tokens) |
| `POST` | `/api/handlers/{id}/toggle` | Enable / disable a handler |
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

Registrations are persisted and reloaded when Core restarts, so a remote handler stays attached across a Core rollout without waiting for its next heartbeat — see [architecture.md](architecture.md#delivery-guarantees). Prefer a stable `url` (a Service DNS name over a pod IP) so the restored registration still points somewhere. One whose TTL expired while Core was down is dropped and has to register again.

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
| `source` | Defaults to `web`. Set e.g. `webhook:voice` to have the message routed and answered exactly like a real `POST /webhook/<token>` call — the app uses this for its per-webhook composer. |

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

Optional body fields:

| Field | Effect |
|-------|--------|
| `text` / `message` | The message. |
| `chat_id` | Records the exchange in that transcript, which is how the reply becomes visible in the app. Without it the message is not stored. |
| `target` | Handler id, category, or plugin channel — skips routing and goes straight there. |

#### multipart/form-data (Pebble Index)

The endpoint also accepts `multipart/form-data` / `application/x-www-form-urlencoded`,
so a [Pebble Index 01](https://help.repebble.com/en/articles/15724406-index-advanced-features-mcp-webhook)
ring can post straight to it — point its webhook URL at `/webhook/<token>` and
send **transcription only**:

| Part | Used as |
|------|---------|
| `transcription` (or `text` / `message`) | the message — **required** (400 otherwise) |
| `recordedAt` | `source_meta.recorded_at` (ms since epoch) |
| `client` | `source_meta.client` (e.g. `ring`) |
| `chat_id` | transcript bucket; if absent (Pebble can't set it) every note from this webhook threads under the **source name** instead of the shared `web` bucket |
| `target` | as above |
| `audio` | ignored |

```bash
curl -X POST http://localhost:8080/webhook/$VOICE_WEBHOOK_TOKEN \
  -F 'transcription=Einkauf Milch und Butter' \
  -F 'recordedAt=1730000000000' -F 'client=ring'
```

### Named webhooks

Configure one token per caller in `core.yaml`. The token decides the envelope
source, so each webhook can be routed — and answered — differently, and a
caller cannot relabel itself:

```yaml
webhooks:
  voice: ${VOICE_WEBHOOK_TOKEN}
  iot:   ${IOT_WEBHOOK_TOKEN}
```

Token values expand `${VAR}` from the environment, so the names can live in a
ConfigMap while the secrets stay in env. An unresolved variable is dropped
rather than registered verbatim.

`POST /webhook/<token>` then produces source `webhook:voice` or `webhook:iot`.
The prefix is always applied, so a webhook can never become `web`, `app` or
`telegram`. The single legacy `webhook_token` still works and stays `webhook`.

```bash
curl -X POST http://localhost:8080/webhook/$VOICE_WEBHOOK_TOKEN \
  -H "Content-Type: application/json" \
  -d '{"text": "mach das licht im bad an", "chat_id": "voice"}'
# → {"id": "...", "status": "queued", "source": "webhook:voice"}
```

Route and answer it with handlers scoped to that source:

```
90.0-router-voice.json  inbound   match: { "source": ["webhook:voice"] }
95.0-pushover.json      outbound  match: { "source": ["webhook:voice"] }
```

---

## Logs & replies

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/logs` | SSE live log stream |
| `GET` | `/api/replies` | SSE outbound replies for `web` / `app` sources |
| `GET` | `/api/transcript` | Recent messages for a `chat_id` + `channel` (bounded) |

SSE clients that cannot set `Authorization` may pass `?token=<auth_token>`.

`/api/replies` events: `{"type": "reply" | "reply_chunk"}` (bot output),
`{"type": "user_message"}` (an inbound message from an entry point with no
client of its own, e.g. a `POST /webhook/{token}` call — pushed so it shows up
live), `{"type": "delivery_failed"}`, `{"type": "channel_status"}`. Events for
web/app/webhook sources carry `"in_pipeline": true` — they also belong in the
cross-source `pipeline` transcript bucket, even when `channel` names a plugin
channel the message routed through.

```bash
curl -N http://localhost:8080/api/logs \
  -H "Authorization: Bearer secret"

curl -N "http://localhost:8080/api/replies?token=secret"
```

### Transcript

Stored in SQLite (same DB as queues). Caps: **200 messages per (chat_id, channel)**; API `limit` default 100, max 200.

```bash
curl "http://localhost:8080/api/transcript?chat_id=web&channel=pipeline&limit=50" \
  -H "Authorization: Bearer secret"
```

`channel` is `pipeline` for the full inbound chain, or a handler id for direct-target chats.

`chat_id=pipeline&channel=pipeline` is the **cross-source activity stream**: every
depth-0 web/app/webhook message and its reply, mirrored here regardless of which
`chat_id` or plugin channel it actually threaded under. Backs the app's Pipeline
and per-webhook tiles.

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
