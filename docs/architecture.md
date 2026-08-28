# SIJBA Architecture

## Contents

- [Overview](#overview)
- [Pipeline flow](#pipeline-flow)
- [Envelope structure](#envelope-structure)
- [Routing strategies](#routing-strategies)
- [Inject and depth](#inject-and-depth)
- [Delivery guarantees](#delivery-guarantees)

---

## Overview

Messages enter via **Sources**, travel through a priority-ordered chain of **Handlers**, and exit via **Sinks** (outbound handlers). Everything is driven by JSON config files — no GUI required, git-versionable.

```
Source (Telegram / Webhook / Web UI)
  ↓
Inbound Queue
  ↓
Handler 1 (priority 0.0)   ← audit, logging
Handler 2 (priority 10.0)  ← telegram source / outbound
Handler 3 (priority 20.0)  ← iobroker  (trigger: "iobroker ...")
Handler N (priority 40.0)  ← router    (LLM fallback)
Handler N (priority 50.0)  ← llm
  ↓
Outbound Queue
  ↓
Sink (Telegram out / Webhook out)
```

---

## Pipeline flow

1. A source (Telegram bot, webhook POST, web UI inject) places an envelope on the **inbound queue**.
2. The inbound worker dequeues it and runs `process_inbound()`.
3. Handlers are iterated in priority order. Each handler can:
   - `continue: true` — pass to next handler
   - `continue: false` — stop the pipeline
   - `respond` — queue an outbound message
   - `inject` — re-queue a new inbound message (with depth tracking)
   - `pipeline_text` — replace the working text for downstream handlers
   - `context_update` — persist state to the conversation store
4. Outbound handlers deliver responses back to the original source.

See [handlers.md](handlers.md) for the full response format reference.

---

## Envelope structure

```json
{
  "id": "uuid",
  "parent_id": "uuid or null",
  "direction": "inbound",
  "source": "telegram",
  "source_meta": { "chat_id": 123 },
  "text": "original text",
  "pipeline_text": "working text (handlers may modify)",
  "depth": 0,
  "metadata": {},
  "context": {},      // injected by core if handler has context config
  "handlers": []      // injected by core if receive_handler_context: true
}
```

---

## Routing strategies

### Trigger-word routing (preferred)
Each handler defines a `match.regex`. Messages that don't match simply pass through to the next handler. Deterministic — no LLM involved.

### Category routing
Handlers declare `"category": ["smarthome"]`. Outbound respond targets and inject targets can reference categories instead of handler IDs.

### Router plugin (fallback)
A catch-all LLM handler at high priority receives messages no other handler claimed. It reads all `router_hint` fields from active handlers (via `receive_handler_context: true`) and either:
- Routes back to a specific handler by injecting the canonical trigger command
- Passes through to the next handler (e.g. a general LLM) if no route found

See [routing.md](routing.md) for the full routing guide.

---

## Inject and depth

Plugins can return `{"inject": {"text": "..."}}` to re-queue a message into the inbound pipeline. `max_inject_depth` (default: 5, set in `core.yaml`) prevents loops.

```json
{ "inject": { "text": "iobroker set Bubble true" } }
```

Direct inject — bypass the full pipeline, go straight to one handler:
```json
{ "inject": { "text": "...", "target": "iobroker" } }
```
Requires `"allow_direct_inject": true` on the target handler.

---

## Delivery guarantees

Restarts are expected to be harmless, which takes three things:

**Registrations survive a restart.** Remote handlers attached via `POST /api/register` (sijba-out-pushover, sijba-kubectl) are mirrored into SQLite and reloaded at startup, with their TTL. Without this they only came back once their own heartbeat failed and re-registered — a window of up to `ttl_seconds / 6` during which outbound messages found no handler at all. A registration whose TTL ran out while Core was down is dropped: its owner has to register again.

**An outbound message nobody delivered goes back on the queue.** `process_outbound` counts how many handlers actually took the message — a handler that could not be invoked (dead remote, timeout, garbage response) does not count. If nothing took it, the message is deferred with a backoff (`5s, 15s, 60s, 300s`) rather than being marked `done`. After `MAX_ATTEMPTS` or an hour it is marked `failed` with the reason in `last_error`, and logged. It never spins forever, and it never disappears quietly.

The exception is sources that carry their own return path: `web`, `app`, and `internal` replies reach the app over the SSE/WebSocket reply stream, so having no outbound handler is the normal case for them and is not retried. An explicitly named `respond.target` is always expected to exist, whatever the source.

**Messages orphaned mid-flight are reclaimed.** A row left `claimed` by a process that died is requeued at startup — nothing else ever looks at `claimed` rows.
