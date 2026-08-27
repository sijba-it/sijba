# SIJBA — Simple JSON Bus Assistant
## Complete Design Specification

> **Historical document.** This is the original design, not a description of
> the current implementation. Parts of it were never built (`type: docker`)
> and parts were built and later removed as unused (`exec_raw`,
> `capture_stdout`, `stdin` templating, `privileged`, persistent `type: http`).
> For what the core actually does today, see [docs/](docs/) and
> [schema/handler.schema.json](schema/handler.schema.json).

---

## Overview

SIJBA is a self-hosted message pipeline / bot framework.
Messages enter via Sources (Telegram, Webhook, Web UI), travel through
a priority-ordered chain of Handlers, and exit via Sinks (Telegram out,
Webhook out, etc.). Everything is controlled via JSON config files.
No GUI required. Git-versionable config.

---

## Repository Structure

Two separate repos:

### Repo 1: `sijba` (Core — can be public)
sijba/
├── core/
│   ├── main.py              # Entrypoint, FastAPI app
│   ├── config.py            # Loads core.yaml + env vars
│   ├── handler_loader.py    # Scans handlers/, validates JSON Schema
│   ├── pipeline.py          # Inbound/outbound chain execution
│   ├── plugin_runner.py     # per_message + persistent, all transports
│   ├── lifecycle.py         # Watchdog, startup, restart_policy
│   ├── queue.py             # SQLite inbound/outbound queues
│   ├── store.py             # SQLite context/conversation store
│   └── web/
│       ├── api.py           # REST endpoints
│       └── ui.html          # Single-file Web UI
├── plugins-builtin/
│   ├── audit.sh
│   ├── echo.py
│   ├── telegram.py          # Combined source + outbound in one process
│   └── llm/
│       └── main.py          # Multi-provider LLM plugin
├── schema/
│   └── handler.schema.json
├── examples/
│   └── handlers/            # Ready-to-use handler configs
├── Dockerfile
├── docker-compose.yml       # Expects CONFIG_DIR as volume mount
└── README.md

### Repo 2: `sijba-config` (Instance config — private)
sijba-config/
├── core.yaml
├── handlers/
│   ├── 0.0-audit.json
│   ├── 1.0-blocklist.json
│   ├── 10.0-telegram-source.json
│   ├── 10.1-telegram-out.json
│   ├── 50.0-llm.json
│   └── 90.0-webhook-out.json
├── plugins/                 # custom plugins (any language)
├── prompts/
│   └── llm_default.md
└── secrets/                 # .gitignore'd
    ├── telegram.env
    └── tokens.env

Core reads config path from env var:
SIJBA_CONFIG_DIR=/path/to/sijba-config

---

## core.yaml Options

```yaml
port: 8080
auth_token: null            # Bearer token for API + Web UI (null = no auth)
webhook_token: null         # Token in /webhook/{token} URL
max_inject_depth: 5         # Max recursive inject depth
log_level: INFO             # DEBUG / INFO / WARNING / ERROR
db_path: sijba.db            # SQLite path (relative to config dir)
runtime: docker             # docker | kubernetes | none  (see Runtime section)
```

All options can be overridden with env vars:
`SIJBA_AUTH_TOKEN`, `SIJBA_WEBHOOK_TOKEN`, `SIJBA_PORT`, `SIJBA_DB_PATH`, `SIJBA_LOG_LEVEL`

---

## Message Envelope

Every message in the system is wrapped in this envelope:

```json
{
  "id": "uuid",
  "parent_id": "uuid|null",
  "direction": "inbound|outbound",
  "source": "telegram|webhook|web|internal",
  "source_meta": {
    "chat_id": "123",
    "user": "robert"
  },
  "text": "original unmodified text",
  "pipeline_text": "current text (can be modified by handlers)",
  "depth": 0,
  "metadata": {},
  "context": {}              # injected by core if handler has context config
}
```

---

## Handler Config Schema

One `handlers/` directory. Filename prefix = priority for sorting.
All handler fields:

```json
{
  "id": "string (unique)",
  "priority": 10.5,
  "direction": "inbound|outbound|source",
  "category": ["smarthome", "actuator"],
  "lifecycle": "per_message|persistent",
  "type": "exec_json|exec_raw|http|docker",
  "required": false,
  "await_response": true,
  "allow_direct_inject": false,
  "continue_on_match": true,
  "match": {
    "regex": "(?i)^(licht|schalte)",
    "source": ["telegram", "web"]
  },
  "exec": "./plugins/smarthome.py",
  "args": [],
  "stdin": "{{pipeline_text}}",
  "capture_stdout": false,
  "timeout_seconds": 10,
  "env": {
    "API_KEY": "${MY_API_KEY}"
  },
  "context": {
    "namespace": "llm",
    "key": "chat_id"
  },
  "shared_process": "other-handler-id",
  "startup": {
    "url": "http://localhost:8765",
    "invoke_path": "/invoke",
    "health_path": "/health",
    "socket_path": "/tmp/sijba/plugin.sock",
    "startup_timeout_seconds": 30,
    "restart_policy": "always|on_failure|never"
  },
  "docker": {
    "image": "ghcr.io/sijba-plugins/llm:latest",
    "port": 8765,
    "env_file": "/secrets/openrouter.env",
    "volumes": ["/config/prompts:/prompts:ro"]
  },
  "kubernetes": {
    "image": "ghcr.io/sijba-plugins/llm:latest",
    "namespace": "sijba",
    "service_port": 8765,
    "env_secret": "openrouter-secret",
    "volume_mounts": [
      {"config_map": "sijba-prompts", "mount_path": "/prompts"}
    ]
  }
}
```

Float priority allows inserting handlers between existing ones without
renaming (e.g. 10.5 sits between 10.0 and 11.0).

---

## Plugin Types

| Type | Lifecycle | Description |
|------|-----------|-------------|
| `exec_raw` | per_message | Any binary. No protocol. Exit 0 = continue, exit 1 = stop. |
| `exec_json` | per_message | Spawn per message. JSON envelope via stdin, JSON response via stdout. |
| `http` | persistent | Long-running process, reachable via HTTP or unix socket. |
| `docker` | persistent | SIJBA starts/stops the container. Communication via `type: http`. |

### `type: exec_json` (per_message)
Spawn process per message. JSON in via stdin, JSON out via stdout.

```bash
#!/usr/bin/env bash
INPUT=$(cat)
TEXT=$(echo "$INPUT" | jq -r '.pipeline_text')
echo '{"continue": true, "metadata": {"logged": true}}'
```

### `type: exec_raw`
Any Linux binary. No protocol knowledge required.
Exit 0 = continue, exit 1 = stop pipeline.
`capture_stdout: true` copies stdout to `pipeline_text`.

```json
{
  "type": "exec_raw",
  "exec": "notify-send",
  "args": ["{{pipeline_text}}"],
  "await_response": false
}
```

### `type: http` (persistent)
Long-running process reachable via HTTP or unix socket.
Core POSTs envelope JSON, expects plugin response JSON back.

### `type: docker` (persistent)
SIJBA starts and stops the container itself via the configured runtime.
Communication still runs via `type: http` — the `docker` block just
describes how to launch the container.

```json
{
  "id": "llm",
  "lifecycle": "persistent",
  "type": "http",
  "docker": {
    "image": "ghcr.io/sijba-plugins/llm:latest",
    "port": 8765,
    "env_file": "/secrets/openrouter.env",
    "volumes": ["/config/prompts:/prompts:ro"]
  },
  "startup": {
    "url": "http://localhost:8765",
    "health_path": "/health",
    "startup_timeout_seconds": 60,
    "restart_policy": "on_failure"
  }
}
```

---

## Plugin Lifecycle

### `lifecycle: per_message`
- Spawned fresh for each message
- stdin/stdout, then exits
- Stateless
- Good for: bash snippets, curl calls, simple transforms, LLM calls

### `lifecycle: persistent`
- Started once at core startup
- Stays running, handles multiple messages
- Transports: `stdio` (NDJSON), `http`, `unix_socket`
- Good for: Telegram bot (long polling), DB connections, docker-managed services
- Monitored by watchdog, restarted per `restart_policy`

NDJSON protocol for persistent stdio (requires req_id correlation):
```
Core → Plugin:   {"req_id": "uuid", "message": { ...envelope... }}\n
Plugin → Core:   {"req_id": "uuid", "continue": true, ...}\n
```

### Combined source + outbound (single process)
A `direction: source` handler can also accept NDJSON invocations on its stdin.
Core's stdout reader dispatches lines with `req_id` as NDJSON responses and
lines without as source events.

A second handler with `"shared_process": "<source-handler-id>"` reuses the
running process for outbound invocations — no second process is started.

---

## Plugin Response Protocol

```json
{
  "continue": true,
  "pipeline_text": "optionally modified text",

  "respond": {
    "text": "reply to user",
    "target": "source"
  },

  "inject": {
    "text": "new inbound event text",
    "target": "smarthome",
    "metadata": {}
  },

  "context_update": {
    "history": [...]
  }
}
```

- `continue: false` — stops the pipeline at this handler
- `pipeline_text` — replaces current pipeline_text for subsequent handlers
- `respond` — puts message into outbound queue → outbound pipeline → sink
- `inject` — puts new message into inbound queue with depth+1
- `context_update` — saved back to the context store under the handler's namespace

### `await_response: false` (fire-and-forget)
- Handler is called but pipeline does not wait for response
- `pipeline_text` cannot be modified (race condition)
- `continue`/`respond`/`inject`/`context_update` from this handler are ignored
- Pipeline always continues past this handler

### `continue_on_match`
Default value for `continue` when the plugin response omits it.
Set `"continue_on_match": false` on a blocklist/gate handler to stop the
pipeline on match without requiring the plugin to return `{"continue": false}`.

---

## Context Store

Persistent per-namespace key/value store backed by the same SQLite database.

Handler opt-in via `context` config field:

```json
"context": {
  "namespace": "llm",
  "key": "chat_id"
}
```

Before invoking the plugin, core:
1. Resolves the key: reads `source_meta[key]` from the envelope (default key field: `chat_id`)
2. Loads `store[namespace][key]` and adds it to the envelope as `context`

Plugin returns `context_update` → core saves it back.

Plugins never call the store directly; the core mediates all access.

REST access (for inspection / management):

```
GET    /api/store/{namespace}          list all keys in namespace
GET    /api/store/{namespace}/{key}    get value
PUT    /api/store/{namespace}/{key}    set value
DELETE /api/store/{namespace}/{key}    delete one entry
DELETE /api/store/{namespace}          clear entire namespace
```

---

## Inject Target Resolution

`inject.target` is a single string. Resolver logic:

1. Exact match on `handler.id` → direct to that handler, bypasses pipeline
2. Match on `handler.category[]` → all handlers with that category,
   respects priority order, filtered to that category
3. No match → log error, discard inject

ID takes precedence over category if names collide.

Direct handler-ID inject requires `allow_direct_inject: true` on the
target handler (opt-in). Category inject does not require opt-in.

No `target` = normal full inbound pipeline from priority 0.

---

## Outbound Routing

`respond.target` controls which outbound handlers receive the response:

- `"source"` (default) — handlers whose `match.source` includes the originating source
- handler ID — only that specific handler
- category — all handlers with that category

---

## Handler States
```
pending    → not yet started
starting   → waiting for health/ready signal
healthy    → running normally
degraded   → required:false, failed to start or crashed
restarting → watchdog is restarting it
stopped    → manually disabled via API or Web UI
```

### Startup behavior
- `required: true` → core blocks on startup until handler is healthy
  or startup_timeout_seconds exceeded → then core exits with error
- `required: false` → if startup fails, handler state = degraded,
  core continues without it

---

## Pipeline Execution

### Inbound pipeline
```
for handler in sorted(inbound_handlers, by=priority):
  if handler.state != healthy: skip
  if not matches(handler.match, envelope): continue
  if handler.await_response == false:
    fire_and_forget(handler, envelope)
    continue
  enrich envelope with context if handler.context configured
  response = plugin_runner.invoke(handler, envelope)
  save response.context_update if present
  if response.inject:
    resolve target
    enqueue_inbound(child_envelope, depth+1)
  if response.respond:
    enqueue_outbound(response.respond, target=envelope.source)
  if response.pipeline_text:
    envelope.pipeline_text = response.pipeline_text
  default_continue = handler.continue_on_match  # default: true
  if not response.get("continue", default_continue):
    break
```

### Outbound pipeline
Same structure, uses `direction: outbound` handlers.
Handler list is pre-filtered by `respond.target` before iteration.

### Depth limit
Configurable `max_inject_depth` in core.yaml (default: 5).
Exceeded → discard + log warning. Prevents inject loops.

---

## Queue

SQLite, two tables: `inbound_queue`, `outbound_queue`.

Fields: `id`, `envelope_json`, `status`, `depth`, `created_at`, `claimed_at`

Statuses: `pending` → `claimed` → `done` | `failed`

One asyncio worker per queue, continuous polling.

---

## Source Plugins

Sources are `direction: source`, `lifecycle: persistent`.
They run as long-running daemons and write one JSON line per message
to stdout → core reads via readline → enqueues to inbound queue.

Source output format (subset of envelope, core fills the rest):
```json
{"source": "telegram", "source_meta": {"chat_id": "123", "user": "robert"}, "text": "message text"}\n
```

A source plugin can also handle outbound sends in the same process.
Lines with `req_id` on stdout are treated as NDJSON responses (not source events).
A second handler with `shared_process` reuses the same process for outbound.

---

## Runtime

Configured in `core.yaml` as `runtime: docker | kubernetes | none`.

### `runtime: docker` (default)
Requires Docker socket mounted into the SIJBA container:
```yaml
services:
  sijba:
    image: sijba:latest
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - ./config:/config
```

When a handler has a `docker` block, SIJBA uses the Docker API to:
- Pull the image on first use
- Start the container with the specified env_file and volumes
- Stop the container when the handler is toggled off or SIJBA exits
- Communicate via the `startup.url` as with any `type: http` handler

### `runtime: kubernetes`
Requires in-cluster ServiceAccount with permission to create Pods and Services.

When a handler has a `kubernetes` block, SIJBA uses the Kubernetes API to:
- Deploy a Pod + Service in the specified namespace
- Communicate via the Service DNS name
- Delete the Pod/Service on stop

```json
{
  "kubernetes": {
    "image": "ghcr.io/sijba-plugins/llm:latest",
    "namespace": "sijba",
    "service_port": 8765,
    "env_secret": "openrouter-secret",
    "volume_mounts": [
      {"config_map": "sijba-prompts", "mount_path": "/prompts"}
    ]
  }
}
```

### `runtime: none`
No container management. Only built-in plugins and externally-started
HTTP services are available. `docker`/`kubernetes` blocks in handlers are ignored.

---

## Web API (FastAPI)

```
GET  /                          → Web UI (single HTML file)
GET  /api/handlers              → list all handlers + state
GET  /api/handlers/{id}         → get single handler config + state
POST /api/handlers/{id}/toggle  → enable/disable handler
POST /api/handlers/reload       → hot-reload handlers dir (SIGHUP equivalent)
POST /api/handlers/create       → create new handler JSON file + reload
PUT  /api/handlers/{id}         → update handler JSON file + reload
GET  /api/logs                  → SSE live log stream
GET  /api/queue                 → queue depth + status counts
POST /api/message               → inject message manually into inbound queue
POST /webhook/{token}           → external webhook → inbound queue
GET  /api/store/{namespace}     → list context store keys
GET  /api/store/{namespace}/{key}   → get store value
PUT  /api/store/{namespace}/{key}   → set store value
DELETE /api/store/{namespace}/{key} → delete store entry
DELETE /api/store/{namespace}       → clear namespace
POST /api/plugins/install       → deploy a plugin script to config dir
```

Auth: shared token in Authorization header for UI + API.
Webhook uses separate secret token in URL path.

---

## Plugin Config-Write API

Handlers can be granted write access to the SIJBA config via:

```json
{
  "id": "claude-agent",
  "allow_config_write": {
    "handlers": true,
    "plugins": false,
    "docker": false
  }
}
```

- `handlers` — CRUD on handler JSON files, auto-reload after write
- `plugins` — deploy script files to `plugins/` in config dir
- `docker` — pull new images for docker-type handlers

The plugin receives the SIJBA API token via the `SIJBA_API_TOKEN` environment
variable (injected by core alongside the handler's own `env` block).
All writes are scoped to the instance's `SIJBA_CONFIG_DIR`.

---

## Hot Reload

- SIGHUP or `POST /api/handlers/reload`
- Reloads `handlers/*.json`, re-sorts by priority
- `per_message` handlers: effective immediately
- `persistent` handlers: NOT restarted automatically on reload
  (destructive — would drop Telegram connection etc.)
- Persistent handler restart requires explicit toggle off/on via API
  or full core restart

---

## Deployment

### Docker (recommended)
```yaml
# in sijba-config repo
services:
  sijba:
    image: ghcr.io/yourname/sijba:latest
    volumes:
      - ./:/config
      - ./secrets:/secrets:ro
      - /var/run/docker.sock:/var/run/docker.sock  # needed for runtime: docker
    environment:
      - SIJBA_CONFIG_DIR=/config
      - SIJBA_SECRETS_DIR=/secrets
    ports:
      - "8080:8080"
    restart: unless-stopped
```

### systemd (bare metal)
```ini
[Service]
Environment=SIJBA_CONFIG_DIR=/opt/sijba-config
ExecStart=/opt/sijba/venv/bin/python -m core.main
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
```

---

## Workflow: Adding a Handler

```
Create handlers/20.5-new-thing.json in sijba-config/
Create plugins/new_thing.sh in sijba-config/
git commit + push
On server: git pull
POST /api/handlers/reload  (or kill -HUP)
No core restart needed (unless lifecycle: persistent)
```

## Workflow: Updating Core

```
docker pull (or git pull on core repo)
docker compose up -d
sijba-config repo untouched
```

## Workflow: Rotate Secrets

```
Edit secrets/telegram.env
scp / ansible to server
Core restart required (env vars)
```

---

## Core Tech Stack

- **Language**: Python 3.11+
- **Web framework**: FastAPI
- **Queue + Store**: SQLite (via stdlib sqlite3)
- **Config validation**: jsonschema
- **Web UI**: single HTML file, SSE for live logs, no framework

Plugins can be any language. No SDK required.
The simplest valid plugin is a bash one-liner.
Multi-file plugins live in a subdirectory with an entry point script.

---

## Built-in Plugins

| Plugin | Type | Description |
|--------|------|-------------|
| `audit.sh` | exec_json | Logs every message, continues pipeline |
| `echo.py` | exec_json | Replies with `echo: <text>`, stops pipeline |
| `telegram.py` | exec_json (source + shared outbound) | Combined Telegram long-poll + send in one process |
| `llm/main.py` | exec_json | Multi-provider LLM with conversation history |

### LLM Plugin (`plugins-builtin/llm/main.py`)

Zero external dependencies. Supports any OpenAI-compatible API plus Anthropic.

```
LLM_API_URL      Base URL: https://api.openai.com/v1
                           http://localhost:11434/v1  (Ollama)
                           https://api.groq.com/openai/v1
LLM_API_KEY      API key (not required for local models)
LLM_MODEL        Model name (e.g. gpt-4o-mini, llama3.2)
LLM_FORMAT       openai (default) | anthropic
LLM_SYSTEM_PROMPT  Text or path to .md file
LLM_MAX_HISTORY  Max conversation turns to keep (default: 20)
LLM_TEMPERATURE  0–2 (default: 0.7)
LLM_MAX_TOKENS   Max response tokens (default: 2048)
```

Conversation history is stored per `chat_id` in the context store
(namespace `llm`) and loaded automatically via the `context` handler config field.

---

## Open Questions / Decisions

1. Hot-reload: persistent handlers NOT auto-restarted on config reload — explicit toggle required.
2. Web UI auth: shared token in Authorization header. Webhook uses separate token in URL path.
3. Plugin Config-Write API: `allow_config_write` is opt-in per handler. Default all false.
4. Runtime: `docker` requires Docker socket mount. `kubernetes` requires in-cluster ServiceAccount.
