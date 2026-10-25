# sijba

Simple JSON Bus Assistant — a self-hosted message pipeline and bot framework.

Messages enter via **Sources** (Telegram, Webhook, Web UI), travel through a priority-ordered chain of **Handlers**, and exit via **Sinks** (Telegram out, Webhook out, etc.). Everything is controlled via JSON config files. Git-versionable. No GUI required
but has a simple one.

I didnt like the typical LLM agents being a bit unpredictable. This Approach
with hardcoded & trigger word based plugins first makes sure that my
commands for various parts are actually done as i want them.
If more complicated parts are sent it will be handled first by LLM based
routing and then if needed send to normal LLM plugins..

---

## Quick start

```bash
pip install -r requirements.txt

mkdir -p sijba-config/handlers
cp examples/handlers/0.0-audit.json sijba-config/handlers/
cp examples/handlers/99.0-echo.json sijba-config/handlers/

SIJBA_CONFIG_DIR=./sijba-config python -m core.main
```

Open `http://localhost:8080` for the web UI.

---

## Config directory (`sijba-config/`)

```
sijba-config/
├── core.yaml          # main config (optional, all fields have defaults)
├── handlers/          # one JSON file per handler, filename prefix = priority
│   ├── 0.0-audit.json
│   └── 99.0-echo.json
├── plugins/           # your custom plugins (any language)
├── prompts/           # prompt files for LLM handlers
└── secrets/           # .gitignore this
```

**`core.yaml` options:**

| Key | Default | Description |
|-----|---------|-------------|
| `port` | `8080` | HTTP port |
| `auth_token` | *(none)* | Bearer token for API + Web UI |
| `webhook_token` | *(none)* | Token in `/webhook/{token}` URL |
| `max_inject_depth` | `5` | Max recursive inject depth |
| `log_level` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |
| `db_path` | `sijba.db` | SQLite path (relative to config dir) |
| `runtime` | `docker` | `docker` / `kubernetes` / `none` — container management for docker-type handlers |

All options can also be set via env vars: `SIJBA_AUTH_TOKEN`, `SIJBA_WEBHOOK_TOKEN`, `SIJBA_PORT`, `SIJBA_DB_PATH`, `SIJBA_LOG_LEVEL`.

---

## Handler config

One `.json` file per handler in `handlers/`. Filename prefix sets the sort priority (`10.0-foo.json` → priority 10.0).

```json
{
  "id": "my-handler",
  "priority": 10.0,
  "direction": "inbound",
  "lifecycle": "per_message",
  "type": "exec_json",
  "exec": "./plugins/my_plugin.py",
  "match": { "regex": "(?i)^hello" },
  "env": { "MY_KEY": "${MY_ENV_VAR}" }
}
```

Key fields: `direction` (`inbound`/`outbound`/`source`), `lifecycle` (`per_message`/`persistent`), `type` (`exec_json`/`http`).

See `schema/handler.schema.json` for the full schema and `examples/handlers/` for ready-to-use configs.

---

## Plugin types

| Type | Lifecycle | Description |
|------|-----------|-------------|
| `exec_json` | per_message | JSON envelope via stdin, JSON response via stdout. |
| `exec_json` | persistent | Long-running process, NDJSON over stdin/stdout. Also used by `source` handlers. |
| `http` | per_message | Core POSTs the envelope to a remote URL. Used by `/api/register` (see [api.md](docs/api.md)). |

---

## Built-in plugins

Located in `plugins-builtin/` — reference with `{sijba_plugins}/...` in handler configs.

| Plugin | Description |
|--------|-------------|
| `audit.sh` | Logs every message, continues pipeline |
| `echo.py` | Replies with `echo: <text>`, stops pipeline |
| `telegram.py` | Combined Telegram source + outbound in one process |
| `llm/main.py` | Multi-provider LLM with per-chat conversation memory |
| `iobroker/main.py` | ioBroker smart home control via REST API |

### Telegram

Copy `examples/handlers/10.0-telegram-source.json` and `10.1-telegram-out.json` to your `handlers/` dir.

```json
"env": { "TELEGRAM_BOT_TOKEN": "${TELEGRAM_BOT_TOKEN}" }
```

### LLM

Copy `examples/handlers/50.0-llm.json` to your `handlers/` dir. Supports any OpenAI-compatible API plus Anthropic natively.

```json
"env": {
  "LLM_API_URL": "https://api.openai.com/v1",
  "LLM_API_KEY": "${OPENAI_API_KEY}",
  "LLM_MODEL": "gpt-4o-mini",
  "LLM_SYSTEM_PROMPT": "You are a helpful assistant."
}
```

For Ollama: `"LLM_API_URL": "http://localhost:11434/v1"` — no key needed.  
For Anthropic: add `"LLM_FORMAT": "anthropic"`.

Conversation history is stored automatically per `chat_id` in the context store.

### ioBroker

Copy `examples/handlers/20.0-iobroker.json` to your `handlers/` dir. Requires the ioBroker REST API adapter.

```json
"env": {
  "IOBROKER_URL": "http://your-iobroker:8081",
  "IOBROKER_CACHE_FILE": "/tmp/iobroker_cache.json",
  "IOBROKER_CACHE_TTL": "3600"
}
```

Commands (prefix with `iobroker`):

| Command | Example |
|---------|---------|
| `list` | `iobroker list` |
| `reload` | `iobroker reload` |
| `get <device>` | `iobroker get living room light` |
| `set <device> <value>` | `iobroker set thermostat 21` |
| `turn on/off <device>` | `iobroker turn on kitchen lamp` |
| `toggle <device>` | `iobroker toggle fan` |
| `<device> on/off` | `iobroker bedroom light off` |

The plugin keeps a local JSON device cache (refreshed every TTL seconds or when a device is not found).

---

## Conversation store

Persistent SQLite-backed key/value store, namespaced per handler. Plugins don't access it directly — the core enriches the envelope with context before invoke and saves `context_update` from the response.

Handler opt-in:
```json
"context": { "namespace": "llm" }
```

Manage via API:
```bash
# inspect conversation history for a chat
curl http://localhost:8080/api/store/llm/123456789 -H "Authorization: Bearer secret"

# reset conversation
curl -X DELETE http://localhost:8080/api/store/llm/123456789 -H "Authorization: Bearer secret"
```

---

## API

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | Web UI |
| `GET` | `/api/handlers` | List handlers and their states |
| `POST` | `/api/handlers/{id}/toggle` | Enable / disable a handler |
| `GET` | `/api/logs` | SSE live log stream |
| `GET` | `/api/queue` | Queue status counts |
| `POST` | `/api/message` | Inject a message (`target` optional → one handler; omit → full pipeline) |
| `GET` | `/api/replies` | SSE replies for `web` / `app` sources |
| `POST` | `/api/register` | Register a remote HTTP handler / pipeline |
| `POST` | `/api/register/{id}/heartbeat` | Refresh registration TTL |
| `DELETE` | `/api/register/{id}` | Unregister remote handler |
| `POST` | `/webhook/{token}` | External webhook → inbound pipeline |
| `GET` | `/api/store/{namespace}` | List context store keys |
| `GET` | `/api/store/{namespace}/{key}` | Get store value |
| `PUT` | `/api/store/{namespace}/{key}` | Set store value |
| `DELETE` | `/api/store/{namespace}/{key}` | Delete store entry |
| `DELETE` | `/api/store/{namespace}` | Clear namespace |

---

## Docker

```bash
docker build -t sijba .
docker run -p 8080:8080 \
  -v $(pwd)/sijba-config:/config \
  -v /var/run/docker.sock:/var/run/docker.sock \
  sijba
```

The Docker socket mount is needed when using `runtime: docker` to let SIJBA manage plugin containers. See `docker-compose.yml` for a full compose template (place it in your `sijba-config` repo).

