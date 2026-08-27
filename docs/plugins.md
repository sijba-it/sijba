# Built-in Plugins

Located in `plugins-builtin/`. Reference with `{sijba_plugins}/...` in handler configs.

## Contents

- [audit.sh](#auditsh)
- [echo.py](#echopy)
- [telegram.py](#telegrampy)
- [llm/main.py](#llmmainpy)
- [iobroker/main.py](#iobrokermainpy)
- [router/main.py](#routermainpy)

---

## audit.sh

Logs every message and continues the pipeline. Place at priority 0 — always runs first.

```json
{
  "id": "audit",
  "priority": 0.0,
  "direction": "inbound",
  "lifecycle": "per_message",
  "type": "exec_json",
  "await_response": false,
  "exec": "{sijba_plugins}/audit.sh"
}
```

---

## echo.py

Replies with `echo: <text>` and stops the pipeline. Useful for testing.

```json
{
  "id": "echo",
  "priority": 99.0,
  "direction": "inbound",
  "lifecycle": "per_message",
  "type": "exec_json",
  "exec": "{sijba_plugins}/echo.py",
  "match": { "regex": "(?i)^echo\\b" }
}
```

---

## telegram.py

Combined Telegram source + outbound sink in one persistent process. Two handlers share
one process via `shared_process`:

```json
// 10.0-telegram-source.json
{
  "id": "telegram-source",
  "direction": "source",
  "lifecycle": "persistent",
  "type": "exec_json",
  "exec": "{sijba_plugins}/telegram.py",
  "env": { "TELEGRAM_BOT_TOKEN": "${TELEGRAM_BOT_TOKEN}" },
  "startup": { "restart_policy": "on_failure" }
}

// 10.1-telegram-out.json
{
  "id": "telegram-out",
  "direction": "outbound",
  "lifecycle": "persistent",
  "type": "exec_json",
  "shared_process": "telegram-source",
  "await_response": true,
  "match": { "source": ["telegram"] }
}
```

---

## llm/main.py

Multi-provider LLM with per-chat conversation memory.

| Env var | Default | Description |
|---------|---------|-------------|
| `LLM_API_URL` | `https://api.openai.com/v1` | Full base URL (OpenAI, OpenRouter, Ollama, etc.) |
| `LLM_API_KEY` | — | API key (not needed for Ollama) |
| `LLM_MODEL` | `gpt-4o-mini` | Model name |
| `LLM_FORMAT` | `openai` | `openai` or `anthropic` |
| `LLM_SYSTEM_PROMPT` | — | System prompt text or path to a `.md`/`.txt` file |
| `LLM_MAX_HISTORY` | `20` | Max messages kept in conversation history |
| `LLM_TEMPERATURE` | `0.7` | Temperature 0–2 |
| `LLM_MAX_TOKENS` | `2048` | Max response tokens |

Requires `context` to persist conversation history per-chat:
```json
{ "context": { "namespace": "llm" } }
```

Add `router_hint` so the router can hand off to it:
```json
{ "router_hint": "General-purpose assistant. Handles questions, chat, summaries." }
```

---

## iobroker/main.py

Controls ioBroker smart home devices via the simple-api REST adapter (port 8087).

Devices are resolved from ioBroker's **enum** assignments (Admin UI → Enumerations →
Rooms / Functions). Only devices assigned to a room or function appear in `list` and
get priority in fuzzy search.

**Trigger:** messages starting with `iobroker`

| Command | Example |
|---------|---------|
| `iobroker list [room]` | `iobroker list Küche` |
| `iobroker reload` | Force refresh device cache |
| `iobroker get <device>` | `iobroker get Bubble` |
| `iobroker set <device> <value>` | `iobroker set Bubble true` |
| `iobroker turn on/off <device>` | `iobroker turn on Wohnzimmer Licht` |
| `iobroker toggle <device>` | `iobroker toggle fan` |
| `iobroker <device> on/off` | `iobroker Bubble on` |

**Values accepted:** `on`/`off`, `ein`/`aus`, `true`/`false`, `yes`/`no`, `1`/`0`, or numeric.  
Booleans are always sent as lowercase `true`/`false` to the ioBroker API.

| Env var | Default | Description |
|---------|---------|-------------|
| `IOBROKER_URL` | `http://iobroker.eb.localdomain:8087` | simple-api base URL |
| `IOBROKER_CACHE_FILE` | `/tmp/iobroker_cache.json` | Local device cache path |
| `IOBROKER_CACHE_TTL` | `3600` | Cache TTL in seconds |

**Note:** Uses read-before-write to avoid double-toggling on devices with toggle
semantics (e.g. Intex SPA where every write triggers a toggle regardless of value).

---

## router/main.py

LLM-based fallback router. Place between trigger-word handlers and the general LLM
(e.g. priority 40.0) to catch natural-language commands that no trigger matched.

Reads all `router_hint` fields from active handlers (via `receive_handler_context: true`)
and injects the canonical trigger command if the message matches a plugin's intent.
If no match, passes through (`continue: true`) to the next handler.

| Env var | Default | Description |
|---------|---------|-------------|
| `LLM_API_URL` | `https://api.openai.com/v1` | Base URL |
| `LLM_API_KEY` | — | API key |
| `LLM_MODEL` | `gpt-4o-mini` | Use a cheap, fast, non-reasoning model |
| `LLM_FORMAT` | `openai` | `openai` or `anthropic` |
| `LLM_TEMPERATURE` | `0.2` | Low — routing is a classification task |
| `LLM_MAX_TOKENS` | `64` | Trigger commands are short |

Requires `receive_handler_context: true` in the handler config.

```
user: "mach die blasen an"
  → iobroker handler: no match (no "iobroker" prefix)
  → router: LLM sees iobroker hint → injects "iobroker set Bubble true"
  → iobroker handler: matches, executes
  → responds: "Bubble set to on"
```

See [routing.md](routing.md) for the full routing guide.
