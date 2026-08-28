# Handler Configuration

## Contents

- [File naming and priority](#file-naming-and-priority)
- [Full schema](#full-schema)
- [Field reference](#field-reference)
- [match](#match)
- [Plugin response format](#plugin-response-format)
- [exec path resolution](#exec-path-resolution)

---

## File naming and priority

One `.json` file per handler in `handlers/`. The filename prefix sets the priority:

```
handlers/
  0.0-audit.json         → priority 0.0   (runs first)
  10.0-telegram-source.json
  10.1-telegram-out.json
  20.0-iobroker.json
  40.0-router-llm.json
  50.0-llm.json
  99.0-echo.json         → priority 99.0  (runs last)
```

Changes to files in `handlers/` take effect on the next Core restart (pod rollout or process restart). There is no hot-reload.

---

## Applying changes

Edit or add handler JSON under `handlers/`, then restart Core so the files are re-read at boot:

```bash
kubectl rollout restart statefulset/sijba   # k8s
# or restart the local process
```

This applies to `core.yaml` as well (port, auth tokens, webhooks, etc.) — every config change requires a restart.

---

## Full schema

```json
{
  "id": "my-handler",
  "priority": 10.0,
  "direction": "inbound",
  "lifecycle": "per_message",
  "type": "exec_json",
  "exec": "{sijba_plugins}/myplugin/main.py",

  "match": {
    "regex": "(?i)^trigger\\b",
    "source": ["telegram", "webhook"]
  },

  "continue_on_match": false,
  "required": false,
  "await_response": true,
  "allow_direct_inject": false,

  "router_hint": "What this handler does + example trigger phrases.",
  "receive_handler_context": false,

  "category": ["smarthome"],
  "context": {
    "namespace": "my-handler",
    "key": "chat_id"
  },
  "env": {
    "MY_KEY": "${MY_ENV_VAR}"
  },
  "timeout_seconds": 30,

  "args": [],
  "shared_process": "other-handler-id",
  "startup": {
    "url": "http://localhost:9000",
    "invoke_path": "/invoke",
    "restart_policy": "on_failure"
  }
}
```

---

## Field reference

| Field | Default | Description |
|-------|---------|-------------|
| `id` | required | Unique handler identifier |
| `priority` | from filename | Lower = runs first |
| `direction` | required | `inbound` / `outbound` / `source` |
| `lifecycle` | `per_message` | `per_message` / `persistent` |
| `type` | required | `exec_json` / `http` |
| `exec` | — | Path to executable. Use `{sijba_plugins}/...` for builtins |
| `continue_on_match` | `true` | Whether to continue pipeline after this handler matches |
| `required` | `false` | If true, pipeline stops on handler failure |
| `await_response` | `true` | If false, fire-and-forget |
| `allow_direct_inject` | `false` | Allow other handlers to inject directly to this one |
| `router_hint` | — | Description + examples for the router LLM (see [routing.md](routing.md)) |
| `receive_handler_context` | `false` | Inject all handlers' router_hints into the envelope |
| `category` | `[]` | Tags for routing (respond target / inject target) |
| `context.namespace` | handler id | Store namespace for conversation context |
| `context.key` | `chat_id` | Field from source_meta to use as store key |
| `env` | `{}` | Extra env vars passed to the process. `${VAR}` expands from host env |
| `timeout_seconds` | `10` | Per-invocation timeout |
| `shared_process` | — | Reuse another persistent handler's process |

---

## match

```json
"match": {
  "regex": "(?i)^iobroker\\b",
  "source": ["telegram"]
}
```

- `regex` — only invoke this handler if `pipeline_text` matches the pattern
- `source` — only invoke if the message source is in this list

Both conditions are AND-ed. Omit either to skip that check.

---

## Plugin response format

```json
{
  "continue": true,
  "pipeline_text": "modified text for downstream handlers",
  "respond": {
    "text": "reply to send",
    "target": "source"
  },
  "inject": {
    "text": "new message to re-queue",
    "target": "handler-id or category (optional)"
  },
  "context_update": {
    "key": "value"
  }
}
```

**`respond.target`**
- `"source"` — route to outbound handlers matching the original source
- `"handler-id"` — route to a specific outbound handler
- `"category-name"` — route to all outbound handlers with that category

**`inject.target`**
- omitted — re-queues through the full inbound pipeline
- `"handler-id"` — direct inject (requires `allow_direct_inject: true`)
- `"category-name"` — direct inject to all handlers with that category

See [routing.md](routing.md) for the full routing guide.

---

## exec path resolution

| Prefix | Resolved to |
|--------|-------------|
| `{sijba_plugins}/name` | `plugins-builtin/name` relative to the sijba install |
| `./plugins/name` | relative to `sijba-config/` |
| absolute path | used as-is |
