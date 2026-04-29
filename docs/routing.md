# Routing & Pipeline

## Contents

- [Philosophy](#philosophy)
- [Trigger-word routing](#trigger-word-routing)
- [Category routing](#category-routing)
- [router_hint](#router_hint)
- [receive_handler_context](#receive_handler_context)
- [Inject mechanism](#inject-mechanism)
- [Router plugin pattern](#router-plugin-pattern)
- [Multi-agent future](#multi-agent-future)

---

## Philosophy

SIJBA uses a layered routing strategy from most to least deterministic:

```
1. Trigger-word handlers  (deterministic, zero LLM)
2. Category routing       (metadata-based)
3. Router LLM             (fallback — reformulates → trigger command)
4. General LLM            (catch-all conversation)
```

The LLM never executes plugin actions directly. At worst it reformulates a message
into a trigger command — the actual work is always done by the deterministic plugin.

---

## Trigger-word routing

Each handler declares a `match.regex`. Messages that don't match simply pass to the next handler. No routing logic needed.

```json
"match": { "regex": "(?i)^iobroker\\b" }
```

**Pros:** Deterministic, auditable, zero hallucination risk.  
**Use for:** All action-taking plugins (device control, system commands, etc.)

---

## Category routing

Handlers can declare a category:
```json
"category": ["smarthome"]
```

Respond targets and inject targets can reference categories:
```json
"respond": { "text": "...", "target": "smarthome" }
"inject":  { "text": "...", "target": "smarthome" }
```

This routes to all handlers with that category without hardcoding handler IDs.

---

## router_hint

Every handler that should be discoverable by the router LLM adds a `router_hint`:

```json
"router_hint": "Controls smart home devices via ioBroker. Trigger: start with 'iobroker'. Examples: 'iobroker turn on bubble', 'iobroker get temperature'."
```

A good `router_hint` contains:
- What the plugin does (one sentence)
- The exact trigger prefix
- 2–3 natural language examples

---

## receive_handler_context

When a handler sets `"receive_handler_context": true`, core injects a `handlers`
field into the envelope before invoking the plugin:

```json
"handlers": [
  { "id": "iobroker", "router_hint": "Controls smart home devices..." },
  { "id": "llm",      "router_hint": "General-purpose assistant..." }
]
```

Only handlers that have a `router_hint` are included. The plugin uses this list to
understand the full capability landscape and make routing decisions.

---

## Inject mechanism

Any plugin can return an `inject` field to re-queue a message into the inbound pipeline:

```json
{
  "continue": false,
  "inject": { "text": "iobroker set Bubble true" }
}
```

The injected message starts a fresh pipeline run with `depth` incremented.
`max_inject_depth` (default: 5, set in `core.yaml`) prevents infinite loops.

**Direct inject** — bypass the full pipeline, go straight to one handler:
```json
"inject": { "text": "...", "target": "iobroker" }
```
Requires `"allow_direct_inject": true` on the target handler.

---

## Router plugin pattern

The router plugin (`router/main.py`) sits between trigger-word handlers and the general LLM:

```
priority 0    audit     (always runs, logs)
priority 20   iobroker  (matches "iobroker ...")
priority 40   router    (LLM fallback — catches natural language commands)
priority 50   llm       (general conversation catch-all)
```

Flow:
1. Receives messages no earlier handler matched
2. Gets all `router_hint` fields via `receive_handler_context: true`
3. Calls a cheap/fast LLM (use a non-reasoning model like `gpt-4.1-nano`)
4. If response starts with a known handler ID → injects back into the pipeline
5. Otherwise → `continue: true` → falls through to the LLM at priority 50

```
user: "mach die blasen an"
  → iobroker (no match — no "iobroker" prefix)
  → router: injects "iobroker set Bubble true"
  → iobroker: matches, executes
  → responds: "Bubble set to on"
```

---

## Multi-agent future

The router pattern scales to multi-agent setups:

- Router detects intent → injects with category or handler ID
- Each "agent" is an LLM handler with its own system prompt, model, and context store
- `"category": ["coding"]`, `"category": ["smarthome"]`, etc.
- Router stays cheap (fast/small model), specialists use powerful models

The inject mechanism is the only primitive needed — no framework required.

See [plugins.md → router/main.py](plugins.md#routermainpy) for configuration details.
