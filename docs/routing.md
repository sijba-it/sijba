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

### Plugin channels are addressed by @mention

Channels registered over the plugin WebSocket are matched on their `triggers`
instead, and **only as an explicit `@mention` at the start of the message**.
A bare word would hijack any sentence beginning with it — "Recherche zu Thema X"
is a message *about* research, not one addressed to the Recherche agent.

Triggers registered without an `@` are normalised to their `@` form rather than
ignored, so a plugin does not have to be redeployed in lockstep with core. A
channel that registers no triggers at all is still reachable as `@<channel_id>`.

This applies to message *text* only. Choosing a channel explicitly — the UI
sidebar, or `target` on `/api/message` and `/webhook/{token}` — matches on
`channel_id` and needs no mention:

```bash
# Same channel, two ways in
-d '{"text": "@Recherche fix the failing test"}'
-d '{"text": "fix the failing test", "target": "cursor-recherche"}'
```

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
  { "id": "iobroker",   "router_hint": "Controls smart home devices..." },
  { "id": "@projekt-a", "router_hint": "Cursor agent for project A" }
]
```

The list contains two kinds of target:

- **Handlers** with a `router_hint` (file-based and remotely registered ones).
- **Live plugin channels** from the WebSocket registry. These are listed under
  their `@` trigger rather than their channel id, because that is what a router
  has to emit — see [Router plugin pattern](#router-plugin-pattern).

The handler asking for the list is never offered itself, since routing to
itself would just loop until `max_inject_depth`.

Without the channels a router is blind to Cursor/Claude and can only ever
answer "no route", so a setup whose only real targets are plugin channels
would burn an LLM call per message for nothing.

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

An injected message runs as `source: "internal"` so `match.source` handlers
can't loop it back, but core stashes the original external source on it
(`_origin_source`). If the injected message ends up at a plugin channel, that
channel's eventual reply is delivered back out as the *original* source — so a
voice note that a router forwards to `@obsidian` still gets the agent's answer
pushed back over the `webhook:voice` return path (Pushover).

**Direct inject** — bypass the full pipeline, go straight to one handler:
```json
"inject": { "text": "...", "target": "iobroker" }
```
Requires `"allow_direct_inject": true` on the target handler.

---

## Router plugin pattern

The LLM router plugin (`llm-router/main.py`) sits between trigger-word handlers and the general LLM:

```
priority 0    audit         (always runs, logs)
priority 20   iobroker      (matches "iobroker ...")
priority 40   router-llm    (LLM fallback — catches natural language commands)
priority 50   llm           (general conversation catch-all)
```

For a deterministic first pass, put a [`regex-router`](../plugins-builtin/regex-router/README.md)
handler *before* the LLM router: it matches spoken keyword prefixes
("Einkauf ...", "Aufgabe ...") against a config-driven regex table and re-injects
a `@channel` message for free, with no LLM call. With `ON_MATCH_CONTINUE` it can
also run as a *tee* at the very top of the chain — mirroring every message to a
channel (e.g. a raw voice-note log) while letting the pipeline carry on to the
real routing.

Flow:
1. Receives messages no earlier handler matched
2. Gets all `router_hint` fields via `receive_handler_context: true`
3. Calls a cheap/fast LLM (use a non-reasoning model like `gpt-4.1-nano`)
4. If response starts with a known handler ID → injects back into the pipeline
5. Otherwise → `continue: true` → falls through to the LLM at priority 50

```
user: "mach die blasen an"
  → iobroker (no match — no "iobroker" prefix)
  → llm-router: injects "iobroker set Bubble true"
  → iobroker: matches, executes
  → responds: "Bubble set to on"
```

The same mechanism reaches plugin channels, because a re-injected message goes
back through the trigger match:

```
user: "fix the failing test in project A"
  → llm-router: injects "@projekt-a fix the failing test"
  → trigger match: @projekt-a → Cursor plugin takes over
```

If the router has no targets at all it skips the LLM call entirely rather than
paying for a reply it would have to discard.

---

## One router per source

Handlers are filtered by `match.source`, so several routers can sit at the same
priority and only the one matching the message's source runs. That gives you
per-source routing behaviour without a second pipeline:

```
90.0-router-llm-voice.json     match: { "source": ["webhook:voice"] }
90.0-router-llm-telegram.json  match: { "source": ["telegram"] }
90.0-router-llm-web.json       match: { "source": ["web", "app"] }
```

Give each one its own behaviour with `ROUTER_RULES` in its `env` block — it is
appended to the routing prompt:

```json
{
  "id": "router-llm-voice",
  "priority": 90.0,
  "type": "exec_json",
  "exec": "{sijba_plugins}/llm-router/main.py",
  "match": { "source": ["webhook:voice"] },
  "receive_handler_context": true,
  "continue_on_match": false,
  "env": {
    "LLM_API_KEY": "${OPENROUTER_API_KEY}",
    "ROUTER_RULES": "Input is transcribed speech and may be misspelled. Prefer smart-home targets."
  }
}
```

`POST /webhook/<token>` with `{"source": "voice"}` produces the source
`webhook:voice`. The label is slugified and always prefixed, so an external
caller can never claim to be `web`, `app` or `telegram`. Pass a `chat_id` too
if the reply should show up in a transcript.

---

## Multi-agent future

The router pattern scales to multi-agent setups:

- Router detects intent → injects with category or handler ID
- Each "agent" is an LLM handler with its own system prompt, model, and context store
- `"category": ["coding"]`, `"category": ["smarthome"]`, etc.
- Router stays cheap (fast/small model), specialists use powerful models

The inject mechanism is the only primitive needed — no framework required.

See [plugins.md → llm-router/main.py](plugins.md#llm-routermainpy) for configuration details.
