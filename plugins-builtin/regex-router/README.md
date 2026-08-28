# regex-router

Deterministic, zero-LLM intent router. Matches the message text against an
ordered list of regex routes (first match wins) and re-injects a rewritten
message — normally prefixed with a `@channel` mention or a trigger command.

No match → `continue` → the message falls through to the next handler.

## Why

- **Free and instant** — no API call, no network.
- **No hallucination risk** — pure template substitution.
- Pair it with [`llm-router`](../llm-router/): a `regex-router` tee up front
  (`ON_MATCH_CONTINUE`) to mirror every message somewhere, another
  `regex-router` to claim the deliberate keyword prefixes, and `llm-router`
  behind them for free-form text.

## Config

| Env var | Description |
|---------|-------------|
| `ROUTES` | JSON array of route objects. Ignored when `ROUTES_FILE` is set. |
| `ROUTES_FILE` | Path to a file with the same JSON array. Preferred for k8s — keeps the escaped JSON out of the ConfigMap. |
| `ROUTES_FLAGS` | Comma-separated regex flags: `i`, `m`, `s`, `x`. Default `i`. |
| `ON_MATCH_CONTINUE` | Truthy → a matched route emits `{"continue": true, "inject": …}`: the message is mirrored *and* the pipeline proceeds. Use for a "tee" handler. Default: a match stops the pipeline. |

### Route object

```json
{
  "pattern": "^(einkauf|einkaufs?liste|besorgen)\\b[:,]?\\s*",
  "inject":  "@obsidian [einkauf] {rest}"
}
```

- `pattern` — matched with `re.match` (anchored at the start of the text).
- `inject` — `str.format` template. Available fields:
  - `{rest}` — text with the matched prefix removed, stripped
  - `{text}` — the full original text, unchanged
  - `{0}`…`{n}` — positional regex capture groups

## Tee example (mirror every message, keep going)

```json
{
  "id": "voice-tee",
  "priority": 10.0,
  "direction": "inbound",
  "lifecycle": "per_message",
  "type": "exec_json",
  "exec": "{sijba_plugins}/regex-router/main.py",
  "match": { "source": ["webhook:voice"] },
  "continue_on_match": true,
  "env": {
    "ROUTES": "[{\"pattern\":\".*\",\"inject\":\"@obsidian [voice] {text}\"}]",
    "ON_MATCH_CONTINUE": "true"
  }
}
```

## Handler example

```json
{
  "id": "router-regex",
  "priority": 70.0,
  "direction": "inbound",
  "lifecycle": "per_message",
  "type": "exec_json",
  "exec": "{sijba_plugins}/regex-router/main.py",
  "match": { "source": ["webhook:voice"] },
  "continue_on_match": false,
  "timeout_seconds": 10,
  "env": { "ROUTES_FILE": "/config/handlers/routes-voice.json" }
}
```

See `edvbartl-ansible/kustomize-k8s/bases/sijba/configmap.yaml` for the full
voice chain (tee → keyword router → llm-router).

## Re-injection

A matched route re-injects a fresh inbound message. `@channel` mentions are
picked up by the trigger match at the top of `process_inbound` *before* the
handler chain runs, so a re-injected `@obsidian …` message goes straight to
the channel and never loops back through the routers. `max_inject_depth`
(`core.yaml`, default 5) still bounds any accidental cycle.
