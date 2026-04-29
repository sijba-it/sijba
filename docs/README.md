# SIJBA Documentation

**S**imple **J**SON **B**ot **A**ssistant — a pipeline-based message bot framework.

## Contents

| File | What's in it |
|------|-------------|
| [architecture.md](architecture.md) | System overview, pipeline flow, envelope structure |
| [handlers.md](handlers.md) | Handler config schema, all fields, response format |
| [plugins.md](plugins.md) | Built-in plugins — audit, echo, telegram, llm, iobroker, router |
| [routing.md](routing.md) | Routing strategies, router_hint, inject mechanism, multi-agent |
| [api.md](api.md) | REST API endpoints, curl examples |

## Quick orientation

```
sijba-config/
  core.yaml          ← port, auth_token, db_path, log_level
  handlers/
    0.0-audit.json   ← always-on logging
    10.0-telegram-source.json
    10.1-telegram-out.json
    20.0-iobroker.json
    40.0-router.json
    50.0-llm.json
```

Messages flow through handlers in **priority order** (lower number = runs first).  
Each handler can stop the pipeline, respond, or inject a new message.  
See [architecture.md](architecture.md) for the full picture.

## Starting sijba

```bash
python -m core.main        # uses SIJBA_CONFIG_DIR env var (default: ./sijba-config)
```

## Common tasks

| Task | Where to look |
|------|--------------|
| Add a new trigger command | [handlers.md → match](handlers.md#match) |
| Configure a new plugin | [plugins.md](plugins.md) |
| Enable natural-language routing | [routing.md → Router plugin pattern](routing.md#router-plugin-pattern) |
| Reset a conversation | [api.md → Context store](api.md#context-store) |
| Hot-reload handlers without restart | [api.md → Handlers](api.md#handlers) |
