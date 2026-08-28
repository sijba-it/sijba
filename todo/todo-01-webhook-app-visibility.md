# TODO 01 — Webhook replies invisible in sijba-web

## Problem

A message sent via `POST /webhook/<token>` — and its reply — never shows up
in the sijba-web app, even though it went through the pipeline correctly and
is sitting in the transcript store. Verified live in prod (2026-08-28) with
the `voice` named webhook: the message and echo reply were both persisted
(`GET /api/transcript?chat_id=voice-live2` showed both), but nothing
appeared in the app.

Two independent causes, both confirmed by reading the code:

### Cause A — `chat_id` mismatch

`/api/message` (what the app itself uses) defaults the transcript key like
this (`core/web/api.py`, in `inject_message`):

```python
source_meta.setdefault("chat_id", "web")
```

`/webhook/{token}` does **not** do this — currently whatever `chat_id` the
caller passes (or none) is used as-is. The app always reads
`chat_id=web, channel=pipeline`. A webhook message with no explicit
`chat_id`, or a different one, lands in a completely separate transcript
bucket the app never queries.

### Cause B — live broadcast is gated to web/app sources

`core/reply.py`, `deliver()`:

```python
if source in ("web", "app"):
    state.broadcast_reply({...})
```

Even with a matching `chat_id`, a webhook-sourced reply is never pushed to
the SSE/WS `/api/replies` stream — only `web`/`app` are. It *is* persisted
to the transcript regardless, so a manual reload of that chat/channel would
show it, but nothing arrives live while the app is open.

Telegram is in a similar boat but doesn't need this — the bot has its own
transport to notify the user. Webhook sources currently have no such
transport, which is the whole reason this matters.

## Fix (small, core-only)

1. In `webhook()` (`core/web/api.py`), default `source_meta["chat_id"]` to
   `"web"` when the caller didn't supply one — mirror `inject_message`'s
   `setdefault`.
2. In `reply.deliver()` (`core/reply.py`), broaden the broadcast condition
   to also cover webhook sources, e.g. `source in ("web", "app") or
   source.startswith("webhook")`. Don't broaden it to *everything* —
   Telegram etc. don't need it and there's no reason to widen the surface
   further than the actual gap.

## Tests to add

- `tests/test_api.py`: a `/webhook/<token>` call with no `chat_id` in the
  body lands in the `chat_id="web"` transcript bucket.
- `tests/test_routing.py` (or wherever `reply.deliver` is exercised): a
  `respond`/plugin-reply from a `webhook:*` source triggers
  `state.broadcast_reply`; one from e.g. `telegram` still does not.

## Deploy

Core code change only — no ConfigMap/ansible change needed. Ships via the
normal `sijba` Jenkins pipeline (image bump + StatefulSet rollout).

## Status

Done (2026-08-28). Both causes fixed as described: `chat_id` setdefault in
`webhook()` and the broadcast condition in `reply.deliver()`. Covered by three
new tests in `tests/test_api.py`; the two behavioural ones fail without the fix.
