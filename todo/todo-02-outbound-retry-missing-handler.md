# TODO 02 — Outbound messages are silently dropped when no handler is registered yet

## Problem

Discovered while discussing whether to remove hot-reload and instead make
Core restarts harmless. Confirmed by reading the code (2026-08-28), not
just suspected.

Dynamically-registered remote handlers (`POST /api/register`, used by
`sijba-out-pushover` and `sijba-kubectl`) live purely in memory
(`state.registered_handlers`, `state.handlers`). A Core restart wipes this
list; the handler only reappears once *its own* heartbeat loop notices a
failed heartbeat and re-registers. Observed live: after a Core pod restart,
`pushover` vanished from `GET /api/handlers` and reappeared roughly
one heartbeat interval later (up to `TTL_SECONDS/3` = 30s by default).

If an outbound envelope is processed by `pipeline.process_outbound` during
that gap, here's what happens (`core/pipeline.py`, `_run`):

```python
handlers = _outbound_handlers_for(envelope)   # empty — nothing registered yet
for handler in handlers:                       # loop body never runs
    ...
# falls through, returns normally — no exception
```

`msg_queue.run_worker` then marks the envelope `done` because `fn(env)`
returned without raising:

```python
await fn(env)
_set_status(table, mid, "done")
```

**The message is gone. No retry, no error, no log line pointing at it.**
This is a different, weaker guarantee than the plugin-channel path:
`registry._pending_replies` / `redeliver_pending_for_channel` *does* retry
when a WebSocket channel reconnects — but that mechanism only covers
replies to a message a plugin channel is expected to answer. It has no
equivalent for the `/api/register` HTTP-handler outbound-dispatch path.

### Related, smaller gap noticed in the same spot

`msg_queue._claim_next` only ever looks at `status = 'pending'`
(`core/msg_queue.py`). If the process dies after claiming a message
(`status='claimed'`) but before finishing it, that row is orphaned forever
— nothing ever reclaims it. Same fix location (a startup sweep or a
claimed-but-stale requeue), worth doing together with the main fix below
rather than as a separate pass.

## Why this matters now

The plan is to lean into "restarts happen, make them harmless" instead of
avoiding them via hot-reload (see todo-03). That only holds if outbound
delivery actually survives the registration gap a restart causes — right
now it doesn't, silently.

## Proposed fix

Mirror the existing pending/redelivery pattern in `core/registry.py`
rather than inventing a new one:

- When `_outbound_handlers_for()` yields nothing (or every match fails to
  invoke), don't mark the envelope `done`. Leave it `pending` — the
  existing worker loop will pick it up again — but cap it, same shape as
  `_pending_max_redeliveries` / `_pending_ttl_seconds`:
  - track an attempt count and first-seen time on the row (or store it
    alongside, since `outbound_queue` doesn't currently have those
    columns — will need a small schema addition, same file as
    `msg_queue.py`'s existing `CREATE TABLE`)
  - give up loudly (log a warning, mark `failed`) past the TTL/attempt cap
    — a message with a genuinely nonexistent target must not spin forever
- Also sweep orphaned `claimed` rows (inbound and outbound) back to
  `pending` on startup, since a fixed process restart is exactly when
  these would exist.
- Cheap complementary change: lower `sijba-out-pushover`'s default
  `HEARTBEAT_EVERY` (currently derived from `TTL_SECONDS=90` →
  `max(15, 90/3) = 30s`) so the worst-case unregistered window shrinks —
  doesn't fix the underlying gap but shrinks its blast radius immediately
  and cheaply.

## Tests to add

- `tests/test_persistence.py` or a new file: an outbound envelope
  processed while zero handlers match must stay retryable, not `done`.
- A handler registering *after* such a message was queued must still
  receive it once it (re-)appears in `state.handlers`.
- A message that never finds a matching handler eventually gives up
  (`failed`, logged) rather than retrying indefinitely.
- Orphaned `claimed` row on startup gets reset to `pending` and
  reprocessed.

## Status

Not started. Larger than todo-01 — touches `core/pipeline.py`,
`core/msg_queue.py`, and probably needs a small `outbound_queue` schema
change (attempt count / first-seen timestamp).
