# TODO 05 — A message that never got delivered is invisible unless you go looking

## Problem

todo-02 made undelivered outbound messages survivable: they are retried
with a backoff, and once the budget is spent they end up as
`status='failed'` in `outbound_queue` with the reason in `last_error` and
a warning in the log. That is a real improvement over silently marking
them `done` — but the user still isn't *told*.

Concretely, after todo-02 a message the system gave up on leaves:

- a row in `outbound_queue` (`status='failed'`, `attempts`, `last_error`)
- one `WARNING` log line (`queue`: "giving up after N attempt(s)")
- a `failed` count in `GET /api/queue`, which returns nothing but
  per-status counts (`core/web/api.py`, `get_queue` → `msg_queue.depths()`)

and nothing else. `sijba-web` doesn't call `/api/queue` at all — the only
core endpoint the app polls is `/api/handlers`
(`sijba-web/lib/services/api.dart:35`). So the practical failure mode is:
you asked something via the voice webhook, no Pushover notification ever
arrives, and unless you happen to open the log viewer you never find out
whether it failed or is still being retried.

The reply *is* in the transcript (it was persisted before it was ever
queued outbound), so this is specifically about the **delivery** having
failed, not about losing the text.

## Open question before designing this

Is a message that reached the app (transcript + live reply stream) but not
Pushover actually a failure worth surfacing? For `webhook:voice` the
answer is probably yes — the whole point is the phone notification. But
"outbound handler failed" and "the user never saw it" are not the same
thing after todo-01 made webhook replies visible in the app, and the fix
should not cry wolf about deliveries the user already saw. Decide this
before building anything.

## Decision

**Transcript flag** (chosen 2026-08-28): mark the bot reply row as
`delivery_status='failed'` when outbound gives up, surface via
`GET /api/transcript`, live via SSE `delivery_failed` event, show inline
in sijba-web. Only for sources where `_expects_outbound_delivery` is true
(`expects_outbound_delivery` in `core/envelope.py`).

## Possible shapes (not decided)

- **Cheapest:** extend `GET /api/queue` with the last N failed rows
  (id, source, target, `last_error`, timestamps) and show a badge/section
  in the app when the count is non-zero. Read-only, no new mechanism.
- **Middle:** a `delivery_failed` event on the existing SSE/WS reply
  stream (`state.broadcast_reply`), so an open app learns about it live
  the same way it learns about replies. Fits the existing broadcast path;
  needs a decision on what the app renders for it (an inline marker on the
  message whose `parent_id` matches?).
- **Most work, best UX:** mark the corresponding transcript entry as
  "not delivered" so it's visible on reload too, not just live. Needs a
  transcript schema addition and a way to correlate the outbound envelope
  back to its transcript row (`envelope_id` / `parent_id` are both
  available at `reply.deliver` time — check whether the outbound envelope
  still carries enough to find the row when it finally fails, minutes
  later and several retries down).

A manual retry ("send it again now") is deliberately *not* on this list
yet — worth considering only once failures are visible at all.

## Tests to add

Depends on the shape chosen. In every case:

- a message that exhausts its retry budget produces whatever the chosen
  surface is (endpoint field / broadcast event / transcript flag)
- a message that succeeds on a later retry produces *no* failure surface
- the failure surface doesn't fire for the sources that legitimately have
  no outbound handler (`web`, `app`, `internal` — see
  `pipeline._SELF_DELIVERING_SOURCES`)

## Status

Done (2026-08-28). Transcript flag + SSE `delivery_failed` + sijba-web inline
warning. Hook in `msg_queue._defer` after the DB write commits (must not run
inside `db.write()` — same-thread lock deadlock otherwise).
