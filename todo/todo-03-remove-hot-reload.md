# TODO 03 — Remove hot-reload

## Decision (2026-08-28)

Discussed and agreed: remove hot-reload for `handlers/*.json` entirely,
rather than keep it as a narrow escape hatch. Reasoning:

- It only ever covered file-based handler configs — never `core.yaml`
  (port, log_level, db_path, webhooks), which has always required a full
  restart. That asymmetry is exactly what caused confusion during the
  named-webhooks rollout (a `core.yaml` push looked like it should take
  effect immediately; it didn't, needed a manual
  `kubectl rollout restart statefulset/sijba`).
- It doesn't help at all for the two most common reasons Core actually
  restarts in practice: an image deploy (every `sijba` Jenkins push) and a
  crash. Only protects the narrow case of "changed only `handlers/*.json`,
  nothing else."
- Plugin WebSocket channels (`sijba-cursor-plugin`, `sijba-agent-controller`)
  and dynamically-registered remote handlers (`sijba-out-pushover`) are
  **not** covered by hot-reload either way — they're separate in-memory
  state that a restart always drops regardless (they already have their
  own reconnect/re-register loops: WS channels use exponential backoff
  5s→300s, `sijba-out-pushover` re-registers on its next failed heartbeat).
  So hot-reload was never protecting the thing that actually matters for
  a restart's real-world cost.

**Do this after (or together with) todo-02.** Once hot-reload is gone,
every config change becomes a restart, which makes restart-safety for
outbound delivery load-bearing rather than a nice-to-have.

## What to remove

- `core/main.py`: the `signal.SIGHUP` handler (`loop.add_signal_handler`)
  and the `import signal` it needs (check nothing else in main.py still
  needs `signal`).
- `core/web/api.py`: `POST /api/handlers/reload` endpoint, and the
  `reload_fn` parameter threaded through `configure()` / `main.py`.
- `core/handler_loader.py`: the `reload()` function and its
  state-preservation logic (the part that keeps `handler_states` for
  handlers that survive the reload, drops it for ones that vanished).
  `load()` itself stays — that's what a fresh boot uses.
- Docs: `docs/handlers.md` and/or `docs/routing.md` — check for hot-reload
  mentions and update to say "restart the pod to pick up handler config
  changes."
- Tests: `tests/test_persistence.py` has
  `test_reload_preserves_state_of_surviving_handlers` and
  `test_reload_keeps_registered_remotes` — both exercise
  `handler_loader.reload()` directly and should be removed along with it.

## Check before removing

- Does **sijba-web**'s UI have a "reload" button or similar that calls
  `POST /api/handlers/reload`? Haven't checked the Flutter code for this
  yet. If it exists, removing the endpoint leaves a dead button — either
  remove the button too (different repo, Dart) or have the endpoint 404
  gracefully with a clear message pointing at a restart instead.

## Status

Not started. Blocked on / sequenced after todo-02 by design choice above,
not a hard technical dependency — could be done standalone if todo-02 is
deprioritized, just accept that restarts stay lossy for outbound delivery
in the interim.
