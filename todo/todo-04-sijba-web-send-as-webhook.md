# TODO 04 — sijba-web: compose a message "as if" it came via a given webhook

## Goal

Let the user type a message in the sijba-web app and send it tagged with an
arbitrary `source` (e.g. `webhook:voice`), so it's matched and routed
exactly like a real webhook call — useful for testing routing/Pushover
without needing the real trigger (a voice shortcut, IoT event, ...).

## The core side already supports this — verified, nothing to build there

`POST /api/message` already accepts an optional `source` field
(`core/web/api.py`, `inject_message`):

```python
source = body.get("source") or "web"
```

`{"text": "...", "source": "webhook:voice", "chat_id": "voice"}` against
`/api/message` today produces exactly the same envelope shape as a real
`POST /webhook/<voice-token>` call — same source, same routing, same
outbound match. Confirmed by reading the code; not yet tried live, but
there's no reason it wouldn't work identically to the real webhook path
tested in prod on 2026-08-28.

So this TODO is **entirely app-side (sijba-web, Flutter/Dart)** — a UI
affordance to set `source` when composing, nothing in Core needs to
change.

## Not yet investigated

Haven't read `sijba-web`'s composer code this session (different repo,
different stack from everything else touched tonight). Before designing
this, need to look at:

- `home_shell.dart` — the message composer widget, and the existing
  slash-command mechanism (`_slashCommands`, e.g. `/archive`, `/pause`,
  `/log [n]` for cursor-plugin channels per its `hint` string) as a
  possible model — a `/as webhook:voice <text>` command might fit the
  existing UX pattern better than a new UI control.
- `controller.dart` / `services/controller.dart` — how a message actually
  gets built and POSTed, to find where to plumb an optional `source`
  through.
- Whether there's already any dev/debug affordance for arbitrary fields
  that this could extend instead of duplicating.

## Status

Done (2026-08-28). Shipped as a **per-webhook tile** in sijba-web rather than
a free-text `/as` command — cleaner and it also covers the follow-up wish of
seeing webhook traffic in its own Kachel.

**Core (`sijba`):**
- `core/web/api.py` — new `GET /api/webhooks` → `[{name, source}]`, never the
  tokens. Built from the existing `_webhooks` module state.
- `core/reply.py` — `deliver()` now puts `source` into the `broadcast_reply`
  payload so a live reply can be attributed to its webhook.
- `/api/message` already accepted `source` — no change; a
  `test_message_source_is_passed_through` was added to pin it.
- Tests: `tests/test_api.py` (webhooks endpoint lists without leaking tokens,
  needs auth; message source passthrough; broadcast carries `source`).
- `docs/api.md` updated.

**App (`sijba-web`):**
- `ChatMessage.source`; `ChannelItem.sendAsSource` / `sourceFilter` +
  `historyKey` / `routesThroughPipeline` getters.
- `SijbaApi.webhooks()`; transcript rows carry `source`.
- `AppController` — one tile per webhook ("Webhook · voice"), a **filtered
  view** of the pipeline transcript (`source == sourceFilter`); composer sends
  `source = sendAsSource`; live reply events routed by `source`; unread badge
  via `_markUnreadForSource`. Messages stay visible in the Pipeline tile too.
- `home_shell.dart` — header title + `@`/`/` autocomplete keyed off
  `routesThroughPipeline`.
- Tests: `test/webhook_channel_test.dart`.

**Deliberate side effect:** a message composed "as webhook:voice" really is
routed like the live webhook — the Pushover outbound handler fires, so the
phone gets a push too. That is the point (test routing/Pushover without the
real trigger).

Deploy: core-only + app; both via their normal Jenkins pipelines, no
ConfigMap/ansible change.
