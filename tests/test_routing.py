"""The three inbound routing paths.

These are deliberate, not accidental duplication:

  1. explicit ``target``  — user picked a channel in the UI sidebar
  2. ``@trigger`` prefix  — plugin channel recognised from the message text
  3. neither              — full priority pipeline, i.e. the LLM router decides

Any refactor of pipeline.process_inbound must keep all three working.
"""
from __future__ import annotations
import asyncio

import pytest

import core.pipeline as pipeline
import core.plugin_runner as plugin_runner
import core.state as state
from conftest import FakeWS, handler, install, register_channel


def envelope(text: str, **overrides) -> dict:
    env = {
        "id": "env-1",
        "parent_id": None,
        "direction": "inbound",
        "source": "web",
        "source_meta": {"chat_id": "web"},
        "text": text,
        "pipeline_text": text,
        "depth": 0,
        "metadata": {},
    }
    env.update(overrides)
    return env


class Calls(list):
    """Records (handler_id, envelope) per invoke; `responses` cans the replies."""

    def __init__(self):
        super().__init__()
        self.responses: dict = {}


@pytest.fixture
def calls(monkeypatch):
    """Replace plugin_runner.invoke; record calls, return canned responses."""
    recorded = Calls()

    async def fake_invoke(h, env, config_dir):
        recorded.append((h["id"], dict(env)))
        return recorded.responses.get(h["id"], {"continue": True})

    monkeypatch.setattr(plugin_runner, "invoke", fake_invoke)
    return recorded


def ids(calls) -> list:
    return [c[0] for c in calls]


# ------------------------------------------------------------------
# Path 1: explicit target
# ------------------------------------------------------------------

async def test_explicit_target_routes_to_plugin_channel(wired, db, calls):
    ws = register_channel("cursor", "projekt-a")
    install(handler("echo", priority=99.0))

    await pipeline.process_inbound(envelope("hallo", _target="projekt-a"))

    assert ws.messages_for("projekt-a"), "plugin channel got nothing"
    payload = ws.messages_for("projekt-a")[0]["payload"]
    assert payload["text"] == "hallo"
    assert ids(calls) == [], "pipeline handlers must not also run"


async def test_explicit_target_beats_a_handler_of_the_same_name(wired, db, calls):
    """Plugin channels win: their ids live in the registry, not state.handlers."""
    ws = register_channel("cursor", "shared-name")
    install(handler("shared-name"))

    await pipeline.process_inbound(envelope("hi", _target="shared-name"))

    assert ws.messages_for("shared-name")
    assert ids(calls) == []


async def test_explicit_target_to_handler_ignores_match_and_direct_inject_gate(
    wired, db, calls
):
    """A user picking a channel is an explicit choice: no trigger word needed,
    and allow_direct_inject (default false) does not block it."""
    install(handler("pods", match={"regex": r"(?i)^pods\b"}, allow_direct_inject=False))

    await pipeline.process_inbound(envelope("wie geht es", _target="pods"))

    assert ids(calls) == ["pods"]


async def test_unknown_target_is_discarded(wired, db, calls):
    install(handler("echo", priority=99.0))

    await pipeline.process_inbound(envelope("hallo", _target="does-not-exist"))

    assert ids(calls) == [], "must not silently fall through to the pipeline"


async def test_target_to_offline_plugin_channel_does_not_fall_through(wired, db, calls):
    register_channel("cursor", "projekt-a", ws=FakeWS(fail=True))
    install(handler("echo", priority=99.0))

    await pipeline.process_inbound(envelope("hallo", _target="projekt-a"))

    assert ids(calls) == []


# ------------------------------------------------------------------
# Path 2: @trigger prefix
# ------------------------------------------------------------------

async def test_at_trigger_routes_to_plugin_channel(wired, db, calls):
    ws = register_channel("cursor", "projekt-a", triggers=["@projekt-a"])
    install(handler("echo", priority=99.0))

    await pipeline.process_inbound(envelope("@projekt-a bau mir was"))

    assert ws.messages_for("projekt-a"), "@ trigger did not reach the plugin"
    assert ids(calls) == [], "plugin takes over, pipeline must not run"


async def test_at_trigger_is_case_insensitive(wired, db, calls):
    ws = register_channel("cursor", "projekt-a", triggers=["@Projekt-A"])

    await pipeline.process_inbound(envelope("@projekt-a hallo"))

    assert ws.messages_for("projekt-a")


async def test_trigger_must_be_a_prefix_not_a_substring(wired, db, calls):
    register_channel("cursor", "projekt-a", triggers=["@projekt-a"])
    install(handler("echo", priority=99.0))

    await pipeline.process_inbound(envelope("frag mal @projekt-a nach dem stand"))

    assert ids(calls) == ["echo"], "mid-sentence mention must not hijack routing"


async def test_plugin_receives_full_text_including_the_trigger(wired, db, calls):
    ws = register_channel("cursor", "projekt-a", triggers=["@projekt-a"])

    await pipeline.process_inbound(envelope("@projekt-a status?"))

    payload = ws.messages_for("projekt-a")[0]["payload"]
    assert payload["text"] == "@projekt-a status?"


async def test_unmatched_trigger_falls_through_to_pipeline(wired, db, calls):
    register_channel("cursor", "projekt-a", triggers=["@projekt-a"])
    install(handler("echo", priority=99.0))

    await pipeline.process_inbound(envelope("@etwas-anderes hallo"))

    assert ids(calls) == ["echo"]


async def test_dead_plugin_channel_falls_through_to_pipeline(wired, db, calls):
    """If the @-matched plugin is unreachable the message still gets handled."""
    register_channel("cursor", "projekt-a", triggers=["@projekt-a"], ws=FakeWS(fail=True))
    install(handler("echo", priority=99.0))

    await pipeline.process_inbound(envelope("@projekt-a hallo"))

    assert ids(calls) == ["echo"]


# ------------------------------------------------------------------
# Path 3: full pipeline / the LLM router
# ------------------------------------------------------------------

async def test_pipeline_runs_handlers_in_priority_order(wired, db, calls):
    install(
        handler("echo", priority=99.0),
        handler("audit", priority=0.0),
        handler("router", priority=90.0),
    )

    await pipeline.process_inbound(envelope("hallo"))

    assert ids(calls) == ["audit", "router", "echo"]


async def test_router_inject_replaces_the_rest_of_the_pipeline(wired, db, calls):
    """The prod router returns {"continue": false, "inject": {...}} — the
    injected text must be re-queued and the current run must stop."""
    inbound, _ = wired
    install(handler("router", priority=90.0), handler("echo", priority=99.0))
    calls.responses["router"] = {"continue": False, "inject": {"text": "pods list"}}

    await pipeline.process_inbound(envelope("welche pods laufen"))

    assert ids(calls) == ["router"], "echo must not run after continue=false"
    assert len(inbound) == 1
    assert inbound[0]["text"] == "pods list"
    assert inbound[0]["source"] == "internal"
    assert inbound[0]["depth"] == 1
    assert inbound[0]["parent_id"] == "env-1"


async def test_handler_match_regex_filters_the_pipeline(wired, db, calls):
    install(
        handler("pods", priority=10.0, match={"regex": r"(?i)^pods\b"}),
        handler("echo", priority=99.0),
    )

    await pipeline.process_inbound(envelope("hallo"))
    assert ids(calls) == ["echo"]

    calls.clear()
    await pipeline.process_inbound(envelope("pods list"))
    assert ids(calls) == ["pods", "echo"]


async def test_match_source_filters_the_pipeline(wired, db, calls):
    install(
        handler("only-telegram", priority=10.0, match={"source": ["telegram"]}),
        handler("echo", priority=99.0),
    )

    await pipeline.process_inbound(envelope("hallo", source="web"))
    assert ids(calls) == ["echo"]

    calls.clear()
    await pipeline.process_inbound(envelope("hallo", source="telegram"))
    assert ids(calls) == ["only-telegram", "echo"]


async def test_non_healthy_handlers_are_skipped(wired, db, calls):
    install(handler("audit", priority=0.0), handler("echo", priority=99.0))
    state.handler_states["audit"] = "stopped"

    await pipeline.process_inbound(envelope("hallo"))

    assert ids(calls) == ["echo"]


async def test_continue_on_match_false_stops_the_pipeline(wired, db, calls):
    """continue_on_match is the *default* used when the response omits it."""
    install(
        handler("first", priority=10.0, continue_on_match=False),
        handler("echo", priority=99.0),
    )
    calls.responses["first"] = {}  # no explicit "continue"

    await pipeline.process_inbound(envelope("hallo"))

    assert ids(calls) == ["first"]


async def test_explicit_continue_in_response_overrides_continue_on_match(
    wired, db, calls
):
    """A handler can opt back into the pipeline despite continue_on_match=false."""
    install(
        handler("first", priority=10.0, continue_on_match=False),
        handler("echo", priority=99.0),
    )
    calls.responses["first"] = {"continue": True}

    await pipeline.process_inbound(envelope("hallo"))

    assert ids(calls) == ["first", "echo"]


async def test_fire_and_forget_handler_does_not_block(wired, db, calls):
    """audit runs with await_response=false — it must not gate the pipeline."""
    install(
        handler("audit", priority=0.0, await_response=False),
        handler("echo", priority=99.0),
    )

    await pipeline.process_inbound(envelope("hallo"))
    await asyncio.sleep(0)  # let the fire-and-forget task run

    assert "echo" in ids(calls)
    assert "audit" in ids(calls)


async def test_pipeline_text_is_threaded_between_handlers(wired, db, calls):
    install(handler("rewrite", priority=10.0), handler("echo", priority=99.0))
    calls.responses["rewrite"] = {"continue": True, "pipeline_text": "umgeschrieben"}

    await pipeline.process_inbound(envelope("original"))

    seen = dict((hid, env) for hid, env in calls)
    assert seen["echo"]["pipeline_text"] == "umgeschrieben"
    assert seen["echo"]["text"] == "original", "original text must be preserved"


# ------------------------------------------------------------------
# Inject depth
# ------------------------------------------------------------------

async def test_inject_depth_limit_is_enforced(wired, db, calls):
    inbound, _ = wired
    install(handler("looper", priority=10.0))
    calls.responses["looper"] = {"continue": False, "inject": {"text": "again"}}

    await pipeline.process_inbound(envelope("start", depth=5))

    assert inbound == [], "inject past max_inject_depth must be discarded"


async def test_inject_to_category_runs_only_that_category(wired, db, calls):
    install(
        handler("trigger", priority=5.0),
        handler("k8s-a", priority=10.0, category=["k8s"]),
        handler("other", priority=20.0),
    )
    calls.responses["trigger"] = {
        "continue": False,
        "inject": {"text": "los", "target": "k8s"},
    }

    await pipeline.process_inbound(envelope("start"))
    await asyncio.sleep(0)

    assert "k8s-a" in ids(calls)
    assert "other" not in ids(calls)


# ------------------------------------------------------------------
# respond → outbound
# ------------------------------------------------------------------

async def test_respond_enqueues_outbound_and_broadcasts_to_web(
    wired, db, calls, replies
):
    _, outbound = wired
    install(handler("echo", priority=99.0))
    calls.responses["echo"] = {"continue": False, "respond": {"text": "pong", "target": "source"}}

    await pipeline.process_inbound(envelope("ping"))

    assert len(outbound) == 1
    assert outbound[0]["text"] == "pong"
    assert outbound[0]["direction"] == "outbound"
    assert outbound[0]["parent_id"] == "env-1"

    events = replies.drain()
    assert [e["type"] for e in events] == ["reply"]
    assert events[0]["text"] == "pong"
    assert events[0]["parent_id"] == "env-1"


async def test_respond_from_non_web_source_is_not_broadcast(wired, db, calls, replies):
    install(handler("echo", priority=99.0))
    calls.responses["echo"] = {"continue": False, "respond": {"text": "pong"}}

    await pipeline.process_inbound(envelope("ping", source="telegram"))

    assert replies.drain() == []


async def test_outbound_handlers_filtered_by_source(wired, db, calls):
    install(
        handler("tg-out", direction="outbound", match={"source": ["telegram"]}),
        handler("web-out", direction="outbound"),
    )

    await pipeline.process_outbound(
        envelope("hi", direction="outbound", source="web") | {"_respond_target": "source"}
    )

    assert ids(calls) == ["web-out"], "handler with no source filter always runs"

    calls.clear()
    await pipeline.process_outbound(
        envelope("hi", direction="outbound", source="telegram")
        | {"_respond_target": "source"}
    )
    assert ids(calls) == ["tg-out", "web-out"]


# ------------------------------------------------------------------
# Fallback must not double-process
# ------------------------------------------------------------------

async def test_pipeline_fallback_cancels_the_plugin_redelivery(wired, db, calls):
    """Regression: an @-matched but offline plugin used to keep the message
    stashed for redelivery *and* let the pipeline answer it, so the plugin
    processed the same turn again after reconnecting."""
    import core.registry as registry

    register_channel("cursor", "projekt-a", triggers=["@projekt-a"],
                     ws=FakeWS(fail=True))
    install(handler("echo", priority=99.0))

    await pipeline.process_inbound(envelope("@projekt-a mach was"))

    assert ids(calls) == ["echo"], "pipeline handled it"
    assert registry.peek_pending_reply("env-1") is None, \
        "must not also be queued for the plugin"

    fresh = FakeWS()
    register_channel("cursor", "projekt-a", triggers=["@projekt-a"], ws=fresh)
    assert await registry.redeliver_pending_for_channel("projekt-a") == 0


async def test_explicit_target_keeps_the_redelivery(wired, db, calls):
    """The opposite case: with an explicit target there is no fallback, so
    the message must stay queued for the channel."""
    import core.registry as registry

    register_channel("cursor", "projekt-a", ws=FakeWS(fail=True))
    install(handler("echo", priority=99.0))

    await pipeline.process_inbound(envelope("hallo", _target="projekt-a"))

    assert ids(calls) == [], "no fallback for an explicitly chosen channel"
    assert registry.peek_pending_reply("env-1") is not None, \
        "must survive for redelivery on reconnect"


# ------------------------------------------------------------------
# The router closes the loop: message -> router -> channel
# ------------------------------------------------------------------

async def test_router_can_route_a_plain_message_to_a_plugin_channel(wired, db, calls):
    """End-to-end: a message with no @ and no target reaches the right
    channel purely because the router picked it.

    The router emits the channel's trigger, that gets injected as a new
    inbound message, and the trigger match then routes it — no extra
    routing path needed.
    """
    inbound, _ = wired
    ws = register_channel("cursor", "projekt-a", triggers=["@projekt-a"])
    install(handler("router", priority=90.0), handler("echo", priority=99.0))
    calls.responses["router"] = {
        "continue": False,
        "inject": {"text": "@projekt-a mach das licht an"},
    }

    await pipeline.process_inbound(envelope("mach das licht an"))

    assert ids(calls) == ["router"], "echo must not answer instead"
    assert len(inbound) == 1, "router's choice was re-queued"

    # The queue worker would hand it straight back to process_inbound.
    await pipeline.process_inbound(inbound[0])

    assert ws.messages_for("projekt-a"), "router's pick never reached the channel"
    assert ws.messages_for("projekt-a")[0]["payload"]["text"] == \
        "@projekt-a mach das licht an"


# ------------------------------------------------------------------
# Text routing requires an explicit @mention
# ------------------------------------------------------------------

async def test_bare_project_name_does_not_hijack_a_message(wired, db, calls):
    """The cursor plugin registers its bare project name as a trigger.
    "Recherche zu Thema X" is a message *about* research, not one addressed
    to the Recherche agent, so it must stay in the pipeline."""
    ws = register_channel("cursor", "cursor-recherche",
                          triggers=["Recherche", "cursor-recherche"])
    install(handler("echo", priority=99.0))

    await pipeline.process_inbound(envelope("Recherche zu Thema X machen"))

    assert ws.messages_for("cursor-recherche") == [], "bare word must not route"
    assert ids(calls) == ["echo"]


async def test_at_mention_of_a_bare_trigger_still_routes(wired, db, calls):
    """Core normalises registered triggers to their @ form, so a plugin that
    has not been redeployed is still reachable — just only via @."""
    ws = register_channel("cursor", "cursor-recherche",
                          triggers=["Recherche", "cursor-recherche"])
    install(handler("echo", priority=99.0))

    await pipeline.process_inbound(envelope("@Recherche fix the test"))

    assert ws.messages_for("cursor-recherche"), "@ form of a bare trigger must route"
    assert ids(calls) == []


async def test_channel_without_triggers_is_addressable_by_its_id(wired, db, calls):
    ws = register_channel("cursor", "projekt-a", triggers=[])

    await pipeline.process_inbound(envelope("@projekt-a hallo"))

    assert ws.messages_for("projekt-a")


async def test_advertised_trigger_is_one_the_match_accepts(wired, db, calls):
    """What the router is told to emit must be what the trigger match takes."""
    import core.registry as registry

    register_channel("cursor", "cursor-recherche", triggers=["Recherche"])
    register_channel("cursor2", "other", triggers=[])

    for target in registry.channel_routing_targets():
        advertised = target["id"]
        channels = registry.list_plugin_channels()
        assert any(advertised in c["triggers"] for c in channels), \
            f"router would be told to emit {advertised!r}, which nothing matches"


async def test_direct_target_ignores_the_at_requirement(wired, db, calls):
    """The @ rule is only about recognising a channel *in message text*.
    Picking the channel explicitly (UI sidebar, or `target` on the API)
    matches on channel_id, so no mention is needed anywhere."""
    ws = register_channel("cursor", "cursor-recherche", triggers=["Recherche"])
    install(handler("echo", priority=99.0))

    await pipeline.process_inbound(
        envelope("fix the failing test", _target="cursor-recherche")
    )

    assert ws.messages_for("cursor-recherche"), "direct target must still work"
    payload = ws.messages_for("cursor-recherche")[0]["payload"]
    assert payload["text"] == "fix the failing test", "text is passed through as-is"
    assert ids(calls) == []


async def test_direct_target_works_for_a_channel_with_no_triggers(wired, db, calls):
    ws = register_channel("cursor", "projekt-a", triggers=[])

    await pipeline.process_inbound(envelope("hallo", _target="projekt-a"))

    assert ws.messages_for("projekt-a")


# ------------------------------------------------------------------
# Return path: both kinds of reply must reach outbound handlers
# ------------------------------------------------------------------

def voice_env(text: str, **overrides) -> dict:
    return envelope(text, source="webhook:voice",
                    source_meta={"chat_id": "voice"}, **overrides)


async def test_handler_reply_reaches_a_source_scoped_outbound_handler(
    wired, db, calls
):
    """A per-webhook return path, e.g. Pushover for a voice webhook."""
    _, outbound = wired
    install(
        handler("assistant", priority=50.0),
        handler("pushover", direction="outbound",
                match={"source": ["webhook:voice"]}),
    )
    calls.responses["assistant"] = {
        "continue": False, "respond": {"text": "Licht ist an"},
    }

    await pipeline.process_inbound(voice_env("licht an"))
    assert len(outbound) == 1
    for out in outbound:
        await pipeline.process_outbound(out)

    assert "pushover" in ids(calls)


async def test_plugin_channel_reply_also_reaches_the_outbound_handler(
    wired, db, calls
):
    """Regression: plugin replies used to skip the outbound pipeline entirely,
    so a per-source return path silently never fired for channel traffic —
    which is where the actual work happens."""
    import core.web.api as api

    _, outbound = wired
    register_channel("cursor", "projekt-a", triggers=["@projekt-a"])
    install(handler("pushover", direction="outbound",
                    match={"source": ["webhook:voice"]}))

    await pipeline.process_inbound(voice_env("@projekt-a mach was"))
    await api._handle_plugin_reply(
        {"channel": "projekt-a", "envelope_id": "env-1", "text": "fertig"}
    )

    assert len(outbound) == 1, "channel reply never reached the outbound queue"
    assert outbound[0]["text"] == "fertig"
    assert outbound[0]["source"] == "webhook:voice"

    for out in outbound:
        await pipeline.process_outbound(out)
    assert "pushover" in ids(calls)


async def test_streamed_chunks_do_not_reach_the_outbound_handler(
    wired, db, calls
):
    """Only the final reply goes out — a Pushover notification per delta
    would be unusable."""
    import core.web.api as api

    _, outbound = wired
    register_channel("cursor", "projekt-a", triggers=["@projekt-a"])

    await pipeline.process_inbound(voice_env("@projekt-a mach was"))
    for part in ("Ich ", "mache ", "das."):
        await api._handle_plugin_reply_chunk(
            {"channel": "projekt-a", "envelope_id": "env-1", "text": part}
        )

    assert outbound == []
