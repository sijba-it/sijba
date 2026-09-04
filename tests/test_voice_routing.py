"""The voice webhook routing chain (regex-router + llm-router).

Mirrors the prod handler layout in
``edvbartl-ansible/kustomize-k8s/bases/sijba/configmap.yaml``:

  10  voice-tee     .* -> "@obsidian [voice] {text}"  (continue)
  70  router-regex  keyword prefix -> @channel        (stop on match)
  90  router-llm    actionable intent -> @channel, else NONE
  99  echo          non-voice last resort

These run the real builtin plugins as subprocesses through
``plugin_runner.invoke`` — only the LLM HTTP call is stubbed with a fake
exec so the test stays offline.
"""
from __future__ import annotations

import json
import sys
import textwrap

import pytest

import core.pipeline as pipeline
import core.plugin_runner as plugin_runner
from conftest import handler, install, register_channel

# Run the builtin plugins with the same interpreter as the test run, so the
# subprocess sees the same installed deps (jinja2). In the prod image the
# shebang (`/usr/bin/env python3`) already resolves to the deps-carrying
# interpreter; this is only to make local runs independent of PATH.
REGEX_ROUTER = str(plugin_runner.BUILTIN_DIR / "regex-router" / "main.py")


def regex_router_handler(hid, priority, env, **overrides):
    return handler(hid, priority=priority, exec=sys.executable,
                   args=[REGEX_ROUTER], env=env, **overrides)

ROUTES_VOICE = [
    {"pattern": r"^(einkauf|einkaufsliste|einkaufszettel|einkaufen|besorgen|kaufen)\b\s*[:,-]?\s*",
     "inject": "@obsidian [einkauf] {{ rest }}"},
    {"pattern": r"^(aufgabe|task|todo|to-do)\b\s*[:,-]?\s*",
     "inject": "@obsidian [task] {{ rest }}"},
    {"pattern": r"^(termin|kalender)\b\s*[:,-]?\s*",
     "inject": "@obsidian [termin] {{ rest }}"},
    {"pattern": r"^(recherche|recherchiere|recherchier)\b\s*[:,-]?\s*",
     "inject": "@Recherche {{ rest }}"},
]


def voice_env(text: str, **overrides) -> dict:
    env = {
        "id": "env-1",
        "parent_id": None,
        "direction": "inbound",
        "source": "webhook:voice",
        "source_meta": {"chat_id": "voice"},
        "text": text,
        "pipeline_text": text,
        "depth": 0,
        "metadata": {},
    }
    env.update(overrides)
    return env


@pytest.fixture
def voice_chain(tmp_path, wired):
    """Install the prod-shaped voice chain. Returns the tmp routes file path."""
    routes_file = tmp_path / "routes-voice.json"
    routes_file.write_text(json.dumps(ROUTES_VOICE))

    # Stand-in for llm-router: no network. Behaviour is set per-test by
    # writing FAKE_LLM_REPLY into the env; empty -> NONE -> continue. Touches
    # RAN_MARKER on every call so a test can assert it was / wasn't reached.
    fake_llm = tmp_path / "fake_llm_router.py"
    fake_llm.write_text(textwrap.dedent("""\
        #!/usr/bin/env python3
        import json, os, pathlib, sys
        json.load(sys.stdin)
        marker = os.environ.get("RAN_MARKER")
        if marker:
            pathlib.Path(marker).write_text("ran")
        reply = os.environ.get("FAKE_LLM_REPLY", "").strip()
        if not reply or reply.upper() == "NONE":
            print(json.dumps({"continue": True}))
        else:
            print(json.dumps({"continue": False, "inject": {"text": reply}}))
    """))
    fake_llm.chmod(0o755)

    install(
        regex_router_handler(
            "voice-tee", 10.0,
            {
                "ROUTES": json.dumps([{"pattern": ".*", "inject": "@obsidian [voice] {{ text }}"}]),
                "ON_MATCH_CONTINUE": "true",
            },
            match={"source": ["webhook:voice"]}, continue_on_match=True,
        ),
        regex_router_handler(
            "router-regex", 70.0, {"ROUTES_FILE": str(routes_file)},
            match={"source": ["webhook:voice"]}, continue_on_match=False,
        ),
        handler(
            "router-llm", priority=90.0, match={"source": ["webhook:voice"]},
            continue_on_match=False, exec=str(fake_llm),
            env={"FAKE_LLM_REPLY": "", "RAN_MARKER": str(tmp_path / "llm_ran")},
        ),
        handler("echo", priority=99.0, exec="{sijba_plugins}/echo.py"),
    )
    return fake_llm


async def _drain_inbound(inbound):
    """Feed everything the pipeline re-queued back through process_inbound,
    the way the queue worker would."""
    seen = 0
    while seen < len(inbound):
        env = inbound[seen]
        seen += 1
        await pipeline.process_inbound(env)


async def test_every_voice_note_is_teed_into_the_daily(voice_chain, wired, db):
    inbound, _ = wired
    obsidian = register_channel("cursor-obs", "obsidian", triggers=["@obsidian"])

    await pipeline.process_inbound(voice_env("nur so ein gedanke zum thema x"))
    await _drain_inbound(inbound)

    texts = [m["payload"]["text"] for m in obsidian.messages_for("obsidian")]
    assert texts == ["@obsidian [voice] nur so ein gedanke zum thema x"]


async def test_keyword_note_lands_in_both_places(voice_chain, wired, db):
    inbound, _ = wired
    obsidian = register_channel("cursor-obs", "obsidian", triggers=["@obsidian"])

    await pipeline.process_inbound(voice_env("Einkauf Milch und Butter"))
    await _drain_inbound(inbound)

    texts = sorted(m["payload"]["text"] for m in obsidian.messages_for("obsidian"))
    assert texts == [
        "@obsidian [einkauf] Milch und Butter",
        "@obsidian [voice] Einkauf Milch und Butter",
    ]


async def test_regex_keyword_stops_before_the_llm(voice_chain, wired, db, tmp_path):
    inbound, _ = wired
    register_channel("cursor-obs", "obsidian", triggers=["@obsidian"])

    await pipeline.process_inbound(voice_env("Aufgabe: Rasen mähen"))
    await _drain_inbound(inbound)

    assert not (tmp_path / "llm_ran").exists(), "regex match must stop before router-llm"


async def test_actionable_freeform_note_is_routed_by_the_llm(voice_chain, wired, db):
    inbound, _ = wired
    obsidian = register_channel("cursor-obs", "obsidian", triggers=["@obsidian"])
    install_llm_reply(voice_chain, "@obsidian [task] Öl nachfüllen")

    await pipeline.process_inbound(voice_env("ich muss unbedingt noch Öl nachfüllen"))
    await _drain_inbound(inbound)

    texts = sorted(m["payload"]["text"] for m in obsidian.messages_for("obsidian"))
    assert texts == [
        "@obsidian [task] Öl nachfüllen",
        "@obsidian [voice] ich muss unbedingt noch Öl nachfüllen",
    ]


async def test_pure_thought_only_gets_teed(voice_chain, wired, db):
    inbound, _ = wired
    obsidian = register_channel("cursor-obs", "obsidian", triggers=["@obsidian"])
    install_llm_reply(voice_chain, "NONE")

    await pipeline.process_inbound(voice_env("schöner Sonnenuntergang heute"))
    await _drain_inbound(inbound)

    texts = [m["payload"]["text"] for m in obsidian.messages_for("obsidian")]
    assert texts == ["@obsidian [voice] schöner Sonnenuntergang heute"]


async def test_recherche_keyword_routes_to_its_own_channel(voice_chain, wired, db):
    inbound, _ = wired
    obsidian = register_channel("cursor-obs", "obsidian", triggers=["@obsidian"])
    recherche = register_channel("cursor-rech", "Recherche", triggers=["@Recherche"])

    await pipeline.process_inbound(voice_env("Recherche was kostet ein Tesla Model 3"))
    await _drain_inbound(inbound)

    assert [m["payload"]["text"] for m in obsidian.messages_for("obsidian")] == [
        "@obsidian [voice] Recherche was kostet ein Tesla Model 3"
    ]
    assert [m["payload"]["text"] for m in recherche.messages_for("Recherche")] == [
        "@Recherche was kostet ein Tesla Model 3"
    ]


async def test_channel_exchange_is_visible_in_its_own_transcript(voice_chain, wired, db):
    """What the channel received and its final reply thread under the channel's
    own id — not whatever chat_id the triggering webhook call happened to
    carry — so it reads like a conversation you had with @obsidian directly,
    the same one the app's Obsidian tile reads regardless of what routed a
    given message there."""
    import core.transcript as transcript
    import core.registry as registry
    import core.web.api as api

    inbound, _ = wired
    register_channel("cursor-obs", "obsidian", triggers=["@obsidian"])

    await pipeline.process_inbound(
        voice_env("Einkauf Kaffee", source_meta={"chat_id": "voice-42"})
    )
    await _drain_inbound(inbound)

    child_id = next(
        eid for eid, e in registry._pending_replies.items()
        if "[einkauf]" in e["payload"]["text"]
    )
    await api._handle_plugin_reply(
        {"channel": "obsidian", "envelope_id": child_id, "text": "→ Einkaufsliste: Kaffee"}
    )

    convo = [(m["role"], m["text"]) for m in transcript.list_messages("obsidian", "obsidian")]
    assert convo == [
        ("user", "@obsidian [voice] Einkauf Kaffee"),
        ("user", "@obsidian [einkauf] Kaffee"),
        ("bot", "→ Einkaufsliste: Kaffee"),
    ]
    assert not transcript.list_messages("voice-42", "obsidian")


async def test_voice_exchange_is_mirrored_into_the_pipeline_stream(voice_chain, wired, db):
    """The Pipeline / per-webhook tiles read one cross-source bucket
    (transcript.PIPELINE_CHAT_ID), so a voice note that routes off to a plugin
    channel still shows there — both the incoming text and the reply."""
    import core.transcript as transcript
    import core.registry as registry
    import core.web.api as api

    inbound, _ = wired
    register_channel("cursor-obs", "obsidian", triggers=["@obsidian"])

    await pipeline.process_inbound(voice_env("Einkauf Kaffee"))
    await _drain_inbound(inbound)

    child_id = next(
        eid for eid, e in registry._pending_replies.items()
        if "[einkauf]" in e["payload"]["text"]
    )
    await api._handle_plugin_reply(
        {"channel": "obsidian", "envelope_id": child_id, "text": "→ Einkaufsliste: Kaffee"}
    )

    convo = [
        (m["role"], m["text"], m["source"])
        for m in transcript.list_messages(
            transcript.PIPELINE_CHAT_ID, transcript.PIPELINE_CHANNEL
        )
    ]
    assert convo == [
        ("user", "Einkauf Kaffee", "webhook:voice"),
        ("bot", "→ Einkaufsliste: Kaffee", "webhook:voice"),
    ]


async def test_activity_broadcast_pushes_the_incoming_note_live(voice_chain, wired, db, replies):
    """A real POST /webhook/<token> sets _activity_broadcast — the pipeline
    then emits a user_message event so an open app shows it without a reload."""
    inbound, _ = wired

    await pipeline.process_inbound(
        voice_env("nur so ein gedanke", _activity_broadcast=True)
    )
    await _drain_inbound(inbound)

    events = [e for e in replies.drain() if e.get("type") == "user_message"]
    assert [(e["text"], e["source"], e["channel"]) for e in events] == [
        ("nur so ein gedanke", "webhook:voice", "pipeline")
    ]


async def test_incoming_note_is_not_broadcast_without_the_flag(voice_chain, wired, db, replies):
    """The web composer's "send as webhook" already echoes locally — no flag,
    no live user_message event (it is still mirrored into the stream)."""
    import core.transcript as transcript

    inbound, _ = wired
    await pipeline.process_inbound(voice_env("nur so ein gedanke"))
    await _drain_inbound(inbound)

    assert not [e for e in replies.drain() if e.get("type") == "user_message"]
    assert transcript.list_messages(
        transcript.PIPELINE_CHAT_ID, transcript.PIPELINE_CHANNEL
    )[0]["text"] == "nur so ein gedanke"


async def test_obsidian_reply_goes_back_out_as_the_voice_source(voice_chain, wired, db):
    """The whole point of keeping the origin source: the cursor agent's reply
    ("→ Einkaufsliste: …") is enqueued outbound as `webhook:voice`, so the
    voice-scoped outbound handler (Pushover) picks it up — even though it
    travelled voice → tee/inject → channel as `internal`."""
    import core.registry as registry
    import core.web.api as api

    inbound, outbound = wired
    register_channel("cursor-obs", "obsidian", triggers=["@obsidian"])

    await pipeline.process_inbound(voice_env("Einkauf Kaffee"))
    await _drain_inbound(inbound)

    child_id = next(
        eid for eid, entry in registry._pending_replies.items()
        if "[einkauf]" in entry["payload"]["text"]
    )
    await api._handle_plugin_reply(
        {"channel": "obsidian", "envelope_id": child_id, "text": "→ Einkaufsliste: Kaffee"}
    )

    assert [(o["source"], o["direction"], o["text"]) for o in outbound] == [
        ("webhook:voice", "outbound", "→ Einkaufsliste: Kaffee")
    ]


async def test_jinja_filters_and_conditionals_in_inject(voice_chain, wired, db):
    """inject templates are Jinja2, not str.format."""
    import core.state as state

    for h in state.file_handlers:
        if h["id"] == "router-regex":
            h["env"]["ROUTES"] = json.dumps([
                {"pattern": r"^merk dir\s+",
                 "inject": "@obsidian [{{ 'task' if 'muss' in text|lower else 'idea' }}] {{ rest | lower }}"},
            ])
            h["env"].pop("ROUTES_FILE", None)

    inbound, _ = wired
    obsidian = register_channel("cursor-obs", "obsidian", triggers=["@obsidian"])

    await pipeline.process_inbound(voice_env("merk dir ich MUSS Öl kaufen"))
    await _drain_inbound(inbound)

    texts = sorted(m["payload"]["text"] for m in obsidian.messages_for("obsidian"))
    assert texts == [
        "@obsidian [task] ich muss öl kaufen",
        "@obsidian [voice] merk dir ich MUSS Öl kaufen",
    ]


def install_llm_reply(fake_llm_path, reply: str) -> None:
    """Repoint the router-llm handler's FAKE_LLM_REPLY."""
    import core.state as state

    for h in state.file_handlers:
        if h["id"] == "router-llm":
            h["env"]["FAKE_LLM_REPLY"] = reply
