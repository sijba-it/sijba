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
import textwrap

import pytest

import core.pipeline as pipeline
from conftest import handler, install, register_channel

ROUTES_VOICE = [
    {"pattern": r"^(einkauf|einkaufsliste|einkaufszettel|einkaufen|besorgen|kaufen)\b\s*[:,-]?\s*",
     "inject": "@obsidian [einkauf] {rest}"},
    {"pattern": r"^(aufgabe|task|todo|to-do)\b\s*[:,-]?\s*",
     "inject": "@obsidian [task] {rest}"},
    {"pattern": r"^(termin|kalender)\b\s*[:,-]?\s*",
     "inject": "@obsidian [termin] {rest}"},
    {"pattern": r"^(recherche|recherchiere|recherchier)\b\s*[:,-]?\s*",
     "inject": "@Recherche {rest}"},
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
        handler(
            "voice-tee", priority=10.0, match={"source": ["webhook:voice"]},
            continue_on_match=True,
            exec="{sijba_plugins}/regex-router/main.py",
            env={
                "ROUTES": json.dumps([{"pattern": ".*", "inject": "@obsidian [voice] {text}"}]),
                "ON_MATCH_CONTINUE": "true",
            },
        ),
        handler(
            "router-regex", priority=70.0, match={"source": ["webhook:voice"]},
            continue_on_match=False,
            exec="{sijba_plugins}/regex-router/main.py",
            env={"ROUTES_FILE": str(routes_file)},
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


def install_llm_reply(fake_llm_path, reply: str) -> None:
    """Repoint the router-llm handler's FAKE_LLM_REPLY."""
    import core.state as state

    for h in state.file_handlers:
        if h["id"] == "router-llm":
            h["env"]["FAKE_LLM_REPLY"] = reply
