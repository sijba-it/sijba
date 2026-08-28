"""Handler invocation: exec_json subprocess contract, context, timeouts."""
from __future__ import annotations
import os
import stat

import core.plugin_runner as plugin_runner
import core.store as store
from conftest import handler, install


def script(tmp_path, name: str, body: str) -> str:
    p = tmp_path / name
    p.write_text("#!/usr/bin/env python3\n" + body)
    p.chmod(p.stat().st_mode | stat.S_IEXEC)
    return str(p)


ECHO_BACK = """
import json, sys
env = json.load(sys.stdin)
print(json.dumps({"continue": True, "respond": {"text": env["pipeline_text"]}}))
"""


async def test_exec_json_passes_envelope_on_stdin_and_parses_stdout(tmp_path, db):
    h = handler("echo", exec=script(tmp_path, "echo.py", ECHO_BACK))
    env = {"id": "e1", "pipeline_text": "hallo", "source_meta": {}}

    resp = await plugin_runner.invoke(h, env, tmp_path)

    assert resp["respond"]["text"] == "hallo"


async def test_exec_json_invalid_output_continues_pipeline(tmp_path, db):
    h = handler("bad", exec=script(tmp_path, "bad.py", "print('not json')"))

    resp = await plugin_runner.invoke(h, {"id": "e1", "source_meta": {}}, tmp_path)

    assert resp["continue"] is True
    assert resp["_error"], "a failed invoke must be distinguishable from a real response"


async def test_exec_json_nonzero_exit_continues_pipeline(tmp_path, db):
    h = handler("boom", exec=script(tmp_path, "boom.py", "import sys; sys.exit(3)"))

    resp = await plugin_runner.invoke(h, {"id": "e1", "source_meta": {}}, tmp_path)

    assert resp["continue"] is True
    assert resp["_error"], "a failed invoke must be distinguishable from a real response"


async def test_exec_json_timeout_continues_pipeline(tmp_path, db):
    h = handler(
        "slow",
        exec=script(tmp_path, "slow.py", "import time; time.sleep(5)"),
        timeout_seconds=0.3,
    )

    resp = await plugin_runner.invoke(h, {"id": "e1", "source_meta": {}}, tmp_path)

    assert resp["continue"] is True
    assert resp["_error"], "a failed invoke must be distinguishable from a real response"


async def test_missing_executable_continues_pipeline(tmp_path, db):
    h = handler("gone", exec=str(tmp_path / "does-not-exist"))

    resp = await plugin_runner.invoke(h, {"id": "e1", "source_meta": {}}, tmp_path)

    assert resp["continue"] is True
    assert resp["_error"], "a failed invoke must be distinguishable from a real response"


async def test_unknown_handler_type_continues_pipeline(tmp_path, db):
    h = handler("weird", type="quantum")

    resp = await plugin_runner.invoke(h, {"id": "e1", "source_meta": {}}, tmp_path)

    assert resp["continue"] is True
    assert resp["_error"], "a failed invoke must be distinguishable from a real response"


async def test_env_block_is_expanded_from_the_host_environment(tmp_path, db, monkeypatch):
    monkeypatch.setenv("SECRET_KEY", "s3cret")
    body = """
import json, os, sys
json.load(sys.stdin)
print(json.dumps({"continue": True, "pipeline_text": os.environ.get("LLM_API_KEY", "")}))
"""
    h = handler("llm", exec=script(tmp_path, "llm.py", body), env={"LLM_API_KEY": "${SECRET_KEY}"})

    resp = await plugin_runner.invoke(h, {"id": "e1", "source_meta": {}}, tmp_path)

    assert resp["pipeline_text"] == "s3cret"


# ------------------------------------------------------------------
# Context store integration
# ------------------------------------------------------------------

CONTEXT_ROUNDTRIP = """
import json, sys
env = json.load(sys.stdin)
seen = env.get("context", {}).get("n", 0)
print(json.dumps({"continue": True, "pipeline_text": str(seen),
                  "context_update": {"n": seen + 1}}))
"""


async def test_context_is_injected_and_persisted(tmp_path, db):
    h = handler(
        "llm",
        exec=script(tmp_path, "ctx.py", CONTEXT_ROUNDTRIP),
        context={"namespace": "llm", "key": "chat_id"},
    )
    env = {"id": "e1", "pipeline_text": "hi", "source_meta": {"chat_id": "42"}}

    first = await plugin_runner.invoke(h, env, tmp_path)
    assert first["pipeline_text"] == "0"
    assert store.get("llm", "42") == {"n": 1}

    second = await plugin_runner.invoke(h, env, tmp_path)
    assert second["pipeline_text"] == "1", "context must survive between turns"


async def test_context_is_keyed_per_chat(tmp_path, db):
    h = handler(
        "llm",
        exec=script(tmp_path, "ctx.py", CONTEXT_ROUNDTRIP),
        context={"namespace": "llm", "key": "chat_id"},
    )

    await plugin_runner.invoke(
        h, {"id": "e1", "pipeline_text": "x", "source_meta": {"chat_id": "a"}}, tmp_path
    )
    resp = await plugin_runner.invoke(
        h, {"id": "e2", "pipeline_text": "x", "source_meta": {"chat_id": "b"}}, tmp_path
    )

    assert resp["pipeline_text"] == "0", "chat b must not see chat a's context"


async def test_context_update_is_stripped_from_the_response(tmp_path, db):
    h = handler(
        "llm",
        exec=script(tmp_path, "ctx.py", CONTEXT_ROUNDTRIP),
        context={"namespace": "llm", "key": "chat_id"},
    )

    resp = await plugin_runner.invoke(
        h, {"id": "e1", "pipeline_text": "x", "source_meta": {"chat_id": "1"}}, tmp_path
    )

    assert "context_update" not in resp


async def test_handler_without_context_gets_none_injected(tmp_path, db):
    body = """
import json, sys
env = json.load(sys.stdin)
print(json.dumps({"continue": True, "pipeline_text": str("context" in env)}))
"""
    h = handler("plain", exec=script(tmp_path, "plain.py", body))

    resp = await plugin_runner.invoke(h, {"id": "e1", "source_meta": {}}, tmp_path)

    assert resp["pipeline_text"] == "False"


# ------------------------------------------------------------------
# Router context (receive_handler_context)
# ------------------------------------------------------------------

ROUTER_VIEW = """
import json, sys
env = json.load(sys.stdin)
print(json.dumps({"continue": True,
                  "pipeline_text": ",".join(h["id"] for h in env.get("handlers", []))}))
"""


async def test_router_receives_handler_summaries(tmp_path, db):
    install(
        handler("pods", router_hint="Kubernetes pods"),
        handler("no-hint"),
        handler("llm", router_hint="General LLM"),
    )
    h = handler("router", exec=script(tmp_path, "r.py", ROUTER_VIEW), receive_handler_context=True)

    resp = await plugin_runner.invoke(h, {"id": "e1", "source_meta": {}}, tmp_path)

    assert resp["pipeline_text"] == "pods,llm", "only handlers with a router_hint"


async def test_handler_summaries_not_sent_without_opt_in(tmp_path, db):
    install(handler("pods", router_hint="Kubernetes pods"))
    h = handler("plain", exec=script(tmp_path, "r.py", ROUTER_VIEW))

    resp = await plugin_runner.invoke(h, {"id": "e1", "source_meta": {}}, tmp_path)

    assert resp["pipeline_text"] == ""


# ------------------------------------------------------------------
# Path resolution
# ------------------------------------------------------------------

def test_builtin_plugins_placeholder_resolves():
    resolved = plugin_runner._resolve_exec("{sijba_plugins}/echo.py", None)

    assert resolved.endswith("plugins-builtin/echo.py")
    assert os.path.isabs(resolved)


def test_relative_exec_resolves_against_config_dir(tmp_path):
    resolved = plugin_runner._resolve_exec("./my/plugin.py", tmp_path)

    assert resolved == str(tmp_path / "my" / "plugin.py")


def test_absolute_exec_is_left_alone(tmp_path):
    assert plugin_runner._resolve_exec("/usr/bin/true", tmp_path) == "/usr/bin/true"


async def test_persistent_handler_without_instance_continues(tmp_path, db):
    h = handler("ghost", lifecycle="persistent")

    resp = await plugin_runner.invoke(h, {"id": "e1", "source_meta": {}}, tmp_path)

    assert resp["continue"] is True
    assert resp["_error"], "a failed invoke must be distinguishable from a real response"


# ------------------------------------------------------------------
# The router must be able to see plugin channels, not just handlers
# ------------------------------------------------------------------

async def test_router_sees_plugin_channels(tmp_path, db):
    """Without this the router is blind to Cursor/Claude and can only ever
    answer "no route" — the channels live in the registry, not state.handlers."""
    from conftest import register_channel

    register_channel("cursor", "projekt-a", triggers=["projekt-a", "@projekt-a"])
    h = handler("router", exec=script(tmp_path, "r.py", ROUTER_VIEW),
                receive_handler_context=True)

    resp = await plugin_runner.invoke(h, {"id": "e1", "source_meta": {}}, tmp_path)

    assert resp["pipeline_text"] == "@projekt-a", \
        "must be addressed by its @ trigger, which is what re-injection matches"


async def test_router_sees_handlers_and_channels_together(tmp_path, db):
    from conftest import register_channel

    install(handler("pods", router_hint="Kubernetes"))
    register_channel("cursor", "projekt-a", triggers=["@projekt-a"])
    h = handler("router", exec=script(tmp_path, "r.py", ROUTER_VIEW),
                receive_handler_context=True)

    resp = await plugin_runner.invoke(h, {"id": "e1", "source_meta": {}}, tmp_path)

    assert resp["pipeline_text"] == "pods,@projekt-a"


async def test_channel_without_triggers_is_advertised_as_its_id(tmp_path, db):
    from conftest import register_channel

    register_channel("cursor", "projekt-a", triggers=[])
    h = handler("router", exec=script(tmp_path, "r.py", ROUTER_VIEW),
                receive_handler_context=True)

    resp = await plugin_runner.invoke(h, {"id": "e1", "source_meta": {}}, tmp_path)

    assert resp["pipeline_text"] == "@projekt-a", "always an @mention"


async def test_bare_registered_trigger_is_advertised_as_an_at_mention(tmp_path, db):
    """The cursor plugin registers a bare project name; the router must still
    be told to emit the @ form, since that is all the trigger match takes."""
    from conftest import register_channel

    register_channel("cursor", "cursor-recherche", triggers=["Recherche"])
    h = handler("router", exec=script(tmp_path, "r.py", ROUTER_VIEW),
                receive_handler_context=True)

    resp = await plugin_runner.invoke(h, {"id": "e1", "source_meta": {}}, tmp_path)

    assert resp["pipeline_text"] == "@Recherche"


async def test_router_is_not_offered_itself_as_a_target(tmp_path, db):
    """Routing to itself would just loop until max_inject_depth."""
    install(handler("router", router_hint="I route things"))
    h = handler("router", exec=script(tmp_path, "r.py", ROUTER_VIEW),
                receive_handler_context=True)

    resp = await plugin_runner.invoke(h, {"id": "e1", "source_meta": {}}, tmp_path)

    assert resp["pipeline_text"] == ""
