"""Outbound delivery: what happens when nobody takes the message.

An outbound envelope that no handler delivered used to be marked `done`
exactly like a delivered one — so a reply that arrived while its remote
handler was restarting (or had not re-registered after a Core restart)
disappeared without a trace. It is now handed back to the queue.

The other half of that guarantee matters just as much: a web/app reply has
no outbound handler *by design* (the app listens on the reply stream), so
"nobody took it" must stay the normal, silent, non-retried case there.
"""
from __future__ import annotations

import pytest

import core.msg_queue as queue
import core.pipeline as pipeline
import core.plugin_runner as plugin_runner
from conftest import handler, install


def outbound(source: str = "webhook:voice", **overrides) -> dict:
    env = {
        "id": "out-1",
        "parent_id": None,
        "direction": "outbound",
        "source": source,
        "source_meta": {"chat_id": "web"},
        "text": "answer",
        "pipeline_text": "answer",
        "depth": 0,
        "metadata": {},
    }
    env.update(overrides)
    return env


class Invocations(list):
    """Records invoked handler ids; `responses` cans what they return."""

    def __init__(self):
        super().__init__()
        self.responses: dict = {}


@pytest.fixture
def invoked(monkeypatch):
    recorded = Invocations()

    async def fake_invoke(h, env, config_dir):
        recorded.append(h["id"])
        return recorded.responses.get(h["id"], {"continue": True})

    monkeypatch.setattr(plugin_runner, "invoke", fake_invoke)
    return recorded


def sink(hid: str = "pushover", sources: list | None = None) -> dict:
    return handler(
        hid,
        direction="outbound",
        match={"source": sources if sources is not None else ["webhook:voice"]},
    )


# ------------------------------------------------------------------
# Nothing took it
# ------------------------------------------------------------------

async def test_message_nobody_took_is_retried(invoked):
    with pytest.raises(queue.Retry):
        await pipeline.process_outbound(outbound())

    assert invoked == []


async def test_web_replies_are_not_retried(invoked):
    """The app has its own return path — no outbound handler is expected,
    and retrying here would fail every single chat reply."""
    await pipeline.process_outbound(outbound(source="web"))
    await pipeline.process_outbound(outbound(source="app"))
    await pipeline.process_outbound(outbound(source="internal"))


async def test_named_target_that_does_not_exist_is_retried(invoked):
    """An explicit respond target was asked for by name; its absence is a
    failure even for a source that would otherwise be self-delivering."""
    with pytest.raises(queue.Retry):
        await pipeline.process_outbound(
            outbound(source="web", _respond_target="pushover")
        )


# ------------------------------------------------------------------
# Something took it (or tried to)
# ------------------------------------------------------------------

async def test_delivered_message_is_not_retried(invoked):
    install(sink())

    await pipeline.process_outbound(outbound())

    assert invoked == ["pushover"]


async def test_handler_that_failed_to_invoke_does_not_count(invoked):
    """plugin_runner.invoke never raises — a dead remote handler returns an
    ordinary-looking response carrying `_error`."""
    install(sink())
    invoked.responses["pushover"] = {"continue": True, "_error": "connection refused"}

    with pytest.raises(queue.Retry):
        await pipeline.process_outbound(outbound())

    assert invoked == ["pushover"]


async def test_handler_registered_after_the_message_still_gets_it(invoked):
    """The restart window: the message is queued while nothing is registered,
    and delivered on a later attempt once the handler is back."""
    env = outbound()
    with pytest.raises(queue.Retry):
        await pipeline.process_outbound(env)

    install(sink())
    await pipeline.process_outbound(env)

    assert invoked == ["pushover"]


async def test_stopped_handler_is_not_treated_as_delivery(invoked):
    import core.state as state

    install(sink())
    state.handler_states["pushover"] = "stopped"

    with pytest.raises(queue.Retry):
        await pipeline.process_outbound(outbound())

    assert invoked == []
