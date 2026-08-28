"""Shared fixtures.

The core modules keep their state in module-level globals (connections,
registries, handler lists), so every test gets them reset explicitly.
"""
from __future__ import annotations
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import core.db as database  # noqa: E402
import core.msg_queue as queue  # noqa: E402
import core.pipeline as pipeline  # noqa: E402
import core.registry as registry  # noqa: E402
import core.reply as reply  # noqa: E402
import core.state as state  # noqa: E402
import core.store as store  # noqa: E402
import core.transcript as transcript  # noqa: E402


@pytest.fixture(autouse=True)
def clean_state():
    """Reset all cross-test global state."""
    state.handlers = []
    state.file_handlers = []
    state.registered_handlers = {}
    state.handler_states = {}
    state.persistent = {}
    state.configure_log_buffer(1000)
    state.log_buffer.clear()
    state.sse_queues = set()
    state.reply_queues = set()

    registry._plugins.clear()
    registry._channel_to_plugin.clear()
    registry._pending_replies.clear()
    yield


@pytest.fixture
def db(tmp_path):
    """A fresh SQLite file wired into queue/store/transcript."""
    path = str(tmp_path / "test.db")
    database.connect(path)
    queue.init()
    store.init()
    transcript.init()
    registry.init()
    return path


class Sent(list):
    """Records envelopes handed to an enqueue callback."""


@pytest.fixture
def wired(tmp_path):
    """pipeline.configure() with capturing enqueue callbacks.

    Returns (inbound, outbound) lists that collect what the pipeline enqueues.
    """
    inbound, outbound = Sent(), Sent()
    reply.configure(enqueue_outbound=outbound.append)
    pipeline.configure(
        enqueue_inbound=inbound.append,
        enqueue_outbound=outbound.append,
        config_dir=tmp_path,
        max_inject_depth=5,
    )
    return inbound, outbound


@pytest.fixture
def replies():
    """Collects everything broadcast to SSE/WS reply clients."""
    import asyncio

    q: asyncio.Queue = asyncio.Queue(maxsize=100)
    state.reply_queues.add(q)

    class Collector:
        def drain(self) -> list:
            out = []
            while not q.empty():
                out.append(q.get_nowait())
            return out

    return Collector()


def handler(hid: str, **overrides) -> dict:
    """Build a handler dict with the loader's defaults applied."""
    h = {
        "id": hid,
        "priority": 50.0,
        "direction": "inbound",
        "lifecycle": "per_message",
        "type": "exec_json",
        "category": [],
        "required": False,
        "await_response": True,
        "allow_direct_inject": False,
        "continue_on_match": True,
        "timeout_seconds": 10,
    }
    h.update(overrides)
    return h


def install(*handlers: dict) -> None:
    """Put handlers into state as if they had been loaded from files."""
    state.file_handlers = list(handlers)
    for h in handlers:
        state.handler_states.setdefault(h["id"], "healthy")
    registry.rebuild()


class FakeWS:
    """Stands in for a plugin's WebSocket; records what Core sends it."""

    def __init__(self, fail: bool = False):
        self.sent: list = []
        self.fail = fail

    async def send_json(self, data: dict) -> None:
        if self.fail:
            raise RuntimeError("socket closed")
        self.sent.append(data)

    def messages_for(self, channel_id: str) -> list:
        return [m for m in self.sent if m.get("channel") == channel_id]


def register_channel(
    plugin_id: str,
    channel_id: str,
    triggers: list | None = None,
    ws: FakeWS | None = None,
) -> FakeWS:
    ws = ws or FakeWS()
    registry.plugin_register(plugin_id, ws)
    registry.plugin_register_channel(
        plugin_id, channel_id, {"label": channel_id, "triggers": triggers or []}
    )
    return ws
