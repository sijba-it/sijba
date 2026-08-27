"""Event fan-out to connected clients, and the log ring buffer."""
from __future__ import annotations
import asyncio
import logging

import core.log as log
import core.state as state


def test_broadcast_reaches_every_subscriber():
    a: asyncio.Queue = asyncio.Queue(maxsize=10)
    b: asyncio.Queue = asyncio.Queue(maxsize=10)
    state.reply_queues.update({a, b})

    state.broadcast_reply({"type": "reply", "text": "hi"})

    assert a.get_nowait()["text"] == "hi"
    assert b.get_nowait()["text"] == "hi"


def test_full_queue_drops_the_oldest_event_not_the_subscriber():
    """A backgrounded client must keep receiving new events once it catches
    up — dropping its queue would leave the connection open but dead."""
    q: asyncio.Queue = asyncio.Queue(maxsize=2)
    state.reply_queues.add(q)

    for i in range(4):
        state.broadcast_reply({"n": i})

    assert q in state.reply_queues, "subscriber must stay subscribed"
    assert [q.get_nowait()["n"] for _ in range(2)] == [2, 3], "newest survive"


def test_slow_subscriber_does_not_block_the_others():
    slow: asyncio.Queue = asyncio.Queue(maxsize=1)
    fast: asyncio.Queue = asyncio.Queue(maxsize=10)
    state.reply_queues.update({slow, fast})

    for i in range(3):
        state.broadcast_reply({"n": i})

    assert fast.qsize() == 3


def test_log_buffer_is_bounded():
    state.configure_log_buffer(3)

    for i in range(10):
        state.broadcast_log({"msg": str(i)})

    assert [e["msg"] for e in state.log_buffer] == ["7", "8", "9"]


def test_resizing_the_log_buffer_keeps_what_fits():
    state.configure_log_buffer(10)
    for i in range(5):
        state.broadcast_log({"msg": str(i)})

    state.configure_log_buffer(2)

    assert [e["msg"] for e in state.log_buffer] == ["3", "4"]


def test_log_broadcast_also_feeds_live_viewers():
    q: asyncio.Queue = asyncio.Queue(maxsize=10)
    state.sse_queues.add(q)

    state.broadcast_log({"msg": "hallo"})

    assert q.get_nowait()["msg"] == "hallo"


def test_logger_emits_into_the_buffer():
    """The stdlib logging handler is what actually populates the buffer."""
    handler = log._SseBroadcastHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    record = logging.LogRecord(
        name="sijba.test", level=logging.INFO, pathname=__file__, lineno=1,
        msg="etwas passiert", args=(), exc_info=None,
    )

    handler.emit(record)

    entry = state.log_buffer[-1]
    assert entry["msg"] == "etwas passiert"
    assert entry["level"] == "INFO"
    assert entry["logger"] == "sijba.test"
    assert entry["ts"]
