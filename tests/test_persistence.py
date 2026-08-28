"""SQLite-backed queue, context store, transcript, and handler loading."""
from __future__ import annotations
import asyncio
import json

import core.db as database
import core.handler_loader as handler_loader
import core.msg_queue as queue
import core.store as store
import core.transcript as transcript


# ------------------------------------------------------------------
# Queue
# ------------------------------------------------------------------

def test_enqueue_and_depths(db):
    queue.enqueue_inbound({"id": "a", "text": "hi"})
    queue.enqueue_outbound({"id": "b", "text": "ho"})

    d = queue.depths()
    assert d["inbound"] == {"pending": 1}
    assert d["outbound"] == {"pending": 1}


def test_enqueue_assigns_an_id(db):
    mid = queue.enqueue_inbound({"text": "hi"})
    assert mid


def test_enqueue_is_idempotent_per_id(db):
    queue.enqueue_inbound({"id": "a", "text": "hi"})
    queue.enqueue_inbound({"id": "a", "text": "different"})

    assert queue.depths()["inbound"] == {"pending": 1}


async def test_worker_processes_and_marks_done(db):
    queue.enqueue_inbound({"id": "a", "text": "hi"})
    seen = []

    async def fn(env):
        seen.append(env)

    task = asyncio.create_task(queue.run_inbound_worker(fn))
    await asyncio.sleep(0.15)
    task.cancel()

    assert [e["text"] for e in seen] == ["hi"]
    assert queue.depths()["inbound"] == {"done": 1}


async def test_worker_marks_failed_and_keeps_running(db):
    queue.enqueue_inbound({"id": "a", "text": "boom"})
    queue.enqueue_inbound({"id": "b", "text": "ok"})
    seen = []

    async def fn(env):
        if env["text"] == "boom":
            raise RuntimeError("nope")
        seen.append(env["text"])

    task = asyncio.create_task(queue.run_inbound_worker(fn))
    await asyncio.sleep(0.2)
    task.cancel()

    assert seen == ["ok"], "one bad message must not stall the queue"
    assert queue.depths()["inbound"] == {"failed": 1, "done": 1}


async def test_worker_retries_a_message_nobody_delivered(db, monkeypatch):
    """Retry is the whole point: an undelivered message stays in the queue
    instead of being marked done like a delivered one."""
    monkeypatch.setattr(queue, "BACKOFF_SECONDS", (0,))
    queue.enqueue_outbound({"id": "a", "text": "hi"})
    tries = []

    async def fn(env):
        tries.append(env["id"])
        if len(tries) < 3:
            raise queue.Retry("no outbound handler took it")

    task = asyncio.create_task(queue.run_outbound_worker(fn))
    await asyncio.sleep(0.25)
    task.cancel()

    assert len(tries) == 3
    assert queue.depths()["outbound"] == {"done": 1}


async def test_worker_gives_up_loudly_instead_of_retrying_forever(db, monkeypatch):
    monkeypatch.setattr(queue, "BACKOFF_SECONDS", (0,))
    queue.enqueue_outbound({"id": "a", "text": "hi"})
    tries = []

    async def fn(env):
        tries.append(env["id"])
        raise queue.Retry("nobody home")

    task = asyncio.create_task(queue.run_outbound_worker(fn))
    await asyncio.sleep(0.4)
    task.cancel()

    assert len(tries) == queue.MAX_ATTEMPTS
    assert queue.depths()["outbound"] == {"failed": 1}
    attempts, last_error = database.query_one(
        "SELECT attempts, last_error FROM outbound_queue WHERE id = 'a'"
    )
    assert attempts == queue.MAX_ATTEMPTS
    assert "nobody home" in last_error


def test_retry_waits_for_its_backoff(db):
    queue.enqueue_outbound({"id": "a", "text": "hi"})
    queue._claim_next(queue.OUTBOUND)

    queue._defer(queue.OUTBOUND, "a", "nobody home")

    assert queue.depths()["outbound"] == {"pending": 1}
    assert queue._claim_next(queue.OUTBOUND) is None, "re-claimed before its backoff"


def test_recover_claimed_requeues_messages_orphaned_by_a_restart(db):
    queue.enqueue_inbound({"id": "a", "text": "hi"})
    queue.enqueue_outbound({"id": "b", "text": "ho"})
    queue._claim_next(queue.INBOUND)
    queue._claim_next(queue.OUTBOUND)
    assert queue.depths() == {"inbound": {"claimed": 1}, "outbound": {"claimed": 1}}

    assert queue.recover_claimed() == 2

    assert queue.depths() == {"inbound": {"pending": 1}, "outbound": {"pending": 1}}
    assert queue._claim_next(queue.INBOUND)[0] == "a", "must be processable again"


async def test_worker_preserves_envelope_fields(db):
    queue.enqueue_inbound(
        {"id": "a", "text": "hi", "source_meta": {"chat_id": "42"}, "depth": 2}
    )
    seen = []

    task = asyncio.create_task(queue.run_inbound_worker(lambda e: _collect(seen, e)))
    await asyncio.sleep(0.15)
    task.cancel()

    assert seen[0]["source_meta"] == {"chat_id": "42"}
    assert seen[0]["depth"] == 2


async def _collect(bucket, env):
    bucket.append(env)


def test_queue_survives_reinit(db):
    """The queue is on disk so a restart does not lose pending work."""
    import core.db as database

    queue.enqueue_inbound({"id": "a", "text": "hi"})
    database.connect(db)  # simulate process restart
    queue.init()

    assert queue.depths()["inbound"] == {"pending": 1}


# ------------------------------------------------------------------
# Context store
# ------------------------------------------------------------------

def test_store_set_get_delete(db):
    store.set("llm", "chat-1", {"history": [1, 2]})

    assert store.get("llm", "chat-1") == {"history": [1, 2]}
    store.delete("llm", "chat-1")
    assert store.get("llm", "chat-1") is None


def test_store_get_missing_returns_none(db):
    assert store.get("llm", "nope") is None


def test_store_set_overwrites(db):
    store.set("llm", "chat-1", {"n": 1})
    store.set("llm", "chat-1", {"n": 2})

    assert store.get("llm", "chat-1") == {"n": 2}


def test_store_namespaces_are_isolated(db):
    store.set("llm", "k", "a")
    store.set("router", "k", "b")

    assert store.get("llm", "k") == "a"
    assert store.get("router", "k") == "b"


def test_store_list_and_clear_namespace(db):
    store.set("llm", "a", 1)
    store.set("llm", "b", 2)
    store.set("other", "c", 3)

    listed = store.list_ns("llm")
    assert set(listed) == {"a", "b"}
    assert listed["a"]["value"] == 1

    assert store.clear_ns("llm") == 2
    assert store.list_ns("llm") == {}
    assert store.get("other", "c") == 3


# ------------------------------------------------------------------
# Transcript
# ------------------------------------------------------------------

def test_transcript_roundtrip(db):
    transcript.append(chat_id="web", channel="pipeline", role="user", text="hallo")
    transcript.append(
        chat_id="web", channel="pipeline", role="bot", text="hi", handler_id="echo"
    )

    msgs = transcript.list_messages("web")
    assert [(m["role"], m["text"]) for m in msgs] == [("user", "hallo"), ("bot", "hi")]
    assert msgs[1]["handler_id"] == "echo"


def test_transcript_is_ordered_oldest_first(db):
    for i in range(5):
        transcript.append(chat_id="web", channel="pipeline", role="user", text=str(i))

    assert [m["text"] for m in transcript.list_messages("web")] == list("01234")


def test_transcript_channels_are_isolated(db):
    transcript.append(chat_id="web", channel="pipeline", role="user", text="a")
    transcript.append(chat_id="web", channel="projekt-a", role="user", text="b")

    assert [m["text"] for m in transcript.list_messages("web", "pipeline")] == ["a"]
    assert [m["text"] for m in transcript.list_messages("web", "projekt-a")] == ["b"]


def test_transcript_prunes_beyond_the_cap(db):
    for i in range(transcript.MAX_PER_CHANNEL + 20):
        transcript.append(chat_id="web", channel="pipeline", role="user", text=str(i))

    msgs = transcript.list_messages("web", limit=transcript.API_MAX_LIMIT)
    assert len(msgs) == transcript.MAX_PER_CHANNEL
    assert msgs[-1]["text"] == str(transcript.MAX_PER_CHANNEL + 19), "newest kept"


def test_transcript_truncates_oversized_text(db):
    transcript.append(
        chat_id="web", channel="pipeline", role="user", text="x" * 20000
    )

    text = transcript.list_messages("web")[0]["text"]
    assert len(text) <= transcript.MAX_TEXT_CHARS
    assert text.endswith("[truncated]")


def test_transcript_limit_is_clamped(db):
    for i in range(10):
        transcript.append(chat_id="web", channel="pipeline", role="user", text=str(i))

    assert len(transcript.list_messages("web", limit=3)) == 3
    assert len(transcript.list_messages("web", limit=99999)) == 10
    assert len(transcript.list_messages("web", limit=0)) == 1


def test_transcript_skips_empty_messages(db):
    transcript.append(chat_id="web", channel="pipeline", role="user", text="")
    transcript.append(chat_id="", channel="pipeline", role="user", text="hi")

    assert transcript.list_messages("web") == []


def test_transcript_keeps_attachments(db):
    atts = [{"id": "f1", "filename": "bild.png", "url": "/api/upload/f1"}]
    transcript.append(
        chat_id="web", channel="pipeline", role="user", text="", attachments=atts
    )

    assert transcript.list_messages("web")[0]["attachments"] == atts


def test_transcript_normalises_unknown_role(db):
    transcript.append(chat_id="web", channel="pipeline", role="hacker", text="hi")

    assert transcript.list_messages("web")[0]["role"] == "sys"


def test_mark_delivery_failed_flags_bot_reply(db):
    transcript.append(
        chat_id="web",
        channel="pipeline",
        role="bot",
        text="hi",
        envelope_id="env-in",
    )

    ctx = transcript.mark_delivery_failed("env-in", "nobody home")

    assert ctx == {"channel": "pipeline", "chat_id": "web"}
    msg = transcript.list_messages("web")[0]
    assert msg["delivery_status"] == "failed"
    assert msg["delivery_error"] == "nobody home"


def test_mark_delivery_failed_is_noop_without_a_bot_row(db):
    assert transcript.mark_delivery_failed("missing", "x") is None


async def test_outbound_give_up_marks_transcript_and_broadcasts(
    db, monkeypatch, replies
):
    monkeypatch.setattr(queue, "MAX_ATTEMPTS", 1)
    monkeypatch.setattr(queue, "BACKOFF_SECONDS", (0,))
    transcript.append(
        chat_id="web",
        channel="pipeline",
        role="bot",
        text="answer",
        envelope_id="env-in",
    )
    queue.enqueue_outbound({
        "id": "out-1",
        "parent_id": "env-in",
        "source": "webhook:voice",
        "source_meta": {"chat_id": "web"},
        "text": "answer",
        "pipeline_text": "answer",
        "depth": 0,
    })

    async def fn(env):
        raise queue.Retry("no outbound handler took [webhook:voice→source]")

    task = asyncio.create_task(queue.run_outbound_worker(fn))
    await asyncio.sleep(0.2)
    task.cancel()

    msg = transcript.list_messages("web")[0]
    assert msg["delivery_status"] == "failed"
    events = replies.drain()
    assert any(
        e.get("type") == "delivery_failed" and e.get("parent_id") == "env-in"
        for e in events
    )


async def test_outbound_give_up_skips_self_delivering_sources(db, monkeypatch):
    monkeypatch.setattr(queue, "MAX_ATTEMPTS", 1)
    monkeypatch.setattr(queue, "BACKOFF_SECONDS", (0,))
    transcript.append(
        chat_id="web",
        channel="pipeline",
        role="bot",
        text="answer",
        envelope_id="env-web",
    )
    queue.enqueue_outbound({
        "id": "out-web",
        "parent_id": "env-web",
        "source": "web",
        "source_meta": {"chat_id": "web"},
        "text": "answer",
        "pipeline_text": "answer",
        "depth": 0,
    })

    async def fn(env):
        raise queue.Retry("no handler")

    task = asyncio.create_task(queue.run_outbound_worker(fn))
    await asyncio.sleep(0.2)
    task.cancel()

    assert transcript.list_messages("web")[0].get("delivery_status") is None


async def test_successful_retry_does_not_mark_transcript(db, monkeypatch):
    monkeypatch.setattr(queue, "BACKOFF_SECONDS", (0,))
    transcript.append(
        chat_id="web",
        channel="pipeline",
        role="bot",
        text="answer",
        envelope_id="env-in",
    )
    queue.enqueue_outbound({
        "id": "out-1",
        "parent_id": "env-in",
        "source": "webhook:voice",
        "source_meta": {"chat_id": "web"},
        "text": "answer",
        "pipeline_text": "answer",
        "depth": 0,
    })
    tries = []

    async def fn(env):
        tries.append(1)
        if len(tries) < 2:
            raise queue.Retry("not yet")

    task = asyncio.create_task(queue.run_outbound_worker(fn))
    await asyncio.sleep(0.25)
    task.cancel()

    assert transcript.list_messages("web")[0].get("delivery_status") is None


# ------------------------------------------------------------------
# Handler loading
# ------------------------------------------------------------------

def write_handler(config_dir, name: str, body: dict) -> None:
    d = config_dir / "handlers"
    d.mkdir(parents=True, exist_ok=True)
    (d / name).write_text(json.dumps(body))


def test_loader_applies_defaults_and_sorts(tmp_path):
    write_handler(tmp_path, "99.0-echo.json", {
        "id": "echo", "direction": "inbound", "type": "exec_json", "exec": "/bin/true",
    })
    write_handler(tmp_path, "0.0-audit.json", {
        "id": "audit", "direction": "inbound", "type": "exec_json", "exec": "/bin/true",
    })

    loaded = handler_loader.load(tmp_path)

    assert [h["id"] for h in loaded] == ["audit", "echo"], "priority from filename"
    assert loaded[0]["priority"] == 0.0
    assert loaded[0]["lifecycle"] == "per_message"
    assert loaded[0]["timeout_seconds"] == 10
    assert loaded[0]["continue_on_match"] is True


def test_loader_explicit_priority_beats_filename(tmp_path):
    write_handler(tmp_path, "99.0-echo.json", {
        "id": "echo", "priority": 1.0, "direction": "inbound",
        "type": "exec_json", "exec": "/bin/true",
    })

    assert handler_loader.load(tmp_path)[0]["priority"] == 1.0


def test_loader_skips_invalid_json(tmp_path):
    (tmp_path / "handlers").mkdir()
    (tmp_path / "handlers" / "broken.json").write_text("{not json")
    write_handler(tmp_path, "10.0-ok.json", {
        "id": "ok", "direction": "inbound", "type": "exec_json", "exec": "/bin/true",
    })

    assert [h["id"] for h in handler_loader.load(tmp_path)] == ["ok"]


def test_loader_skips_schema_violations(tmp_path):
    write_handler(tmp_path, "10.0-bad.json", {"id": "bad"})  # missing required fields
    write_handler(tmp_path, "20.0-ok.json", {
        "id": "ok", "direction": "inbound", "type": "exec_json", "exec": "/bin/true",
    })

    assert [h["id"] for h in handler_loader.load(tmp_path)] == ["ok"]


def test_loader_missing_dir_is_not_fatal(tmp_path):
    assert handler_loader.load(tmp_path) == []
