"""Web API: auth, message injection, uploads, and plugin reply routing."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

import core.msg_queue as queue
import core.registry as registry
import core.transcript as transcript
import core.web.api as api
from conftest import register_channel


@pytest.fixture
def client(tmp_path, db):
    uploads = tmp_path / "uploads"
    uploads.mkdir()
    api.configure(
        auth_token="tok",
        webhook_token="hook",
        config_dir=tmp_path,
        upload_dir=uploads,
    )
    with TestClient(api.app) as c:
        c.headers.update({"Authorization": "Bearer tok"})
        yield c


@pytest.fixture
async def aclient(tmp_path, db):
    """Async client — needed by tests that touch the plugin registry, which
    schedules status broadcasts with asyncio.create_task()."""
    import httpx

    uploads = tmp_path / "uploads"
    uploads.mkdir()
    api.configure(
        auth_token="tok", webhook_token="hook", config_dir=tmp_path,
        upload_dir=uploads,
    )
    transport = httpx.ASGITransport(app=api.app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": "Bearer tok"},
    ) as c:
        yield c


@pytest.fixture
def noauth_client(tmp_path, db):
    uploads = tmp_path / "uploads"
    uploads.mkdir()
    api.configure(
        auth_token=None, webhook_token=None, config_dir=tmp_path,
        upload_dir=uploads,
    )
    with TestClient(api.app) as c:
        yield c


# ------------------------------------------------------------------
# Auth
# ------------------------------------------------------------------

def test_missing_token_is_rejected(client):
    r = client.get("/api/handlers", headers={"Authorization": ""})
    assert r.status_code == 401


def test_wrong_token_is_rejected(client):
    r = client.get("/api/handlers", headers={"Authorization": "Bearer nope"})
    assert r.status_code == 401


def test_query_token_is_accepted_for_sse(client):
    """EventSource cannot set headers, so ?token= must work."""
    r = client.get("/api/handlers?token=tok", headers={"Authorization": ""})
    assert r.status_code == 200


def test_no_auth_configured_allows_everything(noauth_client):
    assert noauth_client.get("/api/handlers").status_code == 200


def test_version_needs_no_auth(client):
    r = client.get("/api/version", headers={"Authorization": ""})
    assert r.status_code == 200
    assert r.json()["service"] == "sijba"


# ------------------------------------------------------------------
# Message injection
# ------------------------------------------------------------------

def test_message_is_queued(client):
    r = client.post("/api/message", json={"text": "hallo"})

    assert r.status_code == 200
    assert r.json()["status"] == "queued"
    assert queue.depths()["inbound"] == {"pending": 1}


def test_message_requires_text_or_attachments(client):
    assert client.post("/api/message", json={}).status_code == 400


def test_message_accepts_attachments_without_text(client):
    r = client.post(
        "/api/message",
        json={"attachments": [{"id": "f1", "filename": "a.png"}]},
    )
    assert r.status_code == 200


def test_message_defaults_source_and_chat_id(client):
    client.post("/api/message", json={"text": "hallo"})

    env = _first_queued()
    assert env["source"] == "web"
    assert env["source_meta"]["chat_id"] == "web"


def test_message_source_is_passed_through(client):
    """todo-04: the app can send a message tagged as a given webhook so it
    routes exactly like a real POST /webhook/<token> call."""
    client.post("/api/message", json={"text": "licht an", "source": "webhook:voice"})

    env = _first_queued()
    assert env["source"] == "webhook:voice"
    assert env["source_meta"]["chat_id"] == "web"


def test_webhook_flags_the_envelope_for_live_broadcast(client):
    """A webhook has no client of its own — the pipeline must push the
    incoming message to web/app clients so it shows up live in the tile."""
    client.post("/webhook/hook", json={"text": "von aussen"})
    assert _first_queued()["_activity_broadcast"] is True


def test_api_message_is_not_flagged_for_live_broadcast(client):
    """The web composer already echoes its own sends locally."""
    client.post("/api/message", json={"text": "hallo", "source": "webhook:voice"})
    assert "_activity_broadcast" not in _first_queued()


def test_message_target_is_passed_through(client):
    r = client.post("/api/message", json={"text": "hallo", "target": "projekt-a"})

    assert r.json()["target"] == "projekt-a"
    assert _first_queued()["_target"] == "projekt-a"


def test_blank_target_is_dropped(client):
    client.post("/api/message", json={"text": "hallo", "target": "   "})

    assert "_target" not in _first_queued()


def test_attachments_land_in_metadata(client):
    atts = [{"id": "f1", "filename": "a.png"}]
    client.post("/api/message", json={"text": "schau", "attachments": atts})

    assert _first_queued()["metadata"]["attachments"] == atts


def _first_queued() -> dict:
    import json

    import core.db as database

    row = database.query_one(
        f"SELECT envelope_json FROM {queue.INBOUND} ORDER BY created_at LIMIT 1"
    )
    return json.loads(row[0])


# ------------------------------------------------------------------
# Webhook
# ------------------------------------------------------------------

def test_webhook_accepts_valid_token(client):
    r = client.post("/webhook/hook", json={"text": "von aussen"})

    assert r.status_code == 200
    assert _first_queued()["source"] == "webhook"


def test_webhook_rejects_bad_token(client):
    assert client.post("/webhook/wrong", json={"text": "x"}).status_code == 403


def test_webhook_defaults_chat_id_to_web(client):
    """Without this the message lands in a bucket the app never reads."""
    client.post("/webhook/hook", json={"text": "von aussen"})

    assert _first_queued()["source_meta"]["chat_id"] == "web"


def test_webhook_keeps_an_explicit_chat_id(client):
    client.post("/webhook/hook", json={"text": "von aussen", "chat_id": "voice"})

    assert _first_queued()["source_meta"]["chat_id"] == "voice"


def test_webhook_accepts_pebble_multipart(client):
    """Pebble Index posts multipart/form-data: transcription + recordedAt + client."""
    r = client.post(
        "/webhook/hook",
        data={"recordedAt": "1730000000000", "client": "ring"},
        files={"transcription": (None, "Einkauf Milch und Butter")},
    )

    assert r.status_code == 200
    env = _first_queued()
    assert env["text"] == "Einkauf Milch und Butter"
    assert env["source_meta"]["recorded_at"] == "1730000000000"
    assert env["source_meta"]["client"] == "ring"
    # Pebble can't set chat_id → threaded under the source, not "web".
    assert env["source_meta"]["chat_id"] == "webhook"


def test_webhook_accepts_form_urlencoded(client):
    r = client.post("/webhook/hook", data={"transcription": "hallo welt"})

    assert r.status_code == 200
    assert _first_queued()["text"] == "hallo welt"


def test_webhook_form_without_transcription_is_rejected(client):
    r = client.post("/webhook/hook", data={"client": "ring"})

    assert r.status_code == 400


def test_webhook_form_ignores_an_audio_part(client):
    r = client.post(
        "/webhook/hook",
        files={
            "transcription": (None, "nur der text zählt"),
            "audio": ("note.m4a", b"\x00\x01\x02fake", "audio/mp4"),
        },
    )

    assert r.status_code == 200
    assert _first_queued()["text"] == "nur der text zählt"


def test_webhook_form_honours_an_explicit_chat_id(client):
    client.post("/webhook/hook", data={"transcription": "x", "chat_id": "voice"})

    assert _first_queued()["source_meta"]["chat_id"] == "voice"


# ------------------------------------------------------------------
# Uploads
# ------------------------------------------------------------------

def test_upload_and_download_roundtrip(client):
    r = client.post("/api/upload", files={"file": ("bild.png", b"binary-data")})

    assert r.status_code == 200
    body = r.json()
    assert body["filename"] == "bild.png"
    assert body["size"] == len(b"binary-data")

    got = client.get(body["url"])
    assert got.status_code == 200
    assert got.content == b"binary-data"


def test_upload_strips_path_components_from_the_filename(client):
    r = client.post("/api/upload", files={"file": ("../../etc/passwd", b"x")})

    assert r.json()["filename"] == "passwd"


def test_upload_rejects_oversized_files(client, monkeypatch):
    monkeypatch.setattr(api, "MAX_UPLOAD_BYTES", 10)

    r = client.post("/api/upload", files={"file": ("big.bin", b"x" * 100)})

    assert r.status_code == 413


def test_download_rejects_non_uuid_ids(client):
    assert client.get("/api/upload/..%2F..%2Fetc").status_code == 404


def test_download_unknown_id_is_404(client):
    assert client.get("/api/upload/3f2504e0-4f89-11d3-9a0c-0305e82c3301").status_code == 404


# ------------------------------------------------------------------
# Transcript endpoint
# ------------------------------------------------------------------

def test_transcript_endpoint_returns_messages(client):
    transcript.append(chat_id="web", channel="pipeline", role="user", text="hallo")

    r = client.get("/api/transcript", params={"chat_id": "web"})

    assert r.status_code == 200
    assert [m["text"] for m in r.json()["messages"]] == ["hallo"]


def test_transcript_endpoint_requires_chat_id(client):
    assert client.get("/api/transcript", params={"chat_id": ""}).status_code == 400


# ------------------------------------------------------------------
# Plugin reply routing (mirrors pipeline `respond` handling)
# ------------------------------------------------------------------

async def test_plugin_reply_broadcasts_and_persists(db, replies):
    register_channel("cursor", "projekt-a")
    await registry.send_to_channel(
        "projekt-a",
        {"envelope_id": "e1"},
        {"source": "web", "source_meta": {"chat_id": "web"},
         "channel": "projekt-a", "parent_id": "e1"},
    )

    await api._handle_plugin_reply(
        {"type": "reply", "channel": "projekt-a", "envelope_id": "e1", "text": "fertig"}
    )

    events = replies.drain()
    assert [e["type"] for e in events] == ["reply"]
    assert events[0]["text"] == "fertig"
    assert events[0]["handler_id"] == "plugin:projekt-a"
    assert events[0]["channel"] == "projekt-a"

    msgs = transcript.list_messages("web", "projekt-a")
    assert [(m["role"], m["text"]) for m in msgs] == [("bot", "fertig")]


async def test_plugin_reply_clears_the_pending_entry(db, replies):
    register_channel("cursor", "projekt-a")
    await registry.send_to_channel(
        "projekt-a", {"envelope_id": "e1"}, {"source": "web", "source_meta": {}}
    )

    await api._handle_plugin_reply(
        {"channel": "projekt-a", "envelope_id": "e1", "text": "fertig"}
    )

    assert registry.peek_pending_reply("e1") is None, "must not be redelivered"


async def test_plugin_reply_for_unknown_envelope_does_not_crash(db, replies):
    await api._handle_plugin_reply(
        {"channel": "projekt-a", "envelope_id": "ghost", "text": "hi"}
    )

    assert replies.drain() == [], "no context means no web client to notify"


async def test_plugin_reply_from_non_web_source_is_not_broadcast(db, replies):
    register_channel("cursor", "projekt-a")
    await registry.send_to_channel(
        "projekt-a",
        {"envelope_id": "e1"},
        {"source": "telegram", "source_meta": {"chat_id": "42"}, "channel": "projekt-a"},
    )

    await api._handle_plugin_reply(
        {"channel": "projekt-a", "envelope_id": "e1", "text": "fertig"}
    )

    assert replies.drain() == []
    assert transcript.list_messages("42", "projekt-a"), "still persisted"


async def test_plugin_reply_from_a_webhook_source_is_broadcast(db, replies):
    """A webhook has no transport of its own — unlike Telegram above — so the
    app is the only place its reply can show up, and only if pushed live."""
    register_channel("cursor", "projekt-a")
    await registry.send_to_channel(
        "projekt-a",
        {"envelope_id": "e1"},
        {"source": "webhook:voice", "source_meta": {"chat_id": "web"},
         "channel": "projekt-a"},
    )

    await api._handle_plugin_reply(
        {"channel": "projekt-a", "envelope_id": "e1", "text": "fertig"}
    )

    events = replies.drain()
    assert [e["text"] for e in events] == ["fertig"]
    # The app filters its per-webhook view by this field.
    assert events[0]["source"] == "webhook:voice"
    assert events[0]["in_pipeline"] is True
    mirror = transcript.list_messages(
        transcript.PIPELINE_CHAT_ID, transcript.PIPELINE_CHANNEL
    )
    assert [(m["role"], m["text"]) for m in mirror] == [("bot", "fertig")]


# ------------------------------------------------------------------
# Streamed chunks
# ------------------------------------------------------------------

async def test_reply_chunk_is_broadcast_but_not_persisted(db, replies):
    register_channel("cursor", "projekt-a")
    await registry.send_to_channel(
        "projekt-a",
        {"envelope_id": "e1"},
        {"source": "web", "source_meta": {"chat_id": "web"},
         "channel": "projekt-a", "parent_id": "e1"},
    )

    await api._handle_plugin_reply_chunk(
        {"channel": "projekt-a", "envelope_id": "e1", "text": "teil "}
    )

    events = replies.drain()
    assert [e["type"] for e in events] == ["reply_chunk"]
    assert events[0]["text"] == "teil "
    assert transcript.list_messages("web", "projekt-a") == [], "chunks are not stored"


async def test_reply_chunk_keeps_the_pending_entry(db, replies):
    register_channel("cursor", "projekt-a")
    await registry.send_to_channel(
        "projekt-a", {"envelope_id": "e1"}, {"source": "web", "source_meta": {}}
    )

    await api._handle_plugin_reply_chunk(
        {"channel": "projekt-a", "envelope_id": "e1", "text": "teil"}
    )

    assert registry.peek_pending_reply("e1") is not None, "more chunks still to come"


async def test_reply_chunk_for_unknown_envelope_is_ignored(db, replies):
    await api._handle_plugin_reply_chunk(
        {"channel": "projekt-a", "envelope_id": "ghost", "text": "x"}
    )

    assert replies.drain() == []


async def test_empty_reply_chunk_is_ignored(db, replies):
    register_channel("cursor", "projekt-a")
    await registry.send_to_channel(
        "projekt-a", {"envelope_id": "e1"}, {"source": "web", "source_meta": {}}
    )

    await api._handle_plugin_reply_chunk({"channel": "projekt-a", "envelope_id": "e1", "text": ""})

    assert replies.drain() == []


# ------------------------------------------------------------------
# Handler listing
# ------------------------------------------------------------------

def test_handlers_endpoint_reports_state(client):
    from conftest import handler, install

    install(handler("echo", priority=99.0, router_hint="Echo"))

    body = client.get("/api/handlers").json()

    assert len(body) == 1
    assert body[0]["id"] == "echo"
    assert body[0]["state"] == "healthy"
    assert body[0]["dynamic"] is False


async def test_plugin_channels_endpoint(aclient):
    register_channel("cursor", "projekt-a", triggers=["@projekt-a"])

    body = (await aclient.get("/api/plugin/channels")).json()

    assert [c["channel_id"] for c in body] == ["projekt-a"]
    assert body[0]["triggers"] == ["@projekt-a"]


# ------------------------------------------------------------------
# Named webhooks: the token decides the source
# ------------------------------------------------------------------

@pytest.fixture
def multi_client(tmp_path, db):
    uploads = tmp_path / "uploads"
    uploads.mkdir()
    api.configure(
        auth_token="tok", webhook_token="legacy", config_dir=tmp_path,
        upload_dir=uploads,
        webhooks={"tok-voice": "voice", "tok-iot": "iot"},
    )
    with TestClient(api.app) as c:
        yield c


def test_named_token_sets_the_source(multi_client):
    r = multi_client.post("/webhook/tok-voice", json={"text": "licht an"})

    assert r.status_code == 200
    assert r.json()["source"] == "webhook:voice"
    assert _first_queued()["source"] == "webhook:voice"


def test_each_token_gets_its_own_source(multi_client):
    multi_client.post("/webhook/tok-iot", json={"text": "x"})
    assert _first_queued()["source"] == "webhook:iot"


def test_legacy_single_token_still_works(multi_client):
    r = multi_client.post("/webhook/legacy", json={"text": "x"})

    assert r.json()["source"] == "webhook"


def test_unknown_token_is_rejected(multi_client):
    assert multi_client.post("/webhook/nope", json={"text": "x"}).status_code == 403


def test_webhooks_endpoint_lists_configured_webhooks(multi_client):
    auth = {"Authorization": "Bearer tok"}
    res = multi_client.get("/api/webhooks", headers=auth)
    body = res.json()

    assert {"name": "iot", "source": "webhook:iot"} in body
    assert {"name": "voice", "source": "webhook:voice"} in body
    # legacy single token is offered too
    assert {"name": "webhook", "source": "webhook"} in body
    # never leak the tokens
    assert "tok-voice" not in res.text
    assert "legacy" not in res.text


def test_webhooks_endpoint_needs_auth(multi_client):
    assert multi_client.get("/api/webhooks").status_code == 401


def test_source_cannot_be_set_from_the_body(multi_client):
    """The token is the identity — a caller must not be able to relabel
    itself and reach handlers scoped to another source."""
    multi_client.post(
        "/webhook/tok-voice", json={"text": "x", "source": "iot"}
    )

    assert _first_queued()["source"] == "webhook:voice"


def test_webhook_source_is_namespaced_away_from_trusted_sources(tmp_path, db):
    """Even a webhook *named* "web" cannot become the source "web"."""
    uploads = tmp_path / "uploads"
    uploads.mkdir()
    api.configure(
        auth_token="tok", webhook_token=None, config_dir=tmp_path,
        upload_dir=uploads,
        webhooks={"t": "web"},
    )
    with TestClient(api.app) as c:
        c.post("/webhook/t", json={"text": "x"})

    assert _first_queued()["source"] == "webhook:web"


def test_no_webhook_config_accepts_anything(noauth_client):
    """Unconfigured stays permissive for local development."""
    r = noauth_client.post("/webhook/whatever", json={"text": "x"})

    assert r.status_code == 200
    assert r.json()["source"] == "webhook"


def test_webhook_chat_id_and_target(multi_client):
    multi_client.post(
        "/webhook/tok-voice",
        json={"text": "x", "chat_id": "voice", "target": "projekt-a"},
    )

    env = _first_queued()
    assert env["source_meta"]["chat_id"] == "voice"
    assert env["_target"] == "projekt-a"


def test_webhook_falls_back_to_message_key(multi_client):
    multi_client.post("/webhook/tok-voice", json={"message": "von aussen"})
    assert _first_queued()["text"] == "von aussen"


def test_root_redirects_to_config_ui_when_configured(client, monkeypatch):
    monkeypatch.setenv("SIJBA_CONFIG_UI_URL", "/config-ui/")
    r = client.get("/", follow_redirects=False)
    assert r.status_code == 307 and r.headers["location"] == "/config-ui/"


def test_root_serves_builtin_ui_without_config_ui(client, monkeypatch):
    monkeypatch.delenv("SIJBA_CONFIG_UI_URL", raising=False)
    r = client.get("/", follow_redirects=False)
    assert r.status_code == 200 and "<html" in r.text.lower()
