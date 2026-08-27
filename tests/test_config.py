"""core.yaml loading, and resolving the named-webhook token table."""
from __future__ import annotations

import core.config as config


def write_cfg(tmp_path, body: str):
    (tmp_path / "core.yaml").write_text(body)
    return tmp_path


def load_in(tmp_path, monkeypatch, body: str) -> dict:
    write_cfg(tmp_path, body)
    monkeypatch.setenv("SIJBA_CONFIG_DIR", str(tmp_path))
    return config.load()


def test_defaults_when_no_file(tmp_path, monkeypatch):
    monkeypatch.setenv("SIJBA_CONFIG_DIR", str(tmp_path))
    cfg = config.load()

    assert cfg["port"] == 8080
    assert cfg["max_inject_depth"] == 5
    assert cfg["webhooks"] == {}


def test_env_overrides_file(tmp_path, monkeypatch):
    monkeypatch.setenv("SIJBA_PORT", "9999")
    cfg = load_in(tmp_path, monkeypatch, "port: 8080\n")

    assert cfg["port"] == 9999


def test_webhooks_are_inverted_to_token_lookup(tmp_path, monkeypatch):
    cfg = load_in(tmp_path, monkeypatch, """
webhooks:
  voice: tok-voice
  iot: tok-iot
""")

    assert cfg["webhooks"] == {"tok-voice": "voice", "tok-iot": "iot"}


def test_webhook_token_expands_from_the_environment(tmp_path, monkeypatch):
    """Names live in the ConfigMap, secrets stay in env."""
    monkeypatch.setenv("VOICE_TOKEN", "s3cret")
    cfg = load_in(tmp_path, monkeypatch, "webhooks:\n  voice: ${VOICE_TOKEN}\n")

    assert cfg["webhooks"] == {"s3cret": "voice"}


def test_unresolved_variable_is_dropped_not_used_literally(tmp_path, monkeypatch):
    """A missing env var must not register the literal "${VAR}" as a valid,
    guessable webhook token."""
    monkeypatch.delenv("MISSING_TOKEN", raising=False)
    cfg = load_in(tmp_path, monkeypatch, "webhooks:\n  voice: ${MISSING_TOKEN}\n")

    assert cfg["webhooks"] == {}


def test_empty_token_is_dropped(tmp_path, monkeypatch):
    cfg = load_in(tmp_path, monkeypatch, "webhooks:\n  voice: ''\n")

    assert cfg["webhooks"] == {}


def test_duplicate_tokens_do_not_collide(tmp_path, monkeypatch):
    cfg = load_in(tmp_path, monkeypatch, """
webhooks:
  voice: same
  iot: same
""")

    assert list(cfg["webhooks"].values()) == ["voice"], "first name wins, second skipped"


def test_webhook_names_are_slugified(tmp_path, monkeypatch):
    cfg = load_in(tmp_path, monkeypatch, 'webhooks:\n  "Voice Note!": tok\n')

    assert cfg["webhooks"] == {"tok": "voicenote"}


def test_unusable_name_is_dropped(tmp_path, monkeypatch):
    cfg = load_in(tmp_path, monkeypatch, 'webhooks:\n  "!!!": tok\n')

    assert cfg["webhooks"] == {}


def test_non_mapping_webhooks_is_rejected_not_crashed(tmp_path, monkeypatch):
    cfg = load_in(tmp_path, monkeypatch, "webhooks:\n  - voice\n  - iot\n")

    assert cfg["webhooks"] == {}
