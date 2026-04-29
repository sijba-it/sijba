#!/usr/bin/env python3
"""LLM plugin for SJBA.

Reads conversation history from envelope context, calls the configured LLM,
and returns the reply plus updated history via context_update.

Zero external dependencies — uses stdlib urllib only.

Environment variables
---------------------
LLM_API_URL         Full base URL of the LLM API endpoint.
                      OpenAI:    https://api.openai.com/v1
                      Anthropic: https://api.anthropic.com/v1
                      Ollama:    http://localhost:11434/v1
                      Any OpenAI-compatible proxy: just set the URL.
LLM_API_KEY         API key (not required for local models like Ollama).
LLM_MODEL           Model name (e.g. gpt-4o-mini, llama3.2, claude-haiku-4-5-20251001).
LLM_FORMAT          API format: openai (default) | anthropic
                      Only needed when the API shape differs from OpenAI.
                      Ollama speaks OpenAI format at /v1 — no change needed.
LLM_SYSTEM_PROMPT   System prompt text, or path to a .md / .txt file.
LLM_MAX_HISTORY     Max messages kept in history (default: 20).
LLM_TEMPERATURE     Temperature 0–2 (default: 0.7).
LLM_MAX_TOKENS      Max tokens in response (default: 2048).

Handler config example (OpenAI)
--------------------------------
{
  "id": "llm",
  "priority": 50.0,
  "direction": "inbound",
  "lifecycle": "per_message",
  "type": "exec_json",
  "exec": "{sijba_plugins}/llm/main.py",
  "continue_on_match": false,
  "context": { "namespace": "llm" },
  "timeout_seconds": 120,
  "env": {
    "LLM_API_URL": "https://api.openai.com/v1",
    "LLM_API_KEY": "${OPENAI_API_KEY}",
    "LLM_MODEL": "gpt-4o-mini",
    "LLM_SYSTEM_PROMPT": "You are a helpful assistant."
  }
}

Handler config example (Ollama, local)
---------------------------------------
  "env": {
    "LLM_API_URL": "http://localhost:11434/v1",
    "LLM_MODEL": "llama3.2"
  }

Handler config example (Anthropic)
------------------------------------
  "env": {
    "LLM_API_URL": "https://api.anthropic.com/v1",
    "LLM_API_KEY": "${ANTHROPIC_API_KEY}",
    "LLM_FORMAT": "anthropic",
    "LLM_MODEL": "claude-haiku-4-5-20251001"
  }
"""
from __future__ import annotations
import json
import os
import sys
import urllib.error
import urllib.request

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

API_URL     = os.environ.get("LLM_API_URL", "https://api.openai.com/v1").rstrip("/")
API_KEY     = os.environ.get("LLM_API_KEY", "")
MODEL       = os.environ.get("LLM_MODEL", "gpt-4o-mini")
FORMAT      = os.environ.get("LLM_FORMAT", "openai").lower()
TEMPERATURE = float(os.environ.get("LLM_TEMPERATURE", "0.7"))
MAX_TOKENS  = int(os.environ.get("LLM_MAX_TOKENS", "2048"))
MAX_HISTORY = int(os.environ.get("LLM_MAX_HISTORY", "20"))


def _system_prompt() -> str:
    raw = os.environ.get("LLM_SYSTEM_PROMPT", "You are a helpful assistant.")
    if raw.endswith((".md", ".txt")) and os.path.isfile(raw):
        with open(raw) as fh:
            return fh.read()
    return raw


# ---------------------------------------------------------------------------
# API callers
# ---------------------------------------------------------------------------

def _post(url: str, payload: dict, extra_headers: dict = None) -> dict:
    data = json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"}
    if API_KEY:
        headers["Authorization"] = f"Bearer {API_KEY}"
    if extra_headers:
        headers.update(extra_headers)
    req = urllib.request.Request(url, data=data, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        raise RuntimeError(f"HTTP {exc.code}: {body}") from exc


def _call_openai(messages: list) -> str:
    resp = _post(
        f"{API_URL}/chat/completions",
        {"model": MODEL, "messages": messages,
         "temperature": TEMPERATURE, "max_tokens": MAX_TOKENS},
    )
    return resp["choices"][0]["message"]["content"]


def _call_anthropic(messages: list) -> str:
    # Anthropic separates system from the message list
    system = next((m["content"] for m in messages if m["role"] == "system"),
                  "You are a helpful assistant.")
    msgs = [m for m in messages if m["role"] != "system"]
    resp = _post(
        f"{API_URL}/messages",
        {"model": MODEL, "max_tokens": MAX_TOKENS,
         "system": system, "messages": msgs, "temperature": TEMPERATURE},
        extra_headers={"x-api-key": API_KEY, "anthropic-version": "2023-06-01"},
    )
    return resp["content"][0]["text"]


def _call_llm(messages: list) -> str:
    if FORMAT == "anthropic":
        return _call_anthropic(messages)
    return _call_openai(messages)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    envelope = json.load(sys.stdin)
    text = envelope.get("pipeline_text") or envelope.get("text", "")
    context = envelope.get("context") or {}
    history: list = list(context.get("history", []))

    system = _system_prompt()
    messages = [{"role": "system", "content": system}] + history + [{"role": "user", "content": text}]

    try:
        reply = _call_llm(messages)
    except Exception as exc:
        print(f"[llm] error: {exc}", file=sys.stderr, flush=True)
        print(json.dumps({"continue": True}))
        return

    history.append({"role": "user", "content": text})
    history.append({"role": "assistant", "content": reply})
    if len(history) > MAX_HISTORY:
        history = history[-MAX_HISTORY:]

    print(json.dumps({
        "continue": False,
        "respond": {"text": reply, "target": "source"},
        "context_update": {"history": history},
    }))


if __name__ == "__main__":
    main()
