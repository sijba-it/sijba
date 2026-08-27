#!/usr/bin/env python3
"""Router plugin for SJBA.

A lightweight LLM-based intent router. Placed at the end of the inbound pipeline
(high priority number), it receives messages that no other handler has claimed and
decides what to do:

  1. Route to a specific handler by injecting the canonical trigger command.
  2. Answer directly as a plain LLM response.

The router is told about all available handlers via the envelope's `handlers` field
(populated by core when `receive_handler_context: true` is set). Each handler exposes
its purpose and example commands via its `router_hint` field.

Environment variables
---------------------
LLM_API_URL         Base URL of the LLM API (default: https://api.openai.com/v1)
LLM_API_KEY         API key
LLM_MODEL           Model name (default: gpt-4o-mini)
LLM_FORMAT          openai (default) | anthropic
LLM_TEMPERATURE     Temperature (default: 0.2 — low for routing)
LLM_MAX_TOKENS      Max tokens (default: 512 — routing responses are short)
ROUTER_RULES        Extra instructions appended to the routing prompt. Combined
                    with `match.source`, this is how you run several routers
                    that behave differently per input (voice vs Telegram vs web).

Handler config example
----------------------
{
  "id": "router",
  "priority": 90.0,
  "direction": "inbound",
  "lifecycle": "per_message",
  "type": "exec_json",
  "exec": "{sijba_plugins}/router/main.py",
  "receive_handler_context": true,
  "continue_on_match": false,
  "timeout_seconds": 30,
  "env": {
    "LLM_API_URL": "https://openrouter.ai/api/v1",
    "LLM_API_KEY": "${OPENROUTER_API_KEY}",
    "LLM_MODEL": "openai/gpt-4o-mini"
  }
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
TEMPERATURE = float(os.environ.get("LLM_TEMPERATURE", "0.2"))
MAX_TOKENS  = int(os.environ.get("LLM_MAX_TOKENS", "512"))
# Extra instructions appended to the routing prompt. This is what makes two
# router handlers behave differently: give each one its own ROUTER_RULES and
# scope it to a source with `match.source` (e.g. a voice webhook vs Telegram).
EXTRA_RULES = os.environ.get("ROUTER_RULES", "").strip()


# ---------------------------------------------------------------------------
# LLM call (reuses same pattern as llm/main.py)
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
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        raise RuntimeError(f"HTTP {exc.code}: {body[:300]}") from exc


def _call_openai(messages: list) -> str:
    resp = _post(
        f"{API_URL}/chat/completions",
        {"model": MODEL, "messages": messages,
         "temperature": TEMPERATURE, "max_tokens": MAX_TOKENS},
    )
    msg = resp["choices"][0]["message"]
    content = msg.get("content")
    # Reasoning models may return content=null with reasoning in reasoning_details
    if not content:
        for rd in msg.get("reasoning_details") or []:
            if rd.get("text"):
                content = rd["text"]
                break
    return content or ""


def _call_anthropic(messages: list) -> str:
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
# System prompt builder
# ---------------------------------------------------------------------------

def _build_system_prompt(handler_summaries: list[dict]) -> str:
    hints = "\n".join(
        f"- [{h['id']}] {h['router_hint']}"
        for h in handler_summaries
    )
    extra = f"\n\nADDITIONAL RULES:\n{EXTRA_RULES}" if EXTRA_RULES else ""
    return f"""You are a routing assistant. Your only job is to detect if a message should
be handled by one of the available targets and output the correct trigger command.

Available targets:
{hints}

RULES:
- If the message clearly matches a target → respond with ONLY the exact trigger
  command, keeping the rest of the user's request after it.
  Examples: "iobroker set Bubble true"
            "@projekt-a fix the failing test"
- If the message does NOT match any target → respond with exactly: NONE

Do not explain. Do not answer questions. Output only the trigger command or NONE.{extra}"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    envelope = json.load(sys.stdin)
    text = (envelope.get("pipeline_text") or envelope.get("text", "")).strip()
    handler_summaries = envelope.get("handlers", [])

    if not text:
        print(json.dumps({"continue": True}))
        return

    # Nothing to route to: the reply could never match a known target, so the
    # call would be paid for and then discarded.
    if not handler_summaries:
        print("[router] no routing targets available, skipping LLM",
              file=sys.stderr, flush=True)
        print(json.dumps({"continue": True}))
        return

    system_prompt = _build_system_prompt(handler_summaries)
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": text},
    ]

    try:
        reply = _call_llm(messages)
    except Exception as exc:
        print(f"[router] LLM error: {exc}", file=sys.stderr, flush=True)
        print(json.dumps({"continue": True}))
        return

    if not reply:
        print(json.dumps({"continue": True}))
        return

    reply = reply.strip()

    # Check if the reply looks like a trigger command for a known handler
    # by seeing if it starts with a known handler id or matches a known prefix.
    # If so, inject it back into the pipeline instead of responding directly.
    known_ids = {h["id"] for h in handler_summaries}

    is_trigger = any(
        reply.lower().startswith(hid.lower())
        for hid in known_ids
    )

    if reply.upper() == "NONE" or not is_trigger:
        # Could not route — pass through to next handler (e.g. LLM)
        print(f"[router] no route found, continuing", file=sys.stderr, flush=True)
        print(json.dumps({"continue": True}))
    else:
        print(f"[router] routing via: {reply!r}", file=sys.stderr, flush=True)
        print(json.dumps({
            "continue": False,
            "inject": {"text": reply},
        }))


if __name__ == "__main__":
    main()
