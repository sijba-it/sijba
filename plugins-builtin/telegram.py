#!/usr/bin/env python3
"""Telegram combined plugin (source + outbound in one process).

Reads TELEGRAM_BOT_TOKEN from env.

stdout protocol (multiplexed):
  - Source events (no req_id):   {"source": "telegram", "source_meta": {...}, "text": "..."}\n
  - NDJSON responses (req_id):   {"req_id": "...", "continue": false}\n

stdin protocol:
  - NDJSON invocations:          {"req_id": "...", "message": {...envelope...}}\n

Handler configs (in sijba-config/handlers/):

  10.0-telegram-source.json
  {
    "id": "telegram-source",
    "direction": "source",
    "lifecycle": "persistent",
    "type": "exec_json",
    "exec": "{sijba_plugins}/telegram.py",
    "required": false,
    "startup": { "restart_policy": "always" }
  }

  10.1-telegram-out.json
  {
    "id": "telegram-out",
    "direction": "outbound",
    "lifecycle": "persistent",
    "type": "exec_json",
    "shared_process": "telegram-source",
    "await_response": false
  }
"""
import asyncio
import json
import os
import sys
import urllib.error
import urllib.request
from urllib.parse import urlencode

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
if not BOT_TOKEN:
    print("[telegram] TELEGRAM_BOT_TOKEN not set", file=sys.stderr, flush=True)
    sys.exit(1)

BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"
POLL_TIMEOUT = 30


def _api(path: str, payload: dict = None) -> dict:
    url = BASE + path
    if payload is None:
        req = urllib.request.Request(url, headers={"Content-Type": "application/json"})
    else:
        data = json.dumps(payload).encode()
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=POLL_TIMEOUT + 5) as resp:
        return json.loads(resp.read())


def _api_get(path: str, params: dict = None) -> dict:
    url = BASE + path
    if params:
        url += "?" + urlencode(params)
    with urllib.request.urlopen(url, timeout=POLL_TIMEOUT + 5) as resp:
        return json.loads(resp.read())


def _emit(obj: dict) -> None:
    """Write a JSON line to stdout (thread-safe via asyncio loop)."""
    print(json.dumps(obj), flush=True)


# ------------------------------------------------------------------
# Long-poll loop
# ------------------------------------------------------------------

async def poll_loop() -> None:
    offset = 0
    print("[telegram] polling started", file=sys.stderr, flush=True)
    while True:
        try:
            data = await asyncio.to_thread(
                _api_get,
                "/getUpdates",
                {"timeout": POLL_TIMEOUT, "offset": offset, "allowed_updates": '["message"]'},
            )
        except urllib.error.URLError as exc:
            print(f"[telegram] network error: {exc}", file=sys.stderr, flush=True)
            await asyncio.sleep(5)
            continue
        except Exception as exc:
            print(f"[telegram] poll error: {exc}", file=sys.stderr, flush=True)
            await asyncio.sleep(2)
            continue

        for update in data.get("result", []):
            offset = update["update_id"] + 1
            msg = update.get("message") or update.get("channel_post")
            if not msg or "text" not in msg:
                continue
            chat = msg["chat"]
            sender = msg.get("from") or {}
            _emit({
                "source": "telegram",
                "source_meta": {
                    "chat_id": str(chat["id"]),
                    "chat_type": chat.get("type", "private"),
                    "chat_title": chat.get("title", ""),
                    "user": sender.get("username") or sender.get("first_name", ""),
                    "user_id": str(sender.get("id", "")),
                    "message_id": msg["message_id"],
                },
                "text": msg["text"],
            })


# ------------------------------------------------------------------
# NDJSON stdin handler (outbound sends)
# ------------------------------------------------------------------

async def stdin_loop() -> None:
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)

    while True:
        try:
            raw = await reader.readline()
        except Exception:
            break
        if not raw:
            break

        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            continue

        req_id = msg.get("req_id")
        envelope = msg.get("message", {})
        text = envelope.get("pipeline_text") or envelope.get("text", "")
        chat_id = (envelope.get("source_meta") or {}).get("chat_id")

        if not chat_id:
            # Message didn't originate from Telegram — skip silently
            _emit({"req_id": req_id, "continue": True})
            continue

        try:
            await asyncio.to_thread(
                _api,
                "/sendMessage",
                {"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            )
            _emit({"req_id": req_id, "continue": False})
        except Exception as exc:
            print(f"[telegram] sendMessage error: {exc}", file=sys.stderr, flush=True)
            _emit({"req_id": req_id, "continue": True, "error": str(exc)})


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

async def main() -> None:
    await asyncio.gather(poll_loop(), stdin_loop())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
