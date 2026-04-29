#!/usr/bin/env python3
"""macop — Master Control Program: manage handler configs and restart sijba.

Without LLM: exact command syntax required.
With LLM:    natural language → LLM maps to command → executed deterministically.

Environment variables (LLM mode, all optional)
-----------------------------------------------
MACOP_LLM_URL     LLM API base URL (OpenAI-compatible or Anthropic)
MACOP_LLM_KEY     API key
MACOP_LLM_MODEL   Model name
MACOP_LLM_FORMAT  openai (default) | anthropic
"""
from __future__ import annotations
import json
import os
import signal
import sys
import urllib.error
import urllib.request
from pathlib import Path

envelope = json.load(sys.stdin)
text = (envelope.get("pipeline_text") or "").strip()

CONFIG_DIR = Path(os.environ.get("SIJBA_CONFIG_DIR", "./sijba-config"))
HANDLERS_DIR = CONFIG_DIR / "handlers"

LLM_URL    = os.environ.get("MACOP_LLM_URL", "").rstrip("/")
LLM_KEY    = os.environ.get("MACOP_LLM_KEY", "")
LLM_MODEL  = os.environ.get("MACOP_LLM_MODEL", "gpt-4o-mini")
LLM_FORMAT = os.environ.get("MACOP_LLM_FORMAT", "openai").lower()

HELP = (
    "macop commands:\n"
    "  macop list                    — list all handlers\n"
    "  macop show <id>               — show handler config\n"
    "  macop set <id> <key> <value>  — update a field (JSON value)\n"
    "  macop restart                 — restart sijba\n"
    "  macop help                    — this message"
)

SYSTEM_PROMPT = """\
You are macop, the Master Control Program of sijba.
You manage handler config files in the sijba config directory.

Available actions — respond with ONLY a JSON object, no markdown, no explanation:

  list all handlers:
    {"action": "list"}

  show one handler:
    {"action": "show", "id": "<handler-id>"}

  change a field in a handler config:
    {"action": "set", "id": "<handler-id>", "key": "<field>", "value": <json-value>}

  restart sijba (applies all config changes):
    {"action": "restart"}

  unknown / not a macop request:
    {"action": "unknown", "reason": "<short explanation>"}

Rules:
- value must be valid JSON (string → quoted, number → unquoted, bool → true/false)
- use the exact handler id from the list below
- if the user wants to disable a handler: set continue_on_match to false
- if the user wants to enable a handler: set continue_on_match to true
- for priority changes use the "priority" field (float)
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _out(msg: str) -> None:
    print(json.dumps({"continue": False, "respond": {"text": msg, "target": "source"}}))
    sys.stdout.flush()


def _find_file(handler_id: str) -> Path | None:
    for p in sorted(HANDLERS_DIR.glob("*.json")):
        try:
            if json.loads(p.read_text()).get("id") == handler_id:
                return p
        except Exception:
            pass
    return None


def _load_handlers() -> list[dict]:
    result = []
    for p in sorted(HANDLERS_DIR.glob("*.json")):
        try:
            result.append(json.loads(p.read_text()))
        except Exception:
            pass
    return result


def _handler_summary(handlers: list[dict]) -> str:
    lines = []
    for h in sorted(handlers, key=lambda h: h.get("priority", 50)):
        lines.append(
            f"  {h.get('id'):30s}  p={h.get('priority', '?'):5}  "
            f"[{h.get('direction', '?')}]  {h.get('type', '?')}"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# LLM call
# ---------------------------------------------------------------------------

def _post(url: str, payload: dict, extra_headers: dict | None = None) -> dict:
    data = json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"}
    if LLM_KEY:
        headers["Authorization"] = f"Bearer {LLM_KEY}"
    if extra_headers:
        headers.update(extra_headers)
    req = urllib.request.Request(url, data=data, headers=headers)
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def _llm_interpret(user_text: str, handlers: list[dict]) -> dict:
    handler_list = _handler_summary(handlers)
    system = SYSTEM_PROMPT + f"\nCurrent handlers:\n{handler_list}\n"
    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": user_text},
    ]
    if LLM_FORMAT == "anthropic":
        sys_content = messages[0]["content"]
        msgs = messages[1:]
        resp = _post(
            f"{LLM_URL}/messages",
            {"model": LLM_MODEL, "max_tokens": 256, "temperature": 0,
             "system": sys_content, "messages": msgs},
            extra_headers={"x-api-key": LLM_KEY, "anthropic-version": "2023-06-01"},
        )
        raw = resp["content"][0]["text"].strip()
    else:
        resp = _post(
            f"{LLM_URL}/chat/completions",
            {"model": LLM_MODEL, "messages": messages, "temperature": 0, "max_tokens": 256},
        )
        raw = resp["choices"][0]["message"]["content"].strip()

    # strip possible markdown fences
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()

    return json.loads(raw)


# ---------------------------------------------------------------------------
# Command execution
# ---------------------------------------------------------------------------

def _exec_list(handlers: list[dict]) -> None:
    summary = _handler_summary(handlers)
    _out("Handlers:\n" + summary if summary else "No handlers found.")


def _exec_show(hid: str) -> None:
    p = _find_file(hid)
    _out(f"```json\n{p.read_text()}```" if p else f"Handler '{hid}' not found.")


def _exec_set(hid: str, key: str, value) -> None:
    p = _find_file(hid)
    if not p:
        _out(f"Handler '{hid}' not found.")
        return
    data = json.loads(p.read_text())
    data[key] = value
    p.write_text(json.dumps(data, indent=2) + "\n")
    _out(f"Updated '{hid}': {key} = {json.dumps(value)}\nRestart to apply: macop restart")


def _exec_restart() -> None:
    _out("Sijba restarting...")
    os.kill(os.getppid(), signal.SIGTERM)


# ---------------------------------------------------------------------------
# Parse & dispatch (exact syntax fallback)
# ---------------------------------------------------------------------------

def _dispatch_parsed(parts: list[str]) -> bool:
    """Try to dispatch from exact command syntax. Returns False if not matched."""
    cmd = parts[1].lower() if len(parts) >= 2 else "help"

    if cmd == "help":
        _out(HELP)
    elif cmd == "list":
        _exec_list(_load_handlers())
    elif cmd == "show":
        _exec_show(parts[2].strip() if len(parts) >= 3 else "")
    elif cmd == "set":
        rest = parts[2].split(None, 2) if len(parts) >= 3 else []
        if len(rest) < 3:
            _out("Usage: macop set <id> <key> <value>")
            return True
        hid, key, raw_val = rest[0], rest[1], rest[2]
        try:
            val = json.loads(raw_val)
        except json.JSONDecodeError:
            val = raw_val
        _exec_set(hid, key, val)
    elif cmd == "restart":
        _exec_restart()
    else:
        return False
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

parts = text.split(None, 2)

# Try LLM interpretation if configured
if LLM_URL:
    handlers = _load_handlers()
    try:
        cmd_obj = _llm_interpret(text, handlers)
    except Exception as exc:
        _out(f"[macop] LLM error: {exc}\n\nFallback to exact syntax:\n{HELP}")
        sys.exit(0)

    action = cmd_obj.get("action", "unknown")

    if action == "list":
        _exec_list(handlers)
    elif action == "show":
        _exec_show(cmd_obj.get("id", ""))
    elif action == "set":
        _exec_set(cmd_obj.get("id", ""), cmd_obj.get("key", ""), cmd_obj.get("value"))
    elif action == "restart":
        _exec_restart()
    elif action == "unknown":
        _out(f"macop: {cmd_obj.get('reason', 'not a macop request')}")
    else:
        _out(f"macop: unknown action '{action}' from LLM")

else:
    # Exact syntax mode
    if not _dispatch_parsed(parts):
        _out(f"Unknown command: {parts[1] if len(parts) >= 2 else '?'}\n\n{HELP}")
