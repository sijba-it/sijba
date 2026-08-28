#!/usr/bin/env python3
"""Regex router plugin for SIJBA.

A deterministic, zero-LLM intent router. Matches the message text against an
ordered list of regex routes; the first match wins. The route's `inject`
template is expanded and re-injected into the inbound pipeline (a fresh run,
`depth + 1`), which is picked up by the trigger match in
`pipeline.process_inbound` — so the template normally starts with a
`@channel` mention or a trigger-word command.

No match -> `{"continue": true}`, the message falls through to the next
handler (e.g. the LLM router).

Use it twice in the same chain:
  - in front of the LLM router, to catch deliberate spoken keywords
    ("Einkauf ...", "Aufgabe ...") for free and with no hallucination risk
  - once more at the very end, with a single catch-all route, as the
    "everything else goes here" default the LLM router falls through to.

Environment variables
---------------------
ROUTES        JSON array of route objects (see below). Ignored if
              ROUTES_FILE is set and readable.
ROUTES_FILE   Path to a file containing the same JSON array. Preferred for
              k8s — keeps the (heavily escaped) JSON out of the handler
              config / ConfigMap.
ROUTES_FLAGS  Comma-separated regex flags: i, m, s, x. Default: "i"
              (case-insensitive). Applied to every pattern.
ON_MATCH_CONTINUE  If truthy ("1", "true", "yes"), a matched route emits
              {"continue": true, "inject": ...} — the injected message is
              queued *and* the current pipeline proceeds to the next
              handler. Use it for a "tee" handler that mirrors every message
              somewhere (e.g. a raw voice-note log) without claiming it.
              Default: false (a match stops the pipeline).

Route object
------------
{
  "pattern": "^(einkauf|einkaufs?liste|besorgen)\\b[:,]?\\s*",
  "inject":  "@obsidian [einkauf] {rest}"
}

`pattern` is matched with re.match (anchored at the start). `inject` is a
Jinja2 template with these variables available:

  {{ rest }}       the text with the matched prefix removed, stripped
  {{ text }}       the full original text, unchanged
  {{ groups[0] }}  positional regex capture groups (0 = first group)

Filters and conditionals work, e.g.
  "@obsidian [einkauf] {{ rest | lower }}"
  "@obsidian [{{ 'task' if 'muss' in text else 'journal' }}] {{ text }}"

Templates come from trusted config, so autoescaping is off. An undefined
variable or a syntax error is logged and the message is passed through
(continue) rather than injecting a broken command.

Handler config example
----------------------
{
  "id": "router-regex",
  "priority": 70.0,
  "direction": "inbound",
  "lifecycle": "per_message",
  "type": "exec_json",
  "exec": "{sijba_plugins}/regex-router/main.py",
  "match": { "source": ["webhook:voice"] },
  "continue_on_match": false,
  "timeout_seconds": 10,
  "env": { "ROUTES_FILE": "/config/handlers/routes-voice.json" }
}
"""
from __future__ import annotations

import json
import os
import re
import sys

import jinja2

_FLAG_MAP = {"i": re.I, "m": re.M, "s": re.S, "x": re.X}
_TRUTHY = {"1", "true", "yes", "on"}
_JINJA = jinja2.Environment(undefined=jinja2.StrictUndefined, autoescape=False)


def _emit(obj: dict) -> None:
    print(json.dumps(obj))


def _flags() -> int:
    out = 0
    for token in os.environ.get("ROUTES_FLAGS", "i").split(","):
        out |= _FLAG_MAP.get(token.strip().lower(), 0)
    return out


def _load_routes() -> list:
    path = os.environ.get("ROUTES_FILE", "").strip()
    if path:
        try:
            with open(path, encoding="utf-8") as fh:
                raw = fh.read()
        except OSError as exc:
            print(f"[regex-router] cannot read ROUTES_FILE {path!r}: {exc}",
                  file=sys.stderr, flush=True)
            return []
    else:
        raw = os.environ.get("ROUTES", "[]")

    try:
        routes = json.loads(raw)
    except json.JSONDecodeError as exc:
        print(f"[regex-router] invalid ROUTES JSON: {exc}",
              file=sys.stderr, flush=True)
        return []

    if not isinstance(routes, list):
        print("[regex-router] ROUTES must be a JSON array", file=sys.stderr, flush=True)
        return []
    return routes


def main() -> None:
    envelope = json.load(sys.stdin)
    text = (envelope.get("pipeline_text") or envelope.get("text") or "").strip()

    routes = _load_routes()
    if not text or not routes:
        _emit({"continue": True})
        return

    flags = _flags()
    for route in routes:
        if not isinstance(route, dict):
            continue
        pattern = route.get("pattern")
        template = route.get("inject")
        if not pattern or not template:
            continue

        try:
            match = re.match(pattern, text, flags)
        except re.error as exc:
            print(f"[regex-router] bad pattern {pattern!r}: {exc}",
                  file=sys.stderr, flush=True)
            continue
        if not match:
            continue

        rest = text[match.end():].strip()
        try:
            new_text = _JINJA.from_string(template).render(
                rest=rest, text=text, groups=list(match.groups()),
            ).strip()
        except jinja2.TemplateError as exc:
            print(f"[regex-router] bad inject template {template!r}: {exc}",
                  file=sys.stderr, flush=True)
            _emit({"continue": True})
            return

        keep_going = os.environ.get("ON_MATCH_CONTINUE", "").strip().lower() in _TRUTHY
        print(f"[regex-router] {pattern!r} -> {new_text[:80]!r}"
              + (" (continue)" if keep_going else ""),
              file=sys.stderr, flush=True)
        _emit({"continue": keep_going, "inject": {"text": new_text}})
        return

    print("[regex-router] no route matched, continuing", file=sys.stderr, flush=True)
    _emit({"continue": True})


if __name__ == "__main__":
    main()
