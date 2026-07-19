"""Load, validate, and sort handler configs from the config directory."""
from __future__ import annotations
import json
import re
from pathlib import Path
from typing import Any, Dict, List

import jsonschema

import core.log as log
import core.state as state

_logger = log.get("handler_loader")
_SCHEMA_PATH = Path(__file__).parent.parent / "schema" / "handler.schema.json"
_schema: dict = {}

DEFAULTS: Dict[str, Any] = {
    "priority": 50.0,
    "category": [],
    "lifecycle": "per_message",
    "required": False,
    "await_response": True,
    "allow_direct_inject": False,
    "continue_on_match": True,
    "args": [],
    "capture_stdout": False,
    "timeout_seconds": 10,
}


def _get_schema() -> dict:
    global _schema
    if not _schema:
        with open(_SCHEMA_PATH) as fh:
            _schema = json.load(fh)
    return _schema


def _priority_from_filename(stem: str) -> float | None:
    m = re.match(r"^(\d+(?:\.\d+)?)", stem)
    return float(m.group(1)) if m else None


def load(config_dir: Path) -> List[Dict[str, Any]]:
    schema = _get_schema()
    handlers_dir = config_dir / "handlers"
    if not handlers_dir.exists():
        _logger.warning(f"handlers/ not found in {config_dir}")
        return []

    loaded: List[Dict[str, Any]] = []
    for path in sorted(handlers_dir.glob("*.json")):
        try:
            with open(path) as fh:
                handler: Dict[str, Any] = json.load(fh)
        except json.JSONDecodeError as exc:
            _logger.error(f"JSON parse error in {path.name}: {exc}")
            continue

        # Derive priority from filename if not explicit
        if "priority" not in handler:
            fp = _priority_from_filename(path.stem)
            if fp is not None:
                handler["priority"] = fp

        # Apply defaults
        for key, val in DEFAULTS.items():
            handler.setdefault(key, val)

        # Validate against schema
        try:
            jsonschema.validate(handler, schema)
        except jsonschema.ValidationError as exc:
            _logger.error(f"Handler {path.name} validation error: {exc.message}")
            continue

        loaded.append(handler)
        _logger.info(f"Loaded handler '{handler['id']}' (priority={handler['priority']})")

    loaded.sort(key=lambda h: h["priority"])
    return loaded


def reload(config_dir: Path) -> None:
    """Hot-reload file handlers, then re-merge dynamic registrations.

    Per-message handlers take effect immediately; persistent handlers are NOT restarted.
    """
    import core.registry as registry

    new_handlers = load(config_dir)
    state.file_handlers = new_handlers

    # Initialise state for new file handlers; preserve state for existing ones
    new_ids = {h["id"] for h in new_handlers} | set(state.registered_handlers.keys())
    for h in new_handlers:
        hid = h["id"]
        if hid not in state.handler_states:
            if h["lifecycle"] == "persistent":
                state.handler_states[hid] = "pending"
            else:
                state.handler_states[hid] = "healthy"

    # Remove stale states for handlers that were removed (keep registered remotes)
    for hid in list(state.handler_states.keys()):
        if hid not in new_ids:
            del state.handler_states[hid]

    registry.rebuild()
    _logger.info(
        f"Reloaded {len(new_handlers)} file handlers "
        f"+ {len(state.registered_handlers)} registered"
    )
