"""Per-channel permission flags — currently just `can_inject` — backed by a
small YAML file in the config directory, not the generic KV store: this is
config (git-versionable, visible in the config-edit tool alongside
core.yaml/handlers), not runtime/session state.

Deliberately a plain flag file, not a capability the connecting plugin
declares about itself: the plugin process runs LLM-driven, potentially
prompt-injected shell commands and must never be the authority on its own
permissions. The Agent Controller (operator-side, trusted the same way
registry.json's `plugin` field already is) sets this via the admin API in
core/web/api.py, which calls set_can_inject() below.

Known limitation, deliberately accepted for now: this only restricts WHICH
channel_ids may act with a permission, not WHO is actually sending on a
given channel_id — every plugin connects with the same shared SIJBA_TOKEN
and self-declares its own channel_id, so nothing here stops one channel
from impersonating another's identity outright. Fine for a single-person
instance; would need real per-channel credentials before ever exposing
this to multiple tenants.
"""
from __future__ import annotations
import os
from pathlib import Path
from typing import Dict

import yaml

import core.log as log

_logger = log.get("channel_permissions")

_FILENAME = "channel_permissions.yaml"
_path: Path | None = None
_permissions: Dict[str, dict] = {}


def init(config_dir: Path) -> None:
    global _path, _permissions
    _path = config_dir / _FILENAME
    _permissions = _load()


def _load() -> Dict[str, dict]:
    if not _path or not _path.exists():
        return {}
    try:
        data = yaml.safe_load(_path.read_text()) or {}
    except (yaml.YAMLError, OSError) as exc:
        _logger.error(f"failed to read {_path}: {exc}")
        return {}
    if not isinstance(data, dict):
        _logger.error(f"{_path} must be a mapping of channel_id -> {{can_inject: bool}}")
        return {}
    return data


def _save() -> None:
    if not _path:
        return
    _path.parent.mkdir(parents=True, exist_ok=True)
    tmp = _path.with_suffix(".yaml.tmp")
    tmp.write_text(yaml.safe_dump(_permissions, sort_keys=True))
    os.replace(tmp, _path)


def can_inject(channel_id: str) -> bool:
    return bool(_permissions.get(channel_id, {}).get("can_inject", False))


def set_can_inject(channel_id: str, allowed: bool) -> None:
    _permissions.setdefault(channel_id, {})["can_inject"] = bool(allowed)
    _save()
    _logger.info(f"can_inject for {channel_id!r} set to {allowed}")


def all_permissions() -> Dict[str, dict]:
    """Read-only snapshot, for the admin API / config-edit tool."""
    return dict(_permissions)
