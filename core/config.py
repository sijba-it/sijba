"""Load core.yaml from SIJBA_CONFIG_DIR, override with env vars."""
from __future__ import annotations
import os
import re
from pathlib import Path
from typing import Any, Dict

import yaml

import core.log as log

_logger = log.get("config")

DEFAULTS: Dict[str, Any] = {
    "port": 8080,
    "max_inject_depth": 5,
    "auth_token": None,
    "webhook_token": None,
    "log_level": "INFO",
    "log_buffer_size": 1000,
    "db_path": "sijba.db",
    "upload_dir": None,
    "webhooks": {},
}


def _webhook_tokens(raw: Any) -> Dict[str, str]:
    """Turn the `webhooks:` block ({name: token}) into {token: name}.

    Token values support ``${VAR}`` so the names can live in a ConfigMap while
    the secrets stay in the environment, the same convention handler `env`
    blocks use. An unresolved ``${VAR}`` is dropped rather than registered
    verbatim — otherwise a missing variable would silently create a webhook
    whose token is the literal, guessable string "${VAR}".
    """
    if not raw:
        return {}
    if not isinstance(raw, dict):
        _logger.error("config: 'webhooks' must be a mapping of name -> token")
        return {}

    out: Dict[str, str] = {}
    for name, token in raw.items():
        slug = re.sub(r"[^a-z0-9_-]", "", str(name).strip().lower())[:32]
        value = os.path.expandvars(str(token or "")).strip()
        if not slug:
            _logger.error(f"config: webhook name {name!r} is not a usable slug — skipped")
            continue
        if not value or "${" in value:
            _logger.error(f"config: webhook '{slug}' has no resolved token — skipped")
            continue
        if value in out:
            _logger.error(
                f"config: webhook '{slug}' reuses the token of '{out[value]}' — skipped"
            )
            continue
        out[value] = slug
    return out

def load() -> Dict[str, Any]:
    config_dir = Path(os.environ.get("SIJBA_CONFIG_DIR", "./sijba-config"))

    cfg = dict(DEFAULTS)
    cfg["config_dir"] = config_dir

    core_yaml = config_dir / "core.yaml"
    if core_yaml.exists():
        with open(core_yaml) as fh:
            data = yaml.safe_load(fh) or {}
        cfg.update(data)

    env_map = {
        "SIJBA_AUTH_TOKEN": "auth_token",
        "SIJBA_WEBHOOK_TOKEN": "webhook_token",
        "SIJBA_PORT": "port",
        "SIJBA_DB_PATH": "db_path",
        "SIJBA_UPLOAD_DIR": "upload_dir",
        "SIJBA_LOG_LEVEL": "log_level",
    }
    for env_key, cfg_key in env_map.items():
        if env_key in os.environ:
            cfg[cfg_key] = os.environ[env_key]

    cfg["port"] = int(cfg["port"])
    cfg["max_inject_depth"] = int(cfg["max_inject_depth"])
    cfg["webhooks"] = _webhook_tokens(cfg.get("webhooks"))
    return cfg
