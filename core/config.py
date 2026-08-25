"""Load core.yaml from SIJBA_CONFIG_DIR, override with env vars."""
from __future__ import annotations
import os
from pathlib import Path
from typing import Any, Dict

import yaml

DEFAULTS: Dict[str, Any] = {
    "port": 8080,
    "max_inject_depth": 5,
    "auth_token": None,
    "webhook_token": None,
    "log_level": "INFO",
    "log_buffer_size": 1000,
    "db_path": "sijba.db",
    "upload_dir": None,
}

_cfg: Dict[str, Any] = {}


def load() -> Dict[str, Any]:
    global _cfg
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
    _cfg = cfg
    return cfg


def get() -> Dict[str, Any]:
    if not _cfg:
        return load()
    return _cfg
