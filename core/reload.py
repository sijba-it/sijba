"""Apply edited config (core.yaml, handlers/) to the running Core without a restart."""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any, Dict, List

import core.config as config
import core.handler_loader as handler_loader
import core.log as log
import core.pipeline as pipeline
import core.registry as registry
import core.state as state
import core.web.api as api

_logger = log.get("reload")

# Read once at startup; changing these needs a process restart.
RESTART_KEYS = ("port", "db_path", "upload_dir", "runtime", "log_buffer_size")

_boot_cfg: Dict[str, Any] = {}
_lock = asyncio.Lock()


def configure(boot_cfg: Dict[str, Any]) -> None:
    global _boot_cfg
    _boot_cfg = dict(boot_cfg)


async def reload() -> Dict[str, Any]:
    async with _lock:
        cfg = config.load()
        config_dir: Path = cfg["config_dir"]

        api.configure(
            auth_token=cfg.get("auth_token"),
            webhook_token=cfg.get("webhook_token"),
            config_dir=config_dir,
            upload_dir=api._upload_dir,
            webhooks=cfg.get("webhooks"),
        )
        lvl = getattr(logging, str(cfg.get("log_level", "INFO")).upper(), logging.INFO)
        log.logger.setLevel(lvl)
        for lh in log.logger.handlers:
            lh.setLevel(lvl)
        pipeline._max_inject_depth = cfg["max_inject_depth"]

        errors: List[str] = []
        new_handlers = handler_loader.load(config_dir, errors=errors)

        removed = {h["id"] for h in state.file_handlers} - {h["id"] for h in new_handlers}
        for hid in removed:
            state.handler_states.pop(hid, None)
        state.file_handlers = new_handlers
        state.load_errors = errors
        for h in new_handlers:
            state.handler_states[h["id"]] = "healthy"
        registry.rebuild()

        restart_required = [k for k in RESTART_KEYS if cfg.get(k) != _boot_cfg.get(k)]
        result = {
            "handlers": [h["id"] for h in new_handlers],
            "webhooks": sorted(set((cfg.get("webhooks") or {}).values())),
            "restarted": [],
            "restart_required": restart_required,
            "errors": errors,
        }
        _logger.info(
            f"config reloaded: {len(new_handlers)} handlers, "
            f"errors={len(errors)}, restart_required={restart_required}"
        )
        return result
