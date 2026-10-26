"""Apply edited config (core.yaml, handlers/) to the running Core without a restart."""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import core.config as config
import core.handler_loader as handler_loader
import core.lifecycle as lifecycle
import core.log as log
import core.pipeline as pipeline
import core.registry as registry
import core.state as state
import core.web.api as api

_logger = log.get("reload")

# Read once at startup; changing these needs a process restart.
RESTART_KEYS = ("port", "db_path", "upload_dir", "runtime", "log_buffer_size")

_boot_cfg: Dict[str, Any] = {}
_source_callback: Optional[Callable] = None
_lock = asyncio.Lock()


def configure(boot_cfg: Dict[str, Any], source_callback: Callable) -> None:
    global _boot_cfg, _source_callback
    _boot_cfg = dict(boot_cfg)
    _source_callback = source_callback


def _persistent(handlers: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {h["id"]: h for h in handlers if h.get("lifecycle") == "persistent"}


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
        for h in log.logger.handlers:
            h.setLevel(lvl)
        pipeline._max_inject_depth = cfg["max_inject_depth"]

        errors: List[str] = []
        new_handlers = handler_loader.load(config_dir, errors=errors)
        old_p, new_p = _persistent(state.file_handlers), _persistent(new_handlers)
        restart = old_p != new_p

        if restart:
            for hid in old_p:
                ph = state.persistent.pop(hid, None)
                if ph is not None:
                    await ph.stop()
                state.handler_states.pop(hid, None)

        removed = {h["id"] for h in state.file_handlers} - {h["id"] for h in new_handlers}
        for hid in removed:
            state.handler_states.pop(hid, None)
        state.file_handlers = new_handlers
        for h in new_handlers:
            if h.get("lifecycle") != "persistent":
                state.handler_states[h["id"]] = "healthy"
        registry.rebuild()

        if restart:
            await lifecycle.start_all(list(new_p.values()), config_dir, _source_callback)

        restart_required = [k for k in RESTART_KEYS if cfg.get(k) != _boot_cfg.get(k)]
        result = {
            "handlers": [h["id"] for h in new_handlers],
            "webhooks": sorted(set((cfg.get("webhooks") or {}).values())),
            "restarted": sorted(new_p) if restart else [],
            "restart_required": restart_required,
            "errors": errors,
        }
        _logger.info(
            f"config reloaded: {len(new_handlers)} handlers, "
            f"restarted={result['restarted']}, errors={len(errors)}, "
            f"restart_required={restart_required}"
        )
        return result
