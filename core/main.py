"""Sijba entrypoint: wires all modules together and starts the server."""
from __future__ import annotations
import asyncio
import signal
import sys
import uuid
from pathlib import Path

import uvicorn

import core.config as config
import core.log as log
import core.state as state
import core.msg_queue as queue
import core.store as store
import core.handler_loader as handler_loader
import core.lifecycle as lifecycle
import core.pipeline as pipeline
from core.web.api import app, configure as configure_api

_logger = None


# ------------------------------------------------------------------
# Source callback: called by source plugins when they emit a message
# ------------------------------------------------------------------

async def _source_callback(raw: dict) -> None:
    env = {
        "id": str(uuid.uuid4()),
        "parent_id": None,
        "direction": "inbound",
        "source": raw.get("source", "unknown"),
        "source_meta": raw.get("source_meta", {}),
        "text": raw.get("text", ""),
        "pipeline_text": raw.get("text", ""),
        "depth": 0,
        "metadata": raw.get("metadata", {}),
    }
    queue.enqueue_inbound(env)


# ------------------------------------------------------------------
# Hot-reload
# ------------------------------------------------------------------

def _do_reload(cfg: dict) -> None:
    _logger.info("Hot-reload triggered")
    handler_loader.reload(cfg["config_dir"])


# ------------------------------------------------------------------
# Startup
# ------------------------------------------------------------------

async def main() -> None:
    global _logger

    cfg = config.load()
    log.setup(cfg.get("log_level", "INFO"))
    _logger = log.get("main")
    state.log_buffer_max = cfg.get("log_buffer_size", 1000)

    config_dir: Path = cfg["config_dir"]
    _logger.info(f"Sijba starting — config_dir={config_dir}")

    # Queue
    db_path = cfg.get("db_path", "sijba.db")
    if not Path(db_path).is_absolute():
        db_path = str(config_dir / db_path)
    queue.init(db_path)
    store.init(db_path)
    _logger.info(f"Queue/store DB: {db_path}")

    # Load handlers
    state.handlers = handler_loader.load(config_dir)
    for h in state.handlers:
        hid = h["id"]
        if h.get("lifecycle") == "persistent":
            state.handler_states[hid] = "pending"
        else:
            state.handler_states[hid] = "healthy"

    # Wire pipeline
    pipeline.configure(
        enqueue_inbound=queue.enqueue_inbound,
        enqueue_outbound=queue.enqueue_outbound,
        config_dir=config_dir,
        max_inject_depth=cfg["max_inject_depth"],
    )

    # Configure web API
    configure_api(
        auth_token=cfg.get("auth_token"),
        webhook_token=cfg.get("webhook_token"),
        config_dir=config_dir,
        reload_fn=lambda: _do_reload(cfg),
    )

    # Start persistent handlers (including sources)
    await lifecycle.start_all(state.handlers, config_dir, _source_callback)

    # SIGHUP → hot reload
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGHUP, lambda: _do_reload(cfg))

    # Queue workers
    asyncio.create_task(queue.run_inbound_worker(pipeline.process_inbound))
    asyncio.create_task(queue.run_outbound_worker(pipeline.process_outbound))

    _logger.info(f"Listening on 0.0.0.0:{cfg['port']}")

    # Run FastAPI
    server_cfg = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=cfg["port"],
        log_config=None,  # we manage logging ourselves
    )
    server = uvicorn.Server(server_cfg)
    try:
        await server.serve()
    finally:
        await lifecycle.stop_all()
        _logger.info("Sijba stopped")


def run() -> None:
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    run()
