"""Sijba entrypoint: wires all modules together and starts the server."""
from __future__ import annotations
import asyncio
import shutil
import time
from pathlib import Path

import uvicorn

import core.config as config
import core.db as db
import core.log as log
import core.state as state
import core.msg_queue as queue
import core.store as store
import core.transcript as transcript
import core.handler_loader as handler_loader
import core.lifecycle as lifecycle
import core.pipeline as pipeline
import core.registry as registry
import core.reply as reply
from core.envelope import new_envelope
from core.web.api import app, configure as configure_api

_logger = None


# ------------------------------------------------------------------
# Source callback: called by source plugins when they emit a message
# ------------------------------------------------------------------

async def _source_callback(raw: dict) -> None:
    queue.enqueue_inbound(new_envelope(
        source=raw.get("source", "unknown"),
        text=raw.get("text", ""),
        source_meta=raw.get("source_meta", {}),
        metadata=raw.get("metadata", {}),
    ))


# ------------------------------------------------------------------
# Startup
# ------------------------------------------------------------------

async def main() -> None:
    global _logger

    cfg = config.load()
    log.setup(cfg.get("log_level", "INFO"))
    _logger = log.get("main")
    state.configure_log_buffer(cfg.get("log_buffer_size", 1000))

    config_dir: Path = cfg["config_dir"]
    _logger.info(f"Sijba starting — config_dir={config_dir}")

    # One SQLite connection, shared by queue + store + transcript
    db_path = cfg.get("db_path", "sijba.db")
    if not Path(db_path).is_absolute():
        db_path = str(config_dir / db_path)
    db.connect(db_path)
    queue.init()
    store.init()
    transcript.init()
    _logger.info(f"Queue/store DB: {db_path}")

    # Uploads — default next to the DB (persistent volume), not config_dir
    # (which is a read-only ConfigMap mount in k8s).
    upload_dir = cfg.get("upload_dir")
    if not upload_dir:
        upload_dir = Path(db_path).parent / "uploads"
    else:
        upload_dir = Path(upload_dir)
        if not upload_dir.is_absolute():
            upload_dir = config_dir / upload_dir
    upload_dir.mkdir(parents=True, exist_ok=True)
    _logger.info(f"Upload dir: {upload_dir}")

    # Load handlers (files), then merge any dynamic registrations (none at boot)
    state.file_handlers = handler_loader.load(config_dir)
    for h in state.file_handlers:
        hid = h["id"]
        if h.get("lifecycle") == "persistent":
            state.handler_states[hid] = "pending"
        else:
            state.handler_states[hid] = "healthy"
    registry.rebuild()

    # Wire pipeline
    reply.configure(enqueue_outbound=queue.enqueue_outbound)
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
        upload_dir=upload_dir,
        webhooks=cfg.get("webhooks"),
    )
    for name in sorted(set((cfg.get("webhooks") or {}).values())):
        _logger.info(f"Webhook configured: webhook:{name}")

    # Start persistent handlers (including sources)
    await lifecycle.start_all(state.handlers, config_dir, _source_callback)

    # Queue workers
    asyncio.create_task(queue.run_inbound_worker(pipeline.process_inbound))
    asyncio.create_task(queue.run_outbound_worker(pipeline.process_outbound))

    # Expire stale remote handler registrations
    async def _expire_loop() -> None:
        while True:
            await asyncio.sleep(15)
            try:
                registry.expire_stale()
            except Exception as exc:
                _logger.error(f"registry expire: {exc}")

    asyncio.create_task(_expire_loop())

    # Delete uploaded files older than the TTL — self-hosted single-user
    # instance, so a simple age sweep beats tracking per-file references.
    UPLOAD_TTL_SECONDS = 3 * 24 * 3600

    async def _upload_cleanup_loop() -> None:
        while True:
            await asyncio.sleep(3600)
            try:
                cutoff = time.time() - UPLOAD_TTL_SECONDS
                for d in upload_dir.iterdir():
                    if d.is_dir() and d.stat().st_mtime < cutoff:
                        shutil.rmtree(d, ignore_errors=True)
                        _logger.info(f"upload cleanup: removed {d.name}")
            except Exception as exc:
                _logger.error(f"upload cleanup: {exc}")

    asyncio.create_task(_upload_cleanup_loop())

    # Start plugin cleanup task
    registry.start_plugin_cleanup_task()

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
        registry.stop_plugin_cleanup_task()
        _logger.info("Sijba stopped")


def run() -> None:
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    run()
