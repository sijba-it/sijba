"""Invoke a single handler against an envelope. Returns plugin response dict."""
from __future__ import annotations
import asyncio
import json
import os
from pathlib import Path
from typing import Any, Dict

import httpx

import core.log as log
import core.state as state

_logger = log.get("plugin_runner")

# Absolute path to the plugins-builtin directory that ships with core.
# Use {sijba_plugins} in handler exec fields to reference it portably.
BUILTIN_DIR = Path(__file__).parent.parent / "plugins-builtin"


def resolve_env(handler: dict) -> dict:
    """Build the environment for a subprocess: inherit current env, merge handler
    ``env`` block, expand ``${VAR}`` references from the host environment."""
    merged = os.environ.copy()
    for key, val in (handler.get("env") or {}).items():
        merged[key] = os.path.expandvars(val)
    return merged


def _resolve_exec(exec_path: str, config_dir: Path) -> str:
    exec_path = exec_path.replace("{sijba_plugins}", str(BUILTIN_DIR))
    if exec_path.startswith("."):
        return str((config_dir / exec_path).resolve())
    return exec_path


def _context_key(handler: dict, envelope: dict) -> tuple[str, str] | None:
    """Return (namespace, key) if this handler uses context, else None."""
    ctx_cfg = handler.get("context")
    if not ctx_cfg:
        return None
    namespace = ctx_cfg.get("namespace") or handler["id"]
    key_field = ctx_cfg.get("key", "chat_id")
    key = (envelope.get("source_meta") or {}).get(key_field) or envelope.get("source", "default")
    return namespace, str(key)


async def invoke(handler: Dict[str, Any], envelope: dict, config_dir: Path) -> dict:
    """Invoke handler, return response. Never raises."""
    import core.store as store

    # Enrich envelope with context if handler requests it
    ctx_loc = _context_key(handler, envelope)
    if ctx_loc:
        namespace, key = ctx_loc
        ctx_data = store.get(namespace, key) or {}
        envelope = {**envelope, "context": ctx_data}

    # Enrich envelope with everything a router can route to: file/remote
    # handlers *and* live plugin channels. Without the channels a router is
    # blind to Cursor/Claude and can only ever answer "no route".
    if handler.get("receive_handler_context"):
        import core.registry as registry

        summaries = [
            {"id": h["id"], "router_hint": h["router_hint"]}
            for h in state.handlers
            if h.get("router_hint") and h["id"] != handler["id"]
        ]
        summaries += registry.channel_routing_targets()
        envelope = {**envelope, "handlers": summaries}

    lifecycle = handler.get("lifecycle", "per_message")

    if lifecycle == "persistent":
        ph = state.persistent.get(handler["id"])
        if ph is None:
            _logger.error(f"No persistent instance for handler '{handler['id']}'")
            return {"continue": True}
        response = await ph.invoke(envelope)
    else:
        response = await _invoke_per_message(handler, envelope, config_dir)

    # Save context_update if returned
    if ctx_loc and "context_update" in response:
        namespace, key = ctx_loc
        store.set(namespace, key, response.pop("context_update"))

    return response


async def _invoke_per_message(handler: Dict[str, Any], envelope: dict, config_dir: Path) -> dict:
    htype = handler.get("type", "exec_json")
    if htype == "exec_json":
        return await _exec_json(handler, envelope, config_dir)
    if htype == "http":
        return await _http_once(handler, envelope)
    _logger.error(f"Unknown handler type '{htype}'")
    return {"continue": True}


async def _exec_json(handler: dict, envelope: dict, config_dir: Path) -> dict:
    """Spawn the plugin, hand it the envelope as JSON on stdin, read a JSON
    response back from stdout."""
    exec_path = _resolve_exec(handler.get("exec", ""), config_dir)
    timeout = handler.get("timeout_seconds", 10)
    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            exec_path, *handler.get("args", []),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=resolve_env(handler),
        )
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(json.dumps(envelope).encode()), timeout=timeout
        )
    except asyncio.TimeoutError:
        _logger.error(f"[{handler['id']}] exec_json timed out after {timeout}s")
        if proc is not None:
            try:
                proc.kill()
            except Exception:
                pass
        return {"continue": True}
    except Exception as exc:
        _logger.error(f"[{handler['id']}] exec_json error: {exc}")
        return {"continue": True}

    if stderr:
        _logger.debug(f"[{handler['id']}] stderr: {stderr.decode().rstrip()}")

    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        _logger.error(f"[{handler['id']}] exec_json invalid JSON output: {stdout!r}")
        return {"continue": True}


async def _http_once(handler: dict, envelope: dict) -> dict:
    """POST the envelope to a remote handler (see registry.register)."""
    startup = handler.get("startup") or {}
    url = startup.get("url", "").rstrip("/") + startup.get("invoke_path", "/invoke")
    try:
        async with httpx.AsyncClient() as client:
            r = await client.post(
                url, json=envelope, timeout=handler.get("timeout_seconds", 10)
            )
            return r.json()
    except Exception as exc:
        _logger.error(f"[{handler['id']}] http invoke error: {exc}")
        return {"continue": True}
