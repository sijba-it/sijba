"""Persistent handler process management: start, watchdog, restart, NDJSON transport.

Combined source+outbound handlers
----------------------------------
A handler with ``direction: source`` normally only emits inbound messages to
the pipeline.  But a plugin may also want to *receive* outbound invocations
(e.g. a Telegram plugin that both polls for messages and sends replies).

To support this, every source handler's stdout reader is unified: lines with
a ``req_id`` key are treated as NDJSON responses; lines without are treated as
source events.  The plugin can therefore multiplex both on a single
stdin/stdout pair.

To wire the outbound side, add ``"shared_process": "<source-handler-id>"``
to the outbound handler config.  That handler will reuse the already-running
process for NDJSON invocations instead of spawning its own process.
"""
from __future__ import annotations
import asyncio
import json
import shlex
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

import httpx

import core.log as log
import core.state as state
from core.plugin_runner import BUILTIN_DIR, resolve_env

_logger = log.get("lifecycle")


class PersistentHandler:
    """Manages one persistent handler process."""

    def __init__(self, handler: Dict[str, Any], config_dir: Path) -> None:
        self.handler = handler
        self.config_dir = config_dir
        self.hid: str = handler["id"]
        self.process: Optional[asyncio.subprocess.Process] = None
        self._watchdog_task: Optional[asyncio.Task] = None
        self._stdout_reader_task: Optional[asyncio.Task] = None
        self._ndjson_pending: Dict[str, asyncio.Future] = {}
        self._source_callback = None

    # ------------------------------------------------------------------
    # State helpers
    # ------------------------------------------------------------------

    def _set_state(self, s: str) -> None:
        state.handler_states[self.hid] = s
        _logger.info(f"[{self.hid}] state → {s}")

    def _get_state(self) -> str:
        return state.handler_states.get(self.hid, "pending")

    # ------------------------------------------------------------------
    # Build command
    # ------------------------------------------------------------------

    def _cmd(self) -> list[str]:
        h = self.handler
        exec_path = h.get("exec", "")
        exec_path = exec_path.replace("{sijba_plugins}", str(BUILTIN_DIR))
        if exec_path.startswith("."):
            exec_path = str((self.config_dir / exec_path).resolve())
        parts = shlex.split(exec_path)
        parts += h.get("args", [])
        return parts

    # ------------------------------------------------------------------
    # Start
    # ------------------------------------------------------------------

    async def start(self, source_callback=None) -> bool:
        """Start the process. Returns True if healthy within timeout."""
        if self._get_state() == "stopped":
            return False

        # shared_process: don't spawn, reuse another handler's process
        shared_id = self.handler.get("shared_process")
        if shared_id:
            return await self._attach_shared(shared_id)

        self._source_callback = source_callback
        self._set_state("starting")

        cmd = self._cmd()
        _logger.info(f"[{self.hid}] starting: {' '.join(cmd)}")

        try:
            self.process = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=resolve_env(self.handler),
            )
        except Exception as exc:
            _logger.error(f"[{self.hid}] failed to start: {exc}")
            return await self._handle_start_failure()

        asyncio.create_task(self._log_stderr())

        startup = self.handler.get("startup") or {}
        timeout = startup.get("startup_timeout_seconds", 30)

        if self.handler["type"] == "http":
            healthy = await self._wait_for_http_health(startup, timeout)
        else:
            healthy = True

        if not healthy:
            return await self._handle_start_failure()

        self._set_state("healthy")

        # Unified stdout reader for ALL persistent non-http handlers.
        # Source lines (no req_id) → source_callback.
        # NDJSON responses (with req_id) → dispatch to waiting futures.
        if self.handler["type"] != "http":
            self._stdout_reader_task = asyncio.create_task(self._read_stdout())

        self._watchdog_task = asyncio.create_task(self._watchdog())
        return True

    async def _attach_shared(self, shared_id: str) -> bool:
        """Wire this handler to an already-running handler's process."""
        owner = state.persistent.get(shared_id)
        if owner is None:
            _logger.error(f"[{self.hid}] shared_process '{shared_id}' not found")
            self._set_state("degraded")
            return False
        # Borrow the process reference; NDJSON pending dict is our own.
        self.process = owner.process
        self._set_state("healthy")
        _logger.info(f"[{self.hid}] attached to shared process of '{shared_id}'")
        return True

    async def _handle_start_failure(self) -> bool:
        if self.handler.get("required", False):
            _logger.error(f"[{self.hid}] required handler failed — aborting")
            raise RuntimeError(f"Required handler '{self.hid}' failed to start")
        self._set_state("degraded")
        return False

    async def _wait_for_http_health(self, startup: dict, timeout: float) -> bool:
        url = startup.get("url", "")
        health_path = startup.get("health_path", "/health")
        health_url = url.rstrip("/") + health_path
        deadline = time.monotonic() + timeout
        async with httpx.AsyncClient() as client:
            while time.monotonic() < deadline:
                try:
                    r = await client.get(health_url, timeout=2.0)
                    if r.status_code < 500:
                        return True
                except Exception:
                    pass
                await asyncio.sleep(0.5)
        _logger.error(f"[{self.hid}] health check timed out after {timeout}s")
        return False

    # ------------------------------------------------------------------
    # Invocation
    # ------------------------------------------------------------------

    async def invoke(self, envelope: dict) -> dict:
        if self._get_state() != "healthy":
            return {"continue": True}
        if self.handler["type"] == "http":
            return await self._invoke_http(envelope)
        return await self._invoke_ndjson(envelope)

    async def _invoke_http(self, envelope: dict) -> dict:
        startup = self.handler.get("startup") or {}
        socket_path = startup.get("socket_path")
        base_url = startup.get("url", "http://localhost:8765")
        url = base_url.rstrip("/") + startup.get("invoke_path", "/invoke")
        timeout = self.handler.get("timeout_seconds", 10)
        try:
            transport = httpx.AsyncHTTPTransport(uds=socket_path) if socket_path else None
            async with httpx.AsyncClient(transport=transport) as client:
                r = await client.post(url, json=envelope, timeout=timeout)
                return r.json()
        except Exception as exc:
            _logger.error(f"[{self.hid}] HTTP invoke error: {exc}")
            return {"continue": True}

    async def _invoke_ndjson(self, envelope: dict) -> dict:
        if not self.process or self.process.stdin is None:
            return {"continue": True}

        req_id = str(uuid.uuid4())
        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        self._ndjson_pending[req_id] = fut

        line = json.dumps({"req_id": req_id, "message": envelope}) + "\n"
        try:
            self.process.stdin.write(line.encode())
            await self.process.stdin.drain()
        except Exception as exc:
            _logger.error(f"[{self.hid}] NDJSON write error: {exc}")
            self._ndjson_pending.pop(req_id, None)
            return {"continue": True}

        timeout = self.handler.get("timeout_seconds", 10)
        try:
            result = await asyncio.wait_for(fut, timeout=timeout)
            result.pop("req_id", None)
            return result
        except asyncio.TimeoutError:
            _logger.error(f"[{self.hid}] NDJSON invoke timed out")
            return {"continue": True}
        finally:
            self._ndjson_pending.pop(req_id, None)

    # ------------------------------------------------------------------
    # Unified stdout reader
    # ------------------------------------------------------------------

    async def _read_stdout(self) -> None:
        """Reads stdout of the process.

        Lines with ``req_id`` → NDJSON response → dispatch to pending futures
        (including futures registered by shared-process handlers).
        Lines without ``req_id`` → source event → source_callback.
        """
        assert self.process and self.process.stdout
        while True:
            try:
                raw = await self.process.stdout.readline()
            except Exception:
                break
            if not raw:
                break
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                _logger.warning(f"[{self.hid}] non-JSON stdout: {raw!r}")
                continue

            req_id = data.get("req_id")
            if req_id:
                # NDJSON response — check our own pending dict first, then all
                # handlers that share our process.
                dispatched = self._dispatch_ndjson(req_id, data)
                if not dispatched:
                    # May belong to a handler sharing this process
                    for ph in state.persistent.values():
                        if ph is not self and ph.process is self.process:
                            if ph._dispatch_ndjson(req_id, data):
                                break
            else:
                if self._source_callback:
                    await self._source_callback(data)

    def _dispatch_ndjson(self, req_id: str, data: dict) -> bool:
        fut = self._ndjson_pending.get(req_id)
        if fut and not fut.done():
            fut.set_result(data)
            return True
        return False

    # ------------------------------------------------------------------
    # Stderr logger
    # ------------------------------------------------------------------

    async def _log_stderr(self) -> None:
        assert self.process and self.process.stderr
        while True:
            line = await self.process.stderr.readline()
            if not line:
                break
            _logger.warning(f"[{self.hid}] stderr: {line.decode().rstrip()}")

    # ------------------------------------------------------------------
    # Watchdog
    # ------------------------------------------------------------------

    async def _watchdog(self) -> None:
        assert self.process
        await self.process.wait()
        rc = self.process.returncode
        _logger.warning(f"[{self.hid}] process exited (rc={rc})")

        if self._get_state() == "stopped":
            return

        policy = (self.handler.get("startup") or {}).get("restart_policy", "on_failure")
        if policy == "never":
            self._set_state("degraded")
            return
        if policy == "on_failure" and rc == 0:
            self._set_state("degraded")
            return

        # Re-attach shared handlers after the owning process restarts
        self._set_state("restarting")
        await asyncio.sleep(2)
        await self.start(self._source_callback)

    # ------------------------------------------------------------------
    # Stop
    # ------------------------------------------------------------------

    async def stop(self) -> None:
        self._set_state("stopped")
        if self._watchdog_task:
            self._watchdog_task.cancel()
        if self._stdout_reader_task:
            self._stdout_reader_task.cancel()
        shared_id = self.handler.get("shared_process")
        if shared_id:
            # Don't kill a process we don't own
            return
        if self.process and self.process.returncode is None:
            try:
                self.process.terminate()
                await asyncio.wait_for(self.process.wait(), timeout=5)
            except Exception:
                self.process.kill()


# ------------------------------------------------------------------
# Module-level helpers called from main.py
# ------------------------------------------------------------------

async def start_all(handlers: list, config_dir: Path, source_callback) -> None:
    # Two passes: start real processes first, then attach shared-process handlers
    owned = [h for h in handlers if h.get("lifecycle") == "persistent" and not h.get("shared_process")]
    shared = [h for h in handlers if h.get("lifecycle") == "persistent" and h.get("shared_process")]

    for h in handlers:
        if h.get("lifecycle") != "persistent":
            state.handler_states[h["id"]] = "healthy"

    for h in owned:
        ph = PersistentHandler(h, config_dir)
        state.persistent[h["id"]] = ph
        state.handler_states[h["id"]] = "pending"
        cb = source_callback if h.get("direction") == "source" else None
        await ph.start(source_callback=cb)

    for h in shared:
        ph = PersistentHandler(h, config_dir)
        state.persistent[h["id"]] = ph
        state.handler_states[h["id"]] = "pending"
        await ph.start()


async def stop_all() -> None:
    # Stop owned processes first (shared handlers will be cleaned up automatically)
    for ph in state.persistent.values():
        await ph.stop()
