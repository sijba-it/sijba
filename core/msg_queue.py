"""SQLite-backed inbound/outbound queues with asyncio worker loops."""
from __future__ import annotations
import asyncio
import json
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from typing import Awaitable, Callable, Optional

import core.log as log

_logger = log.get("queue")
_lock = threading.Lock()
_conn: Optional[sqlite3.Connection] = None


def init(db_path: str) -> None:
    global _conn
    _conn = sqlite3.connect(db_path, check_same_thread=False)
    _conn.execute("PRAGMA journal_mode=WAL")
    _conn.executescript("""
        CREATE TABLE IF NOT EXISTS inbound_queue (
            id          TEXT PRIMARY KEY,
            envelope_json TEXT NOT NULL,
            status      TEXT NOT NULL DEFAULT 'pending',
            depth       INTEGER NOT NULL DEFAULT 0,
            created_at  TEXT NOT NULL,
            claimed_at  TEXT
        );
        CREATE TABLE IF NOT EXISTS outbound_queue (
            id          TEXT PRIMARY KEY,
            envelope_json TEXT NOT NULL,
            status      TEXT NOT NULL DEFAULT 'pending',
            depth       INTEGER NOT NULL DEFAULT 0,
            created_at  TEXT NOT NULL,
            claimed_at  TEXT
        );
    """)
    _conn.commit()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def enqueue_inbound(envelope: dict) -> str:
    mid = envelope.setdefault("id", str(uuid.uuid4()))
    with _lock:
        _conn.execute(
            "INSERT OR IGNORE INTO inbound_queue (id, envelope_json, status, depth, created_at)"
            " VALUES (?, ?, 'pending', ?, ?)",
            (mid, json.dumps(envelope), envelope.get("depth", 0), _now()),
        )
        _conn.commit()
    _logger.debug(f"Enqueued inbound {mid}")
    return mid


def enqueue_outbound(envelope: dict) -> str:
    mid = envelope.setdefault("id", str(uuid.uuid4()))
    with _lock:
        _conn.execute(
            "INSERT OR IGNORE INTO outbound_queue (id, envelope_json, status, depth, created_at)"
            " VALUES (?, ?, 'pending', ?, ?)",
            (mid, json.dumps(envelope), envelope.get("depth", 0), _now()),
        )
        _conn.commit()
    _logger.debug(f"Enqueued outbound {mid}")
    return mid


def _claim_next(table: str) -> Optional[tuple[str, dict]]:
    with _lock:
        row = _conn.execute(
            f"SELECT id, envelope_json FROM {table}"
            " WHERE status = 'pending' ORDER BY created_at LIMIT 1"
        ).fetchone()
        if not row:
            return None
        mid, env_json = row
        _conn.execute(
            f"UPDATE {table} SET status = 'claimed', claimed_at = ? WHERE id = ?",
            (_now(), mid),
        )
        _conn.commit()
    return mid, json.loads(env_json)


def _set_status(table: str, mid: str, status: str) -> None:
    with _lock:
        _conn.execute(f"UPDATE {table} SET status = ? WHERE id = ?", (status, mid))
        _conn.commit()


def depths() -> dict:
    with _lock:
        def counts(table: str) -> dict:
            rows = _conn.execute(
                f"SELECT status, COUNT(*) FROM {table} GROUP BY status"
            ).fetchall()
            return {r[0]: r[1] for r in rows}
        return {"inbound": counts("inbound_queue"), "outbound": counts("outbound_queue")}


async def run_inbound_worker(fn: Callable[[dict], Awaitable[None]]) -> None:
    while True:
        result = _claim_next("inbound_queue")
        if result:
            mid, env = result
            try:
                await fn(env)
                _set_status("inbound_queue", mid, "done")
            except Exception as exc:
                _logger.error(f"Inbound worker error [{mid}]: {exc}")
                _set_status("inbound_queue", mid, "failed")
        else:
            await asyncio.sleep(0.05)


async def run_outbound_worker(fn: Callable[[dict], Awaitable[None]]) -> None:
    while True:
        result = _claim_next("outbound_queue")
        if result:
            mid, env = result
            try:
                await fn(env)
                _set_status("outbound_queue", mid, "done")
            except Exception as exc:
                _logger.error(f"Outbound worker error [{mid}]: {exc}")
                _set_status("outbound_queue", mid, "failed")
        else:
            await asyncio.sleep(0.05)
