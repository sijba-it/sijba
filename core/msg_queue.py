"""SQLite-backed inbound/outbound queues with asyncio worker loops."""
from __future__ import annotations
import asyncio
import json
import uuid
from typing import Awaitable, Callable, Optional

import core.db as db
import core.log as log

_logger = log.get("queue")

INBOUND = "inbound_queue"
OUTBOUND = "outbound_queue"

_SCHEMA = """
    CREATE TABLE IF NOT EXISTS {table} (
        id          TEXT PRIMARY KEY,
        envelope_json TEXT NOT NULL,
        status      TEXT NOT NULL DEFAULT 'pending',
        depth       INTEGER NOT NULL DEFAULT 0,
        created_at  TEXT NOT NULL,
        claimed_at  TEXT
    );
"""


def init() -> None:
    with db.write() as conn:
        conn.executescript(_SCHEMA.format(table=INBOUND) + _SCHEMA.format(table=OUTBOUND))


def enqueue(table: str, envelope: dict) -> str:
    mid = envelope.setdefault("id", str(uuid.uuid4()))
    db.execute(
        f"INSERT OR IGNORE INTO {table} (id, envelope_json, status, depth, created_at)"
        " VALUES (?, ?, 'pending', ?, ?)",
        (mid, json.dumps(envelope), envelope.get("depth", 0), db.now()),
    )
    _logger.debug(f"Enqueued {table} {mid}")
    return mid


def enqueue_inbound(envelope: dict) -> str:
    return enqueue(INBOUND, envelope)


def enqueue_outbound(envelope: dict) -> str:
    return enqueue(OUTBOUND, envelope)


def _claim_next(table: str) -> Optional[tuple[str, dict]]:
    with db.write() as conn:
        row = conn.execute(
            f"SELECT id, envelope_json FROM {table}"
            " WHERE status = 'pending' ORDER BY created_at LIMIT 1"
        ).fetchone()
        if not row:
            return None
        mid, env_json = row
        conn.execute(
            f"UPDATE {table} SET status = 'claimed', claimed_at = ? WHERE id = ?",
            (db.now(), mid),
        )
    return mid, json.loads(env_json)


def _set_status(table: str, mid: str, status: str) -> None:
    db.execute(f"UPDATE {table} SET status = ? WHERE id = ?", (status, mid))


def depths() -> dict:
    def counts(table: str) -> dict:
        rows = db.query(f"SELECT status, COUNT(*) FROM {table} GROUP BY status")
        return {r[0]: r[1] for r in rows}

    return {"inbound": counts(INBOUND), "outbound": counts(OUTBOUND)}


async def run_worker(table: str, fn: Callable[[dict], Awaitable[None]]) -> None:
    """Claim and process one message at a time; never let one failure stall
    the queue."""
    while True:
        claimed = _claim_next(table)
        if not claimed:
            await asyncio.sleep(0.05)
            continue
        mid, env = claimed
        try:
            await fn(env)
            _set_status(table, mid, "done")
        except Exception as exc:
            _logger.error(f"{table} worker error [{mid}]: {exc}")
            _set_status(table, mid, "failed")


async def run_inbound_worker(fn: Callable[[dict], Awaitable[None]]) -> None:
    await run_worker(INBOUND, fn)


async def run_outbound_worker(fn: Callable[[dict], Awaitable[None]]) -> None:
    await run_worker(OUTBOUND, fn)
