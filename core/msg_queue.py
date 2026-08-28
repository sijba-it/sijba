"""SQLite-backed inbound/outbound queues with asyncio worker loops."""
from __future__ import annotations
import asyncio
import json
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable, Optional

import core.db as db
import core.log as log
import core.state as state
import core.transcript as transcript
from core.envelope import expects_outbound_delivery

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

# Added after the fact — prod databases predate these columns.
_MIGRATIONS = (
    "ALTER TABLE {table} ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE {table} ADD COLUMN next_attempt_at TEXT",
    "ALTER TABLE {table} ADD COLUMN last_error TEXT",
)

# Retry budget for a message the pipeline could not deliver. Same shape as
# registry's _pending_max_redeliveries / _pending_ttl_seconds: bounded
# attempts, bounded lifetime, then give up loudly.
MAX_ATTEMPTS = 5
BACKOFF_SECONDS = (5, 15, 60, 300)
RETRY_TTL_SECONDS = 3600.0


class Retry(Exception):
    """Raised by a worker fn: this message was *not* delivered — try again.

    Deliberately distinct from an unexpected exception (which fails the
    message immediately): a Retry means the failure is expected to be
    temporary — the target handler is restarting, has not re-registered
    yet, or is briefly unreachable.
    """


def init() -> None:
    with db.write() as conn:
        conn.executescript(_SCHEMA.format(table=INBOUND) + _SCHEMA.format(table=OUTBOUND))
        for table in (INBOUND, OUTBOUND):
            for stmt in _MIGRATIONS:
                try:
                    conn.execute(stmt.format(table=table))
                except sqlite3.OperationalError:
                    pass  # already migrated


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
            " WHERE status = 'pending'"
            "   AND (next_attempt_at IS NULL OR next_attempt_at <= ?)"
            " ORDER BY created_at LIMIT 1",
            (db.now(),),
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


def _age_seconds(created_at: Optional[str]) -> float:
    if not created_at:
        return 0.0
    try:
        return (datetime.now(timezone.utc) - datetime.fromisoformat(created_at)).total_seconds()
    except ValueError:
        return 0.0


def _surface_outbound_failure(mid: str, reason: str) -> None:
    """Tell the user a reply never reached its outbound channel."""
    row = db.query_one(
        f"SELECT envelope_json FROM {OUTBOUND} WHERE id = ?", (mid,)
    )
    if not row:
        return
    envelope = json.loads(row[0])
    if not expects_outbound_delivery(envelope):
        return
    parent_id = envelope.get("parent_id")
    if not parent_id:
        return
    ctx = transcript.mark_delivery_failed(parent_id, reason)
    if not ctx:
        return
    state.broadcast_reply({
        "type": "delivery_failed",
        "parent_id": parent_id,
        "delivery_error": reason,
        "channel": ctx["channel"],
        "source": envelope.get("source"),
    })


def _defer(table: str, mid: str, reason: str) -> None:
    """Put an undelivered message back on the queue behind a backoff, or give
    up loudly once its budget is spent.

    Silence is the one outcome that must not happen here: before this, a
    message nobody delivered was marked `done` exactly like one that had
    been."""
    surface_failure = False
    with db.write() as conn:
        row = conn.execute(
            f"SELECT attempts, created_at FROM {table} WHERE id = ?", (mid,)
        ).fetchone()
        attempts = (row[0] if row else 0) + 1
        age = _age_seconds(row[1] if row else None)

        if attempts >= MAX_ATTEMPTS or age > RETRY_TTL_SECONDS:
            conn.execute(
                f"UPDATE {table} SET status = 'failed', attempts = ?,"
                " next_attempt_at = NULL, last_error = ? WHERE id = ?",
                (attempts, reason, mid),
            )
        else:
            delay = BACKOFF_SECONDS[min(attempts, len(BACKOFF_SECONDS)) - 1]
            nxt = (datetime.now(timezone.utc) + timedelta(seconds=delay)).isoformat()
            conn.execute(
                f"UPDATE {table} SET status = 'pending', claimed_at = NULL, attempts = ?,"
                " next_attempt_at = ?, last_error = ? WHERE id = ?",
                (attempts, nxt, reason, mid),
            )

    if attempts >= MAX_ATTEMPTS or age > RETRY_TTL_SECONDS:
        _logger.warning(
            f"{table} undelivered [{mid}] — giving up after {attempts} attempt(s),"
            f" {age:.0f}s old: {reason}"
        )
        if table == OUTBOUND:
            _surface_outbound_failure(mid, reason)
        return

    delay = BACKOFF_SECONDS[min(attempts, len(BACKOFF_SECONDS)) - 1]
    _logger.info(
        f"{table} undelivered [{mid}] — retry in {delay}s (attempt {attempts}): {reason}"
    )


def recover_claimed() -> int:
    """Requeue messages left `claimed` by a process that died mid-flight.

    Startup only, where this is unambiguous: one Core process owns the queue,
    so nothing is in flight yet and every `claimed` row is an orphan of the
    previous incarnation. Without this they sat there forever — `_claim_next`
    only ever looks at `pending`."""
    total = 0
    with db.write() as conn:
        for table in (INBOUND, OUTBOUND):
            cur = conn.execute(
                f"UPDATE {table} SET status = 'pending', claimed_at = NULL,"
                " next_attempt_at = NULL WHERE status = 'claimed'"
            )
            if cur.rowcount:
                _logger.warning(
                    f"{table}: requeued {cur.rowcount} message(s) orphaned by a restart"
                )
                total += cur.rowcount
    return total


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
        except Retry as exc:
            _defer(table, mid, str(exc))
        except Exception as exc:
            _logger.error(f"{table} worker error [{mid}]: {exc}")
            _set_status(table, mid, "failed")


async def run_inbound_worker(fn: Callable[[dict], Awaitable[None]]) -> None:
    await run_worker(INBOUND, fn)


async def run_outbound_worker(fn: Callable[[dict], Awaitable[None]]) -> None:
    await run_worker(OUTBOUND, fn)
