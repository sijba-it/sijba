"""Bounded message transcript for cross-device replay.

Stores inbound user messages and outbound replies in SQLite (same DB as
queues/store). Hard-capped per (chat_id, channel) so it cannot grow forever.
"""
from __future__ import annotations
import json
import sqlite3
from typing import Any, Dict, List

import core.db as db

# Hard caps — keep it small and readable
MAX_PER_CHANNEL = 200          # prune oldest beyond this per (chat_id, channel)
MAX_TEXT_CHARS = 8000          # truncate oversized blobs
API_DEFAULT_LIMIT = 100
API_MAX_LIMIT = 200

# Well-known bucket for the cross-source "pipeline activity" view. Every
# depth-0 web/app/webhook message and every top-level reply for those sources
# is mirrored here (in addition to its own chat_id/plugin-channel thread), so
# one query returns the whole flow no matter which chat_id or plugin channel
# it actually threaded under. Backs the "Pipeline" and per-webhook tiles.
PIPELINE_CHAT_ID = "pipeline"
PIPELINE_CHANNEL = "pipeline"


def init() -> None:
    with db.write() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS transcript (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                ts          TEXT NOT NULL,
                chat_id     TEXT NOT NULL,
                channel     TEXT NOT NULL,
                role        TEXT NOT NULL,
                text        TEXT NOT NULL,
                handler_id  TEXT,
                envelope_id TEXT,
                source      TEXT
            )
        """)
        try:
            conn.execute("ALTER TABLE transcript ADD COLUMN attachments TEXT")
        except sqlite3.OperationalError:
            pass  # already migrated
        for col, typedef in (
            ("delivery_status", "TEXT"),
            ("delivery_error", "TEXT"),
        ):
            try:
                conn.execute(f"ALTER TABLE transcript ADD COLUMN {col} {typedef}")
            except sqlite3.OperationalError:
                pass  # already migrated
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_transcript_chat_ch"
            " ON transcript (chat_id, channel, id)"
        )


def _trim(text: str) -> str:
    if text is None:
        return ""
    s = str(text)
    if len(s) <= MAX_TEXT_CHARS:
        return s
    return s[: MAX_TEXT_CHARS - 20] + "\n…[truncated]"


def _prune(conn, chat_id: str, channel: str) -> None:
    """Keep only the newest MAX_PER_CHANNEL rows for this chat/channel."""
    conn.execute(
        """
        DELETE FROM transcript
        WHERE chat_id=? AND channel=?
          AND id NOT IN (
            SELECT id FROM transcript
            WHERE chat_id=? AND channel=?
            ORDER BY id DESC
            LIMIT ?
          )
        """,
        (chat_id, channel, chat_id, channel, MAX_PER_CHANNEL),
    )


def append(
    *,
    chat_id: str,
    channel: str,
    role: str,
    text: str,
    handler_id: str | None = None,
    envelope_id: str | None = None,
    source: str | None = None,
    attachments: list | None = None,
) -> None:
    if not chat_id or not (text or attachments):
        return
    channel = channel or "pipeline"
    role = role if role in ("user", "bot", "sys") else "sys"
    with db.write() as conn:
        conn.execute(
            "INSERT INTO transcript"
            " (ts, chat_id, channel, role, text, handler_id, envelope_id, source, attachments)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            (
                db.now(),
                str(chat_id),
                str(channel),
                role,
                _trim(text),
                handler_id,
                envelope_id,
                source,
                json.dumps(attachments) if attachments else None,
            ),
        )
        _prune(conn, str(chat_id), str(channel))


def mark_delivery_failed(envelope_id: str, error: str) -> dict | None:
    """Flag the bot reply tied to an inbound envelope as undelivered.

    Returns {channel, chat_id} when a row was updated, else None."""
    if not envelope_id:
        return None
    row = db.query_one(
        "SELECT id, channel, chat_id FROM transcript"
        " WHERE envelope_id = ? AND role = 'bot'"
        " ORDER BY id DESC LIMIT 1",
        (str(envelope_id),),
    )
    if not row:
        return None
    tid, channel, chat_id = row
    db.execute(
        "UPDATE transcript SET delivery_status = 'failed', delivery_error = ?"
        " WHERE id = ?",
        (error, tid),
    )
    return {"channel": channel, "chat_id": chat_id}


def list_messages(
    chat_id: str,
    channel: str = "pipeline",
    limit: int = API_DEFAULT_LIMIT,
) -> List[Dict[str, Any]]:
    limit = max(1, min(int(limit), API_MAX_LIMIT))
    channel = channel or "pipeline"
    rows = db.query(
        """
        SELECT ts, role, text, handler_id, envelope_id, source, attachments,
               delivery_status, delivery_error
        FROM (
          SELECT ts, role, text, handler_id, envelope_id, source, attachments,
                 delivery_status, delivery_error, id
          FROM transcript
          WHERE chat_id=? AND channel=?
          ORDER BY id DESC
          LIMIT ?
        )
        ORDER BY id ASC
        """,
        (str(chat_id), str(channel), limit),
    )
    return [
        {
            "ts": r[0],
            "role": r[1],
            "text": r[2],
            "handler_id": r[3],
            "envelope_id": r[4],
            "source": r[5],
            "attachments": json.loads(r[6]) if r[6] else None,
            "delivery_status": r[7],
            "delivery_error": r[8],
        }
        for r in rows
    ]
