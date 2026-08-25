"""Bounded message transcript for cross-device replay.

Stores inbound user messages and outbound replies in SQLite (same DB as
queues/store). Hard-capped per (chat_id, channel) so it cannot grow forever.
"""
from __future__ import annotations
import json
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import core.log as log

_logger = log.get("transcript")
_conn: Optional[sqlite3.Connection] = None
_lock = threading.Lock()

# Hard caps — keep it small and readable
MAX_PER_CHANNEL = 200          # prune oldest beyond this per (chat_id, channel)
MAX_TEXT_CHARS = 8000          # truncate oversized blobs
API_DEFAULT_LIMIT = 100
API_MAX_LIMIT = 200


def init(db_path: str) -> None:
    global _conn
    _conn = sqlite3.connect(db_path, check_same_thread=False)
    _conn.execute("PRAGMA journal_mode=WAL")
    _conn.execute("""
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
        _conn.execute("ALTER TABLE transcript ADD COLUMN attachments TEXT")
    except sqlite3.OperationalError:
        pass  # already migrated
    _conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_transcript_chat_ch"
        " ON transcript (chat_id, channel, id)"
    )
    _conn.commit()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _trim(text: str) -> str:
    if text is None:
        return ""
    s = str(text)
    if len(s) <= MAX_TEXT_CHARS:
        return s
    return s[: MAX_TEXT_CHARS - 20] + "\n…[truncated]"


def _prune(chat_id: str, channel: str) -> None:
    """Keep only the newest MAX_PER_CHANNEL rows for this chat/channel."""
    _conn.execute(
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
    with _lock:
        _conn.execute(
            "INSERT INTO transcript"
            " (ts, chat_id, channel, role, text, handler_id, envelope_id, source, attachments)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            (
                _now(),
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
        _prune(str(chat_id), str(channel))
        _conn.commit()


def list_messages(
    chat_id: str,
    channel: str = "pipeline",
    limit: int = API_DEFAULT_LIMIT,
) -> List[Dict[str, Any]]:
    limit = max(1, min(int(limit), API_MAX_LIMIT))
    channel = channel or "pipeline"
    with _lock:
        rows = _conn.execute(
            """
            SELECT ts, role, text, handler_id, envelope_id, source, attachments
            FROM (
              SELECT ts, role, text, handler_id, envelope_id, source, attachments, id
              FROM transcript
              WHERE chat_id=? AND channel=?
              ORDER BY id DESC
              LIMIT ?
            )
            ORDER BY id ASC
            """,
            (str(chat_id), str(channel), limit),
        ).fetchall()
    return [
        {
            "ts": r[0],
            "role": r[1],
            "text": r[2],
            "handler_id": r[3],
            "envelope_id": r[4],
            "source": r[5],
            "attachments": json.loads(r[6]) if r[6] else None,
        }
        for r in rows
    ]
