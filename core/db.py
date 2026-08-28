"""The single SQLite connection shared by queue, store, and transcript.

All three live in the same database file, so they share one connection and
one lock rather than opening three that then contend with each other.
"""
from __future__ import annotations
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator, Optional, Sequence

_conn: Optional[sqlite3.Connection] = None
_lock = threading.Lock()


def connect(db_path: str) -> sqlite3.Connection:
    """Open (or reopen) the shared connection. Call once at startup."""
    global _conn
    _conn = sqlite3.connect(db_path, check_same_thread=False)
    _conn.execute("PRAGMA journal_mode=WAL")
    return _conn


def is_connected() -> bool:
    """Whether connect() has run. Persistence is optional for callers that
    also work without a DB (registry, exercised directly in tests)."""
    return _conn is not None


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def write() -> Iterator[sqlite3.Connection]:
    """Exclusive access, committed on clean exit. Use for multi-statement
    writes that must land together (e.g. insert + prune)."""
    with _lock:
        yield _conn
        _conn.commit()


@contextmanager
def read() -> Iterator[sqlite3.Connection]:
    with _lock:
        yield _conn


def execute(sql: str, params: Sequence[Any] = ()) -> sqlite3.Cursor:
    with write() as conn:
        return conn.execute(sql, params)


def query(sql: str, params: Sequence[Any] = ()) -> list:
    with read() as conn:
        return conn.execute(sql, params).fetchall()


def query_one(sql: str, params: Sequence[Any] = ()) -> Optional[tuple]:
    with read() as conn:
        return conn.execute(sql, params).fetchone()
