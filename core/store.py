"""Key-value context store backed by SQLite.

Namespaced: each plugin/handler uses its own namespace.
Plugins never call this directly — the core injects context into the envelope
before invoke and saves context_update from the response.
"""
from __future__ import annotations
import json
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import core.log as log

_logger = log.get("store")
_conn: Optional[sqlite3.Connection] = None
_lock = threading.Lock()


def init(db_path: str) -> None:
    global _conn
    _conn = sqlite3.connect(db_path, check_same_thread=False)
    _conn.execute("PRAGMA journal_mode=WAL")
    _conn.execute("""
        CREATE TABLE IF NOT EXISTS context_store (
            namespace   TEXT NOT NULL,
            key         TEXT NOT NULL,
            value_json  TEXT NOT NULL,
            updated_at  TEXT NOT NULL,
            PRIMARY KEY (namespace, key)
        )
    """)
    _conn.commit()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def get(namespace: str, key: str) -> Any:
    with _lock:
        row = _conn.execute(
            "SELECT value_json FROM context_store WHERE namespace=? AND key=?",
            (namespace, key),
        ).fetchone()
    return json.loads(row[0]) if row else None


def set(namespace: str, key: str, value: Any) -> None:
    with _lock:
        _conn.execute(
            "INSERT INTO context_store (namespace, key, value_json, updated_at) VALUES (?,?,?,?)"
            " ON CONFLICT(namespace, key) DO UPDATE SET"
            "  value_json=excluded.value_json, updated_at=excluded.updated_at",
            (namespace, key, json.dumps(value), _now()),
        )
        _conn.commit()


def delete(namespace: str, key: str) -> None:
    with _lock:
        _conn.execute(
            "DELETE FROM context_store WHERE namespace=? AND key=?", (namespace, key)
        )
        _conn.commit()


def list_ns(namespace: str) -> Dict[str, Any]:
    with _lock:
        rows = _conn.execute(
            "SELECT key, value_json, updated_at FROM context_store WHERE namespace=?",
            (namespace,),
        ).fetchall()
    return {r[0]: {"value": json.loads(r[1]), "updated_at": r[2]} for r in rows}


def clear_ns(namespace: str) -> int:
    with _lock:
        cur = _conn.execute(
            "DELETE FROM context_store WHERE namespace=?", (namespace,)
        )
        _conn.commit()
    return cur.rowcount
