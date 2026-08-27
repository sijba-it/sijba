"""Key-value context store backed by SQLite.

Namespaced: each plugin/handler uses its own namespace.
Plugins never call this directly — the core injects context into the envelope
before invoke and saves context_update from the response.
"""
from __future__ import annotations
import json
from typing import Any, Dict

import core.db as db


def init() -> None:
    db.execute("""
        CREATE TABLE IF NOT EXISTS context_store (
            namespace   TEXT NOT NULL,
            key         TEXT NOT NULL,
            value_json  TEXT NOT NULL,
            updated_at  TEXT NOT NULL,
            PRIMARY KEY (namespace, key)
        )
    """)


def get(namespace: str, key: str) -> Any:
    row = db.query_one(
        "SELECT value_json FROM context_store WHERE namespace=? AND key=?",
        (namespace, key),
    )
    return json.loads(row[0]) if row else None


def set(namespace: str, key: str, value: Any) -> None:
    db.execute(
        "INSERT INTO context_store (namespace, key, value_json, updated_at) VALUES (?,?,?,?)"
        " ON CONFLICT(namespace, key) DO UPDATE SET"
        "  value_json=excluded.value_json, updated_at=excluded.updated_at",
        (namespace, key, json.dumps(value), db.now()),
    )


def delete(namespace: str, key: str) -> None:
    db.execute(
        "DELETE FROM context_store WHERE namespace=? AND key=?", (namespace, key)
    )


def list_ns(namespace: str) -> Dict[str, Any]:
    rows = db.query(
        "SELECT key, value_json, updated_at FROM context_store WHERE namespace=?",
        (namespace,),
    )
    return {r[0]: {"value": json.loads(r[1]), "updated_at": r[2]} for r in rows}


def clear_ns(namespace: str) -> int:
    return db.execute(
        "DELETE FROM context_store WHERE namespace=?", (namespace,)
    ).rowcount
