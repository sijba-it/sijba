"""Logging system: Python stdlib logger + circular buffer + SSE broadcast."""
from __future__ import annotations
import logging
from datetime import datetime, timezone

import core.state as state

logger = logging.getLogger("sijba")


class _SseBroadcastHandler(logging.Handler):
    """Appends to the circular buffer and fans out to SSE clients."""

    def emit(self, record: logging.LogRecord) -> None:
        state.broadcast_log({
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": self.format(record),
        })


def setup(level: str = "INFO") -> None:
    """Call once at startup. Sets log level and wires handlers."""
    lvl = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(lvl)
    logger.propagate = False

    # stderr
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)-8s %(name)s: %(message)s"))
    sh.setLevel(lvl)
    logger.addHandler(sh)

    # SSE broadcast
    bh = _SseBroadcastHandler()
    bh.setFormatter(logging.Formatter("%(message)s"))
    bh.setLevel(lvl)
    logger.addHandler(bh)


def get(name: str) -> logging.Logger:
    return logging.getLogger(f"sijba.{name}")
