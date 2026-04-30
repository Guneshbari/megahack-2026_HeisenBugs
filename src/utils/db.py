"""Database connection helpers."""

from __future__ import annotations

from typing import Any

from shared.db_constants import get_db_config

try:
    import psycopg2 as _psycopg2

    _PSYCOPG2_AVAILABLE = True
except ImportError:
    _psycopg2 = None
    _PSYCOPG2_AVAILABLE = False

_DB_CONFIG = get_db_config()

def make_db_connection() -> Any:
    """
    Open a fresh psycopg2 connection using ``_DB_CONFIG``.

    Raises ImportError when psycopg2 is unavailable and propagates connection
    failures so callers can wrap it with retry logic.
    """
    if not _PSYCOPG2_AVAILABLE:
        raise ImportError(
            "psycopg2 is not installed. make_db_connection() is for server-side "
            "scripts only. Install it with: pip install psycopg2-binary"
        )
    return _psycopg2.connect(**_DB_CONFIG)
