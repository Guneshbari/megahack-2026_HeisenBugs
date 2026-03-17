"""
SentinelCore — Shared Reliability Utilities
All modules import from here. One definition, zero duplication.

Public API:
    retry_with_backoff(fn, *args, max_attempts, backoff_base, label, **kwargs)
        -> (result, bool)

    timeout_wrapper(fn, *args, timeout_secs, label, **kwargs)
        -> (result, bool)

    CircuitBreaker(threshold, reset_secs, label)
        .allow() -> bool
        .record_success()
        .record_failure()

    make_db_connection() -> psycopg2 connection   [raises on failure]

    clean_message(raw, max_len) -> str

    structured_log(component, extra, log)
"""

import json
import logging
import re
import time
import threading
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

import psycopg2

from shared_constants import (
    RETRY_MAX_ATTEMPTS,
    RETRY_BACKOFF_SECONDS,
    DB_QUERY_TIMEOUT_SECONDS,
    CIRCUIT_BREAKER_THRESHOLD,
    CIRCUIT_BREAKER_RESET_SECS,
    DB_CONFIG,
)

logger = logging.getLogger("sentinel_utils")


# ============================================================================
# RETRY WITH BACKOFF
# ============================================================================

def retry_with_backoff(
    fn: Callable,
    *args: Any,
    max_attempts: int = RETRY_MAX_ATTEMPTS,
    backoff_base: float = RETRY_BACKOFF_SECONDS,
    label: str = "op",
    **kwargs: Any,
) -> Tuple[Any, bool]:
    """
    Execute fn(*args, **kwargs) with exponential backoff.

    Returns:
        (result, True)  — on success
        (None,  False)  — after all retries exhausted

    Never raises. Caller decides what to do on failure.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            return fn(*args, **kwargs), True
        except Exception as exc:
            wait = backoff_base * (2 ** (attempt - 1))
            if attempt < max_attempts:
                logger.warning(
                    "[retry_with_backoff] %s attempt %d/%d: %s — retry in %.1fs",
                    label, attempt, max_attempts, exc, wait,
                )
                time.sleep(wait)
            else:
                logger.error(
                    "[retry_with_backoff] %s exhausted %d attempts: %s",
                    label, max_attempts, exc,
                )
    return None, False


# ============================================================================
# TIMEOUT WRAPPER
# ============================================================================

def timeout_wrapper(
    fn: Callable,
    *args: Any,
    timeout_secs: float = float(DB_QUERY_TIMEOUT_SECONDS),
    label: str = "op",
    **kwargs: Any,
) -> Tuple[Any, bool]:
    """
    Run fn in a daemon thread; return (result, True) if it finishes in time,
    or (None, False) on timeout. Never hangs the caller.
    """
    result_box: List[Any] = [None]
    exc_box:    List[Optional[Exception]] = [None]

    def _run() -> None:
        try:
            result_box[0] = fn(*args, **kwargs)
        except Exception as exc:
            exc_box[0] = exc

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    thread.join(timeout=timeout_secs)

    if thread.is_alive():
        logger.error(
            "[timeout_wrapper] %s exceeded %.1fs — returning fallback", label, timeout_secs
        )
        return None, False

    if exc_box[0] is not None:
        raise exc_box[0]

    return result_box[0], True


# ============================================================================
# CIRCUIT BREAKER
# ============================================================================

class CircuitBreaker:
    """
    Protects repeated calls to unstable dependencies.

    States:
        CLOSED    — normal operation, calls allowed
        OPEN      — failure threshold hit, calls rejected immediately
        HALF_OPEN — one trial call allowed after reset_secs to test recovery

    Usage:
        cb = CircuitBreaker(label="Kafka")
        if not cb.allow():
            return  # skip or fallback
        try:
            do_risky_thing()
            cb.record_success()
        except Exception:
            cb.record_failure()
    """

    CLOSED    = "CLOSED"
    OPEN      = "OPEN"
    HALF_OPEN = "HALF_OPEN"

    def __init__(
        self,
        threshold: int = CIRCUIT_BREAKER_THRESHOLD,
        reset_secs: float = CIRCUIT_BREAKER_RESET_SECS,
        label: str = "",
    ) -> None:
        self.threshold  = threshold
        self.reset_secs = reset_secs
        self.label      = label
        self._failures  = 0
        self._state     = self.CLOSED
        self._opened_at = 0.0

    @property
    def state(self) -> str:
        if self._state == self.OPEN:
            if time.time() - self._opened_at >= self.reset_secs:
                self._state = self.HALF_OPEN
                logger.info("[CB:%s] → HALF_OPEN — testing recovery", self.label)
        return self._state

    def allow(self) -> bool:
        return self.state in (self.CLOSED, self.HALF_OPEN)

    def record_success(self) -> None:
        if self._state != self.CLOSED:
            logger.info("[CB:%s] → CLOSED — recovered", self.label)
        self._failures = 0
        self._state    = self.CLOSED

    def record_failure(self) -> None:
        self._failures += 1
        if self._state == self.HALF_OPEN or self._failures >= self.threshold:
            self._state     = self.OPEN
            self._opened_at = time.time()
            logger.error(
                "[CB:%s] → OPEN after %d failures (resets in %.0fs)",
                self.label, self._failures, self.reset_secs,
            )


# ============================================================================
# DATABASE CONNECTION FACTORY
# ============================================================================

def make_db_connection() -> psycopg2.extensions.connection:
    """
    Open a fresh psycopg2 connection using DB_CONFIG from shared_constants.
    Raises on failure — wrap with retry_with_backoff at the call site.
    """
    return psycopg2.connect(**DB_CONFIG)


# ============================================================================
# TEXT CLEANING (ML normalisation)
# ============================================================================

def clean_message(raw: str, max_len: int = 500) -> str:
    """
    Strip XML/HTML tags, collapse whitespace, truncate to max_len.
    Lightweight — safe to call per-event in a tight loop.
    Returns '' for empty/None input.
    """
    if not raw:
        return ""
    cleaned = re.sub(r"<[^>]+>", " ", raw)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:max_len]


# ============================================================================
# STRUCTURED LOG EMITTER
# ============================================================================

def structured_log(
    component: str,
    extra: Optional[Dict[str, Any]] = None,
    log: Optional[logging.Logger] = None,
) -> None:
    """
    Emit a single JSON log line. Always includes 'ts' and 'component'.
    Merges any extra fields on top.

    Usage:
        structured_log("collector", {"cycle": 5, "events_sent": 12, "status": "ok"})
    """
    record: Dict[str, Any] = {
        "ts":        datetime.now(timezone.utc).isoformat(),
        "component": component,
    }
    if extra:
        record.update(extra)
    target = log if log is not None else logger
    target.info("[METRICS] %s", json.dumps(record))