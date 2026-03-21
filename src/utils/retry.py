"""Retry helpers for transient failures."""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Tuple

from shared_constants import RETRY_BACKOFF_SECONDS, RETRY_MAX_ATTEMPTS

logger = logging.getLogger("sentinel_utils.retry")


def retry_with_backoff(
    fn: Callable,
    *args: Any,
    max_attempts: int = RETRY_MAX_ATTEMPTS,
    backoff_base: float = RETRY_BACKOFF_SECONDS,
    label: str = "op",
    **kwargs: Any,
) -> Tuple[Any, bool]:
    """
    Execute ``fn(*args, **kwargs)`` with exponential backoff.

    Returns ``(result, True)`` on success and ``(None, False)`` when retries
    are exhausted. Exceptions are never propagated to the caller.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            return fn(*args, **kwargs), True
        except Exception as exc:
            wait_seconds = backoff_base * (2 ** (attempt - 1))
            if attempt < max_attempts:
                logger.warning(
                    "[retry_with_backoff] %s attempt %d/%d: %s - retry in %.1fs",
                    label,
                    attempt,
                    max_attempts,
                    exc,
                    wait_seconds,
                )
                time.sleep(wait_seconds)
            else:
                logger.error(
                    "[retry_with_backoff] %s exhausted %d attempts: %s",
                    label,
                    max_attempts,
                    exc,
                )
    return None, False
