"""Timeout helpers for bounded blocking work."""

from __future__ import annotations

import logging
import threading
from typing import Any, Callable, List, Optional, Tuple

from shared_constants import DB_QUERY_TIMEOUT_SECONDS

logger = logging.getLogger("sentinel_utils.timeout")


def timeout_wrapper(
    fn: Callable,
    *args: Any,
    timeout_secs: float = float(DB_QUERY_TIMEOUT_SECONDS),
    label: str = "op",
    **kwargs: Any,
) -> Tuple[Any, bool]:
    """
    Run ``fn`` in a daemon thread.

    Returns ``(result, True)`` if the call finishes before ``timeout_secs``,
    or ``(None, False)`` on timeout.
    """
    result_box: List[Any] = [None]
    exception_box: List[Optional[Exception]] = [None]

    def run_target() -> None:
        try:
            result_box[0] = fn(*args, **kwargs)
        except Exception as exc:
            exception_box[0] = exc

    worker_thread = threading.Thread(target=run_target, daemon=True)
    worker_thread.start()
    worker_thread.join(timeout=timeout_secs)

    if worker_thread.is_alive():
        logger.error(
            "[timeout_wrapper] %s exceeded %.1fs - returning fallback",
            label,
            timeout_secs,
        )
        return None, False

    if exception_box[0] is not None:
        raise exception_box[0]

    return result_box[0], True
