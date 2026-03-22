"""Timeout helpers for bounded blocking work."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from typing import Any, Callable, Tuple

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
    Run ``fn`` with a strict timeout using ThreadPoolExecutor.

    Returns ``(result, True)`` if the call finishes before ``timeout_secs``,
    or ``(None, False)`` on timeout. Will cancel the future if it times out.
    """
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(fn, *args, **kwargs)
        try:
            result = future.result(timeout=timeout_secs)
            return result, True
        except TimeoutError:
            logger.error(
                "[timeout_wrapper] %s exceeded %.1fs - returning fallback",
                label,
                timeout_secs,
            )
            future.cancel()
            return None, False
        except Exception as exc:
            raise exc
