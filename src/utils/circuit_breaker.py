"""Circuit breaker implementation for external dependencies."""

from __future__ import annotations

import logging
import threading
import time

from shared_constants import CIRCUIT_BREAKER_RESET_SECS, CIRCUIT_BREAKER_THRESHOLD

logger = logging.getLogger("sentinel_utils.circuit_breaker")


class CircuitBreaker:
    """Protect repeated calls to unstable external dependencies (thread-safe)."""

    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"

    def __init__(
        self,
        threshold: int = CIRCUIT_BREAKER_THRESHOLD,
        reset_secs: float = CIRCUIT_BREAKER_RESET_SECS,
        label: str = "",
    ) -> None:
        self.threshold = threshold
        self.reset_secs = reset_secs
        self.label = label
        self._failures = 0
        self._state = self.CLOSED
        self._opened_at = 0.0
        self._lock = threading.Lock()

    @property
    def state(self) -> str:
        with self._lock:
            if self._state == self.OPEN and time.time() - self._opened_at >= self.reset_secs:
                self._state = self.HALF_OPEN
                logger.info("[CB:%s] -> HALF_OPEN - testing recovery", self.label)
            return self._state

    def allow(self) -> bool:
        return self.state in (self.CLOSED, self.HALF_OPEN)

    def record_success(self) -> None:
        with self._lock:
            if self._state != self.CLOSED:
                logger.info("[CB:%s] -> CLOSED - recovered", self.label)
            self._failures = 0
            self._state = self.CLOSED

    def record_failure(self) -> None:
        with self._lock:
            self._failures += 1
            if self._state == self.HALF_OPEN or self._failures >= self.threshold:
                self._state = self.OPEN
                self._opened_at = time.time()
                logger.error(
                    "[CB:%s] -> OPEN after %d failures (resets in %.0fs)",
                    self.label,
                    self._failures,
                    self.reset_secs,
                )
