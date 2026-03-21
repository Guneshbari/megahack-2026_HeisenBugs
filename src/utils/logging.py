"""Structured logging helpers."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger("sentinel_utils.logging")


def structured_log(
    component: str,
    extra: Optional[Dict[str, Any]] = None,
    log: Optional[logging.Logger] = None,
) -> None:
    """Emit a single JSON log line with a stable envelope."""
    record: Dict[str, Any] = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "component": component,
    }
    if extra:
        record.update(extra)
    target = log if log is not None else logger
    target.info("[METRICS] %s", json.dumps(record))
