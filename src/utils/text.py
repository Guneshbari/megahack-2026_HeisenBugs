"""Text normalization helpers."""

from __future__ import annotations

import re


def clean_message(raw: str, max_len: int = 500) -> str:
    """Strip XML or HTML tags, collapse whitespace, and truncate."""
    if not raw:
        return ""
    cleaned = re.sub(r"<[^>]+>", " ", raw)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:max_len]
