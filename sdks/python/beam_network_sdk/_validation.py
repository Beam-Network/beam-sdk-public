"""Shared input validation helpers."""

from __future__ import annotations

import re

_ID_PATTERN = re.compile(r"[a-zA-Z0-9_\-]+")


def validate_id(value: str, name: str) -> None:
    """Raise ValueError if *value* contains unsafe characters for URL path interpolation."""
    if not _ID_PATTERN.fullmatch(value):
        raise ValueError(f"{name} contains invalid characters: {value!r}")
