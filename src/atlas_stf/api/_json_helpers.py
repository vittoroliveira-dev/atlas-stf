"""Shared JSON deserialization helpers for serving Text columns."""

from __future__ import annotations

import json
from typing import Any


def parse_json_list(raw: str | None) -> list[Any]:
    """Parse a JSON string expected to be a list. Returns [] on failure."""
    if not raw:
        return []
    try:
        result = json.loads(raw)
    except TypeError, json.JSONDecodeError:
        return []
    return result if isinstance(result, list) else []


def parse_json_dict(raw: str | None) -> dict[str, Any]:
    """Parse a JSON string expected to be a dict. Returns {} on failure."""
    if not raw:
        return {}
    try:
        result = json.loads(raw)
    except TypeError, json.JSONDecodeError:
        return {}
    return result if isinstance(result, dict) else {}


def parse_json_dict_or_none(raw: str | None) -> dict[str, Any] | None:
    """Parse a JSON string expected to be a dict. Returns None on failure."""
    if not raw:
        return None
    try:
        result = json.loads(raw)
    except TypeError, json.JSONDecodeError:
        return None
    return result if isinstance(result, dict) else None
