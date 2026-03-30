"""Pure I/O helpers for reading JSONL files (no analytics dependencies)."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

__all__ = ["iter_jsonl", "read_jsonl", "read_summary", "extract_alert_counts"]


def iter_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    """Yield JSONL records one at a time (never loads full file)."""
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    return list(iter_jsonl(path))


def read_summary(path: Path) -> dict[str, Any]:
    """Read a JSON summary file, returning {} if absent or invalid."""
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError, OSError:
        return {}
    return data if isinstance(data, dict) else {}


def extract_alert_counts(summary: dict[str, Any]) -> dict[str, int]:
    """Extract alert counts from an outlier_alert_summary dict.

    Returns a dict with ``total``, ``atypical``, and ``inconclusive``.

    The ``atypical`` count comes from ``alert_type_counts.atipicidade``
    (NOT from ``status_counts`` — that dict maps alert_type to status
    using different keys: "novo" for atypical, "inconclusivo" for
    inconclusive).
    """
    return {
        "total": summary.get("alert_count", 0),
        "atypical": summary.get("alert_type_counts", {}).get("atipicidade", 0),
        "inconclusive": summary.get("status_counts", {}).get("inconclusivo", 0),
    }
