"""Pure I/O helpers for reading JSONL files (no analytics dependencies)."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

__all__ = ["iter_jsonl", "read_jsonl"]


def iter_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    """Yield JSONL records one at a time (never loads full file)."""
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    return list(iter_jsonl(path))
