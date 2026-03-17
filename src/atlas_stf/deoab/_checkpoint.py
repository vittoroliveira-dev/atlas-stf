"""Auditable checkpoint for DEOAB gazette extraction.

Each date has a status with metadata for reprocessing decisions:
- missing: no PDF available at that URL
- downloaded: PDF downloaded but not yet parsed
- parsed: PDF parsed with a specific parser_version
- failed: download or parse failed (retryable)

Reprocessing triggers:
- content_length changed (PDF was republished)
- parser_version changed (regex patterns updated)
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class DateEntry:
    """State for a single DEOAB date."""

    status: str  # missing | downloaded | parsed | failed
    content_length: int = 0
    parser_version: int = 0
    source_url: str = ""
    error: str = ""
    updated_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "content_length": self.content_length,
            "parser_version": self.parser_version,
            "source_url": self.source_url,
            "error": self.error,
            "updated_at": self.updated_at,
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> DateEntry:
        return DateEntry(
            status=data.get("status", "failed"),
            content_length=data.get("content_length", 0),
            parser_version=data.get("parser_version", 0),
            source_url=data.get("source_url", ""),
            error=data.get("error", ""),
            updated_at=data.get("updated_at", ""),
        )


@dataclass
class DeoabCheckpoint:
    """Auditable checkpoint with per-date state."""

    _dates: dict[str, DateEntry] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)

    def get(self, date_str: str) -> DateEntry | None:
        return self._dates.get(date_str)

    def set(self, date_str: str, entry: DateEntry) -> None:
        with self._lock:
            entry.updated_at = datetime.now(timezone.utc).isoformat()
            self._dates[date_str] = entry

    def needs_download(self, date_str: str, remote_content_length: int) -> bool:
        """Check if date needs (re)download."""
        entry = self._dates.get(date_str)
        if entry is None:
            return True
        if entry.status in ("missing", "failed"):
            return True
        if entry.content_length != remote_content_length:
            return True  # PDF was republished
        return False

    def needs_parse(self, date_str: str, current_parser_version: int) -> bool:
        """Check if date needs (re)parsing."""
        entry = self._dates.get(date_str)
        if entry is None:
            return True
        if entry.status != "parsed":
            return True
        if entry.parser_version < current_parser_version:
            return True
        return False

    @property
    def stats(self) -> dict[str, int]:
        counts: dict[str, int] = {"missing": 0, "downloaded": 0, "parsed": 0, "failed": 0}
        for entry in self._dates.values():
            counts[entry.status] = counts.get(entry.status, 0) + 1
        return counts


def load_checkpoint(path: Path) -> DeoabCheckpoint:
    """Load checkpoint from JSON file, or return fresh state."""
    if not path.exists():
        return DeoabCheckpoint()
    with path.open(encoding="utf-8") as f:
        data = json.load(f)
    dates = {k: DateEntry.from_dict(v) for k, v in data.get("dates", {}).items()}
    return DeoabCheckpoint(_dates=dates)


def save_checkpoint(state: DeoabCheckpoint, path: Path) -> Path:
    """Atomically write checkpoint (write .tmp then rename)."""
    payload = {
        "dates": {k: v.to_dict() for k, v in sorted(state._dates.items())},
        "stats": state.stats,
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }
    tmp_path = path.with_suffix(".json.tmp")
    path.parent.mkdir(parents=True, exist_ok=True)
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")
    tmp_path.replace(path)
    return path
