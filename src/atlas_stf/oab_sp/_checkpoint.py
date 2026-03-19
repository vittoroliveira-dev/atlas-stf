"""Auditable checkpoint for OAB/SP society fetch.

States:
- completed: detail fetched and saved (terminal)
- not_found: official search returned no results (terminal)
- exhausted: failed after max_retries (terminal)
- failed: transient error, retryable (non-terminal)
"""

from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class OabSpCheckpoint:
    """Checkpoint tracking four states for registration lookups."""

    def __init__(self) -> None:
        self._completed: set[str] = set()
        self._not_found: set[str] = set()
        self._exhausted: set[str] = set()
        self._failed: set[str] = set()
        self._retry_counts: dict[str, int] = {}
        self._lock = threading.Lock()

    def is_resolved(self, reg: str) -> bool:
        """True if in any terminal state."""
        return reg in self._completed or reg in self._not_found or reg in self._exhausted

    def is_retryable(self, reg: str, max_retries: int) -> bool:
        """True if failed and has retries remaining."""
        return reg in self._failed and self._retry_counts.get(reg, 0) < max_retries

    def retry_count(self, key: str) -> int:
        """Return the number of retries recorded for *key* (0 if none)."""
        return self._retry_counts.get(key, 0)

    def mark_completed(self, reg: str) -> None:
        with self._lock:
            self._completed.add(reg)
            self._failed.discard(reg)
            self._retry_counts.pop(reg, None)

    def mark_not_found(self, reg: str) -> None:
        with self._lock:
            self._not_found.add(reg)
            self._failed.discard(reg)
            self._retry_counts.pop(reg, None)

    def mark_failed(self, reg: str) -> None:
        with self._lock:
            self._failed.add(reg)
            self._retry_counts[reg] = self._retry_counts.get(reg, 0) + 1

    def promote_exhausted(self, max_retries: int) -> int:
        """Promote failed entries with retries >= max_retries to exhausted. Returns count promoted."""
        with self._lock:
            to_promote = {r for r in self._failed if self._retry_counts.get(r, 0) >= max_retries}
            for r in to_promote:
                self._exhausted.add(r)
                self._failed.discard(r)
            return len(to_promote)

    @property
    def stats(self) -> dict[str, int]:
        return {
            "completed": len(self._completed),
            "not_found": len(self._not_found),
            "exhausted": len(self._exhausted),
            "failed": len(self._failed),
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "completed": sorted(self._completed),
            "not_found": sorted(self._not_found),
            "exhausted": sorted(self._exhausted),
            "failed": sorted(self._failed),
            "retry_counts": dict(sorted(self._retry_counts.items())),
            "stats": self.stats,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> OabSpCheckpoint:
        cp = cls()
        cp._completed = set(data.get("completed", []))
        cp._not_found = set(data.get("not_found", []))
        cp._exhausted = set(data.get("exhausted", []))
        cp._failed = set(data.get("failed", []))
        cp._retry_counts = dict(data.get("retry_counts", {}))
        return cp


def load_checkpoint(path: Path) -> OabSpCheckpoint:
    """Load checkpoint from JSON, or return fresh state."""
    if not path.exists():
        return OabSpCheckpoint()
    with path.open(encoding="utf-8") as f:
        data = json.load(f)
    return OabSpCheckpoint.from_dict(data)


def save_checkpoint(state: OabSpCheckpoint, path: Path) -> Path:
    """Atomically write checkpoint (write .tmp then rename)."""
    payload = state.to_dict()
    tmp_path = path.with_suffix(".json.tmp")
    path.parent.mkdir(parents=True, exist_ok=True)
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")
    tmp_path.replace(path)
    return path
