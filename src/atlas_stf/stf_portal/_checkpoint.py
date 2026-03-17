"""Checkpoint persistence for incremental STF portal extraction."""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path


@dataclass
class PortalCheckpoint:
    """Tracks extraction progress per process.

    Uses sets internally for O(1) lookup; serializes as sorted lists for
    deterministic JSON output and backward compatibility.

    The ``_incidente_cache`` maps process_number → incidente ID, saving
    one HTTP request per retry (Phase 3 optimization).
    """

    _completed: set[str] = field(default_factory=set)
    _failed: set[str] = field(default_factory=set)
    _incidente_cache: dict[str, str] = field(default_factory=dict)
    total_fetched: int = 0
    last_updated: str = ""
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)

    # --- Public API (thread-safe) ---

    def is_completed(self, process_number: str) -> bool:
        return process_number in self._completed

    def is_failed(self, process_number: str) -> bool:
        return process_number in self._failed

    def mark_completed(self, process_number: str) -> None:
        with self._lock:
            if process_number not in self._completed:
                self._completed.add(process_number)
                self.total_fetched += 1

    def mark_failed(self, process_number: str) -> None:
        with self._lock:
            self._failed.add(process_number)

    def clear_failed(self) -> None:
        """Clear all failed entries so they can be re-tried."""
        with self._lock:
            self._failed.clear()

    # --- Incidente cache (Phase 3) ---

    def get_incidente(self, process_number: str) -> str | None:
        """Return cached incidente ID or None."""
        return self._incidente_cache.get(process_number)

    def set_incidente(self, process_number: str, incidente: str) -> None:
        """Cache incidente in memory (disk write follows batch cycle)."""
        with self._lock:
            self._incidente_cache[process_number] = incidente

    # --- Backward-compatible properties for serialization ---

    @property
    def completed_processes(self) -> list[str]:
        """Return completed as sorted list (for JSON compat and tests)."""
        return sorted(self._completed)

    @property
    def failed_processes(self) -> list[str]:
        """Return failed as sorted list (for JSON compat and tests)."""
        return sorted(self._failed)


def load_checkpoint(checkpoint_path: Path) -> PortalCheckpoint:
    """Load checkpoint from JSON file, or return fresh state.

    Accepts both old format (lists, no incidente_map) and new format.
    """
    if not checkpoint_path.exists():
        return PortalCheckpoint()
    with checkpoint_path.open(encoding="utf-8") as f:
        data = json.load(f)
    return PortalCheckpoint(
        _completed=set(data.get("completed_processes", [])),
        _failed=set(data.get("failed_processes", [])),
        _incidente_cache=dict(data.get("incidente_map", {})),
        total_fetched=data.get("total_fetched", 0),
        last_updated=data.get("last_updated", ""),
    )


def save_checkpoint(state: PortalCheckpoint, checkpoint_path: Path) -> Path:
    """Atomically write checkpoint (write .tmp then rename)."""
    payload = {
        "completed_processes": state.completed_processes,
        "failed_processes": state.failed_processes,
        "incidente_map": state._incidente_cache,
        "total_fetched": state.total_fetched,
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }
    tmp_path = checkpoint_path.with_suffix(".json.tmp")
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")
    tmp_path.replace(checkpoint_path)
    return checkpoint_path
