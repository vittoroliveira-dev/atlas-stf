"""Checkpoint persistence for incremental STF portal extraction."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path


@dataclass
class PortalCheckpoint:
    """Tracks extraction progress per process."""

    completed_processes: list[str] = field(default_factory=list)
    failed_processes: list[str] = field(default_factory=list)
    total_fetched: int = 0
    last_updated: str = ""

    def is_completed(self, process_number: str) -> bool:
        return process_number in self.completed_processes

    def mark_completed(self, process_number: str) -> None:
        if process_number not in self.completed_processes:
            self.completed_processes.append(process_number)
        self.total_fetched += 1

    def mark_failed(self, process_number: str) -> None:
        if process_number not in self.failed_processes:
            self.failed_processes.append(process_number)


def load_checkpoint(checkpoint_path: Path) -> PortalCheckpoint:
    """Load checkpoint from JSON file, or return fresh state."""
    if not checkpoint_path.exists():
        return PortalCheckpoint()
    with checkpoint_path.open(encoding="utf-8") as f:
        data = json.load(f)
    return PortalCheckpoint(**data)


def save_checkpoint(state: PortalCheckpoint, checkpoint_path: Path) -> Path:
    """Atomically write checkpoint (write .tmp then rename)."""
    state.last_updated = datetime.now(timezone.utc).isoformat()
    tmp_path = checkpoint_path.with_suffix(".json.tmp")
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(asdict(state), f, ensure_ascii=False, indent=2)
        f.write("\n")
    tmp_path.replace(checkpoint_path)
    return checkpoint_path
