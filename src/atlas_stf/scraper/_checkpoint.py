"""Atomic checkpoint persistence for incremental scraping."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from ._config import CheckpointState


def load_checkpoint(output_dir: Path) -> CheckpointState | None:
    """Load checkpoint from ``_checkpoint.json``, or None if absent."""
    path = output_dir / "_checkpoint.json"
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return CheckpointState(**data)


def save_checkpoint(state: CheckpointState, output_dir: Path) -> Path:
    """Atomically write checkpoint (write .tmp then rename)."""
    from dataclasses import replace

    snapshot = replace(state, last_updated=datetime.now(timezone.utc).isoformat())
    path = output_dir / "_checkpoint.json"
    tmp_path = output_dir / "_checkpoint.json.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(asdict(snapshot), f, ensure_ascii=False, indent=2)
        f.write("\n")
    tmp_path.rename(path)
    state.last_updated = snapshot.last_updated
    return path


def mark_partition_complete(state: CheckpointState, partition_label: str) -> None:
    """Mark a partition as completed, resetting per-partition state."""
    if partition_label not in state.completed_partitions:
        state.completed_partitions.append(partition_label)
    state.search_after = None
    state.partition_doc_count = 0
