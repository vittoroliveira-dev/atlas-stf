"""I/O helpers, constants and data structures for the run context."""

from __future__ import annotations

import json
import os
import resource
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_HEARTBEAT_INTERVAL_S = 30.0
_STALL_THRESHOLD_S = 300.0
_SNAPSHOT_MIN_INTERVAL_S = 30.0
_CHECKPOINT_SCHEMA_VERSION = 1


def _read_rss_mb() -> float:
    try:
        with open("/proc/self/status") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
    except OSError:
        pass
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def _read_mem_available_mb() -> float | None:
    try:
        with open("/proc/meminfo") as fh:
            for line in fh:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) / 1024
    except OSError:
        pass
    return None


def _atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
        try:
            os.write(fd, json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"))
            os.close(fd)
            os.replace(tmp_name, path)
        except BaseException:
            os.close(fd)
            os.unlink(tmp_name)
            raise
    except OSError:
        pass


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class StepProgress:
    step: int
    desc: str
    items_done: int
    items_total: int | None
    unit: str
    current_item: str | None
    throughput: float | None
    started_at: str
    last_advance_at: str


@dataclass
class RunManifest:
    run_id: str
    builder_name: str
    state: str
    started_at: str
    finished_at: str
    total_wall_time_s: float
    session_count: int
    rss_peak_mb: float
    step_durations: dict[int, float]
    total_items_processed: int
    outputs: list[str]
    memory_marks: list[dict[str, Any]]


def find_resumable_run(builder_name: str, artifact_dir: Path) -> str | None:
    index_path = artifact_dir / ".runs" / "_index.jsonl"
    if not index_path.exists():
        return None

    candidates: list[tuple[str, str]] = []
    try:
        with open(index_path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry: dict[str, Any] = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if entry.get("builder") != builder_name:
                    continue
                run_id = str(entry.get("run_id", ""))
                started_at = str(entry.get("started_at", ""))
                candidates.append((run_id, started_at))
    except OSError:
        return None

    candidates.sort(key=lambda x: x[1], reverse=True)

    runs_root = artifact_dir / ".runs"
    for run_id, _ in candidates:
        run_dir = runs_root / run_id
        if (run_dir / "manifest.json").exists():
            continue
        pid_str = run_id.rsplit("-", 1)[-1]
        try:
            pid = int(pid_str)
            os.kill(pid, 0)
            continue
        except ValueError, ProcessLookupError:
            pass
        except PermissionError:
            continue
        return run_id

    return None
