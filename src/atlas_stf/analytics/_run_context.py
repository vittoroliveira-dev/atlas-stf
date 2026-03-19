"""Observable execution runtime for analytics builders."""

from __future__ import annotations

import fcntl
import json
import logging
import os
import resource
import threading
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

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
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)
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


class RunContext:
    """Observable execution runtime for analytics builders."""

    def __init__(
        self,
        builder_name: str,
        artifact_dir: Path,
        total_steps: int,
        *,
        on_progress: Callable[[int, int, str], None] | None = None,
        memory_budget_pct: float = 0.70,
        memory_budget_mb: int | None = None,
    ) -> None:
        self._builder_name = builder_name
        self._artifact_dir = artifact_dir
        self._total_steps = total_steps
        self._on_progress = on_progress

        self._runs_root = artifact_dir / ".runs"
        ts = datetime.now(timezone.utc)
        self._run_id = f"{builder_name}-{ts:%Y%m%dT%H%M%S}-{os.getpid()}"
        self._run_dir = self._runs_root / self._run_id
        self._run_dir.mkdir(parents=True, exist_ok=True)

        self._state = "RUNNING"
        self._started_at = _now_iso()
        self._monotonic_start = time.monotonic()

        self._step_progress: StepProgress | None = None
        self._step_start_time: float | None = None
        self._rss_peak_mb: float = 0.0
        self._memory_marks: list[dict[str, Any]] = []
        self._step_durations: dict[int, float] = {}
        self._total_items_processed: int = 0
        self._last_snapshot_time: float = 0.0
        self._resume_count: int = 0
        self._resumed_at: str | None = None
        self._session_count: int = 1

        mem_available = _read_mem_available_mb()
        if memory_budget_mb is not None:
            self._memory_budget_mb: float = float(memory_budget_mb)
        elif mem_available is not None:
            self._memory_budget_mb = mem_available * memory_budget_pct
        else:
            self._memory_budget_mb = 16384.0

        self._finished = threading.Event()
        self._lock = threading.Lock()

        self._append_index(
            {"run_id": self._run_id, "builder": builder_name, "started_at": self._started_at, "state": "RUNNING"}
        )
        self._write_snapshot()
        self._emit_event(
            {"event": "run_started", "run_id": self._run_id, "builder": builder_name, "timestamp": _now_iso()}
        )

        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True, name=f"hb-{self._run_id}")
        self._heartbeat_thread.start()

    def __del__(self) -> None:
        self._finished.set()

    @property
    def run_id(self) -> str:
        return self._run_id

    def start_step(self, step: int, desc: str, *, total_items: int | None = None, unit: str = "registros") -> None:
        now_mono = time.monotonic()
        with self._lock:
            if self._step_progress is not None and self._step_start_time is not None:
                self._step_durations[self._step_progress.step] = now_mono - self._step_start_time
            self._step_start_time = now_mono
            self._step_progress = StepProgress(
                step=step,
                desc=desc,
                items_done=0,
                items_total=total_items,
                unit=unit,
                current_item=None,
                throughput=None,
                started_at=_now_iso(),
                last_advance_at=_now_iso(),
            )

        if self._on_progress is not None:
            self._on_progress(step, self._total_steps, desc)

        self._write_snapshot()
        self._emit_event({"event": "step_started", "step": step, "desc": desc, "timestamp": _now_iso()})

    def advance(self, n: int = 1, *, current_item: str | None = None) -> None:
        now_mono = time.monotonic()
        with self._lock:
            sp = self._step_progress
            if sp is None:
                return

            prev_done = sp.items_done
            sp.items_done += n
            if current_item is not None:
                sp.current_item = current_item
            sp.last_advance_at = _now_iso()
            self._total_items_processed += n

            elapsed = now_mono - (self._step_start_time or now_mono)
            sp.throughput = sp.items_done / elapsed if elapsed > 0 else None

            rss = _read_rss_mb()
            if rss > self._rss_peak_mb:
                self._rss_peak_mb = rss

            should_snap = False
            time_elapsed = now_mono - self._last_snapshot_time
            if time_elapsed >= _SNAPSHOT_MIN_INTERVAL_S:
                should_snap = True
            elif sp.items_total and sp.items_total > 0:
                prev_pct = int(prev_done * 100 / sp.items_total) // 5
                new_pct = int(sp.items_done * 100 / sp.items_total) // 5
                if new_pct > prev_pct:
                    should_snap = True

            over_budget = rss > self._memory_budget_mb

        if should_snap:
            self._write_snapshot()

        if over_budget:
            self._emit_event(
                {
                    "event": "memory_budget_exceeded",
                    "rss_mb": rss,
                    "budget_mb": self._memory_budget_mb,
                    "timestamp": _now_iso(),
                }
            )
            raise MemoryError(f"RSS {rss:.1f} MB excedeu orçamento de memória {self._memory_budget_mb:.1f} MB")

    def pulse(self, label: str) -> None:
        with self._lock:
            if self._step_progress is not None:
                self._step_progress.last_advance_at = _now_iso()
        self._emit_event({"event": "pulse", "label": label, "timestamp": _now_iso()})

    def log_memory(self, label: str, structure_count: int | None = None) -> None:
        rss = _read_rss_mb()
        with self._lock:
            if rss > self._rss_peak_mb:
                self._rss_peak_mb = rss
            self._memory_marks.append({"label": label, "rss_mb": rss, "structure_count": structure_count})
        self._emit_event(
            {
                "event": "memory_mark",
                "label": label,
                "rss_mb": rss,
                "structure_count": structure_count,
                "timestamp": _now_iso(),
            }
        )

    def save_checkpoint(self, phase: str, **fields: Any) -> None:
        with self._lock:
            current_step = self._step_progress.step if self._step_progress is not None else None

        checkpoint: dict[str, Any] = {
            "schema_version": _CHECKPOINT_SCHEMA_VERSION,
            "builder_name": self._builder_name,
            "run_id": self._run_id,
            "phase": phase,
            "step": current_step,
            "created_at": _now_iso(),
            **fields,
        }

        _atomic_write_json(self._run_dir / "latest_checkpoint.json", checkpoint)

        cp_line = json.dumps(checkpoint, ensure_ascii=False) + "\n"
        with open(self._run_dir / "checkpoints.jsonl", "a", encoding="utf-8") as fh:
            fh.write(cp_line)

        self._emit_event({"event": "checkpoint_saved", "phase": phase, "timestamp": _now_iso()})

    def load_checkpoint(self) -> dict[str, Any] | None:
        path = self._run_dir / "latest_checkpoint.json"
        if not path.exists():
            return None
        try:
            data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
        except OSError, json.JSONDecodeError:
            return None
        if data.get("schema_version") != _CHECKPOINT_SCHEMA_VERSION:
            return None
        return data

    @classmethod
    def find_resumable_run(cls, builder_name: str, artifact_dir: Path) -> str | None:
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

    @classmethod
    def resume(
        cls,
        run_id: str,
        artifact_dir: Path,
        total_steps: int,
        *,
        on_progress: Callable[[int, int, str], None] | None = None,
        memory_budget_pct: float = 0.70,
        memory_budget_mb: int | None = None,
    ) -> RunContext:
        instance = object.__new__(cls)

        instance._builder_name = run_id.rsplit("-", 2)[0]
        instance._artifact_dir = artifact_dir
        instance._total_steps = total_steps
        instance._on_progress = on_progress
        instance._runs_root = artifact_dir / ".runs"
        instance._run_id = run_id
        instance._run_dir = instance._runs_root / run_id

        instance._state = "RUNNING"
        instance._monotonic_start = time.monotonic()
        instance._started_at = _now_iso()
        instance._resumed_at = _now_iso()
        instance._resume_count = 1
        instance._session_count = 2

        instance._step_progress = None
        instance._step_start_time = None
        instance._rss_peak_mb = 0.0
        instance._memory_marks = []
        instance._step_durations = {}
        instance._total_items_processed = 0
        instance._last_snapshot_time = 0.0

        mem_available = _read_mem_available_mb()
        if memory_budget_mb is not None:
            instance._memory_budget_mb = float(memory_budget_mb)
        elif mem_available is not None:
            instance._memory_budget_mb = mem_available * memory_budget_pct
        else:
            instance._memory_budget_mb = 16384.0

        instance._finished = threading.Event()
        instance._lock = threading.Lock()

        instance._emit_event({"event": "run_resumed", "resumed_at": instance._resumed_at, "timestamp": _now_iso()})
        instance._write_snapshot()

        instance._heartbeat_thread = threading.Thread(target=instance._heartbeat_loop, daemon=True, name=f"hb-{run_id}")
        instance._heartbeat_thread.start()

        return instance

    def finish(self, outputs: list[str] | None = None) -> RunManifest:
        now_mono = time.monotonic()
        with self._lock:
            if self._step_progress is not None and self._step_start_time is not None:
                self._step_durations[self._step_progress.step] = now_mono - self._step_start_time

        self._state = "FINISHED"
        self._finished.set()

        finished_at = _now_iso()
        wall_time = now_mono - self._monotonic_start

        with self._lock:
            manifest = RunManifest(
                run_id=self._run_id,
                builder_name=self._builder_name,
                state="FINISHED",
                started_at=self._started_at,
                finished_at=finished_at,
                total_wall_time_s=wall_time,
                session_count=self._session_count,
                rss_peak_mb=self._rss_peak_mb,
                step_durations=dict(self._step_durations),
                total_items_processed=self._total_items_processed,
                outputs=list(outputs or []),
                memory_marks=list(self._memory_marks),
            )

        _atomic_write_json(self._run_dir / "manifest.json", asdict(manifest))
        self._write_snapshot()
        self._update_index_state("FINISHED")
        self._emit_event({"event": "run_finished", "timestamp": finished_at, "manifest": asdict(manifest)})

        return manifest

    def _write_snapshot(self) -> None:
        now_mono = time.monotonic()
        now_iso = _now_iso()
        rss = _read_rss_mb()

        with self._lock:
            if rss > self._rss_peak_mb:
                self._rss_peak_mb = rss
            sp = self._step_progress
            sp_dict: dict[str, Any] | None = None
            stall_warning = False
            eta_s: float | None = None

            if sp is not None:
                sp_dict = asdict(sp)
                try:
                    last_adv = datetime.fromisoformat(sp.last_advance_at)
                    last_adv_mono = now_mono - (datetime.now(timezone.utc) - last_adv).total_seconds()
                    stall_warning = (now_mono - last_adv_mono) > _STALL_THRESHOLD_S
                except ValueError:
                    stall_warning = False

                if sp.items_total and sp.throughput and sp.throughput > 0:
                    remaining = sp.items_total - sp.items_done
                    eta_s = remaining / sp.throughput

            snapshot: dict[str, Any] = {
                "run_id": self._run_id,
                "builder": self._builder_name,
                "state": self._state,
                "started_at": self._started_at,
                "resumed_at": self._resumed_at,
                "resume_count": self._resume_count,
                "pid": os.getpid(),
                "step_progress": sp_dict,
                "rss_mb": rss,
                "rss_peak_mb": self._rss_peak_mb,
                "memory_marks": list(self._memory_marks),
                "memory_budget_mb": self._memory_budget_mb,
                "stall_warning": stall_warning,
                "elapsed_s": now_mono - self._monotonic_start,
                "eta_s": eta_s,
                "last_heartbeat": now_iso,
            }

        _atomic_write_json(self._run_dir / "status.json", snapshot)
        self._last_snapshot_time = now_mono

    def _heartbeat_loop(self) -> None:
        while not self._finished.is_set():
            try:
                self._write_snapshot()
            except Exception:  # noqa: BLE001 — daemon thread must not crash
                pass
            self._finished.wait(timeout=_HEARTBEAT_INTERVAL_S)

    def _emit_event(self, event: dict[str, Any]) -> None:
        try:
            line = json.dumps(event, ensure_ascii=False) + "\n"
            with open(self._run_dir / "events.jsonl", "a", encoding="utf-8") as fh:
                fh.write(line)
        except OSError:
            pass

    def _append_index(self, entry: dict[str, Any]) -> None:
        index_path = self._runs_root / "_index.jsonl"
        index_path.parent.mkdir(parents=True, exist_ok=True)
        with open(index_path, "a", encoding="utf-8") as fh:
            fcntl.flock(fh, fcntl.LOCK_EX)
            try:
                fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
            finally:
                fcntl.flock(fh, fcntl.LOCK_UN)

    def _update_index_state(self, new_state: str) -> None:
        index_path = self._runs_root / "_index.jsonl"
        if not index_path.exists():
            return
        with open(index_path, "r+", encoding="utf-8") as fh:
            fcntl.flock(fh, fcntl.LOCK_EX)
            try:
                lines = fh.readlines()
                updated: list[str] = []
                for line in lines:
                    stripped = line.strip()
                    if not stripped:
                        updated.append(line)
                        continue
                    try:
                        entry: dict[str, Any] = json.loads(stripped)
                        if entry.get("run_id") == self._run_id:
                            entry["state"] = new_state
                        updated.append(json.dumps(entry, ensure_ascii=False) + "\n")
                    except json.JSONDecodeError:
                        updated.append(line)
                fh.seek(0)
                fh.writelines(updated)
                fh.truncate()
            finally:
                fcntl.flock(fh, fcntl.LOCK_UN)
