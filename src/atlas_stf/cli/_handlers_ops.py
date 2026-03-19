"""Dispatch handlers for operational commands (runs, status, explain-run, tail-run, resume)."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _validate_run_id(run_id: str, runs_root: Path) -> bool:
    """Validate that run_id is a single flat token confined under runs_root."""
    if not run_id or len(Path(run_id).parts) != 1:
        return False
    try:
        resolved = (runs_root / run_id).resolve()
    except OSError, ValueError:
        return False
    return resolved.is_relative_to(runs_root.resolve())


def dispatch_ops(parser: argparse.ArgumentParser, args: argparse.Namespace) -> int | None:
    if args.command == "runs":
        return _handle_runs(args)
    if args.command == "status":
        return _handle_status(args)
    if args.command == "explain-run":
        return _handle_explain_run(args)
    if args.command == "tail-run":
        return _handle_tail_run(args)
    if args.command == "resume":
        return _handle_resume(parser, args)
    return None


def _handle_runs(args: argparse.Namespace) -> int:
    entries = _read_index(args.analytics_dir)
    if args.builder:
        entries = [e for e in entries if e.get("builder") == args.builder]
    entries.sort(key=lambda e: str(e.get("started_at", "")), reverse=True)
    entries = entries[: args.limit]

    header = f"{'RUN ID':<48}  {'STATE':<10}  {'STEP':<8}  {'PROGRESS':<10}  {'RSS PEAK'}"
    print(header)
    print("-" * len(header))

    for entry in entries:
        run_id = str(entry.get("run_id", ""))
        runs_root = args.analytics_dir / ".runs"
        status = _read_json_safe(runs_root / run_id / "status.json")
        manifest = _read_json_safe(runs_root / run_id / "manifest.json")

        if manifest:
            state = str(manifest.get("state", "FINISHED"))
            rss_peak = _format_bytes_gb(float(manifest.get("rss_peak_mb", 0.0)))
            step_durations: Any = manifest.get("step_durations", {})
            total_steps = len(step_durations) if isinstance(step_durations, dict) else 0
            step_str = f"{total_steps}/{total_steps}"
            progress_str = "100.0%"
        elif status:
            state = str(status.get("state", "UNKNOWN"))
            rss_peak = _format_bytes_gb(float(status.get("rss_peak_mb", 0.0)))
            sp: Any = status.get("step_progress")
            if sp and isinstance(sp, dict):
                step = int(sp.get("step", 0))
                total_steps_status: Any = status.get("total_steps")
                total_str = str(total_steps_status) if total_steps_status else "?"
                step_str = f"{step}/{total_str}"
                items_done = int(sp.get("items_done", 0))
                items_total: Any = sp.get("items_total")
                if items_total:
                    pct = items_done * 100.0 / int(items_total)
                    progress_str = f"{pct:.1f}%"
                else:
                    progress_str = "-"
            else:
                step_str = "-"
                progress_str = "-"
        else:
            state = str(entry.get("state", "UNKNOWN"))
            rss_peak = "-"
            step_str = "-"
            progress_str = "-"

        print(f"{run_id:<48}  {state:<10}  {step_str:<8}  {progress_str:<10}  {rss_peak}")

    return 0


def _handle_status(args: argparse.Namespace) -> int:
    entries = _read_index(args.analytics_dir)
    if args.builder:
        entries = [e for e in entries if e.get("builder") == args.builder]

    runs_root = args.analytics_dir / ".runs"
    active: list[dict[str, Any]] = []
    finished: list[dict[str, Any]] = []

    for entry in entries:
        run_id = str(entry.get("run_id", ""))
        status = _read_json_safe(runs_root / run_id / "status.json")
        manifest = _read_json_safe(runs_root / run_id / "manifest.json")

        if manifest:
            finished.append({"entry": entry, "manifest": manifest})
            continue

        pid_str = run_id.rsplit("-", 1)[-1]
        try:
            pid = int(pid_str)
            alive = _is_pid_alive(pid)
        except ValueError:
            alive = False

        if alive and status:
            active.append({"entry": entry, "status": status})

    if active:
        print("ACTIVE RUNS:")
        for item in active:
            status = item["status"]
            run_id = str(status.get("run_id", ""))
            sp: Any = status.get("step_progress")
            rss_peak = _format_bytes_gb(float(status.get("rss_peak_mb", 0.0)))
            last_heartbeat = str(status.get("last_heartbeat", ""))

            step_str = "-"
            progress_str = "-"
            if sp and isinstance(sp, dict):
                step = int(sp.get("step", 0))
                step_str = f"step {step}/??"
                items_done = int(sp.get("items_done", 0))
                items_total: Any = sp.get("items_total")
                if items_total:
                    pct = items_done * 100.0 / int(items_total)
                    progress_str = f"{pct:.1f}%"
                last_adv = str(sp.get("last_advance_at", ""))
                age_str = _elapsed_since(last_adv)
                print(f"  {run_id}  {step_str}  {progress_str}  {rss_peak}  last advance {age_str}")
            else:
                age_str = _elapsed_since(last_heartbeat)
                print(f"  {run_id}  {step_str}  {progress_str}  {rss_peak}  last heartbeat {age_str}")
    else:
        print("ACTIVE RUNS: (none)")

    print()

    if finished:
        finished.sort(key=lambda x: str((x.get("manifest") or {}).get("finished_at", "")), reverse=True)
        last = finished[0]
        manifest = last["manifest"]
        run_id = str(manifest.get("run_id", ""))
        state = str(manifest.get("state", "FINISHED"))
        wall_s = float(manifest.get("total_wall_time_s", 0.0))
        rss_peak = _format_bytes_gb(float(manifest.get("rss_peak_mb", 0.0)))
        print(f"LAST FINISHED:\n  {run_id}  {state}  {_format_duration(wall_s)}  {rss_peak} peak")
    else:
        print("LAST FINISHED: (none)")

    return 0


def _handle_explain_run(args: argparse.Namespace) -> int:
    run_id: str = args.run_id
    runs_root = args.analytics_dir / ".runs"
    if not _validate_run_id(run_id, runs_root):
        print(f"Invalid run ID: {run_id}", file=sys.stderr)
        return 1
    run_dir = runs_root / run_id

    manifest = _read_json_safe(run_dir / "manifest.json")
    status = _read_json_safe(run_dir / "status.json")

    if not manifest and not status:
        print(f"No data found for run: {run_id}", file=sys.stderr)
        return 1

    if manifest:
        state = str(manifest.get("state", "?"))
        builder = str(manifest.get("builder_name", "?"))
        started = str(manifest.get("started_at", "?"))
        finished = str(manifest.get("finished_at", "?"))
        wall_s = float(manifest.get("total_wall_time_s", 0.0))
        session_count = int(manifest.get("session_count", 1))
        resume_count = int(manifest.get("resume_count", 0)) if "resume_count" in manifest else 0
        rss_peak = _format_bytes_gb(float(manifest.get("rss_peak_mb", 0.0)))
        total_items = int(manifest.get("total_items_processed", 0))
        step_durations: Any = manifest.get("step_durations", {})
        outputs: Any = manifest.get("outputs", [])
        memory_marks: Any = manifest.get("memory_marks", [])

        print(f"Run ID:          {run_id}")
        print(f"Builder:         {builder}")
        print(f"State:           {state}")
        print(f"Started:         {started}")
        print(f"Finished:        {finished}")
        print(f"Wall time:       {_format_duration(wall_s)}")
        print(f"Sessions:        {session_count}  (resumes: {resume_count})")
        print(f"RSS peak:        {rss_peak}")
        print(f"Items processed: {total_items:,}")

        if isinstance(step_durations, dict) and step_durations:
            print("\nStep durations:")
            for step_key, dur in sorted(step_durations.items(), key=lambda x: int(x[0]) if str(x[0]).isdigit() else 0):
                print(f"  step {step_key}: {_format_duration(float(dur))}")

        if isinstance(outputs, list) and outputs:
            print("\nOutputs:")
            for out in outputs:
                print(f"  {out}")

        if isinstance(memory_marks, list) and memory_marks:
            print("\nMemory marks:")
            for mark in memory_marks:
                if isinstance(mark, dict):
                    label = mark.get("label", "?")
                    rss = _format_bytes_gb(float(mark.get("rss_mb", 0.0)))
                    count: Any = mark.get("structure_count")
                    count_str = f"  ({count:,} structs)" if count else ""
                    print(f"  {label}: {rss}{count_str}")

    elif status:
        print(f"Run ID:  {run_id}")
        print(f"State:   {status.get('state', '?')}  (no manifest — run may be in progress or aborted)")
        print(f"Builder: {status.get('builder', '?')}")
        print(f"Started: {status.get('started_at', '?')}")
        rss = _format_bytes_gb(float(status.get("rss_mb", 0.0)))
        rss_peak = _format_bytes_gb(float(status.get("rss_peak_mb", 0.0)))
        print(f"RSS now: {rss}  peak: {rss_peak}")

    return 0


def _handle_tail_run(args: argparse.Namespace) -> int:
    run_id: str = args.run_id
    runs_root = args.analytics_dir / ".runs"
    if not _validate_run_id(run_id, runs_root):
        print(f"Invalid run ID: {run_id}", file=sys.stderr)
        return 1
    events_path = runs_root / run_id / "events.jsonl"
    if not events_path.exists():
        print(f"events.jsonl not found for run: {run_id}", file=sys.stderr)
        return 1
    try:
        result = subprocess.run(["tail", "-f", str(events_path)])
    except KeyboardInterrupt:
        return 0
    return result.returncode


def _handle_resume(parser: argparse.ArgumentParser, args: argparse.Namespace) -> int:
    run_id: str | None = getattr(args, "run_id", None)
    builder: str | None = getattr(args, "builder", None)

    if not run_id and not builder:
        parser.error("Specify --run-id or --builder")

    if not run_id and builder:
        index_path = args.analytics_dir / ".runs" / "_index.jsonl"
        if index_path.exists():
            candidates: list[tuple[str, str]] = []
            try:
                with open(index_path, encoding="utf-8") as fh:
                    for line in fh:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entry = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if entry.get("builder") == builder:
                            candidates.append((str(entry.get("run_id", "")), str(entry.get("started_at", ""))))
            except OSError:
                pass
            candidates.sort(key=lambda x: x[1], reverse=True)
            runs_root = args.analytics_dir / ".runs"
            for cand_id, _ in candidates:
                if (runs_root / cand_id / "manifest.json").exists():
                    continue
                pid_str = cand_id.rsplit("-", 1)[-1]
                try:
                    pid = int(pid_str)
                    if _is_pid_alive(pid):
                        continue
                except ValueError:
                    pass
                run_id = cand_id
                break

    if not run_id:
        print(f"No resumable run found for builder: {builder}", file=sys.stderr)
        return 1

    print(f"Resume not yet implemented for builder dispatch — use `make resume RUN={run_id}`")
    return 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _format_duration(seconds: float) -> str:
    """Format seconds as human-readable duration (e.g., '2h50m', '37m', '12s')."""
    total = int(seconds)
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}m"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def _format_bytes_gb(mb: float) -> str:
    """Format MB as GB with 1 decimal."""
    if mb == 0.0:
        return "-"
    return f"{mb / 1024:.1f} GB"


def _is_pid_alive(pid: int) -> bool:
    """Check if PID is alive via os.kill(pid, 0)."""
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _read_json_safe(path: Path) -> dict[str, Any] | None:
    """Read JSON file, return None on error."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            result: dict[str, Any] = raw
            return result
    except OSError, json.JSONDecodeError:
        pass
    return None


def _read_index(analytics_dir: Path) -> list[dict[str, Any]]:
    """Read _index.jsonl from .runs/ directory."""
    index_path = analytics_dir / ".runs" / "_index.jsonl"
    if not index_path.exists():
        return []
    entries: list[dict[str, Any]] = []
    try:
        with open(index_path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    if isinstance(entry, dict):
                        entries.append(entry)
                except json.JSONDecodeError:
                    continue
    except OSError:
        pass
    return entries


def _elapsed_since(iso_ts: str) -> str:
    """Return human-readable elapsed time since an ISO timestamp."""
    if not iso_ts:
        return "?"
    try:
        then = datetime.fromisoformat(iso_ts)
        now = datetime.now(timezone.utc)
        delta_s = (now - then).total_seconds()
        if delta_s < 0:
            return "just now"
        return _format_duration(delta_s) + " ago"
    except ValueError:
        return "?"
