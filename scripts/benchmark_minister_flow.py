#!/usr/bin/env python3
"""Benchmark for minister flow materialization (Phase 2 of serving build).

Measures per-substep timing, throughput, and output fingerprint for
reproducibility.  Uses the real serving database.

Usage::

    uv run python scripts/benchmark_minister_flow.py
    uv run python scripts/benchmark_minister_flow.py --tasks 5000
    uv run python scripts/benchmark_minister_flow.py --output data/benchmarks/minister_flow.json

The output JSON captures all context needed for honest comparison:
commit, timestamp, machine, workers, durations, row counts, fingerprint.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))


def _git_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            text=True,
        ).strip()
    except Exception:
        return "unknown"


def _rss_mb() -> float:
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
    except OSError:
        pass
    return 0.0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tasks", type=int, default=0, help="Limit tasks (0=all)")
    parser.add_argument("--db", type=str, default="data/serving/atlas_stf.db")
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"ERROR: DB not found: {db_path}", file=sys.stderr)
        return 1

    from sqlalchemy import create_engine, select
    from sqlalchemy.orm import Session

    from atlas_stf.core.constants import QueryFilters
    from atlas_stf.serving._builder_flow import (
        FLOW_SHAPES,
        _build_case_index,
        _build_hist_cache,
        _CaseRow,
        _compute_flow,
        _minister_flow_key,
    )
    from atlas_stf.serving.models import ServingAlert, ServingCase

    max_workers = int(os.environ.get("ATLAS_FLOW_WORKERS", "1"))
    commit = _git_commit()
    run_start = datetime.now(timezone.utc)

    # --- Load ---
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    t0 = time.monotonic()
    rss0 = _rss_mb()
    with Session(engine) as session:
        orm_cases = list(session.scalars(select(ServingCase)).all())
        all_cases = [
            _CaseRow(
                decision_event_id=c.decision_event_id or "",
                process_id=c.process_id or "",
                decision_date=c.decision_date,
                period=c.period,
                current_rapporteur=c.current_rapporteur,
                current_rapporteur_lower=(c.current_rapporteur or "").lower(),
                judging_body=c.judging_body,
                process_class=c.process_class,
                is_collegiate=c.is_collegiate,
                decision_type=c.decision_type,
                decision_progress=c.decision_progress,
                thematic_key=c.thematic_key,
            )
            for c in orm_cases
        ]
        del orm_cases
        alert_ids = frozenset(
            eid
            for eid in session.scalars(
                select(ServingAlert.decision_event_id),
            )
            if eid
        )
    t_load = time.monotonic() - t0

    # --- Index ---
    t0 = time.monotonic()
    case_index = _build_case_index(all_cases)
    t_index = time.monotonic() - t0

    # --- Enumerate ---
    periods = sorted({c.period for c in all_cases if c.period}, reverse=True)
    del all_cases
    periods_all: list[str | None] = [None, *periods]
    tasks: list[tuple[str, QueryFilters]] = []
    seen: set[str] = set()

    t0 = time.monotonic()
    for period in periods_all:
        for collegiate in ("all", "colegiado", "monocratico"):
            col_cases = case_index.get((period, collegiate), [])
            for shape in FLOW_SHAPES:
                if not shape:
                    f = QueryFilters(period=period, collegiate=collegiate)
                    k = _minister_flow_key(f)
                    if k not in seen:
                        seen.add(k)
                        tasks.append((k, f))
                    continue
                field_getters = {
                    "minister": lambda c: c.current_rapporteur,
                    "judging_body": lambda c: c.judging_body,
                    "process_class": lambda c: c.process_class,
                }
                getters = [(fld, field_getters[fld]) for fld in shape]
                combos: set[tuple[str | None, ...]] = set()
                for case in col_cases:
                    vals = tuple(g(case) for _, g in getters)
                    if all(v is not None for v in vals):
                        combos.add(vals)
                for combo in sorted(combos):
                    values = dict(zip(shape, combo, strict=True))
                    f = QueryFilters(
                        minister=values.get("minister"),
                        period=period,
                        collegiate=collegiate,
                        judging_body=values.get("judging_body"),
                        process_class=values.get("process_class"),
                    )
                    k = _minister_flow_key(f)
                    if k not in seen:
                        seen.add(k)
                        tasks.append((k, f))
    t_enum = time.monotonic() - t0

    # --- Hist cache ---
    t0 = time.monotonic()
    hist_cache = _build_hist_cache(case_index, tasks)
    t_hist = time.monotonic() - t0

    # --- Compute ---
    task_limit = args.tasks if args.tasks > 0 else len(tasks)
    compute_tasks = tasks[:task_limit]

    t0 = time.monotonic()
    fingerprint_hash = hashlib.sha256()
    total_events = 0
    for i, (key, filters) in enumerate(compute_tasks):
        payload = _compute_flow(case_index, alert_ids, filters, hist_cache)
        ec = payload.get("event_count", 0)
        total_events += ec
        fingerprint_hash.update(f"{key}:{ec}:{payload.get('historical_event_count', 0)}".encode())
    t_compute = time.monotonic() - t0
    rss1 = _rss_mb()
    fingerprint = fingerprint_hash.hexdigest()[:16]

    rate = len(compute_tasks) / t_compute if t_compute > 0 else 0
    ms_per_flow = (t_compute / len(compute_tasks) * 1000) if compute_tasks else 0

    result = {
        "benchmark": "minister_flow_phase2",
        "commit": commit,
        "timestamp": run_start.isoformat(),
        "machine": platform.node(),
        "python": platform.python_version(),
        "os": f"{platform.system()} {platform.release()}",
        "cpu_count": os.cpu_count(),
        "workers": max_workers,
        "db_path": str(db_path),
        "scenario": "full" if args.tasks == 0 else f"subset_{args.tasks}",
        "total_tasks_available": len(tasks),
        "tasks_computed": len(compute_tasks),
        "hist_cache_entries": len(hist_cache),
        "durations_seconds": {
            "load": round(t_load, 2),
            "index": round(t_index, 2),
            "enumerate": round(t_enum, 2),
            "hist_cache": round(t_hist, 2),
            "compute": round(t_compute, 2),
            "total": round(t_load + t_index + t_enum + t_hist + t_compute, 2),
        },
        "throughput": {
            "flows_per_second": round(rate, 1),
            "ms_per_flow": round(ms_per_flow, 2),
        },
        "memory_mb": {
            "rss_start": round(rss0, 1),
            "rss_end": round(rss1, 1),
        },
        "output_equivalence": {
            "fingerprint": fingerprint,
            "total_events_sum": total_events,
            "method": "sha256(key:event_count:hist_count per flow)[:16]",
        },
        "notes": {
            "baseline_comparison": (
                "Baseline from data/serving_build.log: 13253.4s for 162262 flows "
                "(serial, includes ORM insert overhead). This benchmark measures "
                "compute only (no ORM insert). Direct comparison requires same scope."
            ),
            "cold_warm": "Cold start (fresh DB read). No prior cache.",
        },
    }

    # --- Output ---
    print(json.dumps(result, indent=2))

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2)
        print(f"\nReport written to {args.output}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
