"""Build decision velocity analytics: detect anomalous time-to-decision patterns."""

from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.identity import stable_id
from ..core.rules import derive_thematic_key
from ..schema_validate import validate_records
from ._atomic_io import AtomicJsonlWriter
from ._match_io import read_jsonl

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path("schemas/decision_velocity.schema.json")
SUMMARY_SCHEMA_PATH = Path("schemas/decision_velocity_summary.schema.json")
DEFAULT_PROCESS_PATH = Path("data/curated/process.jsonl")
DEFAULT_DECISION_EVENT_PATH = Path("data/curated/decision_event.jsonl")
DEFAULT_SESSION_EVENT_PATH = Path("data/curated/session_event.jsonl")
DEFAULT_OUTPUT_DIR = Path("data/analytics")
MIN_GROUP_SIZE = 10
VELOCITY_FLAG_PERCENTILE_LOW = 5
VELOCITY_FLAG_PERCENTILE_HIGH = 95


def _percentile(sorted_values: list[float], p: float) -> float:
    """Compute percentile from a pre-sorted list using linear interpolation."""
    if not sorted_values:
        return 0.0
    n = len(sorted_values)
    k = (n - 1) * p / 100.0
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_values[int(k)]
    return sorted_values[f] * (c - k) + sorted_values[c] * (k - f)


def _load_process_dates(process_path: Path) -> dict[str, dict[str, Any]]:
    """Load process_id -> {filing_date, first_distribution_date, process_class, thematic_key}."""
    result: dict[str, dict[str, Any]] = {}
    for record in read_jsonl(process_path):
        pid = record.get("process_id")
        if not pid:
            continue
        filing_date = record.get("filing_date")
        if not filing_date:
            continue
        pc = record.get("process_class")
        subjects = record.get("subjects_normalized")
        branch = record.get("branch_of_law")
        tk = derive_thematic_key(
            subjects if isinstance(subjects, list) else None,
            branch,
            fallback="INCERTO",
        )
        result[str(pid)] = {
            "filing_date": str(filing_date),
            "first_distribution_date": record.get("first_distribution_date"),
            "process_class": str(pc) if pc else "",
            "thematic_key": tk,
        }
    return result


def _load_vista_days(session_event_path: Path) -> dict[str, int]:
    """Load total vista duration days per process_id from session events."""
    result: dict[str, int] = defaultdict(int)
    for record in read_jsonl(session_event_path):
        pid = record.get("process_id")
        if not pid or record.get("event_type") != "pedido_de_vista":
            continue
        vd = record.get("vista_duration_days")
        if vd is not None and isinstance(vd, (int, float)):
            result[str(pid)] += int(vd)
    return dict(result)


def _parse_date(date_str: str | None) -> datetime | None:
    if not date_str or not isinstance(date_str, str):
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except ValueError, IndexError:
        return None


def build_decision_velocity(
    *,
    process_path: Path = DEFAULT_PROCESS_PATH,
    decision_event_path: Path = DEFAULT_DECISION_EVENT_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    min_group_size: int = MIN_GROUP_SIZE,
    movement_path: Path | None = None,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Build decision velocity analytics: flag queue-jump and stalled processes.

    When *movement_path* is provided and the file exists, uses
    ``first_distribution_date`` instead of ``filing_date`` for the
    ``days_to_decision`` calculation (falling back to ``filing_date``
    when the distribution date is absent).  Vista days are also deducted
    for fairer comparison.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    if on_progress:
        on_progress(0, 4, "Velocity: Carregando dados...")

    process_ctx = _load_process_dates(process_path)
    events = read_jsonl(decision_event_path)

    # When movement_path is provided and exists, enrich with distribution dates
    # and load vista deductions from session events.
    use_movements = movement_path is not None and movement_path.exists()
    vista_days_map: dict[str, int] = {}
    if use_movements and movement_path is not None:
        session_event_path = movement_path.parent / "session_event.jsonl"
        if session_event_path.exists():
            vista_days_map = _load_vista_days(session_event_path)

    # Compute days_to_decision per decision event
    if on_progress:
        on_progress(1, 4, "Velocity: Calculando velocidade...")

    event_velocities: list[dict[str, Any]] = []
    for event in events:
        pid = str(event.get("process_id") or "")
        ctx = process_ctx.get(pid)
        if not ctx:
            continue

        # Choose start date: prefer first_distribution_date when movements available
        start_date_str = ctx["filing_date"]
        if use_movements:
            fdd = ctx.get("first_distribution_date")
            if fdd:
                start_date_str = str(fdd)

        start_date = _parse_date(start_date_str)
        decision_date = _parse_date(event.get("decision_date"))
        if not start_date or not decision_date:
            continue
        days = (decision_date - start_date).days
        if days < 0:
            continue

        # Deduct vista days when movements are available
        days_in_vista = vista_days_map.get(pid, 0) if use_movements else 0
        adjusted_days = max(days - days_in_vista, 0)

        decision_year = event.get("decision_year")
        if not decision_year:
            continue
        ev_record: dict[str, Any] = {
            "decision_event_id": str(event.get("decision_event_id") or ""),
            "process_id": pid,
            "current_rapporteur": event.get("current_rapporteur"),
            "decision_date": str(event.get("decision_date") or ""),
            "filing_date": ctx["filing_date"],
            "days_to_decision": adjusted_days if use_movements else days,
            "process_class": ctx["process_class"],
            "thematic_key": ctx["thematic_key"],
            "decision_year": int(decision_year),
        }
        if use_movements:
            ev_record["days_in_vista_deducted"] = days_in_vista
        event_velocities.append(ev_record)

    # Group by (process_class, thematic_key, decision_year)
    groups: dict[tuple[str, str, int], list[dict[str, Any]]] = defaultdict(list)
    for ev in event_velocities:
        key = (ev["process_class"], ev["thematic_key"], ev["decision_year"])
        groups[key].append(ev)

    if on_progress:
        on_progress(2, 4, "Velocity: Detectando anomalias...")

    now_iso = datetime.now(timezone.utc).isoformat()
    records: list[dict[str, Any]] = []

    for (pc, tk, year), group_events in groups.items():
        if len(group_events) < min_group_size:
            continue

        days_values = sorted(ev["days_to_decision"] for ev in group_events)
        p5 = _percentile(days_values, VELOCITY_FLAG_PERCENTILE_LOW)
        p10 = _percentile(days_values, 10)
        median = _percentile(days_values, 50)
        p90 = _percentile(days_values, 90)
        p95 = _percentile(days_values, VELOCITY_FLAG_PERCENTILE_HIGH)

        group_stats = {
            "process_class": pc,
            "thematic_key": tk,
            "decision_year": year,
            "group_size": len(group_events),
            "p5_days": round(p5, 1),
            "p10_days": round(p10, 1),
            "median_days": round(median, 1),
            "p90_days": round(p90, 1),
            "p95_days": round(p95, 1),
        }

        for ev in group_events:
            days = ev["days_to_decision"]
            velocity_flag: str | None = None
            if days < p5:
                velocity_flag = "queue_jump"
            elif days > p95:
                velocity_flag = "stalled"

            velocity_id = stable_id(
                "vel-",
                f"{ev['decision_event_id']}:{pc}:{tk}:{year}",
            )
            out_record: dict[str, Any] = {
                "velocity_id": velocity_id,
                "decision_event_id": ev["decision_event_id"],
                "process_id": ev["process_id"],
                "current_rapporteur": ev["current_rapporteur"],
                "decision_date": ev["decision_date"],
                "filing_date": ev["filing_date"],
                "days_to_decision": days,
                "process_class": pc,
                "thematic_key": tk,
                "decision_year": year,
                "group_size": group_stats["group_size"],
                "p5_days": group_stats["p5_days"],
                "p10_days": group_stats["p10_days"],
                "median_days": group_stats["median_days"],
                "p90_days": group_stats["p90_days"],
                "p95_days": group_stats["p95_days"],
                "velocity_flag": velocity_flag,
                "velocity_z_score": (round((days - median) / max(p95 - p5, 1), 4)),
                "generated_at": now_iso,
            }
            if "days_in_vista_deducted" in ev:
                out_record["days_in_vista_deducted"] = ev["days_in_vista_deducted"]
            records.append(out_record)

    if on_progress:
        on_progress(3, 4, "Velocity: Gravando resultados...")

    validate_records(records, SCHEMA_PATH)

    output_path = output_dir / "decision_velocity.jsonl"
    with AtomicJsonlWriter(output_path) as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    flagged = [r for r in records if r["velocity_flag"]]
    queue_jumps = sum(1 for r in flagged if r["velocity_flag"] == "queue_jump")
    stalled = sum(1 for r in flagged if r["velocity_flag"] == "stalled")

    summary = {
        "generated_at": now_iso,
        "total_records": len(records),
        "flagged_count": len(flagged),
        "queue_jump_count": queue_jumps,
        "stalled_count": stalled,
        "min_group_size": min_group_size,
        "groups_analyzed": len([g for g in groups.values() if len(g) >= min_group_size]),
    }
    validate_records([summary], SUMMARY_SCHEMA_PATH)
    summary_path = output_dir / "decision_velocity_summary.json"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    logger.info(
        "Built decision velocity: %d records (%d queue_jump, %d stalled)",
        len(records),
        queue_jumps,
        stalled,
    )
    if on_progress:
        on_progress(4, 4, "Velocity: Concluído")
    return output_path
