"""Build procedural timeline analytics: precise temporal windows per process from movements."""

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
from ..schema_validate import validate_records
from ._atomic_io import AtomicJsonlWriter
from ._match_io import read_jsonl

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path("schemas/procedural_timeline.schema.json")
SUMMARY_SCHEMA_PATH = Path("schemas/procedural_timeline_summary.schema.json")
DEFAULT_CURATED_DIR = Path("data/curated")
DEFAULT_OUTPUT_DIR = Path("data/analytics")
MIN_PEER_GROUP_SIZE = 5
PERCENTILE_LOW = 5
PERCENTILE_HIGH = 95


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


def _parse_date(date_str: str | None) -> datetime | None:
    if not date_str or not isinstance(date_str, str):
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except ValueError, IndexError:
        return None


def _load_movements_by_process(
    movement_path: Path,
) -> dict[str, list[dict[str, Any]]]:
    """Group movement records by process_id."""
    result: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in read_jsonl(movement_path):
        pid = record.get("process_id")
        if pid:
            result[str(pid)].append(record)
    return dict(result)


def _load_session_events_by_process(
    session_event_path: Path,
) -> dict[str, list[dict[str, Any]]]:
    """Group session_event records by process_id."""
    result: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in read_jsonl(session_event_path):
        pid = record.get("process_id")
        if pid:
            result[str(pid)].append(record)
    return dict(result)


def _load_process_metadata(
    process_path: Path,
) -> dict[str, dict[str, Any]]:
    """Load process_id -> {process_number, process_class, first_distribution_date}."""
    result: dict[str, dict[str, Any]] = {}
    for record in read_jsonl(process_path):
        pid = record.get("process_id")
        if not pid:
            continue
        result[str(pid)] = {
            "process_number": record.get("process_number"),
            "process_class": record.get("process_class"),
            "first_distribution_date": record.get("first_distribution_date"),
            "filing_date": record.get("filing_date"),
        }
    return result


def _load_first_decision_dates(
    decision_event_path: Path,
) -> dict[str, str]:
    """Load process_id -> earliest decision_date."""
    earliest: dict[str, str] = {}
    for record in read_jsonl(decision_event_path):
        pid = record.get("process_id")
        dd = record.get("decision_date")
        if not pid or not dd:
            continue
        pid_str = str(pid)
        dd_str = str(dd)
        if pid_str not in earliest or dd_str < earliest[pid_str]:
            earliest[pid_str] = dd_str
    return earliest


def _find_distribution_date(
    movements: list[dict[str, Any]],
    process_meta: dict[str, Any] | None,
) -> str | None:
    """Find first_distribution_date from process metadata or first distribuicao movement."""
    if process_meta:
        fdd = process_meta.get("first_distribution_date")
        if fdd:
            return str(fdd)
    # Fallback: earliest distribuicao movement
    dist_dates: list[str] = []
    for mov in movements:
        if mov.get("movement_category") == "distribuicao":
            md = mov.get("movement_date")
            if md:
                dist_dates.append(str(md))
    if dist_dates:
        return min(dist_dates)
    # Last fallback: filing_date from process metadata
    if process_meta:
        fd = process_meta.get("filing_date")
        if fd:
            return str(fd)
    return None


def _is_redistribution(movement: dict[str, Any]) -> bool:
    """Check if a deslocamento movement is a redistribution."""
    if movement.get("movement_category") != "deslocamento":
        return False
    desc = str(movement.get("movement_raw_description") or "").lower()
    return "redistribu" in desc or "redireciona" in desc


def build_procedural_timeline(
    *,
    curated_dir: Path = DEFAULT_CURATED_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    min_peer_group_size: int = MIN_PEER_GROUP_SIZE,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Build procedural timeline analytics for processes with movement data."""
    output_dir.mkdir(parents=True, exist_ok=True)

    movement_path = curated_dir / "movement.jsonl"
    session_event_path = curated_dir / "session_event.jsonl"
    process_path = curated_dir / "process.jsonl"
    decision_event_path = curated_dir / "decision_event.jsonl"

    if on_progress:
        on_progress(0, 5, "Timeline: Carregando dados...")

    if not movement_path.exists():
        logger.warning("Procedural timeline skipped: movement.jsonl not found")
        output_path = output_dir / "procedural_timeline.jsonl"
        output_path.write_text("", encoding="utf-8")
        return output_path

    movements_by_process = _load_movements_by_process(movement_path)
    session_events_by_process: dict[str, list[dict[str, Any]]] = {}
    if session_event_path.exists():
        session_events_by_process = _load_session_events_by_process(session_event_path)
    process_meta = _load_process_metadata(process_path)
    first_decision_dates = _load_first_decision_dates(decision_event_path)

    if on_progress:
        on_progress(1, 5, "Timeline: Calculando métricas temporais...")

    # Build per-process timeline records
    raw_records: list[dict[str, Any]] = []
    for pid, movements in movements_by_process.items():
        meta = process_meta.get(pid)
        session_events = session_events_by_process.get(pid, [])

        # Distribution date
        dist_date_str = _find_distribution_date(movements, meta)

        # First decision date
        first_dec_str = first_decision_dates.get(pid)

        # Days distribution to first decision
        days_to_decision: int | None = None
        dist_dt = _parse_date(dist_date_str)
        dec_dt = _parse_date(first_dec_str)
        if dist_dt and dec_dt:
            delta = (dec_dt - dist_dt).days
            if delta >= 0:
                days_to_decision = delta

        # Vista metrics from session events
        vista_count = 0
        vista_total_days = 0
        for se in session_events:
            if se.get("event_type") == "pedido_de_vista":
                vista_count += 1
                vd = se.get("vista_duration_days")
                if vd is not None and isinstance(vd, (int, float)):
                    vista_total_days += int(vd)

        # Pauta metrics from session events
        pauta_inclusion_count = sum(1 for se in session_events if se.get("event_type") == "pauta_inclusion")
        pauta_withdrawal_count = sum(1 for se in session_events if se.get("event_type") == "pauta_withdrawal")
        pauta_cycle_count = min(pauta_inclusion_count, pauta_withdrawal_count)

        # Redistribution count
        redistribution_count = sum(1 for m in movements if _is_redistribution(m))

        # Decision year for peer grouping
        decision_year: int | None = None
        if first_dec_str:
            dec_parsed = _parse_date(first_dec_str)
            if dec_parsed:
                decision_year = dec_parsed.year

        process_class = str(meta.get("process_class") or "") if meta else ""

        raw_records.append(
            {
                "process_id": pid,
                "process_number": meta.get("process_number") if meta else None,
                "process_class": process_class if process_class else None,
                "first_distribution_date": dist_date_str,
                "first_decision_date": first_dec_str,
                "days_distribution_to_first_decision": days_to_decision,
                "days_in_vista_total": vista_total_days,
                "vista_count": vista_count,
                "pauta_inclusion_count": pauta_inclusion_count,
                "pauta_withdrawal_count": pauta_withdrawal_count,
                "pauta_cycle_count": pauta_cycle_count,
                "redistribution_count": redistribution_count,
                "total_movement_count": len(movements),
                "decision_year": decision_year,
            }
        )

    if on_progress:
        on_progress(2, 5, "Timeline: Calculando grupos de pares...")

    # Peer group analysis
    peer_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for rec in raw_records:
        pc = rec.get("process_class") or ""
        dy = rec.get("decision_year")
        if pc and dy is not None:
            key = f"{pc}:{dy}"
            peer_groups[key].append(rec)

    # Compute peer group statistics
    peer_stats: dict[str, dict[str, Any]] = {}
    for key, group in peer_groups.items():
        days_values = sorted(
            r["days_distribution_to_first_decision"]
            for r in group
            if r["days_distribution_to_first_decision"] is not None
        )
        vista_values = sorted(r["days_in_vista_total"] for r in group)
        cycle_values = sorted(r["pauta_cycle_count"] for r in group)

        if len(days_values) >= min_peer_group_size:
            peer_stats[key] = {
                "group_size": len(group),
                "median_days": round(_percentile(days_values, 50), 1),
                "p5_days": round(_percentile(days_values, PERCENTILE_LOW), 1),
                "p95_days": round(_percentile(days_values, PERCENTILE_HIGH), 1),
                "vista_p95": round(_percentile([float(v) for v in vista_values], PERCENTILE_HIGH), 1),
                "cycle_p95": round(_percentile([float(c) for c in cycle_values], PERCENTILE_HIGH), 1),
            }

    if on_progress:
        on_progress(3, 5, "Timeline: Detectando anomalias...")

    now_iso = datetime.now(timezone.utc).isoformat()
    records: list[dict[str, Any]] = []

    for rec in raw_records:
        pc = rec.get("process_class") or ""
        dy = rec.get("decision_year")
        peer_key = f"{pc}:{dy}" if pc and dy is not None else None

        stats = peer_stats.get(peer_key or "", None)

        # Peer comparison fields
        peer_group_key: str | None = None
        peer_group_size: int | None = None
        peer_median: float | None = None
        peer_p5: float | None = None
        peer_p95: float | None = None
        vista_flag = False
        pauta_cycle_flag = False
        velocity_flag: str | None = None

        if stats:
            peer_group_key = peer_key
            peer_group_size = stats["group_size"]
            peer_median = stats["median_days"]
            peer_p5 = stats["p5_days"]
            peer_p95 = stats["p95_days"]

            dtd = rec["days_distribution_to_first_decision"]
            if dtd is not None:
                if peer_p5 is not None and dtd < peer_p5:
                    velocity_flag = "fast"
                elif peer_p95 is not None and dtd > peer_p95:
                    velocity_flag = "slow"

            if stats["vista_p95"] > 0 and rec["days_in_vista_total"] > stats["vista_p95"]:
                vista_flag = True

            if stats["cycle_p95"] > 0 and rec["pauta_cycle_count"] > stats["cycle_p95"]:
                pauta_cycle_flag = True

        timeline_id = stable_id("tl_", str(rec["process_id"]))
        records.append(
            {
                "timeline_id": timeline_id,
                "process_id": rec["process_id"],
                "process_number": rec["process_number"],
                "process_class": rec["process_class"],
                "first_distribution_date": rec["first_distribution_date"],
                "first_decision_date": rec["first_decision_date"],
                "days_distribution_to_first_decision": rec["days_distribution_to_first_decision"],
                "days_in_vista_total": rec["days_in_vista_total"],
                "vista_count": rec["vista_count"],
                "pauta_inclusion_count": rec["pauta_inclusion_count"],
                "pauta_withdrawal_count": rec["pauta_withdrawal_count"],
                "pauta_cycle_count": rec["pauta_cycle_count"],
                "redistribution_count": rec["redistribution_count"],
                "total_movement_count": rec["total_movement_count"],
                "peer_group_key": peer_group_key,
                "peer_group_size": peer_group_size,
                "peer_median_days_to_decision": peer_median,
                "peer_p5_days": peer_p5,
                "peer_p95_days": peer_p95,
                "vista_flag": vista_flag,
                "pauta_cycle_flag": pauta_cycle_flag,
                "velocity_flag": velocity_flag,
                "generated_at": now_iso,
            }
        )

    if on_progress:
        on_progress(4, 5, "Timeline: Gravando resultados...")

    validate_records(records, SCHEMA_PATH)

    output_path = output_dir / "procedural_timeline.jsonl"
    with AtomicJsonlWriter(output_path) as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    vista_flags = sum(1 for r in records if r["vista_flag"])
    cycle_flags = sum(1 for r in records if r["pauta_cycle_flag"])
    velocity_flags = sum(1 for r in records if r["velocity_flag"])

    summary = {
        "generated_at": now_iso,
        "total_records": len(records),
        "vista_flag_count": vista_flags,
        "pauta_cycle_flag_count": cycle_flags,
        "velocity_flag_count": velocity_flags,
        "peer_groups_analyzed": len(peer_stats),
    }
    validate_records([summary], SUMMARY_SCHEMA_PATH)
    summary_path = output_dir / "procedural_timeline_summary.json"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    logger.info(
        "Built procedural timeline: %d records (%d vista flags, %d cycle flags, %d velocity flags)",
        len(records),
        vista_flags,
        cycle_flags,
        velocity_flags,
    )
    if on_progress:
        on_progress(5, 5, "Timeline: Concluído")
    return output_path
