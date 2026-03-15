"""Build amicus curiae network analytics from curated representation data."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.identity import stable_id
from ..curated.common import read_jsonl_records, write_jsonl

logger = logging.getLogger(__name__)

DEFAULT_CURATED_DIR = Path("data/curated")
DEFAULT_OUTPUT_DIR = Path("data/analytics")

_AMICUS_ROLE_TYPES: frozenset[str] = frozenset(
    {
        "amicus_representative",
    }
)


def build_amicus_network(
    *,
    curated_dir: Path = DEFAULT_CURATED_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Build amicus curiae network analytics.

    Identifies recurrent amicus appearances by class, theme, minister, period.

    Output fields per record:
    - amicus_id, lawyer_id, lawyer_name
    - process_class_distribution, minister_distribution
    - process_count, edge_count, first_date, last_date
    """
    total = 5
    step = 0

    def tick(desc: str) -> None:
        nonlocal step
        if on_progress:
            on_progress(step, total, desc)
        step += 1

    tick("Amicus: Carregando entidades...")

    lawyer_path = curated_dir / "lawyer_entity.jsonl"
    edge_path = curated_dir / "representation_edge.jsonl"
    event_path = curated_dir / "representation_event.jsonl"
    process_path = curated_dir / "process.jsonl"
    decision_event_path = curated_dir / "decision_event.jsonl"

    lawyers = read_jsonl_records(lawyer_path) if lawyer_path.exists() else []
    edges = read_jsonl_records(edge_path) if edge_path.exists() else []
    events = read_jsonl_records(event_path) if event_path.exists() else []
    processes = read_jsonl_records(process_path) if process_path.exists() else []
    decision_events = read_jsonl_records(decision_event_path) if decision_event_path.exists() else []

    tick("Amicus: Indexando dados...")

    lawyer_lookup: dict[str, str] = {}
    for rec in lawyers:
        lid = rec.get("lawyer_id")
        name = rec.get("lawyer_name_normalized") or rec.get("lawyer_name_raw", "")
        if lid:
            lawyer_lookup[lid] = name

    process_class_map: dict[str, str] = {}
    for rec in processes:
        pid = rec.get("process_id")
        pc = rec.get("process_class")
        if pid and pc:
            process_class_map[pid] = pc

    # Map process_id -> rapporteur (latest)
    rapporteur_map: dict[str, str] = {}
    rapporteur_keys: dict[str, tuple[str, int]] = {}
    for position, rec in enumerate(decision_events):
        pid = rec.get("process_id")
        rap = rec.get("current_rapporteur")
        if pid and rap:
            key = (str(rec.get("decision_date") or ""), position)
            current = rapporteur_keys.get(pid)
            if current is None or key >= current:
                rapporteur_keys[pid] = key
                rapporteur_map[pid] = rap

    tick("Amicus: Filtrando arestas amicus...")

    # Filter edges for amicus role
    amicus_edges = [e for e in edges if e.get("role_type") in _AMICUS_ROLE_TYPES]

    # Also check events for amicus_brief type
    amicus_event_lawyers: dict[str, set[str]] = defaultdict(set)
    for event in events:
        if event.get("event_type") == "amicus_brief":
            pid = event.get("process_id", "")
            lid = event.get("lawyer_id")
            if lid and pid:
                amicus_event_lawyers[pid].add(lid)

    # Group by lawyer_id
    lawyer_amicus_data: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"process_ids": set(), "edge_count": 0, "dates": []}
    )

    for edge in amicus_edges:
        lid = edge.get("lawyer_id")
        if not lid:
            continue
        pid = edge.get("process_id", "")
        data = lawyer_amicus_data[lid]
        data["process_ids"].add(pid)
        data["edge_count"] += 1
        start = edge.get("start_date")
        end = edge.get("end_date")
        if start:
            data["dates"].append(start)
        if end:
            data["dates"].append(end)

    # Merge event-based amicus detections
    for pid, lawyer_ids in amicus_event_lawyers.items():
        for lid in lawyer_ids:
            data = lawyer_amicus_data[lid]
            data["process_ids"].add(pid)

    tick("Amicus: Calculando metricas...")

    timestamp = datetime.now(timezone.utc).isoformat()
    records: list[dict[str, Any]] = []

    for lawyer_id, data in lawyer_amicus_data.items():
        process_ids: set[str] = data["process_ids"]
        process_count = len(process_ids)
        if process_count < 1:
            continue

        # Build distributions
        class_dist: dict[str, int] = defaultdict(int)
        minister_dist: dict[str, int] = defaultdict(int)
        for pid in process_ids:
            pc = process_class_map.get(pid)
            if pc:
                class_dist[pc] += 1
            rap = rapporteur_map.get(pid)
            if rap:
                minister_dist[rap] += 1

        sorted_dates = sorted(data["dates"]) if data["dates"] else []
        first_date = sorted_dates[0] if sorted_dates else None
        last_date = sorted_dates[-1] if sorted_dates else None

        amicus_id = stable_id("ami_", f"amicus:{lawyer_id}")
        records.append(
            {
                "amicus_id": amicus_id,
                "lawyer_id": lawyer_id,
                "lawyer_name": lawyer_lookup.get(lawyer_id, ""),
                "process_class_distribution": dict(class_dist),
                "minister_distribution": dict(minister_dist),
                "process_count": process_count,
                "edge_count": data["edge_count"],
                "first_date": first_date,
                "last_date": last_date,
                "generated_at": timestamp,
            }
        )

    tick("Amicus: Escrevendo resultados...")

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = write_jsonl(records, output_dir / "amicus_network.jsonl")

    all_process_ids: set[str] = set()
    for data in lawyer_amicus_data.values():
        all_process_ids.update(data["process_ids"])

    summary: dict[str, Any] = {
        "total_amicus_lawyers": len(records),
        "total_amicus_edges": sum(r["edge_count"] for r in records),
        "total_processes": len(all_process_ids),
        "lawyers_multi_process": sum(1 for r in records if r["process_count"] >= 2),
        "generated_at": timestamp,
    }

    summary_path = output_dir / "amicus_network_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    tick("Amicus: Concluido")
    logger.info(
        "Amicus network: %d lawyer records written to %s",
        len(records),
        output_path,
    )

    return output_path
