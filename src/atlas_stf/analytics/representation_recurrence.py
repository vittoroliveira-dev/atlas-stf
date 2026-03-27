"""Build representation recurrence analytics from curated representation data."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from collections.abc import Callable
from datetime import date as date_type
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.identity import stable_id
from ..curated.common import read_jsonl_records, write_jsonl
from ..schema_validate import validate_records

logger = logging.getLogger(__name__)

DEFAULT_CURATED_DIR = Path("data/curated")
DEFAULT_OUTPUT_DIR = Path("data/analytics")
SCHEMA_PATH = Path("schemas/representation_recurrence.schema.json")
SUMMARY_SCHEMA_PATH = Path("schemas/representation_recurrence_summary.schema.json")


def build_representation_recurrence(
    *,
    curated_dir: Path = DEFAULT_CURATED_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Compute how often each lawyer<->party pair recurs across processes.

    Output fields per record:
    - recurrence_id, lawyer_id, party_id, lawyer_name, party_name
    - process_count, edge_count, first_seen_date, last_seen_date, span_days
    - process_classes (dict), role_types (dict)
    """
    total = 5
    step = 0

    def tick(desc: str) -> None:
        nonlocal step
        if on_progress:
            on_progress(step, total, desc)
        step += 1

    tick("Recorrencia: Carregando entidades...")

    lawyer_path = curated_dir / "lawyer_entity.jsonl"
    edge_path = curated_dir / "representation_edge.jsonl"
    party_path = curated_dir / "party.jsonl"
    process_path = curated_dir / "process.jsonl"

    lawyers = read_jsonl_records(lawyer_path) if lawyer_path.exists() else []
    edges = read_jsonl_records(edge_path) if edge_path.exists() else []
    parties = read_jsonl_records(party_path) if party_path.exists() else []

    tick("Recorrencia: Indexando dados...")

    lawyer_lookup: dict[str, str] = {}
    for rec in lawyers:
        lid = rec.get("lawyer_id")
        name = rec.get("lawyer_name_raw") or rec.get("lawyer_name_normalized", "")
        if lid:
            lawyer_lookup[lid] = name

    party_lookup: dict[str, str] = {}
    for rec in parties:
        pid = rec.get("party_id")
        name = rec.get("party_name_raw") or rec.get("party_name_normalized", "")
        if pid:
            party_lookup[pid] = name

    process_class_map: dict[str, str] = {}
    if process_path.exists():
        with process_path.open(encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    rec = json.loads(line)
                    pid = rec.get("process_id")
                    pc = rec.get("process_class")
                    if pid and pc:
                        process_class_map[pid] = pc

    # Build process→party index from process_party_link.jsonl
    party_link_path = curated_dir / "process_party_link.jsonl"
    party_links = read_jsonl_records(party_link_path) if party_link_path.exists() else []

    process_party_map: dict[str, set[str]] = defaultdict(set)
    for link in party_links:
        pid = link.get("process_id")
        party_id = link.get("party_id")
        if pid and party_id:
            process_party_map[pid].add(party_id)

    tick("Recorrencia: Agrupando arestas por par advogado-parte...")

    # Derive (lawyer_id, party_id) pairs via shared process_id:
    # edge gives (lawyer_id, process_id), party_link gives (party_id, process_id)
    pair_edges: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for edge in edges:
        lawyer_id = edge.get("lawyer_id")
        process_id = edge.get("process_id")
        if not lawyer_id or not process_id:
            continue
        # Direct party_id on edge (when available)
        direct_party_id = edge.get("party_id")
        if direct_party_id:
            pair_edges[(lawyer_id, direct_party_id)].append(edge)
        else:
            # Derive from process_party_link
            for party_id in process_party_map.get(process_id, ()):
                pair_edges[(lawyer_id, party_id)].append(edge)

    tick("Recorrencia: Calculando metricas...")

    timestamp = datetime.now(timezone.utc).isoformat()
    records: list[dict[str, Any]] = []

    for (lawyer_id, party_id), edge_list in pair_edges.items():
        # Collect unique processes
        process_ids: set[str] = set()
        dates: list[str] = []
        class_counts: dict[str, int] = defaultdict(int)
        role_counts: dict[str, int] = defaultdict(int)

        for edge in edge_list:
            pid = edge.get("process_id", "")
            if pid:
                process_ids.add(pid)
                pc = process_class_map.get(pid)
                if pc:
                    class_counts[pc] += 1

            role = edge.get("role_type")
            if role:
                role_counts[role] += 1

            start = edge.get("start_date")
            end = edge.get("end_date")
            if start:
                dates.append(start)
            if end:
                dates.append(end)

        process_count = len(process_ids)
        if process_count < 2:
            continue

        sorted_dates = sorted(dates) if dates else []
        first_seen = sorted_dates[0] if sorted_dates else None
        last_seen = sorted_dates[-1] if sorted_dates else None

        span_days = 0
        if first_seen and last_seen:
            try:
                d1 = date_type.fromisoformat(first_seen)
                d2 = date_type.fromisoformat(last_seen)
                span_days = (d2 - d1).days
            except ValueError:
                pass

        recurrence_id = stable_id("rec_", f"{lawyer_id}:{party_id}")
        records.append(
            {
                "recurrence_id": recurrence_id,
                "lawyer_id": lawyer_id,
                "party_id": party_id,
                "lawyer_name": lawyer_lookup.get(lawyer_id, ""),
                "party_name": party_lookup.get(party_id, ""),
                "process_count": process_count,
                "edge_count": len(edge_list),
                "first_seen_date": first_seen,
                "last_seen_date": last_seen,
                "span_days": span_days,
                "process_classes": dict(class_counts),
                "role_types": dict(role_counts),
                "generated_at": timestamp,
            }
        )

    tick("Recorrencia: Escrevendo resultados...")

    output_dir.mkdir(parents=True, exist_ok=True)
    validate_records(records, SCHEMA_PATH)
    output_path = write_jsonl(records, output_dir / "representation_recurrence.jsonl")

    summary: dict[str, Any] = {
        "total_pairs": len(records),
        "total_lawyers": len({r["lawyer_id"] for r in records}),
        "total_parties": len({r["party_id"] for r in records}),
        "max_process_count": max((r["process_count"] for r in records), default=0),
        "generated_at": timestamp,
    }
    validate_records([summary], SUMMARY_SCHEMA_PATH)
    summary_path = output_dir / "representation_recurrence_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    tick("Recorrencia: Concluido")
    logger.info(
        "Representation recurrence: %d pairs written to %s",
        len(records),
        output_path,
    )

    return output_path
