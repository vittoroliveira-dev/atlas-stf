"""Build representation graph analytics from curated representation data."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from collections.abc import Callable
from datetime import date as date_type
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..curated.common import read_jsonl_records, write_jsonl

logger = logging.getLogger(__name__)

DEFAULT_CURATED_DIR = Path("data/curated")
DEFAULT_OUTPUT_DIR = Path("data/analytics")


def build_representation_graph(
    *,
    curated_dir: Path = DEFAULT_CURATED_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Build representation graph analytics.

    1. Read lawyer_entity, law_firm_entity, representation_edge, representation_event
    2. For each edge, aggregate: event_count, event_types, first/last_event_date
    3. Calculate: active_span_days, process_count, process_classes, minister_names
    4. Identify co_lawyer_ids (other lawyers on the same side)
    5. Write representation_graph.jsonl + summary
    """
    total = 5
    step = 0

    def tick(desc: str) -> None:
        nonlocal step
        if on_progress:
            on_progress(step, total, desc)
        step += 1

    tick("Grafo: Carregando entidades...")

    lawyer_path = curated_dir / "lawyer_entity.jsonl"
    firm_path = curated_dir / "law_firm_entity.jsonl"
    edge_path = curated_dir / "representation_edge.jsonl"
    event_path = curated_dir / "representation_event.jsonl"

    lawyers = read_jsonl_records(lawyer_path) if lawyer_path.exists() else []
    firms = read_jsonl_records(firm_path) if firm_path.exists() else []
    edges = read_jsonl_records(edge_path) if edge_path.exists() else []
    events = read_jsonl_records(event_path) if event_path.exists() else []

    tick("Grafo: Indexando eventos por aresta...")

    events_by_edge: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        eid = event.get("edge_id")
        if eid:
            events_by_edge[eid].append(event)

    tick("Grafo: Calculando metricas por aresta...")

    lawyer_lookup: dict[str, dict[str, Any]] = {
        rec["lawyer_id"]: rec for rec in lawyers if "lawyer_id" in rec
    }
    firm_lookup: dict[str, dict[str, Any]] = {
        rec["firm_id"]: rec for rec in firms if "firm_id" in rec
    }

    # Group edges by process for co-lawyer detection
    edges_by_process: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for edge in edges:
        pid = edge.get("process_id")
        if pid:
            edges_by_process[pid].append(edge)

    graph_records: list[dict[str, Any]] = []
    timestamp = datetime.now(timezone.utc).isoformat()

    for edge in edges:
        edge_id = edge.get("edge_id", "")
        process_id = edge.get("process_id", "")
        rep_entity_id = edge.get("representative_entity_id", "")
        rep_kind = edge.get("representative_kind", "")
        lawyer_id = edge.get("lawyer_id")
        firm_id = edge.get("firm_id")
        party_id = edge.get("party_id", "")

        # Aggregate events for this edge
        edge_events = events_by_edge.get(edge_id, [])
        event_count = len(edge_events)
        event_types: dict[str, int] = defaultdict(int)
        event_dates: list[str] = []
        for evt in edge_events:
            et = evt.get("event_type", "other")
            event_types[et] += 1
            ed = evt.get("event_date")
            if ed:
                event_dates.append(ed)

        sorted_dates = sorted(event_dates) if event_dates else []
        first_event_date = sorted_dates[0] if sorted_dates else None
        last_event_date = sorted_dates[-1] if sorted_dates else None

        # Calculate active span
        active_span_days = 0
        if first_event_date and last_event_date:
            try:
                d1 = date_type.fromisoformat(first_event_date)
                d2 = date_type.fromisoformat(last_event_date)
                active_span_days = (d2 - d1).days
            except ValueError:
                pass

        # Find co-lawyers on the same side of the same process
        co_lawyer_ids: list[str] = []
        for other_edge in edges_by_process.get(process_id, []):
            other_lawyer = other_edge.get("lawyer_id")
            if (
                other_lawyer
                and other_lawyer != lawyer_id
                and other_edge.get("party_id") == party_id
            ):
                co_lawyer_ids.append(other_lawyer)

        evidence_count = int(edge.get("evidence_count", 0))

        record: dict[str, Any] = {
            "edge_id": edge_id,
            "representative_entity_id": rep_entity_id,
            "representative_kind": rep_kind,
            "lawyer_id": lawyer_id,
            "firm_id": firm_id,
            "party_id": party_id,
            "process_id": process_id,
            "event_count": event_count,
            "event_types": dict(event_types),
            "first_event_date": first_event_date,
            "last_event_date": last_event_date,
            "active_span_days": active_span_days,
            "evidence_count": evidence_count,
            "co_lawyer_ids": sorted(set(co_lawyer_ids)),
            "generated_at": timestamp,
        }
        graph_records.append(record)

    tick("Grafo: Escrevendo resultados...")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = write_jsonl(graph_records, output_dir / "representation_graph.jsonl")

    summary: dict[str, Any] = {
        "total_edges": len(graph_records),
        "total_lawyers": len(lawyer_lookup),
        "total_firms": len(firm_lookup),
        "total_events": len(events),
        "edges_with_events": sum(1 for r in graph_records if r["event_count"] > 0),
        "generated_at": timestamp,
    }
    summary_path = output_dir / "representation_graph_summary.json"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    tick("Grafo: Concluido")
    logger.info(
        "Representation graph: %d records written to %s",
        len(graph_records),
        output_path,
    )

    return output_path
