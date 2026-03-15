"""Orchestrator for building representation-network curated artifacts.

Produces five JSONL files:
- lawyer_entity.jsonl   — individual lawyer entities
- law_firm_entity.jsonl — law firm entities
- representation_edge.jsonl — edges linking representatives to processes
- representation_event.jsonl — events evidencing representation activity
- source_evidence.jsonl — provenance records linking extractions to sources
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

from .common import write_jsonl

logger = logging.getLogger(__name__)

DEFAULT_PROCESS_PATH = Path("data/curated/process.jsonl")
DEFAULT_PORTAL_DIR = Path("data/raw/stf_portal")
DEFAULT_CURATED_DIR = Path("data/curated")


def build_representation_jsonl(
    *,
    process_path: Path = DEFAULT_PROCESS_PATH,
    portal_dir: Path = DEFAULT_PORTAL_DIR,
    curated_dir: Path = DEFAULT_CURATED_DIR,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> dict[str, Path]:
    """Build all representation-network artifacts.

    Returns a mapping of artifact name to output path.
    """
    from ._build_representation_edges import (
        build_representation_edge_records,
        build_representation_event_records,
        build_source_evidence_records,
    )
    from ._build_representation_firms import build_law_firm_entity_records
    from ._build_representation_lawyers import build_lawyer_entity_records

    total = 5
    results: dict[str, Path] = {}

    def _progress(step: int, desc: str) -> None:
        if on_progress:
            on_progress(step, total, desc)

    # Step 1: Lawyer entities
    _progress(0, "Construindo entidades de advogado...")
    lawyer_records = build_lawyer_entity_records(process_path, portal_dir, curated_dir)
    lawyer_path = curated_dir / "lawyer_entity.jsonl"
    results["lawyer_entity"] = write_jsonl(lawyer_records, lawyer_path)
    logger.info("Built %d lawyer entities", len(lawyer_records))

    # Build lookup maps for edges
    lawyer_map: dict[str, dict[str, Any]] = {}
    for rec in lawyer_records:
        key = rec.get("identity_key")
        if key:
            lawyer_map[key] = rec

    # Step 2: Law firm entities
    _progress(1, "Construindo entidades de escritorio...")
    firm_records = build_law_firm_entity_records(process_path, portal_dir, curated_dir)
    firm_path = curated_dir / "law_firm_entity.jsonl"
    results["law_firm_entity"] = write_jsonl(firm_records, firm_path)
    logger.info("Built %d law firm entities", len(firm_records))

    firm_map: dict[str, dict[str, Any]] = {}
    for rec in firm_records:
        key = rec.get("identity_key")
        if key:
            firm_map[key] = rec

    # Build party lookup from party.jsonl if available
    party_map = _load_party_map(curated_dir)

    # Step 3: Representation edges
    _progress(2, "Construindo arestas de representacao...")
    edge_records = build_representation_edge_records(
        process_path=process_path,
        portal_dir=portal_dir,
        curated_dir=curated_dir,
        lawyer_map=lawyer_map,
        firm_map=firm_map,
        party_map=party_map,
    )
    edge_path = curated_dir / "representation_edge.jsonl"
    results["representation_edge"] = write_jsonl(edge_records, edge_path)
    logger.info("Built %d representation edges", len(edge_records))

    # Step 4: Representation events
    _progress(3, "Construindo eventos de representacao...")
    event_records = build_representation_event_records(
        process_path=process_path,
        portal_dir=portal_dir,
    )
    event_path = curated_dir / "representation_event.jsonl"
    results["representation_event"] = write_jsonl(event_records, event_path)
    logger.info("Built %d representation events", len(event_records))

    # Step 5: Source evidence
    _progress(4, "Construindo evidencias de origem...")
    evidence_records = build_source_evidence_records(portal_dir=portal_dir)
    evidence_path = curated_dir / "source_evidence.jsonl"
    results["source_evidence"] = write_jsonl(evidence_records, evidence_path)
    logger.info("Built %d source evidence records", len(evidence_records))

    _progress(total, "Representacao concluida")
    return results


def _load_party_map(curated_dir: Path) -> dict[str, dict[str, Any]]:
    """Load party records into a lookup by identity_key."""
    party_path = curated_dir / "party.jsonl"
    if not party_path.exists():
        return {}
    from .common import read_jsonl_records

    party_map: dict[str, dict[str, Any]] = {}
    for rec in read_jsonl_records(party_path):
        key = rec.get("identity_key")
        if key:
            party_map[key] = rec
    return party_map
