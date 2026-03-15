"""Build representation edges, events, and source evidence records.

Edges connect representative entities (lawyers/firms) to processes.
Events capture specific procedural moments (petitions, oral arguments).
Source evidence provides provenance traceability.
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any

from ..core.identity import (
    build_lawyer_identity_key,
    canonicalize_entity_name,
    normalize_entity_name,
    stable_id,
)
from ..core.parsers import counsel_entries_from_juris_partes, split_name_list
from ..schema_validate import validate_records

logger = logging.getLogger(__name__)

EDGE_SCHEMA_PATH = Path("schemas/representation_edge.schema.json")
EVENT_SCHEMA_PATH = Path("schemas/representation_event.schema.json")
EVIDENCE_SCHEMA_PATH = Path("schemas/source_evidence.schema.json")

COUNSEL_SOURCE_FIELDS = (
    "juris_advogados",
    "juris_procuradores",
    "counsel_raw",
)


def _sha256_short(text: str) -> str:
    """Short hash for evidence dedup."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _resolve_lawyer_id(
    name_normalized: str,
    lawyer_map: dict[str, dict[str, Any]],
    *,
    oab_number: str | None = None,
) -> str | None:
    """Resolve a lawyer entity ID from the map using identity key lookup."""
    identity_key = build_lawyer_identity_key(name=name_normalized, oab_number=oab_number)
    if identity_key and identity_key in lawyer_map:
        return lawyer_map[identity_key]["lawyer_id"]
    return None


def _infer_role_type(label: str | None) -> str | None:
    """Map source label to representation role_type enum."""
    if not label:
        return None
    label_upper = label.upper()
    if "PROC" in label_upper:
        return "public_attorney"
    if "ADV" in label_upper or "DEF" in label_upper:
        return "counsel_of_record"
    return None


def build_representation_edge_records(
    *,
    process_path: Path,
    portal_dir: Path,
    curated_dir: Path,
    lawyer_map: dict[str, dict[str, Any]],
    firm_map: dict[str, dict[str, Any]],
    party_map: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build representation edge records linking lawyers/firms to processes.

    edge_id = stable_id("rep_", f"{process_id}:{party_id}:{representative_entity_id}:{role_type}")
    """
    from .common import read_jsonl_records

    processes = read_jsonl_records(process_path) if process_path.exists() else []
    portal_docs = _load_portal_docs(portal_dir)

    edge_map: dict[str, dict[str, Any]] = {}

    # ---- Source 1: process.jsonl — juris_partes ----
    for process in processes:
        process_id = process["process_id"]
        for label, counsel_name, party_role in counsel_entries_from_juris_partes(
            process.get("juris_partes"),
        ):
            normalized = normalize_entity_name(counsel_name)
            if normalized is None:
                continue
            lawyer_id = _resolve_lawyer_id(normalized, lawyer_map)
            if lawyer_id is None:
                continue
            role_type = _infer_role_type(label)
            party_id = _resolve_party_id(party_role, party_map)
            _register_edge(
                edge_map,
                process_id=process_id,
                representative_entity_id=lawyer_id,
                representative_kind="lawyer",
                role_type=role_type,
                lawyer_id=lawyer_id,
                firm_id=None,
                party_id=party_id,
                source_system="jurisprudencia",
            )

        # ---- Source 2: process.jsonl — counsel source fields ----
        for field in COUNSEL_SOURCE_FIELDS:
            for counsel_name in split_name_list(process.get(field)):
                normalized = normalize_entity_name(counsel_name)
                if normalized is None:
                    continue
                lawyer_id = _resolve_lawyer_id(normalized, lawyer_map)
                if lawyer_id is None:
                    continue
                _register_edge(
                    edge_map,
                    process_id=process_id,
                    representative_entity_id=lawyer_id,
                    representative_kind="lawyer",
                    role_type="counsel_of_record",
                    lawyer_id=lawyer_id,
                    firm_id=None,
                    party_id=None,
                    source_system="jurisprudencia",
                )

    # ---- Source 3: portal representantes ----
    for doc in portal_docs:
        process_number = doc.get("process_number")
        if not process_number:
            continue
        # Match process_id from process records
        process_id = _find_process_id_by_number(processes, process_number)
        if not process_id:
            continue
        for rep in doc.get("representantes", []):
            lawyer_name = rep.get("lawyer_name")
            if not lawyer_name:
                continue
            normalized = normalize_entity_name(lawyer_name)
            if normalized is None:
                continue
            oab_number = rep.get("oab_number")
            lawyer_id = _resolve_lawyer_id(normalized, lawyer_map, oab_number=oab_number)
            if lawyer_id is None:
                continue
            party_name = rep.get("party_name")
            party_id = _resolve_party_id(party_name, party_map) if party_name else None
            _register_edge(
                edge_map,
                process_id=process_id,
                representative_entity_id=lawyer_id,
                representative_kind="lawyer",
                role_type="counsel_of_record",
                lawyer_id=lawyer_id,
                firm_id=None,
                party_id=party_id,
                source_system="portal_stf",
            )

    records = sorted(edge_map.values(), key=lambda item: item["edge_id"])
    validate_records(records, EDGE_SCHEMA_PATH)
    return records


def _register_edge(
    edge_map: dict[str, dict[str, Any]],
    *,
    process_id: str,
    representative_entity_id: str,
    representative_kind: str,
    role_type: str | None,
    lawyer_id: str | None,
    firm_id: str | None,
    party_id: str | None,
    source_system: str,
) -> None:
    """Register or merge an edge in the map."""
    role_str = role_type or "unknown"
    party_str = party_id or "none"
    composite = f"{process_id}:{party_str}:{representative_entity_id}:{role_str}"
    edge_id = stable_id("rep_", composite)

    existing = edge_map.get(edge_id)
    if existing is not None:
        if source_system not in existing["source_systems"]:
            existing["source_systems"].append(source_system)
            existing["evidence_count"] = len(existing["source_systems"])
        return

    edge_map[edge_id] = {
        "edge_id": edge_id,
        "process_id": process_id,
        "representative_entity_id": representative_entity_id,
        "representative_kind": representative_kind,
        "role_type": role_type,
        "lawyer_id": lawyer_id,
        "firm_id": firm_id,
        "party_id": party_id,
        "start_date": None,
        "end_date": None,
        "evidence_count": 1,
        "evidence_ids": [],
        "confidence": 0.8 if source_system == "portal_stf" else 0.6,
        "source_systems": [source_system],
    }


def _resolve_party_id(
    party_ref: str | None,
    party_map: dict[str, dict[str, Any]],
) -> str | None:
    """Resolve a party ID from a name or role string."""
    if not party_ref:
        return None
    normalized = normalize_entity_name(party_ref)
    if normalized is None:
        return None
    canonical = canonicalize_entity_name(normalized)
    if canonical:
        key = f"name:{canonical}"
        rec = party_map.get(key)
        if rec:
            return rec["party_id"]
    return None


def _find_process_id_by_number(
    processes: list[dict[str, Any]],
    process_number: str,
) -> str | None:
    """Find a process_id by process_number in the list."""
    for proc in processes:
        if proc.get("process_number") == process_number:
            return proc["process_id"]
    return None


def _load_portal_docs(portal_dir: Path) -> list[dict[str, Any]]:
    """Load portal JSON documents."""
    if not portal_dir.exists():
        return []
    docs: list[dict[str, Any]] = []
    for path in sorted(portal_dir.glob("*.json")):
        try:
            with path.open(encoding="utf-8") as fh:
                docs.append(json.load(fh))
        except (json.JSONDecodeError, ValueError):
            logger.warning("Skipping invalid portal file: %s", path)
    return docs


# ---------------------------------------------------------------------------
# Representation events
# ---------------------------------------------------------------------------


def build_representation_event_records(
    *,
    process_path: Path,
    portal_dir: Path,
) -> list[dict[str, Any]]:
    """Build representation event records from portal data.

    event_type uses unified enum (oral_argument, petition, etc.).
    """
    from .common import read_jsonl_records

    portal_docs = _load_portal_docs(portal_dir)
    processes = read_jsonl_records(process_path) if process_path.exists() else []
    records: list[dict[str, Any]] = []

    for doc in portal_docs:
        process_number = doc.get("process_number")
        if not process_number:
            continue
        process_id = _find_process_id_by_number(processes, process_number)
        if not process_id:
            continue
        source_url = doc.get("source_url", "")

        # Oral arguments
        for oral in doc.get("oral_arguments", []):
            lawyer_name = oral.get("lawyer_name")
            event_date = oral.get("session_date")
            if not lawyer_name or not event_date:
                continue
            event_desc = f"Sustentacao oral: {lawyer_name}"
            if oral.get("party_represented"):
                event_desc += f" (representando {oral['party_represented']})"
            composite = f"{process_id}:oral_argument:{event_date}:{lawyer_name}"
            event_id = stable_id("evt_", composite)
            records.append({
                "event_id": event_id,
                "process_id": process_id,
                "edge_id": None,
                "lawyer_id": None,
                "firm_id": None,
                "event_type": "oral_argument",
                "event_date": event_date,
                "event_description": event_desc,
                "protocol_number": None,
                "document_type": oral.get("session_type"),
                "source_system": "portal_stf",
                "source_url": source_url,
                "source_evidence_id": None,
                "confidence": 0.8,
            })

        # Detailed petitions
        for pet in doc.get("peticoes_detailed", []):
            petitioner_name = pet.get("petitioner_name")
            event_date = pet.get("date")
            if not petitioner_name or not event_date:
                continue
            doc_type = pet.get("document_type")
            protocol = pet.get("protocol")
            event_desc = f"Peticao: {petitioner_name}"
            if doc_type:
                event_desc += f" ({doc_type})"
            composite = f"{process_id}:petition:{event_date}:{petitioner_name}"
            event_id = stable_id("evt_", composite)
            records.append({
                "event_id": event_id,
                "process_id": process_id,
                "edge_id": None,
                "lawyer_id": None,
                "firm_id": None,
                "event_type": "petition",
                "event_date": event_date,
                "event_description": event_desc,
                "protocol_number": protocol,
                "document_type": doc_type,
                "source_system": "portal_stf",
                "source_url": source_url,
                "source_evidence_id": None,
                "confidence": 0.7,
            })

    validate_records(records, EVENT_SCHEMA_PATH)
    return records


# ---------------------------------------------------------------------------
# Source evidence
# ---------------------------------------------------------------------------


def build_source_evidence_records(
    *,
    portal_dir: Path,
) -> list[dict[str, Any]]:
    """Build source evidence records from portal extractions.

    evidence_id = stable_id("evi_", f"{source}:{process}:{field}:{hash}")
    """
    portal_docs = _load_portal_docs(portal_dir)
    records: list[dict[str, Any]] = []

    for doc in portal_docs:
        process_number = doc.get("process_number")
        source_url = doc.get("source_url", "")
        fetched_at = doc.get("fetched_at")
        if not process_number or not source_url:
            continue

        # Evidence for each representante
        for rep in doc.get("representantes", []):
            lawyer_name = rep.get("lawyer_name", "")
            snippet = f"party={rep.get('party_name', '')}, lawyer={lawyer_name}"
            field_hash = _sha256_short(snippet)
            composite = f"portal_stf:{process_number}:representantes:{field_hash}"
            evidence_id = stable_id("evi_", composite)
            records.append({
                "evidence_id": evidence_id,
                "source_system": "portal_stf",
                "source_url": source_url,
                "entity_id": None,
                "edge_id": None,
                "event_id": None,
                "source_tab": "Partes",
                "process_number": process_number,
                "process_id": None,
                "snippet_text": snippet,
                "raw_field_name": "representantes",
                "parser_version": "representation-v1",
                "extraction_confidence": 0.8,
                "fetched_at": fetched_at,
            })

        # Evidence for oral arguments
        for oral in doc.get("oral_arguments", []):
            lawyer_name = oral.get("lawyer_name", "")
            snippet = f"lawyer={lawyer_name}, party={oral.get('party_represented', '')}"
            field_hash = _sha256_short(snippet)
            composite = f"portal_stf:{process_number}:oral_arguments:{field_hash}"
            evidence_id = stable_id("evi_", composite)
            records.append({
                "evidence_id": evidence_id,
                "source_system": "portal_stf",
                "source_url": source_url,
                "entity_id": None,
                "edge_id": None,
                "event_id": None,
                "source_tab": "Sustentacao Oral",
                "process_number": process_number,
                "process_id": None,
                "snippet_text": snippet,
                "raw_field_name": "oral_arguments",
                "parser_version": "representation-v1",
                "extraction_confidence": 0.8,
                "fetched_at": fetched_at,
            })

        # Evidence for detailed petitions
        for pet in doc.get("peticoes_detailed", []):
            petitioner = pet.get("petitioner_name", "")
            snippet = f"petitioner={petitioner}, type={pet.get('document_type', '')}"
            field_hash = _sha256_short(snippet)
            composite = f"portal_stf:{process_number}:peticoes_detailed:{field_hash}"
            evidence_id = stable_id("evi_", composite)
            records.append({
                "evidence_id": evidence_id,
                "source_system": "portal_stf",
                "source_url": source_url,
                "entity_id": None,
                "edge_id": None,
                "event_id": None,
                "source_tab": "Peticoes",
                "process_number": process_number,
                "process_id": None,
                "snippet_text": snippet,
                "raw_field_name": "peticoes_detailed",
                "parser_version": "representation-v1",
                "extraction_confidence": 0.7,
                "fetched_at": fetched_at,
            })

    validate_records(records, EVIDENCE_SCHEMA_PATH)
    return records
