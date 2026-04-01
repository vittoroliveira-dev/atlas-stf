"""Build representation edges and events records.

Edges connect representative entities (lawyers/firms) to processes.
Events capture specific procedural moments (petitions, oral arguments).
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
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
from ._build_source_evidence import build_source_evidence_records as build_source_evidence_records  # re-export

logger = logging.getLogger(__name__)

EDGE_SCHEMA_PATH = Path("schemas/representation_edge.schema.json")
EVENT_SCHEMA_PATH = Path("schemas/representation_event.schema.json")

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
    number_to_id = _build_process_number_index(processes)

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
        process_id = number_to_id.get(process_number)
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


def _build_process_number_index(
    processes: list[dict[str, Any]],
) -> dict[str, str]:
    """Build process_number → process_id lookup dict (O(n) once)."""
    return {
        proc["process_number"]: proc["process_id"]
        for proc in processes
        if "process_number" in proc and "process_id" in proc
    }


def _load_portal_docs(portal_dir: Path) -> list[dict[str, Any]]:
    """Load portal JSON documents."""
    if not portal_dir.exists():
        return []
    docs: list[dict[str, Any]] = []
    for path in sorted(portal_dir.glob("*.json")):
        try:
            with path.open(encoding="utf-8") as fh:
                docs.append(json.load(fh))
        except json.JSONDecodeError, ValueError:
            logger.warning("Skipping invalid portal file: %s", path)
    return docs


# ---------------------------------------------------------------------------
# Representation events
# ---------------------------------------------------------------------------


# Pattern: "Sustentação Oral - ROLE: PARTY_NAME - recebida em DD/MM/YYYY HH:MM:SS"
_SUSTENTACAO_ORAL_RE = re.compile(
    r"Sustenta[cç][aã]o\s+Oral\s*-\s*"
    r"([^:]+?)\s*:\s*"
    r"(.+?)\s*-\s*recebida\s+em",
    re.IGNORECASE,
)


def _parse_sustentacao_oral_detail(detail: str) -> tuple[str, str] | None:
    """Parse sustentação oral detail into (role, party_name).

    Expected format:
    ``Sustentação Oral - REQUERENTE(S): PARTY NAME - recebida em DD/MM/YYYY HH:MM:SS``
    """
    match = _SUSTENTACAO_ORAL_RE.search(detail)
    if not match:
        return None
    role = match.group(1).strip()
    party_name = match.group(2).strip()
    if not role or not party_name:
        return None
    return role, party_name


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
    number_to_id = _build_process_number_index(processes)
    records: list[dict[str, Any]] = []

    for doc in portal_docs:
        process_number = doc.get("process_number")
        if not process_number:
            continue
        process_id = number_to_id.get(process_number)
        if not process_id:
            continue
        source_url = doc.get("source_url", "")

        # Oral arguments from oral_arguments field (legacy, currently empty)
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
            records.append(
                {
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
                }
            )

        # Oral arguments from andamentos detail field
        for andamento in doc.get("andamentos", []):
            desc = andamento.get("description") or ""
            if desc.lower() != "sustentação oral":
                continue
            detail = andamento.get("detail") or ""
            event_date = andamento.get("date")
            if not detail or not event_date:
                continue
            parsed = _parse_sustentacao_oral_detail(detail)
            if not parsed:
                continue
            role, party_name = parsed
            event_desc = f"Sustentacao oral - {role}: {party_name}"
            composite = f"{process_id}:oral_argument:{event_date}:{party_name}"
            event_id = stable_id("evt_", composite)
            records.append(
                {
                    "event_id": event_id,
                    "process_id": process_id,
                    "edge_id": None,
                    "lawyer_id": None,
                    "firm_id": None,
                    "event_type": "oral_argument",
                    "event_date": event_date,
                    "event_description": event_desc,
                    "protocol_number": None,
                    "document_type": None,
                    "source_system": "portal_stf",
                    "source_url": source_url,
                    "source_evidence_id": None,
                    "confidence": 0.9,
                }
            )

        # Detailed petitions
        # Data has: date, protocol, receiver (institutional dept), tab_name
        for pet in doc.get("peticoes_detailed", []):
            event_date = pet.get("date")
            protocol = pet.get("protocol")
            if not event_date or not protocol:
                continue
            receiver = pet.get("receiver") or ""
            event_desc = f"Peticao {protocol}"
            if receiver:
                event_desc += f" ({receiver})"
            composite = f"{process_id}:petition:{event_date}:{protocol}"
            event_id = stable_id("evt_", composite)
            records.append(
                {
                    "event_id": event_id,
                    "process_id": process_id,
                    "edge_id": None,
                    "lawyer_id": None,
                    "firm_id": None,
                    "event_type": "petition",
                    "event_date": event_date,
                    "event_description": event_desc,
                    "protocol_number": protocol,
                    "document_type": None,
                    "source_system": "portal_stf",
                    "source_url": source_url,
                    "source_evidence_id": None,
                    "confidence": 0.6,
                }
            )

    validate_records(records, EVENT_SCHEMA_PATH)
    return records
