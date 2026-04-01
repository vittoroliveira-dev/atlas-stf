"""Build source evidence records from portal extractions."""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any

from ..core.identity import stable_id
from ..schema_validate import validate_records

logger = logging.getLogger(__name__)

EVIDENCE_SCHEMA_PATH = Path("schemas/source_evidence.schema.json")


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


def _sha256_short(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


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
            records.append(
                {
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
                }
            )

        # Evidence for oral arguments
        for oral in doc.get("oral_arguments", []):
            lawyer_name = oral.get("lawyer_name", "")
            snippet = f"lawyer={lawyer_name}, party={oral.get('party_represented', '')}"
            field_hash = _sha256_short(snippet)
            composite = f"portal_stf:{process_number}:oral_arguments:{field_hash}"
            evidence_id = stable_id("evi_", composite)
            records.append(
                {
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
                }
            )

        # Evidence for detailed petitions
        for pet in doc.get("peticoes_detailed", []):
            petitioner = pet.get("petitioner_name", "")
            snippet = f"petitioner={petitioner}, type={pet.get('document_type', '')}"
            field_hash = _sha256_short(snippet)
            composite = f"portal_stf:{process_number}:peticoes_detailed:{field_hash}"
            evidence_id = stable_id("evi_", composite)
            records.append(
                {
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
                }
            )

    validate_records(records, EVIDENCE_SCHEMA_PATH)
    return records
