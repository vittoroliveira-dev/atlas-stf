"""Build canonical lawyer entity records from process and portal data.

Sources:
- process.jsonl: juris_partes (via counsel_entries_from_juris_partes),
  juris_advogados, juris_procuradores, counsel_raw (via split_name_list)
- Portal JSONL files: representantes with OAB data

Dedup by identity_key (priority: oab > tax_id > name).
lawyer_id = stable_id("law_", identity_key).
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

from ..core.identity import (
    build_lawyer_identity_key,
    canonicalize_entity_name,
    normalize_entity_name,
    normalize_oab_number,
    stable_id,
)
from ..core.parsers import counsel_entries_from_juris_partes, split_name_list
from ..schema_validate import validate_records

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path("schemas/lawyer_entity.schema.json")
NORMALIZATION_VERSION = "lawyer-entity-v1"
COUNSEL_SOURCE_FIELDS = (
    "juris_advogados",
    "juris_procuradores",
    "counsel_raw",
)

_OAB_INLINE_RE = re.compile(
    r"OAB\s*[:/]?\s*(\d{1,6})\s*/\s*([A-Z]{2})",
    re.IGNORECASE,
)


def _extract_oab_from_name(name: str) -> tuple[str | None, str | None]:
    """Try to extract OAB number and state from inline text in a name."""
    match = _OAB_INLINE_RE.search(name)
    if match:
        number = f"{match.group(1)}/{match.group(2).upper()}"
        normalized = normalize_oab_number(number)
        if normalized:
            state = match.group(2).upper()
            return normalized, state
    return None, None


def _determine_identity_strategy(identity_key: str) -> str:
    """Determine identity strategy from the key prefix."""
    if identity_key.startswith("oab:"):
        return "oab"
    if identity_key.startswith("tax:"):
        return "tax_id"
    return "name"


def _load_portal_records(portal_dir: Path) -> list[dict[str, Any]]:
    """Load portal JSONL/JSON files and extract representantes."""
    if not portal_dir.exists():
        return []
    records: list[dict[str, Any]] = []
    for path in sorted(portal_dir.glob("*.json")):
        try:
            with path.open(encoding="utf-8") as fh:
                doc = json.load(fh)
            for rep in doc.get("representantes", []):
                rep["_portal_process_number"] = doc.get("process_number")
                rep["_portal_source_url"] = doc.get("source_url")
                records.append(rep)
        except (json.JSONDecodeError, ValueError):
            logger.warning("Skipping invalid portal file: %s", path)
    return records


def build_lawyer_entity_records(
    process_path: Path,
    portal_dir: Path,
    curated_dir: Path,
) -> list[dict[str, Any]]:
    """Build deduplicated lawyer entity records from all available sources."""
    from .common import read_jsonl_records, utc_now_iso

    processes = read_jsonl_records(process_path) if process_path.exists() else []
    portal_reps = _load_portal_records(portal_dir)
    timestamp = utc_now_iso()

    # Map: identity_key -> record
    lawyer_map: dict[str, dict[str, Any]] = {}

    # ---- Source 1: process.jsonl — juris_partes ----
    for process in processes:
        for _label, counsel_name, _party_role in counsel_entries_from_juris_partes(
            process.get("juris_partes"),
        ):
            _register_lawyer_from_name(
                lawyer_map,
                raw_name=counsel_name,
                source_system="jurisprudencia",
                timestamp=timestamp,
            )

        # ---- Source 2: process.jsonl — juris_advogados, juris_procuradores, counsel_raw ----
        for field in COUNSEL_SOURCE_FIELDS:
            for counsel_name in split_name_list(process.get(field)):
                _register_lawyer_from_name(
                    lawyer_map,
                    raw_name=counsel_name,
                    source_system="jurisprudencia",
                    timestamp=timestamp,
                )

    # ---- Source 3: portal representantes ----
    for rep in portal_reps:
        lawyer_name = rep.get("lawyer_name")
        if not lawyer_name:
            continue
        oab_number = rep.get("oab_number")
        oab_state = rep.get("oab_state")
        _register_lawyer_with_oab(
            lawyer_map,
            raw_name=lawyer_name,
            oab_number=oab_number,
            oab_state=oab_state,
            source_system="portal_stf",
            timestamp=timestamp,
        )

    records = sorted(lawyer_map.values(), key=lambda item: item["lawyer_id"])
    validate_records(records, SCHEMA_PATH)
    return records


def _merge_lawyer_into(target: dict[str, Any], source: dict[str, Any]) -> None:
    """Merge *source* lawyer record into *target*, preserving best-available data."""
    # source_systems: union without duplicates
    for ss in source.get("source_systems", []):
        if ss not in target["source_systems"]:
            target["source_systems"].append(ss)

    # OAB fields: keep target if populated, else fill from source
    for field in (
        "oab_number", "oab_state", "oab_source",
        "oab_validation_method", "oab_status", "oab_last_checked_at",
    ):
        if not target.get(field) and source.get(field):
            target[field] = source[field]

    # Scalar fields: keep target if populated
    for field in ("entity_tax_id", "firm_id", "notes"):
        if not target.get(field) and source.get(field):
            target[field] = source[field]

    # Timestamps: created_at = earliest, updated_at = latest
    src_created = source.get("created_at", "")
    tgt_created = target.get("created_at", "")
    if src_created and (not tgt_created or src_created < tgt_created):
        target["created_at"] = src_created
    src_updated = source.get("updated_at", "")
    tgt_updated = target.get("updated_at", "")
    if src_updated and (not tgt_updated or src_updated > tgt_updated):
        target["updated_at"] = src_updated

    # Date range: min first_seen, max last_seen
    src_first = source.get("first_seen_date")
    tgt_first = target.get("first_seen_date")
    if src_first and (not tgt_first or src_first < tgt_first):
        target["first_seen_date"] = src_first
    src_last = source.get("last_seen_date")
    tgt_last = target.get("last_seen_date")
    if src_last and (not tgt_last or src_last > tgt_last):
        target["last_seen_date"] = src_last

    # Confidence: max
    src_conf = source.get("normalization_confidence", 0.0)
    tgt_conf = target.get("normalization_confidence", 0.0)
    if src_conf > tgt_conf:
        target["normalization_confidence"] = src_conf


def _register_lawyer_from_name(
    lawyer_map: dict[str, dict[str, Any]],
    *,
    raw_name: str,
    source_system: str,
    timestamp: str,
) -> None:
    """Register a lawyer from a raw name, trying to extract OAB inline."""
    normalized = normalize_entity_name(raw_name)
    if normalized is None:
        return

    oab_number, oab_state = _extract_oab_from_name(raw_name)
    _register_lawyer_with_oab(
        lawyer_map,
        raw_name=raw_name,
        oab_number=oab_number,
        oab_state=oab_state,
        source_system=source_system,
        timestamp=timestamp,
    )


def _register_lawyer_with_oab(
    lawyer_map: dict[str, dict[str, Any]],
    *,
    raw_name: str,
    oab_number: str | None,
    oab_state: str | None,
    source_system: str,
    timestamp: str,
) -> None:
    """Register a lawyer entity with optional OAB data."""
    normalized = normalize_entity_name(raw_name)
    if normalized is None:
        return
    canonical = canonicalize_entity_name(normalized)
    identity_key = build_lawyer_identity_key(name=normalized, oab_number=oab_number)
    if identity_key is None:
        return

    existing = lawyer_map.get(identity_key)
    if existing is not None:
        # Merge source systems
        if source_system not in existing["source_systems"]:
            existing["source_systems"].append(source_system)
        # Upgrade OAB if we have better data
        if oab_number and not existing.get("oab_number"):
            existing["oab_number"] = normalize_oab_number(oab_number)
            existing["oab_state"] = oab_state
            existing["oab_source"] = source_system
            existing["oab_validation_method"] = "regex"
            # Re-key if OAB is now available
            new_key = build_lawyer_identity_key(name=normalized, oab_number=oab_number)
            if new_key and new_key != identity_key:
                existing["identity_key"] = new_key
                existing["identity_strategy"] = _determine_identity_strategy(new_key)
                existing["lawyer_id"] = stable_id("law_", new_key)
                # Re-index: remove old key, insert under new key
                del lawyer_map[identity_key]
                occupant = lawyer_map.get(new_key)
                if occupant is not None:
                    _merge_lawyer_into(target=occupant, source=existing)
                else:
                    lawyer_map[new_key] = existing
        existing["updated_at"] = timestamp
        return

    oab_normalized = normalize_oab_number(oab_number)
    strategy = _determine_identity_strategy(identity_key)

    # Absorb existing name-keyed record for the same person (avoids duplicate)
    name_key = f"name:{canonical}" if canonical else None
    name_occupant = lawyer_map.pop(name_key, None) if (name_key and name_key != identity_key) else None

    new_record: dict[str, Any] = {
        "lawyer_id": stable_id("law_", identity_key),
        "lawyer_name_raw": raw_name,
        "lawyer_name_normalized": normalized,
        "canonical_name_normalized": canonical,
        "oab_number": oab_normalized,
        "oab_state": oab_state,
        "oab_status": None,
        "oab_source": source_system if oab_normalized else None,
        "oab_validation_method": "regex" if oab_normalized else None,
        "oab_last_checked_at": None,
        "entity_tax_id": None,
        "identity_key": identity_key,
        "identity_strategy": strategy,
        "firm_id": None,
        "source_systems": [source_system],
        "normalization_confidence": 1.0,
        "normalization_version": NORMALIZATION_VERSION,
        "first_seen_date": None,
        "last_seen_date": None,
        "notes": None,
        "created_at": timestamp,
        "updated_at": timestamp,
    }

    if name_occupant is not None:
        _merge_lawyer_into(target=new_record, source=name_occupant)

    lawyer_map[identity_key] = new_record
