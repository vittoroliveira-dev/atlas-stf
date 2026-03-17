"""Build canonical law firm entity records from portal and DEOAB data.

Sources:
- Portal JSONL files: representantes with firm_name (low confidence)
- DEOAB JSONL: OAB→sociedade links from OAB Electronic Gazette (high confidence)

firm_id = stable_id("firm_", identity_key).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from ..core.identity import (
    build_firm_identity_key,
    canonicalize_entity_name,
    normalize_entity_name,
    stable_id,
)
from ..schema_validate import validate_records

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path("schemas/law_firm_entity.schema.json")


def _determine_identity_strategy(identity_key: str) -> str:
    """Determine identity strategy from the key prefix."""
    if identity_key.startswith("tax:"):
        return "cnpj"
    if identity_key.startswith("cnsa:"):
        return "cnsa"
    return "name"


def _load_portal_firm_names(portal_dir: Path) -> list[dict[str, Any]]:
    """Load portal JSONL/JSON files and extract firm names from representantes."""
    if not portal_dir.exists():
        return []
    records: list[dict[str, Any]] = []
    for path in sorted(portal_dir.glob("*.json")):
        try:
            with path.open(encoding="utf-8") as fh:
                doc = json.load(fh)
            for rep in doc.get("representantes", []):
                firm_name = rep.get("firm_name")
                if firm_name:
                    records.append(
                        {
                            "firm_name": firm_name,
                            "affiliation_confidence": rep.get("affiliation_confidence", "low"),
                            "_portal_process_number": doc.get("process_number"),
                        }
                    )
        except json.JSONDecodeError, ValueError:
            logger.warning("Skipping invalid portal file: %s", path)
    return records


DEFAULT_DEOAB_DIR = Path("data/raw/deoab")


def _load_deoab_firms(deoab_dir: Path) -> list[dict[str, Any]]:
    """Load DEOAB OAB→sociedade vinculos from JSONL."""
    path = deoab_dir / "oab_sociedade_vinculo.jsonl"
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    logger.info("Loaded %d DEOAB firm records from %s", len(records), path)
    return records


def build_law_firm_entity_records(
    process_path: Path,
    portal_dir: Path,
    curated_dir: Path,
    deoab_dir: Path = DEFAULT_DEOAB_DIR,
) -> list[dict[str, Any]]:
    """Build deduplicated law firm entity records.

    Sources:
    1. Portal representantes with firm_name (currently empty)
    2. DEOAB OAB→sociedade links (primary source)
    """
    from .common import utc_now_iso

    portal_firms = _load_portal_firm_names(portal_dir)
    deoab_firms = _load_deoab_firms(deoab_dir)
    timestamp = utc_now_iso()

    firm_map: dict[str, dict[str, Any]] = {}

    # Source 1: Portal
    for entry in portal_firms:
        firm_name_raw = entry["firm_name"]
        normalized = normalize_entity_name(firm_name_raw)
        if normalized is None:
            continue
        canonical = canonicalize_entity_name(normalized)
        identity_key = build_firm_identity_key(name=normalized)
        if identity_key is None:
            continue

        existing = firm_map.get(identity_key)
        if existing is not None:
            if "portal_stf" not in existing["source_systems"]:
                existing["source_systems"].append("portal_stf")
            existing["updated_at"] = timestamp
            continue

        strategy = _determine_identity_strategy(identity_key)

        firm_map[identity_key] = {
            "firm_id": stable_id("firm_", identity_key),
            "firm_name_raw": firm_name_raw,
            "firm_name_normalized": normalized,
            "canonical_name_normalized": canonical,
            "cnpj": None,
            "cnpj_valid": None,
            "cnsa_number": None,
            "identity_key": identity_key,
            "identity_strategy": strategy,
            "source_systems": ["portal_stf"],
            "member_lawyer_ids": [],
            "created_at": timestamp,
            "updated_at": timestamp,
        }

    # Source 2: DEOAB
    for entry in deoab_firms:
        firm_name_raw = entry.get("sociedade_nome")
        if not firm_name_raw:
            continue
        normalized = normalize_entity_name(firm_name_raw)
        if normalized is None:
            continue
        canonical = canonicalize_entity_name(normalized)
        identity_key = build_firm_identity_key(name=normalized)
        if identity_key is None:
            continue

        existing = firm_map.get(identity_key)
        if existing is not None:
            if "deoab" not in existing["source_systems"]:
                existing["source_systems"].append("deoab")
            existing["updated_at"] = timestamp
            continue

        strategy = _determine_identity_strategy(identity_key)
        registro = entry.get("sociedade_registro")

        firm_map[identity_key] = {
            "firm_id": stable_id("firm_", identity_key),
            "firm_name_raw": firm_name_raw,
            "firm_name_normalized": normalized,
            "canonical_name_normalized": canonical,
            "cnpj": None,
            "cnpj_valid": None,
            "cnsa_number": registro,
            "identity_key": identity_key,
            "identity_strategy": strategy,
            "source_systems": ["deoab"],
            "member_lawyer_ids": [],
            "created_at": timestamp,
            "updated_at": timestamp,
        }

    records = sorted(firm_map.values(), key=lambda item: item["firm_id"])
    validate_records(records, SCHEMA_PATH)
    return records
