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
DEFAULT_OABSP_DIR = Path("data/raw/oab_sp")


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


def _load_oab_sp_details(oab_sp_dir: Path) -> dict[str, dict[str, Any]]:
    """Load OAB/SP society details keyed by registration_number."""
    path = oab_sp_dir / "sociedade_detalhe.jsonl"
    if not path.exists():
        return {}
    details: dict[str, dict[str, Any]] = {}
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                record = json.loads(line)
                reg = record.get("registration_number")
                if reg:
                    details[str(reg)] = record
    logger.info("Loaded %d OAB/SP society details from %s", len(details), path)
    return details


def build_law_firm_entity_records(
    process_path: Path,
    portal_dir: Path,
    curated_dir: Path,
    deoab_dir: Path = DEFAULT_DEOAB_DIR,
    oab_sp_dir: Path = DEFAULT_OABSP_DIR,
) -> list[dict[str, Any]]:
    """Build deduplicated law firm entity records.

    Sources:
    1. Portal representantes with firm_name (currently empty)
    2. DEOAB OAB→sociedade links (primary source)
    3. OAB/SP society details (enrichment of DEOAB firms)
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

    # Source 3: OAB/SP society details (enrichment — cadastral)
    oab_sp_details = _load_oab_sp_details(oab_sp_dir)
    if oab_sp_details:
        enriched = 0
        for firm in firm_map.values():
            cnsa = firm.get("cnsa_number")
            if not cnsa:
                continue
            detail = oab_sp_details.get(cnsa)
            if not detail:
                continue
            firm["oab_sp_firm_name"] = detail.get("firm_name")
            firm["address"] = detail.get("address")
            firm["neighborhood"] = detail.get("neighborhood")
            firm["zip_code"] = detail.get("zip_code")
            firm["city"] = detail.get("city")
            firm["state"] = detail.get("state")
            firm["email"] = detail.get("email")
            firm["phone"] = detail.get("phone")
            firm["society_type"] = detail.get("society_type")
            if "oab_sp" not in firm["source_systems"]:
                firm["source_systems"].append("oab_sp")
            firm["updated_at"] = timestamp
            enriched += 1
        logger.info("Enriched %d firms with OAB/SP data", enriched)

    # Source 4: OAB/SP lawyer lookup (member_lawyer_ids via "Sócio de" param)
    _enrich_member_lawyer_ids(firm_map, oab_sp_dir, oab_sp_details, curated_dir, timestamp)

    records = sorted(firm_map.values(), key=lambda item: item["firm_id"])
    validate_records(records, SCHEMA_PATH)
    return records


def _enrich_member_lawyer_ids(
    firm_map: dict[str, dict[str, Any]],
    oab_sp_dir: Path,
    oab_sp_details: dict[str, dict[str, Any]],
    curated_dir: Path,
    timestamp: str,
) -> None:
    """Populate member_lawyer_ids on firms using advogado_consulta.jsonl.

    Links advogados → firms via the "Sócio de" param from the OAB/SP
    inscritos search, matching against sociedade_detalhe.jsonl param values.
    """
    consulta_path = oab_sp_dir / "advogado_consulta.jsonl"
    if not consulta_path.exists():
        return

    # Build param → cnsa_number mapping from raw details
    param_to_cnsa: dict[str, str] = {}
    for reg, detail in oab_sp_details.items():
        param = detail.get("oab_sp_param")
        if param:
            param_to_cnsa[str(param)] = str(reg)

    if not param_to_cnsa:
        return

    # Build cnsa_number → identity_key mapping from firm_map
    cnsa_to_key: dict[str, str] = {}
    for key, firm in firm_map.items():
        cnsa = firm.get("cnsa_number")
        if cnsa:
            cnsa_to_key[cnsa] = key

    # Load lawyer_entity to map oab_number → lawyer_id
    lawyer_oab_to_id: dict[str, str] = {}
    lawyer_path = curated_dir / "lawyer_entity.jsonl"
    if lawyer_path.exists():
        with lawyer_path.open(encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                oab = rec.get("oab_number")
                if oab and rec.get("oab_state") == "SP":
                    lawyer_oab_to_id[oab] = rec["lawyer_id"]

    # Process advogado_consulta.jsonl
    linked = 0
    with consulta_path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            firm_param = record.get("firm_param")
            oab_number = record.get("oab_number")
            if not firm_param or not oab_number:
                continue

            # Find the firm via param → cnsa → identity_key
            cnsa = param_to_cnsa.get(firm_param)
            if not cnsa:
                continue
            firm_key = cnsa_to_key.get(cnsa)
            if not firm_key:
                continue
            firm = firm_map.get(firm_key)
            if not firm:
                continue

            # Find the lawyer_id
            lawyer_id = lawyer_oab_to_id.get(oab_number)
            if not lawyer_id:
                continue

            if lawyer_id not in firm["member_lawyer_ids"]:
                firm["member_lawyer_ids"].append(lawyer_id)
                firm["updated_at"] = timestamp
                linked += 1

    if linked:
        logger.info("Linked %d lawyer→firm memberships from OAB/SP lookup", linked)
