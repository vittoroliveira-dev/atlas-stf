"""Gold set record schema: building and validation."""

from __future__ import annotations

from datetime import datetime, timezone

from ._constants import ADJUDICATION_TYPES, VALID_LABELS


def build_record(
    *,
    rec: dict,
    stratum: str,
    case_id: str,
    heuristic_label: str,
    heuristic_basis: str,
    justification: str,
    adjudication_type: str,
    source_file: str,
) -> dict:
    """Build a gold set record with full provenance and two-layer labeling."""
    is_scl = source_file == "sanction_corporate_link.jsonl"
    is_sanction = source_file == "sanction_match.jsonl"
    is_ambiguous = source_file == "donation_match_ambiguous.jsonl"

    if is_scl:
        donor_name = rec.get("sanction_entity_name", "")
        entity_name = rec.get("stf_entity_name", "")
        entity_id = rec.get("stf_entity_id", "")
        donor_key = f"sanction:{rec.get('sanction_source', '')}:{rec.get('sanction_id', '')}"
        match_strategy = rec.get("stf_match_strategy", "")
        match_score = rec.get("stf_match_score")
        has_tax_id = bool(rec.get("matched_tax_id"))
        match_id = rec.get("link_id")
    elif is_sanction:
        donor_name = ""
        entity_name = rec.get("entity_name_normalized") or rec.get("party_name_normalized", "")
        entity_id = rec.get("entity_id") or rec.get("party_id", "")
        donor_key = f"sanction:{rec.get('sanction_source', '')}:{rec.get('sanction_id', '')}"
        match_strategy = rec.get("match_strategy", "")
        match_score = rec.get("match_score")
        has_tax_id = bool(rec.get("matched_tax_id"))
        match_id = rec.get("match_id")
    elif is_ambiguous:
        donor_name = rec.get("donor_name_normalized", "")
        entity_name = rec.get("sample_candidate_name")
        entity_id = None
        donor_key = rec.get("donor_identity_key", "")
        match_strategy = rec.get("match_strategy", "")
        match_score = rec.get("match_score")
        has_tax_id = bool(rec.get("donor_cpf_cnpj"))
        match_id = None
    else:
        donor_name = rec.get("donor_name_normalized", "")
        entity_name = rec.get("entity_name_normalized") or rec.get("party_name_normalized", "")
        entity_id = rec.get("entity_id") or rec.get("party_id", "")
        donor_key = rec.get("donor_identity_key", "")
        match_strategy = rec.get("match_strategy", "")
        match_score = rec.get("match_score")
        has_tax_id = bool(rec.get("donor_cpf_cnpj") or rec.get("matched_tax_id"))
        match_id = rec.get("match_id")

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    gold: dict = {
        "case_id": case_id,
        "stratum": stratum,
        "source": source_file,
        "match_id": match_id,
        "donor_identity_key": donor_key,
        "donor_name": donor_name,
        "entity_id": entity_id,
        "entity_name": entity_name,
        "match_strategy": match_strategy,
        "match_score": match_score,
        "has_tax_id": has_tax_id,
        # Two-layer labeling
        "heuristic_label": heuristic_label,
        "heuristic_basis": heuristic_basis,
        "final_label": heuristic_label if adjudication_type == "evidence_deterministic" else None,
        "adjudication_type": adjudication_type,
        "adjudicator": "algorithmic_v2" if adjudication_type != "human_review" else None,
        "adjudication_evidence": justification,
        "adjudication_date": now if adjudication_type == "evidence_deterministic" else None,
        "labeling_rule": f"scripts/gold_set/_labeling.py::{heuristic_basis}",
    }

    # SCL-specific fields
    if is_scl:
        gold["bridge_company"] = rec.get("bridge_company_name")
        gold["link_degree"] = rec.get("link_degree")
        gold["evidence_chain"] = rec.get("evidence_chain")

    # Sanction-specific fields
    if is_sanction:
        gold["sanction_source"] = rec.get("sanction_source")
        gold["sanction_id"] = rec.get("sanction_id")

    # Ambiguous-specific fields
    if is_ambiguous:
        gold["candidate_count"] = rec.get("candidate_count")
        gold["sample_candidate"] = rec.get("sample_candidate_name")

    return gold


def validate_record(rec: dict) -> list[str]:
    """Validate a gold set record. Returns list of errors (empty = valid)."""
    errors: list[str] = []
    if not rec.get("case_id"):
        errors.append("missing case_id")
    if not rec.get("stratum"):
        errors.append("missing stratum")
    if not rec.get("source"):
        errors.append("missing source")

    hl = rec.get("heuristic_label")
    if hl and hl not in VALID_LABELS:
        errors.append(f"invalid heuristic_label: {hl}")

    fl = rec.get("final_label")
    if fl and fl not in VALID_LABELS:
        errors.append(f"invalid final_label: {fl}")

    at = rec.get("adjudication_type")
    if at and at not in ADJUDICATION_TYPES:
        errors.append(f"invalid adjudication_type: {at}")

    return errors
