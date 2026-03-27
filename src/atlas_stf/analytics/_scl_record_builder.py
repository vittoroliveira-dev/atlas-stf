"""Link record assembly helpers for sanction corporate links."""

from __future__ import annotations

from typing import Any

_MAX_CNPJS_PER_SANCTION = 5000


def _build_evidence_chain(
    *,
    sanction_source: str,
    sanction_entity_name: str,
    raw_doc: str,
    link_basis: str,
    bridge_company_name: str,
    bridge_cnpj: str,
    scan_cnpj: str,
    company_index: dict[str, dict[str, Any]],
    establishment_index: dict[str, dict[str, Any]],
    partner_name: str,
    stf_entity_type: str,
    match_strategy: str,
    match_score: float | None,
) -> list[str]:
    """Build the human-readable evidence chain for a link record."""
    chain: list[str] = [f"Sanção {sanction_source.upper()}: {sanction_entity_name} ({raw_doc})"]
    if link_basis == "exact_cnpj_basico":
        chain.append(f"→ Empresa {bridge_company_name} (CNPJ base {bridge_cnpj})")
    elif link_basis == "exact_partner_cnpj":
        chain.append(f"→ Sócio PJ em {bridge_company_name} (CNPJ base {bridge_cnpj})")
    else:
        chain.append(f"→ Sócio PF em {bridge_company_name} (CNPJ base {bridge_cnpj})")
    if scan_cnpj != bridge_cnpj:
        scan_company = company_index.get(scan_cnpj, {})
        chain.append(f"→ Grupo econômico: {scan_company.get('razao_social', scan_cnpj)}")
    match_desc = f"match: {match_strategy}"
    if match_score is not None:
        match_desc += f", score {match_score}"
    chain.append(f"→ Co-sócio {partner_name} = {stf_entity_type} STF ({match_desc})")
    establishment = establishment_index.get(bridge_cnpj)
    if establishment:
        if hq_uf := establishment.get("uf"):
            chain.append(f"  Sede: {hq_uf}")
        if hq_cnae := establishment.get("cnae_fiscal"):
            chain.append(f"  CNAE: {hq_cnae}")
    return chain


def _build_link_record(
    *,
    link_id: str,
    sanction_id: str,
    sanction_source: str,
    sanction_entity_name: str,
    raw_doc: str,
    sanction_type: str,
    bridge_cnpj: str,
    bridge_company_name: str,
    link_basis: str,
    truncated: bool,
    bridge_partner: dict[str, Any] | None,
    co_partner: dict[str, Any],
    eg: dict[str, Any] | None,
    stf_entity_type: str,
    stf_entity_id: str,
    stf_entity_name: str,
    match_result: Any,
    link_degree: int,
    stf_process_count: int,
    favorable_rate: float | None,
    baseline_rate: float | None,
    delta: float | None,
    risk_score: float | None,
    red_flag: bool,
    power: float | None,
    confidence: str | None,
    evidence_chain: list[str],
    pre_truncation_count: int,
    unified_cnpj_count: int,
    estimated_degree3: int,
    now_iso: str,
) -> dict[str, Any]:
    """Assemble a complete link record dict."""
    return {
        "link_id": link_id,
        "sanction_id": sanction_id,
        "sanction_source": sanction_source,
        "sanction_entity_name": sanction_entity_name,
        "sanction_entity_tax_id": raw_doc,
        "sanction_type": sanction_type,
        "bridge_company_cnpj_basico": bridge_cnpj,
        "bridge_company_name": bridge_company_name,
        "bridge_link_basis": link_basis,
        "bridge_confidence": "deterministic" if not truncated else "truncated",
        "bridge_partner_role": (bridge_partner.get("qualification_label") if bridge_partner else None),
        "bridge_qualification_code": co_partner.get("qualification_code"),
        "bridge_qualification_label": co_partner.get("qualification_label"),
        "economic_group_id": eg.get("group_id") if eg else None,
        "economic_group_member_count": eg.get("member_count") if eg else None,
        "is_law_firm_group": eg.get("is_law_firm_group") if eg else None,
        "stf_entity_type": stf_entity_type,
        "stf_entity_id": stf_entity_id,
        "stf_entity_name": stf_entity_name,
        "stf_match_strategy": match_result.strategy,
        "stf_match_score": match_result.score,
        "stf_match_confidence": (
            "deterministic" if match_result.strategy == "tax_id"
            else "exact_name" if match_result.strategy in {"exact", "canonical_name", "alias"}
            else "fuzzy"
        ),
        "matched_alias": match_result.matched_alias,
        "matched_tax_id": match_result.matched_tax_id,
        "uncertainty_note": match_result.uncertainty_note,
        "link_degree": link_degree,
        "stf_process_count": stf_process_count,
        "favorable_rate": favorable_rate,
        "baseline_favorable_rate": baseline_rate,
        "favorable_rate_delta": delta,
        "risk_score": risk_score,
        "red_flag": red_flag,
        "red_flag_power": power,
        "red_flag_confidence": confidence,
        "evidence_chain": evidence_chain,
        "source_datasets": sorted({"rfb_socios", sanction_source, *(["economic_group"] if eg else [])} - {""}),
        "truncated": truncated,
        "pre_truncation_cnpj_count": pre_truncation_count if truncated else None,
        # Campos de contexto da sanção, denormalizados por record de propósito.
        # Repetidos identicamente em todos os records da mesma sanção.
        "truncation_reason": (
            f"estimated_{pre_truncation_count}_cnpjs_exceeds_{_MAX_CNPJS_PER_SANCTION}_limit"
            if truncated else None
        ),
        "post_truncation_cnpj_count": unified_cnpj_count if truncated else None,
        "estimated_degree3_count": estimated_degree3,
        "generated_at": now_iso,
    }
