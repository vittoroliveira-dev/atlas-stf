"""Corporate enrichment for donation matches.

Loads donor_corporate_link, economic_group, and corporate_network artifacts
and annotates each donation match in-place with corporate identity, group
membership, and network proximity fields.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Priority order for link_basis (descending)
_LINK_BASIS_PRIORITY: dict[str, int] = {
    "exact_cnpj_basico": 0,
    "exact_partner_cnpj": 1,
    "exact_partner_cpf": 2,
}


@dataclass
class CorporateEnrichmentIndex:
    """Pre-built indices for corporate enrichment lookups."""

    # donor_identity_key → sorted list of deterministic links
    donor_links: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    # company_cnpj_basico → economic group record
    cnpj_to_group: dict[str, dict[str, Any]] = field(default_factory=dict)
    # company_cnpj_basico → list of corporate_network records
    cnpj_to_network: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    # Artifact presence flags
    has_corporate_links: bool = False
    has_economic_groups: bool = False
    has_corporate_network: bool = False


def build_corporate_enrichment_index(analytics_dir: Path) -> CorporateEnrichmentIndex:
    """Build lookup indices from analytics artifacts.

    Loads donor_corporate_link.jsonl, economic_group.jsonl, and
    corporate_network.jsonl if they exist.  Only deterministic links
    are indexed.
    """
    index = CorporateEnrichmentIndex()

    # --- donor_corporate_link ---
    dcl_path = analytics_dir / "donor_corporate_link.jsonl"
    if not dcl_path.exists():
        logger.debug("donor_corporate_link.jsonl not found — skipping corporate enrichment")
        return index

    index.has_corporate_links = True
    raw_links: dict[str, dict[tuple[str, str | None, str], dict[str, Any]]] = {}
    for record in _iter_jsonl(dcl_path):
        if record.get("confidence") != "deterministic":
            continue
        dk = record.get("donor_identity_key", "")
        if not dk:
            continue
        cnpj = record.get("company_cnpj_basico") or ""
        basis = record.get("link_basis", "")
        dedup_key = (dk, cnpj, basis)
        if dk not in raw_links:
            raw_links[dk] = {}
        if dedup_key not in raw_links[dk]:
            raw_links[dk][dedup_key] = record

    # Sort by link_basis priority
    for dk, links_map in raw_links.items():
        sorted_links = sorted(
            links_map.values(),
            key=lambda r: _LINK_BASIS_PRIORITY.get(r.get("link_basis", ""), 99),
        )
        index.donor_links[dk] = sorted_links

    link_count = sum(len(v) for v in index.donor_links.values())
    logger.info(
        "Corporate enrichment: %d deterministic links for %d donors",
        link_count,
        len(index.donor_links),
    )

    # --- economic_group ---
    eg_path = analytics_dir / "economic_group.jsonl"
    if eg_path.exists():
        index.has_economic_groups = True
        for record in _iter_jsonl(eg_path):
            member_cnpjs = record.get("member_cnpjs", [])
            for cnpj in member_cnpjs:
                if cnpj:
                    index.cnpj_to_group[cnpj] = record
        logger.info("Corporate enrichment: %d CNPJ→group mappings", len(index.cnpj_to_group))

    # --- corporate_network ---
    cn_path = analytics_dir / "corporate_network.jsonl"
    if cn_path.exists():
        index.has_corporate_network = True
        for record in _iter_jsonl(cn_path):
            cnpj = record.get("company_cnpj_basico", "")
            if cnpj:
                if cnpj not in index.cnpj_to_network:
                    index.cnpj_to_network[cnpj] = []
                index.cnpj_to_network[cnpj].append(record)
        logger.info("Corporate enrichment: %d CNPJ→network mappings", len(index.cnpj_to_network))

    return index


def enrich_match_corporate(match: dict[str, Any], index: CorporateEnrichmentIndex) -> None:
    """Annotate a donation match dict in-place with corporate enrichment fields.

    If no deterministic links exist for the donor, all 12 fields are set to None.
    """
    dk = match.get("donor_identity_key", "")
    links = index.donor_links.get(dk, [])

    if not links:
        _set_all_none(match)
        return

    # --- Identity fields (from first link in priority order) ---
    first = links[0]
    match["donor_document_type"] = first.get("donor_document_type")
    match["donor_tax_id_normalized"] = first.get("donor_tax_id_normalized")

    # donor_cnpj_basico and donor_company_name: only from Path A (exact_cnpj_basico)
    path_a = next((lk for lk in links if lk.get("link_basis") == "exact_cnpj_basico"), None)
    match["donor_cnpj_basico"] = path_a.get("donor_cnpj_basico") if path_a else None
    match["donor_company_name"] = path_a.get("company_name") if path_a else None

    # --- Collect all company_cnpj_basico from links ---
    all_cnpjs: list[str] = []
    for lk in links:
        cnpj = lk.get("company_cnpj_basico")
        if cnpj and cnpj not in all_cnpjs:
            all_cnpjs.append(cnpj)

    # --- Economic group (largest member_count, tie-break by smallest group_id) ---
    best_group: dict[str, Any] | None = None
    for cnpj in all_cnpjs:
        group = index.cnpj_to_group.get(cnpj)
        if group is None:
            continue
        if best_group is None:
            best_group = group
        else:
            g_count = group.get("member_count", 0)
            b_count = best_group.get("member_count", 0)
            g_id = str(group.get("group_id", ""))
            b_id = str(best_group.get("group_id", ""))
            if g_count > b_count or (g_count == b_count and g_id < b_id):
                best_group = group

    match["economic_group_id"] = best_group.get("group_id") if best_group else None
    match["economic_group_member_count"] = best_group.get("member_count") if best_group else None
    match["is_law_firm_group"] = best_group.get("is_law_firm_group") if best_group else None

    # --- Group flags: OR across all linked groups ---
    has_minister = False
    has_party = False
    has_counsel = False
    any_group_found = False
    for cnpj in all_cnpjs:
        group = index.cnpj_to_group.get(cnpj)
        if group is None:
            continue
        any_group_found = True
        if group.get("has_minister_partner"):
            has_minister = True
        if group.get("has_party_partner"):
            has_party = True
        if group.get("has_counsel_partner"):
            has_counsel = True

    match["donor_group_has_minister_partner"] = has_minister if any_group_found else None
    match["donor_group_has_party_partner"] = has_party if any_group_found else None
    match["donor_group_has_counsel_partner"] = has_counsel if any_group_found else None

    # --- Corporate network: min link_degree, OR red_flag ---
    min_degree: int | None = None
    any_red_flag = False
    any_network_found = False
    for cnpj in all_cnpjs:
        net_records = index.cnpj_to_network.get(cnpj, [])
        for nr in net_records:
            any_network_found = True
            degree = nr.get("link_degree")
            if isinstance(degree, int):
                if min_degree is None or degree < min_degree:
                    min_degree = degree
            if nr.get("red_flag"):
                any_red_flag = True

    match["min_link_degree_to_minister"] = min_degree if any_network_found else None
    match["corporate_link_red_flag"] = any_red_flag if any_network_found else None


def _set_all_none(match: dict[str, Any]) -> None:
    """Set all 12 corporate enrichment fields to None."""
    match["donor_document_type"] = None
    match["donor_tax_id_normalized"] = None
    match["donor_cnpj_basico"] = None
    match["donor_company_name"] = None
    match["economic_group_id"] = None
    match["economic_group_member_count"] = None
    match["is_law_firm_group"] = None
    match["donor_group_has_minister_partner"] = None
    match["donor_group_has_party_partner"] = None
    match["donor_group_has_counsel_partner"] = None
    match["min_link_degree_to_minister"] = None
    match["corporate_link_red_flag"] = None


def _iter_jsonl(path: Path):
    """Yield JSONL records one at a time."""
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)
