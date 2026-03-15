"""Corporate link timeline builder for temporal analysis."""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from ..core.identity import normalize_entity_name, stable_id
from ._match_helpers import (
    build_counsel_process_map,
    build_party_index,
    build_party_process_map,
    compute_favorable_rate_role_aware,
    read_jsonl,
)
from ._temporal_utils import _parse_rfb_date, _round


def _enrichment_fields(
    company_cnpj: str,
    estab_by_cnpj: dict[str, list[dict[str, Any]]],
    eg_by_cnpj: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Build optional enrichment fields for a corporate link timeline record."""
    establishments = estab_by_cnpj.get(company_cnpj, [])
    active = [e for e in establishments if e.get("situacao_cadastral") == "02"]
    hq = next((e for e in establishments if e.get("matriz_filial") == "1"), None)
    eg = eg_by_cnpj.get(company_cnpj, {})
    return {
        "establishment_count": len(establishments) if establishments else None,
        "active_establishment_count": len(active) if establishments else None,
        "headquarters_uf": hq.get("uf") if hq else None,
        "headquarters_cnae_label": hq.get("cnae_fiscal_label") if hq else None,
        "economic_group_id": eg.get("group_id"),
        "economic_group_member_count": eg.get("member_count"),
    }


def _minister_aliases(minister_bio_path: Path) -> dict[str, str]:
    if not minister_bio_path.exists():
        return {}
    payload = json.loads(minister_bio_path.read_text(encoding="utf-8"))
    aliases: dict[str, str] = {}
    for entry in payload.values():
        if not isinstance(entry, dict):
            continue
        minister_name = entry.get("minister_name")
        for raw_name in (minister_name, entry.get("civil_name")):
            normalized = normalize_entity_name(raw_name)
            if normalized:
                aliases[normalized] = str(minister_name)
    return aliases


def _counsel_index(counsel_path: Path) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for record in read_jsonl(counsel_path):
        normalized = normalize_entity_name(record.get("counsel_name_normalized") or record.get("counsel_name_raw"))
        if normalized:
            index.setdefault(normalized, record)
    return index


def _build_corporate_link_records(
    *,
    minister_bio_path: Path,
    party_path: Path,
    counsel_path: Path,
    process_party_link_path: Path,
    process_counsel_link_path: Path,
    decision_event_path: Path,
    rfb_dir: Path,
    generated_at: str,
) -> list[dict[str, Any]]:
    partner_path = rfb_dir / "partners_raw.jsonl"
    if not partner_path.exists():
        return []
    aliases = _minister_aliases(minister_bio_path)
    if not aliases:
        return []
    party_index = build_party_index(party_path)
    counsel_index = _counsel_index(counsel_path)
    party_id_to_name = {
        str(record.get("party_id", "")): normalized
        for normalized, record in party_index.items()
        if record.get("party_id")
    }
    counsel_id_to_name = {
        str(record.get("counsel_id", "")): normalized
        for normalized, record in counsel_index.items()
        if record.get("counsel_id")
    }
    party_process_map = build_party_process_map(process_party_link_path, party_id_to_name)
    counsel_process_map = build_counsel_process_map(process_counsel_link_path, counsel_id_to_name)
    rapporteur_map = {
        str(record.get("process_id")): str(record.get("current_rapporteur"))
        for record in read_jsonl(decision_event_path)
        if record.get("process_id") and record.get("current_rapporteur")
    }
    process_outcomes: dict[str, list[str]] = defaultdict(list)
    for record in read_jsonl(decision_event_path):
        process_id = record.get("process_id")
        if process_id and record.get("decision_progress"):
            process_outcomes[str(process_id)].append(str(record.get("decision_progress")))
    companies = {
        str(record.get("cnpj_basico")): record
        for record in (
            read_jsonl(rfb_dir / "companies_raw.jsonl") if (rfb_dir / "companies_raw.jsonl").exists() else []
        )
        if record.get("cnpj_basico")
    }
    # Optional enrichment: establishments + economic groups
    estab_by_cnpj: dict[str, list[dict[str, Any]]] = defaultdict(list)
    estab_path = rfb_dir / "establishments_raw.jsonl"
    if estab_path.exists():
        for record in read_jsonl(estab_path):
            cnpj = str(record.get("cnpj_basico", ""))
            if cnpj:
                estab_by_cnpj[cnpj].append(record)

    eg_by_cnpj: dict[str, dict[str, Any]] = {}
    eg_path_file = rfb_dir.parent / "analytics" / "economic_group.jsonl"
    if eg_path_file.exists():
        for record in read_jsonl(eg_path_file):
            for cnpj in record.get("member_cnpjs", []):
                eg_by_cnpj[cnpj] = record

    by_company: dict[str, list[dict[str, Any]]] = defaultdict(list)
    pj_links: dict[str, list[tuple[str, dict[str, Any]]]] = defaultdict(list)
    for partner in read_jsonl(partner_path):
        cnpj = str(partner.get("cnpj_basico", ""))
        if not cnpj:
            continue
        by_company[cnpj].append(partner)
        if str(partner.get("partner_type")) == "1" and partner.get("partner_cpf_cnpj"):
            pj_links[str(partner.get("partner_cpf_cnpj"))].append((cnpj, partner))

    def entity_from_partner(partner: dict[str, Any]) -> tuple[str, str, str] | None:
        names = [
            normalize_entity_name(partner.get("partner_name_normalized") or partner.get("partner_name")),
            normalize_entity_name(partner.get("representative_name_normalized") or partner.get("representative_name")),
        ]
        for normalized in names:
            if not normalized:
                continue
            if normalized in party_index:
                return "party", str(party_index[normalized].get("party_id", "")), normalized
            if normalized in counsel_index:
                return "counsel", str(counsel_index[normalized].get("counsel_id", "")), normalized
        return None

    def shared_stats(minister_name: str, entity_name: str, entity_type: str) -> tuple[int, int, int, float | None]:
        outcomes_with_roles: list[tuple[str, str | None]] = []
        if entity_type == "party":
            links = party_process_map.get(entity_name, [])
            for process_id, role in links:
                if rapporteur_map.get(process_id) == minister_name:
                    outcomes_with_roles.extend((progress, role) for progress in process_outcomes.get(process_id, []))
        else:
            links = counsel_process_map.get(entity_name, [])
            for process_id, _side in links:
                if rapporteur_map.get(process_id) == minister_name:
                    outcomes_with_roles.extend((progress, None) for progress in process_outcomes.get(process_id, []))
        favorable = sum(
            1 for progress, role in outcomes_with_roles if compute_favorable_rate_role_aware([(progress, role)]) == 1.0
        )
        total = len(outcomes_with_roles)
        unfavorable = total - favorable
        return total, favorable, unfavorable, compute_favorable_rate_role_aware(outcomes_with_roles)

    records: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str, int]] = set()
    for company_cnpj, partners in by_company.items():
        minister_partners = [
            partner
            for partner in partners
            if normalize_entity_name(partner.get("partner_name_normalized") or partner.get("partner_name")) in aliases
        ]
        if not minister_partners:
            continue
        for minister_partner in minister_partners:
            minister_key = (
                normalize_entity_name(
                    minister_partner.get("partner_name_normalized") or minister_partner.get("partner_name")
                )
                or ""
            )
            minister_name = aliases[minister_key]
            minister_entry = _parse_rfb_date(minister_partner.get("entry_date"))
            for partner in partners:
                entity = entity_from_partner(partner)
                if entity is None:
                    continue
                entity_type, entity_id, entity_name = entity
                key = (minister_name, entity_type, entity_id, company_cnpj, 1)
                if key in seen:
                    continue
                seen.add(key)
                entity_entry = _parse_rfb_date(partner.get("entry_date"))
                valid_dates = [value for value in (minister_entry, entity_entry) if value is not None]
                total, favorable, unfavorable, favorable_rate = shared_stats(minister_name, entity_name, entity_type)
                records.append(
                    {
                        "analysis_kind": "corporate_link_timeline",
                        "record_id": stable_id("tmp_", "|".join(map(str, key))),
                        "rapporteur": minister_name,
                        "linked_entity_type": entity_type,
                        "linked_entity_id": entity_id,
                        "linked_entity_name": entity_name,
                        "company_cnpj_basico": company_cnpj,
                        "company_name": str(companies.get(company_cnpj, {}).get("razao_social", "")),
                        "link_degree": 1,
                        "link_start_date": (max(valid_dates).isoformat() if valid_dates else None),
                        "link_status": "ativo_desde_entrada",
                        "decision_count": total,
                        "favorable_count": favorable,
                        "unfavorable_count": unfavorable,
                        "favorable_rate": _round(favorable_rate),
                        "generated_at": generated_at,
                        **_enrichment_fields(company_cnpj, estab_by_cnpj, eg_by_cnpj),
                    }
                )
            for pj_partner in [
                row for row in partners if str(row.get("partner_type")) == "1" and row.get("partner_cpf_cnpj")
            ]:
                pj_cnpj = str(pj_partner.get("partner_cpf_cnpj"))
                for linked_company, linked_row in pj_links.get(pj_cnpj, []):
                    if linked_company == company_cnpj:
                        continue
                    for downstream in by_company.get(linked_company, []):
                        entity = entity_from_partner(downstream)
                        if entity is None:
                            continue
                        entity_type, entity_id, entity_name = entity
                        key = (minister_name, entity_type, entity_id, linked_company, 2)
                        if key in seen:
                            continue
                        seen.add(key)
                        dates = [
                            _parse_rfb_date(minister_partner.get("entry_date")),
                            _parse_rfb_date(pj_partner.get("entry_date")),
                            _parse_rfb_date(linked_row.get("entry_date")),
                            _parse_rfb_date(downstream.get("entry_date")),
                        ]
                        valid_dates = [value for value in dates if value is not None]
                        total, favorable, unfavorable, favorable_rate = shared_stats(
                            minister_name, entity_name, entity_type
                        )
                        records.append(
                            {
                                "analysis_kind": "corporate_link_timeline",
                                "record_id": stable_id("tmp_", "|".join(map(str, key))),
                                "rapporteur": minister_name,
                                "linked_entity_type": entity_type,
                                "linked_entity_id": entity_id,
                                "linked_entity_name": entity_name,
                                "company_cnpj_basico": linked_company,
                                "company_name": str(companies.get(linked_company, {}).get("razao_social", "")),
                                "link_degree": 2,
                                "link_chain": f"{company_cnpj}->{pj_cnpj}->{linked_company}",
                                "link_start_date": (max(valid_dates).isoformat() if valid_dates else None),
                                "link_status": "ativo_desde_entrada",
                                "decision_count": total,
                                "favorable_count": favorable,
                                "unfavorable_count": unfavorable,
                                "favorable_rate": _round(favorable_rate),
                                "generated_at": generated_at,
                                **_enrichment_fields(linked_company, estab_by_cnpj, eg_by_cnpj),
                            }
                        )
    return records
