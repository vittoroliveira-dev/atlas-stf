"""Loaders for corporate conflicts and economic groups."""

from __future__ import annotations

import json
from pathlib import Path

from ._builder_utils import (
    _coerce_bool,
    _coerce_float,
    _coerce_int,
    _parse_datetime,
    _read_jsonl,
)
from .models import ServingCorporateConflict, ServingEconomicGroup


def load_corporate_conflicts(analytics_dir: Path) -> list[ServingCorporateConflict]:
    cn_path = analytics_dir / "corporate_network.jsonl"
    if not cn_path.exists():
        return []
    results: list[ServingCorporateConflict] = []
    seen: set[str] = set()
    for record in _read_jsonl(cn_path):
        cid = str(record.get("conflict_id", ""))
        if cid in seen:
            continue
        seen.add(cid)
        results.append(
            ServingCorporateConflict(
                conflict_id=cid,
                minister_name=str(record.get("minister_name", "")),
                company_cnpj_basico=str(record.get("company_cnpj_basico", "")),
                company_name=str(record.get("company_name", "")),
                minister_qualification=record.get("minister_qualification"),
                linked_entity_type=str(record.get("linked_entity_type", "")),
                linked_entity_id=str(record.get("linked_entity_id", "")),
                linked_entity_name=str(record.get("linked_entity_name", "")),
                entity_qualification=record.get("entity_qualification"),
                shared_process_ids_json=json.dumps(record.get("shared_process_ids", []), ensure_ascii=False),
                shared_process_count=_coerce_int(record.get("shared_process_count")),
                favorable_rate=record.get("favorable_rate"),
                baseline_favorable_rate=record.get("baseline_favorable_rate"),
                favorable_rate_delta=record.get("favorable_rate_delta"),
                risk_score=record.get("risk_score"),
                decay_factor=record.get("decay_factor"),
                red_flag=_coerce_bool(record.get("red_flag")),
                link_chain=record.get("link_chain"),
                link_degree=_coerce_int(record.get("link_degree", 1)),
                generated_at=_parse_datetime(record.get("generated_at")),
                # Decoded labels
                minister_qualification_label=record.get("minister_qualification_label"),
                entity_qualification_label=record.get("entity_qualification_label"),
                company_natureza_juridica_label=record.get("company_natureza_juridica_label"),
                # Multi-establishment
                establishment_count=record.get("establishment_count"),
                active_establishment_count=record.get("active_establishment_count"),
                headquarters_uf=record.get("headquarters_uf"),
                headquarters_municipio_label=record.get("headquarters_municipio_label"),
                headquarters_cnae_fiscal=record.get("headquarters_cnae_fiscal"),
                headquarters_cnae_label=record.get("headquarters_cnae_label"),
                headquarters_situacao_cadastral=record.get("headquarters_situacao_cadastral"),
                headquarters_motivo_situacao_label=record.get("headquarters_motivo_situacao_label"),
                establishment_ufs_json=json.dumps(record.get("establishment_ufs", []), ensure_ascii=False),
                establishment_cnaes_json=json.dumps(record.get("establishment_cnaes", []), ensure_ascii=False),
                establishment_cnae_labels_json=json.dumps(
                    record.get("establishment_cnae_labels", []), ensure_ascii=False
                ),
                key_establishments_json=json.dumps(record.get("key_establishments", []), ensure_ascii=False),
                # Economic group
                economic_group_id=record.get("economic_group_id"),
                economic_group_member_count=record.get("economic_group_member_count"),
                economic_group_razoes_sociais_json=json.dumps(
                    record.get("economic_group_razoes_sociais", []), ensure_ascii=False
                ),
                # Provenance
                evidence_type=record.get("evidence_type"),
                source_dataset=record.get("source_dataset"),
                source_snapshot=record.get("source_snapshot"),
                evidence_strength=record.get("evidence_strength"),
                # Substantive
                favorable_rate_substantive=_coerce_float(record.get("favorable_rate_substantive")),
                substantive_decision_count=record.get("substantive_decision_count"),
                red_flag_substantive=record.get("red_flag_substantive"),
            )
        )
    return results


def load_economic_groups(analytics_dir: Path) -> list[ServingEconomicGroup]:
    eg_path = analytics_dir / "economic_group.jsonl"
    if not eg_path.exists():
        return []
    results: list[ServingEconomicGroup] = []
    seen: set[str] = set()
    for record in _read_jsonl(eg_path):
        gid = str(record.get("group_id", ""))
        if gid in seen:
            continue
        seen.add(gid)
        results.append(
            ServingEconomicGroup(
                group_id=gid,
                member_cnpjs_json=json.dumps(record.get("member_cnpjs", []), ensure_ascii=False),
                razoes_sociais_json=json.dumps(record.get("razoes_sociais", []), ensure_ascii=False),
                member_count=_coerce_int(record.get("member_count")),
                total_capital_social=_coerce_float(record.get("total_capital_social")),
                cnae_labels_json=json.dumps(record.get("cnae_labels", []), ensure_ascii=False),
                ufs_json=json.dumps(record.get("ufs", []), ensure_ascii=False),
                active_establishment_count=_coerce_int(record.get("active_establishment_count")),
                total_establishment_count=_coerce_int(record.get("total_establishment_count")),
                is_law_firm_group=_coerce_bool(record.get("is_law_firm_group")),
                has_minister_partner=_coerce_bool(record.get("has_minister_partner")),
                has_party_partner=_coerce_bool(record.get("has_party_partner")),
                has_counsel_partner=_coerce_bool(record.get("has_counsel_partner")),
                generated_at=_parse_datetime(record.get("generated_at")),
            )
        )
    return results
