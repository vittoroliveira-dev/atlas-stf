"""Corporate network context dataclass and helper index builders."""

from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..core.identity import normalize_entity_name, stable_id
from ..core.stats import red_flag_confidence_label, red_flag_power
from ._match_helpers import (
    compute_favorable_rate_role_aware,
    compute_favorable_rate_substantive,
    lookup_baseline_rate,
    read_jsonl,
)
from ._match_helpers import (
    degree_decay as _degree_decay,
)

logger = logging.getLogger(__name__)

RED_FLAG_DELTA_THRESHOLD = 0.15
MIN_CASES_FOR_RED_FLAG = 3


@dataclass
class CorporateNetworkContext:
    """Holds all pre-built indices and data needed to compute conflict records."""

    company_index: dict[str, dict[str, Any]]
    partner_index: dict[str, list[dict[str, Any]]]
    process_party_map: dict[str, list[tuple[str, str | None]]]
    process_counsel_map: dict[str, list[tuple[str, str | None]]]
    process_outcomes: dict[str, list[str]]
    stratified_rates: Any
    fallback_rates: Any
    process_jb_map: dict[str, str]
    process_class_map: dict[str, str]
    rapporteur_map: dict[str, str]
    qualificacoes: dict[str, str]
    naturezas: dict[str, str]
    estab_index: dict[str, list[dict[str, Any]]]
    eg_index: dict[str, dict[str, Any]]
    now_iso: str
    red_flag_delta_threshold: float = field(default=RED_FLAG_DELTA_THRESHOLD)
    min_cases_for_red_flag: int = field(default=MIN_CASES_FOR_RED_FLAG)


def _load_minister_names(minister_bio_path: Path) -> dict[str, str]:
    """Load minister names from minister_bio.json. Returns normalized -> original.

    Includes both parliamentary name and civil_name as aliases.
    """
    if not minister_bio_path.exists():
        return {}
    data = json.loads(minister_bio_path.read_text(encoding="utf-8"))
    result: dict[str, str] = {}
    for _key, entry in data.items():
        name = entry.get("minister_name", "")
        if name:
            norm = normalize_entity_name(name)
            if norm:
                result[norm] = name
        civil = entry.get("civil_name", "")
        if civil:
            civil_norm = normalize_entity_name(civil)
            if civil_norm and civil_norm not in result:
                result[civil_norm] = name  # maps civil_name -> parliamentary name
    return result


def _build_counsel_index(counsel_path: Path) -> dict[str, dict[str, Any]]:
    """Index counsels by normalized name -> counsel record.

    When multiple counsels share the same normalized name, keeps the first
    and logs a warning with the collision count.
    """
    index: dict[str, dict[str, Any]] = {}
    collisions = 0
    for record in read_jsonl(counsel_path):
        norm = normalize_entity_name(record.get("counsel_name_normalized") or record.get("counsel_name_raw", ""))
        if norm:
            if norm in index:
                collisions += 1
            else:
                index[norm] = record
    if collisions:
        logger.warning("_build_counsel_index: %d name collisions (first record kept)", collisions)
    return index


def _build_partner_index(rfb_dir: Path) -> dict[str, list[dict[str, Any]]]:
    """Index partners by cnpj_basico -> list of partner records."""
    partners_path = rfb_dir / "partners_raw.jsonl"
    if not partners_path.exists():
        return {}
    index: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in read_jsonl(partners_path):
        cnpj = record.get("cnpj_basico", "")
        if cnpj:
            index[cnpj].append(record)
    return dict(index)


def _build_company_index(rfb_dir: Path) -> dict[str, dict[str, Any]]:
    """Index companies by cnpj_basico -> company record."""
    companies_path = rfb_dir / "companies_raw.jsonl"
    if not companies_path.exists():
        return {}
    index: dict[str, dict[str, Any]] = {}
    for record in read_jsonl(companies_path):
        cnpj = record.get("cnpj_basico", "")
        if cnpj:
            index.setdefault(cnpj, record)
    return dict(index)


def _build_name_to_cnpjs(
    partner_index: dict[str, list[dict[str, Any]]],
) -> dict[str, set[str]]:
    """Build name -> set of cnpj_basico from partner_name AND representative_name."""
    name_to_cnpjs: dict[str, set[str]] = defaultdict(set)
    for cnpj, partners in partner_index.items():
        for p in partners:
            norm = p.get("partner_name_normalized", "")
            if norm:
                name_to_cnpjs[norm].add(cnpj)
            rep_norm = p.get("representative_name_normalized", "")
            if rep_norm:
                name_to_cnpjs[rep_norm].add(cnpj)
    return dict(name_to_cnpjs)


def _build_pj_partner_index(
    partner_index: dict[str, list[dict[str, Any]]],
) -> dict[str, set[str]]:
    """Build PJ partner CNPJ -> set of companies where it participates.

    Only considers partner_type == '1' (PJ).
    """
    pj_index: dict[str, set[str]] = defaultdict(set)
    for cnpj, partners in partner_index.items():
        for p in partners:
            if p.get("partner_type") == "1":
                pj_cnpj = p.get("partner_cpf_cnpj", "").strip()
                if pj_cnpj:
                    pj_index[pj_cnpj].add(cnpj)
    return dict(pj_index)


def _estab_summary(e: dict[str, Any]) -> dict[str, str]:
    """Extract a summary dict from an establishment record."""
    return {
        "cnpj_full": e.get("cnpj_full", ""),
        "matriz_filial": e.get("matriz_filial", ""),
        "nome_fantasia": e.get("nome_fantasia", ""),
        "uf": e.get("uf", ""),
        "municipio_label": e.get("municipio_label", ""),
        "cnae_fiscal": e.get("cnae_fiscal", ""),
        "cnae_label": e.get("cnae_fiscal_label", ""),
        "situacao_cadastral": e.get("situacao_cadastral", ""),
        "data_inicio_atividade": e.get("data_inicio_atividade", ""),
    }


def _compute_conflict(
    ctx: CorporateNetworkContext,
    minister_name: str,
    minister_norm: str,
    cnpj: str,
    entity_name: str,
    entity_display_name: str,
    entity_type: str,
    entity_record: dict[str, Any],
    link_degree: int,
    link_chain: str,
    evidence_type: str,
    entity_qualification: str | None,
) -> dict[str, Any]:
    """Compute a single conflict record with outcome stats."""
    entity_id = entity_record.get("party_id" if entity_type == "party" else "counsel_id", "")

    company = ctx.company_index.get(cnpj, {})
    company_name = company.get("razao_social", "")

    minister_qual: str | None = None
    for p in ctx.partner_index.get(cnpj, []):
        p_norm = p.get("partner_name_normalized", "")
        if p_norm == minister_norm:
            minister_qual = p.get("qualification_code")
            break

    shared_pids: list[str] = []
    if entity_type == "party":
        party_entries = ctx.process_party_map.get(entity_name, [])
        for pid, _role in party_entries:
            if ctx.rapporteur_map.get(pid) == minister_name:
                shared_pids.append(pid)
    elif entity_type == "counsel":
        counsel_entries = ctx.process_counsel_map.get(entity_name, [])
        for pid, _side in counsel_entries:
            if ctx.rapporteur_map.get(pid) == minister_name:
                shared_pids.append(pid)

    outcomes_with_roles: list[tuple[str, str | None]] = []
    class_jb_pairs: list[tuple[str, str]] = []
    shared_pid_set = set(shared_pids)
    seen_pids: set[str] = set()
    if entity_type == "party":
        for pid, role in ctx.process_party_map.get(entity_name, []):
            if pid in shared_pid_set:
                for progress in ctx.process_outcomes.get(pid, []):
                    outcomes_with_roles.append((progress, role))
                if pid not in seen_pids:
                    seen_pids.add(pid)
                    pc = ctx.process_class_map.get(pid)
                    if pc:
                        jb = ctx.process_jb_map.get(pid, "incerto")
                        class_jb_pairs.append((pc, jb))
    elif entity_type == "counsel":
        for pid, _side in ctx.process_counsel_map.get(entity_name, []):
            if pid in shared_pid_set:
                for progress in ctx.process_outcomes.get(pid, []):
                    outcomes_with_roles.append((progress, None))
                if pid not in seen_pids:
                    seen_pids.add(pid)
                    pc = ctx.process_class_map.get(pid)
                    if pc:
                        jb = ctx.process_jb_map.get(pid, "incerto")
                        class_jb_pairs.append((pc, jb))

    favorable_rate = compute_favorable_rate_role_aware(outcomes_with_roles)
    favorable_rate_sub, n_substantive = compute_favorable_rate_substantive(outcomes_with_roles)

    baseline_rate: float | None = None
    if class_jb_pairs:
        most_common_class, most_common_jb = Counter(class_jb_pairs).most_common(1)[0][0]
        baseline_rate = lookup_baseline_rate(
            ctx.stratified_rates,
            ctx.fallback_rates,
            most_common_class,
            most_common_jb,
        )

    delta: float | None = None
    risk_score: float | None = None
    decay_factor = _degree_decay(link_degree)
    red_flag = False
    if favorable_rate is not None and baseline_rate is not None:
        delta = favorable_rate - baseline_rate
        risk_score = delta * decay_factor
        red_flag = risk_score > ctx.red_flag_delta_threshold and len(seen_pids) >= ctx.min_cases_for_red_flag

    red_flag_substantive: bool | None = None
    if favorable_rate_sub is not None and baseline_rate is not None and n_substantive >= ctx.min_cases_for_red_flag:
        sub_risk = (favorable_rate_sub - baseline_rate) * decay_factor
        red_flag_substantive = sub_risk > ctx.red_flag_delta_threshold

    power = red_flag_power(len(seen_pids), baseline_rate) if baseline_rate is not None else None
    confidence = red_flag_confidence_label(power)

    conflict_id = stable_id("cn-", f"{minister_norm}:{cnpj}:{entity_name}:d{link_degree}")

    # Decode qualification labels
    minister_qual_label = ctx.qualificacoes.get(minister_qual or "", "") if minister_qual else None
    company_nj_label = ctx.naturezas.get(company.get("natureza_juridica", ""), "") or None

    # Multi-establishment enrichment
    establishments = ctx.estab_index.get(cnpj, [])
    hq = next((e for e in establishments if e.get("matriz_filial") == "1"), None)
    active_estabs = [e for e in establishments if e.get("situacao_cadastral") == "02"]
    estab_ufs = sorted({e.get("uf", "") for e in establishments if e.get("uf")})
    estab_cnaes = sorted({e.get("cnae_fiscal", "") for e in establishments if e.get("cnae_fiscal")})
    estab_cnae_labels = sorted({e.get("cnae_fiscal_label", "") for e in establishments if e.get("cnae_fiscal_label")})

    # Key establishments: up to 3 active, prioritizing HQ
    key_estabs: list[dict[str, Any]] = []
    if hq and hq.get("situacao_cadastral") == "02":
        key_estabs.append(_estab_summary(hq))
    for e in sorted(active_estabs, key=lambda x: x.get("data_inicio_atividade", ""), reverse=True):
        if len(key_estabs) >= 3:
            break
        if e is not hq:
            key_estabs.append(_estab_summary(e))

    # Economic group
    eg = ctx.eg_index.get(cnpj, {})

    # Evidence provenance
    source_dataset = "socios"
    evidence_strength = "direct" if link_degree == 1 else "indirect"

    # Qualification label
    entity_qual_label = ctx.qualificacoes.get(entity_qualification, "") if entity_qualification else None

    return {
        "conflict_id": conflict_id,
        "minister_name": minister_name,
        "company_cnpj_basico": cnpj,
        "company_name": company_name,
        "minister_qualification": minister_qual,
        "linked_entity_type": entity_type,
        "linked_entity_id": entity_id,
        "linked_entity_name": entity_display_name,
        "entity_qualification": entity_qualification,
        "shared_process_ids": list(seen_pids),
        "shared_process_count": len(seen_pids),
        "favorable_rate": favorable_rate,
        "favorable_rate_substantive": favorable_rate_sub,
        "substantive_decision_count": n_substantive,
        "baseline_favorable_rate": baseline_rate,
        "favorable_rate_delta": delta,
        "risk_score": risk_score,
        "decay_factor": decay_factor,
        "red_flag": red_flag,
        "red_flag_substantive": red_flag_substantive,
        "red_flag_power": power,
        "red_flag_confidence": confidence,
        "link_chain": link_chain,
        "link_degree": link_degree,
        "generated_at": ctx.now_iso,
        # Decoded labels
        "minister_qualification_label": minister_qual_label,
        "entity_qualification_label": entity_qual_label,
        "company_natureza_juridica_label": company_nj_label,
        # Multi-establishment
        "establishment_count": len(establishments),
        "active_establishment_count": len(active_estabs),
        "headquarters_uf": hq.get("uf") if hq else None,
        "headquarters_municipio_label": hq.get("municipio_label") if hq else None,
        "headquarters_cnae_fiscal": hq.get("cnae_fiscal") if hq else None,
        "headquarters_cnae_label": hq.get("cnae_fiscal_label") if hq else None,
        "headquarters_situacao_cadastral": hq.get("situacao_cadastral") if hq else None,
        "headquarters_motivo_situacao_label": hq.get("motivo_situacao_label") if hq else None,
        "establishment_ufs": estab_ufs,
        "establishment_cnaes": estab_cnaes,
        "establishment_cnae_labels": estab_cnae_labels,
        "key_establishments": key_estabs,
        # Economic group
        "economic_group_id": eg.get("group_id"),
        "economic_group_member_count": eg.get("member_count"),
        "economic_group_razoes_sociais": eg.get("razoes_sociais", []),
        # Provenance
        "evidence_type": evidence_type,
        "source_dataset": source_dataset,
        "source_snapshot": None,
        "evidence_strength": evidence_strength,
    }
