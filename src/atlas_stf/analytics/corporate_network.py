"""Build corporate network analytics: detect minister-party/counsel corporate ties."""

from __future__ import annotations

import json
import logging
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..rfb._reference import load_all_reference_tables
from ..schema_validate import validate_records
from ._atomic_io import AtomicJsonlWriter
from ._corporate_network_context import (
    CorporateNetworkContext,
    _build_company_index,
    _build_counsel_index,
    _build_name_to_cnpjs,
    _build_partner_index,
    _build_pj_partner_index,
    _compute_conflict,
    _load_minister_names,
)
from ._match_helpers import (
    build_baseline_rates_stratified,
    build_counsel_process_map,
    build_party_index,
    build_party_process_map,
    build_process_class_map,
    build_process_jb_category_map,
    build_process_outcomes,
    read_jsonl,
)

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path("schemas/corporate_network.schema.json")
SUMMARY_SCHEMA_PATH = Path("schemas/corporate_network_summary.schema.json")
DEFAULT_MAX_LINK_DEGREE = 3


def build_corporate_network(
    *,
    rfb_dir: Path = Path("data/raw/rfb"),
    minister_bio_path: Path = Path("data/curated/minister_bio.json"),
    party_path: Path = Path("data/curated/party.jsonl"),
    counsel_path: Path = Path("data/curated/counsel.jsonl"),
    process_path: Path = Path("data/curated/process.jsonl"),
    decision_event_path: Path = Path("data/curated/decision_event.jsonl"),
    process_party_link_path: Path = Path("data/curated/process_party_link.jsonl"),
    process_counsel_link_path: Path = Path("data/curated/process_counsel_link.jsonl"),
    output_dir: Path = Path("data/analytics"),
    max_link_degree: int = DEFAULT_MAX_LINK_DEGREE,
) -> Path:
    """Build corporate network analytics from RFB data + curated entities."""
    output_dir.mkdir(parents=True, exist_ok=True)
    max_link_degree = min(max(1, int(max_link_degree)), 6)

    partner_index = _build_partner_index(rfb_dir)
    company_index = _build_company_index(rfb_dir)

    output_path = output_dir / "corporate_network.jsonl"
    if not partner_index:
        logger.warning("No partners_raw.jsonl found in %s", rfb_dir)
        with AtomicJsonlWriter(output_path):
            pass  # write empty file
        return output_path

    # Load optional enrichment data
    estab_index: dict[str, list[dict[str, Any]]] = defaultdict(list)
    estab_path = rfb_dir / "establishments_raw.jsonl"
    if estab_path.exists():
        for record in read_jsonl(estab_path):
            cnpj = record.get("cnpj_basico", "")
            if cnpj:
                estab_index[cnpj].append(record)

    ref_tables = load_all_reference_tables(rfb_dir)
    qualificacoes = ref_tables.get("qualificacoes", {})
    naturezas = ref_tables.get("naturezas", {})

    eg_index: dict[str, dict[str, Any]] = {}
    eg_path = output_dir / "economic_group.jsonl"
    if eg_path.exists():
        for record in read_jsonl(eg_path):
            for cnpj in record.get("member_cnpjs", []):
                eg_index[cnpj] = record

    minister_names = _load_minister_names(minister_bio_path)
    party_index = build_party_index(party_path)
    counsel_index = _build_counsel_index(counsel_path)

    party_id_to_name: dict[str, str] = {}
    for norm_name, record in party_index.items():
        pid = record.get("party_id", "")
        if pid:
            party_id_to_name[pid] = norm_name

    process_party_map = build_party_process_map(process_party_link_path, party_id_to_name)

    counsel_id_to_name: dict[str, str] = {}
    for norm_name, record in counsel_index.items():
        cid = record.get("counsel_id", "")
        if cid:
            counsel_id_to_name[cid] = norm_name

    process_counsel_map = build_counsel_process_map(process_counsel_link_path, counsel_id_to_name)
    process_outcomes = build_process_outcomes(decision_event_path)
    stratified_rates, fallback_rates = build_baseline_rates_stratified(decision_event_path, process_path)
    process_jb_map = build_process_jb_category_map(decision_event_path)
    process_class_map = build_process_class_map(process_path)

    rapporteur_map: dict[str, str] = {}
    for record in read_jsonl(decision_event_path):
        pid = record.get("process_id")
        rap = record.get("current_rapporteur")
        if pid and rap:
            rapporteur_map[pid] = rap

    name_to_cnpjs = _build_name_to_cnpjs(partner_index)
    pj_partner_index = _build_pj_partner_index(partner_index)

    now_iso = datetime.now(timezone.utc).isoformat()

    ctx = CorporateNetworkContext(
        company_index=company_index,
        partner_index=partner_index,
        process_party_map=process_party_map,
        process_counsel_map=process_counsel_map,
        process_outcomes=process_outcomes,
        stratified_rates=stratified_rates,
        fallback_rates=fallback_rates,
        process_jb_map=process_jb_map,
        process_class_map=process_class_map,
        rapporteur_map=rapporteur_map,
        qualificacoes=qualificacoes,
        naturezas=naturezas,
        estab_index=dict(estab_index),
        eg_index=eg_index,
        now_iso=now_iso,
    )

    conflicts: list[dict[str, Any]] = []
    seen_conflict_keys: set[str] = set()

    def company_label(cnpj: str) -> str:
        return company_index.get(cnpj, {}).get("razao_social") or cnpj

    for minister_norm, minister_name in minister_names.items():
        minister_cnpjs = sorted(name_to_cnpjs.get(minister_norm, set()))
        if not minister_cnpjs:
            continue

        best_degree: dict[str, int] = {cnpj: 1 for cnpj in minister_cnpjs}
        queue = deque((cnpj, 1, [company_label(cnpj)]) for cnpj in minister_cnpjs)

        while queue:
            cnpj, degree, chain = queue.popleft()
            co_partners = partner_index.get(cnpj, [])
            for co_partner in co_partners:
                repr_raw = co_partner.get("representative_name") or co_partner.get(
                    "representative_name_normalized", ""
                )
                for name_key, display_name, qualification, ev_type in (
                    (
                        co_partner.get("partner_name_normalized", ""),
                        co_partner.get("partner_name") or co_partner.get("partner_name_normalized", ""),
                        co_partner.get("qualification_code"),
                        "partner_pf",
                    ),
                    (
                        co_partner.get("representative_name_normalized", ""),
                        f"(repr.) {repr_raw}",
                        None,
                        "representative",
                    ),
                ):
                    if not name_key or name_key == minister_norm:
                        continue
                    entity_record = party_index.get(name_key) or counsel_index.get(name_key)
                    if entity_record is None:
                        continue
                    conflict_key = f"{minister_norm}:{cnpj}:{name_key}:d{degree}"
                    if conflict_key in seen_conflict_keys:
                        continue
                    seen_conflict_keys.add(conflict_key)
                    link_chain = f"{minister_name} -> {' -> '.join(chain)} -> {display_name}"
                    entity_type = "party" if name_key in party_index else "counsel"
                    conflict = _compute_conflict(
                        ctx,
                        minister_name,
                        minister_norm,
                        cnpj,
                        name_key,
                        display_name,
                        entity_type,
                        entity_record,
                        degree,
                        link_chain,
                        ev_type,
                        qualification,
                    )
                    conflicts.append(conflict)

            if degree >= max_link_degree:
                continue

            for co_partner in co_partners:
                if co_partner.get("partner_type") != "1":
                    continue
                pj_cnpj = co_partner.get("partner_cpf_cnpj", "").strip()
                if not pj_cnpj:
                    continue
                for next_cnpj in sorted(pj_partner_index.get(pj_cnpj, set())):
                    next_degree = degree + 1
                    if next_cnpj == cnpj or next_degree > max_link_degree:
                        continue
                    if next_degree >= best_degree.get(next_cnpj, next_degree + 1):
                        continue
                    best_degree[next_cnpj] = next_degree
                    queue.append((next_cnpj, next_degree, [*chain, company_label(next_cnpj)]))

    # Write conflicts
    validate_records(conflicts, SCHEMA_PATH)
    with AtomicJsonlWriter(output_path) as fh:
        for c in conflicts:
            fh.write(json.dumps(c, ensure_ascii=False) + "\n")

    # Write summary
    degree_counts: dict[int, int] = defaultdict(int)
    for conflict in conflicts:
        degree_counts[int(conflict["link_degree"])] += 1
    summary: dict[str, Any] = {
        "total_conflicts": len(conflicts),
        "red_flag_count": sum(1 for c in conflicts if c["red_flag"]),
        "ministers_involved": len({c["minister_name"] for c in conflicts}),
        "companies_involved": len({c["company_cnpj_basico"] for c in conflicts}),
        "max_link_degree": max_link_degree,
        "degree_counts": {str(k): degree_counts[k] for k in sorted(degree_counts)},
        "generated_at": now_iso,
    }
    for degree in range(1, max_link_degree + 1):
        summary[f"degree_{degree}_count"] = degree_counts.get(degree, 0)
    validate_records([summary], SUMMARY_SCHEMA_PATH)
    summary_path = output_dir / "corporate_network_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info(
        "Built corporate network: %d conflicts (%d red flags, max degree %d)",
        len(conflicts),
        summary["red_flag_count"],
        max_link_degree,
    )
    return output_path
