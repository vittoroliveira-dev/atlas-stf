"""Build corporate network analytics: detect minister-party/counsel corporate ties."""

from __future__ import annotations

import json
import logging
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.identity import normalize_entity_name, stable_id
from ._match_helpers import (
    build_baseline_rates,
    build_counsel_process_map,
    build_party_index,
    build_party_process_map,
    build_process_class_map,
    build_process_outcomes,
    compute_favorable_rate_role_aware,
    read_jsonl,
)

logger = logging.getLogger(__name__)

RED_FLAG_DELTA_THRESHOLD = 0.15
MIN_CASES_FOR_RED_FLAG = 3
DEFAULT_MAX_LINK_DEGREE = 3


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
    """Index counsels by normalized name -> counsel record."""
    index: dict[str, dict[str, Any]] = {}
    for record in read_jsonl(counsel_path):
        norm = normalize_entity_name(record.get("counsel_name_normalized") or record.get("counsel_name_raw", ""))
        if norm:
            index.setdefault(norm, record)
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


def _degree_decay(link_degree: int) -> float:
    return 1.0 if link_degree <= 2 else 0.5 ** (link_degree - 2)


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

    if not partner_index:
        logger.warning("No partners_raw.jsonl found in %s", rfb_dir)
        return output_dir

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
    baseline_rates = build_baseline_rates(decision_event_path, process_path)
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
    conflicts: list[dict[str, Any]] = []
    seen_conflict_keys: set[str] = set()

    def company_label(cnpj: str) -> str:
        return company_index.get(cnpj, {}).get("razao_social") or cnpj

    def _compute_conflict(
        minister_name: str,
        minister_norm: str,
        cnpj: str,
        entity_name: str,
        entity_type: str,
        entity_record: dict[str, Any],
        link_degree: int,
        link_chain: str,
    ) -> dict[str, Any]:
        """Compute a single conflict record with outcome stats."""
        entity_id = entity_record.get("party_id" if entity_type == "party" else "counsel_id", "")

        company = company_index.get(cnpj, {})
        company_name = company.get("razao_social", "")

        minister_qual: str | None = None
        for p in partner_index.get(cnpj, []):
            p_norm = p.get("partner_name_normalized", "")
            if p_norm == minister_norm:
                minister_qual = p.get("qualification_code")
                break

        shared_pids: list[str] = []
        if entity_type == "party":
            party_entries = process_party_map.get(entity_name, [])
            for pid, _role in party_entries:
                if rapporteur_map.get(pid) == minister_name:
                    shared_pids.append(pid)
        elif entity_type == "counsel":
            counsel_entries = process_counsel_map.get(entity_name, [])
            for pid, _side in counsel_entries:
                if rapporteur_map.get(pid) == minister_name:
                    shared_pids.append(pid)

        outcomes_with_roles: list[tuple[str, str | None]] = []
        party_classes: list[str] = []
        shared_pid_set = set(shared_pids)
        seen_pids: set[str] = set()
        if entity_type == "party":
            for pid, role in process_party_map.get(entity_name, []):
                if pid in shared_pid_set:
                    for progress in process_outcomes.get(pid, []):
                        outcomes_with_roles.append((progress, role))
                    if pid not in seen_pids:
                        seen_pids.add(pid)
                        pc = process_class_map.get(pid)
                        if pc:
                            party_classes.append(pc)
        elif entity_type == "counsel":
            for pid, _side in process_counsel_map.get(entity_name, []):
                if pid in shared_pid_set:
                    for progress in process_outcomes.get(pid, []):
                        outcomes_with_roles.append((progress, None))
                    if pid not in seen_pids:
                        seen_pids.add(pid)
                        pc = process_class_map.get(pid)
                        if pc:
                            party_classes.append(pc)

        favorable_rate = compute_favorable_rate_role_aware(outcomes_with_roles)

        baseline_rate: float | None = None
        if party_classes:
            most_common = max(set(party_classes), key=party_classes.count)
            baseline_rate = baseline_rates.get(most_common)

        delta: float | None = None
        risk_score: float | None = None
        decay_factor = _degree_decay(link_degree)
        red_flag = False
        if favorable_rate is not None and baseline_rate is not None:
            delta = favorable_rate - baseline_rate
            risk_score = delta * decay_factor
            red_flag = risk_score > RED_FLAG_DELTA_THRESHOLD and len(seen_pids) >= MIN_CASES_FOR_RED_FLAG

        conflict_id = stable_id("cn-", f"{minister_norm}:{cnpj}:{entity_name}:d{link_degree}")

        return {
            "conflict_id": conflict_id,
            "minister_name": minister_name,
            "company_cnpj_basico": cnpj,
            "company_name": company_name,
            "minister_qualification": minister_qual,
            "linked_entity_type": entity_type,
            "linked_entity_id": entity_id,
            "linked_entity_name": entity_name,
            "entity_qualification": None,
            "shared_process_ids": list(seen_pids),
            "shared_process_count": len(seen_pids),
            "favorable_rate": favorable_rate,
            "baseline_favorable_rate": baseline_rate,
            "favorable_rate_delta": delta,
            "risk_score": risk_score,
            "decay_factor": decay_factor,
            "red_flag": red_flag,
            "link_chain": link_chain,
            "link_degree": link_degree,
            "generated_at": now_iso,
        }

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
                for name_key, display_name, qualification in (
                    (
                        co_partner.get("partner_name_normalized", ""),
                        co_partner.get("partner_name_normalized", ""),
                        co_partner.get("qualification_code"),
                    ),
                    (
                        co_partner.get("representative_name_normalized", ""),
                        f"(repr.) {co_partner.get('representative_name_normalized', '')}",
                        None,
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
                        minister_name,
                        minister_norm,
                        cnpj,
                        name_key,
                        entity_type,
                        entity_record,
                        degree,
                        link_chain,
                    )
                    conflict["entity_qualification"] = qualification
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
    output_path = output_dir / "corporate_network.jsonl"
    with output_path.open("w", encoding="utf-8") as fh:
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
    summary_path = output_dir / "corporate_network_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info(
        "Built corporate network: %d conflicts (%d red flags, max degree %d)",
        len(conflicts),
        summary["red_flag_count"],
        max_link_degree,
    )
    return output_path
