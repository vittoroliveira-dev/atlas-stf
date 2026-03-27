"""Counsel-side donation match helpers (extracted from donation_match.py)."""

from __future__ import annotations

import logging
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from ..core.identity import normalize_entity_name, stable_id
from ..core.stats import red_flag_confidence_label, red_flag_power
from ._donation_aggregator import _build_ambiguous_record
from ._match_helpers import (
    EntityMatchIndex,
    EntityMatchResult,
    build_counsel_process_map,
    build_entity_match_index,
    compute_favorable_rate_role_aware,
    compute_favorable_rate_substantive,
    lookup_baseline_rate,
    read_jsonl,
)
from ._parallel import match_entities_parallel

logger = logging.getLogger(__name__)

__all__ = [
    "build_counsel_match_context",
    "match_donors_to_counsel",
    "process_counsel_match_results",
]


def build_counsel_match_context(
    counsel_path: Path,
    alias_path: Path,
) -> tuple[dict[str, dict[str, Any]], dict[str, str], EntityMatchIndex]:
    """Build counsel lookup indices (once, ~100 MB).

    Returns (counsel_index, counsel_id_to_name, counsel_match_index).
    """
    counsel_records = read_jsonl(counsel_path)
    counsel_index: dict[str, dict[str, Any]] = {}
    counsel_id_to_name: dict[str, str] = {}
    collisions = 0
    for record in counsel_records:
        norm = normalize_entity_name(record.get("counsel_name_normalized") or record.get("counsel_name_raw", ""))
        if norm:
            if norm in counsel_index:
                collisions += 1
            else:
                counsel_index[norm] = record
            cid = record.get("counsel_id", "")
            if cid:
                counsel_id_to_name[cid] = norm
    if collisions:
        logger.warning("build_counsel_match_context: %d name collisions (first record kept)", collisions)

    counsel_match_index = build_entity_match_index(
        counsel_records,
        name_field="counsel_name_normalized",
        alias_path=alias_path,
        entity_kind="counsel",
    )

    return counsel_index, counsel_id_to_name, counsel_match_index


def process_counsel_match_results(
    *,
    counsel_match_results: dict[str, EntityMatchResult | None],
    donor_agg: dict[str, dict[str, Any]],
    counsel_index: dict[str, dict[str, Any]],
    counsel_id_to_name: dict[str, str],
    process_counsel_link_path: Path,
    process_outcomes: dict[str, list[str]],
    process_class_map: dict[str, str],
    process_jb_map: dict[str, str],
    stratified_rates: Any,
    fallback_rates: Any,
    red_flag_delta: float,
    min_cases: int,
    now_iso: str,
    ambiguous_records: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], set[str], int, dict[str, int]]:
    """Process pre-computed counsel match results against donor aggregation.

    Returns (matches, matched_counsel_names, counsel_ambiguous_count, counsel_match_strategy_counts).
    """
    counsel_process_map = build_counsel_process_map(process_counsel_link_path, counsel_id_to_name)
    matched_counsel_names: set[str] = set()
    counsel_ambiguous_count = 0
    counsel_match_strategy_counts: dict[str, int] = defaultdict(int)
    matches: list[dict[str, Any]] = []

    for donor_key, donor_info in donor_agg.items():
        donor_name = donor_info["donor_name_normalized"]
        match = counsel_match_results.get(donor_name)
        if match is None:
            continue
        if match.strategy == "ambiguous":
            counsel_ambiguous_count += 1
            ambiguous_records.append(_build_ambiguous_record(donor_key, donor_info, match, entity_type="counsel"))
            continue

        counsel = match.record
        counsel_id = counsel.get("counsel_id", "")
        counsel_name = str(counsel.get("counsel_name_normalized") or donor_name)
        matched_counsel_names.add(counsel_name)
        counsel_match_strategy_counts[match.strategy] += 1

        # Gather outcomes for this counsel (with side context)
        process_entries = counsel_process_map.get(counsel_name, [])
        outcomes_with_roles: list[tuple[str, str | None]] = []
        class_jb_pairs: list[tuple[str, str]] = []
        seen_pids: set[str] = set()
        for pid, side in process_entries:
            for progress in process_outcomes.get(pid, []):
                outcomes_with_roles.append((progress, side))
            if pid not in seen_pids:
                seen_pids.add(pid)
                pc = process_class_map.get(pid)
                if pc:
                    jb = process_jb_map.get(pid, "incerto")
                    class_jb_pairs.append((pc, jb))

        favorable_rate = compute_favorable_rate_role_aware(outcomes_with_roles)
        favorable_rate_sub, n_substantive = compute_favorable_rate_substantive(outcomes_with_roles)

        baseline_rate = None
        if class_jb_pairs:
            most_common_class, most_common_jb = Counter(class_jb_pairs).most_common(1)[0][0]
            baseline_rate = lookup_baseline_rate(stratified_rates, fallback_rates, most_common_class, most_common_jb)

        delta = None
        red_flag = False
        if favorable_rate is not None and baseline_rate is not None:
            delta = favorable_rate - baseline_rate
            red_flag = delta > red_flag_delta and len(seen_pids) >= min_cases

        red_flag_substantive: bool | None = None
        if favorable_rate_sub is not None and baseline_rate is not None and n_substantive >= min_cases:
            red_flag_substantive = (favorable_rate_sub - baseline_rate) > red_flag_delta

        power = red_flag_power(len(seen_pids), baseline_rate) if baseline_rate is not None else None
        confidence = red_flag_confidence_label(power)

        match_id = stable_id("dm-", f"counsel:{counsel_id}:{donor_key}")
        matches.append(
            {
                "match_id": match_id,
                "entity_type": "counsel",
                "entity_id": counsel_id,
                "entity_name_normalized": counsel_name,
                "donor_cpf_cnpj": donor_info["donor_cpf_cnpj"],
                "donor_name_normalized": donor_name,
                "donor_name_originator": donor_info.get("donor_name_originator", ""),
                "donor_identity_key": donor_key,
                "match_strategy": match.strategy,
                "match_score": match.score,
                "matched_alias": match.matched_alias,
                "matched_tax_id": match.matched_tax_id,
                "uncertainty_note": match.uncertainty_note,
                "total_donated_brl": donor_info["total_donated_brl"],
                "donation_count": donor_info["donation_count"],
                "election_years": donor_info["election_years"],
                "parties_donated_to": donor_info["parties_donated_to"],
                "candidates_donated_to": donor_info["candidates_donated_to"],
                "positions_donated_to": donor_info["positions_donated_to"],
                "first_donation_date": donor_info.get("first_donation_date"),
                "last_donation_date": donor_info.get("last_donation_date"),
                "active_election_year_count": donor_info.get("active_election_year_count", 0),
                "max_single_donation_brl": donor_info.get("max_single_donation_brl", 0.0),
                "avg_donation_brl": donor_info.get("avg_donation_brl", 0.0),
                "top_candidate_share": donor_info.get("top_candidate_share"),
                "top_party_share": donor_info.get("top_party_share"),
                "top_state_share": donor_info.get("top_state_share"),
                "donation_year_span": donor_info.get("donation_year_span"),
                "recent_donation_flag": donor_info.get("recent_donation_flag", False),
                "stf_case_count": len(seen_pids),
                "favorable_rate": favorable_rate,
                "favorable_rate_substantive": favorable_rate_sub,
                "substantive_decision_count": n_substantive,
                "baseline_favorable_rate": baseline_rate,
                "favorable_rate_delta": delta,
                "red_flag": red_flag,
                "red_flag_substantive": red_flag_substantive,
                "red_flag_power": power,
                "red_flag_confidence": confidence,
                "resource_types_observed": donor_info.get("resource_types_seen", []),
                "matched_at": now_iso,
            }
        )

    return (
        matches,
        matched_counsel_names,
        counsel_ambiguous_count,
        dict(counsel_match_strategy_counts),
    )


def match_donors_to_counsel(
    *,
    counsel_path: Path,
    donor_agg: dict[str, dict[str, Any]],
    process_counsel_link_path: Path,
    process_outcomes: dict[str, list[str]],
    process_class_map: dict[str, str],
    process_jb_map: dict[str, str],
    stratified_rates: Any,
    fallback_rates: Any,
    alias_path: Path,
    red_flag_delta: float,
    min_cases: int,
    now_iso: str,
    ambiguous_records: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], set[str], dict[str, str], dict[str, dict[str, Any]], int, dict[str, int]]:
    """Match aggregated donors to counsel entities.

    Returns:
        (matches, matched_counsel_names, counsel_id_to_name,
         counsel_index, counsel_ambiguous_count, counsel_match_strategy_counts)
    """
    counsel_index, counsel_id_to_name, counsel_match_index = build_counsel_match_context(
        counsel_path,
        alias_path,
    )

    counsel_match_items: list[tuple[str, str | None]] = [
        (donor_info["donor_name_normalized"], donor_info.get("donor_cpf_cnpj")) for donor_info in donor_agg.values()
    ]
    counsel_match_results = match_entities_parallel(
        counsel_match_items,
        index=counsel_match_index,
        name_field="counsel_name_normalized",
    )

    (
        matches,
        matched_counsel_names,
        counsel_ambiguous_count,
        counsel_match_strategy_counts,
    ) = process_counsel_match_results(
        counsel_match_results=counsel_match_results,
        donor_agg=donor_agg,
        counsel_index=counsel_index,
        counsel_id_to_name=counsel_id_to_name,
        process_counsel_link_path=process_counsel_link_path,
        process_outcomes=process_outcomes,
        process_class_map=process_class_map,
        process_jb_map=process_jb_map,
        stratified_rates=stratified_rates,
        fallback_rates=fallback_rates,
        red_flag_delta=red_flag_delta,
        min_cases=min_cases,
        now_iso=now_iso,
        ambiguous_records=ambiguous_records,
    )

    return (
        matches,
        matched_counsel_names,
        counsel_id_to_name,
        counsel_index,
        counsel_ambiguous_count,
        counsel_match_strategy_counts,
    )
