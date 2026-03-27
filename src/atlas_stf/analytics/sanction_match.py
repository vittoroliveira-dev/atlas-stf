"""Build sanction match analytics from CGU raw data + curated entities."""

from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.identity import normalize_entity_name, stable_id
from ..core.stats import red_flag_confidence_label, red_flag_power
from ..schema_validate import validate_records
from ._atomic_io import AtomicJsonlWriter
from ._match_helpers import (
    DEFAULT_ALIAS_PATH,
    build_baseline_rates_stratified,
    build_counsel_client_map_from_links,
    build_counsel_process_map,
    build_entity_match_index,
    build_party_index,
    build_party_process_map,
    build_process_jb_category_map,
    build_process_outcomes,
    compute_favorable_rate_role_aware,
    compute_favorable_rate_substantive,
    lookup_baseline_rate,
    read_jsonl,
)
from ._parallel import build_counsel_profiles_parallel, match_entities_parallel
from ._run_context import RunContext

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path("schemas/sanction_match.schema.json")
SUMMARY_SCHEMA_PATH = Path("schemas/sanction_match_summary.schema.json")
DEFAULT_CGU_DIR = Path("data/raw/cgu")
DEFAULT_CVM_DIR = Path("data/raw/cvm")
DEFAULT_PARTY_PATH = Path("data/curated/party.jsonl")
DEFAULT_COUNSEL_PATH = Path("data/curated/counsel.jsonl")
DEFAULT_PROCESS_PATH = Path("data/curated/process.jsonl")
DEFAULT_DECISION_EVENT_PATH = Path("data/curated/decision_event.jsonl")
DEFAULT_PROCESS_PARTY_LINK_PATH = Path("data/curated/process_party_link.jsonl")
DEFAULT_PROCESS_COUNSEL_LINK_PATH = Path("data/curated/process_counsel_link.jsonl")
DEFAULT_OUTPUT_DIR = Path("data/analytics")

RED_FLAG_DELTA_THRESHOLD = 0.15
MIN_CASES_FOR_RED_FLAG = 3


def build_sanction_matches(
    *,
    cgu_dir: Path = DEFAULT_CGU_DIR,
    cvm_dir: Path = DEFAULT_CVM_DIR,
    party_path: Path = DEFAULT_PARTY_PATH,
    counsel_path: Path = DEFAULT_COUNSEL_PATH,
    process_path: Path = DEFAULT_PROCESS_PATH,
    decision_event_path: Path = DEFAULT_DECISION_EVENT_PATH,
    process_party_link_path: Path = DEFAULT_PROCESS_PARTY_LINK_PATH,
    process_counsel_link_path: Path = DEFAULT_PROCESS_COUNSEL_LINK_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    alias_path: Path = DEFAULT_ALIAS_PATH,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Build sanction match analytics from CGU raw data + curated entities."""
    output_dir.mkdir(parents=True, exist_ok=True)
    ctx = RunContext("sanction-match", output_dir, total_steps=7, on_progress=on_progress)

    # Load sanctions from all available sources
    ctx.start_step(0, "Carregando sanções...")
    sanctions: list[dict[str, Any]] = []

    cgu_path = cgu_dir / "sanctions_raw.jsonl"
    if cgu_path.exists():
        cgu_records = read_jsonl(cgu_path)
        sanctions.extend(cgu_records)
        logger.info("Loaded %d CGU sanction records", len(cgu_records))

    cvm_path = cvm_dir / "sanctions_raw.jsonl"
    if cvm_path.exists():
        cvm_records = read_jsonl(cvm_path)
        sanctions.extend(cvm_records)
        logger.info("Loaded %d CVM sanction records", len(cvm_records))

    match_path = output_dir / "sanction_match.jsonl"
    if not sanctions:
        logger.warning("No sanctions_raw.jsonl found in %s or %s", cgu_dir, cvm_dir)
        with AtomicJsonlWriter(match_path):
            pass
        return match_path

    logger.info("Total: %d raw sanction records from all sources", len(sanctions))

    # Index sanctions by normalized entity name
    sanctions_by_name: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for s in sanctions:
        name = normalize_entity_name(s.get("entity_name"))
        if name:
            sanctions_by_name[name].append(s)

    # Build indices
    ctx.start_step(1, "Construindo índices...")
    party_records = read_jsonl(party_path)
    party_index = build_party_index(party_path)
    party_match_index = build_entity_match_index(
        party_records,
        name_field="party_name_normalized",
        alias_path=alias_path,
        entity_kind="party",
    )

    # Build party_id -> name lookup for link-based maps
    party_id_to_name: dict[str, str] = {}
    for norm_name, record in party_index.items():
        pid = record.get("party_id", "")
        if pid:
            party_id_to_name[pid] = norm_name

    process_party_map = build_party_process_map(process_party_link_path, party_id_to_name)
    process_outcomes = build_process_outcomes(decision_event_path)
    stratified_rates, fallback_rates = build_baseline_rates_stratified(decision_event_path, process_path)
    process_jb_map = build_process_jb_category_map(decision_event_path)

    # Process class map for baseline lookup
    process_class_map: dict[str, str] = {}
    for record in read_jsonl(process_path):
        pid = record.get("process_id")
        pc = record.get("process_class")
        if pid and pc:
            process_class_map[pid] = pc

    # --- Match sanctions to parties (parallel across CPU cores) ---
    ctx.start_step(2, "Matching sanções → partes...")
    match_items: list[tuple[str, str | None]] = [
        (norm_name, sanction_list[0].get("entity_cnpj_cpf")) for norm_name, sanction_list in sanctions_by_name.items()
    ]
    match_results = match_entities_parallel(
        match_items,
        index=party_match_index,
        name_field="party_name_normalized",
    )

    matches: list[dict[str, Any]] = []
    seen_matches: set[str] = set()
    matched_party_names: set[str] = set()
    ambiguous_candidate_count = 0
    match_strategy_counts: dict[str, int] = defaultdict(int)
    now_iso = datetime.now(timezone.utc).isoformat()

    for norm_name, sanction_list in sanctions_by_name.items():
        match = match_results.get(norm_name)
        if match is None:
            continue
        if match.strategy == "ambiguous":
            ambiguous_candidate_count += 1
            continue

        party = match.record
        party_id = party.get("party_id", "")
        party_name = str(party.get("party_name_normalized") or norm_name)
        matched_party_names.add(party_name)
        match_strategy_counts[match.strategy] += 1

        # Gather all outcomes for this party (with role context)
        process_entries = process_party_map.get(party_name, [])
        outcomes_with_roles: list[tuple[str, str | None]] = []
        class_jb_pairs: list[tuple[str, str]] = []
        seen_pids: set[str] = set()
        for pid, role in process_entries:
            for progress in process_outcomes.get(pid, []):
                outcomes_with_roles.append((progress, role))
            if pid not in seen_pids:
                seen_pids.add(pid)
                pc = process_class_map.get(pid)
                if pc:
                    jb = process_jb_map.get(pid, "incerto")
                    class_jb_pairs.append((pc, jb))

        favorable_rate = compute_favorable_rate_role_aware(outcomes_with_roles)
        favorable_rate_sub, n_substantive = compute_favorable_rate_substantive(outcomes_with_roles)

        # Determine most common (process_class, jb_category) pair for baseline
        baseline_rate: float | None = None
        if class_jb_pairs:
            most_common_class, most_common_jb = Counter(class_jb_pairs).most_common(1)[0][0]
            baseline_rate = lookup_baseline_rate(stratified_rates, fallback_rates, most_common_class, most_common_jb)

        delta: float | None = None
        red_flag = False
        if favorable_rate is not None and baseline_rate is not None:
            delta = favorable_rate - baseline_rate
            red_flag = delta > RED_FLAG_DELTA_THRESHOLD and len(seen_pids) >= MIN_CASES_FOR_RED_FLAG

        # Substantive red flag (None when insufficient sample)
        red_flag_substantive: bool | None = None
        if favorable_rate_sub is not None and baseline_rate is not None and n_substantive >= MIN_CASES_FOR_RED_FLAG:
            red_flag_substantive = (favorable_rate_sub - baseline_rate) > RED_FLAG_DELTA_THRESHOLD

        power = red_flag_power(len(seen_pids), baseline_rate) if baseline_rate is not None else None
        confidence = red_flag_confidence_label(power)

        for sanction in sanction_list:
            match_id = stable_id("sm-", f"{party_id}:{norm_name}:{sanction.get('sanction_id', '')}")
            if match_id in seen_matches:
                continue
            seen_matches.add(match_id)
            matches.append(
                {
                    "match_id": match_id,
                    "entity_type": "party",
                    "entity_id": party_id,
                    "entity_name_normalized": party_name,
                    "party_id": party_id,
                    "party_name_normalized": party_name,
                    "sanction_source": sanction.get("sanction_source", ""),
                    "sanction_id": sanction.get("sanction_id", ""),
                    "match_strategy": match.strategy,
                    "match_score": match.score,
                    "matched_alias": match.matched_alias,
                    "matched_tax_id": match.matched_tax_id,
                    "uncertainty_note": match.uncertainty_note,
                    "sanctioning_body": sanction.get("sanctioning_body"),
                    "sanction_type": sanction.get("sanction_type"),
                    "sanction_start_date": sanction.get("sanction_start_date"),
                    "sanction_end_date": sanction.get("sanction_end_date"),
                    "sanction_description": sanction.get("sanction_description"),
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
                    "matched_at": now_iso,
                }
            )

    # --- Match sanctions to counsel (parallel across CPU cores) ---
    ctx.start_step(3, "Matching sanções → advogados...")
    counsel_records = read_jsonl(counsel_path)
    counsel_index: dict[str, dict[str, Any]] = {}
    counsel_id_to_name: dict[str, str] = {}
    counsel_name_collisions = 0
    for record in counsel_records:
        norm = normalize_entity_name(record.get("counsel_name_normalized") or record.get("counsel_name_raw", ""))
        if norm:
            if norm in counsel_index:
                counsel_name_collisions += 1
            else:
                counsel_index[norm] = record
            cid = record.get("counsel_id", "")
            if cid:
                counsel_id_to_name[cid] = norm
    if counsel_name_collisions:
        logger.warning(
            "build_sanction_matches: %d counsel name collisions (first record kept)",
            counsel_name_collisions,
        )

    counsel_match_index = build_entity_match_index(
        counsel_records,
        name_field="counsel_name_normalized",
        alias_path=alias_path,
        entity_kind="counsel",
    )

    counsel_match_items: list[tuple[str, str | None]] = [
        (norm_name, sanction_list[0].get("entity_cnpj_cpf")) for norm_name, sanction_list in sanctions_by_name.items()
    ]
    counsel_match_results = match_entities_parallel(
        counsel_match_items,
        index=counsel_match_index,
        name_field="counsel_name_normalized",
    )

    counsel_process_map = build_counsel_process_map(process_counsel_link_path, counsel_id_to_name)
    matched_counsel_names: set[str] = set()
    counsel_ambiguous_count = 0
    counsel_match_strategy_counts: dict[str, int] = defaultdict(int)

    for norm_name, sanction_list in sanctions_by_name.items():
        match = counsel_match_results.get(norm_name)
        if match is None:
            continue
        if match.strategy == "ambiguous":
            counsel_ambiguous_count += 1
            continue

        counsel = match.record
        counsel_id = counsel.get("counsel_id", "")
        counsel_name = str(counsel.get("counsel_name_normalized") or norm_name)
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
            red_flag = delta > RED_FLAG_DELTA_THRESHOLD and len(seen_pids) >= MIN_CASES_FOR_RED_FLAG

        red_flag_substantive: bool | None = None
        if favorable_rate_sub is not None and baseline_rate is not None and n_substantive >= MIN_CASES_FOR_RED_FLAG:
            red_flag_substantive = (favorable_rate_sub - baseline_rate) > RED_FLAG_DELTA_THRESHOLD

        power = red_flag_power(len(seen_pids), baseline_rate) if baseline_rate is not None else None
        confidence = red_flag_confidence_label(power)

        for sanction in sanction_list:
            match_id = stable_id("sm-", f"counsel:{counsel_id}:{norm_name}:{sanction.get('sanction_id', '')}")
            if match_id in seen_matches:
                continue
            seen_matches.add(match_id)
            matches.append(
                {
                    "match_id": match_id,
                    "entity_type": "counsel",
                    "entity_id": counsel_id,
                    "entity_name_normalized": counsel_name,
                    "sanction_source": sanction.get("sanction_source", ""),
                    "sanction_id": sanction.get("sanction_id", ""),
                    "match_strategy": match.strategy,
                    "match_score": match.score,
                    "matched_alias": match.matched_alias,
                    "matched_tax_id": match.matched_tax_id,
                    "uncertainty_note": match.uncertainty_note,
                    "sanctioning_body": sanction.get("sanctioning_body"),
                    "sanction_type": sanction.get("sanction_type"),
                    "sanction_start_date": sanction.get("sanction_start_date"),
                    "sanction_end_date": sanction.get("sanction_end_date"),
                    "sanction_description": sanction.get("sanction_description"),
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
                    "matched_at": now_iso,
                }
            )

    # Write all sanction matches (party + counsel)
    ctx.start_step(4, "Escrevendo matches...")
    validate_records(matches, SCHEMA_PATH)
    with AtomicJsonlWriter(match_path) as fh:
        for m in matches:
            fh.write(json.dumps(m, ensure_ascii=False) + "\n")

    # Build counsel sanction profiles (indirect, via party clients)
    ctx.start_step(5, "Perfis de advogados...")
    counsel_client_map = build_counsel_client_map_from_links(
        process_party_link_path, process_counsel_link_path, party_id_to_name, counsel_id_to_name
    )
    raw_profiles = build_counsel_profiles_parallel(
        counsel_client_map,
        counsel_index=counsel_index,
        process_party_map=process_party_map,
        process_outcomes=process_outcomes,
        matched_names=matched_party_names,
        red_flag_delta=RED_FLAG_DELTA_THRESHOLD,
        min_cases=MIN_CASES_FOR_RED_FLAG,
    )
    # Rename keys to match sanction-specific schema.
    counsel_profiles: list[dict[str, Any]] = [
        {
            "counsel_id": p["counsel_id"],
            "counsel_name_normalized": p["counsel_name_normalized"],
            "sanctioned_client_count": p["flagged_client_count"],
            "total_client_count": p["total_client_count"],
            "sanctioned_client_rate": p["flagged_client_rate"],
            "sanctioned_favorable_rate": p["flagged_favorable_rate"],
            "overall_favorable_rate": p["overall_favorable_rate"],
            "red_flag": p["red_flag"],
        }
        for p in raw_profiles
    ]

    counsel_path_out = output_dir / "counsel_sanction_profile.jsonl"
    with AtomicJsonlWriter(counsel_path_out) as fh:
        for cp in counsel_profiles:
            fh.write(json.dumps(cp, ensure_ascii=False) + "\n")

    # Write summary
    ctx.start_step(6, "Resumo...")
    party_matches = [m for m in matches if m["entity_type"] == "party"]
    counsel_direct_matches = [m for m in matches if m["entity_type"] == "counsel"]

    summary = {
        "total_sanctions_raw": len(sanctions),
        "sources": {
            "ceis": sum(1 for s in sanctions if s.get("sanction_source") == "ceis"),
            "cnep": sum(1 for s in sanctions if s.get("sanction_source") == "cnep"),
            "cvm": sum(1 for s in sanctions if s.get("sanction_source") == "cvm"),
            "leniencia": sum(1 for s in sanctions if s.get("sanction_source") == "leniencia"),
        },
        "matched_party_count": len(matched_party_names),
        "matched_counsel_count": len(matched_counsel_names),
        "sanction_match_count": len(matches),
        "party_match_count": len(party_matches),
        "counsel_match_count": len(counsel_direct_matches),
        "party_red_flag_count": sum(1 for m in party_matches if m["red_flag"]),
        "counsel_direct_red_flag_count": sum(1 for m in counsel_direct_matches if m["red_flag"]),
        "counsel_profile_count": len(counsel_profiles),
        "counsel_red_flag_count": sum(1 for cp in counsel_profiles if cp["red_flag"]),
        "matched_by_tax_id_count": match_strategy_counts.get("tax_id", 0),
        "matched_by_alias_count": match_strategy_counts.get("alias", 0),
        "matched_by_similarity_count": (
            match_strategy_counts.get("jaccard", 0) + match_strategy_counts.get("levenshtein", 0)
        ),
        "ambiguous_candidate_count": ambiguous_candidate_count,
        "generated_at": now_iso,
    }
    validate_records([summary], SUMMARY_SCHEMA_PATH)
    summary_path = output_dir / "sanction_match_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    ctx.finish(outputs=[str(match_path)])
    logger.info(
        "Built sanction matches: %d party + %d counsel matches, %d counsel profiles",
        len(party_matches),
        len(counsel_direct_matches),
        len(counsel_profiles),
    )
    return match_path
