"""Build donation match analytics from TSE raw data + curated entities."""

from __future__ import annotations

import json
import logging
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.identity import normalize_entity_name, stable_id
from ..core.stats import red_flag_confidence_label, red_flag_power
from ..tse._resource_classifier import classify_resource_type
from ._atomic_io import AtomicJsonlWriter
from ._donor_identity import donor_identity_key as _donor_identity_key_fn
from ._match_helpers import (
    DEFAULT_ALIAS_PATH,
    EntityMatchResult,
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
    iter_jsonl,
    lookup_baseline_rate,
    read_jsonl,
)
from ._parallel import build_counsel_profiles_parallel, match_entities_parallel

logger = logging.getLogger(__name__)

DEFAULT_TSE_DIR = Path("data/raw/tse")
DEFAULT_PARTY_PATH = Path("data/curated/party.jsonl")
DEFAULT_COUNSEL_PATH = Path("data/curated/counsel.jsonl")
DEFAULT_PROCESS_PATH = Path("data/curated/process.jsonl")
DEFAULT_DECISION_EVENT_PATH = Path("data/curated/decision_event.jsonl")
DEFAULT_PROCESS_PARTY_LINK_PATH = Path("data/curated/process_party_link.jsonl")
DEFAULT_PROCESS_COUNSEL_LINK_PATH = Path("data/curated/process_counsel_link.jsonl")
DEFAULT_OUTPUT_DIR = Path("data/analytics")

RED_FLAG_DELTA_THRESHOLD = 0.15
MIN_CASES_FOR_RED_FLAG = 3


def _donor_identity_key(name: str, cpf_cnpj: str) -> str:
    """Delegate to shared helper (backward-compatible wrapper)."""
    return _donor_identity_key_fn(name, cpf_cnpj)


def _stream_aggregate_donations(
    path: Path,
) -> tuple[dict[str, dict[str, Any]], int, set[int]]:
    """Aggregate donations line by line without loading the full file.

    Returns (donor_agg, raw_count, election_years_seen).
    The aggregation key is a stable identity key (CPF/CNPJ when available,
    else normalized name) to avoid homonym fusion.
    """
    _ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

    agg: dict[str, dict[str, Any]] = {}
    raw_count = 0
    all_years: set[int] = set()

    for d in iter_jsonl(path):
        raw_count += 1
        name = d.get("donor_name_normalized", "")
        if not name:
            continue
        cpf_cnpj = d.get("donor_cpf_cnpj", "")
        key = _donor_identity_key(name, cpf_cnpj)
        if key not in agg:
            agg[key] = {
                "donor_cpf_cnpj": cpf_cnpj,
                "donor_name_normalized": name,
                "donor_names_seen": set(),
                "donor_name_originator": d.get("donor_name_originator_normalized", ""),
                "total_donated_brl": 0.0,
                "donation_count": 0,
                "election_years": set(),
                "parties_donated_to": set(),
                "candidates_donated_to": set(),
                "positions_donated_to": set(),
                "resource_types_seen": set(),
                # Temporal / concentration accumulators
                "min_donation_date": None,
                "max_donation_date": None,
                "max_single_donation_brl": 0.0,
                "amount_by_candidate": defaultdict(float),
                "amount_by_party": defaultdict(float),
                "amount_by_state": defaultdict(float),
            }
        entry = agg[key]
        entry["donor_names_seen"].add(name)
        amount: float = d.get("donation_amount", 0.0)
        entry["total_donated_brl"] += amount
        entry["donation_count"] += 1
        originator = d.get("donor_name_originator_normalized", "")
        if originator and not entry["donor_name_originator"]:
            entry["donor_name_originator"] = originator
        year = d.get("election_year")
        if year is not None:
            entry["election_years"].add(year)
            all_years.add(int(year))
        party = d.get("party_abbrev", "")
        if party:
            entry["parties_donated_to"].add(party)
        candidate = d.get("candidate_name", "")
        if candidate:
            entry["candidates_donated_to"].add(candidate)
        position = d.get("position", "")
        if position:
            entry["positions_donated_to"].add(position)
        rc = classify_resource_type(d.get("donation_description"))
        entry["resource_types_seen"].add(rc.category)

        # Temporal / concentration tracking
        if amount > entry["max_single_donation_brl"]:
            entry["max_single_donation_brl"] = amount
        donation_date = d.get("donation_date", "")
        if isinstance(donation_date, str) and _ISO_DATE_RE.match(donation_date):
            if entry["min_donation_date"] is None or donation_date < entry["min_donation_date"]:
                entry["min_donation_date"] = donation_date
            if entry["max_donation_date"] is None or donation_date > entry["max_donation_date"]:
                entry["max_donation_date"] = donation_date
        if candidate:
            entry["amount_by_candidate"][candidate] += amount
        if party:
            entry["amount_by_party"][party] += amount
        state = d.get("state", "")
        if state:
            entry["amount_by_state"][state] += amount

    # Derive recent_cycles from corpus-wide election years
    sorted_all_years = sorted(all_years)
    recent_cycles = sorted_all_years[-2:] if len(sorted_all_years) >= 2 else sorted_all_years

    # Convert sets to sorted lists and compute derived metrics
    for entry in agg.values():
        entry["election_years"] = sorted(entry["election_years"])
        entry["parties_donated_to"] = sorted(entry["parties_donated_to"])
        entry["candidates_donated_to"] = sorted(entry["candidates_donated_to"])
        entry["positions_donated_to"] = sorted(entry["positions_donated_to"])
        entry["donor_names_seen"] = sorted(entry["donor_names_seen"])
        entry["resource_types_seen"] = sorted(entry["resource_types_seen"])

        # Temporal metrics
        entry["first_donation_date"] = entry.pop("min_donation_date")
        entry["last_donation_date"] = entry.pop("max_donation_date")
        entry["active_election_year_count"] = len(entry["election_years"])

        total = entry["total_donated_brl"]
        count = entry["donation_count"]
        entry["avg_donation_brl"] = round(total / count, 2) if count > 0 else 0.0

        # Concentration shares
        amt_cand = entry.pop("amount_by_candidate")
        amt_party = entry.pop("amount_by_party")
        amt_state = entry.pop("amount_by_state")
        entry["top_candidate_share"] = round(max(amt_cand.values()) / total, 4) if amt_cand and total > 0 else None
        entry["top_party_share"] = round(max(amt_party.values()) / total, 4) if amt_party and total > 0 else None
        entry["top_state_share"] = round(max(amt_state.values()) / total, 4) if amt_state and total > 0 else None

        # Year span
        years = entry["election_years"]
        entry["donation_year_span"] = (years[-1] - years[0] + 1) if years else None

        # Recent donation flag
        entry["recent_donation_flag"] = bool(set(years) & set(recent_cycles)) if years and recent_cycles else False

    return agg, raw_count, all_years


def _build_ambiguous_record(
    donor_key: str,
    donor_info: dict[str, Any],
    match: EntityMatchResult,
    *,
    entity_type: str,
) -> dict[str, Any]:
    return {
        "donor_identity_key": donor_key,
        "donor_name_normalized": donor_info["donor_name_normalized"],
        "donor_cpf_cnpj": donor_info.get("donor_cpf_cnpj", ""),
        "entity_type": entity_type,
        "match_strategy": match.strategy,
        "match_score": match.score,
        "uncertainty_note": match.uncertainty_note,
        "candidate_count": match.candidate_count,
        "sample_candidate_name": (
            match.record.get("party_name_normalized")
            or match.record.get("counsel_name_normalized")
            or ""
        ),
        "total_donated_brl": donor_info["total_donated_brl"],
        "donation_count": donor_info["donation_count"],
        "election_years": sorted(donor_info["election_years"]),
    }


def build_donation_matches(  # noqa: C901
    *,
    tse_dir: Path = DEFAULT_TSE_DIR,
    party_path: Path = DEFAULT_PARTY_PATH,
    counsel_path: Path = DEFAULT_COUNSEL_PATH,
    process_path: Path = DEFAULT_PROCESS_PATH,
    decision_event_path: Path = DEFAULT_DECISION_EVENT_PATH,
    process_party_link_path: Path = DEFAULT_PROCESS_PARTY_LINK_PATH,
    process_counsel_link_path: Path = DEFAULT_PROCESS_COUNSEL_LINK_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    alias_path: Path = DEFAULT_ALIAS_PATH,
) -> Path:
    """Build donation match analytics from TSE raw data + curated entities."""
    output_dir.mkdir(parents=True, exist_ok=True)

    donations_path = tse_dir / "donations_raw.jsonl"
    if not donations_path.exists():
        logger.warning("No donations_raw.jsonl found in %s", tse_dir)
        return output_dir

    # Stream-aggregate donations (never loads full file into RAM)
    donor_agg, raw_count, all_years = _stream_aggregate_donations(donations_path)
    logger.info("Streamed %d raw donation records, aggregated to %d unique donors", raw_count, len(donor_agg))

    # Build indices
    party_records = read_jsonl(party_path)
    party_index = build_party_index(party_path)
    party_match_index = build_entity_match_index(
        party_records,
        name_field="party_name_normalized",
        alias_path=alias_path,
        entity_kind="party",
    )

    party_id_to_name: dict[str, str] = {}
    for norm_name, record in party_index.items():
        pid = record.get("party_id", "")
        if pid:
            party_id_to_name[pid] = norm_name

    process_party_map = build_party_process_map(process_party_link_path, party_id_to_name)
    process_outcomes = build_process_outcomes(decision_event_path)
    stratified_rates, fallback_rates = build_baseline_rates_stratified(decision_event_path, process_path)
    process_jb_map = build_process_jb_category_map(decision_event_path)

    process_class_map: dict[str, str] = {}
    for record in read_jsonl(process_path):
        pid = record.get("process_id")
        pc = record.get("process_class")
        if pid and pc:
            process_class_map[pid] = pc

    # --- Match donors to parties (parallel across CPU cores) ---
    match_items: list[tuple[str, str | None]] = [
        (donor_info["donor_name_normalized"], donor_info.get("donor_cpf_cnpj")) for donor_info in donor_agg.values()
    ]
    match_results = match_entities_parallel(
        match_items,
        index=party_match_index,
        name_field="party_name_normalized",
    )

    matches: list[dict[str, Any]] = []
    matched_party_names: set[str] = set()
    party_ambiguous_count = 0
    match_strategy_counts: dict[str, int] = defaultdict(int)
    now_iso = datetime.now(timezone.utc).isoformat()
    ambiguous_records: list[dict[str, Any]] = []

    for donor_key, donor_info in donor_agg.items():
        donor_name = donor_info["donor_name_normalized"]
        match = match_results.get(donor_name)
        if match is None:
            continue
        if match.strategy == "ambiguous":
            party_ambiguous_count += 1
            ambiguous_records.append(_build_ambiguous_record(donor_key, donor_info, match, entity_type="party"))
            continue

        party = match.record
        party_id = party.get("party_id", "")
        party_name = str(party.get("party_name_normalized") or donor_name)
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

        baseline_rate: float | None = None
        if class_jb_pairs:
            most_common_class, most_common_jb = Counter(class_jb_pairs).most_common(1)[0][0]
            baseline_rate = lookup_baseline_rate(stratified_rates, fallback_rates, most_common_class, most_common_jb)

        delta: float | None = None
        red_flag = False
        if favorable_rate is not None and baseline_rate is not None:
            delta = favorable_rate - baseline_rate
            red_flag = delta > RED_FLAG_DELTA_THRESHOLD and len(seen_pids) >= MIN_CASES_FOR_RED_FLAG

        red_flag_substantive: bool | None = None
        if favorable_rate_sub is not None and baseline_rate is not None and n_substantive >= MIN_CASES_FOR_RED_FLAG:
            red_flag_substantive = (favorable_rate_sub - baseline_rate) > RED_FLAG_DELTA_THRESHOLD

        power = red_flag_power(len(seen_pids), baseline_rate) if baseline_rate is not None else None
        confidence = red_flag_confidence_label(power)

        match_id = stable_id("dm-", f"{party_id}:{donor_key}")
        matches.append(
            {
                "match_id": match_id,
                "entity_type": "party",
                "entity_id": party_id,
                "entity_name_normalized": party_name,
                "party_id": party_id,
                "party_name_normalized": party_name,
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

    # --- Match donors to counsel (parallel across CPU cores) ---
    counsel_records = read_jsonl(counsel_path)
    counsel_index: dict[str, dict[str, Any]] = {}
    counsel_id_to_name: dict[str, str] = {}
    for record in counsel_records:
        norm = normalize_entity_name(record.get("counsel_name_normalized") or record.get("counsel_name_raw", ""))
        if norm:
            counsel_index.setdefault(norm, record)
            cid = record.get("counsel_id", "")
            if cid:
                counsel_id_to_name[cid] = norm

    counsel_match_index = build_entity_match_index(
        counsel_records,
        name_field="counsel_name_normalized",
        alias_path=alias_path,
        entity_kind="counsel",
    )

    counsel_match_items: list[tuple[str, str | None]] = [
        (donor_info["donor_name_normalized"], donor_info.get("donor_cpf_cnpj")) for donor_info in donor_agg.values()
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
            red_flag = delta > RED_FLAG_DELTA_THRESHOLD and len(seen_pids) >= MIN_CASES_FOR_RED_FLAG

        red_flag_substantive: bool | None = None
        if favorable_rate_sub is not None and baseline_rate is not None and n_substantive >= MIN_CASES_FOR_RED_FLAG:
            red_flag_substantive = (favorable_rate_sub - baseline_rate) > RED_FLAG_DELTA_THRESHOLD

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

    # Corporate enrichment (optional post-processing)
    from ._corporate_enrichment import build_corporate_enrichment_index, enrich_match_corporate

    corp_index = build_corporate_enrichment_index(output_dir)
    for m in matches:
        enrich_match_corporate(m, corp_index)
    corporate_enriched_count = sum(
        1
        for m in matches
        if any(
            m.get(f) is not None
            for f in (
                "donor_document_type",
                "donor_tax_id_normalized",
                "donor_cnpj_basico",
                "donor_company_name",
                "economic_group_id",
                "economic_group_member_count",
                "is_law_firm_group",
                "donor_group_has_minister_partner",
                "donor_group_has_party_partner",
                "donor_group_has_counsel_partner",
                "min_link_degree_to_minister",
                "corporate_link_red_flag",
            )
        )
    )

    # Write all donation matches (party + counsel)
    match_path = output_dir / "donation_match.jsonl"
    with AtomicJsonlWriter(match_path) as fh:
        for m in matches:
            fh.write(json.dumps(m, ensure_ascii=False) + "\n")

    # Write ambiguous match trail
    ambiguous_path = output_dir / "donation_match_ambiguous.jsonl"
    with AtomicJsonlWriter(ambiguous_path) as afh:
        for rec in ambiguous_records:
            afh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    logger.info("Wrote %d ambiguous match records to %s", len(ambiguous_records), ambiguous_path)

    # Build matched donor identity keys for event extraction
    matched_identity_keys: dict[str, str] = {}
    for m in matches:
        dk = m.get("donor_identity_key", "")
        if dk:
            matched_identity_keys[dk] = m["match_id"]

    # Write individual donation events for matched donors (P3: temporal granularity)
    event_path = output_dir / "donation_event.jsonl"
    event_count = 0
    resource_category_counts: dict[str, int] = defaultdict(int)
    resource_subtype_counts: dict[str, int] = defaultdict(int)
    with AtomicJsonlWriter(event_path) as efh:
        for d in iter_jsonl(donations_path):
            name = d.get("donor_name_normalized", "")
            if not name:
                continue
            cpf_cnpj = d.get("donor_cpf_cnpj", "")
            dk = _donor_identity_key(name, cpf_cnpj)
            match_id = matched_identity_keys.get(dk)
            if match_id is None:
                continue
            rc = classify_resource_type(d.get("donation_description"))
            resource_category_counts[rc.category] += 1
            resource_subtype_counts[rc.subtype] += 1
            event_id = stable_id(
                "de-",
                f"{dk}:{d.get('election_year', '')}:{d.get('donation_date', '')}:"
                f"{d.get('donation_amount', '')}:{d.get('candidate_name', '')}",
            )
            event = {
                "event_id": event_id,
                "match_id": match_id,
                "donor_identity_key": dk,
                "election_year": d.get("election_year"),
                "donation_date": d.get("donation_date", ""),
                "donation_amount": d.get("donation_amount", 0.0),
                "candidate_name": d.get("candidate_name", ""),
                "party_abbrev": d.get("party_abbrev", ""),
                "position": d.get("position", ""),
                "state": d.get("state", ""),
                "donor_name": d.get("donor_name", ""),
                "donor_name_originator": d.get("donor_name_originator", ""),
                "donor_cpf_cnpj": cpf_cnpj,
                "donation_description": d.get("donation_description", ""),
                "resource_type_category": rc.category,
                "resource_type_subtype": rc.subtype,
                "resource_classification_confidence": rc.confidence,
                "resource_classification_rule": rc.rule,
                "source_file": d.get("source_file", ""),
                "source_url": d.get("source_url", ""),
                "collected_at": d.get("collected_at", ""),
                "ingest_run_id": d.get("ingest_run_id", ""),
                "record_hash": d.get("record_hash", ""),
            }
            efh.write(json.dumps(event, ensure_ascii=False) + "\n")
            event_count += 1
    logger.info("Wrote %d individual donation events to %s", event_count, event_path)

    # Build counsel donation profiles (indirect, via party clients)
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
    # Rename keys to match donation-specific schema.
    counsel_profiles: list[dict[str, Any]] = [
        {
            "counsel_id": p["counsel_id"],
            "counsel_name_normalized": p["counsel_name_normalized"],
            "donor_client_count": p["flagged_client_count"],
            "total_client_count": p["total_client_count"],
            "donor_client_rate": p["flagged_client_rate"],
            "donor_client_favorable_rate": p["flagged_favorable_rate"],
            "overall_favorable_rate": p["overall_favorable_rate"],
            "red_flag": p["red_flag"],
        }
        for p in raw_profiles
    ]

    counsel_path_out = output_dir / "counsel_donation_profile.jsonl"
    with AtomicJsonlWriter(counsel_path_out) as fh:
        for cp in counsel_profiles:
            fh.write(json.dumps(cp, ensure_ascii=False) + "\n")

    # Compute total donated for matched parties
    party_matches = [m for m in matches if m["entity_type"] == "party"]
    counsel_direct_matches = [m for m in matches if m["entity_type"] == "counsel"]
    total_donated_matched = sum(m["total_donated_brl"] for m in party_matches)

    # Write summary
    summary = {
        "total_donations_raw": raw_count,
        "unique_donors": len(donor_agg),
        "matched_party_count": len(matched_party_names),
        "matched_counsel_count": len(matched_counsel_names),
        "donation_match_count": len(matches),
        "party_match_count": len(party_matches),
        "counsel_match_count": len(counsel_direct_matches),
        "party_red_flag_count": sum(1 for m in party_matches if m["red_flag"]),
        "counsel_direct_red_flag_count": sum(1 for m in counsel_direct_matches if m["red_flag"]),
        "counsel_profile_count": len(counsel_profiles),
        "counsel_red_flag_count": sum(1 for cp in counsel_profiles if cp["red_flag"]),
        "total_donated_brl_matched": total_donated_matched,
        "matched_by_tax_id_count": match_strategy_counts.get("tax_id", 0),
        "matched_by_alias_count": match_strategy_counts.get("alias", 0),
        "matched_by_similarity_count": (
            match_strategy_counts.get("jaccard", 0) + match_strategy_counts.get("levenshtein", 0)
        ),
        "party_ambiguous_candidate_count": party_ambiguous_count,
        "counsel_ambiguous_candidate_count": counsel_ambiguous_count,
        "total_ambiguous_candidate_count": party_ambiguous_count + counsel_ambiguous_count,
        "ambiguous_records_written": len(ambiguous_records),
        "corporate_links_present": corp_index.has_corporate_links,
        "economic_groups_present": corp_index.has_economic_groups,
        "corporate_network_present": corp_index.has_corporate_network,
        "corporate_enriched_count": corporate_enriched_count,
        "donation_event_count": event_count,
        "election_years_covered": sorted(all_years),
        "resource_category_counts": dict(resource_category_counts),
        "resource_subtype_counts": dict(resource_subtype_counts),
        "resource_classification_unknown_count": resource_category_counts.get("unknown", 0),
        "resource_classification_empty_count": resource_category_counts.get("empty", 0),
        "resource_classification_coverage_rate": (
            round((event_count - resource_category_counts.get("unknown", 0)) / event_count, 4)
            if event_count > 0
            else 0.0
        ),
        "resource_classification_nonempty_coverage_rate": (
            round(
                (nonempty_total - resource_category_counts.get("unknown", 0)) / nonempty_total,
                4,
            )
            if (nonempty_total := event_count - resource_category_counts.get("empty", 0)) > 0
            else 0.0
        ),
        "generated_at": now_iso,
    }
    summary_path = output_dir / "donation_match_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info(
        "Built donation matches: %d party + %d counsel matches, %d counsel profiles",
        len(party_matches),
        len(counsel_direct_matches),
        len(counsel_profiles),
    )
    return match_path
