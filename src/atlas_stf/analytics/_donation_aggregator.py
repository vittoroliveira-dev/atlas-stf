"""Donation stream aggregation helpers for donation match analytics."""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path
from typing import Any

from ..tse._resource_classifier import classify_resource_type
from ._donor_identity import donor_identity_key as _donor_identity_key_fn
from ._match_helpers import EntityMatchResult
from ._match_io import iter_jsonl

__all__ = [
    "_build_ambiguous_record",
    "_donor_identity_key",
    "_stream_aggregate_donations",
]


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
            match.record.get("party_name_normalized") or match.record.get("counsel_name_normalized") or ""
        ),
        "total_donated_brl": donor_info["total_donated_brl"],
        "donation_count": donor_info["donation_count"],
        "election_years": sorted(donor_info["election_years"]),
    }
