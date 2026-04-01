from __future__ import annotations

from typing import Any


def build_donation_match_summary(
    *,
    matches: list[dict[str, Any]],
    raw_count: int,
    unique_donor_count: int,
    matched_party_names: set[str],
    matched_counsel_names: set[str],
    counsel_profiles: list[dict[str, Any]],
    match_strategy_counts: dict[str, int],
    party_ambiguous_count: int,
    counsel_ambiguous_count: int,
    ambiguous_records: list[dict[str, Any]],
    corp_index: Any,
    corporate_enriched_count: int,
    event_count: int,
    sorted_years: list[int],
    resource_category_counts: dict[str, int],
    resource_subtype_counts: dict[str, int],
    now_iso: str,
) -> dict[str, Any]:
    party_matches = [m for m in matches if m["entity_type"] == "party"]
    counsel_direct_matches = [m for m in matches if m["entity_type"] == "counsel"]
    total_donated_matched = sum(m["total_donated_brl"] for m in party_matches)

    return {
        "total_donations_raw": raw_count,
        "unique_donors": unique_donor_count,
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
        "election_years_covered": sorted_years,
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
