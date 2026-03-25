from __future__ import annotations

import json
import logging
from hashlib import sha256
from pathlib import Path

from ._builder_loaders_analytics_common import match_confidence
from ._builder_utils import _coerce_bool, _coerce_float, _coerce_int, _parse_date, _parse_datetime, _read_jsonl
from .models import (
    ServingCounselDonationProfile,
    ServingCounselSanctionProfile,
    ServingDonationEvent,
    ServingDonationMatch,
    ServingSanctionCorporateLink,
    ServingSanctionMatch,
)

logger = logging.getLogger(__name__)


def load_sanction_matches(
    analytics_dir: Path,
) -> tuple[list[ServingSanctionMatch], list[ServingCounselSanctionProfile]]:
    sanction_matches: list[ServingSanctionMatch] = []
    sm_path = analytics_dir / "sanction_match.jsonl"
    if sm_path.exists():
        seen: set[str] = set()
        for record in _read_jsonl(sm_path):
            mid = str(record.get("match_id", ""))
            if mid in seen:
                continue
            seen.add(mid)
            strategy = record.get("match_strategy")
            sanction_matches.append(
                ServingSanctionMatch(
                    match_id=str(record.get("match_id", "")),
                    entity_type=str(record.get("entity_type", "party")),
                    party_id=str(record.get("party_id") or record.get("entity_id") or ""),
                    party_name_normalized=str(
                        record.get("party_name_normalized") or record.get("entity_name_normalized") or ""
                    ),
                    sanction_source=str(record.get("sanction_source", "")),
                    sanction_id=str(record.get("sanction_id", "")),
                    sanctioning_body=record.get("sanctioning_body"),
                    sanction_type=record.get("sanction_type"),
                    sanction_start_date=record.get("sanction_start_date"),
                    sanction_end_date=record.get("sanction_end_date"),
                    sanction_description=record.get("sanction_description"),
                    stf_case_count=_coerce_int(record.get("stf_case_count")),
                    favorable_rate=record.get("favorable_rate"),
                    baseline_favorable_rate=record.get("baseline_favorable_rate"),
                    favorable_rate_delta=record.get("favorable_rate_delta"),
                    red_flag=_coerce_bool(record.get("red_flag")),
                    red_flag_power=_coerce_float(record.get("red_flag_power")),
                    red_flag_confidence=record.get("red_flag_confidence"),
                    match_strategy=strategy,
                    match_score=_coerce_float(record.get("match_score")),
                    match_confidence=match_confidence(strategy),
                    matched_at=_parse_datetime(record.get("matched_at")),
                )
            )

    counsel_sanction_profiles: list[ServingCounselSanctionProfile] = []
    csp_path = analytics_dir / "counsel_sanction_profile.jsonl"
    if csp_path.exists():
        for record in _read_jsonl(csp_path):
            counsel_sanction_profiles.append(
                ServingCounselSanctionProfile(
                    counsel_id=str(record.get("counsel_id", "")),
                    counsel_name_normalized=str(record.get("counsel_name_normalized", "")),
                    sanctioned_client_count=_coerce_int(record.get("sanctioned_client_count")),
                    total_client_count=_coerce_int(record.get("total_client_count")),
                    sanctioned_client_rate=float(record.get("sanctioned_client_rate", 0.0)),
                    sanctioned_favorable_rate=record.get("sanctioned_favorable_rate"),
                    overall_favorable_rate=record.get("overall_favorable_rate"),
                    red_flag=_coerce_bool(record.get("red_flag")),
                )
            )
    return sanction_matches, counsel_sanction_profiles


def load_sanction_corporate_links(analytics_dir: Path) -> list[ServingSanctionCorporateLink]:
    scl_path = analytics_dir / "sanction_corporate_link.jsonl"
    if not scl_path.exists():
        return []
    results: list[ServingSanctionCorporateLink] = []
    seen: set[str] = set()
    for record in _read_jsonl(scl_path):
        lid = str(record.get("link_id", ""))
        if lid in seen:
            continue
        seen.add(lid)
        results.append(
            ServingSanctionCorporateLink(
                link_id=lid,
                sanction_id=str(record.get("sanction_id", "")),
                sanction_source=str(record.get("sanction_source", "")),
                sanction_entity_name=str(record.get("sanction_entity_name", "")),
                sanction_entity_tax_id=record.get("sanction_entity_tax_id"),
                sanction_type=record.get("sanction_type"),
                bridge_company_cnpj_basico=str(record.get("bridge_company_cnpj_basico", "")),
                bridge_company_name=record.get("bridge_company_name"),
                bridge_link_basis=str(record.get("bridge_link_basis", "")),
                bridge_confidence=str(record.get("bridge_confidence", "deterministic")),
                bridge_partner_role=record.get("bridge_partner_role"),
                bridge_qualification_code=record.get("bridge_qualification_code"),
                bridge_qualification_label=record.get("bridge_qualification_label"),
                economic_group_id=record.get("economic_group_id"),
                economic_group_member_count=_coerce_int(record.get("economic_group_member_count"))
                if record.get("economic_group_member_count") is not None
                else None,
                is_law_firm_group=_coerce_bool(record.get("is_law_firm_group"))
                if record.get("is_law_firm_group") is not None
                else None,
                stf_entity_type=str(record.get("stf_entity_type", "")),
                stf_entity_id=str(record.get("stf_entity_id", "")),
                stf_entity_name=str(record.get("stf_entity_name", "")),
                stf_match_strategy=record.get("stf_match_strategy"),
                stf_match_score=_coerce_float(record.get("stf_match_score")),
                stf_match_confidence=record.get("stf_match_confidence"),
                matched_alias=record.get("matched_alias"),
                matched_tax_id=record.get("matched_tax_id"),
                uncertainty_note=record.get("uncertainty_note"),
                link_degree=_coerce_int(record.get("link_degree")) or 2,
                stf_process_count=_coerce_int(record.get("stf_process_count")),
                favorable_rate=_coerce_float(record.get("favorable_rate")),
                baseline_favorable_rate=_coerce_float(record.get("baseline_favorable_rate")),
                favorable_rate_delta=_coerce_float(record.get("favorable_rate_delta")),
                risk_score=_coerce_float(record.get("risk_score")),
                red_flag=_coerce_bool(record.get("red_flag")),
                red_flag_power=_coerce_float(record.get("red_flag_power")),
                red_flag_confidence=record.get("red_flag_confidence"),
                evidence_chain_json=json.dumps(record.get("evidence_chain", []), ensure_ascii=False),
                source_datasets_json=json.dumps(record.get("source_datasets", []), ensure_ascii=False),
                record_hash=record.get("record_hash"),
                generated_at=_parse_datetime(record.get("generated_at")),
            )
        )
    return results


def load_donation_matches(
    analytics_dir: Path,
) -> tuple[list[ServingDonationMatch], list[ServingCounselDonationProfile]]:
    donation_matches: list[ServingDonationMatch] = []
    dm_path = analytics_dir / "donation_match.jsonl"
    if dm_path.exists():
        for record in _read_jsonl(dm_path):
            strategy = record.get("match_strategy")
            entity_id = str(record.get("entity_id") or "")
            donation_matches.append(
                ServingDonationMatch(
                    match_id=str(record.get("match_id", "")),
                    entity_type=str(record.get("entity_type", "party")),
                    entity_id=entity_id,
                    party_id=str(record.get("party_id") or entity_id),
                    party_name_normalized=str(
                        record.get("party_name_normalized") or record.get("entity_name_normalized") or ""
                    ),
                    donor_cpf_cnpj=str(record.get("donor_cpf_cnpj", "")),
                    donor_name_normalized=str(record.get("donor_name_normalized") or ""),
                    donor_name_originator=str(record.get("donor_name_originator") or ""),
                    total_donated_brl=float(record.get("total_donated_brl", 0.0)),
                    donation_count=_coerce_int(record.get("donation_count")),
                    election_years_json=json.dumps(record.get("election_years", []), ensure_ascii=False),
                    parties_donated_to_json=json.dumps(record.get("parties_donated_to", []), ensure_ascii=False),
                    candidates_donated_to_json=json.dumps(record.get("candidates_donated_to", []), ensure_ascii=False),
                    positions_donated_to_json=json.dumps(record.get("positions_donated_to", []), ensure_ascii=False),
                    stf_case_count=_coerce_int(record.get("stf_case_count")),
                    favorable_rate=record.get("favorable_rate"),
                    favorable_rate_substantive=_coerce_float(record.get("favorable_rate_substantive")),
                    substantive_decision_count=_coerce_int(record.get("substantive_decision_count")),
                    baseline_favorable_rate=record.get("baseline_favorable_rate"),
                    favorable_rate_delta=record.get("favorable_rate_delta"),
                    red_flag=_coerce_bool(record.get("red_flag")),
                    red_flag_substantive=_coerce_bool(record.get("red_flag_substantive")),
                    red_flag_power=_coerce_float(record.get("red_flag_power")),
                    red_flag_confidence=record.get("red_flag_confidence"),
                    match_strategy=strategy,
                    match_score=_coerce_float(record.get("match_score")),
                    match_confidence=match_confidence(strategy),
                    matched_alias=str(record.get("matched_alias") or ""),
                    matched_tax_id=str(record.get("matched_tax_id") or ""),
                    uncertainty_note=str(record.get("uncertainty_note") or ""),
                    matched_at=_parse_datetime(record.get("matched_at")),
                    donor_identity_key=str(record.get("donor_identity_key") or ""),
                    resource_types_observed_json=json.dumps(
                        record.get("resource_types_observed", []), ensure_ascii=False
                    ),
                    donor_document_type=record.get("donor_document_type"),
                    donor_tax_id_normalized=record.get("donor_tax_id_normalized"),
                    donor_cnpj_basico=record.get("donor_cnpj_basico"),
                    donor_company_name=record.get("donor_company_name"),
                    economic_group_id=record.get("economic_group_id"),
                    economic_group_member_count=_coerce_int(record.get("economic_group_member_count"))
                    if record.get("economic_group_member_count") is not None
                    else None,
                    is_law_firm_group=_coerce_bool(record.get("is_law_firm_group"))
                    if record.get("is_law_firm_group") is not None
                    else None,
                    donor_group_has_minister_partner=_coerce_bool(record.get("donor_group_has_minister_partner"))
                    if record.get("donor_group_has_minister_partner") is not None
                    else None,
                    donor_group_has_party_partner=_coerce_bool(record.get("donor_group_has_party_partner"))
                    if record.get("donor_group_has_party_partner") is not None
                    else None,
                    donor_group_has_counsel_partner=_coerce_bool(record.get("donor_group_has_counsel_partner"))
                    if record.get("donor_group_has_counsel_partner") is not None
                    else None,
                    min_link_degree_to_minister=_coerce_int(record.get("min_link_degree_to_minister"))
                    if record.get("min_link_degree_to_minister") is not None
                    else None,
                    corporate_link_red_flag=_coerce_bool(record.get("corporate_link_red_flag"))
                    if record.get("corporate_link_red_flag") is not None
                    else None,
                    first_donation_date=record.get("first_donation_date"),
                    last_donation_date=record.get("last_donation_date"),
                    active_election_year_count=_coerce_int(record.get("active_election_year_count")),
                    max_single_donation_brl=_coerce_float(record.get("max_single_donation_brl")) or 0.0,
                    avg_donation_brl=_coerce_float(record.get("avg_donation_brl")) or 0.0,
                    top_candidate_share=_coerce_float(record.get("top_candidate_share")),
                    top_party_share=_coerce_float(record.get("top_party_share")),
                    top_state_share=_coerce_float(record.get("top_state_share")),
                    donation_year_span=_coerce_int(record.get("donation_year_span"))
                    if record.get("donation_year_span") is not None
                    else None,
                    recent_donation_flag=_coerce_bool(record.get("recent_donation_flag")),
                )
            )

    counsel_donation_profiles: list[ServingCounselDonationProfile] = []
    cdp_path = analytics_dir / "counsel_donation_profile.jsonl"
    if cdp_path.exists():
        for record in _read_jsonl(cdp_path):
            counsel_donation_profiles.append(
                ServingCounselDonationProfile(
                    counsel_id=str(record.get("counsel_id", "")),
                    counsel_name_normalized=str(record.get("counsel_name_normalized", "")),
                    donor_client_count=_coerce_int(record.get("donor_client_count")),
                    total_client_count=_coerce_int(record.get("total_client_count")),
                    donor_client_rate=float(record.get("donor_client_rate", 0.0)),
                    donor_client_favorable_rate=record.get("donor_client_favorable_rate"),
                    overall_favorable_rate=record.get("overall_favorable_rate"),
                    red_flag=_coerce_bool(record.get("red_flag")),
                )
            )
    return donation_matches, counsel_donation_profiles


def _record_content_hash(record: dict[str, object], exclude_key: str) -> str:
    """Hash record content excluding a specific key, for duplicate classification."""
    filtered = {k: v for k, v in sorted(record.items()) if k != exclude_key}
    return sha256(json.dumps(filtered, ensure_ascii=False, sort_keys=True).encode()).hexdigest()[:16]


def load_donation_events(analytics_dir: Path) -> list[ServingDonationEvent]:
    de_path = analytics_dir / "donation_event.jsonl"
    if not de_path.exists():
        return []
    events: list[ServingDonationEvent] = []
    seen: dict[str, str] = {}
    exact_dupes = 0
    conflicts = 0
    for record in _read_jsonl(de_path):
        eid = str(record.get("event_id", ""))
        if not eid:
            continue
        content_hash = _record_content_hash(record, exclude_key="event_id")
        if eid in seen:
            if seen[eid] == content_hash:
                exact_dupes += 1
            else:
                conflicts += 1
                logger.warning(
                    "Donation event conflict: event_id=%s has divergent content (kept first occurrence)",
                    eid,
                )
            continue
        seen[eid] = content_hash
        events.append(
            ServingDonationEvent(
                event_id=eid,
                match_id=str(record.get("match_id", "")),
                election_year=_coerce_int(record.get("election_year")),
                donation_date=_parse_date(record.get("donation_date")),
                donation_amount=float(record.get("donation_amount", 0.0)),
                candidate_name=str(record.get("candidate_name") or ""),
                party_abbrev=str(record.get("party_abbrev") or ""),
                position=str(record.get("position") or ""),
                state=str(record.get("state") or ""),
                donor_name=str(record.get("donor_name") or ""),
                donor_name_originator=str(record.get("donor_name_originator") or ""),
                donor_cpf_cnpj=str(record.get("donor_cpf_cnpj") or ""),
                donation_description=str(record.get("donation_description") or ""),
                donor_identity_key=str(record.get("donor_identity_key") or ""),
                resource_type_category=record.get("resource_type_category"),
                resource_type_subtype=record.get("resource_type_subtype"),
                resource_classification_confidence=record.get("resource_classification_confidence"),
                resource_classification_rule=record.get("resource_classification_rule"),
                source_file=record.get("source_file"),
                collected_at=record.get("collected_at"),
                source_url=record.get("source_url"),
                ingest_run_id=record.get("ingest_run_id"),
                record_hash=record.get("record_hash"),
            )
        )
    if exact_dupes > 0 or conflicts > 0:
        logger.info(
            "Donation events: %d loaded, %d exact duplicates skipped, %d conflicts (kept first)",
            len(events),
            exact_dupes,
            conflicts,
        )
    return events
