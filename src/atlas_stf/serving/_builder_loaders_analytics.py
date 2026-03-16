from __future__ import annotations

import json
from pathlib import Path

from ._builder_utils import (
    _coerce_bool,
    _coerce_float,
    _coerce_int,
    _parse_date,
    _parse_datetime,
    _read_json,
    _read_jsonl,
)
from .models import (
    ServingAssignmentAudit,
    ServingCompoundRisk,
    ServingCounselAffinity,
    ServingCounselDonationProfile,
    ServingCounselNetworkCluster,
    ServingCounselSanctionProfile,
    ServingDecisionVelocity,
    ServingDonationEvent,
    ServingDonationMatch,
    ServingMinisterBio,
    ServingMlOutlierScore,
    ServingOriginContext,
    ServingPaymentCounterparty,
    ServingRapporteurChange,
    ServingRapporteurProfile,
    ServingSanctionCorporateLink,
    ServingSanctionMatch,
    ServingSequentialAnalysis,
    ServingTemporalAnalysis,
)

_MATCH_STRATEGY_TO_CONFIDENCE: dict[str, str] = {
    "tax_id": "deterministic",
    "alias": "exact_name",
    "exact": "exact_name",
    "canonical_name": "exact_name",
    "jaccard": "fuzzy",
    "levenshtein": "fuzzy",
    "ambiguous": "nominal_review_needed",
}


def _match_confidence(strategy: str | None) -> str:
    if not strategy:
        return "unknown"
    return _MATCH_STRATEGY_TO_CONFIDENCE.get(strategy, "unknown")


def load_rapporteur_profiles(analytics_dir: Path) -> list[ServingRapporteurProfile]:
    rp_path = analytics_dir / "rapporteur_profile.jsonl"
    if not rp_path.exists():
        return []
    results: list[ServingRapporteurProfile] = []
    for record in _read_jsonl(rp_path):
        results.append(
            ServingRapporteurProfile(
                rapporteur=str(record.get("rapporteur", "")),
                process_class=str(record.get("process_class", "")),
                thematic_key=str(record.get("thematic_key", "")),
                decision_year=_coerce_int(record.get("decision_year")),
                event_count=_coerce_int(record.get("event_count")),
                chi2_statistic=record.get("chi2_statistic"),
                p_value_approx=record.get("p_value_approx"),
                deviation_flag=_coerce_bool(record.get("deviation_flag")),
                deviation_direction=record.get("deviation_direction"),
                progress_distribution_json=json.dumps(
                    record.get("progress_distribution", {}),
                    ensure_ascii=False,
                ),
                group_progress_distribution_json=json.dumps(
                    record.get("group_progress_distribution", {}),
                    ensure_ascii=False,
                ),
                monocratic_event_count=_coerce_int(record.get("monocratic_event_count")),
                monocratic_favorable_rate=_coerce_float(record.get("monocratic_favorable_rate")),
                collegiate_event_count=_coerce_int(record.get("collegiate_event_count")),
                collegiate_favorable_rate=_coerce_float(record.get("collegiate_favorable_rate")),
                monocratic_blocking_flag=_coerce_bool(record.get("monocratic_blocking_flag")),
            )
        )
    return results


def load_sequential_analyses(analytics_dir: Path) -> list[ServingSequentialAnalysis]:
    seq_path = analytics_dir / "sequential_analysis.jsonl"
    if not seq_path.exists():
        return []
    results: list[ServingSequentialAnalysis] = []
    for record in _read_jsonl(seq_path):
        results.append(
            ServingSequentialAnalysis(
                rapporteur=str(record.get("rapporteur", "")),
                decision_year=_coerce_int(record.get("decision_year")),
                n_decisions=_coerce_int(record.get("n_decisions")),
                autocorrelation_lag1=float(record.get("autocorrelation_lag1", 0.0)),
                streak_effect_3=record.get("streak_effect_3"),
                streak_effect_5=record.get("streak_effect_5"),
                base_favorable_rate=float(record.get("base_favorable_rate", 0.0)),
                post_streak_favorable_rate_3=record.get("post_streak_favorable_rate_3"),
                post_streak_favorable_rate_5=record.get("post_streak_favorable_rate_5"),
                sequential_bias_flag=_coerce_bool(record.get("sequential_bias_flag")),
            )
        )
    return results


def load_temporal_analyses(analytics_dir: Path) -> list[ServingTemporalAnalysis]:
    ta_path = analytics_dir / "temporal_analysis.jsonl"
    if not ta_path.exists():
        return []
    results: list[ServingTemporalAnalysis] = []
    for record in _read_jsonl(ta_path):
        results.append(
            ServingTemporalAnalysis(
                record_id=str(record.get("record_id", "")),
                analysis_kind=str(record.get("analysis_kind", "")),
                rapporteur=record.get("rapporteur"),
                decision_month=record.get("decision_month"),
                decision_year=record.get("decision_year"),
                month_of_year=record.get("month_of_year"),
                process_class=record.get("process_class"),
                decision_count=_coerce_int(record.get("decision_count")),
                favorable_count=_coerce_int(record.get("favorable_count")),
                unfavorable_count=_coerce_int(record.get("unfavorable_count")),
                favorable_rate=_coerce_float(record.get("favorable_rate")),
                rolling_favorable_rate_6m=_coerce_float(record.get("rolling_favorable_rate_6m")),
                breakpoint_score=_coerce_float(record.get("breakpoint_score")),
                breakpoint_flag=record.get("breakpoint_flag"),
                current_favorable_rate=_coerce_float(
                    record.get("current_favorable_rate", record.get("favorable_rate"))
                ),
                prior_decision_count=record.get("prior_decision_count"),
                prior_favorable_rate=_coerce_float(record.get("prior_favorable_rate")),
                delta_vs_prior_year=_coerce_float(record.get("delta_vs_prior_year")),
                delta_vs_overall=_coerce_float(record.get("delta_vs_overall")),
                event_id=record.get("event_id"),
                event_type=record.get("event_type"),
                event_scope=record.get("event_scope"),
                event_date=_parse_date(record.get("event_date")),
                event_title=record.get("event_title"),
                source=record.get("source"),
                source_url=record.get("source_url"),
                status=record.get("status"),
                before_decision_count=record.get("before_decision_count"),
                before_favorable_rate=_coerce_float(record.get("before_favorable_rate")),
                after_decision_count=record.get("after_decision_count"),
                after_favorable_rate=_coerce_float(record.get("after_favorable_rate")),
                delta_before_after=_coerce_float(record.get("delta_before_after")),
                linked_entity_type=record.get("linked_entity_type"),
                linked_entity_id=record.get("linked_entity_id"),
                linked_entity_name=record.get("linked_entity_name"),
                company_cnpj_basico=record.get("company_cnpj_basico"),
                company_name=record.get("company_name"),
                link_degree=record.get("link_degree"),
                link_chain=record.get("link_chain"),
                link_start_date=_parse_date(record.get("link_start_date")),
                link_status=record.get("link_status"),
                generated_at=_parse_datetime(record.get("generated_at")),
            )
        )
    return results


def load_assignment_audits(analytics_dir: Path) -> list[ServingAssignmentAudit]:
    aa_path = analytics_dir / "assignment_audit.jsonl"
    if not aa_path.exists():
        return []
    results: list[ServingAssignmentAudit] = []
    for record in _read_jsonl(aa_path):
        results.append(
            ServingAssignmentAudit(
                process_class=str(record.get("process_class", "")),
                decision_year=_coerce_int(record.get("decision_year")),
                rapporteur_count=_coerce_int(record.get("rapporteur_count")),
                event_count=_coerce_int(record.get("event_count")),
                chi2_statistic=float(record.get("chi2_statistic", 0.0)),
                p_value_approx=float(record.get("p_value_approx", 1.0)),
                uniformity_flag=_coerce_bool(record.get("uniformity_flag")),
                most_overrepresented_rapporteur=record.get("most_overrepresented_rapporteur"),
                most_underrepresented_rapporteur=record.get("most_underrepresented_rapporteur"),
                rapporteur_distribution_json=json.dumps(record.get("rapporteur_distribution", {}), ensure_ascii=False),
            )
        )
    return results


def load_ml_outlier_scores(analytics_dir: Path) -> list[ServingMlOutlierScore]:
    ml_path = analytics_dir / "ml_outlier_score.jsonl"
    if not ml_path.exists():
        return []
    results: list[ServingMlOutlierScore] = []
    for record in _read_jsonl(ml_path):
        results.append(
            ServingMlOutlierScore(
                decision_event_id=str(record.get("decision_event_id", "")),
                comparison_group_id=str(record.get("comparison_group_id", "")),
                ml_anomaly_score=float(record.get("ml_anomaly_score", 0.0)),
                ml_rarity_score=float(record.get("ml_rarity_score", 0.0)),
                ensemble_score=(float(record["ensemble_score"]) if record.get("ensemble_score") is not None else None),
                n_features=_coerce_int(record.get("n_features")),
                n_samples=_coerce_int(record.get("n_samples")),
                generated_at=_parse_datetime(record.get("generated_at")),
            )
        )
    return results


def load_origin_contexts(analytics_dir: Path) -> list[ServingOriginContext]:
    oc_path = analytics_dir / "origin_context.jsonl"
    if not oc_path.exists():
        return []
    results: list[ServingOriginContext] = []
    for record in _read_jsonl(oc_path):
        results.append(
            ServingOriginContext(
                origin_index=str(record.get("origin_index", "")),
                tribunal_label=str(record.get("tribunal_label", "")),
                state=str(record.get("state", "")),
                datajud_total_processes=_coerce_int(record.get("datajud_total_processes")),
                stf_process_count=_coerce_int(record.get("stf_process_count")),
                stf_share_pct=float(record.get("stf_share_pct", 0.0)),
                top_assuntos_json=json.dumps(record.get("top_assuntos", []), ensure_ascii=False),
                top_orgaos_julgadores_json=json.dumps(record.get("top_orgaos_julgadores", []), ensure_ascii=False),
                class_distribution_json=json.dumps(record.get("class_distribution", []), ensure_ascii=False),
                generated_at=_parse_datetime(record.get("generated_at")),
            )
        )
    return results


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
                    match_confidence=_match_confidence(strategy),
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
                    match_confidence=_match_confidence(strategy),
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


def load_donation_events(analytics_dir: Path) -> list[ServingDonationEvent]:
    de_path = analytics_dir / "donation_event.jsonl"
    if not de_path.exists():
        return []
    events: list[ServingDonationEvent] = []
    for record in _read_jsonl(de_path):
        events.append(
            ServingDonationEvent(
                event_id=str(record.get("event_id", "")),
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
    return events


def load_counsel_affinities(analytics_dir: Path) -> list[ServingCounselAffinity]:
    ca_path = analytics_dir / "counsel_affinity.jsonl"
    if not ca_path.exists():
        return []
    results: list[ServingCounselAffinity] = []
    seen: set[str] = set()
    for record in _read_jsonl(ca_path):
        aid = str(record.get("affinity_id", ""))
        if aid in seen:
            continue
        seen.add(aid)
        results.append(
            ServingCounselAffinity(
                affinity_id=aid,
                rapporteur=str(record.get("rapporteur", "")),
                counsel_id=str(record.get("counsel_id", "")),
                counsel_name_normalized=str(record.get("counsel_name_normalized", "")),
                shared_case_count=_coerce_int(record.get("shared_case_count")),
                favorable_count=_coerce_int(record.get("favorable_count")),
                unfavorable_count=_coerce_int(record.get("unfavorable_count")),
                pair_favorable_rate=record.get("pair_favorable_rate"),
                minister_baseline_favorable_rate=record.get("minister_baseline_favorable_rate"),
                counsel_baseline_favorable_rate=record.get("counsel_baseline_favorable_rate"),
                pair_delta_vs_minister=record.get("pair_delta_vs_minister"),
                pair_delta_vs_counsel=record.get("pair_delta_vs_counsel"),
                red_flag=_coerce_bool(record.get("red_flag")),
                top_process_classes_json=json.dumps(record.get("top_process_classes", []), ensure_ascii=False),
                generated_at=_parse_datetime(record.get("generated_at")),
            )
        )
    return results


def load_compound_risks(analytics_dir: Path) -> list[ServingCompoundRisk]:
    cr_path = analytics_dir / "compound_risk.jsonl"
    if not cr_path.exists():
        return []
    results: list[ServingCompoundRisk] = []
    seen: set[str] = set()
    for record in _read_jsonl(cr_path):
        pair_id = str(record.get("pair_id", ""))
        if pair_id in seen:
            continue
        seen.add(pair_id)
        results.append(
            ServingCompoundRisk(
                pair_id=pair_id,
                minister_name=str(record.get("minister_name", "")),
                entity_type=str(record.get("entity_type", "")),
                entity_id=str(record.get("entity_id", "")),
                entity_name=str(record.get("entity_name", "")),
                signal_count=_coerce_int(record.get("signal_count")),
                signals_json=json.dumps(record.get("signals", []), ensure_ascii=False),
                red_flag=_coerce_bool(record.get("red_flag")),
                shared_process_count=_coerce_int(record.get("shared_process_count")),
                shared_process_ids_json=json.dumps(record.get("shared_process_ids", []), ensure_ascii=False),
                alert_count=_coerce_int(record.get("alert_count")),
                alert_ids_json=json.dumps(record.get("alert_ids", []), ensure_ascii=False),
                max_alert_score=_coerce_float(record.get("max_alert_score")),
                max_rate_delta=_coerce_float(record.get("max_rate_delta")),
                sanction_match_count=_coerce_int(record.get("sanction_match_count")),
                sanction_sources_json=json.dumps(record.get("sanction_sources", []), ensure_ascii=False),
                donation_match_count=_coerce_int(record.get("donation_match_count")),
                donation_total_brl=_coerce_float(record.get("donation_total_brl")),
                corporate_conflict_count=_coerce_int(record.get("corporate_conflict_count")),
                corporate_conflict_ids_json=json.dumps(record.get("corporate_conflict_ids", []), ensure_ascii=False),
                corporate_companies_json=json.dumps(record.get("corporate_companies", []), ensure_ascii=False),
                affinity_count=_coerce_int(record.get("affinity_count")),
                affinity_ids_json=json.dumps(record.get("affinity_ids", []), ensure_ascii=False),
                top_process_classes_json=json.dumps(record.get("top_process_classes", []), ensure_ascii=False),
                supporting_party_ids_json=json.dumps(record.get("supporting_party_ids", []), ensure_ascii=False),
                supporting_party_names_json=json.dumps(record.get("supporting_party_names", []), ensure_ascii=False),
                signal_details_json=json.dumps(record.get("signal_details", {}), ensure_ascii=False),
                earliest_year=record.get("earliest_year"),
                latest_year=record.get("latest_year"),
                sanction_corporate_link_count=_coerce_int(record.get("sanction_corporate_link_count")),
                sanction_corporate_link_ids_json=json.dumps(
                    record.get("sanction_corporate_link_ids", []), ensure_ascii=False
                ),
                sanction_corporate_min_degree=_coerce_int(record.get("sanction_corporate_min_degree"))
                if record.get("sanction_corporate_min_degree") is not None
                else None,
                adjusted_rate_delta=_coerce_float(record.get("adjusted_rate_delta")),
                has_law_firm_group=_coerce_bool(record.get("has_law_firm_group")),
                donor_group_has_minister_partner=_coerce_bool(record.get("donor_group_has_minister_partner")),
                donor_group_has_party_partner=_coerce_bool(record.get("donor_group_has_party_partner")),
                donor_group_has_counsel_partner=_coerce_bool(record.get("donor_group_has_counsel_partner")),
                min_link_degree_to_minister=_coerce_int(record.get("min_link_degree_to_minister"))
                if record.get("min_link_degree_to_minister") is not None
                else None,
                generated_at=_parse_datetime(record.get("generated_at")),
            )
        )
    return results


def load_decision_velocities(analytics_dir: Path) -> list[ServingDecisionVelocity]:
    dv_path = analytics_dir / "decision_velocity.jsonl"
    if not dv_path.exists():
        return []
    results: list[ServingDecisionVelocity] = []
    seen: set[str] = set()
    for record in _read_jsonl(dv_path):
        vid = str(record.get("velocity_id", ""))
        if vid in seen:
            continue
        seen.add(vid)
        results.append(
            ServingDecisionVelocity(
                velocity_id=vid,
                decision_event_id=str(record.get("decision_event_id", "")),
                process_id=str(record.get("process_id", "")),
                current_rapporteur=record.get("current_rapporteur"),
                decision_date=record.get("decision_date"),
                filing_date=record.get("filing_date"),
                days_to_decision=_coerce_int(record.get("days_to_decision")),
                process_class=record.get("process_class"),
                thematic_key=record.get("thematic_key"),
                decision_year=record.get("decision_year"),
                group_size=record.get("group_size"),
                p5_days=_coerce_float(record.get("p5_days")),
                p10_days=_coerce_float(record.get("p10_days")),
                median_days=_coerce_float(record.get("median_days")),
                p90_days=_coerce_float(record.get("p90_days")),
                p95_days=_coerce_float(record.get("p95_days")),
                velocity_flag=record.get("velocity_flag"),
                velocity_z_score=_coerce_float(record.get("velocity_z_score")),
                generated_at=_parse_datetime(record.get("generated_at")),
            )
        )
    return results


def load_rapporteur_changes(analytics_dir: Path) -> list[ServingRapporteurChange]:
    rc_path = analytics_dir / "rapporteur_change.jsonl"
    if not rc_path.exists():
        return []
    results: list[ServingRapporteurChange] = []
    seen: set[str] = set()
    for record in _read_jsonl(rc_path):
        cid = str(record.get("change_id", ""))
        if cid in seen:
            continue
        seen.add(cid)
        results.append(
            ServingRapporteurChange(
                change_id=cid,
                process_id=str(record.get("process_id", "")),
                process_class=record.get("process_class"),
                previous_rapporteur=str(record.get("previous_rapporteur", "")),
                new_rapporteur=str(record.get("new_rapporteur", "")),
                change_date=record.get("change_date"),
                decision_event_id=record.get("decision_event_id"),
                post_change_decision_count=_coerce_int(record.get("post_change_decision_count")),
                post_change_favorable_rate=_coerce_float(record.get("post_change_favorable_rate")),
                new_rapporteur_baseline_rate=_coerce_float(record.get("new_rapporteur_baseline_rate")),
                delta_vs_baseline=_coerce_float(record.get("delta_vs_baseline")),
                red_flag=_coerce_bool(record.get("red_flag")),
                generated_at=_parse_datetime(record.get("generated_at")),
            )
        )
    return results


def load_counsel_network_clusters(analytics_dir: Path) -> list[ServingCounselNetworkCluster]:
    cn_path = analytics_dir / "counsel_network_cluster.jsonl"
    if not cn_path.exists():
        return []
    results: list[ServingCounselNetworkCluster] = []
    seen: set[str] = set()
    for record in _read_jsonl(cn_path):
        cid = str(record.get("cluster_id", ""))
        if cid in seen:
            continue
        seen.add(cid)
        results.append(
            ServingCounselNetworkCluster(
                cluster_id=cid,
                counsel_ids_json=json.dumps(record.get("counsel_ids", []), ensure_ascii=False),
                counsel_names_json=json.dumps(record.get("counsel_names", []), ensure_ascii=False),
                cluster_size=_coerce_int(record.get("cluster_size")),
                shared_client_count=_coerce_int(record.get("shared_client_count")),
                shared_process_count=_coerce_int(record.get("shared_process_count")),
                minister_names_json=json.dumps(record.get("minister_names", []), ensure_ascii=False),
                cluster_favorable_rate=_coerce_float(record.get("cluster_favorable_rate")),
                cluster_case_count=_coerce_int(record.get("cluster_case_count")),
                red_flag=_coerce_bool(record.get("red_flag")),
                generated_at=_parse_datetime(record.get("generated_at")),
            )
        )
    return results


def load_payment_counterparties(analytics_dir: Path) -> list[ServingPaymentCounterparty]:
    pc_path = analytics_dir / "payment_counterparty.jsonl"
    if not pc_path.exists():
        return []
    results: list[ServingPaymentCounterparty] = []
    seen: set[str] = set()
    for record in _read_jsonl(pc_path):
        cid = str(record.get("counterparty_id", ""))
        if cid in seen:
            continue
        seen.add(cid)
        results.append(
            ServingPaymentCounterparty(
                counterparty_id=cid,
                counterparty_identity_key=str(record.get("counterparty_identity_key", "")),
                identity_basis=str(record.get("identity_basis", "")),
                counterparty_name=str(record.get("counterparty_name", "")),
                counterparty_tax_id=str(record.get("counterparty_tax_id") or ""),
                counterparty_tax_id_normalized=str(record.get("counterparty_tax_id_normalized") or ""),
                counterparty_document_type=str(record.get("counterparty_document_type", "")),
                total_received_brl=_coerce_float(record.get("total_received_brl")) or 0.0,
                payment_count=_coerce_int(record.get("payment_count")),
                election_years_json=json.dumps(record.get("election_years", []), ensure_ascii=False),
                payer_parties_json=json.dumps(record.get("payer_parties", []), ensure_ascii=False),
                payer_actor_type=str(record.get("payer_actor_type", "party_org")),
                first_payment_date=record.get("first_payment_date"),
                last_payment_date=record.get("last_payment_date"),
                states_json=json.dumps(record.get("states", []), ensure_ascii=False),
                cnae_codes_json=json.dumps(record.get("cnae_codes", []), ensure_ascii=False),
                provenance_json=json.dumps(record.get("provenance", {}), ensure_ascii=False),
                generated_at=_parse_datetime(record.get("generated_at")),
            )
        )
    return results


def load_minister_bios(curated_dir: Path) -> list[ServingMinisterBio]:
    bio_path = curated_dir / "minister_bio.json"
    if not bio_path.exists():
        return []
    bio_data = _read_json(bio_path)
    results: list[ServingMinisterBio] = []
    for _name, entry in bio_data.items():
        results.append(
            ServingMinisterBio(
                minister_name=str(entry.get("minister_name", "")),
                appointment_date=entry.get("appointment_date"),
                appointing_president=entry.get("appointing_president"),
                birth_date=entry.get("birth_date"),
                birth_state=entry.get("birth_state"),
                career_summary=entry.get("career_summary"),
                political_party_history_json=json.dumps(entry.get("political_party_history"), ensure_ascii=False),
                known_connections_json=json.dumps(entry.get("known_connections"), ensure_ascii=False),
                news_references_json=json.dumps(entry.get("news_references"), ensure_ascii=False),
            )
        )
    return results
