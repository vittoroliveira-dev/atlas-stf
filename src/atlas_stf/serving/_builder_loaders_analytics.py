from __future__ import annotations

import json
from pathlib import Path

from ._builder_utils import (
    _coerce_bool,
    _coerce_float,
    _coerce_int,
    _parse_date,
    _parse_datetime,
    _read_jsonl,
)
from .models import (
    ServingAssignmentAudit,
    ServingMlOutlierScore,
    ServingOriginContext,
    ServingRapporteurProfile,
    ServingSequentialAnalysis,
    ServingTemporalAnalysis,
)


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


from ._builder_loaders_analytics_common import match_confidence as _match_confidence  # noqa: E402, F401
from ._builder_loaders_analytics_risk import (  # noqa: E402, F401
    load_compound_risks,
    load_counsel_affinities,
    load_counsel_network_clusters,
    load_decision_velocities,
    load_minister_bios,
    load_payment_counterparties,
    load_rapporteur_changes,
)
from ._builder_loaders_analytics_sanctions import (  # noqa: E402, F401
    load_donation_events,
    load_donation_matches,
    load_sanction_corporate_links,
    load_sanction_matches,
)
