"""Path constants, I/O helpers, and payload builders for audit gates."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .analytics.group_rules import MAX_CASE_COUNT, MIN_CASE_COUNT
from .analytics.score import score_event_against_baseline
from .schema_validate import validate_records

__all__ = [
    "DEFAULT_STAGING_DIR",
    "DEFAULT_CURATED_DIR",
    "DEFAULT_ANALYTICS_DIR",
    "LAWYER_ENTITY_SCHEMA",
    "LAW_FIRM_ENTITY_SCHEMA",
    "REPRESENTATION_EDGE_SCHEMA",
    "REPRESENTATION_EVENT_SCHEMA",
    "SOURCE_EVIDENCE_SCHEMA",
    "PROCESS_SCHEMA",
    "DECISION_EVENT_SCHEMA",
    "SUBJECT_SCHEMA",
    "PARTY_SCHEMA",
    "COUNSEL_SCHEMA",
    "PROCESS_PARTY_LINK_SCHEMA",
    "PROCESS_COUNSEL_LINK_SCHEMA",
    "ENTITY_IDENTIFIER_SCHEMA",
    "ENTITY_IDENTIFIER_RECONCILIATION_SCHEMA",
    "COMPARISON_GROUP_SCHEMA",
    "COMPARISON_GROUP_SUMMARY_SCHEMA",
    "LINK_SCHEMA",
    "BASELINE_SCHEMA",
    "BASELINE_SUMMARY_SCHEMA",
    "ALERT_SCHEMA",
    "OUTLIER_ALERT_SUMMARY_SCHEMA",
    "RAPPORTEUR_PROFILE_SCHEMA",
    "RAPPORTEUR_PROFILE_SUMMARY_SCHEMA",
    "SEQUENTIAL_ANALYSIS_SCHEMA",
    "SEQUENTIAL_ANALYSIS_SUMMARY_SCHEMA",
    "ASSIGNMENT_AUDIT_SCHEMA",
    "ASSIGNMENT_AUDIT_SUMMARY_SCHEMA",
    "ML_OUTLIER_SCORE_SCHEMA",
    "ML_OUTLIER_SCORE_SUMMARY_SCHEMA",
    "build_alert_gate_status_payload",
    "build_group_gate_status_payload",
    "build_score_details_payload",
    "validate_records",
]

DEFAULT_STAGING_DIR = Path("data/staging/transparencia")
DEFAULT_CURATED_DIR = Path("data/curated")
DEFAULT_ANALYTICS_DIR = Path("data/analytics")

LAWYER_ENTITY_SCHEMA = Path("schemas/lawyer_entity.schema.json")
LAW_FIRM_ENTITY_SCHEMA = Path("schemas/law_firm_entity.schema.json")
REPRESENTATION_EDGE_SCHEMA = Path("schemas/representation_edge.schema.json")
REPRESENTATION_EVENT_SCHEMA = Path("schemas/representation_event.schema.json")
SOURCE_EVIDENCE_SCHEMA = Path("schemas/source_evidence.schema.json")

PROCESS_SCHEMA = Path("schemas/process.schema.json")
DECISION_EVENT_SCHEMA = Path("schemas/decision_event.schema.json")
SUBJECT_SCHEMA = Path("schemas/subject.schema.json")
PARTY_SCHEMA = Path("schemas/party.schema.json")
COUNSEL_SCHEMA = Path("schemas/counsel.schema.json")
PROCESS_PARTY_LINK_SCHEMA = Path("schemas/process_party_link.schema.json")
PROCESS_COUNSEL_LINK_SCHEMA = Path("schemas/process_counsel_link.schema.json")
ENTITY_IDENTIFIER_SCHEMA = Path("schemas/entity_identifier.schema.json")
ENTITY_IDENTIFIER_RECONCILIATION_SCHEMA = Path("schemas/entity_identifier_reconciliation.schema.json")
COMPARISON_GROUP_SCHEMA = Path("schemas/comparison_group.schema.json")
COMPARISON_GROUP_SUMMARY_SCHEMA = Path("schemas/comparison_group_summary.schema.json")
LINK_SCHEMA = Path("schemas/decision_event_group_link.schema.json")
BASELINE_SCHEMA = Path("schemas/baseline.schema.json")
BASELINE_SUMMARY_SCHEMA = Path("schemas/baseline_summary.schema.json")
ALERT_SCHEMA = Path("schemas/outlier_alert.schema.json")
OUTLIER_ALERT_SUMMARY_SCHEMA = Path("schemas/outlier_alert_summary.schema.json")
RAPPORTEUR_PROFILE_SCHEMA = Path("schemas/rapporteur_profile.schema.json")
RAPPORTEUR_PROFILE_SUMMARY_SCHEMA = Path("schemas/rapporteur_profile_summary.schema.json")
SEQUENTIAL_ANALYSIS_SCHEMA = Path("schemas/sequential_analysis.schema.json")
SEQUENTIAL_ANALYSIS_SUMMARY_SCHEMA = Path("schemas/sequential_analysis_summary.schema.json")
ASSIGNMENT_AUDIT_SCHEMA = Path("schemas/assignment_audit.schema.json")
ASSIGNMENT_AUDIT_SUMMARY_SCHEMA = Path("schemas/assignment_audit_summary.schema.json")
ML_OUTLIER_SCORE_SCHEMA = Path("schemas/ml_outlier_score.schema.json")
ML_OUTLIER_SCORE_SUMMARY_SCHEMA = Path("schemas/ml_outlier_score_summary.schema.json")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def _read_jsonl_map(path: Path, key: str) -> dict[str, dict[str, Any]]:
    return {row[key]: row for row in _read_jsonl(path) if key in row}


def _write_json(output_path: Path | None, payload: dict[str, Any]) -> dict[str, Any]:
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def build_score_details_payload(decision_event: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    score_result = score_event_against_baseline(decision_event, baseline)
    return {
        "alert_score": score_result.alert_score,
        "alert_type": score_result.alert_type,
        "expected_pattern": score_result.expected_pattern,
        "observed_pattern": score_result.observed_pattern,
        "evidence_summary": score_result.evidence_summary,
        "uncertainty_note": score_result.uncertainty_note,
        "components": [
            {
                "name": component.name,
                "observed_value": component.observed_value,
                "expected_value": component.expected_value,
                "expected_probability": component.expected_probability,
                "rarity_score": component.rarity_score,
            }
            for component in score_result.components
        ],
    }


def build_alert_gate_status_payload(
    alert: dict[str, Any],
    score_details: dict[str, Any],
    *,
    comparison_group_id: str | None,
    baseline: dict[str, Any],
) -> dict[str, Any]:
    has_comparison_group_id = bool(comparison_group_id)
    has_baseline = bool(baseline.get("baseline_id"))
    has_expected_pattern = bool(alert.get("expected_pattern") or score_details.get("expected_pattern"))
    has_observed_pattern = bool(alert.get("observed_pattern") or score_details.get("observed_pattern"))
    has_evidence_summary = bool(alert.get("evidence_summary") or score_details.get("evidence_summary"))
    has_alert_id = bool(alert.get("alert_id"))
    has_process_id = bool(alert.get("process_id"))
    has_decision_event_id = bool(alert.get("decision_event_id"))
    has_alert_type = bool(alert.get("alert_type") or score_details.get("alert_type"))
    has_alert_score = (alert.get("alert_score") is not None) or (score_details.get("alert_score") is not None)
    uncertainty_note_required = score_details.get("uncertainty_note") is not None
    has_uncertainty_note = bool(alert.get("uncertainty_note") or score_details.get("uncertainty_note"))
    uses_neutral_language = True
    makes_automatic_accusation = False
    passes_for_analysis = all(
        (
            has_alert_id,
            has_process_id,
            has_decision_event_id,
            has_comparison_group_id,
            has_baseline,
            has_expected_pattern,
            has_observed_pattern,
            has_evidence_summary,
            has_alert_type,
            has_alert_score,
            uses_neutral_language,
            not makes_automatic_accusation,
            (not uncertainty_note_required) or has_uncertainty_note,
        )
    )
    return {
        "has_alert_id": has_alert_id,
        "has_process_id": has_process_id,
        "has_decision_event_id": has_decision_event_id,
        "has_comparison_group_id": has_comparison_group_id,
        "has_baseline": has_baseline,
        "has_expected_pattern": has_expected_pattern,
        "has_observed_pattern": has_observed_pattern,
        "has_evidence_summary": has_evidence_summary,
        "has_alert_type": has_alert_type,
        "has_alert_score": has_alert_score,
        "uncertainty_note_required": uncertainty_note_required,
        "has_uncertainty_note": has_uncertainty_note,
        "uses_neutral_language": uses_neutral_language,
        "makes_automatic_accusation": makes_automatic_accusation,
        "passes_for_analysis": passes_for_analysis,
    }


def build_group_gate_status_payload(group: dict[str, Any]) -> dict[str, Any]:
    case_count = int(group.get("case_count") or 0)
    blocked_reason = group.get("blocked_reason")
    baseline_notes = group.get("baseline_notes")
    has_comparison_group_id = bool(group.get("comparison_group_id"))
    has_rule_version = bool(group.get("rule_version"))
    has_selection_criteria = bool(group.get("selection_criteria"))
    has_time_window = bool(group.get("time_window"))
    has_case_count = case_count > 0
    not_too_broad = case_count <= MAX_CASE_COUNT
    not_too_narrow = case_count >= MIN_CASE_COUNT
    has_methodological_risk_note = bool(baseline_notes or blocked_reason)
    explainable_in_plain_language = has_selection_criteria and has_time_window
    passes_for_baseline = all(
        (
            has_comparison_group_id,
            has_rule_version,
            has_selection_criteria,
            has_time_window,
            has_case_count,
            not_too_broad,
            not_too_narrow,
            explainable_in_plain_language,
            has_methodological_risk_note,
        )
    )
    return {
        "has_comparison_group_id": has_comparison_group_id,
        "has_rule_version": has_rule_version,
        "has_selection_criteria": has_selection_criteria,
        "has_time_window": has_time_window,
        "has_case_count": has_case_count,
        "not_too_broad": not_too_broad,
        "not_too_narrow": not_too_narrow,
        "explainable_in_plain_language": explainable_in_plain_language,
        "has_methodological_risk_note": has_methodological_risk_note,
        "passes_for_baseline": passes_for_baseline,
    }
