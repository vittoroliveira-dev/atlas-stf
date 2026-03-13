"""Audit-gate helpers and deterministic audit reports."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .analytics.group_rules import MAX_CASE_COUNT, MIN_CASE_COUNT
from .analytics.score import score_event_against_baseline
from .schema_validate import validate_records

DEFAULT_STAGING_DIR = Path("data/staging/transparencia")
DEFAULT_CURATED_DIR = Path("data/curated")
DEFAULT_ANALYTICS_DIR = Path("data/analytics")

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


def audit_stage(staging_dir: Path = DEFAULT_STAGING_DIR, output_path: Path | None = None) -> dict[str, Any]:
    csv_files = sorted(path.name for path in staging_dir.glob("*.csv"))
    audit_path = staging_dir / "_audit.jsonl"
    audit_rows = _read_jsonl(audit_path) if audit_path.exists() else []
    audited_files = {str(row.get("output_file") or "") for row in audit_rows}
    file_reports = []
    for filename in csv_files:
        file_reports.append(
            {
                "filename": filename,
                "has_audit_entry": filename in audited_files,
                "status": "ok" if filename in audited_files else "missing_audit_entry",
            }
        )
    overall_status = "ok" if csv_files and all(item["has_audit_entry"] for item in file_reports) else "fail"
    payload = {
        "generated_at": _now_iso(),
        "target": "stage",
        "staging_dir": str(staging_dir),
        "has_audit_trail": audit_path.exists(),
        "file_count": len(csv_files),
        "audited_file_count": sum(1 for item in file_reports if item["has_audit_entry"]),
        "overall_status": overall_status,
        "files": file_reports,
    }
    return _write_json(output_path, payload)


def audit_curated(curated_dir: Path = DEFAULT_CURATED_DIR, output_path: Path | None = None) -> dict[str, Any]:
    checks = [
        ("process", curated_dir / "process.jsonl", PROCESS_SCHEMA),
        ("decision_event", curated_dir / "decision_event.jsonl", DECISION_EVENT_SCHEMA),
        ("subject", curated_dir / "subject.jsonl", SUBJECT_SCHEMA),
        ("party", curated_dir / "party.jsonl", PARTY_SCHEMA),
        ("counsel", curated_dir / "counsel.jsonl", COUNSEL_SCHEMA),
        ("process_party_link", curated_dir / "process_party_link.jsonl", PROCESS_PARTY_LINK_SCHEMA),
        ("process_counsel_link", curated_dir / "process_counsel_link.jsonl", PROCESS_COUNSEL_LINK_SCHEMA),
        ("entity_identifier", curated_dir / "entity_identifier.jsonl", ENTITY_IDENTIFIER_SCHEMA),
        (
            "entity_identifier_reconciliation",
            curated_dir / "entity_identifier_reconciliation.jsonl",
            ENTITY_IDENTIFIER_RECONCILIATION_SCHEMA,
        ),
    ]
    reports = []
    has_failures = False
    for label, path, schema in checks:
        if not path.exists():
            reports.append({"artifact": label, "path": str(path), "exists": False, "status": "missing"})
            has_failures = True
            continue
        rows = _read_jsonl(path)
        validate_records(rows, schema)
        reports.append({"artifact": label, "path": str(path), "exists": True, "row_count": len(rows), "status": "ok"})
    payload = {
        "generated_at": _now_iso(),
        "target": "curated",
        "curated_dir": str(curated_dir),
        "overall_status": "fail" if has_failures else "ok",
        "artifacts": reports,
    }
    return _write_json(output_path, payload)


def audit_analytics(
    *,
    comparison_group_path: Path = DEFAULT_ANALYTICS_DIR / "comparison_group.jsonl",
    link_path: Path = DEFAULT_ANALYTICS_DIR / "decision_event_group_link.jsonl",
    baseline_path: Path = DEFAULT_ANALYTICS_DIR / "baseline.jsonl",
    alert_path: Path = DEFAULT_ANALYTICS_DIR / "outlier_alert.jsonl",
    decision_event_path: Path = DEFAULT_CURATED_DIR / "decision_event.jsonl",
    process_path: Path = DEFAULT_CURATED_DIR / "process.jsonl",
    evidence_dir: Path | None = None,
    output_path: Path | None = None,
) -> dict[str, Any]:
    groups = _read_jsonl(comparison_group_path)
    links = _read_jsonl(link_path)
    baselines = _read_jsonl_map(baseline_path, "comparison_group_id")
    alerts = _read_jsonl(alert_path)
    events = _read_jsonl_map(decision_event_path, "decision_event_id")
    processes = _read_jsonl_map(process_path, "process_id")
    group_summary_path = comparison_group_path.parent / "comparison_group_summary.json"
    baseline_summary_path = comparison_group_path.parent / "baseline_summary.json"
    alert_summary_path = comparison_group_path.parent / "outlier_alert_summary.json"

    validate_records(groups, COMPARISON_GROUP_SCHEMA)
    validate_records(links, LINK_SCHEMA)
    validate_records(list(baselines.values()), BASELINE_SCHEMA)
    validate_records(alerts, ALERT_SCHEMA)
    required_summary_checks = [
        ("comparison_group_summary", group_summary_path, COMPARISON_GROUP_SUMMARY_SCHEMA),
        ("baseline_summary", baseline_summary_path, BASELINE_SUMMARY_SCHEMA),
        ("outlier_alert_summary", alert_summary_path, OUTLIER_ALERT_SUMMARY_SCHEMA),
    ]
    required_summary_reports = []
    missing_required_summary_count = 0
    for label, path, schema in required_summary_checks:
        if not path.exists():
            required_summary_reports.append(
                {"artifact": label, "path": str(path), "exists": False, "status": "missing"}
            )
            missing_required_summary_count += 1
            continue
        validate_records([json.loads(path.read_text(encoding="utf-8"))], schema)
        required_summary_reports.append({"artifact": label, "path": str(path), "exists": True, "status": "ok"})

    optional_checks = [
        ("rapporteur_profile", comparison_group_path.parent / "rapporteur_profile.jsonl", RAPPORTEUR_PROFILE_SCHEMA),
        ("sequential_analysis", comparison_group_path.parent / "sequential_analysis.jsonl", SEQUENTIAL_ANALYSIS_SCHEMA),
        ("assignment_audit", comparison_group_path.parent / "assignment_audit.jsonl", ASSIGNMENT_AUDIT_SCHEMA),
        ("ml_outlier_score", comparison_group_path.parent / "ml_outlier_score.jsonl", ML_OUTLIER_SCORE_SCHEMA),
    ]
    optional_summary_checks = [
        (
            "rapporteur_profile_summary",
            comparison_group_path.parent / "rapporteur_profile_summary.json",
            RAPPORTEUR_PROFILE_SUMMARY_SCHEMA,
        ),
        (
            "sequential_analysis_summary",
            comparison_group_path.parent / "sequential_analysis_summary.json",
            SEQUENTIAL_ANALYSIS_SUMMARY_SCHEMA,
        ),
        (
            "assignment_audit_summary",
            comparison_group_path.parent / "assignment_audit_summary.json",
            ASSIGNMENT_AUDIT_SUMMARY_SCHEMA,
        ),
        (
            "ml_outlier_score_summary",
            comparison_group_path.parent / "ml_outlier_score_summary.json",
            ML_OUTLIER_SCORE_SUMMARY_SCHEMA,
        ),
    ]
    optional_reports = []
    for label, path, schema in optional_checks:
        if not path.exists():
            optional_reports.append(
                {"artifact": label, "path": str(path), "exists": False, "status": "optional_missing"}
            )
            continue
        rows = _read_jsonl(path)
        validate_records(rows, schema)
        optional_reports.append(
            {"artifact": label, "path": str(path), "exists": True, "row_count": len(rows), "status": "ok"}
        )
    optional_summary_reports = []
    for label, path, schema in optional_summary_checks:
        if not path.exists():
            optional_summary_reports.append(
                {"artifact": label, "path": str(path), "exists": False, "status": "optional_missing"}
            )
            continue
        validate_records([json.loads(path.read_text(encoding="utf-8"))], schema)
        optional_summary_reports.append({"artifact": label, "path": str(path), "exists": True, "status": "ok"})

    group_reports = []
    valid_group_ids: set[str] = set()
    for group in groups:
        group_id = group["comparison_group_id"]
        gate_status = build_group_gate_status_payload(group)
        has_baseline = group_id in baselines
        if gate_status["passes_for_baseline"] and group.get("status") == "valid" and has_baseline:
            valid_group_ids.add(group_id)
        group_reports.append(
            {
                "comparison_group_id": group_id,
                "status": group.get("status"),
                "has_baseline": has_baseline,
                "gate_status": gate_status,
            }
        )

    link_reports = []
    invalid_link_count = 0
    for link in links:
        group_id = link["comparison_group_id"]
        event_id = link["decision_event_id"]
        process_id = link["process_id"]
        is_valid = group_id in valid_group_ids and event_id in events and process_id in processes
        if not is_valid:
            invalid_link_count += 1
        link_reports.append(
            {
                "decision_event_id": event_id,
                "comparison_group_id": group_id,
                "process_id": process_id,
                "is_valid": is_valid,
            }
        )

    alert_reports = []
    failing_alert_count = 0
    for alert in alerts:
        group_id = alert.get("comparison_group_id")
        event = events.get(str(alert["decision_event_id"]))
        baseline = baselines.get(str(group_id)) if group_id is not None else None
        process = processes.get(str(alert["process_id"]))
        score_details = (
            build_score_details_payload(event, baseline)
            if event is not None and baseline is not None
            else {
                "alert_score": None,
                "alert_type": None,
                "expected_pattern": None,
                "observed_pattern": None,
                "evidence_summary": None,
                "uncertainty_note": "INCERTO",
                "components": [],
            }
        )
        gate_status = build_alert_gate_status_payload(
            alert,
            score_details,
            comparison_group_id=str(group_id) if group_id is not None else None,
            baseline=baseline or {},
        )
        evidence_bundle_exists = None
        if evidence_dir is not None:
            evidence_bundle_exists = (evidence_dir / f"{alert['alert_id']}.json").exists()
        passes = gate_status["passes_for_analysis"] and process is not None and event is not None
        if not passes:
            failing_alert_count += 1
        alert_reports.append(
            {
                "alert_id": alert["alert_id"],
                "comparison_group_id": group_id,
                "has_process": process is not None,
                "has_decision_event": event is not None,
                "evidence_bundle_exists": evidence_bundle_exists,
                "score_details": score_details,
                "gate_status": gate_status,
                "passes": passes,
            }
        )

    payload = {
        "generated_at": _now_iso(),
        "target": "analytics",
        "overall_status": (
            "fail" if invalid_link_count or failing_alert_count or missing_required_summary_count else "ok"
        ),
        "summary": {
            "group_count": len(groups),
            "valid_group_count": len(valid_group_ids),
            "link_count": len(links),
            "invalid_link_count": invalid_link_count,
            "alert_count": len(alerts),
            "failing_alert_count": failing_alert_count,
            "missing_required_summary_count": missing_required_summary_count,
        },
        "groups": group_reports,
        "links": link_reports,
        "alerts": alert_reports,
        "required_summaries": required_summary_reports,
        "optional_artifacts": optional_reports,
        "optional_summaries": optional_summary_reports,
    }
    return _write_json(output_path, payload)
