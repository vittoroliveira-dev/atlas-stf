from __future__ import annotations

import json
from pathlib import Path

import pytest

from atlas_stf.audit_gates import (
    audit_analytics,
    audit_curated,
    audit_stage,
    build_alert_gate_status_payload,
    build_group_gate_status_payload,
    build_score_details_payload,
)
from atlas_stf.schema_validate import SchemaValidationError


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _seed_analytics_inputs(base_dir: Path) -> dict[str, Path]:
    paths = {
        "groups": base_dir / "comparison_group.jsonl",
        "links": base_dir / "decision_event_group_link.jsonl",
        "baseline": base_dir / "baseline.jsonl",
        "alerts": base_dir / "outlier_alert.jsonl",
        "events": base_dir / "decision_event.jsonl",
        "processes": base_dir / "process.jsonl",
        "evidence_dir": base_dir / "evidence",
    }
    paths["evidence_dir"].mkdir()
    _write_jsonl(
        paths["groups"],
        [
            {
                "comparison_group_id": "grp_1",
                "rule_version": "comparison-group-v1",
                "selection_criteria": {"process_class": "AC"},
                "time_window": "2026",
                "case_count": 12,
                "baseline_notes": "Grupo válido",
                "status": "valid",
                "blocked_reason": None,
                "created_at": "2026-03-07T00:00:00+00:00",
                "updated_at": "2026-03-07T00:00:00+00:00",
            }
        ],
    )
    _write_jsonl(
        paths["links"],
        [
            {
                "decision_event_id": "de_1",
                "comparison_group_id": "grp_1",
                "process_id": "proc_1",
                "linked_at": "2026-03-07T00:00:00+00:00",
            }
        ],
    )
    _write_jsonl(
        paths["baseline"],
        [
            {
                "baseline_id": "base_1",
                "comparison_group_id": "grp_1",
                "rule_version": "comparison-group-v1",
                "event_count": 12,
                "process_count": 10,
                "expected_decision_progress_distribution": {"NEGOU PROVIMENTO": 12},
                "expected_rapporteur_distribution": {"MIN X": 12},
                "expected_judging_body_distribution": {"TURMA": 12},
                "observed_period_start": "2026-01-01",
                "observed_period_end": "2026-03-01",
                "generated_at": "2026-03-07T00:00:00+00:00",
                "notes": "Baseline explícito",
            }
        ],
    )
    _write_jsonl(
        paths["events"],
        [
            {
                "decision_event_id": "de_1",
                "source_row_id": "1",
                "process_id": "proc_1",
                "decision_date": "2026-03-07",
                "decision_year": 2026,
                "current_rapporteur": "MIN Y",
                "decision_origin": None,
                "decision_type": "Decisão Final",
                "decision_progress": "DEFERIU PEDIDO",
                "decision_note": None,
                "panel_indicator_raw": "COLEGIADA",
                "is_collegiate": True,
                "judging_body": "PLENARIO",
                "time_bucket": "2026-03",
                "raw_fields": {},
                "normalization_version": "decision-event-v1",
                "source_id": "STF-TRANSP-REGDIST",
                "created_at": "2026-03-07T00:00:00+00:00",
                "updated_at": "2026-03-07T00:00:00+00:00",
            }
        ],
    )
    _write_jsonl(
        paths["processes"],
        [
            {
                "process_id": "proc_1",
                "process_number": "AC 1",
                "process_class": "AC",
                "filing_date": "2026-01-05",
                "closing_date": None,
                "origin_description": None,
                "origin_court_or_body": None,
                "branch_of_law": "DIREITO X",
                "subjects_raw": ["A"],
                "subjects_normalized": ["A"],
                "case_environment": None,
                "procedural_status": None,
                "raw_fields": {},
                "normalization_version": "process-v1",
                "source_id": "STF-TRANSP-REGDIST",
                "source_record_hash": "hash-1",
                "created_at": "2026-03-07T00:00:00+00:00",
                "updated_at": "2026-03-07T00:00:00+00:00",
            }
        ],
    )
    _write_jsonl(
        paths["alerts"],
        [
            {
                "alert_id": "alert_1",
                "process_id": "proc_1",
                "decision_event_id": "de_1",
                "comparison_group_id": "grp_1",
                "alert_type": "atipicidade",
                "alert_score": 0.91,
                "expected_pattern": "esperado",
                "observed_pattern": "observado",
                "evidence_summary": "resumo",
                "uncertainty_note": None,
                "status": "novo",
                "created_at": "2026-03-07T00:00:00+00:00",
                "updated_at": "2026-03-07T00:00:00+00:00",
            }
        ],
    )
    _write_json(
        base_dir / "comparison_group_summary.json",
        {
            "generated_at": "2026-03-07T00:00:00+00:00",
            "rule_version": "comparison-group-v1",
            "group_count": 1,
            "valid_group_count": 1,
            "linked_event_count": 1,
            "skipped_event_count": 0,
        },
    )
    _write_json(
        base_dir / "baseline_summary.json",
        {
            "generated_at": "2026-03-07T00:00:00+00:00",
            "baseline_count": 1,
            "group_count_considered": 1,
            "event_count_linked": 12,
        },
    )
    _write_json(
        base_dir / "outlier_alert_summary.json",
        {
            "generated_at": "2026-03-07T00:00:00+00:00",
            "alert_count": 1,
            "alert_type_counts": {"atipicidade": 1},
            "status_counts": {"novo": 1},
            "threshold": 0.8,
            "min_score": 0.91,
            "max_score": 0.91,
            "avg_score": 0.91,
            "skipped_missing_baseline": 0,
            "skipped_missing_event": 0,
            "skipped_below_threshold": 0,
            "skipped_without_explanation": 0,
        },
    )
    (paths["evidence_dir"] / "alert_1.json").write_text("{}", encoding="utf-8")
    return paths


def test_build_group_gate_status_payload_passes_for_valid_group():
    payload = build_group_gate_status_payload(
        {
            "comparison_group_id": "grp_1",
            "rule_version": "comparison-group-v1",
            "selection_criteria": {"process_class": "AC"},
            "time_window": "2026",
            "case_count": 12,
            "baseline_notes": "Grupo válido",
            "blocked_reason": None,
        }
    )

    assert payload["passes_for_baseline"] is True


def test_build_alert_gate_status_payload_requires_uncertainty_note_when_score_requires_it():
    event = {
        "decision_progress": "DEFERIU PEDIDO",
        "current_rapporteur": None,
        "judging_body": None,
    }
    baseline = {
        "baseline_id": "base_1",
        "event_count": 1,
        "expected_decision_progress_distribution": {"NEGOU PROVIMENTO": 1},
        "expected_rapporteur_distribution": {},
        "expected_judging_body_distribution": {},
    }
    score_details = build_score_details_payload(event, baseline)
    payload = build_alert_gate_status_payload(
        {
            "alert_id": "alert_1",
            "process_id": "proc_1",
            "decision_event_id": "de_1",
            "comparison_group_id": "grp_1",
            "alert_type": "inconclusivo",
            "alert_score": score_details["alert_score"],
            "expected_pattern": score_details["expected_pattern"],
            "observed_pattern": score_details["observed_pattern"],
            "evidence_summary": score_details["evidence_summary"],
            "uncertainty_note": None,
        },
        score_details,
        comparison_group_id="grp_1",
        baseline=baseline,
    )

    assert payload["uncertainty_note_required"] is True
    assert payload["has_uncertainty_note"] is True
    assert payload["passes_for_analysis"] is True


def test_audit_stage_reports_missing_audit_entries(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    (staging_dir / "acervo.csv").write_text("a\n1\n", encoding="utf-8")

    payload = audit_stage(staging_dir=staging_dir)

    assert payload["overall_status"] == "fail"
    assert payload["files"][0]["status"] == "missing_audit_entry"


def test_audit_analytics_reports_optional_artifacts(tmp_path: Path):
    paths = _seed_analytics_inputs(tmp_path)
    _write_jsonl(
        tmp_path / "rapporteur_profile.jsonl",
        [
            {
                "rapporteur": "MIN Y",
                "process_class": "AC",
                "thematic_key": "A",
                "decision_year": 2026,
                "event_count": 10,
                "progress_distribution": {"DEFERIU PEDIDO": 10},
                "group_progress_distribution": {"NEGOU PROVIMENTO": 90, "DEFERIU PEDIDO": 10},
                "group_event_count": 100,
                "chi2_statistic": 5.5,
                "p_value_approx": 0.01,
                "deviation_flag": True,
                "deviation_direction": "sobre-representado em DEFERIU PEDIDO",
            }
        ],
    )
    _write_jsonl(
        tmp_path / "sequential_analysis.jsonl",
        [
            {
                "rapporteur": "MIN Y",
                "decision_year": 2026,
                "n_decisions": 50,
                "n_favorable": 30,
                "n_unfavorable": 20,
                "autocorrelation_lag1": 0.15,
                "streak_effect_3": 0.1,
                "streak_effect_5": None,
                "base_favorable_rate": 0.6,
                "post_streak_favorable_rate_3": 0.7,
                "post_streak_favorable_rate_5": None,
                "sequential_bias_flag": True,
            }
        ],
    )
    _write_jsonl(
        tmp_path / "assignment_audit.jsonl",
        [
            {
                "process_class": "AC",
                "decision_year": 2026,
                "rapporteur_count": 5,
                "event_count": 100,
                "rapporteur_distribution": {"MIN Y": 40, "MIN Z": 10},
                "chi2_statistic": 12.0,
                "p_value_approx": 0.01,
                "uniformity_flag": False,
                "most_overrepresented_rapporteur": "MIN Y",
                "most_underrepresented_rapporteur": "MIN Z",
            }
        ],
    )
    _write_jsonl(
        tmp_path / "ml_outlier_score.jsonl",
        [
            {
                "decision_event_id": "de_1",
                "comparison_group_id": "grp_1",
                "ml_anomaly_score": -0.12,
                "ml_rarity_score": 0.82,
                "ensemble_score": 0.85,
                "n_features": 4,
                "n_samples": 25,
                "generated_at": "2026-03-07T00:00:00+00:00",
            }
        ],
    )
    _write_json(
        tmp_path / "rapporteur_profile_summary.json",
        {
            "generated_at": "2026-03-07T00:00:00+00:00",
            "total_profiles": 1,
            "deviation_count": 1,
            "min_group_size": 30,
        },
    )
    _write_json(
        tmp_path / "sequential_analysis_summary.json",
        {
            "generated_at": "2026-03-07T00:00:00+00:00",
            "total_analyses": 1,
            "bias_flagged_count": 1,
            "min_decisions": 50,
        },
    )
    _write_json(
        tmp_path / "assignment_audit_summary.json",
        {
            "generated_at": "2026-03-07T00:00:00+00:00",
            "total_audits": 1,
            "uniform_count": 0,
            "non_uniform_count": 1,
            "min_events": 50,
        },
    )
    _write_json(
        tmp_path / "ml_outlier_score_summary.json",
        {
            "generated_at": "2026-03-07T00:00:00+00:00",
            "record_count": 1,
            "groups_processed": 1,
            "ensemble_count": 1,
        },
    )

    payload = audit_analytics(
        comparison_group_path=paths["groups"],
        link_path=paths["links"],
        baseline_path=paths["baseline"],
        alert_path=paths["alerts"],
        decision_event_path=paths["events"],
        process_path=paths["processes"],
        evidence_dir=paths["evidence_dir"],
    )

    optional = {item["artifact"]: item for item in payload["optional_artifacts"]}
    required_summaries = {item["artifact"]: item for item in payload["required_summaries"]}
    optional_summaries = {item["artifact"]: item for item in payload["optional_summaries"]}
    assert payload["overall_status"] == "ok"
    assert optional["rapporteur_profile"]["status"] == "ok"
    assert optional["ml_outlier_score"]["status"] == "ok"
    assert required_summaries["comparison_group_summary"]["status"] == "ok"
    assert optional_summaries["ml_outlier_score_summary"]["status"] == "ok"


def test_audit_analytics_rejects_invalid_optional_artifact(tmp_path: Path):
    paths = _seed_analytics_inputs(tmp_path)
    _write_jsonl(
        tmp_path / "assignment_audit.jsonl",
        [
            {
                "process_class": "AC",
                "decision_year": "2026",
            }
        ],
    )

    with pytest.raises(SchemaValidationError):
        audit_analytics(
            comparison_group_path=paths["groups"],
            link_path=paths["links"],
            baseline_path=paths["baseline"],
            alert_path=paths["alerts"],
            decision_event_path=paths["events"],
            process_path=paths["processes"],
            evidence_dir=paths["evidence_dir"],
        )


def test_audit_analytics_missing_required_summary_fails(tmp_path: Path):
    paths = _seed_analytics_inputs(tmp_path)
    (tmp_path / "baseline_summary.json").unlink()

    payload = audit_analytics(
        comparison_group_path=paths["groups"],
        link_path=paths["links"],
        baseline_path=paths["baseline"],
        alert_path=paths["alerts"],
        decision_event_path=paths["events"],
        process_path=paths["processes"],
        evidence_dir=paths["evidence_dir"],
    )

    assert payload["overall_status"] == "fail"
    assert payload["summary"]["missing_required_summary_count"] == 1
    assert any(
        item["artifact"] == "baseline_summary" and item["status"] == "missing" for item in payload["required_summaries"]
    )


def test_audit_curated_passes_with_minimal_valid_artifacts(tmp_path: Path):
    curated_dir = tmp_path / "curated"
    curated_dir.mkdir()
    _seed_analytics_inputs(tmp_path)
    process_path = tmp_path / "process.jsonl"
    event_path = tmp_path / "decision_event.jsonl"
    process_target = curated_dir / "process.jsonl"
    event_target = curated_dir / "decision_event.jsonl"
    process_target.write_text(process_path.read_text(encoding="utf-8"), encoding="utf-8")
    event_target.write_text(event_path.read_text(encoding="utf-8"), encoding="utf-8")
    for name in (
        "subject.jsonl",
        "party.jsonl",
        "counsel.jsonl",
        "process_party_link.jsonl",
        "process_counsel_link.jsonl",
        "entity_identifier.jsonl",
        "entity_identifier_reconciliation.jsonl",
    ):
        (curated_dir / name).write_text("", encoding="utf-8")

    payload = audit_curated(curated_dir=curated_dir)

    assert payload["overall_status"] == "ok"


def test_audit_analytics_passes_with_consistent_inputs(tmp_path: Path):
    paths = _seed_analytics_inputs(tmp_path)

    payload = audit_analytics(
        comparison_group_path=paths["groups"],
        link_path=paths["links"],
        baseline_path=paths["baseline"],
        alert_path=paths["alerts"],
        decision_event_path=paths["events"],
        process_path=paths["processes"],
        evidence_dir=paths["evidence_dir"],
    )

    assert payload["overall_status"] == "ok"
    assert payload["summary"]["failing_alert_count"] == 0
    assert payload["alerts"][0]["gate_status"]["passes_for_analysis"] is True
