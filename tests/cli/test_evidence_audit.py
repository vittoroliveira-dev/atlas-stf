from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.cli import main


def test_cli_evidence_build(tmp_path: Path):
    alert_path = tmp_path / "outlier_alert.jsonl"
    baseline_path = tmp_path / "baseline.jsonl"
    group_path = tmp_path / "comparison_group.jsonl"
    decision_event_path = tmp_path / "decision_event.jsonl"
    process_path = tmp_path / "process.jsonl"
    evidence_dir = tmp_path / "evidence"
    report_dir = tmp_path / "reports"

    alert_path.write_text(
        json.dumps(
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
        )
        + "\n",
        encoding="utf-8",
    )
    baseline_path.write_text(
        json.dumps(
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
                "notes": None,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    group_path.write_text(
        json.dumps(
            {
                "comparison_group_id": "grp_1",
                "rule_version": "comparison-group-v1",
                "selection_criteria": {"process_class": "AC"},
                "time_window": "2026",
                "case_count": 12,
                "baseline_notes": None,
                "status": "valid",
                "blocked_reason": None,
                "created_at": "2026-03-07T00:00:00+00:00",
                "updated_at": "2026-03-07T00:00:00+00:00",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    decision_event_path.write_text(
        json.dumps(
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
        )
        + "\n",
        encoding="utf-8",
    )
    process_path.write_text(
        json.dumps(
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
        )
        + "\n",
        encoding="utf-8",
    )

    code = main(
        [
            "evidence",
            "build",
            "--alert-id",
            "alert_1",
            "--alert-path",
            str(alert_path),
            "--baseline-path",
            str(baseline_path),
            "--comparison-group-path",
            str(group_path),
            "--decision-event-path",
            str(decision_event_path),
            "--process-path",
            str(process_path),
            "--evidence-dir",
            str(evidence_dir),
            "--report-dir",
            str(report_dir),
        ]
    )

    assert code == 0
    assert (evidence_dir / "alert_1.json").exists()
    assert (report_dir / "alert_1.md").exists()


def test_cli_audit_analytics(tmp_path: Path):
    group_path = tmp_path / "comparison_group.jsonl"
    link_path = tmp_path / "decision_event_group_link.jsonl"
    baseline_path = tmp_path / "baseline.jsonl"
    alert_path = tmp_path / "outlier_alert.jsonl"
    decision_event_path = tmp_path / "decision_event.jsonl"
    process_path = tmp_path / "process.jsonl"
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    output = tmp_path / "analytics_audit.json"

    group_path.write_text(
        json.dumps(
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
        )
        + "\n",
        encoding="utf-8",
    )
    link_path.write_text(
        json.dumps(
            {
                "decision_event_id": "de_1",
                "comparison_group_id": "grp_1",
                "process_id": "proc_1",
                "linked_at": "2026-03-07T00:00:00+00:00",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    baseline_path.write_text(
        json.dumps(
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
        )
        + "\n",
        encoding="utf-8",
    )
    alert_path.write_text(
        json.dumps(
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
        )
        + "\n",
        encoding="utf-8",
    )
    decision_event_path.write_text(
        json.dumps(
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
        )
        + "\n",
        encoding="utf-8",
    )
    process_path.write_text(
        json.dumps(
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
        )
        + "\n",
        encoding="utf-8",
    )
    (tmp_path / "comparison_group_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-07T00:00:00+00:00",
                "rule_version": "comparison-group-v1",
                "group_count": 1,
                "valid_group_count": 1,
                "linked_event_count": 1,
                "skipped_event_count": 0,
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "baseline_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-07T00:00:00+00:00",
                "baseline_count": 1,
                "group_count_considered": 1,
                "event_count_linked": 12,
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "outlier_alert_summary.json").write_text(
        json.dumps(
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
            }
        ),
        encoding="utf-8",
    )
    (evidence_dir / "alert_1.json").write_text("{}", encoding="utf-8")

    code = main(
        [
            "audit",
            "analytics",
            "--comparison-group-path",
            str(group_path),
            "--link-path",
            str(link_path),
            "--baseline-path",
            str(baseline_path),
            "--alert-path",
            str(alert_path),
            "--decision-event-path",
            str(decision_event_path),
            "--process-path",
            str(process_path),
            "--evidence-dir",
            str(evidence_dir),
            "--output",
            str(output),
        ]
    )

    assert code == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["overall_status"] == "ok"
