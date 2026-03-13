from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.cli import main


def test_cli_analytics_build_groups(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    decision_event_path = tmp_path / "decision_event.jsonl"
    output_dir = tmp_path / "analytics"
    process_path.write_text(
        json.dumps(
            {
                "process_id": "proc_1",
                "process_number": "AC 1",
                "process_class": "AC",
                "filing_date": None,
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
                "source_record_hash": "1",
                "created_at": "2026-03-07T00:00:00+00:00",
                "updated_at": "2026-03-07T00:00:00+00:00",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    rows = []
    for idx in range(1, 6):
        rows.append(
            json.dumps(
                {
                    "decision_event_id": f"de_{idx}",
                    "source_row_id": str(idx),
                    "process_id": "proc_1",
                    "decision_date": "2026-03-07",
                    "decision_year": 2026,
                    "current_rapporteur": None,
                    "decision_origin": None,
                    "decision_type": "Decisão Final",
                    "decision_progress": None,
                    "decision_note": None,
                    "panel_indicator_raw": "MONOCRÁTICA",
                    "is_collegiate": False,
                    "judging_body": None,
                    "time_bucket": "2026-03",
                    "raw_fields": {},
                    "normalization_version": "decision-event-v1",
                    "source_id": "STF-TRANSP-REGDIST",
                    "created_at": "2026-03-07T00:00:00+00:00",
                    "updated_at": "2026-03-07T00:00:00+00:00",
                }
            )
        )
    decision_event_path.write_text("\n".join(rows) + "\n", encoding="utf-8")

    code = main(
        [
            "analytics",
            "build-groups",
            "--process-path",
            str(process_path),
            "--decision-event-path",
            str(decision_event_path),
            "--output-dir",
            str(output_dir),
        ]
    )

    assert code == 0
    assert (output_dir / "comparison_group.jsonl").exists()


def test_cli_analytics_build_baseline(tmp_path: Path):
    group_path = tmp_path / "comparison_group.jsonl"
    link_path = tmp_path / "decision_event_group_link.jsonl"
    decision_event_path = tmp_path / "decision_event.jsonl"
    output = tmp_path / "baseline.jsonl"
    summary = tmp_path / "baseline_summary.json"

    group_path.write_text(
        json.dumps(
            {
                "comparison_group_id": "grp_1",
                "rule_version": "comparison-group-v1",
                "selection_criteria": {},
                "time_window": "2026",
                "case_count": 5,
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
    decision_event_path.write_text(
        json.dumps(
            {
                "decision_event_id": "de_1",
                "source_row_id": "1",
                "process_id": "proc_1",
                "decision_date": "2026-03-07",
                "decision_year": 2026,
                "current_rapporteur": "MIN X",
                "decision_origin": None,
                "decision_type": "Decisão Final",
                "decision_progress": "NEGOU PROVIMENTO",
                "decision_note": None,
                "panel_indicator_raw": "COLEGIADA",
                "is_collegiate": True,
                "judging_body": "TURMA",
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

    code = main(
        [
            "analytics",
            "build-baseline",
            "--comparison-group-path",
            str(group_path),
            "--link-path",
            str(link_path),
            "--decision-event-path",
            str(decision_event_path),
            "--output",
            str(output),
            "--summary-output",
            str(summary),
        ]
    )

    assert code == 0
    assert output.exists()
    assert summary.exists()


def test_cli_analytics_build_alerts(tmp_path: Path):
    baseline_path = tmp_path / "baseline.jsonl"
    link_path = tmp_path / "decision_event_group_link.jsonl"
    decision_event_path = tmp_path / "decision_event.jsonl"
    output = tmp_path / "outlier_alert.jsonl"
    summary = tmp_path / "outlier_alert_summary.json"

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

    code = main(
        [
            "analytics",
            "build-alerts",
            "--baseline-path",
            str(baseline_path),
            "--link-path",
            str(link_path),
            "--decision-event-path",
            str(decision_event_path),
            "--output",
            str(output),
            "--summary-output",
            str(summary),
        ]
    )

    assert code == 0
    assert output.exists()
    assert summary.exists()
