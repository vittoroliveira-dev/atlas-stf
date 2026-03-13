from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics.build_alerts import build_alerts


def _write_jsonl(path: Path, rows: list[dict]):
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def test_build_alerts_generates_atipicidade_alert(tmp_path: Path):
    baseline_path = tmp_path / "baseline.jsonl"
    link_path = tmp_path / "link.jsonl"
    event_path = tmp_path / "event.jsonl"
    output_path = tmp_path / "outlier_alert.jsonl"
    summary_path = tmp_path / "outlier_alert_summary.json"

    _write_jsonl(
        baseline_path,
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
                "notes": None,
            }
        ],
    )
    _write_jsonl(
        link_path,
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
        event_path,
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

    build_alerts(baseline_path, link_path, event_path, output_path, summary_path)

    payload = json.loads(output_path.read_text(encoding="utf-8").strip())
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert payload["alert_type"] == "atipicidade"
    assert payload["status"] == "novo"
    assert payload["alert_score"] == 1.0
    assert summary["alert_count"] == 1


def test_alert_enriched_with_compound_risk(tmp_path: Path):
    baseline_path = tmp_path / "baseline.jsonl"
    link_path = tmp_path / "link.jsonl"
    event_path = tmp_path / "event.jsonl"
    compound_risk_path = tmp_path / "compound_risk.jsonl"
    output_path = tmp_path / "outlier_alert.jsonl"
    summary_path = tmp_path / "outlier_alert_summary.json"

    _write_jsonl(
        baseline_path,
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
                "notes": None,
            }
        ],
    )
    _write_jsonl(
        link_path,
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
        event_path,
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
        compound_risk_path,
        [
            {
                "pair_id": "cr_1",
                "minister_name": "MIN Y",
                "entity_type": "party",
                "entity_id": "party_1",
                "entity_name": "Empresa X",
                "signal_count": 2,
                "signals": ["sanction", "donation"],
                "shared_process_ids": ["proc_1"],
                "red_flag": True,
            }
        ],
    )

    build_alerts(
        baseline_path, link_path, event_path, output_path, summary_path,
        compound_risk_path=compound_risk_path,
    )

    payload = json.loads(output_path.read_text(encoding="utf-8").strip())
    assert payload["risk_signal_count"] == 2
    assert sorted(payload["risk_signals"]) == ["donation", "sanction"]


def test_alert_without_compound_risk_file(tmp_path: Path):
    baseline_path = tmp_path / "baseline.jsonl"
    link_path = tmp_path / "link.jsonl"
    event_path = tmp_path / "event.jsonl"
    output_path = tmp_path / "outlier_alert.jsonl"
    summary_path = tmp_path / "outlier_alert_summary.json"

    _write_jsonl(
        baseline_path,
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
                "notes": None,
            }
        ],
    )
    _write_jsonl(
        link_path,
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
        event_path,
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

    # compound_risk file does NOT exist
    build_alerts(
        baseline_path, link_path, event_path, output_path, summary_path,
        compound_risk_path=tmp_path / "nonexistent.jsonl",
    )

    payload = json.loads(output_path.read_text(encoding="utf-8").strip())
    assert payload["risk_signal_count"] == 0
    assert payload.get("risk_signals") is None


def test_build_alerts_skips_events_below_threshold(tmp_path: Path):
    baseline_path = tmp_path / "baseline.jsonl"
    link_path = tmp_path / "link.jsonl"
    event_path = tmp_path / "event.jsonl"
    output_path = tmp_path / "outlier_alert.jsonl"
    summary_path = tmp_path / "outlier_alert_summary.json"

    _write_jsonl(
        baseline_path,
        [
            {
                "baseline_id": "base_1",
                "comparison_group_id": "grp_1",
                "rule_version": "comparison-group-v1",
                "event_count": 12,
                "process_count": 10,
                "expected_decision_progress_distribution": {"NEGOU PROVIMENTO": 8, "DEFERIU": 4},
                "expected_rapporteur_distribution": {},
                "expected_judging_body_distribution": {},
                "observed_period_start": "2026-01-01",
                "observed_period_end": "2026-03-01",
                "generated_at": "2026-03-07T00:00:00+00:00",
                "notes": None,
            }
        ],
    )
    _write_jsonl(
        link_path,
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
        event_path,
        [
            {
                "decision_event_id": "de_1",
                "source_row_id": "1",
                "process_id": "proc_1",
                "decision_date": "2026-03-07",
                "decision_year": 2026,
                "current_rapporteur": None,
                "decision_origin": None,
                "decision_type": "Decisão Final",
                "decision_progress": "DEFERIU",
                "decision_note": None,
                "panel_indicator_raw": None,
                "is_collegiate": True,
                "judging_body": None,
                "time_bucket": "2026-03",
                "raw_fields": {},
                "normalization_version": "decision-event-v1",
                "source_id": "STF-TRANSP-REGDIST",
                "created_at": "2026-03-07T00:00:00+00:00",
                "updated_at": "2026-03-07T00:00:00+00:00",
            }
        ],
    )

    build_alerts(baseline_path, link_path, event_path, output_path, summary_path)

    assert output_path.read_text(encoding="utf-8") == ""
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["skipped_below_threshold"] == 1
