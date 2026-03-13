from __future__ import annotations

import json
from pathlib import Path

import pytest

from atlas_stf.evidence.build_bundle import _read_jsonl_map, build_all_evidence_bundles, build_evidence_bundle
from atlas_stf.schema_validate import SchemaValidationError


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def _seed_inputs(base_dir: Path) -> dict[str, Path]:
    paths = {
        "alerts": base_dir / "outlier_alert.jsonl",
        "baseline": base_dir / "baseline.jsonl",
        "groups": base_dir / "comparison_group.jsonl",
        "events": base_dir / "decision_event.jsonl",
        "processes": base_dir / "process.jsonl",
    }
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
                "expected_pattern": "decision_progress tende a 'NEGOU PROVIMENTO'",
                "observed_pattern": "decision_progress='DEFERIU PEDIDO'",
                "evidence_summary": "Resumo auditável",
                "uncertainty_note": None,
                "status": "novo",
                "created_at": "2026-03-07T00:00:00+00:00",
                "updated_at": "2026-03-07T00:00:00+00:00",
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
                "notes": "Baseline sem smoothing",
            }
        ],
    )
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
    return paths


def test_build_evidence_bundle_writes_json_and_markdown(tmp_path: Path):
    paths = _seed_inputs(tmp_path)
    evidence_dir = tmp_path / "evidence"
    report_dir = tmp_path / "reports"

    json_path, md_path = build_evidence_bundle(
        "alert_1",
        alert_path=paths["alerts"],
        baseline_path=paths["baseline"],
        comparison_group_path=paths["groups"],
        decision_event_path=paths["events"],
        process_path=paths["processes"],
        evidence_dir=evidence_dir,
        report_dir=report_dir,
    )

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    markdown = md_path.read_text(encoding="utf-8")
    assert payload["alert"]["alert_id"] == "alert_1"
    assert payload["process"]["process_number"] == "AC 1"
    assert payload["score_details"]["alert_type"] == "atipicidade"
    assert len(payload["score_details"]["components"]) == 3
    assert payload["gate_status"]["passes_for_analysis"] is True
    assert "# Evidência do alerta alert_1" in markdown
    assert "Resumo auditável" in markdown
    assert "## 6. Componentes do score" in markdown
    assert "## 7. Gates de auditoria" in markdown


def test_build_evidence_bundle_uses_score_summary_when_alert_summary_is_missing(tmp_path: Path):
    paths = _seed_inputs(tmp_path)
    evidence_dir = tmp_path / "evidence"
    report_dir = tmp_path / "reports"

    rows = [json.loads(line) for line in paths["alerts"].read_text(encoding="utf-8").splitlines() if line.strip()]
    rows[0]["evidence_summary"] = None
    _write_jsonl(paths["alerts"], rows)

    json_path, _ = build_evidence_bundle(
        "alert_1",
        alert_path=paths["alerts"],
        baseline_path=paths["baseline"],
        comparison_group_path=paths["groups"],
        decision_event_path=paths["events"],
        process_path=paths["processes"],
        evidence_dir=evidence_dir,
        report_dir=report_dir,
    )

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["gate_status"]["has_evidence_summary"] is True
    assert payload["gate_status"]["passes_for_analysis"] is True
    assert payload["score_details"]["evidence_summary"] is not None


def test_build_all_evidence_bundles_builds_every_alert(tmp_path: Path):
    paths = _seed_inputs(tmp_path)
    evidence_dir = tmp_path / "evidence"
    report_dir = tmp_path / "reports"

    outputs = build_all_evidence_bundles(
        alert_path=paths["alerts"],
        baseline_path=paths["baseline"],
        comparison_group_path=paths["groups"],
        decision_event_path=paths["events"],
        process_path=paths["processes"],
        evidence_dir=evidence_dir,
        report_dir=report_dir,
    )

    assert len(outputs) == 1
    assert (evidence_dir / "alert_1.json").exists()
    assert (report_dir / "alert_1.md").exists()


def test_read_jsonl_map_raises_explicit_value_error_for_missing_key(tmp_path: Path):
    path = tmp_path / "broken.jsonl"
    _write_jsonl(path, [{"wrong_key": "value"}])

    with pytest.raises(ValueError, match="missing required key 'alert_id'"):
        _read_jsonl_map(path, "alert_id")


def test_build_evidence_bundle_includes_advanced_analytics(tmp_path: Path):
    paths = _seed_inputs(tmp_path)
    evidence_dir = tmp_path / "evidence"
    report_dir = tmp_path / "reports"

    rp_path = tmp_path / "rapporteur_profile.jsonl"
    seq_path = tmp_path / "sequential_analysis.jsonl"
    aa_path = tmp_path / "assignment_audit.jsonl"

    _write_jsonl(
        rp_path,
        [
            {
                "rapporteur": "MIN Y",
                "process_class": "AC",
                "thematic_key": "A",
                "decision_year": 2026,
                "event_count": 10,
                "chi2_statistic": 5.5,
                "p_value_approx": 0.01,
                "deviation_flag": True,
                "deviation_direction": "over",
                "progress_distribution": {"DEFERIU PEDIDO": 10},
                "group_progress_distribution": {"NEGOU PROVIMENTO": 90, "DEFERIU PEDIDO": 10},
                "group_event_count": 100,
            }
        ],
    )
    _write_jsonl(
        seq_path,
        [
            {
                "rapporteur": "MIN Y",
                "decision_year": 2026,
                "n_decisions": 50,
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
        aa_path,
        [
            {
                "process_class": "AC",
                "decision_year": 2026,
                "rapporteur_count": 5,
                "event_count": 100,
                "chi2_statistic": 12.0,
                "p_value_approx": 0.01,
                "uniformity_flag": False,
                "most_overrepresented_rapporteur": "MIN Y",
                "most_underrepresented_rapporteur": "MIN Z",
                "rapporteur_distribution": {"MIN Y": 40, "MIN Z": 10},
            }
        ],
    )

    json_path, md_path = build_evidence_bundle(
        "alert_1",
        alert_path=paths["alerts"],
        baseline_path=paths["baseline"],
        comparison_group_path=paths["groups"],
        decision_event_path=paths["events"],
        process_path=paths["processes"],
        evidence_dir=evidence_dir,
        report_dir=report_dir,
        rapporteur_profile_path=rp_path,
        sequential_path=seq_path,
        assignment_audit_path=aa_path,
    )

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert "advanced_analytics" in payload
    assert payload["advanced_analytics"]["rapporteur_profile"]["deviation_flag"] is True
    assert payload["advanced_analytics"]["sequential_analysis"]["sequential_bias_flag"] is True
    assert payload["advanced_analytics"]["assignment_audit"]["uniformity_flag"] is False

    markdown = md_path.read_text(encoding="utf-8")
    assert "Contexto analítico adicional" in markdown
    assert "perfil do relator" in markdown


def test_build_evidence_bundle_works_without_analytics_files(tmp_path: Path):
    paths = _seed_inputs(tmp_path)
    evidence_dir = tmp_path / "evidence"
    report_dir = tmp_path / "reports"

    json_path, md_path = build_evidence_bundle(
        "alert_1",
        alert_path=paths["alerts"],
        baseline_path=paths["baseline"],
        comparison_group_path=paths["groups"],
        decision_event_path=paths["events"],
        process_path=paths["processes"],
        evidence_dir=evidence_dir,
        report_dir=report_dir,
        rapporteur_profile_path=tmp_path / "nonexistent_rp.jsonl",
        sequential_path=tmp_path / "nonexistent_seq.jsonl",
        assignment_audit_path=tmp_path / "nonexistent_aa.jsonl",
    )

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert "advanced_analytics" not in payload


def test_build_evidence_bundle_rejects_invalid_optional_jsonl(tmp_path: Path):
    paths = _seed_inputs(tmp_path)
    evidence_dir = tmp_path / "evidence"
    report_dir = tmp_path / "reports"
    broken_rp = tmp_path / "rapporteur_profile.jsonl"
    broken_rp.write_text('{"rapporteur": "MIN Y"}\n{broken-json}\n', encoding="utf-8")

    with pytest.raises(ValueError, match="contains invalid JSON"):
        build_evidence_bundle(
            "alert_1",
            alert_path=paths["alerts"],
            baseline_path=paths["baseline"],
            comparison_group_path=paths["groups"],
            decision_event_path=paths["events"],
            process_path=paths["processes"],
            evidence_dir=evidence_dir,
            report_dir=report_dir,
            rapporteur_profile_path=broken_rp,
            sequential_path=tmp_path / "nonexistent_seq.jsonl",
            assignment_audit_path=tmp_path / "nonexistent_aa.jsonl",
        )


def test_build_evidence_bundle_ignores_optional_rows_without_rapporteur(tmp_path: Path):
    paths = _seed_inputs(tmp_path)
    evidence_dir = tmp_path / "evidence"
    report_dir = tmp_path / "reports"

    event_rows = [json.loads(line) for line in paths["events"].read_text(encoding="utf-8").splitlines() if line.strip()]
    event_rows[0]["current_rapporteur"] = None
    _write_jsonl(paths["events"], event_rows)

    rp_path = tmp_path / "rapporteur_profile.jsonl"
    seq_path = tmp_path / "sequential_analysis.jsonl"
    _write_jsonl(
        rp_path,
        [
            {
                "rapporteur": "",
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
                "deviation_direction": "over",
            }
        ],
    )
    _write_jsonl(
        seq_path,
        [
            {
                "rapporteur": "   ",
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

    json_path, _ = build_evidence_bundle(
        "alert_1",
        alert_path=paths["alerts"],
        baseline_path=paths["baseline"],
        comparison_group_path=paths["groups"],
        decision_event_path=paths["events"],
        process_path=paths["processes"],
        evidence_dir=evidence_dir,
        report_dir=report_dir,
        rapporteur_profile_path=rp_path,
        sequential_path=seq_path,
        assignment_audit_path=tmp_path / "nonexistent_aa.jsonl",
    )

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert "advanced_analytics" not in payload


def test_build_evidence_bundle_rejects_invalid_nested_payload(tmp_path: Path):
    paths = _seed_inputs(tmp_path)
    evidence_dir = tmp_path / "evidence"
    report_dir = tmp_path / "reports"

    paths["processes"].write_text(
        json.dumps(
            {
                "process_id": "proc_1",
                "process_class": "AC",
                "source_id": "STF-TRANSP-REGDIST",
                "source_record_hash": "hash-1",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(SchemaValidationError, match="process_number"):
        build_evidence_bundle(
            "alert_1",
            alert_path=paths["alerts"],
            baseline_path=paths["baseline"],
            comparison_group_path=paths["groups"],
            decision_event_path=paths["events"],
            process_path=paths["processes"],
            evidence_dir=evidence_dir,
            report_dir=report_dir,
        )
