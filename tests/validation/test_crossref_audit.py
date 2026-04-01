"""Tests for the cross-reference audit module."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from atlas_stf.validation.crossref_audit import (
    MINIMUM_GOLD_SET_SIZE,
    CrossrefAuditReport,
    run_crossref_audit,
)


@pytest.fixture()
def analytics_dir(tmp_path: Path) -> Path:
    """Create minimal analytics directory with realistic summary files."""
    d = tmp_path / "analytics"
    d.mkdir()

    # donation_match_summary.json
    (d / "donation_match_summary.json").write_text(
        json.dumps(
            {
                "donation_match_count": 1000,
                "red_flag_count": 50,
                "total_ambiguous_candidate_count": 100,
                "generated_at": "2026-01-01T00:00:00Z",
            }
        )
    )

    # donation_empirical_metrics.json
    (d / "donation_empirical_metrics.json").write_text(
        json.dumps(
            {
                "raw_data_quality": {
                    "identity_key_cpf_count": 8000,
                    "identity_key_name_count": 2000,
                },
                "match_quality": {
                    "total_matches": 1000,
                    "match_strategy_distribution": {
                        "exact": 500,
                        "canonical_name": 20,
                        "jaccard": 400,
                        "levenshtein": 80,
                    },
                    "jaccard_score_histogram": {
                        "[0.80, 0.85)": 300,
                        "[0.85, 0.90)": 10,
                        "[0.90, 0.95)": 0,
                        "[0.95, 1.00]": 90,
                    },
                    "levenshtein_score_histogram": {
                        "0": 1,
                        "1": 30,
                        "2": 49,
                    },
                    "red_flag_by_strategy": {
                        "exact": 40,
                        "canonical_name": 2,
                        "jaccard": 6,
                        "levenshtein": 2,
                    },
                },
                "ambiguous_analysis": {
                    "total_ambiguous": 100,
                },
                "methodology_notes": {},
                "generated_at": "2026-01-01T00:00:00Z",
            }
        )
    )

    # sanction_corporate_link_summary.json
    (d / "sanction_corporate_link_summary.json").write_text(
        json.dumps(
            {
                "total_links": 200,
                "red_flag_count": 10,
                "degree_counts": {"2": 180, "3": 20},
                "truncated_sanctions_count": 5,
                "generated_at": "2026-01-01T00:00:00Z",
            }
        )
    )

    # compound_risk_summary.json
    (d / "compound_risk_summary.json").write_text(
        json.dumps(
            {
                "pair_count": 5000,
                "red_flag_count": 500,
                "signal_frequency": {
                    "sanction": 100,
                    "donation": 200,
                    "corporate": 0,
                    "affinity": 30,
                    "alert": 4000,
                    "velocity": 500,
                    "redistribution": 20,
                },
                "generated_at": "2026-01-01T00:00:00Z",
            }
        )
    )

    # corporate_network_summary.json (empty results)
    (d / "corporate_network_summary.json").write_text(
        json.dumps(
            {
                "total_conflicts": 0,
                "red_flag_count": 0,
                "generated_at": "2026-01-01T00:00:00Z",
            }
        )
    )

    return d


def test_run_crossref_audit_returns_report(analytics_dir: Path) -> None:
    report = run_crossref_audit(analytics_dir=analytics_dir)
    assert isinstance(report, CrossrefAuditReport)
    assert report.generated_at
    assert len(report.modules) > 0
    assert len(report.rule_quality) > 0


def test_module_inventory_detects_missing(analytics_dir: Path) -> None:
    report = run_crossref_audit(analytics_dir=analytics_dir)
    names = {m.name: m for m in report.modules}
    assert names["donation_match"].exists is True
    assert names["donation_match"].record_count == 1000
    assert names["match_calibration"].exists is False
    assert names["donor_corporate_link"].exists is False


def test_rule_quality_extracts_metrics(analytics_dir: Path) -> None:
    report = run_crossref_audit(analytics_dir=analytics_dir)
    rules = {r.rule: r for r in report.rule_quality}

    exact = rules["exact"]
    assert exact.match_count == 500
    assert exact.red_flag_count == 40
    assert exact.confidence == "high"
    assert exact.policy_tier == "nucleus"

    jaccard_low = rules["jaccard_080_085"]
    assert jaccard_low.match_count == 300
    assert jaccard_low.ambiguous_count == 100
    assert jaccard_low.confidence == "medium"
    assert jaccard_low.policy_tier == "restricted"


def test_sensitivity_scenarios_computed(analytics_dir: Path) -> None:
    report = run_crossref_audit(analytics_dir=analytics_dir)
    assert len(report.sensitivity) == 3
    names = {s.name: s for s in report.sensitivity}
    assert "strict" in names
    assert "intermediate" in names
    assert "full" in names

    strict = names["strict"]
    full = names["full"]
    assert strict.match_count <= full.match_count
    assert full.delta_matches_vs_full == 0


def test_policy_checks_warn_on_issues(analytics_dir: Path) -> None:
    report = run_crossref_audit(analytics_dir=analytics_dir)
    checks = {c.check: c for c in report.policy_checks}

    assert checks["corporate_network_no_data"].status == "WARN"
    assert checks["match_calibration_not_run"].status == "WARN"
    assert checks["gold_set_missing"].status == "FAIL"


def test_corporate_network_empty_flagged(analytics_dir: Path) -> None:
    report = run_crossref_audit(analytics_dir=analytics_dir)
    rules = {r.rule: r for r in report.rule_quality}
    cn = rules.get("corporate_network_empty")
    assert cn is not None
    assert cn.confidence == "inconclusive"
    assert cn.policy_tier == "remove"


def test_scl_degree3_review_required(analytics_dir: Path) -> None:
    report = run_crossref_audit(analytics_dir=analytics_dir)
    rules = {r.rule: r for r in report.rule_quality}
    scl3 = rules.get("scl_degree3")
    assert scl3 is not None
    assert scl3.match_count == 20
    assert scl3.policy_tier == "review_required"


def test_gold_set_not_available(analytics_dir: Path) -> None:
    report = run_crossref_audit(analytics_dir=analytics_dir)
    assert report.gold_set.status == "not_available"
    assert report.gold_set.total == 0


def test_gold_set_below_minimum_with_strata(analytics_dir: Path) -> None:
    """Gold set with fewer than MINIMUM records → FAIL policy check + limitations."""
    (analytics_dir / "gold_set_matches.jsonl").write_text(
        '{"case_id":"gs-001","stratum":"exact_with_cpf","final_label":"correct","heuristic_label":"correct","adjudication_type":"evidence_deterministic"}\n'
        '{"case_id":"gs-002","stratum":"levenshtein_dist2","final_label":"incorrect","heuristic_label":"incorrect","adjudication_type":"evidence_deterministic"}\n'
        '{"case_id":"gs-003","stratum":"levenshtein_dist2","heuristic_label":"ambiguous","adjudication_type":"heuristic_provisional"}\n'
    )
    report = run_crossref_audit(analytics_dir=analytics_dir)
    assert report.gold_set.status == "available"
    assert report.gold_set.total == 3
    assert report.gold_set.by_stratum == {"exact_with_cpf": 1, "levenshtein_dist2": 2}
    assert any("minimum" in lim.lower() or str(MINIMUM_GOLD_SET_SIZE) in lim for lim in report.gold_set.limitations)
    # Required strata missing
    assert any("counsel_match" in lim for lim in report.gold_set.limitations)
    # Policy check must FAIL when below minimum
    checks = {c.check: c for c in report.policy_checks}
    assert checks["gold_set_below_minimum"].status == "FAIL"


def test_gold_set_above_minimum_passes(analytics_dir: Path) -> None:
    """Gold set at or above MINIMUM → PASS policy check."""
    lines = [
        json.dumps(
            {
                "case_id": f"gs-{i:04d}",
                "stratum": "exact_with_cpf",
                "final_label": "correct",
                "heuristic_label": "correct",
                "adjudication_type": "evidence_deterministic",
            }
        )
        for i in range(1, MINIMUM_GOLD_SET_SIZE + 1)
    ]
    (analytics_dir / "gold_set_matches.jsonl").write_text("\n".join(lines) + "\n")
    report = run_crossref_audit(analytics_dir=analytics_dir)
    assert report.gold_set.total == MINIMUM_GOLD_SET_SIZE
    assert not any("minimum" in lim.lower() for lim in report.gold_set.limitations)
    checks = {c.check: c for c in report.policy_checks}
    assert checks["gold_set_available"].status == "PASS"


def test_gold_set_all_provisional_fails_gate(analytics_dir: Path) -> None:
    """Gold set with only heuristic_provisional records → FAIL (0 adjudicated)."""
    lines = [
        json.dumps(
            {
                "case_id": f"gs-{i:04d}",
                "stratum": "exact_with_cpf",
                "heuristic_label": "correct",
                "adjudication_type": "heuristic_provisional",
            }
        )
        for i in range(1, MINIMUM_GOLD_SET_SIZE + 1)
    ]
    (analytics_dir / "gold_set_matches.jsonl").write_text("\n".join(lines) + "\n")
    report = run_crossref_audit(analytics_dir=analytics_dir)
    # Gate counts adjudicated records (final_label set), not total
    checks = {c.check: c for c in report.policy_checks}
    assert checks["gold_set_below_minimum"].status == "FAIL"
    assert "0 adjudicated" in checks["gold_set_below_minimum"].detail
    # Limitations also report heuristic_provisional
    assert any("heuristic_provisional" in lim for lim in report.gold_set.limitations)


def test_empty_analytics_dir(tmp_path: Path) -> None:
    empty = tmp_path / "empty"
    empty.mkdir()
    report = run_crossref_audit(analytics_dir=empty)
    assert isinstance(report, CrossrefAuditReport)
    assert all(not m.exists for m in report.modules)
    assert len(report.rule_quality) == 0
    assert len(report.sensitivity) == 0


def test_json_serialization(analytics_dir: Path) -> None:
    from dataclasses import asdict

    report = run_crossref_audit(analytics_dir=analytics_dir)
    d = asdict(report)
    serialized = json.dumps(d, default=str)
    parsed = json.loads(serialized)
    assert parsed["generated_at"]
    assert len(parsed["modules"]) == 13
    assert len(parsed["rule_quality"]) > 0
