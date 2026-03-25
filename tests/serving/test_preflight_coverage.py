"""Tests for preflight PK validation coverage and movement_id integrity."""

from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.core.identity import stable_id
from atlas_stf.serving._builder_utils import _PK_CHECKS, _validate_inputs


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


# ── Source file → PK field mapping used by the serving build loaders ──
# Each entry is (category, filename, pk_field). Files with auto-increment
# integer PKs in the serving model (rapporteur_profile, sequential_analysis,
# assignment_audit, ml_outlier_score) are excluded because they have no
# single PK field in the JSONL artifact.
_LOADER_SOURCE_FILES: list[tuple[str, str, str]] = [
    # Curated
    ("curated", "process.jsonl", "process_id"),
    ("curated", "decision_event.jsonl", "decision_event_id"),
    ("curated", "party.jsonl", "party_id"),
    ("curated", "process_party_link.jsonl", "link_id"),
    ("curated", "counsel.jsonl", "counsel_id"),
    ("curated", "process_counsel_link.jsonl", "link_id"),
    ("curated", "movement.jsonl", "movement_id"),
    ("curated", "session_event.jsonl", "session_event_id"),
    ("curated", "lawyer_entity.jsonl", "lawyer_id"),
    ("curated", "law_firm_entity.jsonl", "firm_id"),
    ("curated", "representation_edge.jsonl", "edge_id"),
    ("curated", "representation_event.jsonl", "event_id"),
    ("curated", "agenda_event.jsonl", "agenda_event_id"),
    ("curated", "agenda_coverage.jsonl", "coverage_id"),
    # Analytics (with single PK field)
    ("analytics", "outlier_alert.jsonl", "alert_id"),
    ("analytics", "donation_event.jsonl", "event_id"),
    ("analytics", "economic_group.jsonl", "group_id"),
    ("analytics", "sanction_match.jsonl", "match_id"),
    ("analytics", "donation_match.jsonl", "match_id"),
    ("analytics", "compound_risk.jsonl", "pair_id"),
    ("analytics", "sanction_corporate_link.jsonl", "link_id"),
    ("analytics", "corporate_network.jsonl", "conflict_id"),
    ("analytics", "counsel_affinity.jsonl", "affinity_id"),
    ("analytics", "temporal_analysis.jsonl", "record_id"),
    ("analytics", "decision_velocity.jsonl", "velocity_id"),
    ("analytics", "rapporteur_change.jsonl", "change_id"),
    ("analytics", "counsel_network_cluster.jsonl", "cluster_id"),
    ("analytics", "counsel_sanction_profile.jsonl", "counsel_id"),
    ("analytics", "counsel_donation_profile.jsonl", "counsel_id"),
    ("analytics", "payment_counterparty.jsonl", "counterparty_id"),
    ("analytics", "agenda_exposure.jsonl", "exposure_id"),
]


def test_preflight_covers_all_critical_artifacts():
    """Every JSONL file with a single PK that is loaded by the serving build
    must be covered by _PK_CHECKS in _builder_utils."""
    pk_check_set = {(c.category, c.filename, c.pk_field) for c in _PK_CHECKS}

    missing = []
    for category, filename, pk_field in _LOADER_SOURCE_FILES:
        if (category, filename, pk_field) not in pk_check_set:
            missing.append(f"{category}/{filename} (pk={pk_field})")

    assert not missing, (
        f"Preflight _PK_CHECKS is missing {len(missing)} loader source file(s):\n  "
        + "\n  ".join(missing)
    )


def test_preflight_does_not_contain_orphan_entries():
    """Every entry in _PK_CHECKS must correspond to an actual loader source file."""
    expected_set = {(cat, fn, pk) for cat, fn, pk in _LOADER_SOURCE_FILES}
    orphans = []
    for check in _PK_CHECKS:
        if (check.category, check.filename, check.pk_field) not in expected_set:
            orphans.append(check.label)

    assert not orphans, (
        f"_PK_CHECKS contains {len(orphans)} orphan entry/entries not in loader source files:\n  "
        + "\n  ".join(orphans)
    )


def test_movement_id_includes_detail():
    """Two movements with same process/date/description but different detail
    must produce different movement_ids."""
    process_number = "ADI 1234"
    date = "2026-01-15"
    description = "Juntada de peticao"

    mid_a = stable_id("mov_", f"{process_number}:{date}:{description}:detalhe A")
    mid_b = stable_id("mov_", f"{process_number}:{date}:{description}:detalhe B")
    mid_no_detail = stable_id("mov_", f"{process_number}:{date}:{description}:")

    assert mid_a != mid_b, "Different details must produce different movement_ids"
    assert mid_a != mid_no_detail, "Detail present vs absent must produce different IDs"
    assert mid_b != mid_no_detail


def test_movement_id_same_detail_produces_same_id():
    """Identical inputs must produce identical movement_ids (determinism)."""
    process_number = "ADI 1234"
    date = "2026-01-15"
    description = "Juntada de peticao"
    detail = "detalhe X"

    mid_1 = stable_id("mov_", f"{process_number}:{date}:{description}:{detail}")
    mid_2 = stable_id("mov_", f"{process_number}:{date}:{description}:{detail}")

    assert mid_1 == mid_2


def test_validate_inputs_clean_run(tmp_path: Path):
    """Validation of clean inputs produces no errors."""
    curated_dir = tmp_path / "curated"
    analytics_dir = tmp_path / "analytics"
    _write_jsonl(curated_dir / "process.jsonl", [{"process_id": "p1"}])
    _write_jsonl(curated_dir / "decision_event.jsonl", [{"decision_event_id": "e1"}])

    report_path = tmp_path / "report.json"
    results = _validate_inputs(curated_dir, analytics_dir, report_path=report_path)

    assert len(results) >= 2
    assert report_path.exists()
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["decision"] == "continue_clean"


def test_validate_inputs_detects_conflict(tmp_path: Path):
    """Validation must fail when a file has conflicting duplicate PKs."""
    import pytest

    curated_dir = tmp_path / "curated"
    analytics_dir = tmp_path / "analytics"
    _write_jsonl(
        curated_dir / "process.jsonl",
        [
            {"process_id": "p1", "name": "A"},
            {"process_id": "p1", "name": "B"},
        ],
    )

    with pytest.raises(ValueError, match="conflicting"):
        _validate_inputs(curated_dir, analytics_dir)
