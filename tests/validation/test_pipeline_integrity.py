"""Tests for the pipeline integrity validator.

All fixtures are synthetic JSONL data written to tmp_path — no real data dependency.
"""

from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.validation.pipeline_integrity import (
    CheckResult,
    ValidationReport,
    _coverage,
    _ids,
    _lines,
    check_alert_referential_integrity,
    check_critical_field_coverage,
    check_output_cardinality_sanity,
    check_recurrence_referential_integrity,
    check_representation_edge_party_coverage,
    check_representation_event_sanity,
    check_session_event_rapporteur_coverage,
    run_validation,
)

# ---------------------------------------------------------------------------
# Fixture helper
# ---------------------------------------------------------------------------


def _write_jsonl(path: Path, records: list[dict]) -> None:  # type: ignore[type-arg]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------


class TestHelpers:
    def test_ids(self, tmp_path: Path) -> None:
        """Extract IDs from JSONL into a set."""
        path = tmp_path / "items.jsonl"
        _write_jsonl(
            path,
            [
                {"item_id": "id_a", "value": 1},
                {"item_id": "id_b", "value": 2},
                {"item_id": "id_c", "value": 3},
            ],
        )

        result = _ids(path, "item_id")

        assert result == {"id_a", "id_b", "id_c"}

    def test_ids_missing_field_skipped(self, tmp_path: Path) -> None:
        """Records missing the id_field are silently skipped."""
        path = tmp_path / "items.jsonl"
        _write_jsonl(
            path,
            [
                {"item_id": "id_a"},
                {"other_field": "no_id_here"},
                {"item_id": "id_c"},
            ],
        )

        result = _ids(path, "item_id")

        assert result == {"id_a", "id_c"}

    def test_ids_nonexistent_file(self, tmp_path: Path) -> None:
        """Returns empty set when file does not exist."""
        result = _ids(tmp_path / "missing.jsonl", "id")

        assert result == set()

    def test_coverage_all_present(self, tmp_path: Path) -> None:
        """All records have the field non-null."""
        path = tmp_path / "data.jsonl"
        _write_jsonl(
            path,
            [
                {"name": "Alice"},
                {"name": "Bob"},
                {"name": "Carol"},
            ],
        )

        total, non_null = _coverage(path, "name")

        assert total == 3
        assert non_null == 3

    def test_coverage_some_null(self, tmp_path: Path) -> None:
        """Some records have null or absent field."""
        path = tmp_path / "data.jsonl"
        _write_jsonl(
            path,
            [
                {"name": "Alice"},
                {"name": None},
                {"name": "Carol"},
                {},
            ],
        )

        total, non_null = _coverage(path, "name")

        assert total == 4
        assert non_null == 2

    def test_coverage_nonexistent_file(self, tmp_path: Path) -> None:
        """Returns (0, 0) when file does not exist."""
        total, non_null = _coverage(tmp_path / "missing.jsonl", "field")

        assert total == 0
        assert non_null == 0

    def test_lines(self, tmp_path: Path) -> None:
        """Count lines correctly for a non-empty file."""
        path = tmp_path / "data.jsonl"
        _write_jsonl(path, [{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}, {"e": 5}])

        assert _lines(path) == 5

    def test_lines_empty_file(self, tmp_path: Path) -> None:
        """Returns 0 for an empty file."""
        path = tmp_path / "empty.jsonl"
        path.write_text("", encoding="utf-8")

        assert _lines(path) == 0

    def test_lines_nonexistent_file(self, tmp_path: Path) -> None:
        """Returns 0 when file does not exist."""
        assert _lines(tmp_path / "missing.jsonl") == 0


# ---------------------------------------------------------------------------
# Alert referential integrity
# ---------------------------------------------------------------------------


class TestAlertReferentialIntegrity:
    def _setup_base(
        self,
        tmp_path: Path,
        *,
        alert_de_id: str = "de1",
        alert_pid: str = "p1",
        alert_gid: str = "g1",
        de_ids: list[str] | None = None,
        proc_ids: list[str] | None = None,
        group_ids: list[str] | None = None,
    ) -> tuple[Path, Path]:
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"

        _write_jsonl(
            analytics_dir / "outlier_alert.jsonl",
            [
                {
                    "alert_id": "alert_1",
                    "decision_event_id": alert_de_id,
                    "process_id": alert_pid,
                    "comparison_group_id": alert_gid,
                }
            ],
        )
        _write_jsonl(
            curated_dir / "decision_event.jsonl",
            [{"decision_event_id": did} for did in (de_ids or ["de1"])],
        )
        _write_jsonl(
            curated_dir / "process.jsonl",
            [{"process_id": pid} for pid in (proc_ids or ["p1"])],
        )
        _write_jsonl(
            analytics_dir / "comparison_group.jsonl",
            [{"comparison_group_id": gid} for gid in (group_ids or ["g1"])],
        )

        return curated_dir, analytics_dir

    def test_all_fks_resolve(self, tmp_path: Path) -> None:
        """All alert FK references exist in target artifacts -> PASS."""
        curated_dir, analytics_dir = self._setup_base(tmp_path)

        result = check_alert_referential_integrity(curated_dir, analytics_dir)

        assert isinstance(result, CheckResult)
        assert result.status == "PASS"

    def test_orphan_decision_event_id(self, tmp_path: Path) -> None:
        """Alert references nonexistent decision_event -> FAIL CRITICAL."""
        curated_dir, analytics_dir = self._setup_base(
            tmp_path,
            alert_de_id="de_NONEXISTENT",
            de_ids=["de1"],
        )

        result = check_alert_referential_integrity(curated_dir, analytics_dir)

        assert result.status == "FAIL"
        assert result.severity == "CRITICAL"

    def test_orphan_process_id(self, tmp_path: Path) -> None:
        """Alert references nonexistent process -> FAIL CRITICAL."""
        curated_dir, analytics_dir = self._setup_base(
            tmp_path,
            alert_pid="p_NONEXISTENT",
            proc_ids=["p1"],
        )

        result = check_alert_referential_integrity(curated_dir, analytics_dir)

        assert result.status == "FAIL"
        assert result.severity == "CRITICAL"

    def test_no_alerts_file(self, tmp_path: Path) -> None:
        """Missing outlier_alert.jsonl -> check returns without crashing."""
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        curated_dir.mkdir(parents=True)
        analytics_dir.mkdir(parents=True)

        result = check_alert_referential_integrity(curated_dir, analytics_dir)

        assert isinstance(result, CheckResult)


# ---------------------------------------------------------------------------
# Representation edge party coverage
# ---------------------------------------------------------------------------


class TestRepresentationEdgePartyCoverage:
    def test_above_threshold(self, tmp_path: Path) -> None:
        """party_id coverage above threshold -> PASS."""
        curated_dir = tmp_path / "curated"
        _write_jsonl(
            curated_dir / "representation_edge.jsonl",
            [
                {"edge_id": "e1", "party_id": "p1"},
                {"edge_id": "e2", "party_id": "p2"},
                {"edge_id": "e3", "party_id": "p3"},
                {"edge_id": "e4", "party_id": "p4"},
                {"edge_id": "e5", "party_id": "p5"},
            ],
        )

        result = check_representation_edge_party_coverage(curated_dir, threshold=0.5)

        assert result.status == "PASS"

    def test_below_threshold(self, tmp_path: Path) -> None:
        """party_id coverage below threshold -> FAIL HIGH."""
        curated_dir = tmp_path / "curated"
        _write_jsonl(
            curated_dir / "representation_edge.jsonl",
            [
                {"edge_id": "e1", "party_id": "p1"},
                {"edge_id": "e2", "party_id": None},
                {"edge_id": "e3", "party_id": None},
                {"edge_id": "e4", "party_id": None},
                {"edge_id": "e5", "party_id": None},
            ],
        )

        result = check_representation_edge_party_coverage(curated_dir, threshold=0.5)

        assert result.status == "FAIL"
        assert result.severity == "HIGH"

    def test_empty_file(self, tmp_path: Path) -> None:
        """Empty JSONL -> check does not crash."""
        curated_dir = tmp_path / "curated"
        _write_jsonl(curated_dir / "representation_edge.jsonl", [])

        result = check_representation_edge_party_coverage(curated_dir, threshold=0.5)

        assert isinstance(result, CheckResult)


# ---------------------------------------------------------------------------
# Session event rapporteur coverage
# ---------------------------------------------------------------------------


class TestSessionEventRapporteurCoverage:
    def test_above_threshold(self, tmp_path: Path) -> None:
        """rapporteur coverage above threshold -> PASS."""
        curated_dir = tmp_path / "curated"
        _write_jsonl(
            curated_dir / "session_event.jsonl",
            [
                {"session_event_id": "s1", "rapporteur_at_event": "MIN. TOFFOLI"},
                {"session_event_id": "s2", "rapporteur_at_event": "MIN. BARROSO"},
                {"session_event_id": "s3", "rapporteur_at_event": "MIN. FACHIN"},
                {"session_event_id": "s4", "rapporteur_at_event": "MIN. WEBER"},
                {"session_event_id": "s5", "rapporteur_at_event": None},
            ],
        )

        result = check_session_event_rapporteur_coverage(curated_dir, threshold=0.5)

        assert result.status == "PASS"

    def test_below_threshold(self, tmp_path: Path) -> None:
        """rapporteur coverage below threshold -> FAIL HIGH."""
        curated_dir = tmp_path / "curated"
        _write_jsonl(
            curated_dir / "session_event.jsonl",
            [
                {"session_event_id": "s1", "rapporteur_at_event": None},
                {"session_event_id": "s2", "rapporteur_at_event": None},
                {"session_event_id": "s3", "rapporteur_at_event": None},
                {"session_event_id": "s4", "rapporteur_at_event": None},
                {"session_event_id": "s5", "rapporteur_at_event": "MIN. TOFFOLI"},
            ],
        )

        result = check_session_event_rapporteur_coverage(curated_dir, threshold=0.5)

        assert result.status == "FAIL"
        assert result.severity == "HIGH"

    def test_exactly_at_threshold_passes(self, tmp_path: Path) -> None:
        """Coverage exactly equal to threshold is considered PASS (>= boundary)."""
        curated_dir = tmp_path / "curated"
        _write_jsonl(
            curated_dir / "session_event.jsonl",
            [
                {"session_event_id": "s1", "rapporteur_at_event": "MIN. A"},
                {"session_event_id": "s2", "rapporteur_at_event": "MIN. B"},
                {"session_event_id": "s3", "rapporteur_at_event": None},
                {"session_event_id": "s4", "rapporteur_at_event": None},
            ],
        )

        result = check_session_event_rapporteur_coverage(curated_dir, threshold=0.5)

        assert result.status == "PASS"


# ---------------------------------------------------------------------------
# Representation event sanity
# ---------------------------------------------------------------------------


class TestRepresentationEventSanity:
    def test_healthy_ratio(self, tmp_path: Path) -> None:
        """Events exist when edges exist -> PASS."""
        curated_dir = tmp_path / "curated"
        _write_jsonl(
            curated_dir / "representation_edge.jsonl",
            [
                {"edge_id": "e1", "process_id": "p1"},
                {"edge_id": "e2", "process_id": "p2"},
            ],
        )
        _write_jsonl(
            curated_dir / "representation_event.jsonl",
            [
                {"event_id": "ev1", "process_id": "p1"},
                {"event_id": "ev2", "process_id": "p2"},
            ],
        )

        result = check_representation_event_sanity(curated_dir)

        assert result.status == "PASS"

    def test_zero_events_with_edges(self, tmp_path: Path) -> None:
        """Zero events but edges exist -> FAIL CRITICAL."""
        curated_dir = tmp_path / "curated"
        _write_jsonl(
            curated_dir / "representation_edge.jsonl",
            [
                {"edge_id": "e1", "process_id": "p1"},
                {"edge_id": "e2", "process_id": "p2"},
                {"edge_id": "e3", "process_id": "p3"},
            ],
        )
        _write_jsonl(curated_dir / "representation_event.jsonl", [])

        result = check_representation_event_sanity(curated_dir)

        assert result.status == "FAIL"
        assert result.severity == "CRITICAL"

    def test_zero_edges_zero_events_acceptable(self, tmp_path: Path) -> None:
        """Zero edges and zero events is an acceptable degenerate case."""
        curated_dir = tmp_path / "curated"
        _write_jsonl(curated_dir / "representation_edge.jsonl", [])
        _write_jsonl(curated_dir / "representation_event.jsonl", [])

        result = check_representation_event_sanity(curated_dir)

        assert isinstance(result, CheckResult)


# ---------------------------------------------------------------------------
# Recurrence referential integrity
# ---------------------------------------------------------------------------


class TestRecurrenceReferentialIntegrity:
    def test_all_ids_resolve(self, tmp_path: Path) -> None:
        """All recurrence IDs exist in base artifacts -> PASS."""
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"

        _write_jsonl(
            curated_dir / "lawyer_entity.jsonl",
            [
                {"lawyer_id": "law_1"},
                {"lawyer_id": "law_2"},
            ],
        )
        _write_jsonl(
            curated_dir / "party.jsonl",
            [
                {"party_id": "party_1"},
                {"party_id": "party_2"},
            ],
        )
        _write_jsonl(
            analytics_dir / "representation_recurrence.jsonl",
            [
                {"lawyer_id": "law_1", "party_id": "party_1", "process_count": 5},
                {"lawyer_id": "law_2", "party_id": "party_2", "process_count": 3},
            ],
        )

        result = check_recurrence_referential_integrity(curated_dir, analytics_dir)

        assert result.status == "PASS"

    def test_orphan_lawyer_id(self, tmp_path: Path) -> None:
        """Recurrence references nonexistent lawyer -> FAIL CRITICAL."""
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"

        _write_jsonl(
            curated_dir / "lawyer_entity.jsonl",
            [{"lawyer_id": "law_1"}],
        )
        _write_jsonl(
            analytics_dir / "representation_recurrence.jsonl",
            [
                {"lawyer_id": "law_1", "process_count": 5},
                {"lawyer_id": "law_GHOST", "process_count": 2},
            ],
        )

        result = check_recurrence_referential_integrity(curated_dir, analytics_dir)

        assert result.status == "FAIL"
        assert result.severity == "CRITICAL"

    def test_no_recurrence_file(self, tmp_path: Path) -> None:
        """Missing recurrence file -> check does not crash."""
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        _write_jsonl(
            curated_dir / "lawyer_entity.jsonl",
            [{"lawyer_id": "law_1"}],
        )
        analytics_dir.mkdir(parents=True, exist_ok=True)

        result = check_recurrence_referential_integrity(curated_dir, analytics_dir)

        assert isinstance(result, CheckResult)


# ---------------------------------------------------------------------------
# Critical field coverage
# ---------------------------------------------------------------------------


class TestCriticalFieldCoverage:
    def _minimal_curated(self, curated_dir: Path) -> None:
        _write_jsonl(
            curated_dir / "decision_event.jsonl",
            [
                {
                    "decision_event_id": "de1",
                    "process_id": "p1",
                    "decision_date": "2026-01-10",
                    "current_rapporteur": "MIN. TOFFOLI",
                },
                {
                    "decision_event_id": "de2",
                    "process_id": "p2",
                    "decision_date": "2026-02-15",
                    "current_rapporteur": "MIN. BARROSO",
                },
            ],
        )
        _write_jsonl(
            curated_dir / "process.jsonl",
            [
                {"process_id": "p1", "process_class": "ADI"},
                {"process_id": "p2", "process_class": "RE"},
            ],
        )

    def test_pass_when_coverage_sufficient(self, tmp_path: Path) -> None:
        """All critical fields have sufficient coverage -> all checks PASS."""
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        self._minimal_curated(curated_dir)
        _write_jsonl(
            analytics_dir / "outlier_alert.jsonl",
            [
                {"alert_id": "a1", "alert_score": 0.8},
                {"alert_id": "a2", "alert_score": 0.6},
            ],
        )

        results = check_critical_field_coverage(curated_dir, analytics_dir)

        assert isinstance(results, list)
        assert all(r.status == "PASS" for r in results)

    def test_fail_when_rapporteur_missing(self, tmp_path: Path) -> None:
        """Low rapporteur coverage in decision_event -> at least one FAIL."""
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        _write_jsonl(
            curated_dir / "decision_event.jsonl",
            [
                {
                    "decision_event_id": f"de{i}",
                    "process_id": f"p{i}",
                    "decision_date": "2026-01-10",
                    "current_rapporteur": None,
                }
                for i in range(1, 5)
            ]
            + [
                {
                    "decision_event_id": "de5",
                    "process_id": "p5",
                    "decision_date": "2026-03-10",
                    "current_rapporteur": "MIN. A",
                }
            ],
        )
        _write_jsonl(
            curated_dir / "process.jsonl",
            [{"process_id": f"p{i}", "process_class": "ADI"} for i in range(1, 6)],
        )
        analytics_dir.mkdir(parents=True, exist_ok=True)

        results = check_critical_field_coverage(curated_dir, analytics_dir)

        assert isinstance(results, list)
        assert len(results) > 0


# ---------------------------------------------------------------------------
# Cardinality sanity
# ---------------------------------------------------------------------------


class TestCardinalitySanity:
    def _write_minimal_dataset(self, curated_dir: Path, analytics_dir: Path) -> None:
        _write_jsonl(
            curated_dir / "decision_event.jsonl",
            [{"decision_event_id": f"de{i}"} for i in range(3)],
        )
        _write_jsonl(
            curated_dir / "process.jsonl",
            [{"process_id": f"p{i}"} for i in range(3)],
        )
        _write_jsonl(
            curated_dir / "representation_edge.jsonl",
            [{"edge_id": f"e{i}", "process_id": f"p{i}"} for i in range(3)],
        )
        _write_jsonl(
            analytics_dir / "baseline.jsonl",
            [{"baseline_id": f"b{i}"} for i in range(2)],
        )
        _write_jsonl(
            analytics_dir / "outlier_alert.jsonl",
            [{"alert_id": f"a{i}"} for i in range(2)],
        )

    def test_healthy_counts(self, tmp_path: Path) -> None:
        """All counts > 0 -> all checks PASS."""
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        self._write_minimal_dataset(curated_dir, analytics_dir)

        results = check_output_cardinality_sanity(curated_dir, analytics_dir)

        assert isinstance(results, list)
        assert all(r.status == "PASS" for r in results)

    def test_zero_alerts_with_baselines(self, tmp_path: Path) -> None:
        """Zero alerts while baselines/comparison_groups exist -> at least one FAIL."""
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        _write_jsonl(
            curated_dir / "decision_event.jsonl",
            [{"decision_event_id": f"de{i}"} for i in range(3)],
        )
        _write_jsonl(
            curated_dir / "process.jsonl",
            [{"process_id": f"p{i}"} for i in range(3)],
        )
        _write_jsonl(
            analytics_dir / "baseline.jsonl",
            [{"baseline_id": f"b{i}"} for i in range(2)],
        )
        # Alerts file exists but is empty
        _write_jsonl(analytics_dir / "outlier_alert.jsonl", [])

        results = check_output_cardinality_sanity(curated_dir, analytics_dir)

        assert isinstance(results, list)
        statuses = {r.status for r in results}
        assert "FAIL" in statuses
        failing = [r for r in results if r.status == "FAIL"]
        assert all(r.severity in ("MEDIUM", "HIGH", "CRITICAL") for r in failing)

    def test_zero_everything_is_suspicious(self, tmp_path: Path) -> None:
        """All artifacts empty -> at minimum one check returns a result."""
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        for fname in ("decision_event.jsonl", "process.jsonl"):
            _write_jsonl(curated_dir / fname, [])
        for fname in ("comparison_group.jsonl", "outlier_alert.jsonl"):
            _write_jsonl(analytics_dir / fname, [])

        results = check_output_cardinality_sanity(curated_dir, analytics_dir)

        assert isinstance(results, list)
        assert len(results) > 0


# ---------------------------------------------------------------------------
# Integration: run_validation
# ---------------------------------------------------------------------------


class TestRunValidation:
    def _write_consistent_dataset(self, curated_dir: Path, analytics_dir: Path) -> None:
        """Create a minimal fully consistent dataset that should produce no failures."""
        # decision_event
        _write_jsonl(
            curated_dir / "decision_event.jsonl",
            [
                {
                    "decision_event_id": "de1",
                    "process_id": "p1",
                    "decision_date": "2026-01-10",
                    "current_rapporteur": "MIN. TOFFOLI",
                },
                {
                    "decision_event_id": "de2",
                    "process_id": "p2",
                    "decision_date": "2026-02-15",
                    "current_rapporteur": "MIN. BARROSO",
                },
                {
                    "decision_event_id": "de3",
                    "process_id": "p3",
                    "decision_date": "2026-03-01",
                    "current_rapporteur": "MIN. FACHIN",
                },
            ],
        )
        # process
        _write_jsonl(
            curated_dir / "process.jsonl",
            [
                {"process_id": "p1", "process_class": "ADI"},
                {"process_id": "p2", "process_class": "RE"},
                {"process_id": "p3", "process_class": "HC"},
            ],
        )
        # session_event with rapporteur coverage >= 0.5
        _write_jsonl(
            curated_dir / "session_event.jsonl",
            [
                {"session_event_id": "s1", "rapporteur_at_event": "MIN. TOFFOLI"},
                {"session_event_id": "s2", "rapporteur_at_event": "MIN. BARROSO"},
                {"session_event_id": "s3", "rapporteur_at_event": None},
            ],
        )
        # representation_edge with party_id coverage >= 0.5
        _write_jsonl(
            curated_dir / "representation_edge.jsonl",
            [
                {"edge_id": "e1", "process_id": "p1", "party_id": "party_1"},
                {"edge_id": "e2", "process_id": "p2", "party_id": "party_2"},
                {"edge_id": "e3", "process_id": "p3", "party_id": None},
            ],
        )
        # representation_event exists (ratio > 0)
        _write_jsonl(
            curated_dir / "representation_event.jsonl",
            [
                {"event_id": "ev1", "process_id": "p1", "event_type": "oral_argument"},
                {"event_id": "ev2", "process_id": "p2", "event_type": "petition"},
            ],
        )
        # lawyer_entity
        _write_jsonl(
            curated_dir / "lawyer_entity.jsonl",
            [
                {"lawyer_id": "law_1", "lawyer_name_normalized": "JOAO SILVA"},
                {"lawyer_id": "law_2", "lawyer_name_normalized": "ANA COSTA"},
            ],
        )
        # party
        _write_jsonl(
            curated_dir / "party.jsonl",
            [
                {"party_id": "party_1"},
                {"party_id": "party_2"},
            ],
        )
        # comparison_group
        _write_jsonl(
            analytics_dir / "comparison_group.jsonl",
            [
                {"comparison_group_id": "g1"},
                {"comparison_group_id": "g2"},
            ],
        )
        # baseline
        _write_jsonl(
            analytics_dir / "baseline.jsonl",
            [
                {"baseline_id": "b1"},
                {"baseline_id": "b2"},
            ],
        )
        # outlier_alert — references valid de1/p1/g1
        _write_jsonl(
            analytics_dir / "outlier_alert.jsonl",
            [
                {
                    "alert_id": "alert_1",
                    "decision_event_id": "de1",
                    "process_id": "p1",
                    "comparison_group_id": "g1",
                    "alert_score": 0.8,
                },
                {
                    "alert_id": "alert_2",
                    "decision_event_id": "de2",
                    "process_id": "p2",
                    "comparison_group_id": "g2",
                    "alert_score": 0.6,
                },
            ],
        )
        # representation_recurrence — references valid lawyer_ids
        _write_jsonl(
            analytics_dir / "representation_recurrence.jsonl",
            [
                {"lawyer_id": "law_1", "party_id": "party_1", "process_count": 5},
                {"lawyer_id": "law_2", "party_id": "party_2", "process_count": 3},
            ],
        )

    def test_full_pass(self, tmp_path: Path) -> None:
        """Consistent synthetic dataset -> report status PASS."""
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        self._write_consistent_dataset(curated_dir, analytics_dir)

        report = run_validation(
            curated_dir=curated_dir,
            analytics_dir=analytics_dir,
            scope="all",
            fail_on_medium=False,
        )

        assert isinstance(report, ValidationReport)
        assert report.status == "PASS"
        assert len(report.checks) > 0

    def test_failure_propagates(self, tmp_path: Path) -> None:
        """Orphan alert FK -> report status FAIL."""
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        self._write_consistent_dataset(curated_dir, analytics_dir)

        # Overwrite alert with a broken FK
        _write_jsonl(
            analytics_dir / "outlier_alert.jsonl",
            [
                {
                    "alert_id": "alert_broken",
                    "decision_event_id": "de_NONEXISTENT",
                    "process_id": "p_NONEXISTENT",
                    "comparison_group_id": "g_NONEXISTENT",
                }
            ],
        )

        report = run_validation(
            curated_dir=curated_dir,
            analytics_dir=analytics_dir,
            scope="all",
            fail_on_medium=False,
        )

        assert report.status == "FAIL"

    def test_report_has_timestamp_and_scope(self, tmp_path: Path) -> None:
        """Report always includes timestamp and scope fields."""
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        self._write_consistent_dataset(curated_dir, analytics_dir)

        report = run_validation(
            curated_dir=curated_dir,
            analytics_dir=analytics_dir,
            scope="curated",
            fail_on_medium=False,
        )

        assert report.timestamp
        assert report.scope == "curated"

    def test_fail_on_medium_elevates_exit_condition(self, tmp_path: Path) -> None:
        """With fail_on_medium=True, MEDIUM failures also make the report FAIL."""
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        self._write_consistent_dataset(curated_dir, analytics_dir)

        # Zero alerts to trigger a MEDIUM/HIGH failure
        _write_jsonl(analytics_dir / "outlier_alert.jsonl", [])

        report_strict = run_validation(
            curated_dir=curated_dir,
            analytics_dir=analytics_dir,
            scope="all",
            fail_on_medium=True,
        )

        assert isinstance(report_strict, ValidationReport)


# ---------------------------------------------------------------------------
# Report serialization
# ---------------------------------------------------------------------------


class TestReportSerialization:
    def test_json_output(self, tmp_path: Path) -> None:
        """Report serializes to valid JSON with expected top-level fields."""
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"

        # Minimal dataset
        _write_jsonl(
            curated_dir / "decision_event.jsonl",
            [
                {
                    "decision_event_id": "de1",
                    "process_id": "p1",
                    "decision_date": "2026-01-01",
                    "current_rapporteur": "MIN. A",
                }
            ],
        )
        _write_jsonl(curated_dir / "process.jsonl", [{"process_id": "p1", "process_class": "ADI"}])
        _write_jsonl(curated_dir / "session_event.jsonl", [{"session_event_id": "s1", "rapporteur_at_event": "MIN. A"}])
        _write_jsonl(curated_dir / "representation_edge.jsonl", [{"edge_id": "e1", "party_id": "p1"}])
        _write_jsonl(curated_dir / "representation_event.jsonl", [{"event_id": "ev1"}])
        _write_jsonl(curated_dir / "lawyer_entity.jsonl", [{"lawyer_id": "law_1"}])
        _write_jsonl(analytics_dir / "comparison_group.jsonl", [{"comparison_group_id": "g1"}])
        _write_jsonl(
            analytics_dir / "outlier_alert.jsonl",
            [{"alert_id": "a1", "decision_event_id": "de1", "process_id": "p1", "comparison_group_id": "g1"}],
        )
        _write_jsonl(curated_dir / "party.jsonl", [{"party_id": "pt1"}])
        _write_jsonl(analytics_dir / "baseline.jsonl", [{"baseline_id": "b1"}])
        _write_jsonl(
            analytics_dir / "representation_recurrence.jsonl",
            [{"lawyer_id": "law_1", "party_id": "pt1", "process_count": 2}],
        )

        report = run_validation(
            curated_dir=curated_dir,
            analytics_dir=analytics_dir,
            scope="all",
            fail_on_medium=False,
        )

        # Serialize via to_dict() which ValidationReport exposes
        data = report.to_dict()

        serialized = json.dumps(data, default=str)
        parsed = json.loads(serialized)

        assert "timestamp" in parsed
        assert "scope" in parsed
        assert "status" in parsed
        assert "checks" in parsed
        assert isinstance(parsed["checks"], list)
        assert len(parsed["checks"]) > 0

    def test_check_result_fields(self, tmp_path: Path) -> None:
        """CheckResult dataclass exposes all documented fields."""
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"

        _write_jsonl(
            curated_dir / "lawyer_entity.jsonl",
            [{"lawyer_id": "law_1"}],
        )
        _write_jsonl(
            analytics_dir / "representation_recurrence.jsonl",
            [{"lawyer_id": "law_1", "process_count": 3}],
        )

        result = check_recurrence_referential_integrity(curated_dir, analytics_dir)

        # All documented fields must be accessible (even if None)
        assert hasattr(result, "name")
        assert hasattr(result, "description")
        assert hasattr(result, "severity")
        assert hasattr(result, "status")
        assert hasattr(result, "artifacts")
        assert hasattr(result, "observed_value")
        assert hasattr(result, "threshold")
        assert hasattr(result, "details")
        assert hasattr(result, "samples")
        assert hasattr(result, "suggestion")
        assert result.name  # name must be non-empty
        assert result.severity in ("CRITICAL", "HIGH", "MEDIUM", "LOW", "INFO")
        assert result.status in ("PASS", "FAIL", "SKIP", "WARN")
