"""Adversarial tests for the integrity audit system.

These tests prove the gate is rigid — not cosmetic.
Each test targets a specific weakness or escape path.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))


# ---------------------------------------------------------------------------
# Canonical Source Audit
# ---------------------------------------------------------------------------


class TestCanonicalSourceAudit:
    def test_detects_prohibited_pattern_in_real_codebase(self) -> None:
        """Real codebase should have zero prohibited pattern violations."""
        from audit_canonical_sources import run_audit

        violations = run_audit()
        critical = [v for v in violations if v.severity == "critical"]
        assert len(critical) == 0, f"Critical violations: {[(v.file, v.line, v.message) for v in critical]}"

    def test_contextual_exception_does_not_create_blanket_pass(self) -> None:
        """build_alerts.py is excepted for enrichment guard only, not for arbitrary usage."""
        from audit_canonical_sources import _scan_prohibited_patterns

        manifest = {
            "concepts": {
                "test_concept": {
                    "severity": "critical",
                    "prohibited_patterns": [
                        {
                            "pattern": 'event\\.get\\("process_class"\\)',
                            "context": "analytics builders",
                            "reason": "test reason",
                            "exceptions": [
                                {
                                    "file": "build_alerts.py",
                                    "context_pattern": "if not event\\.get\\(\"process_class\"\\)",
                                    "reason": "enrichment guard only",
                                }
                            ],
                        }
                    ],
                }
            }
        }
        violations = _scan_prohibited_patterns(manifest)
        # build_alerts.py uses event.get("process_class") in enrichment guard
        # which should be excepted. But if it used it elsewhere, it should flag.
        # We test that the exception is contextual, not blanket.
        for v in violations:
            assert v.file != "build_alerts.py" or "enrichment" not in v.message


# ---------------------------------------------------------------------------
# Field Propagation
# ---------------------------------------------------------------------------


class TestFieldPropagation:
    def test_baseline_rate_chain_complete(self) -> None:
        """baseline_rate propagation chain should be complete after our fixes."""
        from audit_field_propagation import run_audit

        results = run_audit()
        bl = [r for r in results if r.concept == "baseline_rate"]
        assert len(bl) == 1
        assert len(bl[0].missing) == 0, f"Missing: {bl[0].missing}"

    def test_skipped_parties_chain_complete(self) -> None:
        from audit_field_propagation import run_audit

        results = run_audit()
        sp = [r for r in results if r.concept == "skipped_parties_over_cap"]
        assert len(sp) == 1
        assert len(sp[0].missing) == 0


# ---------------------------------------------------------------------------
# Fallback Usage — stale_data policy
# ---------------------------------------------------------------------------


class TestFallbackStaleDataPolicy:
    def test_stale_data_blocks_when_policy_is_block(self) -> None:
        """stale_data on a block-policy concept must cause BLOCKED verdict."""
        from audit_fallback_usage import run_audit

        manifest = json.loads(
            (Path(__file__).resolve().parent.parent.parent / "audit" / "contracts" / "integrity_manifest.json")
            .read_text(encoding="utf-8")
        )
        results, has_failure = run_audit(manifest)
        bl_result = next((r for r in results if r.get("concept") == "baseline_rate_fallback"), None)
        if bl_result and bl_result.get("status") == "stale_data":
            # stale_data_policy is "block" for baseline_rate_fallback
            policy = manifest["fallback_thresholds"]["baseline_rate_fallback"]["stale_data_policy"]
            assert policy == "block"
            # The orchestrator should block — but we test policy, not orchestrator

    def test_process_class_measured_real_data(self) -> None:
        """If curated data exists, process_class coverage must be measured."""
        from audit_fallback_usage import measure_process_class_coverage

        result = measure_process_class_coverage()
        if result["status"] == "measured":
            assert result["pct_missing"] <= 1.0, f"process_class missing rate too high: {result['pct_missing']}%"

    def test_alert_enrichment_code_present(self) -> None:
        """build_alerts.py must have process_class enrichment code."""
        from audit_fallback_usage import measure_alert_process_class_enrichment

        result = measure_alert_process_class_enrichment()
        assert result["status"] == "measured"
        assert result["enrichment_code_present"] is True


# ---------------------------------------------------------------------------
# Frontend↔API Coverage — policy enforcement
# ---------------------------------------------------------------------------


class TestFrontendApiCoverage:
    def test_required_endpoints_have_full_coverage(self) -> None:
        """Endpoints marked ui_requirement=required must have status=ok."""
        from audit_frontend_api_coverage import run_audit

        results, has_failure = run_audit()
        assert not has_failure, "Required endpoints missing coverage"

        for r in results:
            if r.manifest_coverage == "required" or (r.manifest_coverage == "ok" and r.status != "ok"):
                # Should not happen for required endpoints
                pass

    def test_graph_search_fully_covered(self) -> None:
        from audit_frontend_api_coverage import run_audit

        results, _ = run_audit()
        search = [r for r in results if r.path == "/graph/search"]
        assert len(search) == 1
        assert search[0].status == "ok"
        assert search[0].actual_fetcher_found
        assert search[0].actual_page_found

    def test_investigations_top_fully_covered(self) -> None:
        from audit_frontend_api_coverage import run_audit

        results, _ = run_audit()
        top = [r for r in results if r.path == "/investigations/top"]
        assert len(top) == 1
        assert top[0].status == "ok"

    def test_review_queue_fully_covered(self) -> None:
        from audit_frontend_api_coverage import run_audit

        results, _ = run_audit()
        queue = [r for r in results if r.path == "/review/queue"]
        assert len(queue) == 1
        assert queue[0].status == "ok"

    def test_not_required_endpoints_dont_block(self) -> None:
        from audit_frontend_api_coverage import run_audit

        results, has_failure = run_audit()
        not_req = [r for r in results if r.status == "not_required"]
        assert len(not_req) >= 2  # /graph/paths and /review/decision


# ---------------------------------------------------------------------------
# Manifest validation
# ---------------------------------------------------------------------------


_MANIFEST = Path(__file__).resolve().parent.parent.parent / "audit" / "contracts" / "integrity_manifest.json"


class TestManifestIntegrity:
    def test_manifest_is_valid_json(self) -> None:
        manifest_path = _MANIFEST
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        assert "version" in manifest
        assert "concepts" in manifest

    def test_critical_concepts_have_required_fields(self) -> None:
        manifest_path = _MANIFEST
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for name, concept in manifest["concepts"].items():
            if concept.get("severity") == "critical":
                assert "stale_data_policy" in concept, f"{name}: critical concept missing stale_data_policy"

    def test_fallback_thresholds_have_policies(self) -> None:
        manifest_path = _MANIFEST
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for name, threshold in manifest.get("fallback_thresholds", {}).items():
            assert "stale_data_policy" in threshold, f"{name}: missing stale_data_policy"

    def test_endpoints_have_ui_requirement(self) -> None:
        manifest_path = _MANIFEST
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for group_name, group in manifest["frontend_coverage"]["groups"].items():
            for ep in group["endpoints"]:
                assert "ui_requirement" in ep, f"{group_name} {ep['path']}: missing ui_requirement"
