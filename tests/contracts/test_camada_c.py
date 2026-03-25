"""Tests for Camada C: provenance, dedup, semantic audit, entity identity."""

from __future__ import annotations

from pathlib import Path

import pytest

from atlas_stf.contracts._entity_identity import (
    IdentifierQuality,
    IdentifierType,
    best_identifier,
    canonical_join_key,
    classify_identifier,
)

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_STAGING_DIR = _PROJECT_ROOT / "data" / "staging" / "transparencia"
_OBSERVED_DIR = _PROJECT_ROOT / "data" / "contracts" / "observed"
_GOVERNANCE_DIR = _PROJECT_ROOT / "data" / "contracts" / "governance"

_skip_no_staging = pytest.mark.skipif(not _STAGING_DIR.exists(), reason="staging dir not available")
_skip_no_governance = pytest.mark.skipif(not _GOVERNANCE_DIR.exists(), reason="governance dir not available")


# ---------------------------------------------------------------------------
# TestEntityIdentity — unit tests, no data dependency
# ---------------------------------------------------------------------------


class TestEntityIdentity:
    def test_valid_cnpj_classified_deterministic(self) -> None:
        result = classify_identifier("33000167000101")
        assert result.identifier_type == IdentifierType.CNPJ_VALIDATED
        assert result.quality == IdentifierQuality.DETERMINISTIC

    def test_valid_cpf_classified_deterministic(self) -> None:
        result = classify_identifier("12345678909")
        assert result.identifier_type == IdentifierType.CPF_VALIDATED
        assert result.quality == IdentifierQuality.DETERMINISTIC

    def test_masked_cpf_classified_blocked(self) -> None:
        result = classify_identifier("***123456**")
        assert result.identifier_type == IdentifierType.TAX_ID_MASKED
        assert result.quality == IdentifierQuality.BLOCKED

    def test_invalid_digits_classified_medium(self) -> None:
        # 8+ digits but not a valid CPF/CNPJ → TAX_ID_UNVALIDATED, MEDIUM
        result = classify_identifier("12345678")
        assert result.identifier_type == IdentifierType.TAX_ID_UNVALIDATED
        assert result.quality == IdentifierQuality.MEDIUM

    def test_name_classified_medium(self) -> None:
        result = classify_identifier("EMPRESA TESTE LTDA")
        assert result.identifier_type == IdentifierType.NAME_NORMALIZED
        assert result.quality == IdentifierQuality.MEDIUM

    def test_empty_classified_blocked(self) -> None:
        result = classify_identifier("")
        assert result.quality == IdentifierQuality.BLOCKED

    def test_best_identifier_prefers_cnpj(self) -> None:
        cnpj_id = classify_identifier("33000167000101")
        name_id = classify_identifier("EMPRESA TESTE LTDA")
        best = best_identifier([cnpj_id, name_id])
        assert best is not None
        assert best.identifier_type == IdentifierType.CNPJ_VALIDATED

    def test_best_identifier_skips_blocked(self) -> None:
        empty_id = classify_identifier("")
        masked_id = classify_identifier("***123456**")
        assert best_identifier([empty_id, masked_id]) is None

    def test_canonical_join_key_format(self) -> None:
        cnpj_id = classify_identifier("33000167000101")
        name_id = classify_identifier("EMPRESA TESTE LTDA")
        assert canonical_join_key(cnpj_id) == "taxid:33000167000101"
        assert canonical_join_key(name_id) == "name:EMPRESA TESTE LTDA"

    # Edge cases
    def test_invalid_cpf_not_deterministic(self) -> None:
        """111.111.111-11 has valid length but fails check digit."""
        result = classify_identifier("11111111111")
        assert result.identifier_type == IdentifierType.TAX_ID_UNVALIDATED
        assert result.quality == IdentifierQuality.MEDIUM

    def test_formatted_cnpj_normalized(self) -> None:
        """CNPJ with dots/slashes should be normalized to digits."""
        result = classify_identifier("33.000.167/0001-01")
        assert result.identifier_type == IdentifierType.CNPJ_VALIDATED
        assert result.normalized_value == "33000167000101"

    def test_whitespace_only_is_blocked(self) -> None:
        result = classify_identifier("   ")
        assert result.quality == IdentifierQuality.BLOCKED

    def test_none_like_is_blocked(self) -> None:
        result = classify_identifier("")
        assert result.quality == IdentifierQuality.BLOCKED
        key = canonical_join_key(result)
        assert key is None

    def test_short_digits_are_name(self) -> None:
        """Fewer than 8 digits → classified as name, not tax ID."""
        result = classify_identifier("123")
        assert result.identifier_type == IdentifierType.NAME_NORMALIZED

    def test_accentuated_name_normalized(self) -> None:
        """Brazilian names with accents get accent-stripped + uppercased."""
        result = classify_identifier("José da Silva")
        assert result.identifier_type == IdentifierType.NAME_NORMALIZED
        assert result.normalized_value == "JOSE DA SILVA"  # accents stripped


# ---------------------------------------------------------------------------
# TestSemanticAudit — uses governance + observed data when available
# ---------------------------------------------------------------------------


class TestSemanticAudit:
    @pytest.fixture(scope="class")
    def audit(self) -> dict:  # type: ignore[type-arg]
        from atlas_stf.contracts._semantic_audit import build_semantic_audit

        return build_semantic_audit(_GOVERNANCE_DIR, _OBSERVED_DIR)

    @_skip_no_governance
    def test_semantic_audit_has_known_findings(self, audit: dict) -> None:  # type: ignore[type-arg]
        assert len(audit["findings"]) >= 6

    @_skip_no_governance
    def test_rfb_representative_blocked(self, audit: dict) -> None:  # type: ignore[type-arg]
        finding = next(
            (f for f in audit["findings"] if f["finding_id"] == "rfb_representative_high_null"),
            None,
        )
        assert finding is not None, "finding rfb_representative_high_null not present"
        assert finding["status"] == "blocked"

    @_skip_no_governance
    def test_covid_dual_use_detected(self, audit: dict) -> None:  # type: ignore[type-arg]
        finding = next(
            (f for f in audit["findings"] if f["finding_id"] == "covid_tipo_decisao_dual_use"),
            None,
        )
        assert finding is not None, "finding covid_tipo_decisao_dual_use not present"
        assert finding["status"] == "degraded"

    @_skip_no_governance
    def test_no_unknown_statuses(self, audit: dict) -> None:  # type: ignore[type-arg]
        valid_statuses = {"safe", "degraded", "blocked", "requires_manual_review"}
        for f in audit["findings"]:
            assert f["status"] in valid_statuses, f"finding {f['finding_id']!r} has unknown status {f['status']!r}"

    @_skip_no_governance
    def test_summary_counts_match(self, audit: dict) -> None:  # type: ignore[type-arg]
        assert audit["summary"]["total_findings"] == len(audit["findings"])


# ---------------------------------------------------------------------------
# TestStfOverlap — needs staging data
# ---------------------------------------------------------------------------


class TestStfOverlap:
    @pytest.fixture(scope="class")
    def overlap(self) -> dict:  # type: ignore[type-arg]
        from atlas_stf.contracts._stf_overlap import analyze_stf_overlap

        return analyze_stf_overlap(_STAGING_DIR)

    @_skip_no_staging
    def test_overlap_analysis_structure(self, overlap: dict) -> None:  # type: ignore[type-arg]
        assert "pairwise_overlap" in overlap
        assert "structural_duplicates" in overlap
        assert "per_file_profile" in overlap

    @_skip_no_staging
    def test_per_file_profiles_present(self, overlap: dict) -> None:  # type: ignore[type-arg]
        assert len(overlap["per_file_profile"]) >= 2

    @_skip_no_staging
    def test_overlap_rates_plausible(self, overlap: dict) -> None:  # type: ignore[type-arg]
        rates = [info.get("overlap_rate") for info in overlap["pairwise_overlap"].values()]
        # No pair should have overlap = 1.0 (would indicate complete duplication bug)
        assert all(r != 1.0 for r in rates), "at least one pair has overlap_rate=1.0 (likely bug)"
        # At least one pair must have non-zero overlap (sanity check that analysis ran)
        assert any(r is not None and r > 0.0 for r in rates), (
            "all pairs have overlap_rate=0.0 — analysis may have failed"
        )

    @_skip_no_staging
    def test_structural_duplicates_detected(self, overlap: dict) -> None:  # type: ignore[type-arg]
        assert overlap["structural_duplicates"]["total_candidate_count"] > 0

    @_skip_no_staging
    def test_key_mismatch_flagged(self, overlap: dict) -> None:
        """Pairs with different process columns should be flagged as key_format_mismatch."""
        pairwise = overlap["pairwise_overlap"]
        mismatch_pairs = [k for k, v in pairwise.items() if v.get("match_status") == "key_format_mismatch"]
        # distribuidos uses no_do_processo while others use processo
        assert len(mismatch_pairs) > 0, "No key_format_mismatch pairs found (distribuidos should trigger)"
        # Pairs without mismatch should be "matched"
        matched_pairs = [k for k, v in pairwise.items() if v.get("match_status") == "matched"]
        assert len(matched_pairs) > 0, "No matched pairs found"
