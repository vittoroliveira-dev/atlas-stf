"""Tests for donation_match: corporate enrichment."""

from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics._corporate_enrichment import (
    build_corporate_enrichment_index,
    enrich_match_corporate,
)
from atlas_stf.analytics.donation_match import build_donation_matches
from tests.analytics._donation_match_helpers import _setup_test_data, _write_jsonl


class TestCorporateEnrichment:
    """Corporate enrichment: annotate matches with corporate identity, group, and network."""

    def _make_match(self, donor_identity_key: str = "cpf:12345678000199") -> dict:
        return {
            "match_id": "dm-test",
            "entity_type": "party",
            "donor_identity_key": donor_identity_key,
        }

    def test_enrichment_with_all_artifacts(self, tmp_path: Path) -> None:
        """All 12 fields should be populated when artifacts are present."""
        _write_jsonl(
            tmp_path / "donor_corporate_link.jsonl",
            [
                {
                    "link_id": "lk1",
                    "donor_identity_key": "cpf:12345678000199",
                    "donor_document_type": "cnpj",
                    "donor_tax_id_normalized": "12345678000199",
                    "donor_cnpj_basico": "12345678",
                    "link_basis": "exact_cnpj_basico",
                    "company_cnpj_basico": "12345678",
                    "company_name": "ACME CORP LTDA",
                    "confidence": "deterministic",
                },
            ],
        )
        _write_jsonl(
            tmp_path / "economic_group.jsonl",
            [
                {
                    "group_id": "g1",
                    "member_cnpjs": ["12345678"],
                    "member_count": 3,
                    "is_law_firm_group": False,
                    "has_minister_partner": True,
                    "has_party_partner": False,
                    "has_counsel_partner": True,
                },
            ],
        )
        _write_jsonl(
            tmp_path / "corporate_network.jsonl",
            [
                {
                    "company_cnpj_basico": "12345678",
                    "link_degree": 2,
                    "red_flag": True,
                },
            ],
        )
        index = build_corporate_enrichment_index(tmp_path)
        match = self._make_match()
        enrich_match_corporate(match, index)

        assert match["donor_document_type"] == "cnpj"
        assert match["donor_tax_id_normalized"] == "12345678000199"
        assert match["donor_cnpj_basico"] == "12345678"
        assert match["donor_company_name"] == "ACME CORP LTDA"
        assert match["economic_group_id"] == "g1"
        assert match["economic_group_member_count"] == 3
        assert match["is_law_firm_group"] is False
        assert match["donor_group_has_minister_partner"] is True
        assert match["donor_group_has_party_partner"] is False
        assert match["donor_group_has_counsel_partner"] is True
        assert match["min_link_degree_to_minister"] == 2
        assert match["corporate_link_red_flag"] is True

    def test_no_artifacts_all_none(self, tmp_path: Path) -> None:
        """When no artifacts exist, all 12 fields should be None."""
        index = build_corporate_enrichment_index(tmp_path)
        match = self._make_match()
        enrich_match_corporate(match, index)

        for field in (
            "donor_document_type",
            "donor_tax_id_normalized",
            "donor_cnpj_basico",
            "donor_company_name",
            "economic_group_id",
            "economic_group_member_count",
            "is_law_firm_group",
            "donor_group_has_minister_partner",
            "donor_group_has_party_partner",
            "donor_group_has_counsel_partner",
            "min_link_degree_to_minister",
            "corporate_link_red_flag",
        ):
            assert match[field] is None, f"{field} should be None"

    def test_pj_vs_pf_cnpj_basico(self, tmp_path: Path) -> None:
        """donor_cnpj_basico should only populate for Path A (exact_cnpj_basico)."""
        # PF donor with Path C (exact_partner_cpf) → no donor_cnpj_basico
        _write_jsonl(
            tmp_path / "donor_corporate_link.jsonl",
            [
                {
                    "link_id": "lk-pf",
                    "donor_identity_key": "cpf:11122233344",
                    "donor_document_type": "cpf",
                    "donor_tax_id_normalized": "11122233344",
                    "donor_cnpj_basico": None,
                    "link_basis": "exact_partner_cpf",
                    "company_cnpj_basico": "99887766",
                    "company_name": "EMPRESA X",
                    "confidence": "deterministic",
                },
            ],
        )
        index = build_corporate_enrichment_index(tmp_path)
        match = self._make_match("cpf:11122233344")
        enrich_match_corporate(match, index)

        assert match["donor_document_type"] == "cpf"
        assert match["donor_tax_id_normalized"] == "11122233344"
        assert match["donor_cnpj_basico"] is None
        assert match["donor_company_name"] is None

    def test_multiple_links_aggregation(self, tmp_path: Path) -> None:
        """Multiple links: largest group selected, flags OR-ed, min degree."""
        _write_jsonl(
            tmp_path / "donor_corporate_link.jsonl",
            [
                {
                    "link_id": "lk1",
                    "donor_identity_key": "cpf:AAA",
                    "donor_document_type": "cnpj",
                    "donor_tax_id_normalized": "AAA",
                    "donor_cnpj_basico": "11111111",
                    "link_basis": "exact_cnpj_basico",
                    "company_cnpj_basico": "11111111",
                    "company_name": "COMPANY A",
                    "confidence": "deterministic",
                },
                {
                    "link_id": "lk2",
                    "donor_identity_key": "cpf:AAA",
                    "donor_document_type": "cnpj",
                    "donor_tax_id_normalized": "AAA",
                    "donor_cnpj_basico": None,
                    "link_basis": "exact_partner_cnpj",
                    "company_cnpj_basico": "22222222",
                    "company_name": "COMPANY B",
                    "confidence": "deterministic",
                },
            ],
        )
        _write_jsonl(
            tmp_path / "economic_group.jsonl",
            [
                {
                    "group_id": "g-small",
                    "member_cnpjs": ["11111111"],
                    "member_count": 2,
                    "is_law_firm_group": False,
                    "has_minister_partner": False,
                    "has_party_partner": True,
                    "has_counsel_partner": False,
                },
                {
                    "group_id": "g-big",
                    "member_cnpjs": ["22222222"],
                    "member_count": 10,
                    "is_law_firm_group": True,
                    "has_minister_partner": True,
                    "has_party_partner": False,
                    "has_counsel_partner": False,
                },
            ],
        )
        _write_jsonl(
            tmp_path / "corporate_network.jsonl",
            [
                {"company_cnpj_basico": "11111111", "link_degree": 3, "red_flag": False},
                {"company_cnpj_basico": "22222222", "link_degree": 1, "red_flag": True},
            ],
        )
        index = build_corporate_enrichment_index(tmp_path)
        match = self._make_match("cpf:AAA")
        enrich_match_corporate(match, index)

        # Largest group is g-big (member_count=10)
        assert match["economic_group_id"] == "g-big"
        assert match["economic_group_member_count"] == 10
        # OR of flags across both groups
        assert match["donor_group_has_minister_partner"] is True
        assert match["donor_group_has_party_partner"] is True
        # Min degree across all network records
        assert match["min_link_degree_to_minister"] == 1
        assert match["corporate_link_red_flag"] is True

    def test_low_confidence_links_ignored(self, tmp_path: Path) -> None:
        """Links with confidence != deterministic should not enrich."""
        _write_jsonl(
            tmp_path / "donor_corporate_link.jsonl",
            [
                {
                    "link_id": "lk-low",
                    "donor_identity_key": "cpf:IGNORED",
                    "donor_document_type": "cpf",
                    "donor_tax_id_normalized": "IGNORED",
                    "donor_cnpj_basico": None,
                    "link_basis": "not_in_rfb_corpus",
                    "company_cnpj_basico": None,
                    "company_name": None,
                    "confidence": "low",
                },
                {
                    "link_id": "lk-unresolved",
                    "donor_identity_key": "cpf:IGNORED",
                    "donor_document_type": "unknown",
                    "donor_tax_id_normalized": None,
                    "donor_cnpj_basico": None,
                    "link_basis": "masked_cpf",
                    "company_cnpj_basico": None,
                    "company_name": None,
                    "confidence": "unresolved",
                },
            ],
        )
        index = build_corporate_enrichment_index(tmp_path)
        assert index.has_corporate_links is True
        match = self._make_match("cpf:IGNORED")
        enrich_match_corporate(match, index)

        for field in (
            "donor_document_type",
            "donor_tax_id_normalized",
            "donor_cnpj_basico",
            "donor_company_name",
            "economic_group_id",
            "economic_group_member_count",
            "is_law_firm_group",
            "donor_group_has_minister_partner",
            "donor_group_has_party_partner",
            "donor_group_has_counsel_partner",
            "min_link_degree_to_minister",
            "corporate_link_red_flag",
        ):
            assert match[field] is None, f"{field} should be None for non-deterministic"

    def test_group_tiebreak_by_group_id(self, tmp_path: Path) -> None:
        """Two groups with same member_count: select smallest group_id."""
        _write_jsonl(
            tmp_path / "donor_corporate_link.jsonl",
            [
                {
                    "link_id": "lk1",
                    "donor_identity_key": "cpf:TIE",
                    "donor_document_type": "cnpj",
                    "donor_tax_id_normalized": "TIE",
                    "donor_cnpj_basico": "AAAA",
                    "link_basis": "exact_cnpj_basico",
                    "company_cnpj_basico": "AAAA",
                    "company_name": "A",
                    "confidence": "deterministic",
                },
                {
                    "link_id": "lk2",
                    "donor_identity_key": "cpf:TIE",
                    "donor_document_type": "cnpj",
                    "donor_tax_id_normalized": "TIE",
                    "donor_cnpj_basico": None,
                    "link_basis": "exact_partner_cnpj",
                    "company_cnpj_basico": "BBBB",
                    "company_name": "B",
                    "confidence": "deterministic",
                },
            ],
        )
        _write_jsonl(
            tmp_path / "economic_group.jsonl",
            [
                {
                    "group_id": "g-zebra",
                    "member_cnpjs": ["AAAA"],
                    "member_count": 5,
                    "is_law_firm_group": False,
                    "has_minister_partner": False,
                    "has_party_partner": False,
                    "has_counsel_partner": False,
                },
                {
                    "group_id": "g-alpha",
                    "member_cnpjs": ["BBBB"],
                    "member_count": 5,
                    "is_law_firm_group": False,
                    "has_minister_partner": False,
                    "has_party_partner": False,
                    "has_counsel_partner": False,
                },
            ],
        )
        index = build_corporate_enrichment_index(tmp_path)
        match = self._make_match("cpf:TIE")
        enrich_match_corporate(match, index)

        assert match["economic_group_id"] == "g-alpha"

    def test_partial_enrichment_no_group_no_network(self, tmp_path: Path) -> None:
        """donor_corporate_link present, but no group/network artifacts."""
        _write_jsonl(
            tmp_path / "donor_corporate_link.jsonl",
            [
                {
                    "link_id": "lk1",
                    "donor_identity_key": "cpf:PARTIAL",
                    "donor_document_type": "cnpj",
                    "donor_tax_id_normalized": "PARTIAL",
                    "donor_cnpj_basico": "55555555",
                    "link_basis": "exact_cnpj_basico",
                    "company_cnpj_basico": "55555555",
                    "company_name": "PARTIAL CORP",
                    "confidence": "deterministic",
                },
            ],
        )
        index = build_corporate_enrichment_index(tmp_path)
        assert index.has_corporate_links is True
        assert index.has_economic_groups is False
        assert index.has_corporate_network is False

        match = self._make_match("cpf:PARTIAL")
        enrich_match_corporate(match, index)

        # Identity fields filled
        assert match["donor_document_type"] == "cnpj"
        assert match["donor_tax_id_normalized"] == "PARTIAL"
        assert match["donor_cnpj_basico"] == "55555555"
        assert match["donor_company_name"] == "PARTIAL CORP"
        # Group and network fields None
        assert match["economic_group_id"] is None
        assert match["economic_group_member_count"] is None
        assert match["is_law_firm_group"] is None
        assert match["donor_group_has_minister_partner"] is None
        assert match["min_link_degree_to_minister"] is None
        assert match["corporate_link_red_flag"] is None

    def test_build_donation_matches_includes_enrichment_in_summary(self, tmp_path: Path) -> None:
        """Summary should include corporate enrichment metadata."""
        paths = _setup_test_data(tmp_path)
        build_donation_matches(**paths)

        summary = json.loads((paths["output_dir"] / "donation_match_summary.json").read_text())
        assert "corporate_links_present" in summary
        assert "economic_groups_present" in summary
        assert "corporate_network_present" in summary
        assert "corporate_enriched_count" in summary
        # No artifacts → all False, enriched_count = 0
        assert summary["corporate_links_present"] is False
        assert summary["corporate_enriched_count"] == 0
