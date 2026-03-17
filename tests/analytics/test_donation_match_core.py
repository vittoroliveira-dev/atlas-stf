"""Tests for donation_match: stream aggregation, core matching, counsel, identity, stability."""

from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics.donation_match import (
    _donor_identity_key,
    _stream_aggregate_donations,
    build_donation_matches,
)
from tests.analytics._donation_match_helpers import _setup_test_data, _write_jsonl


class TestStreamAggregation:
    def test_streaming_matches_batch(self, tmp_path: Path) -> None:
        """Stream aggregation should produce identical results to the old batch approach."""
        path = tmp_path / "donations.jsonl"
        _write_jsonl(
            path,
            [
                {
                    "donor_name_normalized": "A",
                    "donor_cpf_cnpj": "111",
                    "donation_amount": 100.0,
                    "election_year": 2022,
                },
                {
                    "donor_name_normalized": "A",
                    "donor_cpf_cnpj": "111",
                    "donation_amount": 200.0,
                    "election_year": 2018,
                },
                {"donor_name_normalized": "B", "donor_cpf_cnpj": "222", "donation_amount": 50.0},
            ],
        )
        agg, raw_count, all_years = _stream_aggregate_donations(path)
        assert raw_count == 3
        assert len(agg) == 2
        # Keys are now stable identity keys (cpf:NNN or name:NNN)
        assert agg["cpf:111"]["total_donated_brl"] == 300.0
        assert agg["cpf:111"]["donation_count"] == 2
        assert agg["cpf:111"]["donor_name_normalized"] == "A"
        assert all_years == {2018, 2022}

    def test_aggregation_by_cpf_prevents_homonym_fusion(self, tmp_path: Path) -> None:
        """Donors with same name but different CPF/CNPJ should NOT be merged."""
        path = tmp_path / "donations.jsonl"
        _write_jsonl(
            path,
            [
                {"donor_name_normalized": "JOAO SILVA", "donor_cpf_cnpj": "111", "donation_amount": 100.0},
                {"donor_name_normalized": "JOAO SILVA", "donor_cpf_cnpj": "222", "donation_amount": 200.0},
            ],
        )
        agg, raw_count, _ = _stream_aggregate_donations(path)
        assert raw_count == 2
        assert len(agg) == 2
        assert agg["cpf:111"]["total_donated_brl"] == 100.0
        assert agg["cpf:222"]["total_donated_brl"] == 200.0

    def test_aggregation_falls_back_to_name_when_no_cpf(self, tmp_path: Path) -> None:
        """Donors without CPF/CNPJ should be aggregated by name."""
        path = tmp_path / "donations.jsonl"
        _write_jsonl(
            path,
            [
                {"donor_name_normalized": "ACME", "donor_cpf_cnpj": "", "donation_amount": 100.0},
                {"donor_name_normalized": "ACME", "donor_cpf_cnpj": "", "donation_amount": 50.0},
            ],
        )
        agg, raw_count, _ = _stream_aggregate_donations(path)
        assert raw_count == 2
        assert len(agg) == 1
        assert agg["name:ACME"]["total_donated_brl"] == 150.0

    def test_empty_file(self, tmp_path: Path) -> None:
        path = tmp_path / "empty.jsonl"
        path.write_text("")
        agg, raw_count, all_years = _stream_aggregate_donations(path)
        assert raw_count == 0
        assert len(agg) == 0
        assert all_years == set()


class TestBuildDonationMatches:
    def test_basic_match(self, tmp_path: Path) -> None:
        paths = _setup_test_data(tmp_path)
        build_donation_matches(**paths)

        match_path = paths["output_dir"] / "donation_match.jsonl"
        assert match_path.exists()
        matches = [json.loads(line) for line in match_path.read_text().strip().split("\n")]
        party_matches = [m for m in matches if m["entity_type"] == "party"]
        assert len(party_matches) == 1
        assert party_matches[0]["party_id"] == "p1"
        assert party_matches[0]["donor_cpf_cnpj"] == "12345678000199"
        assert party_matches[0]["stf_case_count"] == 3
        assert party_matches[0]["entity_type"] == "party"

    def test_donation_aggregation(self, tmp_path: Path) -> None:
        paths = _setup_test_data(tmp_path)
        # Add a second donation for same donor
        tse_dir = paths["tse_dir"]
        donations = [
            {
                "election_year": 2022,
                "donor_name_normalized": "ACME CORP",
                "donor_cpf_cnpj": "12345678000199",
                "donation_amount": 50000.0,
                "party_abbrev": "PT",
                "candidate_name": "FULANO",
                "position": "SENADOR",
            },
            {
                "election_year": 2018,
                "donor_name_normalized": "ACME CORP",
                "donor_cpf_cnpj": "12345678000199",
                "donation_amount": 30000.0,
                "party_abbrev": "MDB",
                "candidate_name": "CICLANO",
                "position": "DEPUTADO",
            },
        ]
        _write_jsonl(tse_dir / "donations_raw.jsonl", donations)

        build_donation_matches(**paths)

        match_path = paths["output_dir"] / "donation_match.jsonl"
        matches = [json.loads(line) for line in match_path.read_text().strip().split("\n")]
        party_matches = [m for m in matches if m["entity_type"] == "party"]
        assert len(party_matches) == 1
        assert party_matches[0]["total_donated_brl"] == 80000.0
        assert party_matches[0]["donation_count"] == 2
        assert set(party_matches[0]["election_years"]) == {2018, 2022}
        assert set(party_matches[0]["parties_donated_to"]) == {"MDB", "PT"}

    def test_favorable_rate_computed(self, tmp_path: Path) -> None:
        paths = _setup_test_data(tmp_path)
        build_donation_matches(**paths)

        match_path = paths["output_dir"] / "donation_match.jsonl"
        matches = [json.loads(line) for line in match_path.read_text().strip().split("\n")]
        party_matches = [m for m in matches if m["entity_type"] == "party"]
        assert party_matches[0]["favorable_rate"] == 1.0

    def test_red_flag_detection(self, tmp_path: Path) -> None:
        paths = _setup_test_data(tmp_path)
        build_donation_matches(**paths)

        match_path = paths["output_dir"] / "donation_match.jsonl"
        matches = [json.loads(line) for line in match_path.read_text().strip().split("\n")]
        party_matches = [m for m in matches if m["entity_type"] == "party"]
        # ACME has 100% favorable (4/4), baseline is 66.7% (4 fav / 6 total for RE)
        # delta = 0.33 > 0.15 and stf_case_count=3 >= MIN_CASES, so red_flag should be True
        assert party_matches[0]["red_flag"] is True
        # Power analysis fields present with correct types
        assert "red_flag_power" in party_matches[0]
        assert "red_flag_confidence" in party_matches[0]
        assert isinstance(party_matches[0]["red_flag_power"], float)
        assert isinstance(party_matches[0]["red_flag_confidence"], str)

    def test_counsel_profile(self, tmp_path: Path) -> None:
        paths = _setup_test_data(tmp_path)
        build_donation_matches(**paths)

        counsel_path = paths["output_dir"] / "counsel_donation_profile.jsonl"
        assert counsel_path.exists()
        profiles = [json.loads(line) for line in counsel_path.read_text().strip().split("\n")]
        assert len(profiles) == 1
        assert profiles[0]["counsel_id"] == "c1"
        assert profiles[0]["donor_client_count"] == 1
        assert profiles[0]["total_client_count"] == 2

    def test_summary(self, tmp_path: Path) -> None:
        paths = _setup_test_data(tmp_path)
        build_donation_matches(**paths)

        summary_path = paths["output_dir"] / "donation_match_summary.json"
        assert summary_path.exists()
        summary = json.loads(summary_path.read_text())
        assert summary["party_match_count"] == 1
        assert summary["matched_party_count"] == 1
        assert summary["total_donated_brl_matched"] == 50000.0
        assert "matched_counsel_count" in summary

    def test_no_donations_file(self, tmp_path: Path) -> None:
        output_dir = tmp_path / "analytics"
        curated_dir = tmp_path / "curated"
        _write_jsonl(curated_dir / "process_party_link.jsonl", [])
        _write_jsonl(curated_dir / "process_counsel_link.jsonl", [])
        result = build_donation_matches(
            tse_dir=tmp_path / "empty",
            party_path=tmp_path / "party.jsonl",
            counsel_path=tmp_path / "counsel.jsonl",
            process_path=tmp_path / "process.jsonl",
            decision_event_path=tmp_path / "de.jsonl",
            process_party_link_path=curated_dir / "process_party_link.jsonl",
            process_counsel_link_path=curated_dir / "process_counsel_link.jsonl",
            output_dir=output_dir,
        )
        assert result == output_dir

    def test_no_matching_party(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"

        _write_jsonl(
            tse_dir / "donations_raw.jsonl",
            [{"donor_name_normalized": "UNKNOWN CORP", "donor_cpf_cnpj": "999", "donation_amount": 100.0}],
        )
        _write_jsonl(curated_dir / "party.jsonl", [{"party_id": "p1", "party_name_normalized": "OTHER"}])
        _write_jsonl(curated_dir / "counsel.jsonl", [])
        _write_jsonl(curated_dir / "process.jsonl", [])
        _write_jsonl(curated_dir / "decision_event.jsonl", [])
        _write_jsonl(curated_dir / "process_party_link.jsonl", [])
        _write_jsonl(curated_dir / "process_counsel_link.jsonl", [])

        build_donation_matches(
            tse_dir=tse_dir,
            party_path=curated_dir / "party.jsonl",
            counsel_path=curated_dir / "counsel.jsonl",
            process_path=curated_dir / "process.jsonl",
            decision_event_path=curated_dir / "decision_event.jsonl",
            process_party_link_path=curated_dir / "process_party_link.jsonl",
            process_counsel_link_path=curated_dir / "process_counsel_link.jsonl",
            output_dir=analytics_dir,
        )

        match_path = analytics_dir / "donation_match.jsonl"
        assert match_path.exists()
        assert match_path.read_text().strip() == ""

    def test_matches_by_alias_when_exact_name_differs(self, tmp_path: Path) -> None:
        paths = _setup_test_data(tmp_path)
        curated_dir = tmp_path / "curated"
        _write_jsonl(
            curated_dir / "party.jsonl",
            [
                {
                    "party_id": "p1",
                    "party_name_raw": "Petróleo Brasileiro S.A.",
                    "party_name_normalized": "PETRÓLEO BRASILEIRO S.A.",
                    "canonical_name_normalized": "PETRÓLEO BRASILEIRO",
                    "identity_key": "name:PETRÓLEO BRASILEIRO",
                    "identity_strategy": "name",
                    "entity_tax_id": None,
                }
            ],
        )
        _write_jsonl(
            paths["tse_dir"] / "donations_raw.jsonl",
            [
                {
                    "election_year": 2022,
                    "donor_name_normalized": "PETROBRAS",
                    "donor_cpf_cnpj": "12345678000199",
                    "donation_amount": 50000.0,
                    "party_abbrev": "PT",
                    "candidate_name": "FULANO",
                    "position": "SENADOR",
                },
            ],
        )
        alias_path = tmp_path / "entity_alias.jsonl"
        _write_jsonl(
            alias_path,
            [
                {
                    "alias_normalized": "PETROBRAS",
                    "canonical_name_normalized": "PETRÓLEO BRASILEIRO",
                    "entity_tax_id": "12345678000199",
                    "entity_kind": "party",
                    "source": "test",
                    "active": True,
                }
            ],
        )

        build_donation_matches(**paths, alias_path=alias_path)

        match_path = paths["output_dir"] / "donation_match.jsonl"
        matches = [json.loads(line) for line in match_path.read_text().strip().split("\n")]
        party_matches = [m for m in matches if m["entity_type"] == "party"]
        assert len(party_matches) == 1
        assert party_matches[0]["party_id"] == "p1"
        assert party_matches[0]["match_strategy"] == "alias"
        assert party_matches[0]["matched_alias"] == "PETROBRAS"

    def test_matches_by_tax_id_when_name_diverges(self, tmp_path: Path) -> None:
        paths = _setup_test_data(tmp_path)
        curated_dir = tmp_path / "curated"
        _write_jsonl(
            curated_dir / "party.jsonl",
            [
                {
                    "party_id": "p1",
                    "party_name_raw": "Acme Corp",
                    "party_name_normalized": "ACME CORP",
                    "canonical_name_normalized": "ACME CORP",
                    "identity_key": "tax:12345678000199",
                    "identity_strategy": "tax_id",
                    "entity_tax_id": "12345678000199",
                }
            ],
        )
        _write_jsonl(
            paths["tse_dir"] / "donations_raw.jsonl",
            [
                {
                    "election_year": 2022,
                    "donor_name_normalized": "COMPANHIA ACME HOLDING",
                    "donor_cpf_cnpj": "12345678000199",
                    "donation_amount": 50000.0,
                    "party_abbrev": "PT",
                    "candidate_name": "FULANO",
                    "position": "SENADOR",
                },
            ],
        )

        build_donation_matches(**paths)

        match_path = paths["output_dir"] / "donation_match.jsonl"
        matches = [json.loads(line) for line in match_path.read_text().strip().split("\n")]
        party_matches = [m for m in matches if m["entity_type"] == "party"]
        assert len(party_matches) == 1
        assert party_matches[0]["party_id"] == "p1"
        assert party_matches[0]["match_strategy"] == "tax_id"
        assert party_matches[0]["matched_tax_id"] == "12345678000199"

    def test_does_not_auto_match_ambiguous_similarity_candidates(self, tmp_path: Path) -> None:
        paths = _setup_test_data(tmp_path)
        curated_dir = tmp_path / "curated"
        _write_jsonl(
            curated_dir / "party.jsonl",
            [
                {"party_id": "p1", "party_name_raw": "Joao Marcos Silva", "party_name_normalized": "JOAO MARCOS SILVA"},
                {"party_id": "p2", "party_name_raw": "Joao Marcoz Silva", "party_name_normalized": "JOAO MARCOZ SILVA"},
            ],
        )
        _write_jsonl(
            paths["tse_dir"] / "donations_raw.jsonl",
            [
                {
                    "election_year": 2022,
                    "donor_name_normalized": "JOAO MARCOX SILVA",
                    "donor_cpf_cnpj": "12345678000199",
                    "donation_amount": 50000.0,
                    "party_abbrev": "PT",
                    "candidate_name": "FULANO",
                    "position": "SENADOR",
                },
            ],
        )

        build_donation_matches(**paths)

        match_path = paths["output_dir"] / "donation_match.jsonl"
        assert match_path.exists()
        content = match_path.read_text().strip()
        # May have counsel matches but no party matches
        if content:
            matches = [json.loads(line) for line in content.split("\n")]
            party_matches = [m for m in matches if m["entity_type"] == "party"]
            assert len(party_matches) == 0
        summary = json.loads((paths["output_dir"] / "donation_match_summary.json").read_text())
        assert summary["party_ambiguous_candidate_count"] == 1
        assert summary["total_ambiguous_candidate_count"] >= 1


class TestCounselDonationMatch:
    def test_counsel_direct_match(self, tmp_path: Path) -> None:
        """When a donor name matches a counsel name, a counsel match record is created."""
        tse_dir = tmp_path / "tse"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"

        _write_jsonl(
            tse_dir / "donations_raw.jsonl",
            [
                {
                    "election_year": 2022,
                    "donor_name_normalized": "ADV SILVA",
                    "donor_cpf_cnpj": "99988877766",
                    "donation_amount": 10000.0,
                },
            ],
        )
        _write_jsonl(curated_dir / "party.jsonl", [{"party_id": "p1", "party_name_normalized": "OTHER"}])
        _write_jsonl(
            curated_dir / "counsel.jsonl",
            [{"counsel_id": "c1", "counsel_name_normalized": "ADV SILVA"}],
        )
        _write_jsonl(curated_dir / "process.jsonl", [{"process_id": "proc1", "process_class": "RE"}])
        _write_jsonl(
            curated_dir / "decision_event.jsonl",
            [{"decision_event_id": "de1", "process_id": "proc1", "decision_progress": "Provido"}],
        )
        _write_jsonl(curated_dir / "process_party_link.jsonl", [])
        _write_jsonl(
            curated_dir / "process_counsel_link.jsonl",
            [{"link_id": "pcl1", "process_id": "proc1", "counsel_id": "c1"}],
        )

        build_donation_matches(
            tse_dir=tse_dir,
            party_path=curated_dir / "party.jsonl",
            counsel_path=curated_dir / "counsel.jsonl",
            process_path=curated_dir / "process.jsonl",
            decision_event_path=curated_dir / "decision_event.jsonl",
            process_party_link_path=curated_dir / "process_party_link.jsonl",
            process_counsel_link_path=curated_dir / "process_counsel_link.jsonl",
            output_dir=analytics_dir,
        )

        match_path = analytics_dir / "donation_match.jsonl"
        matches = [json.loads(line) for line in match_path.read_text().strip().split("\n") if line.strip()]
        counsel_matches = [m for m in matches if m["entity_type"] == "counsel"]
        assert len(counsel_matches) == 1
        assert counsel_matches[0]["entity_id"] == "c1"
        assert counsel_matches[0]["entity_name_normalized"] == "ADV SILVA"
        assert counsel_matches[0]["total_donated_brl"] == 10000.0
        assert counsel_matches[0]["stf_case_count"] == 1

    def test_summary_includes_counsel_counts(self, tmp_path: Path) -> None:
        """Summary should include counsel match counts."""
        paths = _setup_test_data(tmp_path)
        build_donation_matches(**paths)

        summary = json.loads((paths["output_dir"] / "donation_match_summary.json").read_text())
        assert "counsel_match_count" in summary
        assert "matched_counsel_count" in summary


class TestDonorIdentityKey:
    """P2: identity key normalization, formatting, masking, and fallback."""

    def test_same_cpf_different_formatting_same_key(self) -> None:
        """CPFs with dots/dashes must produce the same identity key."""
        assert _donor_identity_key("X", "12.345.678/0001-99") == _donor_identity_key("X", "12345678000199")
        assert _donor_identity_key("X", "123.456.789-01") == _donor_identity_key("X", "12345678901")

    def test_masked_cpf_falls_back_to_name(self) -> None:
        """Masked CPFs (***.***.***-**) contain no digits and must fallback."""
        key = _donor_identity_key("ACME CORP", "***.***.***-**")
        assert key == "name:ACME CORP"

    def test_empty_cpf_falls_back_to_name(self) -> None:
        key = _donor_identity_key("ACME CORP", "")
        assert key == "name:ACME CORP"

    def test_none_equivalent_cpf_falls_back_to_name(self) -> None:
        key = _donor_identity_key("ACME CORP", "---")
        assert key == "name:ACME CORP"

    def test_valid_cpf_produces_normalized_key(self) -> None:
        key = _donor_identity_key("IRRELEVANT", "12345678000199")
        assert key == "cpf:12345678000199"

    def test_same_cpf_different_masks_in_aggregation(self, tmp_path: Path) -> None:
        """Same CPF with different formatting must be aggregated together."""
        path = tmp_path / "donations.jsonl"
        _write_jsonl(
            path,
            [
                {"donor_name_normalized": "ACME", "donor_cpf_cnpj": "12.345.678/0001-99", "donation_amount": 100.0},
                {"donor_name_normalized": "ACME", "donor_cpf_cnpj": "12345678000199", "donation_amount": 200.0},
            ],
        )
        agg, raw_count, _ = _stream_aggregate_donations(path)
        assert raw_count == 2
        assert len(agg) == 1
        assert agg["cpf:12345678000199"]["total_donated_brl"] == 300.0

    def test_masked_cpf_aggregates_by_name(self, tmp_path: Path) -> None:
        """Masked CPFs should aggregate by name, not by mask string."""
        path = tmp_path / "donations.jsonl"
        _write_jsonl(
            path,
            [
                {"donor_name_normalized": "JOAO", "donor_cpf_cnpj": "***.***.***-**", "donation_amount": 100.0},
                {"donor_name_normalized": "JOAO", "donor_cpf_cnpj": "", "donation_amount": 200.0},
            ],
        )
        agg, raw_count, _ = _stream_aggregate_donations(path)
        assert raw_count == 2
        assert len(agg) == 1
        assert agg["name:JOAO"]["total_donated_brl"] == 300.0
