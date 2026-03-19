"""Tests for memory-safe donation match (year-partitioned matching).

Phase 4: integration tests for multi-year partitioned matching,
cross-year identity key collection, ambiguous records after reaggregation,
and recent_donation_flag with global corpus.
"""

from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics._donation_aggregator import (
    _reaggregate_matched_donors,
    _scan_donations_metadata,
    _stream_aggregate_year,
)
from atlas_stf.analytics.donation_match import build_donation_matches
from tests.analytics._donation_match_helpers import _setup_test_data, _write_jsonl


class TestScanDonationsMetadata:
    def test_basic_scan(self, tmp_path: Path) -> None:
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
                {"donor_name_normalized": "B", "donor_cpf_cnpj": "222", "donation_amount": 50.0, "election_year": 2022},
            ],
        )
        sorted_years, raw_count, unique_count = _scan_donations_metadata(path)
        assert sorted_years == [2018, 2022]
        assert raw_count == 3
        assert unique_count == 2

    def test_empty_file(self, tmp_path: Path) -> None:
        path = tmp_path / "empty.jsonl"
        path.write_text("")
        sorted_years, raw_count, unique_count = _scan_donations_metadata(path)
        assert sorted_years == []
        assert raw_count == 0
        assert unique_count == 0

    def test_donors_without_cpf_counted_by_name(self, tmp_path: Path) -> None:
        path = tmp_path / "donations.jsonl"
        _write_jsonl(
            path,
            [
                {
                    "donor_name_normalized": "ACME",
                    "donor_cpf_cnpj": "",
                    "donation_amount": 100.0,
                    "election_year": 2022,
                },
                {"donor_name_normalized": "ACME", "donor_cpf_cnpj": "", "donation_amount": 50.0, "election_year": 2022},
            ],
        )
        _, _, unique_count = _scan_donations_metadata(path)
        assert unique_count == 1

    def test_masked_cpf_identity_key(self, tmp_path: Path) -> None:
        """Masked CPF normalizes to partial digits, not name fallback."""
        path = tmp_path / "donations.jsonl"
        _write_jsonl(
            path,
            [
                {
                    "donor_name_normalized": "X",
                    "donor_cpf_cnpj": "***.222.333-**",
                    "donation_amount": 1.0,
                    "election_year": 2022,
                },
                {
                    "donor_name_normalized": "X",
                    "donor_cpf_cnpj": "11122233344",
                    "donation_amount": 1.0,
                    "election_year": 2022,
                },
            ],
        )
        _, _, unique_count = _scan_donations_metadata(path)
        # cpf:222333 and cpf:11122233344 are different identity keys
        assert unique_count == 2


class TestStreamAggregateYear:
    def test_filters_by_year(self, tmp_path: Path) -> None:
        path = tmp_path / "donations.jsonl"
        _write_jsonl(
            path,
            [
                {
                    "donor_name_normalized": "A",
                    "donor_cpf_cnpj": "111",
                    "donation_amount": 100.0,
                    "election_year": 2018,
                },
                {
                    "donor_name_normalized": "B",
                    "donor_cpf_cnpj": "222",
                    "donation_amount": 200.0,
                    "election_year": 2022,
                },
                {
                    "donor_name_normalized": "C",
                    "donor_cpf_cnpj": "333",
                    "donation_amount": 300.0,
                    "election_year": 2018,
                },
            ],
        )
        result = _stream_aggregate_year(path, 2018)
        assert len(result) == 2
        assert "cpf:111" in result
        assert "cpf:333" in result
        assert "cpf:222" not in result

    def test_returns_name_and_cpf(self, tmp_path: Path) -> None:
        path = tmp_path / "donations.jsonl"
        _write_jsonl(
            path,
            [
                {
                    "donor_name_normalized": "ACME",
                    "donor_cpf_cnpj": "12345",
                    "donation_amount": 100.0,
                    "election_year": 2022,
                }
            ],
        )
        result = _stream_aggregate_year(path, 2022)
        assert result["cpf:12345"] == ("ACME", "12345")

    def test_no_matching_year(self, tmp_path: Path) -> None:
        path = tmp_path / "donations.jsonl"
        _write_jsonl(
            path,
            [{"donor_name_normalized": "A", "donor_cpf_cnpj": "111", "donation_amount": 100.0, "election_year": 2018}],
        )
        result = _stream_aggregate_year(path, 2022)
        assert result == {}

    def test_deduplicates_same_donor_within_year(self, tmp_path: Path) -> None:
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
                    "election_year": 2022,
                },
            ],
        )
        result = _stream_aggregate_year(path, 2022)
        assert len(result) == 1


class TestReaggregatMatchedDonors:
    def test_filters_by_matched_keys(self, tmp_path: Path) -> None:
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
                    "donor_name_normalized": "B",
                    "donor_cpf_cnpj": "222",
                    "donation_amount": 200.0,
                    "election_year": 2022,
                },
            ],
        )
        result = _reaggregate_matched_donors(path, {"cpf:111"}, [2022])
        assert len(result) == 1
        assert "cpf:111" in result
        assert result["cpf:111"]["total_donated_brl"] == 100.0

    def test_aggregates_across_years(self, tmp_path: Path) -> None:
        path = tmp_path / "donations.jsonl"
        _write_jsonl(
            path,
            [
                {
                    "donor_name_normalized": "A",
                    "donor_cpf_cnpj": "111",
                    "donation_amount": 100.0,
                    "election_year": 2018,
                },
                {
                    "donor_name_normalized": "A",
                    "donor_cpf_cnpj": "111",
                    "donation_amount": 200.0,
                    "election_year": 2022,
                },
            ],
        )
        result = _reaggregate_matched_donors(path, {"cpf:111"}, [2018, 2022])
        entry = result["cpf:111"]
        assert entry["total_donated_brl"] == 300.0
        assert entry["donation_count"] == 2
        assert entry["election_years"] == [2018, 2022]

    def test_recent_flag_uses_caller_cycles(self, tmp_path: Path) -> None:
        path = tmp_path / "donations.jsonl"
        _write_jsonl(
            path,
            [
                {
                    "donor_name_normalized": "OLD",
                    "donor_cpf_cnpj": "111",
                    "donation_amount": 100.0,
                    "election_year": 2014,
                },
                {
                    "donor_name_normalized": "NEW",
                    "donor_cpf_cnpj": "222",
                    "donation_amount": 100.0,
                    "election_year": 2022,
                },
            ],
        )
        result = _reaggregate_matched_donors(path, {"cpf:111", "cpf:222"}, [2018, 2022])
        assert result["cpf:111"]["recent_donation_flag"] is False
        assert result["cpf:222"]["recent_donation_flag"] is True

    def test_empty_matched_keys(self, tmp_path: Path) -> None:
        path = tmp_path / "donations.jsonl"
        _write_jsonl(
            path,
            [{"donor_name_normalized": "A", "donor_cpf_cnpj": "111", "donation_amount": 100.0, "election_year": 2022}],
        )
        result = _reaggregate_matched_donors(path, set(), [2022])
        assert result == {}


class TestMultiYearMatching:
    def test_cross_year_donor_aggregates_correctly(self, tmp_path: Path) -> None:
        """Donor donating in 2018 and 2022 should have both years in match output."""
        paths = _setup_test_data(tmp_path)
        tse_dir = paths["tse_dir"]
        _write_jsonl(
            tse_dir / "donations_raw.jsonl",
            [
                {
                    "election_year": 2018,
                    "donor_name_normalized": "ACME CORP",
                    "donor_cpf_cnpj": "12345678000199",
                    "donation_amount": 30000.0,
                    "party_abbrev": "PT",
                    "candidate_name": "FULANO",
                    "position": "SENADOR",
                },
                {
                    "election_year": 2022,
                    "donor_name_normalized": "ACME CORP",
                    "donor_cpf_cnpj": "12345678000199",
                    "donation_amount": 50000.0,
                    "party_abbrev": "MDB",
                    "candidate_name": "CICLANO",
                    "position": "DEPUTADO",
                },
            ],
        )

        build_donation_matches(**paths)

        match_path = paths["output_dir"] / "donation_match.jsonl"
        matches = [json.loads(line) for line in match_path.read_text().strip().split("\n")]
        party_matches = [m for m in matches if m["entity_type"] == "party"]
        assert len(party_matches) == 1
        assert party_matches[0]["total_donated_brl"] == 80000.0
        assert party_matches[0]["donation_count"] == 2
        assert set(party_matches[0]["election_years"]) == {2018, 2022}
        assert set(party_matches[0]["parties_donated_to"]) == {"MDB", "PT"}

    def test_cross_year_identity_key_collection(self, tmp_path: Path) -> None:
        """Two homonyms with different CPFs in different years should both appear."""
        tse_dir = tmp_path / "tse"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"

        _write_jsonl(
            tse_dir / "donations_raw.jsonl",
            [
                {
                    "election_year": 2018,
                    "donor_name_normalized": "JOAO SILVA",
                    "donor_cpf_cnpj": "11111111111",
                    "donation_amount": 10000.0,
                },
                {
                    "election_year": 2022,
                    "donor_name_normalized": "JOAO SILVA",
                    "donor_cpf_cnpj": "22222222222",
                    "donation_amount": 20000.0,
                },
            ],
        )
        _write_jsonl(
            curated_dir / "party.jsonl",
            [{"party_id": "p1", "party_name_normalized": "JOAO SILVA"}],
        )
        _write_jsonl(curated_dir / "counsel.jsonl", [])
        _write_jsonl(
            curated_dir / "process.jsonl",
            [{"process_id": "proc1", "process_class": "RE"}],
        )
        _write_jsonl(
            curated_dir / "decision_event.jsonl",
            [{"decision_event_id": "de1", "process_id": "proc1", "decision_progress": "Provido"}],
        )
        _write_jsonl(
            curated_dir / "process_party_link.jsonl",
            [{"link_id": "ppl1", "process_id": "proc1", "party_id": "p1"}],
        )
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
        matches = [json.loads(line) for line in match_path.read_text().strip().split("\n") if line.strip()]
        party_matches = [m for m in matches if m["entity_type"] == "party"]
        # Both identity keys should produce matches (same name, different CPFs)
        identity_keys = {m["donor_identity_key"] for m in party_matches}
        assert "cpf:11111111111" in identity_keys
        assert "cpf:22222222222" in identity_keys
        assert len(party_matches) == 2

    def test_ambiguous_records_have_full_donor_info(self, tmp_path: Path) -> None:
        """Ambiguous records built after reaggregation have complete fields."""
        tse_dir = tmp_path / "tse"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"

        _write_jsonl(
            tse_dir / "donations_raw.jsonl",
            [
                {
                    "election_year": 2022,
                    "donor_name_normalized": "JOAO MARCOX SILVA",
                    "donor_cpf_cnpj": "12345678000199",
                    "donation_amount": 50000.0,
                },
            ],
        )
        _write_jsonl(
            curated_dir / "party.jsonl",
            [
                {"party_id": "p1", "party_name_normalized": "JOAO MARCOS SILVA"},
                {"party_id": "p2", "party_name_normalized": "JOAO MARCOZ SILVA"},
            ],
        )
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

        ambiguous_path = analytics_dir / "donation_match_ambiguous.jsonl"
        assert ambiguous_path.exists()
        records = [json.loads(line) for line in ambiguous_path.read_text().strip().split("\n") if line.strip()]
        assert len(records) >= 1
        rec = records[0]
        assert rec["total_donated_brl"] == 50000.0
        assert rec["donation_count"] == 1
        assert rec["election_years"] == [2022]

    def test_recent_donation_flag_with_year_partition(self, tmp_path: Path) -> None:
        """recent_donation_flag uses the 2 most recent years of the GLOBAL corpus."""
        tse_dir = tmp_path / "tse"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"

        _write_jsonl(
            tse_dir / "donations_raw.jsonl",
            [
                {
                    "election_year": 2014,
                    "donor_name_normalized": "OLD DONOR",
                    "donor_cpf_cnpj": "11111111111",
                    "donation_amount": 10000.0,
                },
                {
                    "election_year": 2018,
                    "donor_name_normalized": "RECENT DONOR",
                    "donor_cpf_cnpj": "22222222222",
                    "donation_amount": 20000.0,
                },
                {
                    "election_year": 2022,
                    "donor_name_normalized": "ALSO RECENT",
                    "donor_cpf_cnpj": "33333333333",
                    "donation_amount": 30000.0,
                },
            ],
        )
        _write_jsonl(
            curated_dir / "party.jsonl",
            [
                {"party_id": "p1", "party_name_normalized": "OLD DONOR"},
                {"party_id": "p2", "party_name_normalized": "RECENT DONOR"},
                {"party_id": "p3", "party_name_normalized": "ALSO RECENT"},
            ],
        )
        _write_jsonl(curated_dir / "counsel.jsonl", [])
        _write_jsonl(curated_dir / "process.jsonl", [{"process_id": "proc1", "process_class": "RE"}])
        _write_jsonl(
            curated_dir / "decision_event.jsonl",
            [{"decision_event_id": "de1", "process_id": "proc1", "decision_progress": "Provido"}],
        )
        _write_jsonl(
            curated_dir / "process_party_link.jsonl",
            [
                {"link_id": "ppl1", "process_id": "proc1", "party_id": "p1"},
                {"link_id": "ppl2", "process_id": "proc1", "party_id": "p2"},
                {"link_id": "ppl3", "process_id": "proc1", "party_id": "p3"},
            ],
        )
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
        matches = [json.loads(line) for line in match_path.read_text().strip().split("\n") if line.strip()]
        party_matches = {m["donor_name_normalized"]: m for m in matches if m["entity_type"] == "party"}

        # Global recent cycles = [2018, 2022]
        assert party_matches["OLD DONOR"]["recent_donation_flag"] is False
        assert party_matches["RECENT DONOR"]["recent_donation_flag"] is True
        assert party_matches["ALSO RECENT"]["recent_donation_flag"] is True

    def test_summary_unique_donors_from_scan(self, tmp_path: Path) -> None:
        """unique_donors in summary reflects total corpus, not just matched donors."""
        paths = _setup_test_data(tmp_path)
        tse_dir = paths["tse_dir"]
        _write_jsonl(
            tse_dir / "donations_raw.jsonl",
            [
                {
                    "election_year": 2022,
                    "donor_name_normalized": "ACME CORP",
                    "donor_cpf_cnpj": "12345678000199",
                    "donation_amount": 50000.0,
                },
                {
                    "election_year": 2022,
                    "donor_name_normalized": "UNKNOWN CORP",
                    "donor_cpf_cnpj": "99999999999",
                    "donation_amount": 10000.0,
                },
            ],
        )

        build_donation_matches(**paths)

        summary = json.loads((paths["output_dir"] / "donation_match_summary.json").read_text())
        assert summary["unique_donors"] == 2
        assert summary["total_donations_raw"] == 2
