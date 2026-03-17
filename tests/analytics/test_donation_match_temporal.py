"""Tests for donation_match: temporal and concentration metrics."""

from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics.donation_match import (
    _stream_aggregate_donations,
    build_donation_matches,
)
from tests.analytics._donation_match_helpers import _setup_test_data, _write_jsonl


class TestTemporalConcentrationMetrics:
    """Tests for temporal and concentration metrics in donation_match."""

    def test_temporal_metrics_computed(self, tmp_path: Path) -> None:
        """Donations with varied dates, candidates, UFs → verify all derived fields."""
        path = tmp_path / "donations.jsonl"
        _write_jsonl(
            path,
            [
                {
                    "donor_name_normalized": "A",
                    "donor_cpf_cnpj": "111",
                    "donation_amount": 100.0,
                    "election_year": 2018,
                    "donation_date": "2018-09-15",
                    "candidate_name": "C1",
                    "party_abbrev": "PT",
                    "state": "SP",
                },
                {
                    "donor_name_normalized": "A",
                    "donor_cpf_cnpj": "111",
                    "donation_amount": 300.0,
                    "election_year": 2022,
                    "donation_date": "2022-08-10",
                    "candidate_name": "C2",
                    "party_abbrev": "MDB",
                    "state": "RJ",
                },
                {
                    "donor_name_normalized": "A",
                    "donor_cpf_cnpj": "111",
                    "donation_amount": 100.0,
                    "election_year": 2022,
                    "donation_date": "2022-10-01",
                    "candidate_name": "C1",
                    "party_abbrev": "PT",
                    "state": "SP",
                },
            ],
        )
        agg, _, _ = _stream_aggregate_donations(path)
        entry = list(agg.values())[0]
        assert entry["first_donation_date"] == "2018-09-15"
        assert entry["last_donation_date"] == "2022-10-01"
        assert entry["active_election_year_count"] == 2
        assert entry["max_single_donation_brl"] == 300.0
        assert entry["avg_donation_brl"] == round(500.0 / 3, 2)
        # C1 received 200, C2 received 300 → top_candidate_share = 300/500 = 0.6
        assert entry["top_candidate_share"] == 0.6
        # MDB received 300, PT received 200 → top_party_share = 300/500 = 0.6
        assert entry["top_party_share"] == 0.6
        # SP received 200, RJ received 300 → top_state_share = 300/500 = 0.6
        assert entry["top_state_share"] == 0.6
        # Year span: 2022 - 2018 + 1 = 5
        assert entry["donation_year_span"] == 5

    def test_recent_flag_true(self, tmp_path: Path) -> None:
        """Donor active in the 2 most recent cycles → flag True."""
        path = tmp_path / "donations.jsonl"
        _write_jsonl(
            path,
            [
                {"donor_name_normalized": "A", "donor_cpf_cnpj": "1", "donation_amount": 1.0, "election_year": 2014},
                {"donor_name_normalized": "A", "donor_cpf_cnpj": "1", "donation_amount": 1.0, "election_year": 2018},
                {"donor_name_normalized": "A", "donor_cpf_cnpj": "1", "donation_amount": 1.0, "election_year": 2022},
            ],
        )
        agg, _, _ = _stream_aggregate_donations(path)
        entry = list(agg.values())[0]
        # Recent cycles = [2018, 2022]; donor has both → True
        assert entry["recent_donation_flag"] is True

    def test_recent_flag_false(self, tmp_path: Path) -> None:
        """Donor only in old cycles → flag False."""
        path = tmp_path / "donations.jsonl"
        _write_jsonl(
            path,
            [
                {"donor_name_normalized": "OLD", "donor_cpf_cnpj": "2", "donation_amount": 1.0, "election_year": 2010},
                {"donor_name_normalized": "OLD", "donor_cpf_cnpj": "2", "donation_amount": 1.0, "election_year": 2014},
                {
                    "donor_name_normalized": "NEW",
                    "donor_cpf_cnpj": "3",
                    "donation_amount": 1.0,
                    "election_year": 2018,
                },
                {
                    "donor_name_normalized": "NEW",
                    "donor_cpf_cnpj": "3",
                    "donation_amount": 1.0,
                    "election_year": 2022,
                },
            ],
        )
        agg, _, _ = _stream_aggregate_donations(path)
        # Recent cycles = [2018, 2022] — OLD has neither
        old = next(v for v in agg.values() if v["donor_name_normalized"] == "OLD")
        assert old["recent_donation_flag"] is False
        new = next(v for v in agg.values() if v["donor_name_normalized"] == "NEW")
        assert new["recent_donation_flag"] is True

    def test_recent_flag_single_year_corpus(self, tmp_path: Path) -> None:
        """Corpus with 1 year → flag True if donor participated."""
        path = tmp_path / "donations.jsonl"
        _write_jsonl(
            path,
            [
                {"donor_name_normalized": "A", "donor_cpf_cnpj": "1", "donation_amount": 1.0, "election_year": 2022},
            ],
        )
        agg, _, _ = _stream_aggregate_donations(path)
        entry = list(agg.values())[0]
        assert entry["recent_donation_flag"] is True

    def test_metrics_empty_dates(self, tmp_path: Path) -> None:
        """Donations without donation_date → first/last = None, other metrics ok."""
        path = tmp_path / "donations.jsonl"
        _write_jsonl(
            path,
            [
                {
                    "donor_name_normalized": "A",
                    "donor_cpf_cnpj": "1",
                    "donation_amount": 100.0,
                    "election_year": 2022,
                },
                {
                    "donor_name_normalized": "A",
                    "donor_cpf_cnpj": "1",
                    "donation_amount": 200.0,
                    "election_year": 2022,
                },
            ],
        )
        agg, _, _ = _stream_aggregate_donations(path)
        entry = list(agg.values())[0]
        assert entry["first_donation_date"] is None
        assert entry["last_donation_date"] is None
        assert entry["max_single_donation_brl"] == 200.0
        assert entry["avg_donation_brl"] == 150.0

    def test_metrics_malformed_dates(self, tmp_path: Path) -> None:
        """Malformed dates are silently ignored."""
        path = tmp_path / "donations.jsonl"
        _write_jsonl(
            path,
            [
                {
                    "donor_name_normalized": "A",
                    "donor_cpf_cnpj": "1",
                    "donation_amount": 50.0,
                    "donation_date": "2022-13-XX",
                },
                {
                    "donor_name_normalized": "A",
                    "donor_cpf_cnpj": "1",
                    "donation_amount": 50.0,
                    "donation_date": "abc",
                },
            ],
        )
        agg, _, _ = _stream_aggregate_donations(path)
        entry = list(agg.values())[0]
        assert entry["first_donation_date"] is None
        assert entry["last_donation_date"] is None

    def test_single_donation_concentration(self, tmp_path: Path) -> None:
        """1 donation with all fields → top_*_share = 1.0."""
        path = tmp_path / "donations.jsonl"
        _write_jsonl(
            path,
            [
                {
                    "donor_name_normalized": "SOLO",
                    "donor_cpf_cnpj": "99",
                    "donation_amount": 500.0,
                    "candidate_name": "CAND",
                    "party_abbrev": "PT",
                    "state": "SP",
                    "election_year": 2022,
                    "donation_date": "2022-09-01",
                },
            ],
        )
        agg, _, _ = _stream_aggregate_donations(path)
        entry = list(agg.values())[0]
        assert entry["top_candidate_share"] == 1.0
        assert entry["top_party_share"] == 1.0
        assert entry["top_state_share"] == 1.0
        assert entry["donation_year_span"] == 1

    def test_partial_dimensions(self, tmp_path: Path) -> None:
        """Candidate filled, party empty, UF empty → only candidate share computed."""
        path = tmp_path / "donations.jsonl"
        _write_jsonl(
            path,
            [
                {
                    "donor_name_normalized": "PARTIAL",
                    "donor_cpf_cnpj": "88",
                    "donation_amount": 100.0,
                    "candidate_name": "C1",
                    "party_abbrev": "",
                    "state": "",
                    "election_year": 2022,
                },
            ],
        )
        agg, _, _ = _stream_aggregate_donations(path)
        entry = list(agg.values())[0]
        assert entry["top_candidate_share"] == 1.0
        assert entry["top_party_share"] is None
        assert entry["top_state_share"] is None

    def test_metrics_in_match_output(self, tmp_path: Path) -> None:
        """Pipeline end-to-end: temporal fields present in donation_match.jsonl."""
        paths = _setup_test_data(tmp_path)
        # Add donation_date to the raw data
        tse_dir = paths["tse_dir"]
        _write_jsonl(
            tse_dir / "donations_raw.jsonl",
            [
                {
                    "election_year": 2022,
                    "state": "SP",
                    "position": "SENADOR",
                    "candidate_name": "FULANO",
                    "party_abbrev": "PT",
                    "donor_name": "ACME CORP",
                    "donor_cpf_cnpj": "12345678000199",
                    "donor_name_normalized": "ACME CORP",
                    "donation_amount": 50000.0,
                    "donation_description": "Doacao em dinheiro",
                    "donation_date": "2022-09-15",
                },
            ],
        )
        build_donation_matches(**paths)
        matches = [json.loads(line) for line in (paths["output_dir"] / "donation_match.jsonl").read_text().splitlines()]
        party_match = next(m for m in matches if m["entity_type"] == "party")
        assert party_match["first_donation_date"] == "2022-09-15"
        assert party_match["last_donation_date"] == "2022-09-15"
        assert party_match["active_election_year_count"] == 1
        assert party_match["max_single_donation_brl"] == 50000.0
        assert party_match["avg_donation_brl"] == 50000.0
        assert party_match["top_candidate_share"] == 1.0
        assert party_match["top_party_share"] == 1.0
        assert party_match["top_state_share"] == 1.0
        assert party_match["donation_year_span"] == 1
        assert party_match["recent_donation_flag"] is True
