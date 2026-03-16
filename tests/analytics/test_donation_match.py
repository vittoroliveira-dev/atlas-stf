"""Tests for analytics/donation_match.py."""

from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics._corporate_enrichment import (
    build_corporate_enrichment_index,
    enrich_match_corporate,
)
from atlas_stf.analytics.donation_match import (
    _donor_identity_key,
    _stream_aggregate_donations,
    build_donation_matches,
)


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


def _setup_test_data(tmp_path: Path) -> dict[str, Path]:
    tse_dir = tmp_path / "tse"
    curated_dir = tmp_path / "curated"
    analytics_dir = tmp_path / "analytics"

    _write_jsonl(
        tse_dir / "donations_raw.jsonl",
        [
            {
                "election_year": 2022,
                "state": "SP",
                "position": "SENADOR",
                "candidate_name": "FULANO",
                "candidate_cpf": "12345678901",
                "candidate_number": "123",
                "party_abbrev": "PT",
                "party_name": "PARTIDO DOS TRABALHADORES",
                "donor_name": "ACME CORP",
                "donor_name_rfb": "ACME CORP",
                "donor_cpf_cnpj": "12345678000199",
                "donor_name_normalized": "ACME CORP",
                "donation_amount": 50000.0,
                "donation_description": "Doacao em dinheiro",
                "donor_cnae_code": "4110700",
                "donor_cnae_description": "Incorporacao",
                "donor_state": "SP",
            },
        ],
    )

    _write_jsonl(
        curated_dir / "party.jsonl",
        [
            {"party_id": "p1", "party_name_raw": "ACME Corp", "party_name_normalized": "ACME CORP"},
            {"party_id": "p2", "party_name_raw": "Clean Co", "party_name_normalized": "CLEAN CO"},
        ],
    )

    _write_jsonl(
        curated_dir / "counsel.jsonl",
        [
            {"counsel_id": "c1", "counsel_name_raw": "Adv Silva", "counsel_name_normalized": "ADV SILVA"},
        ],
    )

    _write_jsonl(
        curated_dir / "process.jsonl",
        [
            {"process_id": "proc1", "process_class": "RE"},
            {"process_id": "proc2", "process_class": "RE"},
            {"process_id": "proc3", "process_class": "RE"},
            {"process_id": "proc4", "process_class": "RE"},
        ],
    )

    # ACME (p1) has 3 processes, CLEAN (p2) has 2 — ensures min_cases >= 3 for red_flag
    _write_jsonl(
        curated_dir / "process_party_link.jsonl",
        [
            {"link_id": "ppl1", "process_id": "proc1", "party_id": "p1"},
            {"link_id": "ppl3", "process_id": "proc3", "party_id": "p1"},
            {"link_id": "ppl4", "process_id": "proc4", "party_id": "p1"},
            {"link_id": "ppl2", "process_id": "proc2", "party_id": "p2"},
        ],
    )

    _write_jsonl(
        curated_dir / "process_counsel_link.jsonl",
        [
            {"link_id": "pcl1", "process_id": "proc1", "counsel_id": "c1"},
            {"link_id": "pcl2", "process_id": "proc2", "counsel_id": "c1"},
            {"link_id": "pcl3", "process_id": "proc3", "counsel_id": "c1"},
            {"link_id": "pcl4", "process_id": "proc4", "counsel_id": "c1"},
        ],
    )

    _write_jsonl(
        curated_dir / "decision_event.jsonl",
        [
            {"decision_event_id": "de1", "process_id": "proc1", "decision_progress": "Provido"},
            {"decision_event_id": "de2", "process_id": "proc1", "decision_progress": "Provido"},
            {"decision_event_id": "de3", "process_id": "proc2", "decision_progress": "Desprovido"},
            {"decision_event_id": "de4", "process_id": "proc2", "decision_progress": "Desprovido"},
            {"decision_event_id": "de5", "process_id": "proc3", "decision_progress": "Provido"},
            {"decision_event_id": "de6", "process_id": "proc4", "decision_progress": "Provido"},
        ],
    )

    return {
        "tse_dir": tse_dir,
        "party_path": curated_dir / "party.jsonl",
        "counsel_path": curated_dir / "counsel.jsonl",
        "process_path": curated_dir / "process.jsonl",
        "decision_event_path": curated_dir / "decision_event.jsonl",
        "process_party_link_path": curated_dir / "process_party_link.jsonl",
        "process_counsel_link_path": curated_dir / "process_counsel_link.jsonl",
        "output_dir": analytics_dir,
    }


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


class TestDonationEvents:
    """P3: individual donation events should be written for matched donors."""

    def test_events_written_for_matched_donors(self, tmp_path: Path) -> None:
        paths = _setup_test_data(tmp_path)
        build_donation_matches(**paths)

        event_path = paths["output_dir"] / "donation_event.jsonl"
        assert event_path.exists()
        events = [json.loads(line) for line in event_path.read_text().strip().split("\n") if line.strip()]
        assert len(events) >= 1
        assert events[0]["event_id"]
        assert events[0]["match_id"]
        assert events[0]["donor_cpf_cnpj"] == "12345678000199"

    def test_events_include_date_field(self, tmp_path: Path) -> None:
        paths = _setup_test_data(tmp_path)
        # Add a donation with a date
        _write_jsonl(
            paths["tse_dir"] / "donations_raw.jsonl",
            [
                {
                    "election_year": 2022,
                    "donor_name_normalized": "ACME CORP",
                    "donor_cpf_cnpj": "12345678000199",
                    "donation_amount": 50000.0,
                    "donation_date": "2022-06-15",
                    "party_abbrev": "PT",
                    "candidate_name": "FULANO",
                    "position": "SENADOR",
                },
            ],
        )
        build_donation_matches(**paths)

        event_path = paths["output_dir"] / "donation_event.jsonl"
        events = [json.loads(line) for line in event_path.read_text().strip().split("\n") if line.strip()]
        assert len(events) == 1
        assert events[0]["donation_date"] == "2022-06-15"

    def test_summary_includes_event_count(self, tmp_path: Path) -> None:
        paths = _setup_test_data(tmp_path)
        build_donation_matches(**paths)

        summary = json.loads((paths["output_dir"] / "donation_match_summary.json").read_text())
        assert "donation_event_count" in summary
        assert summary["donation_event_count"] >= 1


class TestDonationEventResourceClassification:
    """Resource type classification fields should propagate to donation_event.jsonl."""

    def test_events_include_classification_fields(self, tmp_path: Path) -> None:
        paths = _setup_test_data(tmp_path)
        build_donation_matches(**paths)

        event_path = paths["output_dir"] / "donation_event.jsonl"
        events = [json.loads(line) for line in event_path.read_text().strip().split("\n") if line.strip()]
        assert len(events) >= 1
        e = events[0]
        assert "resource_type_category" in e
        assert "resource_type_subtype" in e
        assert "resource_classification_confidence" in e
        assert "resource_classification_rule" in e

    def test_known_description_classified(self, tmp_path: Path) -> None:
        paths = _setup_test_data(tmp_path)
        _write_jsonl(
            paths["tse_dir"] / "donations_raw.jsonl",
            [
                {
                    "election_year": 2022,
                    "donor_name_normalized": "ACME CORP",
                    "donor_cpf_cnpj": "12345678000199",
                    "donation_amount": 50000.0,
                    "donation_description": "Recursos de Pessoas Físicas",
                    "party_abbrev": "PT",
                    "candidate_name": "FULANO",
                    "position": "SENADOR",
                },
            ],
        )
        build_donation_matches(**paths)

        event_path = paths["output_dir"] / "donation_event.jsonl"
        events = [json.loads(line) for line in event_path.read_text().strip().split("\n") if line.strip()]
        assert len(events) == 1
        assert events[0]["resource_type_category"] == "source_type"
        assert events[0]["resource_type_subtype"] == "individual"
        assert events[0]["resource_classification_confidence"] == "high"

    def test_match_includes_resource_types_observed(self, tmp_path: Path) -> None:
        paths = _setup_test_data(tmp_path)
        build_donation_matches(**paths)

        match_path = paths["output_dir"] / "donation_match.jsonl"
        matches = [json.loads(line) for line in match_path.read_text().strip().split("\n") if line.strip()]
        party_matches = [m for m in matches if m["entity_type"] == "party"]
        assert len(party_matches) >= 1
        assert "resource_types_observed" in party_matches[0]
        assert isinstance(party_matches[0]["resource_types_observed"], list)

    def test_summary_includes_coverage_metrics(self, tmp_path: Path) -> None:
        paths = _setup_test_data(tmp_path)
        build_donation_matches(**paths)

        summary = json.loads((paths["output_dir"] / "donation_match_summary.json").read_text())
        assert "resource_category_counts" in summary
        assert "resource_subtype_counts" in summary
        assert "resource_classification_coverage_rate" in summary
        assert "resource_classification_nonempty_coverage_rate" in summary
        assert isinstance(summary["resource_classification_coverage_rate"], float)


class TestDonationEventProvenance:
    """Provenance fields should propagate from raw to donation_event.jsonl."""

    def test_provenance_propagated_to_events(self, tmp_path: Path) -> None:
        paths = _setup_test_data(tmp_path)
        # Add provenance to raw record
        _write_jsonl(
            paths["tse_dir"] / "donations_raw.jsonl",
            [
                {
                    "election_year": 2022,
                    "donor_name_normalized": "ACME CORP",
                    "donor_cpf_cnpj": "12345678000199",
                    "donation_amount": 50000.0,
                    "party_abbrev": "PT",
                    "candidate_name": "FULANO",
                    "position": "SENADOR",
                    "source_file": "candidato/SP/ReceitasCandidatos.txt",
                    "source_url": "https://cdn.tse.jus.br/file.zip",
                    "collected_at": "2026-03-15T12:00:00+00:00",
                    "ingest_run_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                    "record_hash": "ab" * 32,
                },
            ],
        )
        build_donation_matches(**paths)

        event_path = paths["output_dir"] / "donation_event.jsonl"
        events = [json.loads(line) for line in event_path.read_text().strip().split("\n") if line.strip()]
        assert len(events) == 1
        e = events[0]
        assert e["source_file"] == "candidato/SP/ReceitasCandidatos.txt"
        assert e["source_url"] == "https://cdn.tse.jus.br/file.zip"
        assert e["collected_at"] == "2026-03-15T12:00:00+00:00"
        assert e["ingest_run_id"] == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        assert e["record_hash"] == "ab" * 32

    def test_missing_provenance_defaults_to_empty(self, tmp_path: Path) -> None:
        """Raw records without provenance should produce events with empty fallback."""
        paths = _setup_test_data(tmp_path)
        build_donation_matches(**paths)

        event_path = paths["output_dir"] / "donation_event.jsonl"
        events = [json.loads(line) for line in event_path.read_text().strip().split("\n") if line.strip()]
        assert len(events) >= 1
        e = events[0]
        assert e["source_file"] == ""
        assert e["record_hash"] == ""


class TestDonorOriginatorInMatch:
    """P1: originator info should be preserved through the match pipeline."""

    def test_match_includes_originator(self, tmp_path: Path) -> None:
        paths = _setup_test_data(tmp_path)
        _write_jsonl(
            paths["tse_dir"] / "donations_raw.jsonl",
            [
                {
                    "election_year": 2022,
                    "donor_name_normalized": "ACME CORP",
                    "donor_cpf_cnpj": "12345678000199",
                    "donor_name_originator_normalized": "REAL CORP",
                    "donation_amount": 50000.0,
                    "party_abbrev": "PT",
                    "candidate_name": "FULANO",
                    "position": "SENADOR",
                },
            ],
        )
        build_donation_matches(**paths)

        match_path = paths["output_dir"] / "donation_match.jsonl"
        matches = [json.loads(line) for line in match_path.read_text().strip().split("\n") if line.strip()]
        party_matches = [m for m in matches if m["entity_type"] == "party"]
        assert len(party_matches) == 1
        assert party_matches[0]["donor_name_originator"] == "REAL CORP"
        assert party_matches[0]["donor_name_normalized"] == "ACME CORP"

    def test_match_includes_identity_key(self, tmp_path: Path) -> None:
        paths = _setup_test_data(tmp_path)
        build_donation_matches(**paths)

        match_path = paths["output_dir"] / "donation_match.jsonl"
        matches = [json.loads(line) for line in match_path.read_text().strip().split("\n") if line.strip()]
        party_matches = [m for m in matches if m["entity_type"] == "party"]
        assert len(party_matches) == 1
        assert party_matches[0]["donor_identity_key"] == "cpf:12345678000199"


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


class TestMatchIdStability:
    """P3: match_id must be deterministic across rebuilds with same code."""

    def test_match_id_deterministic_on_rebuild(self, tmp_path: Path) -> None:
        paths = _setup_test_data(tmp_path)
        build_donation_matches(**paths)
        match_path = paths["output_dir"] / "donation_match.jsonl"
        first_ids = [
            json.loads(line)["match_id"] for line in match_path.read_text().strip().split("\n") if line.strip()
        ]

        # Rebuild
        build_donation_matches(**paths)
        second_ids = [
            json.loads(line)["match_id"] for line in match_path.read_text().strip().split("\n") if line.strip()
        ]

        assert first_ids == second_ids


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


class TestAmbiguousTrail:
    """Tests for donation_match_ambiguous.jsonl output."""

    def test_ambiguous_trail_written(self, tmp_path: Path) -> None:
        """Two parties with very similar names → ambiguous match → trail written."""
        tse_dir = tmp_path / "tse"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"

        # Donor name is similar to both parties → Jaccard ambiguity
        _write_jsonl(
            tse_dir / "donations_raw.jsonl",
            [
                {
                    "election_year": 2022,
                    "donor_name_normalized": "EMPRESA CONSTRUTORA ALPHA BETA",
                    "donor_cpf_cnpj": "",
                    "donation_amount": 10000.0,
                },
            ],
        )

        # Two parties with same Jaccard score to the donor name
        _write_jsonl(
            curated_dir / "party.jsonl",
            [
                {"party_id": "p1", "party_name_normalized": "EMPRESA CONSTRUTORA ALPHA BETA LTDA"},
                {"party_id": "p2", "party_name_normalized": "EMPRESA CONSTRUTORA ALPHA BETA SA"},
            ],
        )

        _write_jsonl(curated_dir / "counsel.jsonl", [])
        _write_jsonl(curated_dir / "process.jsonl", [{"process_id": "proc1", "process_class": "RE"}])
        _write_jsonl(curated_dir / "process_party_link.jsonl", [])
        _write_jsonl(curated_dir / "process_counsel_link.jsonl", [])
        _write_jsonl(curated_dir / "decision_event.jsonl", [])

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
        records = [json.loads(line) for line in ambiguous_path.read_text().splitlines() if line.strip()]

        if records:
            rec = records[0]
            assert "sample_candidate_name" in rec
            assert rec["candidate_count"] is not None and rec["candidate_count"] >= 2
            assert rec["entity_type"] == "party"
            assert rec["total_donated_brl"] == 10000.0

    def test_ambiguous_summary_fields(self, tmp_path: Path) -> None:
        """Summary has explicit party/counsel/total ambiguous counts."""
        paths = _setup_test_data(tmp_path)
        build_donation_matches(**paths)

        summary_path = paths["output_dir"] / "donation_match_summary.json"
        summary = json.loads(summary_path.read_text())
        assert "party_ambiguous_candidate_count" in summary
        assert "counsel_ambiguous_candidate_count" in summary
        assert "total_ambiguous_candidate_count" in summary
        assert "ambiguous_records_written" in summary
        assert summary["total_ambiguous_candidate_count"] == (
            summary["party_ambiguous_candidate_count"] + summary["counsel_ambiguous_candidate_count"]
        )


class TestDonationStratifiedBaseline:
    def test_red_flag_with_stratified_baseline(self, tmp_path: Path) -> None:
        """Stratified baseline should be used when cell has enough data."""
        tse_dir = tmp_path / "tse"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"

        _write_jsonl(
            tse_dir / "donations_raw.jsonl",
            [{
                "election_year": 2022,
                "donor_name_normalized": "TARGET CORP",
                "donor_cpf_cnpj": "11111111000100",
                "donation_amount": 50000.0,
            }],
        )
        _write_jsonl(
            curated_dir / "party.jsonl",
            [{"party_id": "p1", "party_name_normalized": "TARGET CORP"}],
        )
        _write_jsonl(curated_dir / "counsel.jsonl", [])

        # 12 processes, all class RE
        processes = [{"process_id": f"proc{i}", "process_class": "RE"} for i in range(12)]
        _write_jsonl(curated_dir / "process.jsonl", processes)

        # p1 linked to 3 processes
        links = [{"link_id": f"pp{i}", "process_id": f"proc{i}", "party_id": "p1"} for i in range(3)]
        _write_jsonl(curated_dir / "process_party_link.jsonl", links)
        _write_jsonl(curated_dir / "process_counsel_link.jsonl", [])

        # proc0..2 = Provido (TARGET), proc3..7 = Provido, proc8..11 = Desprovido (all turma)
        events = []
        for i in range(8):
            events.append({
                "decision_event_id": f"de{i}",
                "process_id": f"proc{i}",
                "decision_progress": "Provido",
                "judging_body": "Primeira Turma",
                "is_collegiate": True,
            })
        for i in range(8, 12):
            events.append({
                "decision_event_id": f"de{i}",
                "process_id": f"proc{i}",
                "decision_progress": "Desprovido",
                "judging_body": "Primeira Turma",
                "is_collegiate": True,
            })
        _write_jsonl(curated_dir / "decision_event.jsonl", events)

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
        assert len(party_matches) == 1
        m = party_matches[0]
        assert m["favorable_rate"] == 1.0
        assert m["baseline_favorable_rate"] is not None
        assert m["red_flag"] is True
