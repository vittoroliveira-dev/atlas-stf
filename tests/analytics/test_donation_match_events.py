"""Tests for donation_match: events, resource classification, provenance, originator, ambiguous trail, baseline."""

from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics.donation_match import build_donation_matches
from tests.analytics._donation_match_helpers import _setup_test_data, _write_jsonl


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
            [
                {
                    "election_year": 2022,
                    "donor_name_normalized": "TARGET CORP",
                    "donor_cpf_cnpj": "11111111000100",
                    "donation_amount": 50000.0,
                }
            ],
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
            events.append(
                {
                    "decision_event_id": f"de{i}",
                    "process_id": f"proc{i}",
                    "decision_progress": "Provido",
                    "judging_body": "Primeira Turma",
                    "is_collegiate": True,
                }
            )
        for i in range(8, 12):
            events.append(
                {
                    "decision_event_id": f"de{i}",
                    "process_id": f"proc{i}",
                    "decision_progress": "Desprovido",
                    "judging_body": "Primeira Turma",
                    "is_collegiate": True,
                }
            )
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
