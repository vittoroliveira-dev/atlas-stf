"""Tests for analytics/sanction_match.py — core match building (TestBuildSanctionMatches)."""

from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics.sanction_match import build_sanction_matches
from tests.analytics._sanction_match_helpers import _setup_test_data, _write_jsonl


class TestBuildSanctionMatches:
    def test_basic_match(self, tmp_path: Path) -> None:
        paths = _setup_test_data(tmp_path)
        build_sanction_matches(**paths)

        match_path = paths["output_dir"] / "sanction_match.jsonl"
        assert match_path.exists()
        matches = [json.loads(line) for line in match_path.read_text().strip().split("\n")]
        party_matches = [m for m in matches if m["entity_type"] == "party"]
        assert len(party_matches) == 1
        assert party_matches[0]["party_id"] == "p1"
        assert party_matches[0]["sanction_source"] == "ceis"
        assert party_matches[0]["stf_case_count"] == 3
        assert party_matches[0]["entity_type"] == "party"

    def test_favorable_rate_computed(self, tmp_path: Path) -> None:
        paths = _setup_test_data(tmp_path)
        build_sanction_matches(**paths)

        match_path = paths["output_dir"] / "sanction_match.jsonl"
        matches = [json.loads(line) for line in match_path.read_text().strip().split("\n")]
        party_matches = [m for m in matches if m["entity_type"] == "party"]
        assert party_matches[0]["favorable_rate"] == 1.0

    def test_red_flag_detection(self, tmp_path: Path) -> None:
        paths = _setup_test_data(tmp_path)
        build_sanction_matches(**paths)

        match_path = paths["output_dir"] / "sanction_match.jsonl"
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

    def test_composite_decision_progress(self, tmp_path: Path) -> None:
        """Test that composite STF-style decision_progress values are classified."""
        paths = _setup_test_data(tmp_path)
        # Override decisions with composite format
        curated_dir = tmp_path / "curated"
        _write_jsonl(
            curated_dir / "decision_event.jsonl",
            [
                {
                    "decision_event_id": "de1",
                    "process_id": "proc1",
                    "decision_progress": "JULGAMENTO DA PRIMEIRA TURMA - NEGADO PROVIMENTO",
                },
                {
                    "decision_event_id": "de2",
                    "process_id": "proc1",
                    "decision_progress": "DECISÃO DO(A) RELATOR(A) - NEGADO SEGUIMENTO",
                },
                {
                    "decision_event_id": "de3",
                    "process_id": "proc2",
                    "decision_progress": "DECISÃO DO(A) RELATOR(A) - DEFERIDO",
                },
                {
                    "decision_event_id": "de4",
                    "process_id": "proc2",
                    "decision_progress": "JULGAMENTO DA SEGUNDA TURMA - REJEITADOS",
                },
            ],
        )
        build_sanction_matches(**paths)

        match_path = paths["output_dir"] / "sanction_match.jsonl"
        matches = [json.loads(line) for line in match_path.read_text().strip().split("\n")]
        party_matches = [m for m in matches if m["entity_type"] == "party"]
        # proc1 (ACME): 2 unfavorable -> rate=0.0
        assert party_matches[0]["favorable_rate"] == 0.0

    def test_counsel_profile(self, tmp_path: Path) -> None:
        paths = _setup_test_data(tmp_path)
        build_sanction_matches(**paths)

        counsel_path = paths["output_dir"] / "counsel_sanction_profile.jsonl"
        assert counsel_path.exists()
        profiles = [json.loads(line) for line in counsel_path.read_text().strip().split("\n")]
        assert len(profiles) == 1
        assert profiles[0]["counsel_id"] == "c1"
        assert profiles[0]["sanctioned_client_count"] == 1
        assert profiles[0]["total_client_count"] == 2

    def test_summary(self, tmp_path: Path) -> None:
        paths = _setup_test_data(tmp_path)
        build_sanction_matches(**paths)

        summary_path = paths["output_dir"] / "sanction_match_summary.json"
        assert summary_path.exists()
        summary = json.loads(summary_path.read_text())
        assert summary["party_match_count"] == 1
        assert summary["matched_party_count"] == 1
        assert "matched_counsel_count" in summary

    def test_no_sanctions_file(self, tmp_path: Path) -> None:
        output_dir = tmp_path / "analytics"
        curated_dir = tmp_path / "curated"
        _write_jsonl(curated_dir / "process_party_link.jsonl", [])
        _write_jsonl(curated_dir / "process_counsel_link.jsonl", [])
        result = build_sanction_matches(
            cgu_dir=tmp_path / "empty",
            cvm_dir=tmp_path / "empty_cvm",
            party_path=tmp_path / "party.jsonl",
            counsel_path=tmp_path / "counsel.jsonl",
            process_path=tmp_path / "process.jsonl",
            decision_event_path=tmp_path / "de.jsonl",
            process_party_link_path=curated_dir / "process_party_link.jsonl",
            process_counsel_link_path=curated_dir / "process_counsel_link.jsonl",
            output_dir=output_dir,
        )
        assert result == output_dir / "sanction_match.jsonl"
        assert result.exists()

    def test_cvm_source_records_flow_through(self, tmp_path: Path) -> None:
        """CVM sanction records should flow through the builder with source preserved."""
        paths = _setup_test_data(tmp_path)
        # Add CVM sanctions
        cvm_dir = tmp_path / "cvm"
        _write_jsonl(
            cvm_dir / "sanctions_raw.jsonl",
            [
                {
                    "sanction_source": "cvm",
                    "sanction_id": "PAS-001",
                    "entity_name": "ACME CORP",
                    "sanctioning_body": "CVM",
                    "sanction_type": "Fraude",
                    "sanction_start_date": "2023-05-15",
                    "sanction_end_date": "",
                    "sanction_description": "Processo sancionador",
                },
            ],
        )
        build_sanction_matches(**paths, cvm_dir=cvm_dir)

        match_path = paths["output_dir"] / "sanction_match.jsonl"
        matches = [json.loads(line) for line in match_path.read_text().strip().split("\n")]
        party_matches = [m for m in matches if m["entity_type"] == "party"]
        # Should have 2 party matches: 1 CEIS + 1 CVM
        assert len(party_matches) == 2
        sources = {m["sanction_source"] for m in party_matches}
        assert sources == {"ceis", "cvm"}

    def test_mixed_sources_cgu_and_cvm(self, tmp_path: Path) -> None:
        """Both CGU and CVM records should be merged correctly."""
        paths = _setup_test_data(tmp_path)
        cvm_dir = tmp_path / "cvm"
        _write_jsonl(
            cvm_dir / "sanctions_raw.jsonl",
            [
                {
                    "sanction_source": "cvm",
                    "sanction_id": "PAS-002",
                    "entity_name": "CLEAN CO",
                    "sanctioning_body": "CVM",
                    "sanction_type": "Insider trading",
                    "sanction_start_date": "2024-01-01",
                    "sanction_end_date": "",
                    "sanction_description": "Test CVM",
                },
            ],
        )
        build_sanction_matches(**paths, cvm_dir=cvm_dir)

        match_path = paths["output_dir"] / "sanction_match.jsonl"
        matches = [json.loads(line) for line in match_path.read_text().strip().split("\n")]
        party_matches = [m for m in matches if m["entity_type"] == "party"]
        # ACME matches CEIS, CLEAN CO matches CVM
        assert len(party_matches) == 2
        party_sources = {(m["party_name_normalized"], m["sanction_source"]) for m in party_matches}
        assert ("ACME CORP", "ceis") in party_sources
        assert ("CLEAN CO", "cvm") in party_sources

    def test_summary_includes_source_breakdown(self, tmp_path: Path) -> None:
        """Summary should include per-source counts."""
        paths = _setup_test_data(tmp_path)
        cvm_dir = tmp_path / "cvm"
        _write_jsonl(
            cvm_dir / "sanctions_raw.jsonl",
            [
                {
                    "sanction_source": "cvm",
                    "sanction_id": "PAS-003",
                    "entity_name": "ACME CORP",
                    "sanctioning_body": "CVM",
                    "sanction_type": "Fraude",
                    "sanction_start_date": "2023-05-15",
                    "sanction_end_date": "",
                    "sanction_description": "Test",
                },
            ],
        )
        build_sanction_matches(**paths, cvm_dir=cvm_dir)

        summary_path = paths["output_dir"] / "sanction_match_summary.json"
        summary = json.loads(summary_path.read_text())
        assert "sources" in summary
        assert summary["sources"]["ceis"] == 1
        assert summary["sources"]["cvm"] == 1

    def test_cvm_only_no_cgu(self, tmp_path: Path) -> None:
        """Builder should work with only CVM data (no CGU)."""
        cvm_dir = tmp_path / "cvm"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"

        _write_jsonl(
            cvm_dir / "sanctions_raw.jsonl",
            [
                {
                    "sanction_source": "cvm",
                    "sanction_id": "PAS-X",
                    "entity_name": "ACME CORP",
                    "sanctioning_body": "CVM",
                    "sanction_type": "Test",
                    "sanction_start_date": "",
                    "sanction_end_date": "",
                    "sanction_description": "",
                },
            ],
        )
        _write_jsonl(curated_dir / "party.jsonl", [{"party_id": "p1", "party_name_normalized": "ACME CORP"}])
        _write_jsonl(curated_dir / "counsel.jsonl", [])
        _write_jsonl(curated_dir / "process.jsonl", [])
        _write_jsonl(curated_dir / "decision_event.jsonl", [])
        _write_jsonl(curated_dir / "process_party_link.jsonl", [])
        _write_jsonl(curated_dir / "process_counsel_link.jsonl", [])

        build_sanction_matches(
            cgu_dir=tmp_path / "empty_cgu",
            cvm_dir=cvm_dir,
            party_path=curated_dir / "party.jsonl",
            counsel_path=curated_dir / "counsel.jsonl",
            process_path=curated_dir / "process.jsonl",
            decision_event_path=curated_dir / "decision_event.jsonl",
            process_party_link_path=curated_dir / "process_party_link.jsonl",
            process_counsel_link_path=curated_dir / "process_counsel_link.jsonl",
            output_dir=analytics_dir,
        )

        match_path = analytics_dir / "sanction_match.jsonl"
        assert match_path.exists()
        matches = [json.loads(line) for line in match_path.read_text().strip().split("\n")]
        party_matches = [m for m in matches if m["entity_type"] == "party"]
        assert len(party_matches) == 1
        assert party_matches[0]["sanction_source"] == "cvm"

    def test_leniencia_source_records_flow_through(self, tmp_path: Path) -> None:
        """Leniência sanction records should flow through the builder with source preserved."""
        paths = _setup_test_data(tmp_path)
        # Add leniência records to CGU sanctions_raw.jsonl
        cgu_dir = paths["cgu_dir"]
        existing = [json.loads(line) for line in (cgu_dir / "sanctions_raw.jsonl").read_text().strip().split("\n")]
        existing.append(
            {
                "sanction_source": "leniencia",
                "sanction_id": "PROC-LEN-001",
                "entity_name": "ACME CORP",
                "sanctioning_body": "CGU",
                "sanction_type": "Em execução",
                "sanction_start_date": "2018-01-01",
                "sanction_end_date": "",
                "sanction_description": "Acordo de leniência",
            },
        )
        _write_jsonl(cgu_dir / "sanctions_raw.jsonl", existing)
        build_sanction_matches(**paths)

        match_path = paths["output_dir"] / "sanction_match.jsonl"
        matches = [json.loads(line) for line in match_path.read_text().strip().split("\n")]
        party_matches = [m for m in matches if m["entity_type"] == "party"]
        assert len(party_matches) == 2
        sources = {m["sanction_source"] for m in party_matches}
        assert sources == {"ceis", "leniencia"}

    def test_summary_includes_leniencia_breakdown(self, tmp_path: Path) -> None:
        """Summary should include leniencia in source breakdown."""
        paths = _setup_test_data(tmp_path)
        cgu_dir = paths["cgu_dir"]
        existing = [json.loads(line) for line in (cgu_dir / "sanctions_raw.jsonl").read_text().strip().split("\n")]
        existing.append(
            {
                "sanction_source": "leniencia",
                "sanction_id": "PROC-LEN-002",
                "entity_name": "ACME CORP",
                "sanctioning_body": "CGU",
                "sanction_type": "Cumprido",
                "sanction_start_date": "2019-01-01",
                "sanction_end_date": "2024-01-01",
                "sanction_description": "Acordo de leniência",
            },
        )
        _write_jsonl(cgu_dir / "sanctions_raw.jsonl", existing)
        build_sanction_matches(**paths)

        summary_path = paths["output_dir"] / "sanction_match_summary.json"
        summary = json.loads(summary_path.read_text())
        assert "leniencia" in summary["sources"]
        assert summary["sources"]["leniencia"] == 1

    def test_no_matching_party(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"

        _write_jsonl(
            cgu_dir / "sanctions_raw.jsonl",
            [{"sanction_source": "ceis", "sanction_id": "1", "entity_name": "UNKNOWN CORP"}],
        )
        _write_jsonl(curated_dir / "party.jsonl", [{"party_id": "p1", "party_name_normalized": "OTHER"}])
        _write_jsonl(curated_dir / "counsel.jsonl", [])
        _write_jsonl(curated_dir / "process.jsonl", [])
        _write_jsonl(curated_dir / "decision_event.jsonl", [])
        _write_jsonl(curated_dir / "process_party_link.jsonl", [])
        _write_jsonl(curated_dir / "process_counsel_link.jsonl", [])

        build_sanction_matches(
            cgu_dir=cgu_dir,
            party_path=curated_dir / "party.jsonl",
            counsel_path=curated_dir / "counsel.jsonl",
            process_path=curated_dir / "process.jsonl",
            decision_event_path=curated_dir / "decision_event.jsonl",
            process_party_link_path=curated_dir / "process_party_link.jsonl",
            process_counsel_link_path=curated_dir / "process_counsel_link.jsonl",
            output_dir=analytics_dir,
        )

        match_path = analytics_dir / "sanction_match.jsonl"
        assert match_path.exists()
        assert match_path.read_text().strip() == ""
