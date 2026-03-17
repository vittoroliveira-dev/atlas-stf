"""Tests for analytics/sanction_match.py — counsel matching and stratified baseline."""

from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics.sanction_match import build_sanction_matches
from tests.analytics._sanction_match_helpers import _setup_test_data, _write_jsonl


class TestCounselSanctionMatch:
    def test_counsel_direct_match(self, tmp_path: Path) -> None:
        """When a sanctioned entity name matches a counsel name, a counsel match is created."""
        cgu_dir = tmp_path / "cgu"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"

        _write_jsonl(
            cgu_dir / "sanctions_raw.jsonl",
            [
                {
                    "sanction_source": "ceis",
                    "sanction_id": "200",
                    "entity_name": "ADV SILVA",
                    "sanctioning_body": "CGU",
                    "sanction_type": "Inidoneidade",
                    "sanction_start_date": "2021-01-01",
                    "sanction_end_date": "",
                    "sanction_description": "Test counsel sanction",
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
        matches = [json.loads(line) for line in match_path.read_text().strip().split("\n") if line.strip()]
        counsel_matches = [m for m in matches if m["entity_type"] == "counsel"]
        assert len(counsel_matches) == 1
        assert counsel_matches[0]["entity_id"] == "c1"
        assert counsel_matches[0]["entity_name_normalized"] == "ADV SILVA"
        assert counsel_matches[0]["sanction_source"] == "ceis"
        assert counsel_matches[0]["stf_case_count"] == 1

    def test_summary_includes_counsel_counts(self, tmp_path: Path) -> None:
        """Summary should include counsel match counts."""
        paths = _setup_test_data(tmp_path)
        build_sanction_matches(**paths)

        summary = json.loads((paths["output_dir"] / "sanction_match_summary.json").read_text())
        assert "counsel_match_count" in summary
        assert "matched_counsel_count" in summary


class TestSanctionStratifiedBaseline:
    def test_red_flag_with_stratified_baseline(self, tmp_path: Path) -> None:
        """Stratified baseline should be used when cell has enough data."""
        cgu_dir = tmp_path / "cgu"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"

        _write_jsonl(
            cgu_dir / "sanctions_raw.jsonl",
            [{"sanction_source": "ceis", "sanction_id": "300", "entity_name": "TARGET CORP"}],
        )
        _write_jsonl(
            curated_dir / "party.jsonl",
            [{"party_id": "p1", "party_name_normalized": "TARGET CORP"}],
        )
        _write_jsonl(curated_dir / "counsel.jsonl", [])

        # 12 processes all class RE, all judged by turma
        processes = [{"process_id": f"proc{i}", "process_class": "RE"} for i in range(12)]
        _write_jsonl(curated_dir / "process.jsonl", processes)

        # Link p1 to 3 processes (proc0..proc2) — enough for red flag
        links = [{"link_id": f"pp{i}", "process_id": f"proc{i}", "party_id": "p1"} for i in range(3)]
        _write_jsonl(curated_dir / "process_party_link.jsonl", links)
        _write_jsonl(curated_dir / "process_counsel_link.jsonl", [])

        # Decision events: proc0..2 = Provido (TARGET's cases)
        # proc3..11 = 5 Provido + 4 Desprovido (others, all turma)
        # Stratified baseline for (RE, turma): (3+5)/(12) = 0.667
        # TARGET favorable_rate: 3/3 = 1.0; delta = 0.333 > 0.15 -> red_flag
        events = []
        for i in range(3):
            events.append(
                {
                    "decision_event_id": f"de_t{i}",
                    "process_id": f"proc{i}",
                    "decision_progress": "Provido",
                    "judging_body": "Segunda Turma",
                    "is_collegiate": True,
                }
            )
        for i in range(3, 8):
            events.append(
                {
                    "decision_event_id": f"de_o{i}",
                    "process_id": f"proc{i}",
                    "decision_progress": "Provido",
                    "judging_body": "Segunda Turma",
                    "is_collegiate": True,
                }
            )
        for i in range(8, 12):
            events.append(
                {
                    "decision_event_id": f"de_o{i}",
                    "process_id": f"proc{i}",
                    "decision_progress": "Desprovido",
                    "judging_body": "Segunda Turma",
                    "is_collegiate": True,
                }
            )
        _write_jsonl(curated_dir / "decision_event.jsonl", events)

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
        matches = [json.loads(line) for line in match_path.read_text().strip().split("\n") if line.strip()]
        party_matches = [m for m in matches if m["entity_type"] == "party"]
        assert len(party_matches) == 1
        m = party_matches[0]
        assert m["favorable_rate"] == 1.0
        assert m["baseline_favorable_rate"] is not None
        assert m["red_flag"] is True
