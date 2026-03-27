"""Tests for corporate network analytics builder — basic scenarios."""

from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics.corporate_network import build_corporate_network
from tests.analytics.conftest import corporate_network_setup, write_json, write_jsonl


class TestBuildCorporateNetwork:
    def test_detects_conflict(self, tmp_path: Path) -> None:
        paths = corporate_network_setup(tmp_path)
        result = build_corporate_network(**paths)
        assert result.exists()
        conflicts = [json.loads(line) for line in result.read_text(encoding="utf-8").strip().split("\n")]
        assert len(conflicts) >= 1
        c = conflicts[0]
        assert c["minister_name"] == "MIN. TESTE"
        assert c["linked_entity_name"] == "AUTOR A"
        assert c["linked_entity_type"] == "party"
        assert c["company_cnpj_basico"] == "11111111"
        assert c["link_degree"] == 1

    def test_summary_written(self, tmp_path: Path) -> None:
        paths = corporate_network_setup(tmp_path)
        build_corporate_network(**paths)
        summary_path = paths["output_dir"] / "corporate_network_summary.json"
        assert summary_path.exists()
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        assert "total_conflicts" in summary
        assert summary["total_conflicts"] >= 1
        assert "degree_1_count" in summary
        assert "degree_2_count" in summary

    def test_no_rfb_data(self, tmp_path: Path) -> None:
        paths = corporate_network_setup(tmp_path)
        # Remove RFB data
        (paths["rfb_dir"] / "partners_raw.jsonl").unlink()
        result = build_corporate_network(**paths)
        assert result == paths["output_dir"] / "corporate_network.jsonl"
        assert result.exists()

    def test_no_match(self, tmp_path: Path) -> None:
        paths = corporate_network_setup(tmp_path)
        # Replace partner with a name not in curated
        write_jsonl(
            paths["rfb_dir"] / "partners_raw.jsonl",
            [
                {
                    "cnpj_basico": "11111111",
                    "partner_name": "MIN. TESTE",
                    "partner_name_normalized": "MIN. TESTE",
                    "qualification_code": "49",
                },
                {
                    "cnpj_basico": "11111111",
                    "partner_name": "DESCONHECIDO",
                    "partner_name_normalized": "DESCONHECIDO",
                    "qualification_code": "22",
                },
            ],
        )
        result = build_corporate_network(**paths)
        conflicts = [
            json.loads(line) for line in result.read_text(encoding="utf-8").strip().split("\n") if line.strip()
        ]
        assert len(conflicts) == 0

    def test_civil_name_matching(self, tmp_path: Path) -> None:
        """minister_bio has civil_name, partner matches civil_name -> conflict detected."""
        paths = corporate_network_setup(tmp_path)
        # Update bio to have civil_name different from minister_name
        bio_path = paths["minister_bio_path"]
        write_json(
            bio_path,
            {
                "m1": {
                    "minister_name": "MIN. TESTE",
                    "civil_name": "FULANO XAVIER TESTE",
                }
            },
        )
        # Replace partners: minister uses civil name in RFB
        write_jsonl(
            paths["rfb_dir"] / "partners_raw.jsonl",
            [
                {
                    "cnpj_basico": "11111111",
                    "partner_name": "FULANO XAVIER TESTE",
                    "partner_name_normalized": "FULANO XAVIER TESTE",
                    "qualification_code": "49",
                },
                {
                    "cnpj_basico": "11111111",
                    "partner_name": "AUTOR A",
                    "partner_name_normalized": "AUTOR A",
                    "qualification_code": "22",
                },
            ],
        )
        result = build_corporate_network(**paths)
        conflicts = [json.loads(line) for line in result.read_text(encoding="utf-8").strip().split("\n")]
        assert len(conflicts) >= 1
        assert conflicts[0]["minister_name"] == "MIN. TESTE"

    def test_representative_link(self, tmp_path: Path) -> None:
        """Partner with representative_name that is a party -> conflict detected."""
        paths = corporate_network_setup(tmp_path)
        write_jsonl(
            paths["rfb_dir"] / "partners_raw.jsonl",
            [
                {
                    "cnpj_basico": "11111111",
                    "partner_name": "MIN. TESTE",
                    "partner_name_normalized": "MIN. TESTE",
                    "qualification_code": "49",
                },
                {
                    "cnpj_basico": "11111111",
                    "partner_type": "1",
                    "partner_name": "PJ QUALQUER LTDA",
                    "partner_name_normalized": "PJ QUALQUER LTDA",
                    "partner_cpf_cnpj": "",
                    "qualification_code": "22",
                    "representative_name": "AUTOR A",
                    "representative_name_normalized": "AUTOR A",
                    "representative_cpf_cnpj": "12345678901",
                },
            ],
        )
        result = build_corporate_network(**paths)
        conflicts = [json.loads(line) for line in result.read_text(encoding="utf-8").strip().split("\n")]
        # Should find AUTOR A via representative
        rep_conflicts = [c for c in conflicts if "(repr.)" in c.get("link_chain", "")]
        assert len(rep_conflicts) >= 1
        assert rep_conflicts[0]["linked_entity_name"] == "(repr.) AUTOR A"
        assert rep_conflicts[0]["evidence_type"] == "representative"

    def test_counsel_conflict_gets_outcome_analysis(self, tmp_path: Path) -> None:
        """Counsel co-partner should get favorable_rate and red_flag analysis."""
        paths = corporate_network_setup(tmp_path)
        # Replace partners: minister + ADV B (counsel) as co-partners
        write_jsonl(
            paths["rfb_dir"] / "partners_raw.jsonl",
            [
                {
                    "cnpj_basico": "11111111",
                    "partner_name": "MIN. TESTE",
                    "partner_name_normalized": "MIN. TESTE",
                    "qualification_code": "49",
                },
                {
                    "cnpj_basico": "11111111",
                    "partner_name": "ADV B",
                    "partner_name_normalized": "ADV B",
                    "qualification_code": "22",
                },
            ],
        )
        # Add counsel links to all 3 processes
        write_jsonl(
            paths["process_counsel_link_path"],
            [
                {"link_id": "pc1", "process_id": "proc_1", "counsel_id": "c1", "side_in_case": "REQTE.(S)"},
                {"link_id": "pc2", "process_id": "proc_2", "counsel_id": "c1", "side_in_case": "REQTE.(S)"},
                {"link_id": "pc3", "process_id": "proc_3", "counsel_id": "c1", "side_in_case": "REQTE.(S)"},
            ],
        )
        result = build_corporate_network(**paths)
        conflicts = [json.loads(line) for line in result.read_text(encoding="utf-8").strip().split("\n")]
        counsel_conflicts = [c for c in conflicts if c["linked_entity_type"] == "counsel"]
        assert len(counsel_conflicts) >= 1
        cc = counsel_conflicts[0]
        assert cc["linked_entity_name"] == "ADV B"
        assert cc["shared_process_count"] == 3
        assert cc["favorable_rate"] is not None
        # Power analysis fields present with correct types
        assert "red_flag_power" in cc
        assert "red_flag_confidence" in cc
        assert isinstance(cc["red_flag_power"], float)
        assert isinstance(cc["red_flag_confidence"], str)

    def test_stratified_baseline_with_decay(self, tmp_path: Path) -> None:
        """Stratified baseline should work with degree decay in corporate network."""
        paths = corporate_network_setup(tmp_path)

        # Replace decision events with enough data for stratified cell (12+ turma events)
        events = []
        processes = []
        for i in range(12):
            pid = f"proc_{i + 1}"
            processes.append({"process_id": pid, "process_class": "ADI"})
            events.append(
                {
                    "decision_event_id": f"e{i + 1}",
                    "process_id": pid,
                    "current_rapporteur": "MIN. TESTE",
                    "decision_progress": "Procedente" if i < 8 else "Improcedente",
                    "judging_body": "Segunda Turma",
                    "is_collegiate": True,
                }
            )
        write_jsonl(paths["process_path"], processes)
        write_jsonl(paths["decision_event_path"], events)

        # Update party links to use new process IDs
        party_links = [
            {"link_id": f"pp{i}", "process_id": f"proc_{i + 1}", "party_id": "p1", "role_in_case": "REQTE.(S)"}
            for i in range(3)
        ]
        write_jsonl(paths["process_party_link_path"], party_links)

        result = build_corporate_network(**paths)
        conflicts = [json.loads(line) for line in result.read_text(encoding="utf-8").strip().split("\n")]
        party_conflicts = [c for c in conflicts if c["linked_entity_type"] == "party"]
        assert len(party_conflicts) >= 1
        c = party_conflicts[0]
        assert c["baseline_favorable_rate"] is not None
        assert c["decay_factor"] == 1.0  # degree 1
