"""Tests for corporate network analytics builder — degree 2/3 and BFS scenarios."""

from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics.corporate_network import build_corporate_network
from tests.analytics.conftest import corporate_network_setup, write_jsonl


class TestCorporateNetworkAdvanced:
    def test_grau2_pj_expansion(self, tmp_path: Path) -> None:
        """Company A has PJ partner (company B), company B has co-partner party -> degree 2."""
        paths = corporate_network_setup(tmp_path)
        write_jsonl(
            paths["rfb_dir"] / "partners_raw.jsonl",
            [
                # Minister is partner of company 11111111
                {
                    "cnpj_basico": "11111111",
                    "partner_type": "2",
                    "partner_name": "MIN. TESTE",
                    "partner_name_normalized": "MIN. TESTE",
                    "partner_cpf_cnpj": "00011122233",
                    "qualification_code": "49",
                },
                # Company 22222222 is PJ partner of company 11111111
                {
                    "cnpj_basico": "11111111",
                    "partner_type": "1",
                    "partner_name": "EMPRESA B LTDA",
                    "partner_name_normalized": "EMPRESA B LTDA",
                    "partner_cpf_cnpj": "22222222",
                    "qualification_code": "22",
                },
                # Company 22222222 also appears as partner in company 33333333
                {
                    "cnpj_basico": "33333333",
                    "partner_type": "1",
                    "partner_name": "EMPRESA B LTDA",
                    "partner_name_normalized": "EMPRESA B LTDA",
                    "partner_cpf_cnpj": "22222222",
                    "qualification_code": "22",
                },
                # AUTOR A is co-partner in company 33333333 (degree 2 link)
                {
                    "cnpj_basico": "33333333",
                    "partner_type": "2",
                    "partner_name": "AUTOR A",
                    "partner_name_normalized": "AUTOR A",
                    "partner_cpf_cnpj": "44455566677",
                    "qualification_code": "22",
                },
            ],
        )
        write_jsonl(
            paths["rfb_dir"] / "companies_raw.jsonl",
            [
                {"cnpj_basico": "11111111", "razao_social": "EMPRESA A LTDA"},
                {"cnpj_basico": "33333333", "razao_social": "EMPRESA C LTDA"},
            ],
        )
        result = build_corporate_network(**paths)
        conflicts = [json.loads(line) for line in result.read_text(encoding="utf-8").strip().split("\n")]
        degree2 = [c for c in conflicts if c.get("link_degree") == 2]
        assert len(degree2) >= 1
        assert degree2[0]["linked_entity_name"] == "AUTOR A"
        assert degree2[0]["company_cnpj_basico"] == "33333333"

    def test_grau3_bfs_applies_decay_factor(self, tmp_path: Path) -> None:
        """Degree-3 chains must be discovered and their risk score decayed."""
        paths = corporate_network_setup(tmp_path)
        write_jsonl(
            paths["rfb_dir"] / "partners_raw.jsonl",
            [
                {
                    "cnpj_basico": "11111111",
                    "partner_type": "2",
                    "partner_name": "MIN. TESTE",
                    "partner_name_normalized": "MIN. TESTE",
                    "partner_cpf_cnpj": "00011122233",
                    "qualification_code": "49",
                },
                {
                    "cnpj_basico": "11111111",
                    "partner_type": "1",
                    "partner_name": "EMPRESA B LTDA",
                    "partner_name_normalized": "EMPRESA B LTDA",
                    "partner_cpf_cnpj": "22222222",
                    "qualification_code": "22",
                },
                {
                    "cnpj_basico": "33333333",
                    "partner_type": "1",
                    "partner_name": "EMPRESA B LTDA",
                    "partner_name_normalized": "EMPRESA B LTDA",
                    "partner_cpf_cnpj": "22222222",
                    "qualification_code": "22",
                },
                {
                    "cnpj_basico": "33333333",
                    "partner_type": "1",
                    "partner_name": "EMPRESA D LTDA",
                    "partner_name_normalized": "EMPRESA D LTDA",
                    "partner_cpf_cnpj": "44444444",
                    "qualification_code": "22",
                },
                {
                    "cnpj_basico": "55555555",
                    "partner_type": "1",
                    "partner_name": "EMPRESA D LTDA",
                    "partner_name_normalized": "EMPRESA D LTDA",
                    "partner_cpf_cnpj": "44444444",
                    "qualification_code": "22",
                },
                {
                    "cnpj_basico": "55555555",
                    "partner_type": "2",
                    "partner_name": "AUTOR A",
                    "partner_name_normalized": "AUTOR A",
                    "partner_cpf_cnpj": "44455566677",
                    "qualification_code": "22",
                },
            ],
        )
        write_jsonl(
            paths["rfb_dir"] / "companies_raw.jsonl",
            [
                {"cnpj_basico": "11111111", "razao_social": "EMPRESA A LTDA"},
                {"cnpj_basico": "33333333", "razao_social": "EMPRESA C LTDA"},
                {"cnpj_basico": "55555555", "razao_social": "EMPRESA E LTDA"},
            ],
        )
        write_jsonl(
            paths["process_path"],
            [
                {"process_id": "proc_1", "process_class": "ADI"},
                {"process_id": "proc_2", "process_class": "ADI"},
                {"process_id": "proc_3", "process_class": "ADI"},
                {"process_id": "proc_4", "process_class": "ADI"},
                {"process_id": "proc_5", "process_class": "ADI"},
                {"process_id": "proc_6", "process_class": "ADI"},
            ],
        )
        write_jsonl(
            paths["decision_event_path"],
            [
                {
                    "decision_event_id": "e1",
                    "process_id": "proc_1",
                    "current_rapporteur": "MIN. TESTE",
                    "decision_progress": "Procedente",
                },
                {
                    "decision_event_id": "e2",
                    "process_id": "proc_2",
                    "current_rapporteur": "MIN. TESTE",
                    "decision_progress": "Procedente",
                },
                {
                    "decision_event_id": "e3",
                    "process_id": "proc_3",
                    "current_rapporteur": "MIN. TESTE",
                    "decision_progress": "Procedente",
                },
                {
                    "decision_event_id": "e4",
                    "process_id": "proc_4",
                    "current_rapporteur": "MIN. BASE",
                    "decision_progress": "Improcedente",
                },
                {
                    "decision_event_id": "e5",
                    "process_id": "proc_5",
                    "current_rapporteur": "MIN. BASE",
                    "decision_progress": "Improcedente",
                },
                {
                    "decision_event_id": "e6",
                    "process_id": "proc_6",
                    "current_rapporteur": "MIN. BASE",
                    "decision_progress": "Improcedente",
                },
            ],
        )
        write_jsonl(
            paths["process_party_link_path"],
            [
                {"link_id": "pp1", "process_id": "proc_1", "party_id": "p1", "role_in_case": "REQTE.(S)"},
                {"link_id": "pp2", "process_id": "proc_2", "party_id": "p1", "role_in_case": "REQTE.(S)"},
                {"link_id": "pp3", "process_id": "proc_3", "party_id": "p1", "role_in_case": "REQTE.(S)"},
            ],
        )

        result = build_corporate_network(**paths, max_link_degree=3)
        conflicts = [json.loads(line) for line in result.read_text(encoding="utf-8").strip().split("\n")]
        degree3 = [c for c in conflicts if c.get("link_degree") == 3]

        assert len(degree3) == 1
        conflict = degree3[0]
        assert conflict["linked_entity_name"] == "AUTOR A"
        assert conflict["company_cnpj_basico"] == "55555555"
        assert conflict["decay_factor"] == 0.5
        assert conflict["favorable_rate_delta"] == 0.5
        assert conflict["risk_score"] == 0.25
        assert conflict["red_flag"] is True
        assert "EMPRESA C LTDA" in conflict["link_chain"]
        assert "EMPRESA E LTDA" in conflict["link_chain"]

    def test_max_link_degree_2_does_not_emit_grau3(self, tmp_path: Path) -> None:
        """When max_link_degree is 2, degree-3 chains must remain out of scope."""
        paths = corporate_network_setup(tmp_path)
        write_jsonl(
            paths["rfb_dir"] / "partners_raw.jsonl",
            [
                {
                    "cnpj_basico": "11111111",
                    "partner_type": "2",
                    "partner_name": "MIN. TESTE",
                    "partner_name_normalized": "MIN. TESTE",
                    "partner_cpf_cnpj": "00011122233",
                    "qualification_code": "49",
                },
                {
                    "cnpj_basico": "11111111",
                    "partner_type": "1",
                    "partner_name": "EMPRESA B LTDA",
                    "partner_name_normalized": "EMPRESA B LTDA",
                    "partner_cpf_cnpj": "22222222",
                    "qualification_code": "22",
                },
                {
                    "cnpj_basico": "33333333",
                    "partner_type": "1",
                    "partner_name": "EMPRESA B LTDA",
                    "partner_name_normalized": "EMPRESA B LTDA",
                    "partner_cpf_cnpj": "22222222",
                    "qualification_code": "22",
                },
                {
                    "cnpj_basico": "33333333",
                    "partner_type": "1",
                    "partner_name": "EMPRESA D LTDA",
                    "partner_name_normalized": "EMPRESA D LTDA",
                    "partner_cpf_cnpj": "44444444",
                    "qualification_code": "22",
                },
                {
                    "cnpj_basico": "55555555",
                    "partner_type": "1",
                    "partner_name": "EMPRESA D LTDA",
                    "partner_name_normalized": "EMPRESA D LTDA",
                    "partner_cpf_cnpj": "44444444",
                    "qualification_code": "22",
                },
                {
                    "cnpj_basico": "55555555",
                    "partner_type": "2",
                    "partner_name": "AUTOR A",
                    "partner_name_normalized": "AUTOR A",
                    "partner_cpf_cnpj": "44455566677",
                    "qualification_code": "22",
                },
            ],
        )
        write_jsonl(
            paths["rfb_dir"] / "companies_raw.jsonl",
            [
                {"cnpj_basico": "11111111", "razao_social": "EMPRESA A LTDA"},
                {"cnpj_basico": "33333333", "razao_social": "EMPRESA C LTDA"},
                {"cnpj_basico": "55555555", "razao_social": "EMPRESA E LTDA"},
            ],
        )

        result = build_corporate_network(**paths, max_link_degree=2)
        lines = result.read_text(encoding="utf-8").strip().split("\n")
        conflicts = [json.loads(line) for line in lines if line.strip()]

        assert all(conflict["link_degree"] <= 2 for conflict in conflicts)
