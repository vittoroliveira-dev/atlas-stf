"""Smoke validation tests — Phase A: materiality classification and substantive rate.

Validates:
  A. classify_outcome_materiality + compute_favorable_rate_substantive + builder output
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from atlas_stf.analytics._match_helpers import compute_favorable_rate_substantive
from atlas_stf.analytics.sanction_match import build_sanction_matches
from atlas_stf.core.rules import classify_outcome_materiality
from tests._smoke_helpers import _write_jsonl


class TestPhaseAMateriality:
    """Validate classify_outcome_materiality and builder output."""

    @pytest.mark.parametrize(
        "decision_progress, expected",
        [
            ("Liminar deferida", "provisional"),
            ("Liminar prejudicada", "provisional"),
            ("Não conhecido", "procedural"),
            ("Negado seguimento", "procedural"),
            ("Homologada a desistência", "procedural"),
            ("Provido", "substantive"),
            ("Não provido", "substantive"),
            ("Procedente", "substantive"),
            ("Concedida a ordem", "substantive"),
            ("Embargos rejeitados", "unknown"),
            ("Deferido", "unknown"),
            ("Decisão referendada", "unknown"),
            ("Agravo regimental provido", "substantive"),
            ("JULGAMENTO DO PLENO - NEGADO PROVIMENTO", "substantive"),
        ],
    )
    def test_materiality_classification_all_categories(
        self,
        decision_progress: str,
        expected: str,
    ) -> None:
        result = classify_outcome_materiality(decision_progress)
        assert result == expected

    def test_substantive_rate_excludes_non_substantive(self) -> None:
        outcomes: list[tuple[str, str | None]] = [
            ("Provido", None),
            ("Não provido", None),
            ("Procedente", None),
            ("Liminar deferida", None),
            ("Liminar prejudicada", None),
            ("Negado seguimento", None),
            ("Embargos rejeitados", None),
        ]
        rate, n_sub = compute_favorable_rate_substantive(outcomes)
        assert n_sub == 3
        assert rate is not None
        assert abs(rate - 2 / 3) < 1e-6

    def test_substantive_rate_none_when_no_substantive(self) -> None:
        outcomes: list[tuple[str, str | None]] = [
            ("Liminar deferida", None),
            ("Liminar prejudicada", None),
            ("Negado seguimento", None),
        ]
        rate, n_sub = compute_favorable_rate_substantive(outcomes)
        assert n_sub == 0
        assert rate is None

    def test_builder_output_contains_substantive_fields(
        self,
        tmp_path: Path,
    ) -> None:
        curated = tmp_path / "curated"
        cgu = tmp_path / "cgu"
        analytics = tmp_path / "analytics"

        _write_jsonl(
            cgu / "sanctions_raw.jsonl",
            [
                {
                    "entity_name": "ACME CORP",
                    "sanction_source": "ceis",
                    "sanction_id": "s1",
                    "entity_cnpj_cpf": "",
                }
            ],
        )
        _write_jsonl(
            curated / "party.jsonl",
            [
                {
                    "party_id": "p1",
                    "party_name_raw": "ACME CORP",
                    "party_name_normalized": "ACME CORP",
                }
            ],
        )
        _write_jsonl(
            curated / "process.jsonl",
            [{"process_id": f"proc{i}", "process_number": f"RE {i}", "process_class": "RE"} for i in range(1, 5)],
        )
        _write_jsonl(
            curated / "decision_event.jsonl",
            [
                {"decision_event_id": "e1", "process_id": "proc1", "decision_progress": "Provido"},
                {"decision_event_id": "e2", "process_id": "proc2", "decision_progress": "Liminar deferida"},
                {"decision_event_id": "e3", "process_id": "proc3", "decision_progress": "Não provido"},
                {"decision_event_id": "e4", "process_id": "proc4", "decision_progress": "Provido"},
            ],
        )
        _write_jsonl(
            curated / "process_party_link.jsonl",
            [
                {"link_id": f"lnk{i}", "process_id": f"proc{i}", "party_id": "p1", "role_in_case": "RECTE.(S)"}
                for i in range(1, 5)
            ],
        )
        _write_jsonl(curated / "counsel.jsonl", [])
        _write_jsonl(curated / "process_counsel_link.jsonl", [])

        result_path = build_sanction_matches(
            cgu_dir=cgu,
            cvm_dir=tmp_path / "cvm_empty",
            party_path=curated / "party.jsonl",
            counsel_path=curated / "counsel.jsonl",
            process_path=curated / "process.jsonl",
            decision_event_path=curated / "decision_event.jsonl",
            process_party_link_path=curated / "process_party_link.jsonl",
            process_counsel_link_path=curated / "process_counsel_link.jsonl",
            output_dir=analytics,
            alias_path=curated / "entity_alias.jsonl",
        )

        lines = result_path.read_text(encoding="utf-8").strip().split("\n")
        records = [json.loads(line) for line in lines if line]
        party_matches = [r for r in records if r["entity_type"] == "party"]
        assert len(party_matches) >= 1
        m = party_matches[0]

        assert m["favorable_rate"] is not None
        assert abs(m["favorable_rate"] - 0.75) < 1e-6

        assert m["favorable_rate_substantive"] is not None
        assert abs(m["favorable_rate_substantive"] - 2 / 3) < 1e-6

        assert m["substantive_decision_count"] == 3
        assert m["red_flag_substantive"] is not None
        assert isinstance(m["red_flag_substantive"], bool)
        assert "red_flag" in m
        assert isinstance(m["red_flag"], bool)

    def test_red_flag_substantive_none_below_threshold(
        self,
        tmp_path: Path,
    ) -> None:
        curated = tmp_path / "curated"
        cgu = tmp_path / "cgu"
        analytics = tmp_path / "analytics"

        _write_jsonl(
            cgu / "sanctions_raw.jsonl",
            [
                {
                    "entity_name": "SMALLCO",
                    "sanction_source": "ceis",
                    "sanction_id": "s2",
                    "entity_cnpj_cpf": "",
                }
            ],
        )
        _write_jsonl(
            curated / "party.jsonl",
            [
                {
                    "party_id": "p2",
                    "party_name_raw": "SMALLCO",
                    "party_name_normalized": "SMALLCO",
                }
            ],
        )
        _write_jsonl(
            curated / "process.jsonl",
            [{"process_id": f"sp{i}", "process_number": f"RE {i}", "process_class": "RE"} for i in range(1, 3)],
        )
        _write_jsonl(
            curated / "decision_event.jsonl",
            [
                {"decision_event_id": "se1", "process_id": "sp1", "decision_progress": "Provido"},
                {"decision_event_id": "se2", "process_id": "sp2", "decision_progress": "Não provido"},
            ],
        )
        _write_jsonl(
            curated / "process_party_link.jsonl",
            [
                {"link_id": f"sl{i}", "process_id": f"sp{i}", "party_id": "p2", "role_in_case": "RECTE.(S)"}
                for i in range(1, 3)
            ],
        )
        _write_jsonl(curated / "counsel.jsonl", [])
        _write_jsonl(curated / "process_counsel_link.jsonl", [])

        result_path = build_sanction_matches(
            cgu_dir=cgu,
            cvm_dir=tmp_path / "cvm_empty",
            party_path=curated / "party.jsonl",
            counsel_path=curated / "counsel.jsonl",
            process_path=curated / "process.jsonl",
            decision_event_path=curated / "decision_event.jsonl",
            process_party_link_path=curated / "process_party_link.jsonl",
            process_counsel_link_path=curated / "process_counsel_link.jsonl",
            output_dir=analytics,
            alias_path=curated / "entity_alias.jsonl",
        )

        lines = result_path.read_text(encoding="utf-8").strip().split("\n")
        records = [json.loads(line) for line in lines if line]
        party_matches = [r for r in records if r["entity_type"] == "party"]
        assert len(party_matches) >= 1
        m = party_matches[0]

        assert m["substantive_decision_count"] == 2
        assert m["red_flag_substantive"] is None

    def test_favorable_rate_legacy_vs_substantive_differ(
        self,
        tmp_path: Path,
    ) -> None:
        curated = tmp_path / "curated"
        cgu = tmp_path / "cgu"
        analytics = tmp_path / "analytics"

        _write_jsonl(
            cgu / "sanctions_raw.jsonl",
            [
                {
                    "entity_name": "MIXCORP",
                    "sanction_source": "ceis",
                    "sanction_id": "s3",
                    "entity_cnpj_cpf": "",
                }
            ],
        )
        _write_jsonl(
            curated / "party.jsonl",
            [
                {
                    "party_id": "p3",
                    "party_name_raw": "MIXCORP",
                    "party_name_normalized": "MIXCORP",
                }
            ],
        )
        _write_jsonl(
            curated / "process.jsonl",
            [{"process_id": f"mp{i}", "process_number": f"RE {i}", "process_class": "RE"} for i in range(1, 5)],
        )
        _write_jsonl(
            curated / "decision_event.jsonl",
            [
                {"decision_event_id": "me1", "process_id": "mp1", "decision_progress": "Provido"},
                {"decision_event_id": "me2", "process_id": "mp2", "decision_progress": "Liminar deferida"},
                {"decision_event_id": "me3", "process_id": "mp3", "decision_progress": "Não provido"},
                {"decision_event_id": "me4", "process_id": "mp4", "decision_progress": "Provido"},
            ],
        )
        _write_jsonl(
            curated / "process_party_link.jsonl",
            [
                {"link_id": f"ml{i}", "process_id": f"mp{i}", "party_id": "p3", "role_in_case": "RECTE.(S)"}
                for i in range(1, 5)
            ],
        )
        _write_jsonl(curated / "counsel.jsonl", [])
        _write_jsonl(curated / "process_counsel_link.jsonl", [])

        result_path = build_sanction_matches(
            cgu_dir=cgu,
            cvm_dir=tmp_path / "cvm_empty",
            party_path=curated / "party.jsonl",
            counsel_path=curated / "counsel.jsonl",
            process_path=curated / "process.jsonl",
            decision_event_path=curated / "decision_event.jsonl",
            process_party_link_path=curated / "process_party_link.jsonl",
            process_counsel_link_path=curated / "process_counsel_link.jsonl",
            output_dir=analytics,
            alias_path=curated / "entity_alias.jsonl",
        )

        lines = result_path.read_text(encoding="utf-8").strip().split("\n")
        records = [json.loads(line) for line in lines if line]
        party_matches = [r for r in records if r["entity_type"] == "party"]
        assert len(party_matches) >= 1
        m = party_matches[0]

        assert m["favorable_rate"] is not None
        assert m["favorable_rate_substantive"] is not None
        assert m["favorable_rate"] != m["favorable_rate_substantive"]
