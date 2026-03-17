"""Tests for sanction_corporate_link: evidence chain, audit fields, degree, risk decay, red flag threshold."""

from __future__ import annotations

from pathlib import Path

from atlas_stf.analytics.sanction_corporate_link import (
    RED_FLAG_DELTA_THRESHOLD,
    _degree_decay,
    build_sanction_corporate_links,
)
from tests.analytics._scl_helpers import (
    PARTY_CPF,
    PARTY_NAME,
    SANCTION_CNPJ,
    SANCTION_CNPJ_BASICO,
    _base_curated,
    _read_jsonl,
    _setup_curated,
    _setup_rfb,
    _setup_sanctions,
    _write_jsonl,
)


class TestEvidenceChainComplete:
    """evidence_chain should be a non-empty list of readable strings."""

    def test_evidence_chain_complete(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        out_dir = tmp_path / "output"

        _setup_sanctions(
            cgu_dir,
            [
                {
                    "sanction_id": "S010",
                    "sanction_source": "ceis",
                    "entity_name": "EMPRESA EVIDENCIA",
                    "entity_cnpj_cpf": SANCTION_CNPJ,
                    "sanction_type": "Inidoneidade",
                },
            ],
        )
        _setup_rfb(
            rfb_dir,
            partners=[
                {
                    "cnpj_basico": SANCTION_CNPJ_BASICO,
                    "partner_name": PARTY_NAME,
                    "partner_name_normalized": PARTY_NAME,
                    "partner_cpf_cnpj": PARTY_CPF,
                    "qualification_code": "49",
                },
            ],
            companies=[
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "razao_social": "EVIDENCIA LTDA"},
            ],
            establishments=[],
        )
        _base_curated(curated_dir)

        build_sanction_corporate_links(
            cgu_dir=cgu_dir,
            cvm_dir=tmp_path / "cvm_empty",
            rfb_dir=rfb_dir,
            curated_dir=curated_dir,
            analytics_dir=analytics_dir,
            output_dir=out_dir,
        )

        records = _read_jsonl(out_dir / "sanction_corporate_link.jsonl")
        assert len(records) >= 1
        chain = records[0]["evidence_chain"]
        assert isinstance(chain, list)
        assert len(chain) >= 2
        assert all(isinstance(s, str) and len(s) > 0 for s in chain)
        # First element mentions the sanction source
        assert "CEIS" in chain[0].upper() or "ceis" in chain[0].lower()


class TestAuditFieldsPreserved:
    """matched_alias, matched_tax_id and uncertainty_note should be present."""

    def test_audit_fields_preserved(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        out_dir = tmp_path / "output"

        _setup_sanctions(
            cgu_dir,
            [
                {
                    "sanction_id": "S020",
                    "sanction_source": "ceis",
                    "entity_name": "EMPRESA AUDIT",
                    "entity_cnpj_cpf": SANCTION_CNPJ,
                    "sanction_type": "Inidoneidade",
                },
            ],
        )
        _setup_rfb(
            rfb_dir,
            partners=[
                {
                    "cnpj_basico": SANCTION_CNPJ_BASICO,
                    "partner_name": PARTY_NAME,
                    "partner_name_normalized": PARTY_NAME,
                    "partner_cpf_cnpj": PARTY_CPF,
                    "qualification_code": "49",
                },
            ],
            companies=[
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "razao_social": "AUDIT LTDA"},
            ],
            establishments=[],
        )
        _base_curated(curated_dir)

        build_sanction_corporate_links(
            cgu_dir=cgu_dir,
            cvm_dir=tmp_path / "cvm_empty",
            rfb_dir=rfb_dir,
            curated_dir=curated_dir,
            analytics_dir=analytics_dir,
            output_dir=out_dir,
        )

        records = _read_jsonl(out_dir / "sanction_corporate_link.jsonl")
        assert len(records) >= 1
        rec = records[0]
        # These fields must be present in every record (even if None)
        assert "matched_alias" in rec
        assert "matched_tax_id" in rec
        assert "uncertainty_note" in rec
        assert "stf_match_strategy" in rec
        assert "stf_match_confidence" in rec


class TestDegreeAlwaysGe2:
    """All records must have link_degree >= 2."""

    def test_degree_always_ge_2(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        out_dir = tmp_path / "output"

        _setup_sanctions(
            cgu_dir,
            [
                {
                    "sanction_id": "S030",
                    "sanction_source": "ceis",
                    "entity_name": "GRAU MINIMO",
                    "entity_cnpj_cpf": SANCTION_CNPJ,
                    "sanction_type": "Inidoneidade",
                },
            ],
        )
        _setup_rfb(
            rfb_dir,
            partners=[
                {
                    "cnpj_basico": SANCTION_CNPJ_BASICO,
                    "partner_name": PARTY_NAME,
                    "partner_name_normalized": PARTY_NAME,
                    "partner_cpf_cnpj": PARTY_CPF,
                    "qualification_code": "49",
                },
            ],
            companies=[
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "razao_social": "GRAU LTDA"},
            ],
            establishments=[],
        )
        _base_curated(curated_dir)

        build_sanction_corporate_links(
            cgu_dir=cgu_dir,
            cvm_dir=tmp_path / "cvm_empty",
            rfb_dir=rfb_dir,
            curated_dir=curated_dir,
            analytics_dir=analytics_dir,
            output_dir=out_dir,
        )

        records = _read_jsonl(out_dir / "sanction_corporate_link.jsonl")
        assert len(records) >= 1
        for rec in records:
            assert rec["link_degree"] >= 2


class TestRiskScoreDecay:
    """risk_score should equal delta * 0.5^(degree-2) for degree >= 2."""

    def test_degree_decay_function(self) -> None:
        assert _degree_decay(1) == 1.0
        assert _degree_decay(2) == 1.0
        assert _degree_decay(3) == 0.5
        assert _degree_decay(4) == 0.25

    def test_risk_score_decay(self, tmp_path: Path) -> None:
        """Degree-3 links (economic group) should have half the risk_score of degree-2."""
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        out_dir = tmp_path / "output"

        _setup_sanctions(
            cgu_dir,
            [
                {
                    "sanction_id": "S040",
                    "sanction_source": "ceis",
                    "entity_name": "EMPRESA DECAY",
                    "entity_cnpj_cpf": SANCTION_CNPJ,
                    "sanction_type": "Inidoneidade",
                },
            ],
        )

        # Economic group: SANCTION_CNPJ_BASICO + "33445566" are in the same group.
        # Party is co-partner at "33445566" (reached via group expansion → degree=3).
        _write_jsonl(
            analytics_dir / "economic_group.jsonl",
            [
                {
                    "group_id": "G001",
                    "member_cnpjs": [SANCTION_CNPJ_BASICO, "33445566"],
                    "member_count": 2,
                    "is_law_firm_group": False,
                },
            ],
        )
        _setup_rfb(
            rfb_dir,
            partners=[
                # Co-partner at the GROUP member company (not the bridge itself)
                {
                    "cnpj_basico": "33445566",
                    "partner_name": PARTY_NAME,
                    "partner_name_normalized": PARTY_NAME,
                    "partner_cpf_cnpj": PARTY_CPF,
                    "qualification_code": "49",
                },
            ],
            companies=[
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "razao_social": "EMPRESA DECAY LTDA"},
                {"cnpj_basico": "33445566", "razao_social": "GRUPO MEMBRO LTDA"},
            ],
            establishments=[],
        )
        _base_curated(curated_dir)

        build_sanction_corporate_links(
            cgu_dir=cgu_dir,
            cvm_dir=tmp_path / "cvm_empty",
            rfb_dir=rfb_dir,
            curated_dir=curated_dir,
            analytics_dir=analytics_dir,
            output_dir=out_dir,
        )

        records = _read_jsonl(out_dir / "sanction_corporate_link.jsonl")
        degree3 = [r for r in records if r["link_degree"] == 3]
        assert len(degree3) >= 1
        for rec in degree3:
            if rec["risk_score"] is not None and rec["favorable_rate_delta"] is not None:
                expected = rec["favorable_rate_delta"] * 0.5
                assert abs(rec["risk_score"] - expected) < 1e-9


class TestRedFlagThreshold:
    """red_flag=True iff risk_score > 0.15 AND process_count >= 3."""

    def test_red_flag_threshold(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        out_dir = tmp_path / "output"

        _setup_sanctions(
            cgu_dir,
            [
                {
                    "sanction_id": "S050",
                    "sanction_source": "ceis",
                    "entity_name": "REDFLAG CORP",
                    "entity_cnpj_cpf": SANCTION_CNPJ,
                    "sanction_type": "Inidoneidade",
                },
            ],
        )
        _setup_rfb(
            rfb_dir,
            partners=[
                {
                    "cnpj_basico": SANCTION_CNPJ_BASICO,
                    "partner_name": PARTY_NAME,
                    "partner_name_normalized": PARTY_NAME,
                    "partner_cpf_cnpj": PARTY_CPF,
                    "qualification_code": "49",
                },
            ],
            companies=[
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "razao_social": "REDFLAG LTDA"},
            ],
            establishments=[],
        )
        # 4 processes all favorable → favorable_rate=1.0
        # Baseline with other unfavorable cases so delta > 0.15
        processes = [{"process_id": f"proc{i}", "process_class": "RE"} for i in range(12)]
        party_links = [{"link_id": f"ppl{i}", "process_id": f"proc{i}", "party_id": "p1"} for i in range(4)]
        events = []
        # p1's cases: all Provido
        for i in range(4):
            events.append(
                {
                    "decision_event_id": f"de{i}",
                    "process_id": f"proc{i}",
                    "decision_progress": "Provido",
                    "judging_body": "Segunda Turma",
                    "is_collegiate": True,
                }
            )
        # Other cases: mix (baseline ~50%)
        for i in range(4, 8):
            events.append(
                {
                    "decision_event_id": f"de{i}",
                    "process_id": f"proc{i}",
                    "decision_progress": "Provido",
                    "judging_body": "Segunda Turma",
                    "is_collegiate": True,
                }
            )
        for i in range(8, 12):
            events.append(
                {
                    "decision_event_id": f"de{i}",
                    "process_id": f"proc{i}",
                    "decision_progress": "Desprovido",
                    "judging_body": "Segunda Turma",
                    "is_collegiate": True,
                }
            )

        _setup_curated(
            curated_dir,
            parties=[{"party_id": "p1", "party_name_normalized": PARTY_NAME}],
            processes=processes,
            decision_events=events,
            process_party_links=party_links,
        )

        build_sanction_corporate_links(
            cgu_dir=cgu_dir,
            cvm_dir=tmp_path / "cvm_empty",
            rfb_dir=rfb_dir,
            curated_dir=curated_dir,
            analytics_dir=analytics_dir,
            output_dir=out_dir,
        )

        records = _read_jsonl(out_dir / "sanction_corporate_link.jsonl")
        assert len(records) >= 1
        rec = records[0]
        # favorable_rate=1.0, baseline≈0.667, delta≈0.333 > 0.15 AND process_count=4 >= 3
        assert rec["red_flag"] is True
        assert rec["risk_score"] is not None
        assert rec["risk_score"] > RED_FLAG_DELTA_THRESHOLD
        assert rec["stf_process_count"] >= 3
        # Power analysis fields
        assert "red_flag_power" in rec
        assert "red_flag_confidence" in rec

    def test_red_flag_false_insufficient_cases(self, tmp_path: Path) -> None:
        """With only 2 processes, red_flag should be False even with high delta."""
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        out_dir = tmp_path / "output"

        _setup_sanctions(
            cgu_dir,
            [
                {
                    "sanction_id": "S051",
                    "sanction_source": "ceis",
                    "entity_name": "FEW CASES",
                    "entity_cnpj_cpf": SANCTION_CNPJ,
                    "sanction_type": "Inidoneidade",
                },
            ],
        )
        _setup_rfb(
            rfb_dir,
            partners=[
                {
                    "cnpj_basico": SANCTION_CNPJ_BASICO,
                    "partner_name": PARTY_NAME,
                    "partner_name_normalized": PARTY_NAME,
                    "partner_cpf_cnpj": PARTY_CPF,
                    "qualification_code": "49",
                },
            ],
            companies=[
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "razao_social": "FEW LTDA"},
            ],
            establishments=[],
        )
        # Only 2 processes for the party
        _base_curated(curated_dir, process_count=2)

        build_sanction_corporate_links(
            cgu_dir=cgu_dir,
            cvm_dir=tmp_path / "cvm_empty",
            rfb_dir=rfb_dir,
            curated_dir=curated_dir,
            analytics_dir=analytics_dir,
            output_dir=out_dir,
        )

        records = _read_jsonl(out_dir / "sanction_corporate_link.jsonl")
        assert len(records) >= 1
        assert records[0]["red_flag"] is False
