"""Tests for sanction_corporate_link: economic group expansion, record hash, dedup routes."""

from __future__ import annotations

from pathlib import Path

from atlas_stf.analytics.sanction_corporate_link import (
    _record_hash,
    build_sanction_corporate_links,
)
from tests.analytics._scl_helpers import (
    OTHER_COMPANY_CNPJ_BASICO,
    PARTY_CPF,
    PARTY_NAME,
    SANCTION_CNPJ,
    SANCTION_CNPJ_BASICO,
    _base_curated,
    _read_jsonl,
    _setup_rfb,
    _setup_sanctions,
    _write_jsonl,
)


class TestEconomicGroupExpansion:
    """Company in economic group → group members generate matches."""

    def test_economic_group_expansion(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        out_dir = tmp_path / "output"

        _setup_sanctions(
            cgu_dir,
            [
                {
                    "sanction_id": "S060",
                    "sanction_source": "ceis",
                    "entity_name": "GRUPO SANCIONADO",
                    "entity_cnpj_cpf": SANCTION_CNPJ,
                    "sanction_type": "Inidoneidade",
                },
            ],
        )

        group_member_cnpj = "77889900"
        _write_jsonl(
            analytics_dir / "economic_group.jsonl",
            [
                {
                    "group_id": "G100",
                    "member_cnpjs": [SANCTION_CNPJ_BASICO, group_member_cnpj],
                    "member_count": 2,
                    "is_law_firm_group": False,
                },
            ],
        )
        _setup_rfb(
            rfb_dir,
            partners=[
                {
                    "cnpj_basico": group_member_cnpj,
                    "partner_name": PARTY_NAME,
                    "partner_name_normalized": PARTY_NAME,
                    "partner_cpf_cnpj": PARTY_CPF,
                    "qualification_code": "49",
                },
            ],
            companies=[
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "razao_social": "GRUPO SANCIONADO LTDA"},
                {"cnpj_basico": group_member_cnpj, "razao_social": "MEMBRO GRUPO LTDA"},
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
        # Should have a degree-3 link via economic group
        group_links = [r for r in records if r["link_degree"] == 3]
        assert len(group_links) >= 1
        assert group_links[0]["economic_group_id"] == "G100"
        # source_datasets should include economic_group
        assert "economic_group" in group_links[0]["source_datasets"]


class TestRecordHashChangesWithPayload:
    """record_hash must change when risk_score or evidence_chain changes."""

    def test_record_hash_changes_with_payload(self) -> None:
        base = {
            "link_id": "test",
            "sanction_id": "S1",
            "risk_score": 0.3,
            "evidence_chain": ["step1", "step2"],
        }
        h1 = _record_hash(base)

        modified = {**base, "risk_score": 0.5}
        h2 = _record_hash(modified)
        assert h1 != h2

        modified2 = {**base, "evidence_chain": ["step1", "step2", "step3"]}
        h3 = _record_hash(modified2)
        assert h1 != h3


class TestDedupPreservesDistinctRoutes:
    """Same STF target with different bridge_link_basis → both preserved."""

    def test_dedup_preserves_distinct_routes(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        out_dir = tmp_path / "output"

        _setup_sanctions(
            cgu_dir,
            [
                {
                    "sanction_id": "S070",
                    "sanction_source": "ceis",
                    "entity_name": "DUAL PATH CORP",
                    "entity_cnpj_cpf": SANCTION_CNPJ,
                    "sanction_type": "Inidoneidade",
                },
            ],
        )
        # Path A: CNPJ → company directly → co-partner JOAO
        # Path B: CNPJ is PJ partner in another company → co-partner JOAO there too
        _setup_rfb(
            rfb_dir,
            partners=[
                # Co-partner at the direct company (path A)
                {
                    "cnpj_basico": SANCTION_CNPJ_BASICO,
                    "partner_name": PARTY_NAME,
                    "partner_name_normalized": PARTY_NAME,
                    "partner_cpf_cnpj": PARTY_CPF,
                    "qualification_code": "49",
                },
                # Sanctioned CNPJ is partner at another company (path B)
                {
                    "cnpj_basico": OTHER_COMPANY_CNPJ_BASICO,
                    "partner_name": "DUAL PATH CORP",
                    "partner_cpf_cnpj": SANCTION_CNPJ,
                    "qualification_code": "22",
                },
                # Co-partner at that other company (path B)
                {
                    "cnpj_basico": OTHER_COMPANY_CNPJ_BASICO,
                    "partner_name": PARTY_NAME,
                    "partner_name_normalized": PARTY_NAME,
                    "partner_cpf_cnpj": PARTY_CPF,
                    "qualification_code": "49",
                },
            ],
            companies=[
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "razao_social": "DUAL PATH LTDA"},
                {"cnpj_basico": OTHER_COMPANY_CNPJ_BASICO, "razao_social": "OUTRA EMPRESA"},
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
        bases = {r["bridge_link_basis"] for r in records}
        # Both routes should be preserved
        assert "exact_cnpj_basico" in bases
        assert "exact_partner_cnpj" in bases
