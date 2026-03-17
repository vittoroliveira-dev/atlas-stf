"""Tests for donor_corporate_link — multi-company, dedup, summary, HQ, graceful degradation."""

from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics.donor_corporate_link import build_donor_corporate_links
from tests.analytics._donor_corporate_link_helpers import (
    _read_jsonl,
    _setup_donations,
    _setup_rfb,
)


class TestMultipleCompaniesPerDonor:
    def test_multiple_links(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        rfb_dir = tmp_path / "rfb"
        out_dir = tmp_path / "analytics"

        _setup_donations(
            tse_dir,
            [{"donor_name_normalized": "JOAO MULTIPLO", "donor_cpf_cnpj": "52998224725"}],
        )
        _setup_rfb(
            rfb_dir,
            partners=[
                {
                    "cnpj_basico": "11111111",
                    "partner_name": "JOAO MULTIPLO",
                    "partner_cpf_cnpj": "52998224725",
                    "qualification_code": "49",
                },
                {
                    "cnpj_basico": "22222222",
                    "partner_name": "JOAO MULTIPLO",
                    "partner_cpf_cnpj": "52998224725",
                    "qualification_code": "22",
                },
            ],
            companies=[
                {
                    "cnpj_basico": "11111111",
                    "razao_social": "EMPRESA UM",
                    "natureza_juridica": "2062",
                    "capital_social": 10000.0,
                },
                {
                    "cnpj_basico": "22222222",
                    "razao_social": "EMPRESA DOIS",
                    "natureza_juridica": "2062",
                    "capital_social": 20000.0,
                },
            ],
            establishments=[],
        )

        build_donor_corporate_links(tse_dir=tse_dir, rfb_dir=rfb_dir, output_dir=out_dir)

        records = _read_jsonl(out_dir / "donor_corporate_link.jsonl")
        cpf_links = [r for r in records if r["link_basis"] == "exact_partner_cpf"]
        assert len(cpf_links) == 2
        companies = {r["company_name"] for r in cpf_links}
        assert companies == {"EMPRESA UM", "EMPRESA DOIS"}


class TestPjBothPaths:
    def test_both_empresa_and_socia(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        rfb_dir = tmp_path / "rfb"
        out_dir = tmp_path / "analytics"

        _setup_donations(
            tse_dir,
            [{"donor_name_normalized": "EMPRESA DUAL", "donor_cpf_cnpj": "11222333000181"}],
        )
        _setup_rfb(
            rfb_dir,
            partners=[
                {
                    "cnpj_basico": "99887766",
                    "partner_name": "EMPRESA DUAL",
                    "partner_cpf_cnpj": "11222333000181",
                    "qualification_code": "22",
                }
            ],
            companies=[
                {
                    "cnpj_basico": "11222333",
                    "razao_social": "EMPRESA DUAL LTDA",
                    "natureza_juridica": "2062",
                    "capital_social": 100000.0,
                },
                {
                    "cnpj_basico": "99887766",
                    "razao_social": "HOLDING MASTER",
                    "natureza_juridica": "2062",
                    "capital_social": 500000.0,
                },
            ],
            establishments=[],
        )

        build_donor_corporate_links(tse_dir=tse_dir, rfb_dir=rfb_dir, output_dir=out_dir)

        records = _read_jsonl(out_dir / "donor_corporate_link.jsonl")
        bases = {r["link_basis"] for r in records}
        assert "exact_cnpj_basico" in bases
        assert "exact_partner_cnpj" in bases
        assert len(records) == 2


class TestNoRfbDataGraceful:
    def test_no_rfb_files(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        rfb_dir = tmp_path / "rfb"
        rfb_dir.mkdir(parents=True)
        out_dir = tmp_path / "analytics"

        _setup_donations(
            tse_dir,
            [{"donor_name_normalized": "DONOR X", "donor_cpf_cnpj": "52998224725"}],
        )

        build_donor_corporate_links(tse_dir=tse_dir, rfb_dir=rfb_dir, output_dir=out_dir)

        records = _read_jsonl(out_dir / "donor_corporate_link.jsonl")
        assert len(records) == 1
        assert records[0]["link_basis"] == "not_in_rfb_corpus"


class TestDeduplication:
    def test_same_donor_same_company(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        rfb_dir = tmp_path / "rfb"
        out_dir = tmp_path / "analytics"

        # Same donor appears twice in donations_raw
        _setup_donations(
            tse_dir,
            [
                {"donor_name_normalized": "JOAO DEDUP", "donor_cpf_cnpj": "52998224725"},
                {"donor_name_normalized": "JOAO DEDUP", "donor_cpf_cnpj": "52998224725"},
            ],
        )
        _setup_rfb(
            rfb_dir,
            partners=[
                {
                    "cnpj_basico": "11111111",
                    "partner_name": "JOAO DEDUP",
                    "partner_cpf_cnpj": "52998224725",
                    "qualification_code": "49",
                }
            ],
            companies=[
                {
                    "cnpj_basico": "11111111",
                    "razao_social": "DEDUP CORP",
                    "natureza_juridica": "2062",
                    "capital_social": 10000.0,
                }
            ],
            establishments=[],
        )

        build_donor_corporate_links(tse_dir=tse_dir, rfb_dir=rfb_dir, output_dir=out_dir)

        records = _read_jsonl(out_dir / "donor_corporate_link.jsonl")
        assert len(records) == 1  # deduplicated


class TestSummaryCountsByLinkBasisAndConfidence:
    def test_summary(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        rfb_dir = tmp_path / "rfb"
        out_dir = tmp_path / "analytics"

        _setup_donations(
            tse_dir,
            [
                {"donor_name_normalized": "RESOLVIDO", "donor_cpf_cnpj": "52998224725"},
                {"donor_name_normalized": "MASCARADO", "donor_cpf_cnpj": "***.111.222-**"},
            ],
        )
        _setup_rfb(
            rfb_dir,
            partners=[
                {
                    "cnpj_basico": "44556677",
                    "partner_name": "RESOLVIDO",
                    "partner_cpf_cnpj": "52998224725",
                    "qualification_code": "49",
                }
            ],
            companies=[
                {
                    "cnpj_basico": "44556677",
                    "razao_social": "EMPRESA OK",
                    "natureza_juridica": "2062",
                    "capital_social": 10000.0,
                }
            ],
            establishments=[],
        )

        build_donor_corporate_links(tse_dir=tse_dir, rfb_dir=rfb_dir, output_dir=out_dir)

        summary = json.loads((out_dir / "donor_corporate_link_summary.json").read_text())
        assert summary["total_donors"] == 2
        assert summary["total_output_records"] == 2
        assert summary["resolved_record_count"] == 1
        assert summary["unresolved_record_count"] == 1
        assert "exact_partner_cpf" in summary["counts_by_link_basis"]
        assert "masked_cpf" in summary["counts_by_link_basis"]
        assert "deterministic" in summary["counts_by_confidence"]
        assert "unresolved" in summary["counts_by_confidence"]


class TestHeadquartersSelected:
    def test_matriz_preferred(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        rfb_dir = tmp_path / "rfb"
        out_dir = tmp_path / "analytics"

        _setup_donations(
            tse_dir,
            [{"donor_name_normalized": "EMPRESA HQ", "donor_cpf_cnpj": "11222333000181"}],
        )
        _setup_rfb(
            rfb_dir,
            partners=[],
            companies=[
                {
                    "cnpj_basico": "11222333",
                    "razao_social": "HQ CORP",
                    "natureza_juridica": "2062",
                    "capital_social": 100000.0,
                }
            ],
            establishments=[
                {
                    "cnpj_basico": "11222333",
                    "cnpj_ordem": "0002",
                    "cnpj_dv": "62",
                    "cnpj_full": "11222333000262",
                    "matriz_filial": "2",
                    "uf": "RJ",
                },
                {
                    "cnpj_basico": "11222333",
                    "cnpj_ordem": "0001",
                    "cnpj_dv": "81",
                    "cnpj_full": "11222333000181",
                    "matriz_filial": "1",
                    "uf": "SP",
                },
            ],
        )

        build_donor_corporate_links(tse_dir=tse_dir, rfb_dir=rfb_dir, output_dir=out_dir)

        records = _read_jsonl(out_dir / "donor_corporate_link.jsonl")
        assert len(records) == 1
        assert records[0]["establishment_cnpj_full"] == "11222333000181"
        assert records[0]["establishment_uf"] == "SP"
