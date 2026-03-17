"""Tests for donor_corporate_link — resolution paths (A/B/C) + unresolved + identity key."""

from __future__ import annotations

from pathlib import Path

from atlas_stf.analytics._donor_identity import donor_identity_key
from atlas_stf.analytics.donation_match import _donor_identity_key
from atlas_stf.analytics.donor_corporate_link import build_donor_corporate_links
from tests.analytics._donor_corporate_link_helpers import (
    _read_jsonl,
    _setup_donations,
    _setup_rfb,
)


class TestPjCnpjResolvedViaCnpjBasico:
    def test_pj_cnpj_resolved(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        rfb_dir = tmp_path / "rfb"
        out_dir = tmp_path / "analytics"

        # Valid CNPJ: 11.222.333/0001-81 (valid checksum)
        _setup_donations(
            tse_dir,
            [{"donor_name_normalized": "EMPRESA ABC", "donor_cpf_cnpj": "11222333000181"}],
        )
        _setup_rfb(
            rfb_dir,
            partners=[],
            companies=[
                {
                    "cnpj_basico": "11222333",
                    "razao_social": "EMPRESA ABC LTDA",
                    "natureza_juridica": "2062",
                    "capital_social": 100000.0,
                }
            ],
            establishments=[
                {
                    "cnpj_basico": "11222333",
                    "cnpj_ordem": "0001",
                    "cnpj_dv": "81",
                    "cnpj_full": "11222333000181",
                    "matriz_filial": "1",
                    "uf": "SP",
                }
            ],
        )

        build_donor_corporate_links(tse_dir=tse_dir, rfb_dir=rfb_dir, output_dir=out_dir)

        records = _read_jsonl(out_dir / "donor_corporate_link.jsonl")
        assert len(records) >= 1
        exact = [r for r in records if r["link_basis"] == "exact_cnpj_basico"]
        assert len(exact) == 1
        assert exact[0]["company_name"] == "EMPRESA ABC LTDA"
        assert exact[0]["confidence"] == "deterministic"
        assert exact[0]["donor_tax_id_valid"] is True


class TestPjCnpjResolvedViaPartner:
    def test_pj_as_partner(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        rfb_dir = tmp_path / "rfb"
        out_dir = tmp_path / "analytics"

        _setup_donations(
            tse_dir,
            [{"donor_name_normalized": "EMPRESA PJ", "donor_cpf_cnpj": "11222333000181"}],
        )
        _setup_rfb(
            rfb_dir,
            partners=[
                {
                    "cnpj_basico": "99887766",
                    "partner_name": "EMPRESA PJ",
                    "partner_cpf_cnpj": "11222333000181",
                    "qualification_code": "22",
                }
            ],
            companies=[
                {
                    "cnpj_basico": "99887766",
                    "razao_social": "HOLDING XYZ",
                    "natureza_juridica": "2062",
                    "capital_social": 500000.0,
                }
            ],
            establishments=[],
        )

        build_donor_corporate_links(tse_dir=tse_dir, rfb_dir=rfb_dir, output_dir=out_dir)

        records = _read_jsonl(out_dir / "donor_corporate_link.jsonl")
        partner_links = [r for r in records if r["link_basis"] == "exact_partner_cnpj"]
        assert len(partner_links) == 1
        assert partner_links[0]["company_name"] == "HOLDING XYZ"
        assert partner_links[0]["confidence"] == "deterministic"


class TestPfCpfResolvedViaQsa:
    def test_pf_cpf_resolved(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        rfb_dir = tmp_path / "rfb"
        out_dir = tmp_path / "analytics"

        # Valid CPF: 529.982.247-25
        _setup_donations(
            tse_dir,
            [{"donor_name_normalized": "JOAO SILVA", "donor_cpf_cnpj": "52998224725"}],
        )
        _setup_rfb(
            rfb_dir,
            partners=[
                {
                    "cnpj_basico": "44556677",
                    "partner_name": "JOAO SILVA",
                    "partner_cpf_cnpj": "52998224725",
                    "qualification_code": "49",
                    "qualification_label": "Socio-Administrador",
                }
            ],
            companies=[
                {
                    "cnpj_basico": "44556677",
                    "razao_social": "SILVA E FILHOS LTDA",
                    "natureza_juridica": "2062",
                    "capital_social": 50000.0,
                }
            ],
            establishments=[],
        )

        build_donor_corporate_links(tse_dir=tse_dir, rfb_dir=rfb_dir, output_dir=out_dir)

        records = _read_jsonl(out_dir / "donor_corporate_link.jsonl")
        cpf_links = [r for r in records if r["link_basis"] == "exact_partner_cpf"]
        assert len(cpf_links) == 1
        assert cpf_links[0]["company_name"] == "SILVA E FILHOS LTDA"
        assert cpf_links[0]["partner_name"] == "JOAO SILVA"
        assert cpf_links[0]["confidence"] == "deterministic"


class TestMaskedCpfEmitsUnresolved:
    def test_masked_cpf(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        rfb_dir = tmp_path / "rfb"
        out_dir = tmp_path / "analytics"

        _setup_donations(
            tse_dir,
            [{"donor_name_normalized": "FULANO MASCARADO", "donor_cpf_cnpj": "***.982.247-**"}],
        )
        _setup_rfb(rfb_dir, partners=[], companies=[], establishments=[])

        build_donor_corporate_links(tse_dir=tse_dir, rfb_dir=rfb_dir, output_dir=out_dir)

        records = _read_jsonl(out_dir / "donor_corporate_link.jsonl")
        assert len(records) == 1
        assert records[0]["link_basis"] == "masked_cpf"
        assert records[0]["confidence"] == "unresolved"
        assert records[0]["company_cnpj_basico"] is None


class TestEmptyDocumentEmitsUnresolved:
    def test_empty_doc(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        rfb_dir = tmp_path / "rfb"
        out_dir = tmp_path / "analytics"

        _setup_donations(
            tse_dir,
            [{"donor_name_normalized": "FULANO SEM DOC", "donor_cpf_cnpj": ""}],
        )
        _setup_rfb(rfb_dir, partners=[], companies=[], establishments=[])

        build_donor_corporate_links(tse_dir=tse_dir, rfb_dir=rfb_dir, output_dir=out_dir)

        records = _read_jsonl(out_dir / "donor_corporate_link.jsonl")
        assert len(records) == 1
        assert records[0]["link_basis"] == "missing_document"
        assert records[0]["confidence"] == "unresolved"


class TestInvalidChecksumEmitsUnresolved:
    def test_invalid_checksum(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        rfb_dir = tmp_path / "rfb"
        out_dir = tmp_path / "analytics"

        # Invalid CPF (bad checksum)
        _setup_donations(
            tse_dir,
            [{"donor_name_normalized": "FULANO INVALIDO", "donor_cpf_cnpj": "12345678900"}],
        )
        _setup_rfb(rfb_dir, partners=[], companies=[], establishments=[])

        build_donor_corporate_links(tse_dir=tse_dir, rfb_dir=rfb_dir, output_dir=out_dir)

        records = _read_jsonl(out_dir / "donor_corporate_link.jsonl")
        assert len(records) == 1
        assert records[0]["link_basis"] == "invalid_document"
        assert records[0]["donor_tax_id_valid"] is False
        assert records[0]["confidence"] == "unresolved"


class TestValidCpfNotInRfb:
    def test_valid_cpf_not_in_rfb(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        rfb_dir = tmp_path / "rfb"
        out_dir = tmp_path / "analytics"

        _setup_donations(
            tse_dir,
            [{"donor_name_normalized": "JOAO AUSENTE", "donor_cpf_cnpj": "52998224725"}],
        )
        _setup_rfb(rfb_dir, partners=[], companies=[], establishments=[])

        build_donor_corporate_links(tse_dir=tse_dir, rfb_dir=rfb_dir, output_dir=out_dir)

        records = _read_jsonl(out_dir / "donor_corporate_link.jsonl")
        assert len(records) == 1
        assert records[0]["link_basis"] == "not_in_rfb_corpus"
        assert records[0]["donor_tax_id_valid"] is True
        assert records[0]["confidence"] == "low"


class TestEveryDonorEmitsAtLeastOneRecord:
    def test_invariant(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        rfb_dir = tmp_path / "rfb"
        out_dir = tmp_path / "analytics"

        _setup_donations(
            tse_dir,
            [
                {"donor_name_normalized": "DONOR A", "donor_cpf_cnpj": "52998224725"},
                {"donor_name_normalized": "DONOR B", "donor_cpf_cnpj": ""},
                {"donor_name_normalized": "DONOR C", "donor_cpf_cnpj": "***.111.222-**"},
            ],
        )
        _setup_rfb(rfb_dir, partners=[], companies=[], establishments=[])

        build_donor_corporate_links(tse_dir=tse_dir, rfb_dir=rfb_dir, output_dir=out_dir)

        records = _read_jsonl(out_dir / "donor_corporate_link.jsonl")
        donor_keys = {r["donor_identity_key"] for r in records}
        assert len(donor_keys) == 3
        assert len(records) >= 3


class TestDonorIdentityKeySharedWithDonationMatch:
    def test_shared_key(self) -> None:
        """donor_identity_key from _donor_identity matches _donor_identity_key from donation_match."""
        assert donor_identity_key("JOSE", "52998224725") == _donor_identity_key("JOSE", "52998224725")
        assert donor_identity_key("FULANO", "") == _donor_identity_key("FULANO", "")
        assert donor_identity_key("MASKED", "***.111.222-**") == _donor_identity_key("MASKED", "***.111.222-**")
