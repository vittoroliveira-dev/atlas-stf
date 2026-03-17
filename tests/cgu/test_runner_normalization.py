"""Tests for cgu/_runner.py — normalization unit tests."""

from __future__ import annotations

from pathlib import Path

from atlas_stf.cgu._runner import (
    _canonicalize_tipo_pessoa,
    _load_csv_sanctions,
    _load_leniencia_csv,
    _looks_like_entity,
    _normalize_csv_record,
    _normalize_date,
    _normalize_leniencia_record,
)
from tests.cgu._runner_helpers import _make_ceis_csv, _make_leniencia_csv


class TestLooksLikeEntity:
    def test_ltda(self) -> None:
        assert _looks_like_entity("ACME LTDA")

    def test_sa(self) -> None:
        assert _looks_like_entity("BRASKEM S.A.")

    def test_engenharia(self) -> None:
        assert _looks_like_entity("ANDRADE GUTIERREZ ENGENHARIA S/A")

    def test_person_name(self) -> None:
        assert not _looks_like_entity("JOAO DA SILVA")

    def test_outro_as(self) -> None:
        # "E OUTRO(A/S)" should not trigger S/A match
        assert not _looks_like_entity("JOAO DA SILVA E OUTRO(A/S)")


class TestNormalizeCsvRecord:
    def test_ceis_record(self) -> None:
        row = [
            "CEIS",
            "12345",
            "J",
            "12.345.678/0001-00",
            "ACME CORP",
            "ACME",
            "ACME LTDA",
            "",
            "000123",
            "Suspensão",
            "01/01/2020",
            "01/01/2025",
            "01/01/2020",
            "DOU",
            "",
            "",
            "",
            "CGU",
        ]
        from atlas_stf.cgu._runner import _CEIS_COL

        result = _normalize_csv_record(row, "ceis", _CEIS_COL)
        assert result["sanction_source"] == "ceis"
        assert result["sanction_id"] == "12345"
        assert result["entity_name"] == "ACME CORP"
        assert result["entity_cnpj_cpf"] == "12345678000100"
        assert result["entity_cnpj_cpf_raw"] == "12.345.678/0001-00"
        assert result["entity_type_pf_pj"] == "PJ"
        assert result["entity_type_pf_pj_raw"] == "J"
        assert result["sanctioning_body"] == "CGU"
        assert result["sanction_type"] == "Suspensão"
        assert result["sanction_start_date"] == "2020-01-01"
        assert result["sanction_start_date_raw"] == "01/01/2020"
        assert result["sanction_end_date"] == "2025-01-01"
        assert result["sanction_end_date_raw"] == "01/01/2025"


class TestNormalizeLenienciaRecord:
    def test_basic_record(self) -> None:
        # Real CSV structure: ID, CNPJ, RAZAO SOCIAL, FANTASIA, DATA INICIO,
        # DATA FIM, SITUACAO, DATA INFO, NUM PROCESSO, TERMOS, ORGAO
        row = [
            "100001",
            "12.345.678/0001-00",
            "ACME CONSTRUTORA LTDA",
            "ACME",
            "01/01/2018",
            "01/01/2023",
            "Cumprido",
            "",
            "08012.000123/2014-56",
            "Termos do acordo",
            "CGU",
        ]
        result = _normalize_leniencia_record(row)
        assert result["sanction_source"] == "leniencia"
        assert result["entity_name"] == "ACME CONSTRUTORA LTDA"
        assert result["entity_cnpj_cpf"] == "12345678000100"
        assert result["entity_cnpj_cpf_raw"] == "12.345.678/0001-00"
        assert result["entity_type_pf_pj"] == "PJ"
        assert result["sanction_id"] == "08012.000123/2014-56"
        assert result["sanctioning_body"] == "CGU"
        assert result["sanction_type"] == "Cumprido"
        assert result["sanction_start_date"] == "2018-01-01"
        assert result["sanction_end_date"] == "2023-01-01"

    def test_name_fallback_to_fantasia(self) -> None:
        row = [
            "100002",
            "12.345.678/0001-00",
            "",  # RAZAO SOCIAL empty
            "NOME FANTASIA LTDA",
            "01/01/2020",
            "",
            "Em Execução",
            "",
            "PROC-001",
            "",
            "CGU",
        ]
        result = _normalize_leniencia_record(row)
        assert result["entity_name"] == "NOME FANTASIA LTDA"


class TestLoadLenienciaCsv:
    def test_loads_records(self, tmp_path: Path) -> None:
        csv_content = _make_leniencia_csv(
            [
                [
                    "100001",
                    "12.345.678/0001-00",
                    "ACME CONSTRUTORA",
                    "ACME",
                    "01/01/2018",
                    "",
                    "Cumprido",
                    "",
                    "PROC-001",
                    "Termos",
                    "CGU",
                ],
                [
                    "100002",
                    "98.765.432/0001-00",
                    "",  # empty RAZAO SOCIAL
                    "XYZ ENGENHARIA",
                    "01/06/2019",
                    "31/12/2024",
                    "Em Execução",
                    "",
                    "PROC-002",
                    "",
                    "CGU",
                ],
            ]
        )
        csv_path = tmp_path / "acordos-leniencia.csv"
        csv_path.write_text(csv_content, encoding="latin-1")
        records = _load_leniencia_csv(csv_path)
        assert len(records) == 2
        assert records[0]["sanction_source"] == "leniencia"
        assert records[0]["entity_name"] == "ACME CONSTRUTORA"
        # Second record uses NOME FANTASIA fallback
        assert records[1]["entity_name"] == "XYZ ENGENHARIA"

    def test_skips_empty_name(self, tmp_path: Path) -> None:
        csv_content = _make_leniencia_csv([["100003", "00.000.000/0001-00", "", "", "", "", "", "", "", "", ""]])
        csv_path = tmp_path / "acordos-leniencia.csv"
        csv_path.write_text(csv_content, encoding="latin-1")
        records = _load_leniencia_csv(csv_path)
        assert len(records) == 0


class TestLoadCsvSanctions:
    def test_loads_ceis(self, tmp_path: Path) -> None:
        csv_content = _make_ceis_csv(
            [
                [
                    "CEIS",
                    "100",
                    "J",
                    "12.345/0001-00",
                    "ACME CORP",
                    "ACME",
                    "",
                    "",
                    "",
                    "Suspensão",
                    "01/01/2020",
                    "01/01/2025",
                    "",
                    "DOU",
                    "",
                    "",
                    "",
                    "CGU",
                ],
                [
                    "CEIS",
                    "101",
                    "J",
                    "98.765/0001-00",
                    "XYZ LTDA",
                    "XYZ",
                    "",
                    "",
                    "",
                    "Inidoneidade",
                    "01/06/2021",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "TCU",
                ],
            ]
        )
        csv_path = tmp_path / "ceis.csv"
        csv_path.write_text(csv_content, encoding="latin-1")
        records = _load_csv_sanctions(csv_path, "ceis")
        assert len(records) == 2
        assert records[0]["entity_name"] == "ACME CORP"
        assert records[1]["sanction_type"] == "Inidoneidade"


class TestNormalizeTaxId:
    def test_normalize_cpf_formatted(self) -> None:
        from atlas_stf.core.identity import normalize_tax_id

        assert normalize_tax_id("123.456.789-00") == "12345678900"

    def test_normalize_cnpj_formatted(self) -> None:
        from atlas_stf.core.identity import normalize_tax_id

        assert normalize_tax_id("12.345.678/0001-90") == "12345678000190"

    def test_normalize_cpf_raw(self) -> None:
        from atlas_stf.core.identity import normalize_tax_id

        assert normalize_tax_id("12345678900") == "12345678900"

    def test_normalize_empty(self) -> None:
        from atlas_stf.core.identity import normalize_tax_id

        assert normalize_tax_id("") is None


class TestCanonicalizeTipoPessoa:
    def test_pf(self) -> None:
        assert _canonicalize_tipo_pessoa("PF") == "PF"

    def test_pj(self) -> None:
        assert _canonicalize_tipo_pessoa("PJ") == "PJ"

    def test_f(self) -> None:
        assert _canonicalize_tipo_pessoa("F") == "PF"

    def test_j(self) -> None:
        assert _canonicalize_tipo_pessoa("J") == "PJ"

    def test_unknown(self) -> None:
        assert _canonicalize_tipo_pessoa("X") == ""

    def test_empty(self) -> None:
        assert _canonicalize_tipo_pessoa("") == ""


class TestNormalizeDateCgu:
    def test_ddmmyyyy(self) -> None:
        assert _normalize_date("25/03/2023") == "2023-03-25"

    def test_already_iso(self) -> None:
        assert _normalize_date("2023-03-25") == "2023-03-25"

    def test_empty(self) -> None:
        assert _normalize_date("") is None

    def test_partial(self) -> None:
        assert _normalize_date("03/2023") is None

    def test_invalid(self) -> None:
        assert _normalize_date("abc") is None


class TestPreserveRawFields:
    def test_preserve_raw_tipo_pessoa(self) -> None:
        row = [
            "CEIS",
            "12345",
            "F",
            "123.456.789-00",
            "JOAO",
            "ORG",
            "RAZAO",
            "",
            "PROC",
            "Cat",
            "01/01/2020",
            "01/01/2025",
            "",
            "",
            "",
            "",
            "",
            "CGU",
        ]
        from atlas_stf.cgu._runner import _CEIS_COL

        result = _normalize_csv_record(row, "ceis", _CEIS_COL)
        assert result["entity_type_pf_pj"] == "PF"
        assert result["entity_type_pf_pj_raw"] == "F"
        assert result["entity_cnpj_cpf"] == "12345678900"
        assert result["entity_cnpj_cpf_raw"] == "123.456.789-00"
