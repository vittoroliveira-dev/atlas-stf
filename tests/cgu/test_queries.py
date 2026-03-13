"""Tests for cgu/_queries.py."""

from __future__ import annotations

from atlas_stf.cgu._queries import (
    build_ceis_name_params,
    build_cnep_name_params,
    normalize_ceis_record,
    normalize_cnep_record,
)


class TestBuildParams:
    def test_ceis_name_params(self) -> None:
        params = build_ceis_name_params("ACME CORP", page=2)
        assert params["nomeSancionado"] == "ACME CORP"
        assert params["pagina"] == 2

    def test_cnep_name_params(self) -> None:
        params = build_cnep_name_params("XYZ LTDA")
        assert params["nomeSancionado"] == "XYZ LTDA"
        assert params["pagina"] == 1


class TestNormalize:
    def test_normalize_ceis_record(self) -> None:
        raw = {
            "id": 123,
            "sancionado": {"nome": "ACME CORP", "cnpjFormatado": "12.345.678/0001-00"},
            "orgaoSancionador": {"nome": "CGU"},
            "tipoSancao": {"descricaoResumida": "Inidoneidade"},
            "dataInicioSancao": "2020-01-01",
            "dataFimSancao": "2025-01-01",
            "textoPublicacao": "Publicado no DOU",
        }
        result = normalize_ceis_record(raw)
        assert result["sanction_source"] == "ceis"
        assert result["sanction_id"] == "123"
        assert result["entity_name"] == "ACME CORP"
        assert result["entity_cnpj_cpf"] == "12.345.678/0001-00"
        assert result["sanctioning_body"] == "CGU"
        assert result["sanction_type"] == "Inidoneidade"
        assert result["sanction_start_date"] == "2020-01-01"
        assert result["sanction_end_date"] == "2025-01-01"

    def test_normalize_cnep_record(self) -> None:
        raw = {
            "id": 456,
            "sancionado": {"nome": "XYZ LTDA", "cpfFormatado": "123.456.789-00"},
            "orgaoSancionador": {"nome": "TCU"},
            "tipoSancao": {"descricaoResumida": "Impedimento"},
            "dataInicioSancao": "2021-06-15",
            "dataFimSancao": "",
            "textoPublicacao": "",
        }
        result = normalize_cnep_record(raw)
        assert result["sanction_source"] == "cnep"
        assert result["entity_cnpj_cpf"] == "123.456.789-00"

    def test_normalize_empty_record(self) -> None:
        result = normalize_ceis_record({})
        assert result["sanction_source"] == "ceis"
        assert result["entity_name"] == ""
        assert result["sanctioning_body"] == ""

    def test_normalize_cnep_empty_record(self) -> None:
        result = normalize_cnep_record({})
        assert result["sanction_source"] == "cnep"
        assert result["entity_name"] == ""
