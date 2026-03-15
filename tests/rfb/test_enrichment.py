"""Tests for RFB record enrichment with decoded labels."""

from __future__ import annotations

from atlas_stf.rfb._enrichment import (
    enrich_company_record,
    enrich_establishment_record,
    enrich_partner_record,
)


class TestEnrichPartnerRecord:
    def test_adds_qualification_label(self) -> None:
        record = {"qualification_code": "49", "representative_qualification": "05"}
        qualificacoes = {"49": "Socio-Administrador", "05": "Administrador"}
        result = enrich_partner_record(record, qualificacoes)
        assert result["qualification_label"] == "Socio-Administrador"
        assert result["representative_qualification_label"] == "Administrador"

    def test_missing_code_returns_empty_string(self) -> None:
        record = {"qualification_code": "99", "representative_qualification": ""}
        qualificacoes = {"49": "Socio-Administrador"}
        result = enrich_partner_record(record, qualificacoes)
        assert result["qualification_label"] == ""
        assert result["representative_qualification_label"] == ""


class TestEnrichCompanyRecord:
    def test_adds_natureza_juridica_label(self) -> None:
        record = {"natureza_juridica": "2062"}
        naturezas = {"2062": "Sociedade Empresaria Limitada"}
        result = enrich_company_record(record, naturezas)
        assert result["natureza_juridica_label"] == "Sociedade Empresaria Limitada"


class TestEnrichEstablishmentRecord:
    def test_adds_cnae_municipio_motivo_labels(self) -> None:
        record = {
            "cnae_fiscal": "6911701",
            "municipio": "7107",
            "motivo_situacao_cadastral": "01",
            "cnae_fiscal_secundaria": ["6920601", "7020400"],
        }
        cnaes = {"6911701": "Servicos advocaticios", "6920601": "Contabilidade", "7020400": "Consultoria"}
        municipios = {"7107": "SAO PAULO"}
        motivos = {"01": "Extincao por encerramento"}
        result = enrich_establishment_record(record, cnaes, municipios, motivos)
        assert result["cnae_fiscal_label"] == "Servicos advocaticios"
        assert result["municipio_label"] == "SAO PAULO"
        assert result["motivo_situacao_label"] == "Extincao por encerramento"

    def test_decodes_secondary_cnaes(self) -> None:
        record = {
            "cnae_fiscal": "6911701",
            "municipio": "",
            "motivo_situacao_cadastral": "",
            "cnae_fiscal_secundaria": ["6920601", "7020400"],
        }
        cnaes = {"6911701": "Advocacia", "6920601": "Contabilidade", "7020400": "Consultoria"}
        result = enrich_establishment_record(record, cnaes, {}, {})
        assert result["cnae_secundaria_labels"] == ["Contabilidade", "Consultoria"]

    def test_empty_reference_tables_return_empty_strings(self) -> None:
        record = {
            "cnae_fiscal": "9999999",
            "municipio": "0000",
            "motivo_situacao_cadastral": "99",
            "cnae_fiscal_secundaria": [],
        }
        result = enrich_establishment_record(record, {}, {}, {})
        assert result["cnae_fiscal_label"] == ""
        assert result["municipio_label"] == ""
        assert result["motivo_situacao_label"] == ""
        assert result["cnae_secundaria_labels"] == []
