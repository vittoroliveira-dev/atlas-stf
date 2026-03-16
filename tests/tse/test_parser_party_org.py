"""Tests for tse/_parser_party_org.py."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from atlas_stf.tse._parser_party_org import (
    iter_despesas_csv,
    iter_receitas_csv,
    normalize_party_org_record,
)

_RECEITAS_HEADER = ";".join(
    [
        '"DT_GERACAO"',
        '"HH_GERACAO"',
        '"AA_ELEICAO"',
        '"CD_TIPO_ELEICAO"',
        '"NM_TIPO_ELEICAO"',
        '"TP_PRESTACAO_CONTAS"',
        '"DT_PRESTACAO_CONTAS"',
        '"SQ_PRESTADOR_CONTAS"',
        '"CD_ESFERA_PARTIDARIA"',
        '"DS_ESFERA_PARTIDARIA"',
        '"SG_UF"',
        '"CD_MUNICIPIO"',
        '"NM_MUNICIPIO"',
        '"NR_CNPJ_PRESTADOR_CONTA"',
        '"NR_PARTIDO"',
        '"SG_PARTIDO"',
        '"NM_PARTIDO"',
        '"CD_FONTE_RECEITA"',
        '"DS_FONTE_RECEITA"',
        '"CD_ORIGEM_RECEITA"',
        '"DS_ORIGEM_RECEITA"',
        '"CD_NATUREZA_RECEITA"',
        '"DS_NATUREZA_RECEITA"',
        '"CD_ESPECIE_RECEITA"',
        '"DS_ESPECIE_RECEITA"',
        '"CD_CNAE_DOADOR"',
        '"DS_CNAE_DOADOR"',
        '"NR_CPF_CNPJ_DOADOR"',
        '"NM_DOADOR"',
        '"NM_DOADOR_RFB"',
        '"CD_ESFERA_PARTIDARIA_DOADOR"',
        '"DS_ESFERA_PARTIDARIA_DOADOR"',
        '"SG_UF_DOADOR"',
        '"CD_MUNICIPIO_DOADOR"',
        '"NM_MUNICIPIO_DOADOR"',
        '"SQ_CANDIDATO_DOADOR"',
        '"NR_CANDIDATO_DOADOR"',
        '"CD_CARGO_CANDIDATO_DOADOR"',
        '"DS_CARGO_CANDIDATO_DOADOR"',
        '"NR_PARTIDO_DOADOR"',
        '"SG_PARTIDO_DOADOR"',
        '"NM_PARTIDO_DOADOR"',
        '"NR_RECIBO_DOACAO"',
        '"NR_DOCUMENTO_DOACAO"',
        '"SQ_RECEITA"',
        '"DT_RECEITA"',
        '"DS_RECEITA"',
        '"VR_RECEITA"',
    ]
)

_DESPESAS_HEADER = ";".join(
    [
        '"DT_GERACAO"',
        '"HH_GERACAO"',
        '"AA_ELEICAO"',
        '"CD_TIPO_ELEICAO"',
        '"NM_TIPO_ELEICAO"',
        '"TP_PRESTACAO_CONTAS"',
        '"DT_PRESTACAO_CONTAS"',
        '"SQ_PRESTADOR_CONTAS"',
        '"CD_ESFERA_PARTIDARIA"',
        '"DS_ESFERA_PARTIDARIA"',
        '"SG_UF"',
        '"SG_UE"',
        '"NM_UE"',
        '"CD_MUNICIPIO"',
        '"NM_MUNICIPIO"',
        '"NR_CNPJ_PRESTADOR_CONTA"',
        '"NR_PARTIDO"',
        '"SG_PARTIDO"',
        '"NM_PARTIDO"',
        '"CD_TIPO_FORNECEDOR"',
        '"DS_TIPO_FORNECEDOR"',
        '"CD_CNAE_FORNECEDOR"',
        '"DS_CNAE_FORNECEDOR"',
        '"NR_CPF_CNPJ_FORNECEDOR"',
        '"NM_FORNECEDOR"',
        '"NM_FORNECEDOR_RFB"',
        '"CD_ESFERA_PART_FORNECEDOR"',
        '"DS_ESFERA_PART_FORNECEDOR"',
        '"SG_UF_FORNECEDOR"',
        '"CD_MUNICIPIO_FORNECEDOR"',
        '"NM_MUNICIPIO_FORNECEDOR"',
        '"SQ_CANDIDATO_FORNECEDOR"',
        '"NR_CANDIDATO_FORNECEDOR"',
        '"CD_CARGO_FORNECEDOR"',
        '"DS_CARGO_FORNECEDOR"',
        '"NR_PARTIDO_FORNECEDOR"',
        '"SG_PARTIDO_FORNECEDOR"',
        '"NM_PARTIDO_FORNECEDOR"',
        '"DS_TIPO_DOCUMENTO"',
        '"NR_DOCUMENTO"',
        '"CD_ORIGEM_DESPESA"',
        '"DS_ORIGEM_DESPESA"',
        '"SQ_DESPESA"',
        '"DT_DESPESA"',
        '"DS_DESPESA"',
        '"VR_DESPESA_CONTRATADA"',
    ]
)


def _make_receitas_row(**overrides: str) -> str:
    defaults: dict[str, str] = {
        "DT_GERACAO": "01/01/2025",
        "HH_GERACAO": "10:00:00",
        "AA_ELEICAO": "2024",
        "CD_TIPO_ELEICAO": "1",
        "NM_TIPO_ELEICAO": "Eleicao Ordinaria",
        "TP_PRESTACAO_CONTAS": "Final",
        "DT_PRESTACAO_CONTAS": "01/03/2025",
        "SQ_PRESTADOR_CONTAS": "1001",
        "CD_ESFERA_PARTIDARIA": "M",
        "DS_ESFERA_PARTIDARIA": "Municipal",
        "SG_UF": "SP",
        "CD_MUNICIPIO": "71072",
        "NM_MUNICIPIO": "SAO PAULO",
        "NR_CNPJ_PRESTADOR_CONTA": "12345678000199",
        "NR_PARTIDO": "13",
        "SG_PARTIDO": "PT",
        "NM_PARTIDO": "PARTIDO DOS TRABALHADORES",
        "CD_FONTE_RECEITA": "1",
        "DS_FONTE_RECEITA": "Fundo Partidario",
        "CD_ORIGEM_RECEITA": "1",
        "DS_ORIGEM_RECEITA": "Recursos de pessoas fisicas",
        "CD_NATUREZA_RECEITA": "1",
        "DS_NATUREZA_RECEITA": "Doacao",
        "CD_ESPECIE_RECEITA": "1",
        "DS_ESPECIE_RECEITA": "Dinheiro",
        "CD_CNAE_DOADOR": "4110700",
        "DS_CNAE_DOADOR": "Incorporacao",
        "NR_CPF_CNPJ_DOADOR": "11122233344",
        "NM_DOADOR": "JOAO DA SILVA",
        "NM_DOADOR_RFB": "JOAO DA SILVA",
        "CD_ESFERA_PARTIDARIA_DOADOR": "",
        "DS_ESFERA_PARTIDARIA_DOADOR": "",
        "SG_UF_DOADOR": "SP",
        "CD_MUNICIPIO_DOADOR": "71072",
        "NM_MUNICIPIO_DOADOR": "SAO PAULO",
        "SQ_CANDIDATO_DOADOR": "",
        "NR_CANDIDATO_DOADOR": "",
        "CD_CARGO_CANDIDATO_DOADOR": "",
        "DS_CARGO_CANDIDATO_DOADOR": "",
        "NR_PARTIDO_DOADOR": "",
        "SG_PARTIDO_DOADOR": "",
        "NM_PARTIDO_DOADOR": "",
        "NR_RECIBO_DOACAO": "001",
        "NR_DOCUMENTO_DOACAO": "DOC001",
        "SQ_RECEITA": "10001",
        "DT_RECEITA": "15/08/2024",
        "DS_RECEITA": "Doacao em dinheiro",
        "VR_RECEITA": "5000,00",
    }
    defaults.update(overrides)
    return ";".join(f'"{v}"' for v in defaults.values())


def _make_despesas_row(**overrides: str) -> str:
    defaults: dict[str, str] = {
        "DT_GERACAO": "01/01/2025",
        "HH_GERACAO": "10:00:00",
        "AA_ELEICAO": "2024",
        "CD_TIPO_ELEICAO": "1",
        "NM_TIPO_ELEICAO": "Eleicao Ordinaria",
        "TP_PRESTACAO_CONTAS": "Final",
        "DT_PRESTACAO_CONTAS": "01/03/2025",
        "SQ_PRESTADOR_CONTAS": "2001",
        "CD_ESFERA_PARTIDARIA": "E",
        "DS_ESFERA_PARTIDARIA": "Estadual",
        "SG_UF": "RJ",
        "SG_UE": "RJ",
        "NM_UE": "RIO DE JANEIRO",
        "CD_MUNICIPIO": "",
        "NM_MUNICIPIO": "",
        "NR_CNPJ_PRESTADOR_CONTA": "98765432000188",
        "NR_PARTIDO": "45",
        "SG_PARTIDO": "PSDB",
        "NM_PARTIDO": "PARTIDO DA SOCIAL DEMOCRACIA BRASILEIRA",
        "CD_TIPO_FORNECEDOR": "1",
        "DS_TIPO_FORNECEDOR": "Pessoa Juridica",
        "CD_CNAE_FORNECEDOR": "1811301",
        "DS_CNAE_FORNECEDOR": "Impressao de jornais",
        "NR_CPF_CNPJ_FORNECEDOR": "55667788000111",
        "NM_FORNECEDOR": "GRAFICA RAPIDA LTDA",
        "NM_FORNECEDOR_RFB": "GRAFICA RAPIDA LTDA",
        "CD_ESFERA_PART_FORNECEDOR": "",
        "DS_ESFERA_PART_FORNECEDOR": "",
        "SG_UF_FORNECEDOR": "RJ",
        "CD_MUNICIPIO_FORNECEDOR": "60011",
        "NM_MUNICIPIO_FORNECEDOR": "RIO DE JANEIRO",
        "SQ_CANDIDATO_FORNECEDOR": "",
        "NR_CANDIDATO_FORNECEDOR": "",
        "CD_CARGO_FORNECEDOR": "",
        "DS_CARGO_FORNECEDOR": "",
        "NR_PARTIDO_FORNECEDOR": "",
        "SG_PARTIDO_FORNECEDOR": "",
        "NM_PARTIDO_FORNECEDOR": "",
        "DS_TIPO_DOCUMENTO": "Nota Fiscal",
        "NR_DOCUMENTO": "NF12345",
        "CD_ORIGEM_DESPESA": "1",
        "DS_ORIGEM_DESPESA": "Gastos eleitorais",
        "SQ_DESPESA": "20001",
        "DT_DESPESA": "20/09/2024",
        "DS_DESPESA": "Impressao de material grafico",
        "VR_DESPESA_CONTRATADA": "12500,50",
    }
    defaults.update(overrides)
    return ";".join(f'"{v}"' for v in defaults.values())


def _write_csv(tmp_path: Path, name: str, header: str, *rows: str) -> Path:
    path = tmp_path / name
    content = header + "\n" + "\n".join(rows) + "\n"
    path.write_text(content, encoding="utf-8")
    return path


class TestIterReceitasCsv:
    def test_parse_basic_revenue(self, tmp_path: Path) -> None:
        path = _write_csv(tmp_path, "receitas.csv", _RECEITAS_HEADER, _make_receitas_row())
        records = list(iter_receitas_csv(path))
        assert len(records) == 1
        r = records[0]
        assert r["record_kind"] == "revenue"
        assert r["counterparty_name"] == "JOAO DA SILVA"
        assert r["counterparty_tax_id"] == "11122233344"
        assert r["org_party_abbrev"] == "PT"
        assert r["amount_raw"] == "5000,00"
        assert r["date_raw"] == "15/08/2024"

    def test_multiple_rows(self, tmp_path: Path) -> None:
        row1 = _make_receitas_row(NM_DOADOR="DOADOR A", VR_RECEITA="1000,00")
        row2 = _make_receitas_row(NM_DOADOR="DOADOR B", VR_RECEITA="2000,00")
        path = _write_csv(tmp_path, "receitas.csv", _RECEITAS_HEADER, row1, row2)
        records = list(iter_receitas_csv(path))
        assert len(records) == 2
        assert records[0]["counterparty_name"] == "DOADOR A"
        assert records[1]["counterparty_name"] == "DOADOR B"

    def test_missing_counterparty_preserved(self, tmp_path: Path) -> None:
        """Records with empty counterparty should be preserved, not discarded."""
        row = _make_receitas_row(NM_DOADOR="", NR_CPF_CNPJ_DOADOR="", VR_RECEITA="1000,00")
        path = _write_csv(tmp_path, "receitas.csv", _RECEITAS_HEADER, row)
        records = list(iter_receitas_csv(path))
        assert len(records) == 1
        assert records[0]["counterparty_name"] == ""

    def test_structurally_invalid_discarded(self, tmp_path: Path) -> None:
        """Records with no amount, no counterparty, and no description should be discarded."""
        row = _make_receitas_row(NM_DOADOR="", VR_RECEITA="", DS_RECEITA="")
        path = _write_csv(tmp_path, "receitas.csv", _RECEITAS_HEADER, row)
        records = list(iter_receitas_csv(path))
        assert len(records) == 0


class TestIterDespesasCsv:
    def test_parse_basic_expense(self, tmp_path: Path) -> None:
        path = _write_csv(tmp_path, "despesas.csv", _DESPESAS_HEADER, _make_despesas_row())
        records = list(iter_despesas_csv(path))
        assert len(records) == 1
        r = records[0]
        assert r["record_kind"] == "expense"
        assert r["counterparty_name"] == "GRAFICA RAPIDA LTDA"
        assert r["counterparty_tax_id"] == "55667788000111"
        assert r["org_party_abbrev"] == "PSDB"
        assert r["amount_raw"] == "12500,50"
        assert r["date_raw"] == "20/09/2024"

    def test_missing_supplier_preserved(self, tmp_path: Path) -> None:
        row = _make_despesas_row(NM_FORNECEDOR="", NR_CPF_CNPJ_FORNECEDOR="", VR_DESPESA_CONTRATADA="500,00")
        path = _write_csv(tmp_path, "despesas.csv", _DESPESAS_HEADER, row)
        records = list(iter_despesas_csv(path))
        assert len(records) == 1
        assert records[0]["counterparty_name"] == ""


class TestNormalizePartyOrgRecord:
    def _raw(self, **overrides: Any) -> dict[str, Any]:
        base: dict[str, Any] = {
            "record_kind": "revenue",
            "election_year_raw": "2024",
            "state": "SP",
            "org_scope": "Municipal",
            "org_party_name": "PARTIDO DOS TRABALHADORES",
            "org_party_abbrev": "PT",
            "org_cnpj": "12345678000199",
            "counterparty_name": "JOAO DA SILVA",
            "counterparty_name_rfb": "JOAO DA SILVA",
            "counterparty_tax_id": "11122233344",
            "counterparty_cnae_code": "4110700",
            "counterparty_cnae_desc": "Incorporacao",
            "amount_raw": "5000,00",
            "date_raw": "15/08/2024",
            "description": "Doacao em dinheiro",
        }
        base.update(overrides)
        return base

    def test_normalize_revenue(self) -> None:
        normalized = normalize_party_org_record(self._raw(), 2024)
        assert normalized["record_kind"] == "revenue"
        assert normalized["actor_kind"] == "party_org"
        assert normalized["election_year"] == 2024
        assert normalized["counterparty_name"] == "JOAO DA SILVA"
        assert normalized["counterparty_name_normalized"] == "JOAO DA SILVA"
        assert normalized["transaction_amount"] == 5000.0
        assert normalized["transaction_date"] == "2024-08-15"

    def test_normalize_expense(self) -> None:
        raw = self._raw(
            record_kind="expense",
            counterparty_name="GRAFICA LTDA",
            counterparty_name_rfb="",
            amount_raw="12500,50",
            date_raw="20/09/2024",
        )
        normalized = normalize_party_org_record(raw, 2024)
        assert normalized["record_kind"] == "expense"
        assert normalized["actor_kind"] == "party_org"
        assert normalized["counterparty_name_normalized"] == "GRAFICA LTDA"
        assert normalized["transaction_amount"] == 12500.50
        assert normalized["transaction_date"] == "2024-09-20"

    def test_rfb_name_preferred_for_normalization(self) -> None:
        raw = self._raw(counterparty_name="joao silva", counterparty_name_rfb="JOAO DA SILVA LTDA")
        normalized = normalize_party_org_record(raw, 2024)
        assert normalized["counterparty_name_normalized"] == "JOAO DA SILVA LTDA"

    def test_empty_counterparty(self) -> None:
        raw = self._raw(counterparty_name="", counterparty_name_rfb="")
        normalized = normalize_party_org_record(raw, 2024)
        assert normalized["counterparty_name"] == ""
        assert normalized["counterparty_name_normalized"] == ""

    def test_amount_parsing_brazilian_format(self) -> None:
        raw = self._raw(amount_raw="1.234.567,89")
        normalized = normalize_party_org_record(raw, 2024)
        assert normalized["transaction_amount"] == 1234567.89

    def test_date_parsing_dd_mm_yyyy(self) -> None:
        raw = self._raw(date_raw="05/11/2022")
        normalized = normalize_party_org_record(raw, 2022)
        assert normalized["transaction_date"] == "2022-11-05"
