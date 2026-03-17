"""Tests for tse/_parser_expenses.py — parse/normalize, date, column resolution."""

from __future__ import annotations

from pathlib import Path

from atlas_stf.tse._parser_expenses import (
    _resolve_expense_column,
    normalize_expense_record,
    parse_despesas_csv,
)


def _make_gen6_csv(rows: list[dict[str, str]] | None = None) -> str:
    """2022+ despesas_contratadas format (essential columns only)."""
    header = ";".join(
        [
            '"AA_ELEICAO"',
            '"SG_UF"',
            '"DS_CARGO"',
            '"NM_CANDIDATO"',
            '"NR_CPF_CANDIDATO"',
            '"NR_CANDIDATO"',
            '"SG_PARTIDO"',
            '"NM_PARTIDO"',
            '"NR_CPF_CNPJ_FORNECEDOR"',
            '"NM_FORNECEDOR"',
            '"NM_FORNECEDOR_RFB"',
            '"CD_CNAE_FORNECEDOR"',
            '"DS_CNAE_FORNECEDOR"',
            '"SG_UF_FORNECEDOR"',
            '"VR_DESPESA_CONTRATADA"',
            '"DT_DESPESA"',
            '"DS_DESPESA"',
            '"DS_TIPO_DOCUMENTO"',
            '"NR_DOCUMENTO"',
            '"DS_ORIGEM_DESPESA"',
        ]
    )
    if rows is None:
        rows = [{}]
    lines = [header]
    for row in rows:
        vals = [
            row.get("year", "2022"),
            row.get("state", "SP"),
            row.get("position", "PREFEITO"),
            row.get("candidate", "FULANO DE TAL"),
            row.get("candidate_cpf", "12345678901"),
            row.get("candidate_number", "45"),
            row.get("party_abbrev", "PT"),
            row.get("party_name", "PARTIDO DOS TRABALHADORES"),
            row.get("supplier_tax_id", "98765432000111"),
            row.get("supplier_name", "GRAFICA ABC LTDA"),
            row.get("supplier_name_rfb", "GRAFICA ABC LTDA ME"),
            row.get("cnae_code", "1813001"),
            row.get("cnae_desc", "Impressao de material para uso publicitario"),
            row.get("supplier_state", "SP"),
            row.get("amount", "15000,00"),
            row.get("date", "10/08/2022"),
            row.get("description", "Impressao de santinhos"),
            row.get("doc_type", "Nota Fiscal"),
            row.get("doc_number", "NF-001"),
            row.get("origin", "Recurso de outros candidatos"),
        ]
        lines.append(";".join(f'"{v}"' for v in vals))
    return "\n".join(lines)


class TestParseDespesasCsv:
    def test_basic_parsing(self, tmp_path: Path) -> None:
        csv_content = _make_gen6_csv([{"supplier_name": "GRAFICA ABC LTDA", "amount": "15000,00"}])
        path = tmp_path / "despesas.csv"
        path.write_text(csv_content, encoding="utf-8")

        records = parse_despesas_csv(path)
        assert len(records) == 1
        assert records[0]["supplier_name"] == "GRAFICA ABC LTDA"
        assert records[0]["expense_amount_raw"] == "15000,00"

    def test_latin1_encoding(self, tmp_path: Path) -> None:
        csv_content = _make_gen6_csv([{"description": "Impressão de material"}])
        path = tmp_path / "despesas.csv"
        path.write_text(csv_content, encoding="latin-1")

        records = parse_despesas_csv(path)
        assert len(records) == 1

    def test_multiple_records(self, tmp_path: Path) -> None:
        csv_content = _make_gen6_csv(
            [
                {"supplier_name": "FORNECEDOR A", "amount": "100,00"},
                {"supplier_name": "FORNECEDOR B", "amount": "200,00"},
            ]
        )
        path = tmp_path / "despesas.csv"
        path.write_text(csv_content, encoding="utf-8")

        records = parse_despesas_csv(path)
        assert len(records) == 2


class TestNullableSupplier:
    def test_empty_supplier_preserved(self, tmp_path: Path) -> None:
        """Records with empty supplier_name must be preserved, not skipped."""
        csv_content = _make_gen6_csv([{"supplier_name": "", "amount": "500,00"}])
        path = tmp_path / "despesas.csv"
        path.write_text(csv_content, encoding="utf-8")

        records = parse_despesas_csv(path)
        assert len(records) == 1
        assert records[0]["supplier_name"] == ""

    def test_supplier_nan_treated_as_empty(self, tmp_path: Path) -> None:
        csv_content = _make_gen6_csv([{"supplier_name": "nan"}])
        path = tmp_path / "despesas.csv"
        path.write_text(csv_content, encoding="utf-8")

        records = parse_despesas_csv(path)
        assert len(records) == 1
        assert records[0]["supplier_name"] == ""


class TestNormalizeExpenseRecord:
    def test_basic_normalization(self) -> None:
        raw = {
            "supplier_name": "Grafica Modelo",
            "supplier_name_rfb": "GRAFICA MODELO LTDA",
            "supplier_tax_id": "98765432000111",
            "expense_amount_raw": "15.000,00",
            "expense_date_raw": "10/08/2022",
            "state": "SP",
            "position": "PREFEITO",
            "candidate_name": "FULANO",
            "candidate_cpf": "12345678901",
            "candidate_number": "45",
            "party_abbrev": "PT",
            "party_name": "PARTIDO DOS TRABALHADORES",
            "expense_description": "Impressao santinhos",
        }
        result = normalize_expense_record(raw, 2022)
        assert result["election_year"] == 2022
        assert result["expense_amount"] == 15000.0
        assert result["expense_date"] == "2022-08-10"
        assert result["supplier_name_normalized"] == "GRAFICA MODELO LTDA"

    def test_uses_supplier_name_when_no_rfb(self) -> None:
        raw = {
            "supplier_name": "Acme Corp",
            "supplier_name_rfb": None,
            "expense_amount_raw": "100,00",
        }
        result = normalize_expense_record(raw, 2002)
        assert result["supplier_name_normalized"] == "ACME CORP"

    def test_absent_fields_are_none(self) -> None:
        """Fields not in raw dict should be None in normalized output."""
        raw = {
            "supplier_name": "ACME",
            "expense_amount_raw": "100,00",
        }
        result = normalize_expense_record(raw, 2002)
        assert result["candidate_cpf"] is None
        assert result["party_name"] is None
        assert result["supplier_name_rfb"] is None
        assert result["supplier_cnae_code"] is None

    def test_none_supplier_normalization(self) -> None:
        raw = {
            "supplier_name": None,
            "supplier_name_rfb": None,
            "expense_amount_raw": "0",
        }
        result = normalize_expense_record(raw, 2002)
        assert result["supplier_name"] is None
        assert result["supplier_name_normalized"] is None


class TestExpenseDateParsing:
    def test_br_date(self) -> None:
        raw = {"expense_date_raw": "25/12/2022", "expense_amount_raw": "0"}
        result = normalize_expense_record(raw, 2022)
        assert result["expense_date"] == "2022-12-25"

    def test_iso_date(self) -> None:
        raw = {"expense_date_raw": "2022-12-25", "expense_amount_raw": "0"}
        result = normalize_expense_record(raw, 2022)
        assert result["expense_date"] == "2022-12-25"

    def test_empty_date(self) -> None:
        raw = {"expense_date_raw": "", "expense_amount_raw": "0"}
        result = normalize_expense_record(raw, 2022)
        assert result["expense_date"] == ""

    def test_none_date(self) -> None:
        raw = {"expense_date_raw": None, "expense_amount_raw": "0"}
        result = normalize_expense_record(raw, 2022)
        assert result["expense_date"] == ""


class TestResolveExpenseColumn:
    def test_known_column(self) -> None:
        header = ["NM_CANDIDATO", "VR_DESPESA_CONTRATADA"]
        assert _resolve_expense_column(header, "candidate_name") == "NM_CANDIDATO"

    def test_unknown_header(self) -> None:
        header = ["COLUNA_INEXISTENTE", "OUTRA_COLUNA"]
        assert _resolve_expense_column(header, "candidate_name") is None

    def test_case_insensitive(self) -> None:
        header = ["nm_candidato", "vr_despesa"]
        assert _resolve_expense_column(header, "candidate_name") == "nm_candidato"
