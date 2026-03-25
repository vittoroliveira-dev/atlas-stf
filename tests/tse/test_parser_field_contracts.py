"""Parser field contracts for TSE donations and expenses."""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from atlas_stf.tse._parser import _iter_receitas_csv
from atlas_stf.tse._parser_expenses import _iter_despesas_csv
from atlas_stf.tse._runner_expenses import _SUPPORTED_EXPENSE_YEARS, _validate_years


def _write_csv(
    path: Path,
    header: list[str],
    rows: list[list[str]],
    delimiter: str = ";",
) -> None:
    """Write a CSV file with the given header and rows."""
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, delimiter=delimiter, quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(header)
        for row in rows:
            writer.writerow(row)


class TestDonationsAbsentVsEmpty:
    """E2 core contract: absent field → None, empty field → ''."""

    def test_absent_field_is_none(self, tmp_path: Path) -> None:
        """CSV without any donor_cpf_cnpj alias → output should be None."""
        # Header has no column matching any alias for donor_cpf_cnpj
        header = ["NO_CAND", "NO_DOADOR", "SG_PART", "SG_UF", "DS_CARGO", "VR_RECEITA"]
        rows = [["CANDIDATO A", "DOADOR X", "PT", "SP", "PREFEITO", "1000,00"]]
        csv_path = tmp_path / "receitas.csv"
        _write_csv(csv_path, header, rows)

        records = list(_iter_receitas_csv(csv_path))
        assert len(records) == 1
        assert records[0]["donor_cpf_cnpj"] is None

    def test_present_but_empty_is_empty_string(self, tmp_path: Path) -> None:
        """CSV with column present but empty value → field should be ''."""
        header = ["NM_CANDIDATO", "NM_DOADOR", "NR_CPF_CNPJ_DOADOR", "SG_PARTIDO", "VR_RECEITA"]
        rows = [["CANDIDATO A", "DOADOR X", "", "PT", "1000,00"]]
        csv_path = tmp_path / "receitas.csv"
        _write_csv(csv_path, header, rows)

        records = list(_iter_receitas_csv(csv_path))
        assert len(records) == 1
        assert records[0]["donor_cpf_cnpj"] == ""

    def test_present_with_value(self, tmp_path: Path) -> None:
        header = ["NM_CANDIDATO", "NM_DOADOR", "NR_CPF_CNPJ_DOADOR", "SG_PARTIDO", "VR_RECEITA"]
        rows = [["CANDIDATO A", "DOADOR X", "12345678000100", "PT", "500,00"]]
        csv_path = tmp_path / "receitas.csv"
        _write_csv(csv_path, header, rows)

        records = list(_iter_receitas_csv(csv_path))
        assert len(records) == 1
        assert records[0]["donor_cpf_cnpj"] == "12345678000100"


class TestDonationsGen6HappyPath:
    """Gen6 (2018+): all donation fields present."""

    def test_all_fields_present(self, tmp_path: Path) -> None:
        header = [
            "ANO_ELEICAO",
            "SG_UF",
            "DS_CARGO",
            "NM_CANDIDATO",
            "NR_CPF_CANDIDATO",
            "NR_CANDIDATO",
            "SG_PARTIDO",
            "NM_PARTIDO",
            "NM_DOADOR",
            "NM_DOADOR_RFB",
            "NM_DOADOR_ORIGINARIO",
            "NR_CPF_CNPJ_DOADOR",
            "CD_CNAE_DOADOR",
            "DS_CNAE_DOADOR",
            "SG_UF_DOADOR",
            "VR_RECEITA",
            "DT_RECEITA",
            "DS_RECEITA",
        ]
        rows = [
            [
                "2022",
                "SP",
                "DEPUTADO FEDERAL",
                "CAND TESTE",
                "12345678901",
                "12345",
                "PT",
                "PARTIDO DOS TRABALHADORES",
                "DOADOR SA",
                "DOADOR SA RFB",
                "DOADOR ORIG",
                "12345678000100",
                "4110700",
                "RESTAURANTES",
                "SP",
                "10000,50",
                "15/08/2022",
                "Doação estimada",
            ]
        ]
        csv_path = tmp_path / "receitas_2022.csv"
        _write_csv(csv_path, header, rows)

        records = list(_iter_receitas_csv(csv_path))
        assert len(records) == 1
        rec = records[0]
        assert rec["election_year_raw"] == "2022"
        assert rec["state"] == "SP"
        assert rec["position"] == "DEPUTADO FEDERAL"
        assert rec["candidate_name"] == "CAND TESTE"
        assert rec["candidate_cpf"] == "12345678901"
        assert rec["donor_name"] == "DOADOR SA"
        assert rec["donor_name_rfb"] == "DOADOR SA RFB"
        assert rec["donor_cpf_cnpj"] == "12345678000100"
        assert rec["donation_amount_raw"] == "10000,50"
        # All fields present in Gen6 should be str, not None
        for key in ("donor_cnae_code", "donor_cnae_description", "donor_state"):
            assert rec[key] is not None, f"Field {key} should not be None in Gen6"


class TestDonationsEarlyEra:
    """Early era (2002): many fields absent."""

    def test_gen1_absent_fields_are_none(self, tmp_path: Path) -> None:
        header = ["NO_CAND", "NO_DOADOR", "CD_CPF_CGC", "SG_PART", "SG_UF", "DS_CARGO", "VR_RECEITA"]
        rows = [["CAND A", "DOADOR B", "12345678000100", "PT", "SP", "PREFEITO", "5000,00"]]
        csv_path = tmp_path / "receitas_2002.csv"
        _write_csv(csv_path, header, rows)

        records = list(_iter_receitas_csv(csv_path))
        assert len(records) == 1
        rec = records[0]
        # Fields not in Gen1 header should be None
        assert rec["donor_name_rfb"] is None
        assert rec["donor_name_originator"] is None
        assert rec["donor_cnae_code"] is None
        assert rec["donor_cnae_description"] is None
        # Fields present in Gen1 should be str
        assert rec["donor_cpf_cnpj"] == "12345678000100"
        assert rec["donor_name"] == "DOADOR B"


class TestExpensesAbsentVsEmpty:
    """Expense parser: absent field → None, empty field → ''."""

    def test_absent_field_is_none(self, tmp_path: Path) -> None:
        # Gen1 header: no NR_CPF_CANDIDATO or NM_FORNECEDOR_RFB columns
        header = ["NO_CAND", "NR_CAND", "SG_PART", "SG_UF", "DS_CARGO", "VR_DESPESA", "NO_FOR", "CD_CPF_CGC"]
        rows = [["CAND A", "123", "PT", "SP", "PREFEITO", "1000,00", "FORN X", "12345678000100"]]
        csv_path = tmp_path / "despesas.csv"
        _write_csv(csv_path, header, rows)

        records = list(_iter_despesas_csv(csv_path))
        assert len(records) == 1
        assert records[0]["candidate_cpf"] is None  # not in Gen1
        assert records[0]["supplier_name_rfb"] is None  # not in Gen1

    def test_present_but_empty_is_empty_string(self, tmp_path: Path) -> None:
        # NR_CPF_CANDIDATO present but empty
        header = ["NM_CANDIDATO", "NR_CPF_CANDIDATO", "SG_PARTIDO", "VR_DESPESA_CONTRATADA", "NM_FORNECEDOR"]
        rows = [["CAND A", "", "PT", "1000,00", "FORN X"]]
        csv_path = tmp_path / "despesas.csv"
        _write_csv(csv_path, header, rows)

        records = list(_iter_despesas_csv(csv_path))
        assert len(records) == 1
        assert records[0]["candidate_cpf"] == ""  # present but empty


class TestExpenseUnsupportedYear:
    def test_year_2018_raises(self) -> None:
        with pytest.raises(ValueError, match="SQ_PRESTADOR_CONTAS"):
            _validate_years((2018,))

    def test_year_2012_raises(self) -> None:
        with pytest.raises(ValueError, match="not implemented"):
            _validate_years((2012,))

    def test_supported_years_pass(self) -> None:
        _validate_years(_SUPPORTED_EXPENSE_YEARS)  # should not raise
