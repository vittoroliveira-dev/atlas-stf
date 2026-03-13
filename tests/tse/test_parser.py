"""Tests for tse/_parser.py."""

from __future__ import annotations

from pathlib import Path

import pytest

from atlas_stf.tse._parser import detect_encoding, normalize_donation_record, parse_receitas_csv


def _make_receitas_csv(rows: list[dict[str, str]], encoding: str = "utf-8") -> str:
    """Build a receitas CSV string with semicolon separator."""
    header = ";".join(
        [
            '"ANO_ELEICAO"',
            '"SG_UF"',
            '"DS_CARGO"',
            '"NM_CANDIDATO"',
            '"NR_CPF_CANDIDATO"',
            '"NR_CANDIDATO"',
            '"SG_PARTIDO"',
            '"NM_PARTIDO"',
            '"NM_DOADOR"',
            '"NM_DOADOR_RFB"',
            '"NR_CPF_CNPJ_DOADOR"',
            '"CD_CNAE_DOADOR"',
            '"DS_CNAE_DOADOR"',
            '"SG_UF_DOADOR"',
            '"VR_RECEITA"',
            '"DS_RECEITA"',
        ]
    )
    lines = [header]
    for row in rows:
        vals = [
            row.get("year", "2022"),
            row.get("state", "SP"),
            row.get("position", "SENADOR"),
            row.get("candidate", "FULANO"),
            row.get("candidate_cpf", "12345678901"),
            row.get("candidate_number", "123"),
            row.get("party_abbrev", "PT"),
            row.get("party_name", "PARTIDO DOS TRABALHADORES"),
            row.get("donor_name", "ACME LTDA"),
            row.get("donor_name_rfb", "ACME LTDA"),
            row.get("donor_cpf_cnpj", "12345678000199"),
            row.get("cnae_code", "4110700"),
            row.get("cnae_desc", "Incorporacao"),
            row.get("donor_state", "SP"),
            row.get("amount", "50000,00"),
            row.get("description", "Doacao em dinheiro"),
        ]
        lines.append(";".join(f'"{v}"' for v in vals))
    return "\n".join(lines)


class TestDetectEncoding:
    def test_utf8(self, tmp_path: Path) -> None:
        path = tmp_path / "test.csv"
        path.write_text("hello world", encoding="utf-8")
        assert detect_encoding(path) == "utf-8"

    def test_latin1(self, tmp_path: Path) -> None:
        path = tmp_path / "test.csv"
        path.write_bytes("doação".encode("latin-1"))
        assert detect_encoding(path) == "latin-1"


class TestParseReceitasCsv:
    def test_basic_parsing(self, tmp_path: Path) -> None:
        csv_content = _make_receitas_csv(
            [
                {"donor_name": "ACME LTDA", "amount": "50000,00"},
                {"donor_name": "XYZ SA", "amount": "1.250.000,50"},
            ]
        )
        path = tmp_path / "receitas.csv"
        path.write_text(csv_content, encoding="utf-8")

        records = parse_receitas_csv(path)
        assert len(records) == 2
        assert records[0]["donor_name"] == "ACME LTDA"
        assert records[1]["donor_name"] == "XYZ SA"

    def test_empty_donor_skipped(self, tmp_path: Path) -> None:
        csv_content = _make_receitas_csv(
            [
                {"donor_name": "", "amount": "100,00"},
            ]
        )
        path = tmp_path / "receitas.csv"
        path.write_text(csv_content, encoding="utf-8")

        records = parse_receitas_csv(path)
        assert len(records) == 0

    def test_latin1_encoding(self, tmp_path: Path) -> None:
        csv_content = _make_receitas_csv(
            [
                {"donor_name": "FUNDAÇÃO ABC", "amount": "100,00"},
            ]
        )
        path = tmp_path / "receitas.csv"
        path.write_text(csv_content, encoding="latin-1")

        records = parse_receitas_csv(path)
        assert len(records) == 1

    def test_parse_receitas_csv_does_not_use_read_text(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        csv_content = _make_receitas_csv([{"donor_name": "ACME LTDA", "amount": "50000,00"}])
        path = tmp_path / "receitas.csv"
        path.write_text(csv_content, encoding="utf-8")

        def _fail_read_text(self: Path, *args, **kwargs) -> str:
            raise AssertionError("read_text should not be used for TSE CSV parsing")

        monkeypatch.setattr(Path, "read_text", _fail_read_text)

        records = parse_receitas_csv(path)
        assert len(records) == 1
        assert records[0]["donor_name"] == "ACME LTDA"


class TestNormalizeDonationRecord:
    def test_basic_normalization(self) -> None:
        raw = {
            "donor_name": "Acme Corp",
            "donor_name_rfb": "ACME CORP LTDA",
            "donor_cpf_cnpj": "12345678000199",
            "donation_amount_raw": "50.000,00",
            "state": "SP",
            "position": "SENADOR",
            "candidate_name": "FULANO",
            "candidate_cpf": "12345678901",
            "candidate_number": "123",
            "party_abbrev": "PT",
            "party_name": "PARTIDO DOS TRABALHADORES",
            "donor_cnae_code": "4110700",
            "donor_cnae_description": "Incorporacao",
            "donor_state": "SP",
        }
        result = normalize_donation_record(raw, 2022)
        assert result["election_year"] == 2022
        assert result["donation_amount"] == 50000.0
        assert result["donor_name_normalized"] == "ACME CORP LTDA"
        assert result["donor_cpf_cnpj"] == "12345678000199"

    def test_uses_donor_name_when_no_rfb(self) -> None:
        raw = {
            "donor_name": "Acme Corp",
            "donor_name_rfb": "",
            "donation_amount_raw": "100,00",
        }
        result = normalize_donation_record(raw, 2020)
        assert result["donor_name_normalized"] == "ACME CORP"


class TestDonationDescriptionExtraction:
    """Bug 7 fix: donation_description should be extracted from DS_RECEITA."""

    def test_parse_receitas_includes_donation_description(self, tmp_path: Path) -> None:
        csv_content = _make_receitas_csv([{"donor_name": "ACME LTDA", "description": "Doacao em dinheiro"}])
        path = tmp_path / "receitas.csv"
        path.write_text(csv_content, encoding="utf-8")

        records = parse_receitas_csv(path)
        assert len(records) == 1
        assert records[0]["donation_description"] == "Doacao em dinheiro"

    def test_normalize_preserves_donation_description(self) -> None:
        raw = {
            "donor_name": "ACME",
            "donor_name_rfb": "",
            "donation_amount_raw": "100,00",
            "donation_description": "Doacao em dinheiro",
        }
        result = normalize_donation_record(raw, 2022)
        assert result["donation_description"] == "Doacao em dinheiro"
