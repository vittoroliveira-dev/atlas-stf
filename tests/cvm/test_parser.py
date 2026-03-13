"""Tests for CVM parser functions."""

from __future__ import annotations

from pathlib import Path

from atlas_stf.cvm._parser import (
    detect_encoding,
    join_and_normalize,
    parse_accused_csv,
    parse_process_csv,
)


def _write_csv(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


class TestDetectEncoding:
    def test_utf8(self, tmp_path: Path) -> None:
        p = tmp_path / "utf8.csv"
        p.write_text("café", encoding="utf-8")
        assert detect_encoding(p) == "utf-8"

    def test_latin1(self, tmp_path: Path) -> None:
        p = tmp_path / "latin1.csv"
        p.write_bytes("café".encode("latin-1"))
        assert detect_encoding(p) == "latin-1"


class TestParseProcessCsv:
    def test_semicolon_delimiter(self, tmp_path: Path) -> None:
        csv_content = "NUMERO_PROCESSO;ASSUNTO;DATA_ABERTURA;FASE_ATUAL;OBJETO;EMENTA\n"
        csv_content += "PAS-2023-001;Fraude;2023-05-15;Citacao;Mercado de capitais;Ementa do processo\n"
        _write_csv(tmp_path / "processo.csv", csv_content)

        result = parse_process_csv(tmp_path / "processo.csv")
        assert "PAS-2023-001" in result
        assert result["PAS-2023-001"]["subject"] == "Fraude"
        assert result["PAS-2023-001"]["opening_date"] == "2023-05-15"
        assert result["PAS-2023-001"]["summary"] == "Ementa do processo"

    def test_comma_delimiter(self, tmp_path: Path) -> None:
        csv_content = "NUMERO_PROCESSO,ASSUNTO,DATA_ABERTURA,FASE_ATUAL,OBJETO,EMENTA\n"
        csv_content += "PAS-2023-002,Insider trading,2023-06-01,Defesa,Negociacao,Resumo\n"
        _write_csv(tmp_path / "processo.csv", csv_content)

        result = parse_process_csv(tmp_path / "processo.csv")
        assert "PAS-2023-002" in result

    def test_empty_file(self, tmp_path: Path) -> None:
        _write_csv(tmp_path / "empty.csv", "")
        assert parse_process_csv(tmp_path / "empty.csv") == {}

    def test_alternate_column_names(self, tmp_path: Path) -> None:
        csv_content = "NR_PROCESSO;DS_ASSUNTO;DT_ABERTURA;DS_FASE;DS_OBJETO;DS_EMENTA\n"
        csv_content += "PAS-ALT;Manipulacao;2024-01-01;Instrucao;Obj;Ement\n"
        _write_csv(tmp_path / "alt.csv", csv_content)

        result = parse_process_csv(tmp_path / "alt.csv")
        assert "PAS-ALT" in result
        assert result["PAS-ALT"]["subject"] == "Manipulacao"


class TestParseAccusedCsv:
    def test_basic(self, tmp_path: Path) -> None:
        csv_content = "NUMERO_PROCESSO;NOME_ACUSADO;CPF_CNPJ\n"
        csv_content += "PAS-2023-001;ACME S.A.;12345678000199\n"
        csv_content += "PAS-2023-001;JOHN DOE;12345678901\n"
        _write_csv(tmp_path / "acusado.csv", csv_content)

        result = parse_accused_csv(tmp_path / "acusado.csv")
        assert len(result) == 2
        assert result[0]["accused_name"] == "ACME S.A."
        assert result[0]["accused_cpf_cnpj"] == "12345678000199"

    def test_skip_empty_name(self, tmp_path: Path) -> None:
        csv_content = "NUMERO_PROCESSO;NOME_ACUSADO;CPF_CNPJ\n"
        csv_content += "PAS-001;;123\n"
        _write_csv(tmp_path / "acusado.csv", csv_content)

        result = parse_accused_csv(tmp_path / "acusado.csv")
        assert len(result) == 0


class TestJoinAndNormalize:
    def test_one_to_many_join(self) -> None:
        processes = {
            "PAS-001": {
                "process_number": "PAS-001",
                "subject": "Fraude",
                "opening_date": "2023-05-15",
                "current_phase": "Citacao",
                "object": "Mercado",
                "summary": "Ementa",
            }
        }
        accused = [
            {"process_number": "PAS-001", "accused_name": "ACME S.A.", "accused_cpf_cnpj": "12345678000199"},
            {"process_number": "PAS-001", "accused_name": "JOHN DOE", "accused_cpf_cnpj": "12345678901"},
        ]

        result = join_and_normalize(processes, accused)
        assert len(result) == 2
        assert all(r["sanction_source"] == "cvm" for r in result)
        assert all(r["sanctioning_body"] == "CVM" for r in result)
        assert all(r["sanction_id"] == "PAS-001" for r in result)
        assert result[0]["sanction_type"] == "Fraude"

    def test_missing_process_skipped(self) -> None:
        processes = {
            "PAS-001": {
                "process_number": "PAS-001",
                "subject": "X",
                "opening_date": "",
                "current_phase": "",
                "object": "",
                "summary": "",
            }
        }
        accused = [
            {"process_number": "PAS-999", "accused_name": "ORPHAN", "accused_cpf_cnpj": ""},
        ]

        result = join_and_normalize(processes, accused)
        assert len(result) == 0

    def test_name_normalization(self) -> None:
        processes = {
            "P1": {
                "process_number": "P1",
                "subject": "",
                "opening_date": "",
                "current_phase": "",
                "object": "",
                "summary": "",
            }
        }
        accused = [{"process_number": "P1", "accused_name": "  acme  corp  ltda  ", "accused_cpf_cnpj": ""}]

        result = join_and_normalize(processes, accused)
        assert len(result) == 1
        # normalize_entity_name uppercases and collapses whitespace
        assert result[0]["entity_name"] == "ACME CORP LTDA"

    def test_date_preserved(self) -> None:
        processes = {
            "P1": {
                "process_number": "P1",
                "subject": "",
                "opening_date": "2024-03-15",
                "current_phase": "",
                "object": "",
                "summary": "",
            }
        }
        accused = [{"process_number": "P1", "accused_name": "TEST", "accused_cpf_cnpj": ""}]

        result = join_and_normalize(processes, accused)
        assert result[0]["sanction_start_date"] == "2024-03-15"
        assert result[0]["sanction_end_date"] == ""
