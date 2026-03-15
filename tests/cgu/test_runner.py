"""Tests for cgu/_runner.py."""

from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from atlas_stf.cgu._config import CguFetchConfig
from atlas_stf.cgu._runner import (
    _canonicalize_tipo_pessoa,
    _download_and_extract_csv,
    _load_csv_sanctions,
    _load_leniencia_csv,
    _looks_like_entity,
    _normalize_csv_record,
    _normalize_date,
    _normalize_leniencia_record,
    _search_all_pages,
    fetch_sanctions_data,
)


class _FakeUrlopenResponse:
    def __init__(self, body: bytes | None = None, *, chunks: list[bytes] | None = None) -> None:
        if chunks is not None:
            self._chunks = list(chunks)
        else:
            self._chunks = [body or b""]

    def __enter__(self) -> _FakeUrlopenResponse:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def read(self, _size: int = -1) -> bytes:
        if not self._chunks:
            return b""
        return self._chunks.pop(0)


def _write_party_jsonl(path: Path, parties: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        for p in parties:
            fh.write(json.dumps(p) + "\n")


def _make_ceis_csv(records: list[list[str]]) -> str:
    """Build a CEIS CSV string with header + rows."""
    header = ";".join(
        [
            '"CADASTRO"',
            '"CÓDIGO"',
            '"TIPO"',
            '"CPF/CNPJ"',
            '"NOME"',
            '"NOME ORG"',
            '"RAZAO"',
            '"FANTASIA"',
            '"PROCESSO"',
            '"CATEGORIA"',
            '"DATA INÍCIO"',
            '"DATA FIM"',
            '"DATA PUB"',
            '"PUBLICAÇÃO"',
            '"DETALHE"',
            '"TRANSITO"',
            '"ABRANGENCIA"',
            '"ÓRGÃO"',
        ]
    )
    lines = [header]
    for row in records:
        lines.append(";".join(f'"{v}"' for v in row))
    return "\n".join(lines)


def _make_csv_zip(csv_content: str, csv_name: str = "20260306_CEIS.csv") -> bytes:
    """Create a ZIP containing one CSV file."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(csv_name, csv_content.encode("latin-1"))
    return buf.getvalue()


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


def _make_leniencia_csv(records: list[list[str]]) -> str:
    """Build a Leniência CSV string with header + rows (real CSV structure)."""
    header = ";".join(
        [
            '"ID DO ACORDO"',
            '"CNPJ DO SANCIONADO"',
            '"RAZAO SOCIAL - CADASTRO RECEITA"',
            '"NOME FANTASIA - CADASTRO RECEITA"',
            '"DATA DE INICIO DO ACORDO"',
            '"DATA DE FIM DO ACORDO"',
            '"SITUACAO DO ACORDO DE LENIENCIA"',
            '"DATA DA INFORMACAO"',
            '"NUMERO DO PROCESSO"',
            '"TERMOS DO ACORDO"',
            '"ORGAO SANCIONADOR"',
        ]
    )
    lines = [header]
    for row in records:
        lines.append(";".join(f'"{v}"' for v in row))
    return "\n".join(lines)


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


class TestFetchSanctionsData:
    @patch("atlas_stf.cgu._runner.urlopen")
    def test_download_and_extract_csv_rejects_traversal_member(
        self,
        mock_urlopen: MagicMock,
        tmp_path: Path,
    ) -> None:
        csv_content = _make_ceis_csv(
            [["CEIS", "100", "J", "12", "ACME", "", "", "", "", "", "", "", "", "", "", "", "", "CGU"]]
        )
        mock_urlopen.return_value = _FakeUrlopenResponse(_make_csv_zip(csv_content, "../../evil.csv"))

        assert _download_and_extract_csv("ceis", "20260306", tmp_path) is None

    def test_search_all_pages_stops_at_max_pages(self) -> None:
        client = MagicMock()
        client.search_ceis.return_value = [{"id": 1}] * 15

        results = _search_all_pages(
            client,
            "search_ceis",
            lambda name, page: {"name": name, "page": page},
            lambda raw: raw,
            "ACME",
            max_pages=3,
        )

        assert len(results) == 45
        assert client.search_ceis.call_count == 3

    @patch("atlas_stf.cgu._runner.urlopen")
    def test_download_and_extract_csv_rejects_large_zip(
        self,
        mock_urlopen: MagicMock,
        monkeypatch,
        tmp_path: Path,
    ) -> None:
        monkeypatch.setattr("atlas_stf.cgu._runner._CGU_MAX_ZIP_UNCOMPRESSED_BYTES", 1)
        csv_content = _make_ceis_csv(
            [["CEIS", "100", "J", "12", "ACME", "", "", "", "", "", "", "", "", "", "", "", "", "CGU"]]
        )
        zip_bytes = _make_csv_zip(csv_content)
        mock_urlopen.return_value = _FakeUrlopenResponse(zip_bytes)

        assert _download_and_extract_csv("ceis", "20260306", tmp_path) is None

    @patch("atlas_stf.cgu._runner.urlopen")
    def test_download_and_extract_csv_rejects_oversized_stream(
        self,
        mock_urlopen: MagicMock,
        monkeypatch,
        tmp_path: Path,
    ) -> None:
        monkeypatch.setattr("atlas_stf.cgu._runner._CGU_MAX_DOWNLOAD_BYTES", 4)
        mock_urlopen.return_value = _FakeUrlopenResponse(chunks=[b"12", b"345"])

        assert _download_and_extract_csv("ceis", "20260306", tmp_path) is None
        assert not (tmp_path / "ceis.zip").exists()

    def test_dry_run(self, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        config = CguFetchConfig(output_dir=output_dir, dry_run=True)
        result = fetch_sanctions_data(config)
        assert result == output_dir
        assert output_dir.exists()

    @patch("atlas_stf.cgu._runner._fetch_via_csv")
    def test_csv_primary(self, mock_csv: MagicMock, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        mock_csv.return_value = [
            {
                "sanction_source": "ceis",
                "sanction_id": "100",
                "entity_name": "ACME CORP",
                "entity_cnpj_cpf": "",
                "sanctioning_body": "CGU",
                "sanction_type": "Suspensão",
                "sanction_start_date": "01/01/2020",
                "sanction_end_date": "",
                "sanction_description": "",
                "uf_sancionado": "",
            },
        ]
        config = CguFetchConfig(output_dir=output_dir)
        fetch_sanctions_data(config)

        raw_path = output_dir / "sanctions_raw.jsonl"
        assert raw_path.exists()
        records = [json.loads(line) for line in raw_path.read_text().strip().split("\n")]
        assert len(records) == 1
        assert records[0]["sanction_source"] == "ceis"

    @patch("atlas_stf.cgu._runner._fetch_via_csv", return_value=None)
    @patch("atlas_stf.cgu._runner.CguClient")
    def test_api_fallback(self, mock_client_cls: MagicMock, mock_csv: MagicMock, tmp_path: Path) -> None:
        party_path = tmp_path / "party.jsonl"
        _write_party_jsonl(
            party_path,
            [{"party_id": "p1", "party_name_raw": "ACME LTDA", "party_name_normalized": "ACME LTDA"}],
        )
        output_dir = tmp_path / "output"

        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.search_ceis.return_value = [
            {
                "id": 1,
                "sancionado": {"nome": "ACME LTDA"},
                "orgaoSancionador": {"nome": "CGU"},
                "tipoSancao": {"descricaoResumida": "Inidoneidade"},
                "dataInicioSancao": "2020-01-01",
                "dataFimSancao": "",
                "textoPublicacao": "",
            },
        ]
        mock_client.search_cnep.return_value = []

        config = CguFetchConfig(output_dir=output_dir, api_key="test-key", party_path=party_path)
        fetch_sanctions_data(config)

        raw_path = output_dir / "sanctions_raw.jsonl"
        assert raw_path.exists()
        records = [json.loads(line) for line in raw_path.read_text().strip().split("\n")]
        assert len(records) == 1
        assert records[0]["sanction_source"] == "ceis"

    @patch("atlas_stf.cgu._runner._fetch_via_csv", return_value=None)
    def test_no_api_key_raises(self, mock_csv: MagicMock, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        config = CguFetchConfig(output_dir=output_dir)
        import pytest

        with pytest.raises(RuntimeError, match="no API key"):
            fetch_sanctions_data(config)

    @patch("atlas_stf.cgu._runner._fetch_via_csv")
    def test_empty_csv_results(self, mock_csv: MagicMock, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        mock_csv.return_value = []
        config = CguFetchConfig(output_dir=output_dir)
        fetch_sanctions_data(config)

        raw_path = output_dir / "sanctions_raw.jsonl"
        assert raw_path.exists()
        assert raw_path.read_text().strip() == ""


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
            "CEIS", "12345", "F", "123.456.789-00", "JOAO",
            "ORG", "RAZAO", "", "PROC", "Cat",
            "01/01/2020", "01/01/2025", "", "", "", "", "", "CGU",
        ]
        from atlas_stf.cgu._runner import _CEIS_COL

        result = _normalize_csv_record(row, "ceis", _CEIS_COL)
        assert result["entity_type_pf_pj"] == "PF"
        assert result["entity_type_pf_pj_raw"] == "F"
        assert result["entity_cnpj_cpf"] == "12345678900"
        assert result["entity_cnpj_cpf_raw"] == "123.456.789-00"
