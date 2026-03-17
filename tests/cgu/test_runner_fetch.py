"""Tests for cgu/_runner.py — fetch pipeline and integration tests."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from atlas_stf.cgu._config import CguFetchConfig
from atlas_stf.cgu._runner import (
    _download_and_extract_csv,
    _search_all_pages,
    fetch_sanctions_data,
)
from tests.cgu._runner_helpers import (
    _FakeUrlopenResponse,
    _make_ceis_csv,
    _make_csv_zip,
    _write_party_jsonl,
)


class TestFetchSanctionsData:
    @patch("atlas_stf.cgu._runner_csv.urlopen")
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

    @patch("atlas_stf.cgu._runner_csv.urlopen")
    def test_download_and_extract_csv_rejects_large_zip(
        self,
        mock_urlopen: MagicMock,
        monkeypatch,
        tmp_path: Path,
    ) -> None:
        monkeypatch.setattr("atlas_stf.cgu._runner_csv._CGU_MAX_ZIP_UNCOMPRESSED_BYTES", 1)
        csv_content = _make_ceis_csv(
            [["CEIS", "100", "J", "12", "ACME", "", "", "", "", "", "", "", "", "", "", "", "", "CGU"]]
        )
        zip_bytes = _make_csv_zip(csv_content)
        mock_urlopen.return_value = _FakeUrlopenResponse(zip_bytes)

        assert _download_and_extract_csv("ceis", "20260306", tmp_path) is None

    @patch("atlas_stf.cgu._runner_csv.urlopen")
    def test_download_and_extract_csv_rejects_oversized_stream(
        self,
        mock_urlopen: MagicMock,
        monkeypatch,
        tmp_path: Path,
    ) -> None:
        monkeypatch.setattr("atlas_stf.cgu._runner_csv._CGU_MAX_DOWNLOAD_BYTES", 4)
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
