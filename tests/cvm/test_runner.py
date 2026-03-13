"""Tests for CVM fetch runner."""

from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path
from unittest.mock import patch

from atlas_stf.cvm._config import CvmFetchConfig
from atlas_stf.cvm._runner import _download_zip, _process_zip, fetch_cvm_data


class _FakeStreamResponse:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks

    def __enter__(self) -> _FakeStreamResponse:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def raise_for_status(self) -> None:
        return None

    def iter_bytes(self):
        yield from self._chunks


def _make_cvm_zip() -> bytes:
    """Create a minimal CVM ZIP with process + accused CSVs."""
    process_csv = "NUMERO_PROCESSO;ASSUNTO;DATA_ABERTURA;FASE_ATUAL;OBJETO;EMENTA\n"
    process_csv += "PAS-001;Fraude;2023-05-15;Citacao;Mercado;Ementa test\n"

    accused_csv = "NUMERO_PROCESSO;NOME_ACUSADO;CPF_CNPJ\n"
    accused_csv += "PAS-001;ACME S.A.;12345678000199\n"
    accused_csv += "PAS-001;JOHN DOE;12345678901\n"

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("processo_sancionador.csv", process_csv)
        zf.writestr("processo_sancionador_acusado.csv", accused_csv)
    return buf.getvalue()


class TestFetchDryRun:
    def test_dry_run_no_download(self, tmp_path: Path) -> None:
        config = CvmFetchConfig(output_dir=tmp_path / "cvm", dry_run=True)
        result = fetch_cvm_data(config)
        assert result == config.output_dir
        assert not (config.output_dir / "sanctions_raw.jsonl").exists()

    def test_process_zip_rejects_large_archive(self, monkeypatch, tmp_path: Path) -> None:
        monkeypatch.setattr("atlas_stf.cvm._runner._CVM_MAX_ZIP_UNCOMPRESSED_BYTES", 1)
        zip_path = tmp_path / "cvm_source.zip"
        zip_path.write_bytes(_make_cvm_zip())

        assert _process_zip(zip_path, tmp_path / "extract") == []

    @patch("atlas_stf.cvm._runner.httpx.stream")
    def test_download_zip_rejects_oversized_stream(self, mock_stream, monkeypatch, tmp_path: Path) -> None:
        monkeypatch.setattr("atlas_stf.cvm._runner._CVM_MAX_DOWNLOAD_BYTES", 4)
        destination = tmp_path / "cvm_source.zip"
        mock_stream.return_value = _FakeStreamResponse([b"12", b"345"])

        assert _download_zip("https://example.test/cvm.zip", destination, timeout=5) is None
        assert not destination.exists()


class TestFetchDownload:
    @patch("atlas_stf.cvm._runner._download_zip")
    def test_downloads_and_extracts(self, mock_download_zip, tmp_path: Path) -> None:
        zip_bytes = _make_cvm_zip()
        output_dir = tmp_path / "cvm"
        zip_path = output_dir / "cvm_source.zip"
        output_dir.mkdir(parents=True, exist_ok=True)
        zip_path.write_bytes(zip_bytes)
        mock_download_zip.return_value = zip_path

        config = CvmFetchConfig(output_dir=output_dir)
        fetch_cvm_data(config)

        output_path = config.output_dir / "sanctions_raw.jsonl"
        assert output_path.exists()

        records = [json.loads(line) for line in output_path.read_text().strip().split("\n")]
        assert len(records) == 2
        assert all(r["sanction_source"] == "cvm" for r in records)
        assert all(r["sanctioning_body"] == "CVM" for r in records)
        assert records[0]["sanction_id"] == "PAS-001"

    @patch("atlas_stf.cvm._runner._download_zip", return_value=None)
    def test_download_failure_returns_dir(self, _mock_download_zip, tmp_path: Path) -> None:
        config = CvmFetchConfig(output_dir=tmp_path / "cvm")
        result = fetch_cvm_data(config)

        assert result == config.output_dir
        assert not (config.output_dir / "sanctions_raw.jsonl").exists()
