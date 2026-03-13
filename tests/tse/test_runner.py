"""Tests for tse/_runner.py."""

from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from atlas_stf.tse._config import TseFetchConfig
from atlas_stf.tse._runner import (
    _Checkpoint,
    _download_year_zip,
    _extract_zip,
    _YearMeta,
    fetch_donation_data,
)


def _make_receitas_csv_content() -> str:
    """Build a minimal receitas CSV for testing."""
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
    row = ";".join(
        [
            '"2022"',
            '"SP"',
            '"SENADOR"',
            '"FULANO"',
            '"12345678901"',
            '"123"',
            '"PT"',
            '"PARTIDO DOS TRABALHADORES"',
            '"ACME LTDA"',
            '"ACME LTDA"',
            '"12345678000199"',
            '"4110700"',
            '"Incorporacao"',
            '"SP"',
            '"50000,00"',
            '"Doacao em dinheiro"',
        ]
    )
    return f"{header}\n{row}\n"


def _make_zip_with_csv(csv_name: str, csv_content: str) -> bytes:
    """Create a ZIP containing one CSV file."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(csv_name, csv_content.encode("utf-8"))
    return buf.getvalue()


_FAKE_META = _YearMeta(url="http://test/file.zip", content_length=1234, etag='"abc"')


class _FakeStreamResponse:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks
        self.headers = {"etag": '"abc"', "content-length": "999"}

    def __enter__(self) -> _FakeStreamResponse:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def raise_for_status(self) -> None:
        return None

    def iter_bytes(self):
        yield from self._chunks


class TestCheckpoint:
    def test_load_empty(self, tmp_path: Path) -> None:
        cp = _Checkpoint.load(tmp_path)
        assert cp.completed_years == set()
        assert cp.year_meta == {}

    def test_save_and_load(self, tmp_path: Path) -> None:
        cp = _Checkpoint(
            completed_years={2022, 2024},
            year_meta={2022: _FAKE_META},
        )
        cp.save(tmp_path)
        loaded = _Checkpoint.load(tmp_path)
        assert loaded.completed_years == {2022, 2024}
        assert loaded.year_meta[2022].etag == '"abc"'

    def test_backward_compat_old_checkpoint(self, tmp_path: Path) -> None:
        """Old checkpoint format (no year_meta) should still load."""
        old = {"completed_years": [2022]}
        (tmp_path / "_checkpoint.json").write_text(json.dumps(old))
        loaded = _Checkpoint.load(tmp_path)
        assert loaded.completed_years == {2022}
        assert loaded.year_meta == {}


class TestYearMeta:
    def test_matches_etag(self) -> None:
        import httpx

        meta = _YearMeta(url="http://x", content_length=100, etag='"abc"')
        headers = httpx.Headers({"etag": '"abc"', "content-length": "999"})
        assert meta.matches(headers) is True

    def test_no_match_etag(self) -> None:
        import httpx

        meta = _YearMeta(url="http://x", content_length=100, etag='"abc"')
        headers = httpx.Headers({"etag": '"def"', "content-length": "100"})
        assert meta.matches(headers) is False

    def test_matches_content_length_no_etag(self) -> None:
        import httpx

        meta = _YearMeta(url="http://x", content_length=500, etag="")
        headers = httpx.Headers({"content-length": "500"})
        assert meta.matches(headers) is True

    def test_no_match_content_length(self) -> None:
        import httpx

        meta = _YearMeta(url="http://x", content_length=500, etag="")
        headers = httpx.Headers({"content-length": "600"})
        assert meta.matches(headers) is False


class TestFetchDonationData:
    def test_extract_zip_rejects_large_archive(self, monkeypatch, tmp_path: Path) -> None:
        monkeypatch.setattr("atlas_stf.tse._runner._TSE_MAX_ZIP_UNCOMPRESSED_BYTES", 1)
        zip_path = tmp_path / "too-large.zip"
        zip_path.write_bytes(_make_zip_with_csv("receitas_candidatos_2022_BRASIL.csv", _make_receitas_csv_content()))

        assert _extract_zip(zip_path, tmp_path / "extract") is None

    @patch("atlas_stf.tse._runner.httpx.stream")
    def test_download_year_zip_rejects_oversized_stream(self, mock_stream, monkeypatch, tmp_path: Path) -> None:
        monkeypatch.setattr("atlas_stf.tse._runner._TSE_MAX_DOWNLOAD_BYTES", 4)
        mock_stream.return_value = _FakeStreamResponse([b"12", b"345"])

        zip_path, meta = _download_year_zip(2022, tmp_path / "output", timeout=5)

        assert zip_path is None
        assert meta is None
        assert not (tmp_path / "output" / "tse_2022.zip").exists()

    def test_dry_run(self, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        config = TseFetchConfig(output_dir=output_dir, years=(2022,), dry_run=True)
        result = fetch_donation_data(config)
        assert result == output_dir
        assert output_dir.exists()

    @patch("atlas_stf.tse._runner._download_year_zip")
    def test_download_and_parse(self, mock_download_year_zip: MagicMock, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        csv_content = _make_receitas_csv_content()
        zip_bytes = _make_zip_with_csv("receitas_candidatos_2022_BRASIL.csv", csv_content)
        zip_path = output_dir / "tse_2022.zip"
        output_dir.mkdir(parents=True, exist_ok=True)
        zip_path.write_bytes(zip_bytes)
        mock_download_year_zip.return_value = (zip_path, _FAKE_META)

        config = TseFetchConfig(output_dir=output_dir, years=(2022,))
        fetch_donation_data(config)

        raw_path = output_dir / "donations_raw.jsonl"
        assert raw_path.exists()
        records = [json.loads(line) for line in raw_path.read_text().strip().split("\n")]
        assert len(records) == 1
        assert records[0]["donor_name"] == "ACME LTDA"
        assert records[0]["donation_amount"] == 50000.0
        assert records[0]["election_year"] == 2022
        assert not (output_dir / "extracted_2022").exists()

    @patch("atlas_stf.tse._runner._download_year_zip")
    def test_checkpoint_resumability(self, mock_download_year_zip: MagicMock, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        output_dir.mkdir(parents=True)

        # Pre-populate checkpoint and existing data
        cp = _Checkpoint(completed_years={2022})
        cp.save(output_dir)
        raw_path = output_dir / "donations_raw.jsonl"
        raw_path.write_text(json.dumps({"donor_name": "EXISTING", "election_year": 2022}) + "\n")

        csv_content = _make_receitas_csv_content()
        zip_bytes = _make_zip_with_csv("receitas_candidatos_2024_BRASIL.csv", csv_content)
        zip_path = output_dir / "tse_2024.zip"
        zip_path.write_bytes(zip_bytes)
        mock_download_year_zip.return_value = (zip_path, _FAKE_META)

        config = TseFetchConfig(output_dir=output_dir, years=(2022, 2024))
        fetch_donation_data(config)

        records = [json.loads(line) for line in raw_path.read_text().strip().split("\n")]
        # Should have existing record + new record
        assert len(records) == 2
        # download helper should only be called once (for 2024, not 2022)
        assert mock_download_year_zip.call_count == 1

    @patch("atlas_stf.tse._runner._download_year_zip")
    def test_empty_zip(self, mock_download_year_zip: MagicMock, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"

        # ZIP with no CSV files
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("readme.txt", "No CSV here")
        zip_path = output_dir / "tse_2022.zip"
        output_dir.mkdir(parents=True, exist_ok=True)
        zip_path.write_bytes(buf.getvalue())
        mock_download_year_zip.return_value = (zip_path, _FAKE_META)

        config = TseFetchConfig(output_dir=output_dir, years=(2022,))
        fetch_donation_data(config)

        raw_path = output_dir / "donations_raw.jsonl"
        assert raw_path.exists()
        # Should have 0 records but not fail
        assert raw_path.read_text().strip() == ""

    @patch("atlas_stf.tse._runner._download_year_zip")
    def test_empty_year_not_checkpointed(self, mock_download_year_zip: MagicMock, tmp_path: Path) -> None:
        """Bug 3 fix: year with 0 records should NOT be marked as completed."""
        output_dir = tmp_path / "output"

        # ZIP with no CSV files → 0 records
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("readme.txt", "No CSV here")
        zip_path = output_dir / "tse_2022.zip"
        output_dir.mkdir(parents=True, exist_ok=True)
        zip_path.write_bytes(buf.getvalue())
        mock_download_year_zip.return_value = (zip_path, _FAKE_META)

        config = TseFetchConfig(output_dir=output_dir, years=(2022,))
        fetch_donation_data(config)

        # Year should NOT be in checkpoint
        cp = _Checkpoint.load(output_dir)
        assert 2022 not in cp.completed_years

    @patch("atlas_stf.tse._runner._download_year_zip")
    def test_meta_saved_on_success(self, mock_download_year_zip: MagicMock, tmp_path: Path) -> None:
        """After successful download+parse, year_meta should persist."""
        output_dir = tmp_path / "output"
        csv_content = _make_receitas_csv_content()
        zip_bytes = _make_zip_with_csv("receitas_candidatos_2022_BRASIL.csv", csv_content)
        zip_path = output_dir / "tse_2022.zip"
        output_dir.mkdir(parents=True, exist_ok=True)
        zip_path.write_bytes(zip_bytes)
        meta = _YearMeta(url="http://test/2022.zip", content_length=len(zip_bytes), etag='"xyz"')
        mock_download_year_zip.return_value = (zip_path, meta)

        config = TseFetchConfig(output_dir=output_dir, years=(2022,))
        fetch_donation_data(config)

        cp = _Checkpoint.load(output_dir)
        assert 2022 in cp.completed_years
        assert cp.year_meta[2022].etag == '"xyz"'
        assert cp.year_meta[2022].content_length == len(zip_bytes)

    @patch("atlas_stf.tse._runner._download_year_zip")
    def test_unchanged_file_skipped(self, mock_download_year_zip: MagicMock, tmp_path: Path) -> None:
        """If _download_year_zip returns (None, None), year is skipped without error."""
        output_dir = tmp_path / "output"
        output_dir.mkdir(parents=True)

        # Pre-populate with existing data
        raw_path = output_dir / "donations_raw.jsonl"
        raw_path.write_text(json.dumps({"donor_name": "OLD", "election_year": 2022}) + "\n")
        cp = _Checkpoint(
            completed_years={2022},
            year_meta={2022: _FAKE_META},
        )
        cp.save(output_dir)

        # Simulate unchanged file → download returns (None, None)
        mock_download_year_zip.return_value = (None, None)

        config = TseFetchConfig(output_dir=output_dir, years=(2022,))
        fetch_donation_data(config)

        # Existing data should be preserved
        records = [json.loads(line) for line in raw_path.read_text().strip().split("\n")]
        assert len(records) == 1
        assert records[0]["donor_name"] == "OLD"
