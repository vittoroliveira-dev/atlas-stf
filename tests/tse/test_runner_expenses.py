"""Tests for tse/_runner_expenses.py."""

from __future__ import annotations

import io
import json
import re
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from atlas_stf.tse._config import TseExpenseFetchConfig
from atlas_stf.tse._runner import _YearMeta
from atlas_stf.tse._runner_expenses import (
    _SUPPORTED_EXPENSE_YEARS,
    _Checkpoint,
    _find_despesas_files,
    _validate_years,
    fetch_expense_data,
)


def _make_gen6_csv_content() -> str:
    """Build a minimal Gen6 (2022+) despesas_contratadas CSV for testing."""
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
    row = ";".join(
        [
            '"2022"',
            '"SP"',
            '"PREFEITO"',
            '"FULANO DE TAL"',
            '"12345678901"',
            '"45"',
            '"PT"',
            '"PARTIDO DOS TRABALHADORES"',
            '"98765432000111"',
            '"GRAFICA ABC LTDA"',
            '"GRAFICA ABC LTDA ME"',
            '"1813001"',
            '"Impressao"',
            '"SP"',
            '"15000,00"',
            '"10/08/2022"',
            '"Santinhos"',
            '"Nota Fiscal"',
            '"NF001"',
            '"Fundo partidario"',
        ]
    )
    return f"{header}\n{row}\n"


def _make_gen1_csv_content() -> str:
    """Build a minimal Gen1 (2002) despesas CSV for testing."""
    header = ";".join(
        [
            '"SG_UF"',
            '"SG_PART"',
            '"DS_CARGO"',
            '"NO_CAND"',
            '"NR_CAND"',
            '"DT_DOC_DESP"',
            '"CD_CPF_CGC"',
            '"NO_FOR"',
            '"VR_DESPESA"',
            '"DS_TITULO"',
        ]
    )
    row = ";".join(
        [
            '"AC"',
            '"PL"',
            '"Deputado Estadual"',
            '"JOAO SILVA"',
            '"22234"',
            '"14/08/2002"',
            '"04116398000187"',
            '"ACME LTDA"',
            '"160,00"',
            '"Publicidade"',
        ]
    )
    return f"{header}\n{row}\n"


def _make_zip_with_csv(csv_name: str, csv_content: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(csv_name, csv_content.encode("utf-8"))
    return buf.getvalue()


_FAKE_META = _YearMeta(url="http://test/file.zip", content_length=1234, etag='"abc"')


class TestExpenseCheckpoint:
    def test_load_empty(self, tmp_path: Path) -> None:
        cp = _Checkpoint.load(tmp_path)
        assert cp.completed_years == set()
        assert cp.year_meta == {}

    def test_save_and_load(self, tmp_path: Path) -> None:
        cp = _Checkpoint(completed_years={2022, 2024}, year_meta={2022: _FAKE_META})
        cp.save(tmp_path)
        loaded = _Checkpoint.load(tmp_path)
        assert loaded.completed_years == {2022, 2024}
        assert loaded.year_meta[2022].etag == '"abc"'

    def test_separate_from_donation_checkpoint(self, tmp_path: Path) -> None:
        """Expense checkpoint uses different filename from donation checkpoint."""
        cp = _Checkpoint(completed_years={2022})
        cp.save(tmp_path)
        assert (tmp_path / "_checkpoint_expenses.json").exists()
        assert not (tmp_path / "_checkpoint.json").exists()


class TestValidateYears:
    def test_valid_years(self) -> None:
        _validate_years((2002, 2022, 2024))  # should not raise

    def test_2018_rejected_with_specific_reason(self) -> None:
        with pytest.raises(ValueError, match="SQ_PRESTADOR_CONTAS"):
            _validate_years((2018,))

    def test_unimplemented_year_rejected(self) -> None:
        with pytest.raises(ValueError, match="not implemented"):
            _validate_years((2012,))

    def test_2016_rejected(self) -> None:
        with pytest.raises(ValueError, match="not implemented"):
            _validate_years((2016,))

    def test_2020_rejected(self) -> None:
        with pytest.raises(ValueError, match="not implemented"):
            _validate_years((2020,))


class TestFindDespesasFiles:
    def test_finds_contratadas_brasil(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "despesas_contratadas_candidatos_2022_BRASIL.csv"
        csv_file.write_text("header")
        result = _find_despesas_files(tmp_path, 2022)
        assert len(result) == 1
        assert result[0] == csv_file

    def test_finds_subdir_despesa_candidato(self, tmp_path: Path) -> None:
        subdir = tmp_path / "Candidato" / "Despesa"
        subdir.mkdir(parents=True)
        csv_file = subdir / "DespesaCandidato.csv"
        csv_file.write_text("header")
        result = _find_despesas_files(tmp_path, 2002)
        assert len(result) == 1
        assert result[0] == csv_file

    def test_finds_per_uf_txt(self, tmp_path: Path) -> None:
        for uf in ("PE", "SP"):
            subdir = tmp_path / "candidato" / uf
            subdir.mkdir(parents=True)
            (subdir / "DespesasCandidatos.txt").write_text("header")
        result = _find_despesas_files(tmp_path, 2010)
        assert len(result) == 2

    def test_excludes_comite(self, tmp_path: Path) -> None:
        cand_dir = tmp_path / "Candidato" / "Despesa"
        cand_dir.mkdir(parents=True)
        (cand_dir / "DespesaCandidato.csv").write_text("header")
        comite_dir = tmp_path / "Comitê" / "Despesa"
        comite_dir.mkdir(parents=True)
        (comite_dir / "DespesaComitê.csv").write_text("header")
        result = _find_despesas_files(tmp_path, 2002)
        assert len(result) == 1
        assert "Candidato" in str(result[0])

    def test_excludes_pagas(self, tmp_path: Path) -> None:
        (tmp_path / "despesas_contratadas_candidatos_2022_BRASIL.csv").write_text("header")
        (tmp_path / "despesas_pagas_candidatos_2022_BRASIL.csv").write_text("header")
        result = _find_despesas_files(tmp_path, 2022)
        assert len(result) == 1
        assert "contratadas" in result[0].name

    def test_empty_dir(self, tmp_path: Path) -> None:
        result = _find_despesas_files(tmp_path, 2022)
        assert result == []


class TestFetchExpenseData:
    def test_dry_run(self, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        config = TseExpenseFetchConfig(output_dir=output_dir, years=(2022,), dry_run=True)
        result = fetch_expense_data(config)
        assert result == output_dir
        assert output_dir.exists()

    def test_dry_run_validates_years(self, tmp_path: Path) -> None:
        config = TseExpenseFetchConfig(output_dir=tmp_path, years=(2018,), dry_run=True)
        with pytest.raises(ValueError, match="SQ_PRESTADOR_CONTAS"):
            fetch_expense_data(config)

    def test_unsupported_year_rejected(self, tmp_path: Path) -> None:
        config = TseExpenseFetchConfig(output_dir=tmp_path, years=(2014,))
        with pytest.raises(ValueError, match="not implemented"):
            fetch_expense_data(config)

    @patch("atlas_stf.tse._runner_expenses._download_year_zip_base")
    def test_download_and_parse(self, mock_download: MagicMock, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        csv_content = _make_gen6_csv_content()
        zip_bytes = _make_zip_with_csv("despesas_contratadas_candidatos_2022_BRASIL.csv", csv_content)
        zip_path = output_dir / "tse_2022.zip"
        output_dir.mkdir(parents=True, exist_ok=True)
        zip_path.write_bytes(zip_bytes)
        mock_download.return_value = (zip_path, _FAKE_META)

        config = TseExpenseFetchConfig(output_dir=output_dir, years=(2022,))
        fetch_expense_data(config)

        raw_path = output_dir / "campaign_expenses_raw.jsonl"
        assert raw_path.exists()
        records = [json.loads(line) for line in raw_path.read_text().strip().split("\n")]
        assert len(records) == 1
        assert records[0]["supplier_name"] == "GRAFICA ABC LTDA"
        assert records[0]["expense_amount"] == 15000.0
        assert records[0]["election_year"] == 2022
        assert not (output_dir / "extracted_expenses_2022").exists()

    @patch("atlas_stf.tse._runner_expenses._download_year_zip_base")
    def test_checkpoint_resumability(self, mock_download: MagicMock, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        output_dir.mkdir(parents=True)

        cp = _Checkpoint(completed_years={2022})
        cp.save(output_dir)
        raw_path = output_dir / "campaign_expenses_raw.jsonl"
        raw_path.write_text(json.dumps({"supplier_name": "EXISTING", "election_year": 2022}) + "\n")

        csv_content = _make_gen6_csv_content()
        zip_bytes = _make_zip_with_csv("despesas_contratadas_candidatos_2024_BRASIL.csv", csv_content)
        zip_path = output_dir / "tse_2024.zip"
        zip_path.write_bytes(zip_bytes)
        mock_download.return_value = (zip_path, _FAKE_META)

        config = TseExpenseFetchConfig(output_dir=output_dir, years=(2022, 2024))
        fetch_expense_data(config)

        records = [json.loads(line) for line in raw_path.read_text().strip().split("\n")]
        assert len(records) == 2
        assert mock_download.call_count == 1

    @patch("atlas_stf.tse._runner_expenses._download_year_zip_base")
    def test_empty_zip(self, mock_download: MagicMock, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("readme.txt", "No CSV here")
        zip_path = output_dir / "tse_2022.zip"
        output_dir.mkdir(parents=True, exist_ok=True)
        zip_path.write_bytes(buf.getvalue())
        mock_download.return_value = (zip_path, _FAKE_META)

        config = TseExpenseFetchConfig(output_dir=output_dir, years=(2022,))
        fetch_expense_data(config)

        raw_path = output_dir / "campaign_expenses_raw.jsonl"
        assert raw_path.exists()
        assert raw_path.read_text().strip() == ""

    @patch("atlas_stf.tse._runner_expenses._download_year_zip_base")
    def test_empty_year_not_checkpointed(self, mock_download: MagicMock, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("readme.txt", "No CSV here")
        zip_path = output_dir / "tse_2022.zip"
        output_dir.mkdir(parents=True, exist_ok=True)
        zip_path.write_bytes(buf.getvalue())
        mock_download.return_value = (zip_path, _FAKE_META)

        config = TseExpenseFetchConfig(output_dir=output_dir, years=(2022,))
        fetch_expense_data(config)

        cp = _Checkpoint.load(output_dir)
        assert 2022 not in cp.completed_years

    @patch("atlas_stf.tse._runner_expenses._download_year_zip_base")
    def test_force_refresh_no_duplicate(self, mock_download: MagicMock, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        csv_content = _make_gen6_csv_content()
        zip_bytes = _make_zip_with_csv("despesas_contratadas_candidatos_2022_BRASIL.csv", csv_content)
        zip_path = output_dir / "tse_2022.zip"
        output_dir.mkdir(parents=True, exist_ok=True)

        cp = _Checkpoint(completed_years={2022}, year_meta={2022: _FAKE_META})
        cp.save(output_dir)
        raw_path = output_dir / "campaign_expenses_raw.jsonl"
        old_records = [
            json.dumps({"supplier_name": "OLD_A", "election_year": 2022}),
            json.dumps({"supplier_name": "OLD_B", "election_year": 2022}),
        ]
        raw_path.write_text("\n".join(old_records) + "\n")

        zip_path.write_bytes(zip_bytes)
        mock_download.return_value = (zip_path, _FAKE_META)

        config = TseExpenseFetchConfig(output_dir=output_dir, years=(2022,), force_refresh=True)
        fetch_expense_data(config)

        records = [json.loads(line) for line in raw_path.read_text().strip().split("\n") if line.strip()]
        assert len(records) == 1
        assert records[0]["supplier_name"] == "GRAFICA ABC LTDA"
        assert all("OLD_" not in r["supplier_name"] for r in records)

    @patch("atlas_stf.tse._runner_expenses._download_year_zip_base")
    def test_force_refresh_preserves_other_years(self, mock_download: MagicMock, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

        cp = _Checkpoint(completed_years={2002, 2022}, year_meta={2002: _FAKE_META, 2022: _FAKE_META})
        cp.save(output_dir)
        raw_path = output_dir / "campaign_expenses_raw.jsonl"
        existing = [
            json.dumps({"supplier_name": "KEEP_2002", "election_year": 2002}),
            json.dumps({"supplier_name": "OLD_2022", "election_year": 2022}),
        ]
        raw_path.write_text("\n".join(existing) + "\n")

        csv_content = _make_gen6_csv_content()
        zip_bytes = _make_zip_with_csv("despesas_contratadas_candidatos_2022_BRASIL.csv", csv_content)
        zip_path = output_dir / "tse_2022.zip"
        zip_path.write_bytes(zip_bytes)
        mock_download.return_value = (zip_path, _FAKE_META)

        config = TseExpenseFetchConfig(output_dir=output_dir, years=(2022,), force_refresh=True)
        fetch_expense_data(config)

        records = [json.loads(line) for line in raw_path.read_text().strip().split("\n") if line.strip()]
        names = [r["supplier_name"] for r in records]
        assert "KEEP_2002" in names
        assert "OLD_2022" not in names
        assert "GRAFICA ABC LTDA" in names

    @patch("atlas_stf.tse._runner_expenses._download_year_zip_base")
    def test_unchanged_file_skipped(self, mock_download: MagicMock, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        output_dir.mkdir(parents=True)

        raw_path = output_dir / "campaign_expenses_raw.jsonl"
        raw_path.write_text(json.dumps({"supplier_name": "OLD", "election_year": 2022}) + "\n")
        cp = _Checkpoint(completed_years={2022}, year_meta={2022: _FAKE_META})
        cp.save(output_dir)

        mock_download.return_value = (None, None)

        config = TseExpenseFetchConfig(output_dir=output_dir, years=(2022,))
        fetch_expense_data(config)

        records = [json.loads(line) for line in raw_path.read_text().strip().split("\n")]
        assert len(records) == 1
        assert records[0]["supplier_name"] == "OLD"

    @patch("atlas_stf.tse._runner_expenses._download_year_zip_base")
    def test_record_hash_deterministic(self, mock_download: MagicMock, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        csv_content = _make_gen6_csv_content()
        zip_bytes = _make_zip_with_csv("despesas_contratadas_candidatos_2022_BRASIL.csv", csv_content)

        hashes: list[str] = []
        for run_idx in range(2):
            run_dir = output_dir / f"run{run_idx}"
            zip_path = run_dir / "tse_2022.zip"
            run_dir.mkdir(parents=True, exist_ok=True)
            zip_path.write_bytes(zip_bytes)
            mock_download.return_value = (zip_path, _FAKE_META)

            config = TseExpenseFetchConfig(output_dir=run_dir, years=(2022,))
            fetch_expense_data(config)

            raw_path = run_dir / "campaign_expenses_raw.jsonl"
            records = [json.loads(line) for line in raw_path.read_text().strip().split("\n")]
            hashes.append(records[0]["record_hash"])

        assert hashes[0] == hashes[1]

    @patch("atlas_stf.tse._runner_expenses._download_year_zip_base")
    def test_gen1_zip_parsing(self, mock_download: MagicMock, tmp_path: Path) -> None:
        """Gen1 (2002) CSV inside subdir path works end-to-end."""
        output_dir = tmp_path / "output"
        csv_content = _make_gen1_csv_content()
        csv_name = "prestacao_contas_2002/2002/Candidato/Despesa/DespesaCandidato.csv"
        zip_bytes = _make_zip_with_csv(csv_name, csv_content)
        zip_path = output_dir / "tse_2002.zip"
        output_dir.mkdir(parents=True, exist_ok=True)
        zip_path.write_bytes(zip_bytes)
        mock_download.return_value = (zip_path, _FAKE_META)

        config = TseExpenseFetchConfig(output_dir=output_dir, years=(2002,))
        fetch_expense_data(config)

        raw_path = output_dir / "campaign_expenses_raw.jsonl"
        records = [json.loads(line) for line in raw_path.read_text().strip().split("\n")]
        assert len(records) == 1
        assert records[0]["candidate_name"] == "JOAO SILVA"
        assert records[0]["candidate_cpf"] is None  # absent in Gen1
        assert records[0]["supplier_name"] == "ACME LTDA"

    def test_default_years_uses_supported(self, tmp_path: Path) -> None:
        config = TseExpenseFetchConfig(output_dir=tmp_path, years=None, dry_run=True)
        fetch_expense_data(config)  # should not raise — uses _SUPPORTED_EXPENSE_YEARS


class TestProvenanceFields:
    @patch("atlas_stf.tse._runner_expenses._download_year_zip_base")
    def test_provenance_complete(self, mock_download: MagicMock, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        csv_content = _make_gen6_csv_content()
        zip_bytes = _make_zip_with_csv("despesas_contratadas_candidatos_2022_BRASIL.csv", csv_content)
        zip_path = output_dir / "tse_2022.zip"
        output_dir.mkdir(parents=True, exist_ok=True)
        zip_path.write_bytes(zip_bytes)
        mock_download.return_value = (zip_path, _FAKE_META)

        config = TseExpenseFetchConfig(output_dir=output_dir, years=(2022,))
        fetch_expense_data(config)

        raw_path = output_dir / "campaign_expenses_raw.jsonl"
        records = [json.loads(line) for line in raw_path.read_text().strip().split("\n")]
        r = records[0]

        # record_hash: 64 hex chars
        assert len(r["record_hash"]) == 64
        assert re.fullmatch(r"[0-9a-f]{64}", r["record_hash"])

        # source_file: relative path (not basename)
        assert r["source_file"] == "despesas_contratadas_candidatos_2022_BRASIL.csv"

        # source_url: present and not empty
        assert r["source_url"] == _FAKE_META.url

        # collected_at: ISO timestamp
        assert "T" in r["collected_at"]

        # ingest_run_id: valid UUID
        assert re.fullmatch(
            r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
            r["ingest_run_id"],
        )


class TestSupportedExpenseYears:
    def test_expected_years(self) -> None:
        assert _SUPPORTED_EXPENSE_YEARS == (2002, 2004, 2006, 2008, 2010, 2022, 2024)

    def test_2018_not_in_supported(self) -> None:
        assert 2018 not in _SUPPORTED_EXPENSE_YEARS
