"""Tests for tse/_runner_party_org.py."""

from __future__ import annotations

import io
import json
import re
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from atlas_stf.tse._config import TSE_PARTY_ORG_YEARS, TsePartyOrgFetchConfig
from atlas_stf.tse._runner import _YearMeta
from atlas_stf.tse._runner_party_org import (
    _Checkpoint,
    _download_year_zip,
    fetch_party_org_data,
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


def _make_receitas_row() -> str:
    vals = [
        "01/01/2025",
        "10:00:00",
        "2024",
        "1",
        "Eleicao Ordinaria",
        "Final",
        "01/03/2025",
        "1001",
        "M",
        "Municipal",
        "SP",
        "71072",
        "SAO PAULO",
        "12345678000199",
        "13",
        "PT",
        "PARTIDO DOS TRABALHADORES",
        "1",
        "Fundo Partidario",
        "1",
        "Recursos PF",
        "1",
        "Doacao",
        "1",
        "Dinheiro",
        "4110700",
        "Incorporacao",
        "11122233344",
        "ACME DOADOR",
        "ACME DOADOR",
        "",
        "",
        "SP",
        "71072",
        "SAO PAULO",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "001",
        "DOC001",
        "10001",
        "15/08/2024",
        "Doacao",
        "5000,00",
    ]
    return ";".join(f'"{v}"' for v in vals)


def _make_despesas_row() -> str:
    vals = [
        "01/01/2025",
        "10:00:00",
        "2024",
        "1",
        "Eleicao Ordinaria",
        "Final",
        "01/03/2025",
        "2001",
        "E",
        "Estadual",
        "RJ",
        "RJ",
        "RIO DE JANEIRO",
        "",
        "",
        "98765432000188",
        "45",
        "PSDB",
        "PSDB NOME",
        "1",
        "PJ",
        "1811301",
        "Impressao",
        "55667788000111",
        "GRAFICA LTDA",
        "GRAFICA LTDA",
        "",
        "",
        "RJ",
        "60011",
        "RIO DE JANEIRO",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "NF",
        "NF12345",
        "1",
        "Gastos",
        "20001",
        "20/09/2024",
        "Material grafico",
        "12500,50",
    ]
    return ";".join(f'"{v}"' for v in vals)


def _make_zip_with_party_org(year: int) -> bytes:
    """Create a ZIP with receitas and despesas CSVs."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        receitas_csv = _RECEITAS_HEADER + "\n" + _make_receitas_row() + "\n"
        zf.writestr(f"receitas_orgaos_partidarios_{year}_BRASIL.csv", receitas_csv.encode("utf-8"))

        despesas_csv = _DESPESAS_HEADER + "\n" + _make_despesas_row() + "\n"
        zf.writestr(f"despesas_contratadas_orgaos_partidarios_{year}_BRASIL.csv", despesas_csv.encode("utf-8"))
    return buf.getvalue()


_FAKE_META = _YearMeta(url="http://test/party_org.zip", content_length=1234, etag='"abc"')


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
        cp = _Checkpoint(completed_years={2022, 2024}, year_meta={2022: _FAKE_META})
        cp.save(tmp_path)
        loaded = _Checkpoint.load(tmp_path)
        assert loaded.completed_years == {2022, 2024}
        assert loaded.year_meta[2022].etag == '"abc"'
        # Uses separate checkpoint file from candidates
        assert (tmp_path / "_checkpoint_party_org.json").exists()
        assert not (tmp_path / "_checkpoint.json").exists()

    def test_isolation_from_candidate_checkpoint(self, tmp_path: Path) -> None:
        """Party org checkpoint must not interfere with candidate checkpoint."""
        # Write candidate checkpoint
        candidate_data = {"completed_years": [2020], "year_meta": {}}
        (tmp_path / "_checkpoint.json").write_text(json.dumps(candidate_data))

        # Write party org checkpoint
        cp = _Checkpoint(completed_years={2024})
        cp.save(tmp_path)

        # Both should coexist independently
        assert (tmp_path / "_checkpoint.json").exists()
        assert (tmp_path / "_checkpoint_party_org.json").exists()

        loaded = _Checkpoint.load(tmp_path)
        assert loaded.completed_years == {2024}


class TestFetchPartyOrgData:
    def test_dry_run(self, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        config = TsePartyOrgFetchConfig(output_dir=output_dir, years=(2024,), dry_run=True)
        result = fetch_party_org_data(config)
        assert result == output_dir
        assert output_dir.exists()

    @patch("atlas_stf.tse._runner_party_org._download_year_zip")
    def test_download_and_parse(self, mock_download: MagicMock, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        zip_bytes = _make_zip_with_party_org(2024)
        zip_path = output_dir / "tse_party_org_2024.zip"
        output_dir.mkdir(parents=True, exist_ok=True)
        zip_path.write_bytes(zip_bytes)
        mock_download.return_value = (zip_path, _FAKE_META)

        config = TsePartyOrgFetchConfig(output_dir=output_dir, years=(2024,))
        fetch_party_org_data(config)

        raw_path = output_dir / "party_org_finance_raw.jsonl"
        assert raw_path.exists()
        records = [json.loads(line) for line in raw_path.read_text().strip().split("\n")]
        # Should have 1 revenue + 1 expense
        assert len(records) == 2
        kinds = {r["record_kind"] for r in records}
        assert kinds == {"revenue", "expense"}

        # All records should have actor_kind = party_org
        assert all(r["actor_kind"] == "party_org" for r in records)

        # Check revenue record
        revenue = [r for r in records if r["record_kind"] == "revenue"][0]
        assert revenue["counterparty_name"] == "ACME DOADOR"
        assert revenue["transaction_amount"] == 5000.0
        assert revenue["election_year"] == 2024

        # Check expense record
        expense = [r for r in records if r["record_kind"] == "expense"][0]
        assert expense["counterparty_name"] == "GRAFICA LTDA"
        assert expense["transaction_amount"] == 12500.50

        # Provenance fields
        for r in records:
            assert len(r["record_hash"]) == 64
            assert re.fullmatch(r"[0-9a-f]{64}", r["record_hash"])
            assert r["source_url"] == _FAKE_META.url
            assert "T" in r["collected_at"]
            assert re.fullmatch(
                r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
                r["ingest_run_id"],
            )

        # Extraction dir cleaned up
        assert not (output_dir / "extracted_party_org_2024").exists()

    @patch("atlas_stf.tse._runner_party_org._download_year_zip")
    def test_checkpoint_resumability(self, mock_download: MagicMock, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        output_dir.mkdir(parents=True)

        # Pre-populate checkpoint with 2022 done
        cp = _Checkpoint(completed_years={2022})
        cp.save(output_dir)
        raw_path = output_dir / "party_org_finance_raw.jsonl"
        raw_path.write_text(json.dumps({"counterparty_name": "EXISTING", "election_year": 2022}) + "\n")

        # Download 2024
        zip_bytes = _make_zip_with_party_org(2024)
        zip_path = output_dir / "tse_party_org_2024.zip"
        zip_path.write_bytes(zip_bytes)
        mock_download.return_value = (zip_path, _FAKE_META)

        config = TsePartyOrgFetchConfig(output_dir=output_dir, years=(2022, 2024))
        fetch_party_org_data(config)

        records = [json.loads(line) for line in raw_path.read_text().strip().split("\n")]
        # Existing + 2 new (revenue + expense)
        assert len(records) == 3
        assert mock_download.call_count == 1

    @patch("atlas_stf.tse._runner_party_org._download_year_zip")
    def test_empty_year_not_checkpointed(self, mock_download: MagicMock, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("readme.txt", "No CSV here")
        zip_path = output_dir / "tse_party_org_2024.zip"
        output_dir.mkdir(parents=True, exist_ok=True)
        zip_path.write_bytes(buf.getvalue())
        mock_download.return_value = (zip_path, _FAKE_META)

        config = TsePartyOrgFetchConfig(output_dir=output_dir, years=(2024,))
        fetch_party_org_data(config)

        cp = _Checkpoint.load(output_dir)
        assert 2024 not in cp.completed_years

    @patch("atlas_stf.tse._runner_party_org._download_year_zip")
    def test_force_refresh_no_duplication(self, mock_download: MagicMock, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Pre-populate with old records
        cp = _Checkpoint(completed_years={2024}, year_meta={2024: _FAKE_META})
        cp.save(output_dir)
        raw_path = output_dir / "party_org_finance_raw.jsonl"
        old_records = [json.dumps({"counterparty_name": "OLD", "election_year": 2024})]
        raw_path.write_text("\n".join(old_records) + "\n")

        # Force-refresh
        zip_bytes = _make_zip_with_party_org(2024)
        zip_path = output_dir / "tse_party_org_2024.zip"
        zip_path.write_bytes(zip_bytes)
        mock_download.return_value = (zip_path, _FAKE_META)

        config = TsePartyOrgFetchConfig(output_dir=output_dir, years=(2024,), force_refresh=True)
        fetch_party_org_data(config)

        records = [json.loads(line) for line in raw_path.read_text().strip().split("\n") if line.strip()]
        # 2 new records (revenue + expense), no old ones
        assert len(records) == 2
        assert all(r["counterparty_name"] != "OLD" for r in records)

    @patch("atlas_stf.tse._runner_party_org._download_year_zip")
    def test_record_kind_only_revenue_or_expense(self, mock_download: MagicMock, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        zip_bytes = _make_zip_with_party_org(2024)
        zip_path = output_dir / "tse_party_org_2024.zip"
        output_dir.mkdir(parents=True, exist_ok=True)
        zip_path.write_bytes(zip_bytes)
        mock_download.return_value = (zip_path, _FAKE_META)

        config = TsePartyOrgFetchConfig(output_dir=output_dir, years=(2024,))
        fetch_party_org_data(config)

        raw_path = output_dir / "party_org_finance_raw.jsonl"
        records = [json.loads(line) for line in raw_path.read_text().strip().split("\n")]
        for r in records:
            assert r["record_kind"] in ("revenue", "expense")

    @patch("atlas_stf.tse._runner_party_org.httpx.stream")
    def test_download_rejects_oversized_stream(self, mock_stream, monkeypatch, tmp_path: Path) -> None:
        monkeypatch.setattr("atlas_stf.tse._runner_party_org._TSE_MAX_DOWNLOAD_BYTES", 4)
        mock_stream.return_value = _FakeStreamResponse([b"12", b"345"])

        zip_path, meta = _download_year_zip(2024, tmp_path / "output", timeout=5)
        assert zip_path is None
        assert meta is None


class TestPartyOrgYears:
    def test_supported_years(self) -> None:
        assert TSE_PARTY_ORG_YEARS == (2018, 2020, 2022, 2024)

    def test_config_defaults_to_supported_years(self) -> None:
        config = TsePartyOrgFetchConfig()
        assert config.years == TSE_PARTY_ORG_YEARS

    def test_config_accepts_valid_year(self) -> None:
        config = TsePartyOrgFetchConfig(years=(2024,))
        assert config.years == (2024,)
