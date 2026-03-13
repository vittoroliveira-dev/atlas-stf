"""Integration tests for the staging pipeline runner."""

from pathlib import Path

import pandas as pd
import pytest

from atlas_stf.staging._config import CONFIGS
from atlas_stf.staging._runner import RowCountMismatchError, clean_all, process_file

RAW_DIR = Path("data/raw/transparencia")
STAGING_DIR = Path("data/staging/transparencia")


@pytest.fixture
def small_raw_csv(tmp_path: Path) -> tuple[Path, Path]:
    """Create a small raw CSV for testing."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()

    df = pd.DataFrame(
        {
            "Data da autuação": ["06/01/2003", "15/02/2004"],
            "Data da baixa": ["02/10/2003", "-"],
            "Data do andamento": ["06/01/2003", "15/02/2004"],
            "Assunto completo": [
                "1 - DIREITO CIVIL || OBRIGAÇÕES",
                "1 - TRIBUTÁRIO || ALÍQUOTA\n2 - PENAL || CRIMES",
            ],
            "Nome": ["  test  ", "  data  "],
        }
    )
    df.to_csv(raw_dir / "distribuidos.csv", index=False)
    return raw_dir, staging_dir


def test_process_file_roundtrip(small_raw_csv: tuple[Path, Path]):
    raw_dir, staging_dir = small_raw_csv
    config = CONFIGS["distribuidos.csv"]
    record = process_file(config, raw_dir, staging_dir)

    assert record is not None
    assert record.raw_row_count == record.staging_row_count == 2
    assert record.raw_sha256 != record.staging_sha256

    # Read staging output
    staging_df = pd.read_csv(staging_dir / "distribuidos.csv")
    assert len(staging_df) == 2

    # Column names should be snake_case
    assert all("_" in col or col.isalpha() for col in staging_df.columns)

    # Dates should be normalized
    assert staging_df["data_da_autuacao"].iloc[0] == "2003-01-06"

    # Whitespace should be stripped
    assert staging_df["nome"].iloc[0] == "test"


def test_dry_run_produces_no_output(small_raw_csv: tuple[Path, Path]):
    raw_dir, staging_dir = small_raw_csv
    config = CONFIGS["distribuidos.csv"]
    record = process_file(config, raw_dir, staging_dir, dry_run=True)

    assert record is None
    assert not (staging_dir / "distribuidos.csv").exists()


def test_clean_all_records_cross_file_reconciliation_warning(tmp_path: Path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()

    for filename, config in CONFIGS.items():
        path = raw_dir / filename
        if path.exists():
            continue
        headers = list(dict.fromkeys(config.required_fields + config.date_columns))
        pd.DataFrame(columns=headers).to_csv(path, index=False)

    pd.DataFrame(
        [
            {
                "Processo": "AC 1",
                "Número único": "00000000000000000001",
                "Data autuação": "06/01/2003",
            }
        ]
    ).to_csv(raw_dir / "acervo.csv", index=False)

    pd.DataFrame(
        [
            {
                "idFatoDecisao": "1",
                "Processo": "AC 999",
                "Data da decisão": "06/01/2003 00:00:00",
                "Tipo decisão": "Negado seguimento",
                "Andamento decisão": "Baixa",
            }
        ]
    ).to_csv(raw_dir / "decisoes.csv", index=False)

    records = clean_all(raw_dir=raw_dir, staging_dir=staging_dir)

    records_by_file = {record.filename: record for record in records}
    assert "acervo.csv" in records_by_file
    assert "decisoes.csv" in records_by_file
    assert any(
        "cross_file_reconciliation:decisoes.csv" in warning for warning in records_by_file["decisoes.csv"].warnings
    )
    assert (staging_dir / "acervo.csv").exists()
    assert (staging_dir / "decisoes.csv").exists()


def test_process_file_fails_on_row_count_mismatch(small_raw_csv: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch):
    raw_dir, staging_dir = small_raw_csv
    config = CONFIGS["distribuidos.csv"]

    def _drop_last_row(df: pd.DataFrame) -> pd.DataFrame:
        return df.iloc[:-1].copy()

    monkeypatch.setattr("atlas_stf.staging._runner.strip_whitespace", _drop_last_row)

    with pytest.raises(RowCountMismatchError, match=r"Row count mismatch: raw=2, staging=1"):
        process_file(config, raw_dir, staging_dir)

    assert not (staging_dir / "distribuidos.csv").exists()


@pytest.mark.skipif(not (RAW_DIR / "reclamacoes.csv").exists(), reason="Raw data not available")
def test_real_reclamacoes_row_count():
    """Smoke test: process reclamacoes (smallest simple CSV) and check row count."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        staging = Path(tmp)
        config = CONFIGS["reclamacoes.csv"]
        record = process_file(config, RAW_DIR, staging)
        assert record is not None
        assert record.raw_row_count == record.staging_row_count
        assert record.raw_row_count > 0
