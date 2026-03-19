from __future__ import annotations

import json
from pathlib import Path

import pytest

from atlas_stf.validate import (
    _join_checks,
    _read_column_values,
    _read_header,
    _validate_file,
    validate_staging,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_csv(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


# ---------------------------------------------------------------------------
# _read_header
# ---------------------------------------------------------------------------


def test_read_header_returns_column_names(tmp_path: Path):
    csv_file = tmp_path / "sample.csv"
    _write_csv(csv_file, "col_a,col_b,col_c\n1,2,3\n")

    headers = _read_header(csv_file)

    assert headers == ["col_a", "col_b", "col_c"]


def test_read_header_single_column(tmp_path: Path):
    csv_file = tmp_path / "single.csv"
    _write_csv(csv_file, "only_col\nval\n")

    headers = _read_header(csv_file)

    assert headers == ["only_col"]


# ---------------------------------------------------------------------------
# _read_column_values
# ---------------------------------------------------------------------------


def test_read_column_values_returns_series(tmp_path: Path):
    csv_file = tmp_path / "data.csv"
    _write_csv(csv_file, "processo,outro\nRCL 1,x\nRCL 2,y\n")

    series = _read_column_values(csv_file, "processo")

    assert list(series) == ["RCL 1", "RCL 2"]


def test_read_column_values_with_null_values(tmp_path: Path):
    csv_file = tmp_path / "data.csv"
    # NA value in the middle; pandas reads "" as NaN when dtype=str is used
    _write_csv(csv_file, "processo\nRCL 1\nNA\nRCL 3\n")

    series = _read_column_values(csv_file, "processo")

    assert len(series) == 3
    assert list(series.dropna()) == ["RCL 1", "RCL 3"]


# ---------------------------------------------------------------------------
# _validate_file — missing file path (line 67)
# ---------------------------------------------------------------------------


def test_validate_file_missing_returns_missing_status(tmp_path: Path):
    nonexistent = tmp_path / "does_not_exist.csv"

    result = _validate_file(nonexistent)

    assert result.exists is False
    assert result.status == "missing"
    assert result.row_count == 0
    assert result.column_count == 0
    assert result.missing_columns == []
    assert result.filename == "does_not_exist.csv"


# ---------------------------------------------------------------------------
# _validate_file — existing file with all columns
# ---------------------------------------------------------------------------


def test_validate_file_ok_for_unknown_filename(tmp_path: Path):
    """Files with no validation rule should always be status=ok."""
    csv_file = tmp_path / "unknown.csv"
    _write_csv(csv_file, "any_col\nvalue\n")

    result = _validate_file(csv_file)

    assert result.exists is True
    assert result.status == "ok"
    assert result.missing_columns == []
    assert result.row_count == 1


# ---------------------------------------------------------------------------
# validate_staging — no filename (line 133, glob branch)
# ---------------------------------------------------------------------------


def test_validate_staging_scans_all_csvs(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    _write_csv(staging_dir / "reclamacoes.csv", "processo,numero_unico,data_autuacao\nRCL 1,0001,2026-01-01\n")
    _write_csv(staging_dir / "unknown.csv", "col_x\nval\n")

    report = validate_staging(staging_dir)

    filenames = {f.filename for f in report.files}
    assert "reclamacoes.csv" in filenames
    assert "unknown.csv" in filenames


def test_validate_staging_overall_status_ok_when_all_files_ok(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    # File with no known validation rule → always ok
    _write_csv(staging_dir / "unknown.csv", "col_x\nval\n")

    report = validate_staging(staging_dir)

    assert report.overall_status == "ok"


def test_validate_staging_overall_status_failed_when_any_file_missing(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    # reclamacoes.csv missing required columns
    _write_csv(staging_dir / "reclamacoes.csv", "processo\nRCL 1\n")

    report = validate_staging(staging_dir)

    assert report.overall_status == "failed"


def test_validate_staging_default_output_path(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    _write_csv(staging_dir / "unknown.csv", "col_x\nval\n")

    validate_staging(staging_dir)

    default_output = staging_dir / "_validation.json"
    assert default_output.exists()
    payload = json.loads(default_output.read_text(encoding="utf-8"))
    assert "generated_at" in payload


# ---------------------------------------------------------------------------
# validate_staging — ok for reclamacoes (existing test, kept for baseline)
# ---------------------------------------------------------------------------


def test_validate_staging_ok_for_reclamacoes(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    (staging_dir / "reclamacoes.csv").write_text(
        "processo,numero_unico,data_autuacao,relator_atual\nRCL 1,0001,2026-03-06,MIN X\n",
        encoding="utf-8",
    )

    output = tmp_path / "validation.json"
    report = validate_staging(staging_dir, output_path=output, filename="reclamacoes.csv")

    assert report.overall_status == "ok"
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["files"][0]["missing_columns"] == []


def test_validate_staging_flags_missing_columns(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    (staging_dir / "reclamacoes.csv").write_text("processo\nRCL 1\n", encoding="utf-8")

    report = validate_staging(staging_dir, filename="reclamacoes.csv")

    assert report.overall_status == "failed"
    assert report.files[0].status == "failed"
    assert "numero_unico" in report.files[0].missing_columns


def test_validate_staging_rejects_filename_outside_input_dir(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    outside = tmp_path / "outside.csv"
    outside.write_text("processo\nRCL 1\n", encoding="utf-8")

    with pytest.raises(ValueError, match="inside input_dir"):
        validate_staging(staging_dir, filename="../outside.csv")


# ---------------------------------------------------------------------------
# _join_checks
# ---------------------------------------------------------------------------


def test_join_checks_returns_empty_when_fewer_than_two_process_files(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    _write_csv(staging_dir / "reclamacoes.csv", "processo,numero_unico\nRCL 1,001\n")

    result = _join_checks(staging_dir, ["reclamacoes.csv"])

    assert result == []


def test_join_checks_returns_empty_when_no_processo_column(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    _write_csv(staging_dir / "file_a.csv", "col_x\nval\n")
    _write_csv(staging_dir / "file_b.csv", "col_y\nval\n")

    result = _join_checks(staging_dir, ["file_a.csv", "file_b.csv"])

    assert result == []


def test_join_checks_produces_check_for_overlapping_files(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    _write_csv(staging_dir / "decisoes.csv", "processo\nRCL 1\nRCL 2\n")
    _write_csv(staging_dir / "reclamacoes.csv", "processo\nRCL 1\nRCL 3\n")

    result = _join_checks(staging_dir, ["decisoes.csv", "reclamacoes.csv"])

    assert len(result) == 1
    check = result[0]
    assert check.key == "processo"
    assert check.anchor_file == "decisoes.csv"
    assert check.target_file == "reclamacoes.csv"
    assert check.overlap_count == 1
    assert check.status == "ok"


def test_join_checks_status_incerto_when_no_overlap(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    _write_csv(staging_dir / "decisoes.csv", "processo\nRCL 1\nRCL 2\n")
    _write_csv(staging_dir / "reclamacoes.csv", "processo\nRCL 99\n")

    result = _join_checks(staging_dir, ["decisoes.csv", "reclamacoes.csv"])

    assert len(result) == 1
    assert result[0].status == "incerto"
    assert result[0].overlap_count == 0


def test_join_checks_anchor_defaults_to_decisoes_when_present(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    _write_csv(staging_dir / "decisoes.csv", "processo\nRCL 1\n")
    _write_csv(staging_dir / "reclamacoes.csv", "processo\nRCL 1\n")
    _write_csv(staging_dir / "acervo.csv", "processo\nRCL 1\n")

    result = _join_checks(staging_dir, ["decisoes.csv", "reclamacoes.csv", "acervo.csv"])

    anchor_files = {check.anchor_file for check in result}
    assert anchor_files == {"decisoes.csv"}


def test_join_checks_anchor_uses_first_file_when_no_decisoes(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    _write_csv(staging_dir / "acervo.csv", "processo\nRCL 1\nRCL 2\n")
    _write_csv(staging_dir / "reclamacoes.csv", "processo\nRCL 1\n")

    result = _join_checks(staging_dir, ["acervo.csv", "reclamacoes.csv"])

    assert len(result) == 1
    assert result[0].anchor_file == "acervo.csv"


def test_join_checks_skips_missing_files(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    _write_csv(staging_dir / "decisoes.csv", "processo\nRCL 1\n")
    # reclamacoes.csv deliberately not created

    result = _join_checks(staging_dir, ["decisoes.csv", "reclamacoes.csv"])

    # Only one file with 'processo' → fewer than 2 → returns []
    assert result == []


def test_join_checks_overlap_ratio_is_zero_for_empty_target(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    _write_csv(staging_dir / "decisoes.csv", "processo\nRCL 1\n")
    _write_csv(staging_dir / "reclamacoes.csv", "processo\n")

    result = _join_checks(staging_dir, ["decisoes.csv", "reclamacoes.csv"])

    # target is empty → target_values is empty set → overlap_ratio = 0.0 → status incerto
    assert len(result) == 1
    assert result[0].overlap_ratio == 0.0
    assert result[0].status == "incerto"


def test_validate_staging_includes_join_checks_when_no_filename(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    _write_csv(
        staging_dir / "decisoes.csv",
        "processo,idfatodecisao,data_da_decisao,tipo_decisao,andamento_decisao,data_de_autuacao,data_baixa\n"
        "RCL 1,1,2026-01-01,X,Y,2025-01-01,2026-01-02\n",
    )
    _write_csv(
        staging_dir / "reclamacoes.csv",
        "processo,numero_unico,data_autuacao\nRCL 1,001,2026-01-01\n",
    )

    report = validate_staging(staging_dir)

    assert len(report.join_checks) >= 1
    assert report.join_checks[0].key == "processo"


def test_validate_staging_no_join_checks_when_single_filename(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    _write_csv(staging_dir / "reclamacoes.csv", "processo,numero_unico,data_autuacao\nRCL 1,001,2026-01-01\n")

    report = validate_staging(staging_dir, filename="reclamacoes.csv")

    assert report.join_checks == []
