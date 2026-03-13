from __future__ import annotations

import json
from pathlib import Path

import pytest

from atlas_stf.validate import validate_staging


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
