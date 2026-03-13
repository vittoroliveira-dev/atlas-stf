from __future__ import annotations

import json
from pathlib import Path

import pytest

from atlas_stf.profile import profile_dataset, profile_staging


def test_profile_dataset_detects_date_and_key(tmp_path: Path):
    path = tmp_path / "sample.csv"
    path.write_text("processo,data_decisao,nome\nP1,2026-03-06,A\nP2,2026-03-07,B\n", encoding="utf-8")

    profile = profile_dataset(path)
    process_col = next(column for column in profile.columns if column.name == "processo")
    date_col = next(column for column in profile.columns if column.name == "data_decisao")

    assert profile.row_count == 2
    assert process_col.looks_like_key is True
    assert date_col.looks_like_date is True


def test_profile_staging_writes_json_files(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    (staging_dir / "sample.csv").write_text("a,b\n1,2\n", encoding="utf-8")

    output_dir = tmp_path / "profile"
    profiles = profile_staging(staging_dir, output_dir=output_dir)

    assert len(profiles) == 1
    payload = json.loads((output_dir / "sample.json").read_text(encoding="utf-8"))
    assert payload["filename"] == "sample.csv"
    assert (output_dir / "summary.json").exists()


def test_profile_staging_rejects_filename_outside_input_dir(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    outside = tmp_path / "outside.csv"
    outside.write_text("a,b\n1,2\n", encoding="utf-8")

    with pytest.raises(ValueError, match="inside input_dir"):
        profile_staging(staging_dir, filename="../outside.csv")
