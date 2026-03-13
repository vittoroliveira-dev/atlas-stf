from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.cli import main


def test_cli_manifest_raw(tmp_path: Path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    (raw_dir / "sample.csv").write_text("a,b\n1,2\n", encoding="utf-8")
    output = tmp_path / "manifest.jsonl"

    code = main(["manifest", "raw", "--dir", str(raw_dir), "--output", str(output)])

    assert code == 0
    lines = output.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["filename"] == "sample.csv"


def test_cli_profile_staging(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    (staging_dir / "sample.csv").write_text("data\n2026-03-06\n", encoding="utf-8")
    out_dir = tmp_path / "profiles"

    code = main(["profile", "staging", "--dir", str(staging_dir), "--output-dir", str(out_dir)])

    assert code == 0
    assert (out_dir / "sample.json").exists()
    assert (out_dir / "summary.json").exists()


def test_cli_validate_staging(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    (staging_dir / "reclamacoes.csv").write_text(
        "processo,numero_unico,data_autuacao\nRCL 1,0001,2026-03-06\n",
        encoding="utf-8",
    )
    output = tmp_path / "validation.json"

    code = main(
        ["validate", "staging", "--dir", str(staging_dir), "--output", str(output), "--file", "reclamacoes.csv"]
    )

    assert code == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["files"][0]["status"] == "ok"


def test_cli_audit_stage(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    (staging_dir / "acervo.csv").write_text("a\n1\n", encoding="utf-8")
    (staging_dir / "_audit.jsonl").write_text(
        json.dumps({"output_file": "acervo.csv"}) + "\n",
        encoding="utf-8",
    )
    output = tmp_path / "stage_audit.json"

    code = main(["audit", "stage", "--staging-dir", str(staging_dir), "--output", str(output)])

    assert code == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["overall_status"] == "ok"
