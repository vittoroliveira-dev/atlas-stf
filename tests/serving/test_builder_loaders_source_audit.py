"""Tests for source audit sanitization in serving builder loaders."""

from __future__ import annotations

from pathlib import Path

from atlas_stf.serving._builder_loaders import build_source_audits
from atlas_stf.serving._builder_utils import SourceFile


def test_build_source_audits_uses_label_slash_filename(tmp_path: Path):
    csv_file = tmp_path / "decisoes" / "raw.csv"
    csv_file.parent.mkdir(parents=True, exist_ok=True)
    csv_file.write_text("header\n", encoding="utf-8")
    source = SourceFile(label="decisoes", category="raw", path=csv_file)
    result = build_source_audits([source])
    assert len(result) == 1
    assert result[0].relative_path == "decisoes/raw.csv"


def test_build_source_audits_never_contains_absolute_path(tmp_path: Path):
    files = []
    for name in ["ceis.csv", "receitas.csv", "staging.csv"]:
        p = tmp_path / name
        p.write_text("header\n", encoding="utf-8")
        files.append(p)
    sources = [
        SourceFile(label="cgu", category="external", path=files[0]),
        SourceFile(label="tse", category="external", path=files[1]),
        SourceFile(label="staging", category="raw", path=files[2]),
    ]
    result = build_source_audits(sources)
    for audit in result:
        assert not audit.relative_path.startswith("/"), f"Absolute path leaked: {audit.relative_path}"
        assert ".." not in audit.relative_path
