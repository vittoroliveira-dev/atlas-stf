"""Tests for AtomicJsonlWriter."""

from __future__ import annotations

from pathlib import Path

from atlas_stf.analytics._atomic_io import AtomicJsonlWriter


def test_creates_file_on_clean_exit(tmp_path: Path):
    target = tmp_path / "output.jsonl"
    with AtomicJsonlWriter(target) as fh:
        fh.write('{"key": "value"}\n')
    assert target.exists()
    assert target.read_text(encoding="utf-8") == '{"key": "value"}\n'
    assert not target.with_suffix(".jsonl.tmp").exists()


def test_preserves_original_on_error(tmp_path: Path):
    target = tmp_path / "output.jsonl"
    target.write_text("original content\n", encoding="utf-8")
    try:
        with AtomicJsonlWriter(target) as fh:
            fh.write("partial\n")
            raise RuntimeError("simulated crash")
    except RuntimeError:
        pass
    assert target.read_text(encoding="utf-8") == "original content\n"
    assert not target.with_suffix(".jsonl.tmp").exists()


def test_cleans_tmp_on_error_no_existing_target(tmp_path: Path):
    target = tmp_path / "new.jsonl"
    try:
        with AtomicJsonlWriter(target) as fh:
            fh.write("partial\n")
            raise RuntimeError("simulated crash")
    except RuntimeError:
        pass
    assert not target.exists()
    assert not target.with_suffix(".jsonl.tmp").exists()


def test_creates_parent_dirs(tmp_path: Path):
    target = tmp_path / "sub" / "dir" / "output.jsonl"
    with AtomicJsonlWriter(target) as fh:
        fh.write('{"ok": true}\n')
    assert target.exists()
