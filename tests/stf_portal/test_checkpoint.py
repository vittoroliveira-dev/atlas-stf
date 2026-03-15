"""Tests for STF portal checkpoint persistence."""

from __future__ import annotations

from pathlib import Path

from atlas_stf.stf_portal._checkpoint import (
    PortalCheckpoint,
    load_checkpoint,
    save_checkpoint,
)


def test_fresh_checkpoint():
    cp = PortalCheckpoint()
    assert cp.completed_processes == []
    assert cp.total_fetched == 0
    assert cp.is_completed("ADI 1234") is False


def test_mark_completed():
    cp = PortalCheckpoint()
    cp.mark_completed("ADI 1234")
    assert cp.is_completed("ADI 1234") is True
    assert cp.total_fetched == 1
    # Idempotent
    cp.mark_completed("ADI 1234")
    assert cp.completed_processes.count("ADI 1234") == 1


def test_mark_failed():
    cp = PortalCheckpoint()
    cp.mark_failed("HC 999")
    assert "HC 999" in cp.failed_processes
    cp.mark_failed("HC 999")
    assert cp.failed_processes.count("HC 999") == 1


def test_save_and_load(tmp_path: Path):
    path = tmp_path / "checkpoint.json"
    cp = PortalCheckpoint()
    cp.mark_completed("ADI 1234")
    cp.mark_completed("HC 999")
    cp.mark_failed("RE 5555")

    save_checkpoint(cp, path)
    assert path.exists()

    loaded = load_checkpoint(path)
    assert loaded.is_completed("ADI 1234") is True
    assert loaded.is_completed("HC 999") is True
    assert loaded.is_completed("RE 5555") is False
    assert "RE 5555" in loaded.failed_processes
    assert loaded.total_fetched == 2
    assert loaded.last_updated != ""


def test_load_missing_file(tmp_path: Path):
    path = tmp_path / "nonexistent.json"
    cp = load_checkpoint(path)
    assert cp.completed_processes == []
    assert cp.total_fetched == 0
