"""Tests for DEOAB checkpoint with auditable states."""

from __future__ import annotations

from pathlib import Path

from atlas_stf.deoab._checkpoint import (
    DateEntry,
    DeoabCheckpoint,
    load_checkpoint,
    save_checkpoint,
)


def test_fresh_checkpoint():
    cp = DeoabCheckpoint()
    stats = cp.stats
    assert stats["missing"] == 0
    assert stats["parsed"] == 0
    assert stats["failed"] == 0
    assert stats["downloaded"] == 0


def test_needs_download_new():
    """New date should need download."""
    cp = DeoabCheckpoint()
    assert cp.needs_download("2026-01-01", 4000000) is True


def test_needs_download_content_changed():
    """Content length change should trigger re-download."""
    cp = DeoabCheckpoint()
    cp.set("2026-01-01", DateEntry(status="parsed", content_length=4000000, parser_version=1))
    assert cp.needs_download("2026-01-01", 5000000) is True


def test_needs_download_same_content_no_redownload():
    """Same content length should NOT trigger re-download (stable path)."""
    cp = DeoabCheckpoint()
    cp.set("2026-01-01", DateEntry(status="parsed", content_length=4000000, parser_version=1))
    assert cp.needs_download("2026-01-01", 4000000) is False


def test_needs_download_failed_retries():
    """Failed status should trigger re-download."""
    cp = DeoabCheckpoint()
    cp.set("2026-01-01", DateEntry(status="failed", content_length=4000000, error="download"))
    assert cp.needs_download("2026-01-01", 4000000) is True


def test_needs_parse_version_changed():
    """Parser version bump should trigger re-parse."""
    cp = DeoabCheckpoint()
    cp.set("2026-01-01", DateEntry(status="parsed", content_length=4000000, parser_version=1))
    assert cp.needs_parse("2026-01-01", 2) is True


def test_needs_parse_same_version_no_reparse():
    """Same parser version should NOT trigger re-parse (stable path)."""
    cp = DeoabCheckpoint()
    cp.set("2026-01-01", DateEntry(status="parsed", content_length=4000000, parser_version=1))
    assert cp.needs_parse("2026-01-01", 1) is False


def test_needs_parse_downloaded_needs_parse():
    """Downloaded but not parsed should need parse."""
    cp = DeoabCheckpoint()
    cp.set("2026-01-01", DateEntry(status="downloaded", content_length=4000000))
    assert cp.needs_parse("2026-01-01", 1) is True


def test_needs_parse_failed_needs_parse():
    """Failed status should need parse."""
    cp = DeoabCheckpoint()
    cp.set("2026-01-01", DateEntry(status="failed", content_length=4000000, error="pdftotext"))
    assert cp.needs_parse("2026-01-01", 1) is True


def test_save_and_load_roundtrip(tmp_path: Path):
    cp = DeoabCheckpoint()
    cp.set(
        "2026-01-01", DateEntry(status="parsed", content_length=4000000, parser_version=1, source_url="https://x.com")
    )
    cp.set("2026-01-02", DateEntry(status="missing"))

    path = tmp_path / ".checkpoint.json"
    save_checkpoint(cp, path)

    loaded = load_checkpoint(path)
    e1 = loaded.get("2026-01-01")
    assert e1 is not None
    assert e1.status == "parsed"
    assert e1.content_length == 4000000
    assert e1.parser_version == 1

    e2 = loaded.get("2026-01-02")
    assert e2 is not None
    assert e2.status == "missing"

    assert loaded.stats["parsed"] == 1
    assert loaded.stats["missing"] == 1
