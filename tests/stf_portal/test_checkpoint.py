"""Tests for STF portal checkpoint persistence."""

from __future__ import annotations

import json
import threading
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
    # Idempotent — duplicate mark does NOT inflate total_fetched
    cp.mark_completed("ADI 1234")
    assert cp.completed_processes.count("ADI 1234") == 1
    assert cp.total_fetched == 1


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


def test_round_trip_preserves_content(tmp_path: Path):
    """Save → load → save produces same content (excluding volatile last_updated)."""
    path = tmp_path / "checkpoint.json"
    cp = PortalCheckpoint()
    cp.mark_completed("ADI 100")
    cp.mark_completed("HC 200")
    cp.mark_failed("RE 300")

    save_checkpoint(cp, path)
    loaded = load_checkpoint(path)

    # Content matches (ignoring last_updated)
    assert loaded.completed_processes == cp.completed_processes
    assert loaded.failed_processes == cp.failed_processes
    assert loaded.total_fetched == cp.total_fetched


def test_backward_compat_loads_old_format(tmp_path: Path):
    """Load checkpoint saved with old list-based format (no incidente_map)."""
    path = tmp_path / "checkpoint.json"
    old_data = {
        "completed_processes": ["ADI 1234", "HC 999"],
        "failed_processes": ["RE 5555"],
        "total_fetched": 2,
        "last_updated": "2026-03-17T00:00:00+00:00",
    }
    path.write_text(json.dumps(old_data), encoding="utf-8")

    loaded = load_checkpoint(path)
    assert loaded.is_completed("ADI 1234") is True
    assert loaded.is_completed("HC 999") is True
    assert loaded.total_fetched == 2
    # incidente_cache should be empty (backward compat)
    assert loaded.get_incidente("ADI 1234") is None


def test_concurrent_mark_completed():
    """Multiple threads marking different processes concurrently."""
    cp = PortalCheckpoint()
    n_per_thread = 500
    n_threads = 8

    def worker(thread_id: int) -> None:
        for i in range(n_per_thread):
            cp.mark_completed(f"T{thread_id}-P{i}")

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    expected = n_threads * n_per_thread
    assert len(cp.completed_processes) == expected
    assert cp.total_fetched == expected


def test_duplicate_mark_completed_idempotent():
    """mark_completed with same process_number does not inflate total_fetched."""
    cp = PortalCheckpoint()
    for _ in range(10):
        cp.mark_completed("ADI 1234")
    assert cp.total_fetched == 1
    assert len(cp.completed_processes) == 1


# --- Incidente cache tests (Phase 3) ---


def test_incidente_cache_set_and_get():
    cp = PortalCheckpoint()
    assert cp.get_incidente("ADI 100") is None
    cp.set_incidente("ADI 100", "123456")
    assert cp.get_incidente("ADI 100") == "123456"


def test_incidente_cache_persists_through_save_load(tmp_path: Path):
    path = tmp_path / "checkpoint.json"
    cp = PortalCheckpoint()
    cp.set_incidente("ADI 100", "111")
    cp.set_incidente("HC 200", "222")
    cp.mark_completed("ADI 100")

    save_checkpoint(cp, path)
    loaded = load_checkpoint(path)

    assert loaded.get_incidente("ADI 100") == "111"
    assert loaded.get_incidente("HC 200") == "222"


def test_incidente_cache_thread_safety():
    cp = PortalCheckpoint()
    n_per_thread = 200
    n_threads = 4

    def worker(thread_id: int) -> None:
        for i in range(n_per_thread):
            cp.set_incidente(f"T{thread_id}-P{i}", str(thread_id * 1000 + i))

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Verify all entries exist
    for tid in range(n_threads):
        for i in range(n_per_thread):
            assert cp.get_incidente(f"T{tid}-P{i}") == str(tid * 1000 + i)
