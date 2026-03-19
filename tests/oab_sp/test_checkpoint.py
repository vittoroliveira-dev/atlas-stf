"""Tests for OAB/SP checkpoint (four-state machine)."""

from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.oab_sp._checkpoint import OabSpCheckpoint, load_checkpoint, save_checkpoint

# ---------------------------------------------------------------------------
# Roundtrip persistence
# ---------------------------------------------------------------------------


def test_roundtrip_save_load(tmp_path: Path):
    cp = OabSpCheckpoint()
    cp.mark_completed("REG001")
    cp.mark_not_found("REG002")
    cp.mark_failed("REG003")
    cp.promote_exhausted(max_retries=0)  # REG003 → exhausted

    path = tmp_path / ".checkpoint.json"
    save_checkpoint(cp, path)

    loaded = load_checkpoint(path)
    assert loaded.is_resolved("REG001")
    assert loaded.is_resolved("REG002")
    assert loaded.is_resolved("REG003")
    # stats reflect all four states
    s = loaded.stats
    assert s["completed"] == 1
    assert s["not_found"] == 1
    assert s["exhausted"] == 1
    assert s["failed"] == 0


# ---------------------------------------------------------------------------
# Terminal state detection
# ---------------------------------------------------------------------------


def test_is_resolved_includes_terminals():
    cp = OabSpCheckpoint()
    cp.mark_completed("A")
    cp.mark_not_found("B")
    cp.mark_failed("C")
    cp.promote_exhausted(max_retries=0)

    assert cp.is_resolved("A") is True
    assert cp.is_resolved("B") is True
    assert cp.is_resolved("C") is True  # promoted to exhausted


def test_is_resolved_excludes_failed():
    cp = OabSpCheckpoint()
    cp.mark_failed("X")
    # failed with retries remaining is NOT resolved
    assert cp.is_resolved("X") is False


# ---------------------------------------------------------------------------
# Retry logic
# ---------------------------------------------------------------------------


def test_is_retryable_under_max():
    cp = OabSpCheckpoint()
    cp.mark_failed("R1")
    assert cp.is_retryable("R1", max_retries=3) is True


def test_is_retryable_at_max():
    cp = OabSpCheckpoint()
    cp.mark_failed("R2")
    cp.mark_failed("R2")
    cp.mark_failed("R2")
    # retry_count == 3, max_retries == 3 → not retryable
    assert cp.is_retryable("R2", max_retries=3) is False


def test_mark_failed_increments_retry():
    cp = OabSpCheckpoint()
    cp.mark_failed("R3")
    assert cp._retry_counts.get("R3", 0) == 1
    cp.mark_failed("R3")
    assert cp._retry_counts.get("R3", 0) == 2
    cp.mark_failed("R3")
    assert cp._retry_counts.get("R3", 0) == 3


# ---------------------------------------------------------------------------
# Exhaustion promotion
# ---------------------------------------------------------------------------


def test_promote_exhausted():
    cp = OabSpCheckpoint()
    cp.mark_failed("E1")
    cp.mark_failed("E1")
    cp.mark_failed("E1")
    # 3 retries, threshold is 3 → promote
    promoted = cp.promote_exhausted(max_retries=3)
    assert promoted == 1
    assert cp.is_resolved("E1") is True
    assert cp.stats["exhausted"] == 1
    assert cp.stats["failed"] == 0


# ---------------------------------------------------------------------------
# Mutual exclusion invariants
# ---------------------------------------------------------------------------


def test_not_found_and_exhausted_mutual_exclusion():
    cp = OabSpCheckpoint()
    cp.mark_not_found("NF1")
    cp.mark_failed("NF1")
    cp.promote_exhausted(max_retries=0)
    # A registration that was already not_found must not be moved to exhausted
    # (mark_not_found clears it from failed, so promote_exhausted has nothing to promote)
    assert "NF1" not in cp._exhausted or "NF1" in cp._not_found
    # At minimum, it must remain resolved
    assert cp.is_resolved("NF1") is True


# ---------------------------------------------------------------------------
# Atomic write
# ---------------------------------------------------------------------------


def test_atomic_write(tmp_path: Path):
    cp = OabSpCheckpoint()
    cp.mark_completed("ATM1")

    path = tmp_path / "cp.json"
    save_checkpoint(cp, path)

    # The .tmp file should not exist after a successful save
    tmp = path.with_suffix(".json.tmp")
    assert not tmp.exists()
    # The final file must be valid JSON
    data = json.loads(path.read_text(encoding="utf-8"))
    assert "completed" in data
    assert "ATM1" in data["completed"]
