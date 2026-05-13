"""Tests for analytics run I/O helpers."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from atlas_stf.analytics._run_context import RunContext
from atlas_stf.analytics._run_io import _atomic_write_json


def test_atomic_write_json_propagates_replace_failure_and_cleans_tmp(tmp_path: Path) -> None:
    target = tmp_path / "state.json"

    with patch("atlas_stf.analytics._run_io.os.replace", side_effect=OSError("replace failed")):
        with pytest.raises(OSError, match="replace failed"):
            _atomic_write_json(target, {"ok": True})

    assert not target.exists()
    assert list(tmp_path.iterdir()) == []


def test_save_checkpoint_aborts_when_atomic_write_fails(tmp_path: Path) -> None:
    ctx = RunContext("test-builder", tmp_path, total_steps=1)
    checkpoint_path = tmp_path / ".runs" / ctx.run_id / "latest_checkpoint.json"
    history_path = tmp_path / ".runs" / ctx.run_id / "checkpoints.jsonl"

    with patch("atlas_stf.analytics._run_io.os.replace", side_effect=OSError("replace failed")):
        with pytest.raises(OSError, match="replace failed"):
            ctx.save_checkpoint("phase-1", records=10)

    assert not checkpoint_path.exists()
    assert not history_path.exists()
    assert not any(path.suffix == ".tmp" for path in checkpoint_path.parent.iterdir())
    ctx.finish()
