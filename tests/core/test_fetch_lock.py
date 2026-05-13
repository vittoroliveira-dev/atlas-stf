"""Tests for core/fetch_lock.py — advisory file-based lock."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from atlas_stf.core.fetch_lock import FetchLock, FetchLockError


class TestFetchLock:
    def test_acquires_and_releases(self, tmp_path: Path) -> None:
        with FetchLock(tmp_path, "tse"):
            lock_file = tmp_path / ".fetch_tse.lock"
            assert lock_file.exists()
        # After exit, lock is released (file persists but lock is gone)
        assert lock_file.exists()

    def test_concurrent_lock_raises(self, tmp_path: Path) -> None:
        with FetchLock(tmp_path, "cgu"):
            with pytest.raises(FetchLockError, match="already running"):
                with FetchLock(tmp_path, "cgu"):
                    pass  # pragma: no cover

    def test_different_sources_do_not_conflict(self, tmp_path: Path) -> None:
        with FetchLock(tmp_path, "tse_donations"):
            with FetchLock(tmp_path, "tse_expenses"):
                assert (tmp_path / ".fetch_tse_donations.lock").exists()
                assert (tmp_path / ".fetch_tse_expenses.lock").exists()

    def test_reentrant_after_release(self, tmp_path: Path) -> None:
        with FetchLock(tmp_path, "cvm"):
            pass
        # Second acquisition should succeed
        with FetchLock(tmp_path, "cvm"):
            pass

    def test_creates_parent_dirs(self, tmp_path: Path) -> None:
        deep_dir = tmp_path / "a" / "b" / "c"
        with FetchLock(deep_dir, "rfb"):
            assert (deep_dir / ".fetch_rfb.lock").exists()

    def test_lock_released_on_exception(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="boom"):
            with FetchLock(tmp_path, "test"):
                raise ValueError("boom")
        # Lock should be released — re-acquire must succeed
        with FetchLock(tmp_path, "test"):
            pass

    def test_permission_error_does_not_reclaim_live_lock(self, tmp_path: Path) -> None:
        with FetchLock(tmp_path, "cgu"):
            with patch("atlas_stf.core.fetch_lock.os.kill", side_effect=PermissionError("denied")):
                with pytest.raises(FetchLockError, match="already running"):
                    with FetchLock(tmp_path, "cgu"):
                        pass  # pragma: no cover

    def test_is_holder_dead_returns_false_on_permission_error(self, tmp_path: Path) -> None:
        lock = FetchLock(tmp_path, "cgu")
        lock._lock_path.write_text('{"pid": 1234}', encoding="utf-8")

        with patch("atlas_stf.core.fetch_lock.os.kill", side_effect=PermissionError("denied")):
            assert lock._is_holder_dead() is False

    def test_is_holder_dead_returns_true_on_process_lookup_error(self, tmp_path: Path) -> None:
        lock = FetchLock(tmp_path, "cgu")
        lock._lock_path.write_text('{"pid": 1234}', encoding="utf-8")

        with patch("atlas_stf.core.fetch_lock.os.kill", side_effect=ProcessLookupError()):
            assert lock._is_holder_dead() is True
