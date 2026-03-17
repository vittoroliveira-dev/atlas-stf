"""Tests for transparencia fetch runner."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from atlas_stf.transparencia._config import TransparenciaFetchConfig
from atlas_stf.transparencia._runner import fetch_transparencia_data


class TestDryRun:
    def test_returns_output_dir(self, tmp_path: Path) -> None:
        config = TransparenciaFetchConfig(
            output_dir=tmp_path / "raw",
            paineis=("acervo", "decisoes"),
            dry_run=True,
        )
        result = fetch_transparencia_data(config)
        assert result == config.output_dir

    def test_does_not_launch_playwright(self, tmp_path: Path) -> None:
        config = TransparenciaFetchConfig(
            output_dir=tmp_path / "raw",
            dry_run=True,
        )
        with patch("atlas_stf.transparencia._runner.sync_playwright") as mock_pw:
            fetch_transparencia_data(config)
        mock_pw.assert_not_called()


class TestValidation:
    def test_invalid_painel_raises(self, tmp_path: Path) -> None:
        config = TransparenciaFetchConfig(
            output_dir=tmp_path / "raw",
            paineis=("acervo", "nonexistent_panel"),
        )
        with pytest.raises(ValueError, match="nonexistent_panel"):
            fetch_transparencia_data(config)


class TestProgressCallback:
    def test_on_progress_called_in_dry_run(self, tmp_path: Path) -> None:
        """Even in dry run, the function completes without calling on_progress."""
        config = TransparenciaFetchConfig(
            output_dir=tmp_path / "raw",
            paineis=("acervo",),
            dry_run=True,
        )
        callback = MagicMock()
        fetch_transparencia_data(config, on_progress=callback)
        callback.assert_not_called()


class TestOutputDirCreated:
    def test_creates_output_dir(self, tmp_path: Path) -> None:
        out = tmp_path / "nested" / "dir"
        config = TransparenciaFetchConfig(
            output_dir=out,
            dry_run=True,
        )
        fetch_transparencia_data(config)
        assert out.exists()
