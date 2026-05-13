"""Tests for transparencia fetch runner."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from playwright.sync_api import TimeoutError as PlaywrightTimeout

from atlas_stf.transparencia._config import TransparenciaFetchConfig
from atlas_stf.transparencia._runner import _download_csv, fetch_transparencia_data


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


class TestDownloadBoundary:
    @staticmethod
    def _make_download_page(download: MagicMock) -> MagicMock:
        page = MagicMock()
        locator = MagicMock()
        button = MagicMock()

        locator.count.return_value = 1
        locator.first = button
        button.is_visible.return_value = True
        page.locator.return_value = locator

        class _DownloadCM:
            def __enter__(self) -> MagicMock:
                return MagicMock(value=download)

            def __exit__(self, *_: object) -> bool:
                return False

        page.expect_download.return_value = _DownloadCM()
        return page

    def test_download_csv_surfaces_permission_error_with_destination(self, tmp_path: Path) -> None:
        download = MagicMock()
        download.suggested_filename = "export.csv"
        download.save_as.side_effect = PermissionError(13, "Permission denied")
        page = self._make_download_page(download)

        with pytest.raises(PermissionError, match=r".*acervo\.csv") as exc_info:
            _download_csv(page, tmp_path, "acervo", 1000)

        assert isinstance(exc_info.value.__cause__, PermissionError)

    def test_download_csv_surfaces_file_not_found_with_destination(self, tmp_path: Path) -> None:
        download = MagicMock()
        download.suggested_filename = "export.csv"
        download.save_as.side_effect = FileNotFoundError(2, "No such file or directory")
        page = self._make_download_page(download)

        with pytest.raises(FileNotFoundError, match=r".*acervo\.csv") as exc_info:
            _download_csv(page, tmp_path, "acervo", 1000)

        assert isinstance(exc_info.value.__cause__, FileNotFoundError)

    def test_download_csv_surfaces_generic_oserror_with_destination(self, tmp_path: Path) -> None:
        download = MagicMock()
        download.suggested_filename = "export.csv"
        download.save_as.side_effect = OSError(5, "Input/output error")
        page = self._make_download_page(download)

        with pytest.raises(OSError, match=r".*acervo\.csv") as exc_info:
            _download_csv(page, tmp_path, "acervo", 1000)

        assert isinstance(exc_info.value.__cause__, OSError)

    def test_download_csv_times_out_returns_none(self, tmp_path: Path) -> None:
        page = MagicMock()
        locator = MagicMock()
        button = MagicMock()

        locator.count.return_value = 1
        locator.first = button
        button.is_visible.return_value = True
        page.locator.return_value = locator

        class _TimeoutCM:
            def __enter__(self) -> None:
                raise PlaywrightTimeout("timed out")

            def __exit__(self, *_: object) -> bool:
                return False

        page.expect_download.return_value = _TimeoutCM()

        assert _download_csv(page, tmp_path, "acervo", 1000) is None
