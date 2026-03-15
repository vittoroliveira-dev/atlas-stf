"""Tests for STF portal configuration."""

from __future__ import annotations

from pathlib import Path

from atlas_stf.stf_portal._config import StfPortalConfig


def test_default_config(tmp_path: Path):
    config = StfPortalConfig(output_dir=tmp_path / "portal")
    assert config.rate_limit_seconds == 2.0
    assert config.max_concurrent == 1
    assert config.refetch_after_days == 30
    assert (tmp_path / "portal").exists()


def test_custom_config(tmp_path: Path):
    config = StfPortalConfig(
        output_dir=tmp_path / "out",
        rate_limit_seconds=5.0,
        max_processes=100,
    )
    assert config.rate_limit_seconds == 5.0
    assert config.max_processes == 100
