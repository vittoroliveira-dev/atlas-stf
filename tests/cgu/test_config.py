"""Tests for cgu/_config.py."""

from __future__ import annotations

from pathlib import Path

from atlas_stf.cgu._config import CGU_API_KEY_ENV, CGU_BASE_URL, CGU_DOWNLOAD_URL, CguFetchConfig


class TestCguFetchConfig:
    def test_defaults(self) -> None:
        config = CguFetchConfig()
        assert config.api_key == ""
        assert config.party_path == Path("data/curated/party.jsonl")
        assert config.output_dir == Path("data/raw/cgu")
        assert config.rate_limit_seconds == 0.7
        assert config.max_retries == 3
        assert config.timeout_seconds == 30
        assert config.dry_run is False

    def test_with_api_key(self) -> None:
        config = CguFetchConfig(api_key="test-key")
        assert config.api_key == "test-key"

    def test_frozen(self) -> None:
        config = CguFetchConfig()
        import dataclasses

        assert dataclasses.is_dataclass(config)

    def test_constants(self) -> None:
        assert "portaldatransparencia" in CGU_BASE_URL
        assert "portaldatransparencia" in CGU_DOWNLOAD_URL
        assert CGU_API_KEY_ENV == "CGU_API_KEY"
