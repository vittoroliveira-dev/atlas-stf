"""Tests for tse/_config.py."""

from __future__ import annotations

from pathlib import Path

from atlas_stf.tse._config import TSE_ELECTION_YEARS, TseFetchConfig


class TestTseFetchConfig:
    def test_defaults(self) -> None:
        config = TseFetchConfig()
        assert config.output_dir == Path("data/raw/tse")
        assert config.years == TSE_ELECTION_YEARS
        assert config.timeout_seconds == 120
        assert config.dry_run is False

    def test_custom_years(self) -> None:
        config = TseFetchConfig(years=(2022, 2024))
        assert config.years == (2022, 2024)

    def test_election_years_coverage(self) -> None:
        assert 2002 in TSE_ELECTION_YEARS
        assert 2024 in TSE_ELECTION_YEARS
        assert len(TSE_ELECTION_YEARS) == 12
