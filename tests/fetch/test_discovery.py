"""Tests for unit discovery per source."""

from __future__ import annotations

from pathlib import Path

import pytest

from atlas_stf.fetch._discovery import discover_units


class TestDiscoverTseDonations:
    def test_yields_all_years(self, tmp_path: Path) -> None:
        units = list(discover_units("tse_donations", output_dir=tmp_path))
        assert len(units) == 12
        ids = {u.unit_id for u in units}
        assert "tse_donations:2022" in ids
        assert "tse_donations:2024" in ids

    def test_custom_years(self, tmp_path: Path) -> None:
        units = list(discover_units("tse_donations", output_dir=tmp_path, years=(2022,)))
        assert len(units) == 1
        assert units[0].unit_id == "tse_donations:2022"

    def test_has_candidate_urls_metadata(self, tmp_path: Path) -> None:
        units = list(discover_units("tse_donations", output_dir=tmp_path, years=(2022,)))
        assert "candidate_urls" in units[0].metadata
        assert len(units[0].metadata["candidate_urls"]) == 3


class TestDiscoverTseExpenses:
    def test_default_years(self, tmp_path: Path) -> None:
        units = list(discover_units("tse_expenses", output_dir=tmp_path))
        assert len(units) == 7
        ids = {u.unit_id for u in units}
        assert "tse_expenses:2022" in ids
        assert "tse_expenses:2018" not in ids  # excluded


class TestDiscoverTsePartyOrg:
    def test_default_years(self, tmp_path: Path) -> None:
        units = list(discover_units("tse_party_org", output_dir=tmp_path))
        assert len(units) == 4


class TestDiscoverCgu:
    def test_three_datasets(self, tmp_path: Path) -> None:
        units = list(discover_units("cgu", output_dir=tmp_path))
        assert len(units) == 3
        sources = {u.unit_id for u in units}
        assert "cgu:ceis" in sources
        assert "cgu:cnep" in sources
        assert "cgu:acordos_leniencia" in sources

    def test_with_date(self, tmp_path: Path) -> None:
        units = list(discover_units("cgu", output_dir=tmp_path, date_str="20260323"))
        assert len(units) == 3
        assert "20260323" in units[0].unit_id


class TestDiscoverCvm:
    def test_single_unit(self, tmp_path: Path) -> None:
        units = list(discover_units("cvm", output_dir=tmp_path))
        assert len(units) == 1
        assert units[0].unit_id == "cvm:sanctions"


class TestDiscoverRfb:
    def test_pass_count(self, tmp_path: Path) -> None:
        units = list(discover_units("rfb", output_dir=tmp_path))
        assert len(units) == 40  # 4 passes × 10 files each


class TestDiscoverUnknownSource:
    def test_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="Unknown source"):
            list(discover_units("nonexistent", output_dir=tmp_path))
