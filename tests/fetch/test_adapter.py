"""Tests for fetch source adapters — Protocol compliance, registry, planner integration."""

from __future__ import annotations

from pathlib import Path

import pytest

from atlas_stf.fetch._adapter import (
    CguAdapter,
    CvmAdapter,
    DatajudAdapter,
    FetchSourceAdapter,
    RfbAdapter,
    TseDonationsAdapter,
    TseExpensesAdapter,
    TsePartyOrgAdapter,
    get_adapter,
    list_sources,
)
from atlas_stf.fetch._manifest_model import REFRESH_POLICIES, FetchUnit, RemoteState

# ---------------------------------------------------------------------------
# Protocol compliance
# ---------------------------------------------------------------------------


class TestProtocolCompliance:
    """Every concrete adapter must satisfy FetchSourceAdapter at runtime."""

    def test_tse_donations(self, tmp_path: Path) -> None:
        a = TseDonationsAdapter(tmp_path)
        assert isinstance(a, FetchSourceAdapter)

    def test_tse_expenses(self, tmp_path: Path) -> None:
        a = TseExpensesAdapter(tmp_path)
        assert isinstance(a, FetchSourceAdapter)

    def test_tse_party_org(self, tmp_path: Path) -> None:
        a = TsePartyOrgAdapter(tmp_path)
        assert isinstance(a, FetchSourceAdapter)

    def test_cgu(self, tmp_path: Path) -> None:
        a = CguAdapter(tmp_path)
        assert isinstance(a, FetchSourceAdapter)

    def test_cvm(self, tmp_path: Path) -> None:
        a = CvmAdapter(tmp_path)
        assert isinstance(a, FetchSourceAdapter)

    def test_rfb(self, tmp_path: Path) -> None:
        a = RfbAdapter(tmp_path)
        assert isinstance(a, FetchSourceAdapter)

    def test_datajud(self, tmp_path: Path) -> None:
        a = DatajudAdapter(tmp_path)
        assert isinstance(a, FetchSourceAdapter)


# ---------------------------------------------------------------------------
# source_name and policy
# ---------------------------------------------------------------------------


class TestAdapterProperties:
    def test_source_names_match_policies(self, tmp_path: Path) -> None:
        for source in list_sources():
            adapter = get_adapter(source, tmp_path)
            assert adapter.source_name == source
            assert adapter.policy == REFRESH_POLICIES[source]


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class TestRegistry:
    def test_list_sources_complete(self) -> None:
        sources = list_sources()
        expected = {"tse_donations", "tse_expenses", "tse_party_org", "cgu", "cvm", "rfb", "datajud"}
        assert set(sources) == expected

    def test_get_adapter_known(self, tmp_path: Path) -> None:
        adapter = get_adapter("cvm", tmp_path)
        assert adapter.source_name == "cvm"

    def test_get_adapter_unknown(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="No adapter"):
            get_adapter("nonexistent", tmp_path)

    def test_get_adapter_resolves_subdir(self, tmp_path: Path) -> None:
        """TSE adapters share 'tse' subdir."""
        a = get_adapter("tse_donations", tmp_path)
        # The adapter should have been given tmp_path/tse as output_dir
        units = list(a.discover_units())
        assert all(str(tmp_path / "tse") in u.local_path for u in units)


# ---------------------------------------------------------------------------
# Discovery via adapter
# ---------------------------------------------------------------------------


class TestAdapterDiscovery:
    def test_cvm_yields_one_unit(self, tmp_path: Path) -> None:
        adapter = CvmAdapter(tmp_path)
        units = list(adapter.discover_units())
        assert len(units) == 1
        assert units[0].unit_id == "cvm:sanctions"

    def test_tse_custom_years(self, tmp_path: Path) -> None:
        adapter = TseDonationsAdapter(tmp_path, years=(2022,))
        units = list(adapter.discover_units())
        assert len(units) == 1
        assert units[0].unit_id == "tse_donations:2022"

    def test_cgu_with_date(self, tmp_path: Path) -> None:
        adapter = CguAdapter(tmp_path, date_str="20260323")
        units = list(adapter.discover_units())
        assert len(units) == 3
        assert any("20260323" in u.unit_id for u in units)

    def test_datajud_without_process_path(self, tmp_path: Path) -> None:
        """No process_path → 0 units (graceful)."""
        adapter = DatajudAdapter(tmp_path)
        units = list(adapter.discover_units())
        assert units == []


# ---------------------------------------------------------------------------
# Probe via adapter
# ---------------------------------------------------------------------------


class TestAdapterProbe:
    def test_datajud_returns_empty_state(self, tmp_path: Path) -> None:
        adapter = DatajudAdapter(tmp_path)
        unit = FetchUnit("datajud:x", "datajud", "X", "", RemoteState(url=""))
        state = adapter.probe_remote(unit)
        assert state.url == ""
        assert state.probed_at != ""


# ---------------------------------------------------------------------------
# Planner integration
# ---------------------------------------------------------------------------


class TestPlannerWithAdapters:
    def test_generate_plan_from_adapters(self, tmp_path: Path) -> None:
        from atlas_stf.fetch._manifest_planner import generate_plan_from_adapters

        adapter = CvmAdapter(tmp_path / "cvm")
        plan = generate_plan_from_adapters([adapter], base_dir=tmp_path)

        assert plan.sources == ["cvm"]
        assert len(plan.items) == 1
        assert plan.items[0].unit_id == "cvm:sanctions"
        assert plan.items[0].action == "download"

    def test_multi_adapter_plan(self, tmp_path: Path) -> None:
        from atlas_stf.fetch._manifest_planner import generate_plan_from_adapters

        adapters = [
            CvmAdapter(tmp_path / "cvm"),
            TseDonationsAdapter(tmp_path / "tse", years=(2024,)),
        ]
        plan = generate_plan_from_adapters(adapters, base_dir=tmp_path)

        sources = set(plan.sources)
        assert sources == {"cvm", "tse_donations"}
        assert len(plan.items) == 2

    def test_adapter_plan_deterministic(self, tmp_path: Path) -> None:
        """Same adapter twice → same plan_id."""
        from atlas_stf.fetch._manifest_planner import generate_plan_from_adapters

        adapter = CvmAdapter(tmp_path / "cvm")
        plan1 = generate_plan_from_adapters([adapter], base_dir=tmp_path)
        plan2 = generate_plan_from_adapters([adapter], base_dir=tmp_path)
        assert plan1.plan_id == plan2.plan_id
