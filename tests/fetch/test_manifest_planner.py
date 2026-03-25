"""Tests for the read-only plan generator."""

from __future__ import annotations

from pathlib import Path

from atlas_stf.fetch._manifest_model import (
    FetchUnit,
    RefreshPolicy,
    RemoteState,
    SourceManifest,
)
from atlas_stf.fetch._manifest_planner import generate_plan
from atlas_stf.fetch._manifest_store import save_manifest_locked


def _stub_probe(source: str, unit: FetchUnit, policy: RefreshPolicy) -> RemoteState:
    """Probe stub that returns the unit's own remote_state (no HTTP)."""
    return unit.remote_state


class TestPlanIdempotency:
    """plan 2x without changes → 0 downloads."""

    def test_all_skipped_when_manifest_matches(self, tmp_path: Path) -> None:
        out = tmp_path / "tse"
        m = SourceManifest(source="tse_donations", units={
            f"tse_donations:{y}": FetchUnit(
                unit_id=f"tse_donations:{y}",
                source="tse_donations",
                label=f"TSE {y}",
                remote_url="url",
                remote_state=RemoteState(url="url", etag='"abc"'),
                status="committed",
            )
            for y in (2022, 2024)
        })
        save_manifest_locked(m, out)

        def probe(_s: str, unit: FetchUnit, _p: RefreshPolicy) -> RemoteState:
            return RemoteState(url="url", etag='"abc"')

        plan = generate_plan(
            sources=["tse_donations"],
            base_dir=tmp_path,
            probe_fn=probe,
            discovery_kwargs={"tse_donations": {"years": (2022, 2024)}},
        )

        actions = {i.action for i in plan.items}
        assert actions == {"skip"}


class TestNewUnit:
    """New unit not in manifest → download."""

    def test_new_unit_downloads(self, tmp_path: Path) -> None:
        plan = generate_plan(
            sources=["cvm"],
            base_dir=tmp_path,
            probe_fn=_stub_probe,
        )
        assert len(plan.items) == 1
        assert plan.items[0].action == "download"
        assert "new unit" in plan.items[0].reason


class TestSelectivity:
    """Change in 1 unit → only that one redownloads."""

    def test_only_changed_redownloads(self, tmp_path: Path) -> None:
        out = tmp_path / "tse"
        m = SourceManifest(source="tse_donations", units={
            "tse_donations:2022": FetchUnit(
                "tse_donations:2022", "tse_donations", "2022", "url",
                RemoteState(url="url", etag='"old"'), status="committed",
            ),
            "tse_donations:2024": FetchUnit(
                "tse_donations:2024", "tse_donations", "2024", "url",
                RemoteState(url="url", etag='"same"'), status="committed",
            ),
        })
        save_manifest_locked(m, out)

        def probe(_s: str, unit: FetchUnit, _p: RefreshPolicy) -> RemoteState:
            if unit.unit_id == "tse_donations:2022":
                return RemoteState(url="url", etag='"new"')
            return RemoteState(url="url", etag='"same"')

        plan = generate_plan(
            sources=["tse_donations"],
            base_dir=tmp_path,
            probe_fn=probe,
            discovery_kwargs={"tse_donations": {"years": (2022, 2024)}},
        )

        by_id = {i.unit_id: i for i in plan.items}
        assert by_id["tse_donations:2022"].action == "redownload"
        assert by_id["tse_donations:2024"].action == "skip"


class TestDrift:
    """Unit in manifest but not discovered → repair."""

    def test_missing_unit_repaired(self, tmp_path: Path) -> None:
        out = tmp_path / "tse"
        m = SourceManifest(source="tse_donations", units={
            "tse_donations:2022": FetchUnit(
                "tse_donations:2022", "tse_donations", "2022", "url",
                RemoteState(url="url", etag='"e"'), status="committed",
            ),
            "tse_donations:2000": FetchUnit(
                "tse_donations:2000", "tse_donations", "2000", "url",
                RemoteState(url="url"), status="committed",
            ),
        })
        save_manifest_locked(m, out)

        plan = generate_plan(
            sources=["tse_donations"],
            base_dir=tmp_path,
            probe_fn=_stub_probe,
            discovery_kwargs={"tse_donations": {"years": (2022,)}},
        )

        by_id = {i.unit_id: i for i in plan.items}
        assert "tse_donations:2000" in by_id
        assert by_id["tse_donations:2000"].action == "repair"
        assert "drift" in by_id["tse_donations:2000"].reason


class TestFailureKind:
    """transform failure → repair; download failure → redownload."""

    def test_transform_failure_repair(self, tmp_path: Path) -> None:
        out = tmp_path / "cvm"
        m = SourceManifest(source="cvm", units={
            "cvm:sanctions": FetchUnit(
                "cvm:sanctions", "cvm", "CVM", "url",
                RemoteState(url="url"), status="failed", failure_kind="transform", last_error="parse error",
            ),
        })
        save_manifest_locked(m, out)

        plan = generate_plan(sources=["cvm"], base_dir=tmp_path, probe_fn=_stub_probe)
        assert plan.items[0].action == "repair"

    def test_download_failure_redownload(self, tmp_path: Path) -> None:
        out = tmp_path / "cvm"
        m = SourceManifest(source="cvm", units={
            "cvm:sanctions": FetchUnit(
                "cvm:sanctions", "cvm", "CVM", "url",
                RemoteState(url="url"), status="failed", failure_kind="download", last_error="timeout",
            ),
        })
        save_manifest_locked(m, out)

        plan = generate_plan(sources=["cvm"], base_dir=tmp_path, probe_fn=_stub_probe)
        assert plan.items[0].action == "redownload"


class TestConfidenceRules:
    """confidence=none → never skip; weak + allow_weak_skip=False → redownload."""

    def test_confidence_none_never_skips(self, tmp_path: Path) -> None:
        out = tmp_path / "cgu"
        m = SourceManifest(source="cgu", units={
            "cgu:ceis": FetchUnit(
                "cgu:ceis", "cgu", "CEIS", "url",
                RemoteState(url="url"), status="committed",
            ),
        })
        save_manifest_locked(m, out)

        def probe(_s: str, unit: FetchUnit, _p: RefreshPolicy) -> RemoteState:
            return RemoteState(url="url")

        plan = generate_plan(sources=["cgu"], base_dir=tmp_path, probe_fn=probe)
        ceis = [i for i in plan.items if i.unit_id == "cgu:ceis"][0]
        assert ceis.action == "redownload"
        assert "confidence=none" in ceis.reason

    def test_weak_match_no_skip_when_disallowed(self, tmp_path: Path) -> None:
        out = tmp_path / "rfb"
        m = SourceManifest(source="rfb", units={
            "rfb:socios_pass1_0": FetchUnit(
                "rfb:socios_pass1_0", "rfb", "RFB", "url",
                RemoteState(url="url", content_length=500), status="committed",
            ),
        })
        save_manifest_locked(m, out)

        def probe(_s: str, unit: FetchUnit, _p: RefreshPolicy) -> RemoteState:
            return RemoteState(url="url", content_length=500)

        plan = generate_plan(
            sources=["rfb"],
            base_dir=tmp_path,
            probe_fn=probe,
            discovery_kwargs={"rfb": {"base_url": "http://test"}},
        )
        socios = [i for i in plan.items if i.unit_id == "rfb:socios_pass1_0"][0]
        assert socios.action == "redownload"
        assert "allow_weak_skip=False" in socios.reason


class TestPlanSnapshot:
    """PlanItem contains expected_remote_state and PolicySnapshot."""

    def test_snapshot_present(self, tmp_path: Path) -> None:
        plan = generate_plan(sources=["cvm"], base_dir=tmp_path, probe_fn=_stub_probe)
        item = plan.items[0]
        assert item.expected_remote_state is not None
        assert item.policy_snapshot.source == "cvm"


class TestPlanReadOnly:
    """generate_plan() must not create any files."""

    def test_no_files_created(self, tmp_path: Path) -> None:
        before = set(tmp_path.rglob("*"))
        generate_plan(sources=["cvm"], base_dir=tmp_path, probe_fn=_stub_probe)
        after = set(tmp_path.rglob("*"))
        assert before == after


class TestPlanReproducible:
    """Same input → same plan_id."""

    def test_same_input_same_id(self, tmp_path: Path) -> None:
        plan1 = generate_plan(
            sources=["cvm"], base_dir=tmp_path, probe_fn=_stub_probe,
        )
        plan2 = generate_plan(
            sources=["cvm"], base_dir=tmp_path, probe_fn=_stub_probe,
        )
        assert plan1.plan_id == plan2.plan_id


class TestDeferredRun:
    """DataJud supports_deferred_run=False is in snapshot."""

    def test_datajud_snapshot(self, tmp_path: Path) -> None:
        # DataJud needs process_path; without it, yields 0 units
        plan = generate_plan(sources=["datajud"], base_dir=tmp_path, probe_fn=_stub_probe)
        assert plan.sources == ["datajud"]
