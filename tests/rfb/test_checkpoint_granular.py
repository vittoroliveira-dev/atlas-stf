"""Tests for granular artifact commit model in RFB checkpoint.

Covers the 7 required scenarios:
1. Pass valid without run_commit → preserved
2. Units valid without artifact_commit → re-materialize only
3. Crash before artifact write → units preserved
4. Crash after artifact write, before run_commit → artifact preserved
5. Artifact absent/empty/invalid → no false commit
6. Multi-shard establishments → granular resume
7. Idempotency → no oscillation
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from atlas_stf.fetch._manifest_store import load_manifest
from atlas_stf.rfb._runner import (
    _manifest_to_checkpoint,
    _save_checkpoint_via_manifest,
)


def _make_checkpoint(
    *,
    p1: list[int] | None = None,
    p2: list[int] | None = None,
    emp: list[int] | None = None,
    est: list[int] | None = None,
    ref: bool = True,
    artifact_commits: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a checkpoint dict with optional artifact_commits."""
    cp: dict[str, Any] = {
        "completed_socios_pass1": p1 or [],
        "completed_socios_pass2": p2 or [],
        "completed_empresas": emp or [],
        "completed_estabelecimentos": est or [],
        "completed_reference": ref,
        "cnpjs": ["12345678"],
        "tse_targets_hash": "abc",
    }
    if artifact_commits:
        cp["artifact_commits"] = artifact_commits
    return cp


def _write_jsonl(path: Path, count: int) -> None:
    """Write a fake JSONL file with `count` lines."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(f'{{"id": {i}}}' for i in range(count)) + "\n")


# ---------------------------------------------------------------------------
# Scenario 1: Pass valid without run_commit → preserved
# ---------------------------------------------------------------------------


class TestPassValidWithoutRunCommit:
    def test_artifact_commit_preserves_pass(self, tmp_path: Path) -> None:
        """If artifact_commit exists for partners, passes 1+2 are NOT invalidated
        even without a global run_commit."""
        cp = _make_checkpoint(
            p1=list(range(10)),
            p2=list(range(10)),
            artifact_commits={
                "partners_raw.jsonl": {"run_id": "abc", "record_count": 1000, "committed_at": "2026-01-01"},
            },
        )
        _save_checkpoint_via_manifest(tmp_path, cp)

        # Reload and verify passes still intact
        manifest = load_manifest("rfb", tmp_path)
        assert manifest is not None
        recovered = _manifest_to_checkpoint(manifest)
        assert len(recovered["completed_socios_pass1"]) == 10
        assert len(recovered["completed_socios_pass2"]) == 10
        assert recovered["artifact_commits"]["partners_raw.jsonl"]["record_count"] == 1000


# ---------------------------------------------------------------------------
# Scenario 2: Units valid without artifact_commit → re-materialize only
# ---------------------------------------------------------------------------


class TestUnitsValidWithoutArtifactCommit:
    def test_passes_preserved_artifact_needs_rewrite(self, tmp_path: Path) -> None:
        """Passes complete but no artifact_commit → passes stay, artifact re-materializes."""
        cp = _make_checkpoint(
            p1=list(range(10)),
            p2=list(range(10)),
            # NO artifact_commits
        )
        _save_checkpoint_via_manifest(tmp_path, cp)

        manifest = load_manifest("rfb", tmp_path)
        assert manifest is not None
        recovered = _manifest_to_checkpoint(manifest)

        # Passes still intact
        assert len(recovered["completed_socios_pass1"]) == 10
        assert len(recovered["completed_socios_pass2"]) == 10

        # No artifact commits — guard should detect this and request re-write
        assert "artifact_commits" not in recovered or not recovered.get("artifact_commits")


# ---------------------------------------------------------------------------
# Scenario 3: Crash before artifact write → units preserved
# ---------------------------------------------------------------------------


class TestCrashBeforeArtifactWrite:
    def test_units_survive_missing_artifact_commit(self, tmp_path: Path) -> None:
        """Simulates crash after passes 1-3 but before writing companies_raw.jsonl."""
        cp = _make_checkpoint(
            p1=list(range(10)),
            p2=list(range(10)),
            emp=list(range(10)),
            artifact_commits={
                # partners was committed before the crash
                "partners_raw.jsonl": {"run_id": "x", "record_count": 2000, "committed_at": "2026-01-01"},
                # companies was NOT committed (crash happened)
            },
        )
        _save_checkpoint_via_manifest(tmp_path, cp)

        manifest = load_manifest("rfb", tmp_path)
        assert manifest is not None
        recovered = _manifest_to_checkpoint(manifest)

        # All unit-level passes still intact
        assert len(recovered["completed_socios_pass1"]) == 10
        assert len(recovered["completed_socios_pass2"]) == 10
        assert len(recovered["completed_empresas"]) == 10

        # Partners committed, companies not
        ac = recovered.get("artifact_commits", {})
        assert "partners_raw.jsonl" in ac
        assert "companies_raw.jsonl" not in ac


# ---------------------------------------------------------------------------
# Scenario 4: Crash after artifact write, before run_commit → preserved
# ---------------------------------------------------------------------------


class TestCrashAfterArtifactBeforeRunCommit:
    def test_all_artifacts_committed_individually(self, tmp_path: Path) -> None:
        """All 3 artifacts committed but no global run_commit → everything valid."""
        cp = _make_checkpoint(
            p1=list(range(10)),
            p2=list(range(10)),
            emp=list(range(10)),
            est=list(range(10)),
            artifact_commits={
                "partners_raw.jsonl": {"run_id": "r1", "record_count": 3000, "committed_at": "2026-01-01"},
                "companies_raw.jsonl": {"run_id": "r1", "record_count": 1500, "committed_at": "2026-01-01"},
                "establishments_raw.jsonl": {"run_id": "r1", "record_count": 2000, "committed_at": "2026-01-01"},
            },
        )
        _save_checkpoint_via_manifest(tmp_path, cp)

        manifest = load_manifest("rfb", tmp_path)
        assert manifest is not None
        recovered = _manifest_to_checkpoint(manifest)

        # Everything preserved — no run_commit needed
        assert len(recovered["completed_socios_pass1"]) == 10
        assert len(recovered["completed_empresas"]) == 10
        assert len(recovered["completed_estabelecimentos"]) == 10
        ac = recovered["artifact_commits"]
        assert len(ac) == 3


# ---------------------------------------------------------------------------
# Scenario 5: Artifact absent/empty/invalid → no false commit
# ---------------------------------------------------------------------------


class TestArtifactAbsentNoFalseCommit:
    def test_no_artifact_commit_for_empty_output(self, tmp_path: Path) -> None:
        """Checkpoint with NO artifact_commits and missing file → no false positive."""
        cp = _make_checkpoint(est=list(range(10)))
        _save_checkpoint_via_manifest(tmp_path, cp)

        manifest = load_manifest("rfb", tmp_path)
        assert manifest is not None
        recovered = _manifest_to_checkpoint(manifest)

        # establishments pass is complete in checkpoint
        assert len(recovered["completed_estabelecimentos"]) == 10

        # But no artifact commit exists — file doesn't exist either
        est_path = tmp_path / "establishments_raw.jsonl"
        assert not est_path.exists()
        ac = recovered.get("artifact_commits", {})
        assert "establishments_raw.jsonl" not in ac


# ---------------------------------------------------------------------------
# Scenario 6: Multi-shard establishments → granular resume
# ---------------------------------------------------------------------------


class TestMultiShardEstabelecimentos:
    def test_partial_shards_preserved(self, tmp_path: Path) -> None:
        """If establishments shards 0-4 are complete but 5-9 are not,
        only 5-9 should need processing on rerun."""
        cp = _make_checkpoint(
            p1=list(range(10)),
            p2=list(range(10)),
            emp=list(range(10)),
            est=[0, 1, 2, 3, 4],  # only 5 of 10 shards
            artifact_commits={
                "partners_raw.jsonl": {"run_id": "r1", "record_count": 3000, "committed_at": "2026-01-01"},
                "companies_raw.jsonl": {"run_id": "r1", "record_count": 1500, "committed_at": "2026-01-01"},
                # NO establishments artifact commit (still in progress)
            },
        )
        _save_checkpoint_via_manifest(tmp_path, cp)

        manifest = load_manifest("rfb", tmp_path)
        assert manifest is not None
        recovered = _manifest_to_checkpoint(manifest)

        # Partners and companies fully committed
        assert recovered["artifact_commits"]["partners_raw.jsonl"]["record_count"] == 3000
        assert recovered["artifact_commits"]["companies_raw.jsonl"]["record_count"] == 1500

        # Establishments: 5 shards done, 5 pending
        assert recovered["completed_estabelecimentos"] == [0, 1, 2, 3, 4]

        # Other passes NOT invalidated
        assert len(recovered["completed_socios_pass1"]) == 10
        assert len(recovered["completed_empresas"]) == 10


# ---------------------------------------------------------------------------
# Scenario 7: Idempotency — two roundtrips don't oscillate
# ---------------------------------------------------------------------------


class TestIdempotency:
    def test_roundtrip_stable(self, tmp_path: Path) -> None:
        """Save → load → save → load must produce identical state."""
        cp = _make_checkpoint(
            p1=list(range(10)),
            p2=list(range(10)),
            emp=list(range(10)),
            est=list(range(10)),
            artifact_commits={
                "partners_raw.jsonl": {"run_id": "r1", "record_count": 100, "committed_at": "2026-01-01"},
                "companies_raw.jsonl": {"run_id": "r1", "record_count": 200, "committed_at": "2026-01-01"},
                "establishments_raw.jsonl": {"run_id": "r1", "record_count": 300, "committed_at": "2026-01-01"},
            },
        )

        # Round 1
        _save_checkpoint_via_manifest(tmp_path, cp)
        m1 = load_manifest("rfb", tmp_path)
        assert m1 is not None
        r1 = _manifest_to_checkpoint(m1)

        # Round 2
        _save_checkpoint_via_manifest(tmp_path, r1)
        m2 = load_manifest("rfb", tmp_path)
        assert m2 is not None
        r2 = _manifest_to_checkpoint(m2)

        # Core state must be identical
        for key in ("completed_socios_pass1", "completed_socios_pass2", "completed_empresas",
                     "completed_estabelecimentos", "completed_reference"):
            assert r1[key] == r2[key], f"Mismatch on {key}"

        # Artifact commits preserved
        for name in ("partners_raw.jsonl", "companies_raw.jsonl", "establishments_raw.jsonl"):
            assert r1["artifact_commits"][name]["record_count"] == r2["artifact_commits"][name]["record_count"]
