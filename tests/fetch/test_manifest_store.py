"""Tests for manifest store — load/save with locking."""

from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.fetch._manifest_model import FetchUnit, RemoteState, SourceManifest
from atlas_stf.fetch._manifest_store import load_all_manifests, load_manifest, save_manifest_locked


class TestLoadManifest:
    def test_returns_none_when_absent(self, tmp_path: Path) -> None:
        assert load_manifest("tse_donations", tmp_path) is None

    def test_returns_none_on_corrupt_json(self, tmp_path: Path) -> None:
        (tmp_path / "_manifest_tse_donations.json").write_text("{bad", encoding="utf-8")
        assert load_manifest("tse_donations", tmp_path) is None

    def test_returns_none_on_source_mismatch(self, tmp_path: Path) -> None:
        data = {"source": "cvm", "schema_version": "2.0", "units": {}}
        (tmp_path / "_manifest_tse_donations.json").write_text(json.dumps(data), encoding="utf-8")
        assert load_manifest("tse_donations", tmp_path) is None

    def test_loads_valid_manifest(self, tmp_path: Path) -> None:
        m = SourceManifest(source="cvm", units={
            "cvm:sanctions": FetchUnit("cvm:sanctions", "cvm", "CVM", "", RemoteState(url=""), status="committed"),
        })
        save_manifest_locked(m, tmp_path)
        loaded = load_manifest("cvm", tmp_path)
        assert loaded is not None
        assert "cvm:sanctions" in loaded.units


class TestSaveManifestLocked:
    def test_atomic_write(self, tmp_path: Path) -> None:
        m = SourceManifest(source="test", units={
            "test:a": FetchUnit("test:a", "test", "A", "", RemoteState(url="")),
        })
        path = save_manifest_locked(m, tmp_path)
        assert path.exists()
        data = json.loads(path.read_text(encoding="utf-8"))
        assert data["source"] == "test"

    def test_creates_dir_if_needed(self, tmp_path: Path) -> None:
        deep = tmp_path / "a" / "b"
        m = SourceManifest(source="deep")
        save_manifest_locked(m, deep)
        assert (deep / "_manifest_deep.json").exists()

    def test_overwrites_existing(self, tmp_path: Path) -> None:
        m1 = SourceManifest(source="s", units={
            "s:a": FetchUnit("s:a", "s", "A", "", RemoteState(url="")),
        })
        save_manifest_locked(m1, tmp_path)

        m2 = SourceManifest(source="s", units={
            "s:a": FetchUnit("s:a", "s", "A", "", RemoteState(url="")),
            "s:b": FetchUnit("s:b", "s", "B", "", RemoteState(url="")),
        })
        save_manifest_locked(m2, tmp_path)

        loaded = load_manifest("s", tmp_path)
        assert loaded is not None
        assert len(loaded.units) == 2

    def test_no_temp_files_left(self, tmp_path: Path) -> None:
        m = SourceManifest(source="clean")
        save_manifest_locked(m, tmp_path)
        temps = list(tmp_path.glob(".manifest_*"))
        assert len(temps) == 0


class TestLoadAllManifests:
    def test_loads_multiple(self, tmp_path: Path) -> None:
        for src in ("a", "b"):
            save_manifest_locked(SourceManifest(source=src), tmp_path)
        result = load_all_manifests(tmp_path)
        assert set(result) == {"a", "b"}

    def test_skips_corrupt(self, tmp_path: Path) -> None:
        save_manifest_locked(SourceManifest(source="good"), tmp_path)
        (tmp_path / "_manifest_bad.json").write_text("{corrupt", encoding="utf-8")
        result = load_all_manifests(tmp_path)
        assert set(result) == {"good"}

    def test_empty_dir(self, tmp_path: Path) -> None:
        assert load_all_manifests(tmp_path) == {}

    def test_nonexistent_dir(self, tmp_path: Path) -> None:
        assert load_all_manifests(tmp_path / "nope") == {}
