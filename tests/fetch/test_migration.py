"""Tests for legacy checkpoint migration."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from atlas_stf.fetch._manifest_store import load_manifest
from atlas_stf.fetch._migration import MigrationError, migrate_source

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_checkpoint(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# TSE donations
# ---------------------------------------------------------------------------


class TestMigrateTseDonations:
    def test_converts_checkpoint(self, tmp_path: Path) -> None:
        out = tmp_path / "tse"
        _write_checkpoint(out / "_checkpoint.json", {
            "completed_years": [2022, 2024],
            "year_meta": {
                "2022": {"url": "https://cdn.tse.jus.br/2022.zip", "content_length": 5000, "etag": '"e22"'},
                "2024": {"url": "https://cdn.tse.jus.br/2024.zip", "content_length": 8000, "etag": '"e24"'},
            },
        })

        report = migrate_source("tse_donations", out)
        assert report.units_inferred == 2
        assert report.units_committed == 2
        assert report.legacy_path is not None

        # Legacy removed
        assert not (out / "_checkpoint.json").exists()

        # Manifest written
        m = load_manifest("tse_donations", out)
        assert m is not None
        assert "tse_donations:2022" in m.units
        assert m.units["tse_donations:2022"].remote_state.etag == '"e22"'

    def test_dry_run_no_side_effects(self, tmp_path: Path) -> None:
        out = tmp_path / "tse"
        ckpt = out / "_checkpoint.json"
        _write_checkpoint(ckpt, {"completed_years": [2022], "year_meta": {}})

        report = migrate_source("tse_donations", out, dry_run=True)
        assert report.units_inferred == 1

        # Legacy not removed
        assert ckpt.exists()
        # Manifest not written
        assert load_manifest("tse_donations", out) is None


# ---------------------------------------------------------------------------
# CGU
# ---------------------------------------------------------------------------


class TestMigrateCgu:
    def test_converts_cgu_checkpoint(self, tmp_path: Path) -> None:
        out = tmp_path / "cgu"
        _write_checkpoint(out / "_checkpoint.json", {
            "csv_completed_datasets": {
                "ceis": {"content_length": 12345, "download_date": "20260101"},
                "cnep": {"content_length": 6789, "download_date": "20260101"},
            },
            "csv_download_date": "20260101",
        })

        report = migrate_source("cgu", out)
        assert report.units_committed == 2

        m = load_manifest("cgu", out)
        assert m is not None
        assert "cgu:ceis" in m.units
        assert m.units["cgu:ceis"].remote_state.content_length == 12345


# ---------------------------------------------------------------------------
# CVM
# ---------------------------------------------------------------------------


class TestMigrateCvm:
    def test_converts_cvm_checkpoint(self, tmp_path: Path) -> None:
        out = tmp_path / "cvm"
        _write_checkpoint(out / "_checkpoint.json", {
            "content_length": 99999,
            "etag": '"cvm-etag"',
            "record_count": 42,
        })

        report = migrate_source("cvm", out)
        assert report.units_committed == 1

        m = load_manifest("cvm", out)
        assert m is not None
        u = m.units["cvm:sanctions"]
        assert u.remote_state.etag == '"cvm-etag"'
        assert u.published_record_count == 42


# ---------------------------------------------------------------------------
# RFB
# ---------------------------------------------------------------------------


class TestMigrateRfb:
    def test_converts_rfb_checkpoint(self, tmp_path: Path) -> None:
        out = tmp_path / "rfb"
        _write_checkpoint(out / "_rfb_checkpoint.json", {
            "completed_socios_pass1": [0, 1, 2],
            "completed_socios_pass2": [0, 1],
            "completed_empresas": [0],
            "completed_estabelecimentos": [],
            "completed_reference": True,
            "cnpjs": ["12345678"],
        })

        report = migrate_source("rfb", out)
        # 3 pass1 + 2 pass2 + 1 empresas + 0 estabelecimentos + 1 reference = 7
        assert report.units_committed == 7

        m = load_manifest("rfb", out)
        assert m is not None
        assert "rfb:socios_pass1_0" in m.units
        assert "rfb:reference" in m.units


# ---------------------------------------------------------------------------
# DataJud
# ---------------------------------------------------------------------------


class TestMigrateDatajud:
    def test_converts_datajud_checkpoint(self, tmp_path: Path) -> None:
        out = tmp_path / "datajud"
        _write_checkpoint(out / "_checkpoint.json", {
            "completed": ["STF_SS_1", "TRF_4"],
        })

        report = migrate_source("datajud", out)
        assert report.units_committed == 2

        m = load_manifest("datajud", out)
        assert m is not None
        assert "datajud:stf_ss_1" in m.units
        assert "datajud:trf_4" in m.units


# ---------------------------------------------------------------------------
# No legacy
# ---------------------------------------------------------------------------


class TestMigrateNoLegacy:
    def test_returns_empty_report(self, tmp_path: Path) -> None:
        report = migrate_source("cvm", tmp_path / "cvm")
        assert report.units_inferred == 0
        assert report.legacy_path is None


# ---------------------------------------------------------------------------
# Transactional recovery
# ---------------------------------------------------------------------------


class TestMigrationRecovery:
    def test_marker_with_valid_manifest(self, tmp_path: Path) -> None:
        """Marker + valid manifest → conclude (remove legacy + marker)."""
        out = tmp_path / "cvm"
        out.mkdir(parents=True)

        # Write a valid manifest
        from atlas_stf.fetch._manifest_model import FetchUnit, RemoteState, SourceManifest
        from atlas_stf.fetch._manifest_store import save_manifest_locked

        m = SourceManifest(source="cvm", units={
            "cvm:sanctions": FetchUnit("cvm:sanctions", "cvm", "CVM", "", RemoteState(url=""), status="committed"),
        })
        save_manifest_locked(m, out)

        # Write marker + legacy
        marker = out / "._migration_cvm_in_progress"
        marker.write_text("", encoding="utf-8")
        _write_checkpoint(out / "_checkpoint.json", {"content_length": 1, "etag": "", "record_count": 0})

        report = migrate_source("cvm", out)
        assert report.units_committed == 1
        assert not marker.exists()
        assert not (out / "_checkpoint.json").exists()

    def test_marker_with_corrupt_manifest(self, tmp_path: Path) -> None:
        """Marker + corrupt manifest → abort with error."""
        out = tmp_path / "cvm"
        out.mkdir(parents=True)

        (out / "_manifest_cvm.json").write_text("{corrupt", encoding="utf-8")
        (out / "._migration_cvm_in_progress").write_text("", encoding="utf-8")

        with pytest.raises(MigrationError, match="corrupt"):
            migrate_source("cvm", out)

    def test_marker_no_manifest_with_legacy(self, tmp_path: Path) -> None:
        """Marker + no manifest + legacy → retry migration."""
        out = tmp_path / "cvm"
        out.mkdir(parents=True)

        (out / "._migration_cvm_in_progress").write_text("", encoding="utf-8")
        _write_checkpoint(out / "_checkpoint.json", {
            "content_length": 100, "etag": '"e"', "record_count": 5,
        })

        report = migrate_source("cvm", out)
        assert report.units_committed == 1
        assert not (out / "._migration_cvm_in_progress").exists()

    def test_marker_no_manifest_no_legacy(self, tmp_path: Path) -> None:
        """Marker + no manifest + no legacy → irrecoverable."""
        out = tmp_path / "cvm"
        out.mkdir(parents=True)
        (out / "._migration_cvm_in_progress").write_text("", encoding="utf-8")

        with pytest.raises(MigrationError, match="absent"):
            migrate_source("cvm", out)
