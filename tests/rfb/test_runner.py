"""Tests for RFB fetch runner."""

from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path
from unittest.mock import patch

from atlas_stf.rfb import _runner
from atlas_stf.rfb._config import RfbFetchConfig
from atlas_stf.rfb._runner import (
    _build_target_names,
    _compute_tse_targets_hash,
    _discover_latest_month,
    _download_zip,
    _extract_csv_from_zip,
    _extract_tse_donor_targets,
    fetch_rfb_data,
)


class _FakeStreamResponse:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks

    def __enter__(self) -> _FakeStreamResponse:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def raise_for_status(self) -> None:
        return None

    def iter_bytes(self):
        yield from self._chunks


def _make_zip(csv_content: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("data.csv", csv_content)
    return buf.getvalue()


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")


class TestBuildTargetNames:
    def test_from_minister_bio(self, tmp_path: Path) -> None:
        bio_path = tmp_path / "minister_bio.json"
        bio_path.write_text(json.dumps({"m1": {"minister_name": "DIAS TOFFOLI"}}), encoding="utf-8")
        config = RfbFetchConfig(
            output_dir=tmp_path / "rfb",
            minister_bio_path=bio_path,
            party_path=tmp_path / "party.jsonl",
            counsel_path=tmp_path / "counsel.jsonl",
        )
        names = _build_target_names(config)
        assert "DIAS TOFFOLI" in names

    def test_from_party(self, tmp_path: Path) -> None:
        _write_jsonl(tmp_path / "party.jsonl", [{"party_name_normalized": "JOSE DA SILVA"}])
        config = RfbFetchConfig(
            output_dir=tmp_path / "rfb",
            minister_bio_path=tmp_path / "bio.json",
            party_path=tmp_path / "party.jsonl",
            counsel_path=tmp_path / "counsel.jsonl",
        )
        names = _build_target_names(config)
        assert "JOSE DA SILVA" in names

    def test_from_counsel(self, tmp_path: Path) -> None:
        _write_jsonl(tmp_path / "counsel.jsonl", [{"counsel_name_normalized": "ADV FULANO"}])
        config = RfbFetchConfig(
            output_dir=tmp_path / "rfb",
            minister_bio_path=tmp_path / "bio.json",
            party_path=tmp_path / "party.jsonl",
            counsel_path=tmp_path / "counsel.jsonl",
        )
        names = _build_target_names(config)
        assert "ADV FULANO" in names

    def test_build_target_names_includes_civil_name(self, tmp_path: Path) -> None:
        """minister_bio.json with civil_name -> _build_target_names returns both names."""
        bio_path = tmp_path / "minister_bio.json"
        bio_path.write_text(
            json.dumps(
                {
                    "m1": {
                        "minister_name": "DIAS TOFFOLI",
                        "civil_name": "JOSE ANTONIO DIAS TOFFOLI",
                    }
                }
            ),
            encoding="utf-8",
        )
        config = RfbFetchConfig(
            output_dir=tmp_path / "rfb",
            minister_bio_path=bio_path,
            party_path=tmp_path / "party.jsonl",
            counsel_path=tmp_path / "counsel.jsonl",
        )
        names = _build_target_names(config)
        assert "DIAS TOFFOLI" in names
        assert "JOSE ANTONIO DIAS TOFFOLI" in names


class TestExtractTseDonorTargets:
    def test_pj(self, tmp_path: Path) -> None:
        """Valid 14-digit CNPJ -> cnpj_basico in pj set + full CNPJ in pj_full set."""
        donations = tmp_path / "donations_raw.jsonl"
        # 11222333000181 is a valid CNPJ
        _write_jsonl(donations, [{"donor_cpf_cnpj": "11222333000181", "donor_name_normalized": "X"}])
        pj_basico, pf_cpfs, pj_full = _extract_tse_donor_targets(donations)
        assert "11222333" in pj_basico
        assert "11222333000181" in pj_full
        assert len(pf_cpfs) == 0

    def test_pf(self, tmp_path: Path) -> None:
        """Valid 11-digit CPF -> in pf set."""
        donations = tmp_path / "donations_raw.jsonl"
        # 52998224725 is a valid CPF
        _write_jsonl(donations, [{"donor_cpf_cnpj": "52998224725", "donor_name_normalized": "Y"}])
        pj_basico, pf_cpfs, pj_full = _extract_tse_donor_targets(donations)
        assert "52998224725" in pf_cpfs
        assert len(pj_basico) == 0
        assert len(pj_full) == 0

    def test_masked(self, tmp_path: Path) -> None:
        """Masked CPF -> ignored."""
        donations = tmp_path / "donations_raw.jsonl"
        _write_jsonl(donations, [{"donor_cpf_cnpj": "***.982.247-**", "donor_name_normalized": "Z"}])
        pj_basico, pf_cpfs, pj_full = _extract_tse_donor_targets(donations)
        assert len(pj_basico) == 0
        assert len(pf_cpfs) == 0
        assert len(pj_full) == 0

    def test_empty(self, tmp_path: Path) -> None:
        """No file -> 3 empty sets."""
        pj_basico, pf_cpfs, pj_full = _extract_tse_donor_targets(tmp_path / "nonexistent.jsonl")
        assert pj_basico == set()
        assert pf_cpfs == set()
        assert pj_full == set()

    def test_invalid_checksum(self, tmp_path: Path) -> None:
        """Invalid CNPJ/CPF -> ignored."""
        donations = tmp_path / "donations_raw.jsonl"
        _write_jsonl(donations, [{"donor_cpf_cnpj": "12345678000100", "donor_name_normalized": "BAD"}])
        pj_basico, pf_cpfs, pj_full = _extract_tse_donor_targets(donations)
        assert len(pj_basico) == 0
        assert len(pj_full) == 0

    def test_missing_file(self, tmp_path: Path) -> None:
        """Missing file -> 3 empty sets, no error."""
        pj, cpfs, full = _extract_tse_donor_targets(tmp_path / "does_not_exist.jsonl")
        assert pj == set()
        assert cpfs == set()
        assert full == set()

    def test_load_tse_targets_logs_malformed_jsonl(self, tmp_path: Path, caplog) -> None:
        """Malformed JSONL line -> logged warning, valid lines still processed."""
        import logging

        donations = tmp_path / "donations_raw.jsonl"
        # 11222333000181 is a valid CNPJ
        donations.write_text(
            '{"donor_cpf_cnpj": "11222333000181", "donor_name_normalized": "OK"}\n'
            "NOT VALID JSON\n",
            encoding="utf-8",
        )
        with caplog.at_level(logging.WARNING, logger="atlas_stf.rfb._runner"):
            pj_basico, pf_cpfs, pj_full = _extract_tse_donor_targets(donations)
        assert "malformed" in caplog.text.lower()
        assert "11222333" in pj_basico


class TestTseTargetsHash:
    def test_deterministic(self) -> None:
        h1 = _compute_tse_targets_hash({"A", "B"}, {"C"}, {"D"})
        h2 = _compute_tse_targets_hash({"B", "A"}, {"C"}, {"D"})
        assert h1 == h2

    def test_changes_on_different_input(self) -> None:
        h1 = _compute_tse_targets_hash({"A"}, set(), set())
        h2 = _compute_tse_targets_hash({"B"}, set(), set())
        assert h1 != h2


class TestCheckpointTseIntegration:
    def test_checkpoint_without_tse_hash(self, tmp_path: Path) -> None:
        """Old checkpoint without tse_targets_hash loads normally."""
        from atlas_stf.rfb._runner import _load_checkpoint

        checkpoint_path = tmp_path / "_rfb_checkpoint.json"
        checkpoint_path.write_text(json.dumps({
            "completed_socios_pass1": [0, 1],
            "completed_socios_pass2": [],
            "completed_empresas": [],
            "completed_estabelecimentos": [],
            "completed_reference": True,
            "cnpjs": ["12345678"],
        }))
        cp = _load_checkpoint(tmp_path)
        assert "tse_targets_hash" not in cp
        assert cp["completed_socios_pass1"] == [0, 1]

    def test_checkpoint_invalidation_on_tse_change(self, tmp_path: Path) -> None:
        """When TSE hash changes, passes are invalidated."""
        from atlas_stf.rfb._runner import _load_checkpoint, _save_checkpoint

        old_hash = _compute_tse_targets_hash({"A"}, set(), set())
        checkpoint_path = tmp_path / "_rfb_checkpoint.json"
        checkpoint_path.write_text(json.dumps({
            "completed_socios_pass1": [0, 1, 2],
            "completed_socios_pass2": [0],
            "completed_empresas": [0],
            "completed_estabelecimentos": [0],
            "completed_reference": True,
            "cnpjs": ["12345678"],
            "tse_targets_hash": old_hash,
        }))

        cp = _load_checkpoint(tmp_path)
        new_hash = _compute_tse_targets_hash({"A", "B"}, set(), set())
        assert old_hash != new_hash

        # Simulate what fetch_rfb_data does
        if cp.get("tse_targets_hash", "") and cp["tse_targets_hash"] != new_hash:
            cp["completed_socios_pass1"] = []
            cp["completed_socios_pass2"] = []
            cp["completed_empresas"] = []
            cp["completed_estabelecimentos"] = []
            cp["cnpjs"] = []
            _save_checkpoint(tmp_path, cp)

        reloaded = _load_checkpoint(tmp_path)
        assert reloaded["completed_socios_pass1"] == []
        assert reloaded["completed_empresas"] == []


class TestExtractCsvFromZip:
    def test_valid_zip(self) -> None:
        zip_path = Path("/tmp/test-rfb-valid.zip")
        zip_path.write_bytes(_make_zip("a;b;c\n1;2;3\n"))
        result = _extract_csv_from_zip(zip_path)
        assert result is not None
        assert b"a;b;c" in result
        zip_path.unlink(missing_ok=True)

    def test_bad_zip(self) -> None:
        zip_path = Path("/tmp/test-rfb-invalid.zip")
        zip_path.write_bytes(b"not a zip")
        assert _extract_csv_from_zip(zip_path) is None
        zip_path.unlink(missing_ok=True)

    def test_zip_without_csv(self) -> None:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("readme.txt", "hello")
        zip_path = Path("/tmp/test-rfb-no-csv.zip")
        zip_path.write_bytes(buf.getvalue())
        assert _extract_csv_from_zip(zip_path) is None
        zip_path.unlink(missing_ok=True)

    def test_mainframe_style_filename(self) -> None:
        """RFB ZIPs use mainframe-style names like K3241.K03200Y0.D60214.SOCIOCSV."""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("K3241.K03200Y0.D60214.SOCIOCSV", "a;b;c\n1;2;3\n")
        zip_path = Path("/tmp/test-rfb-mainframe.zip")
        zip_path.write_bytes(buf.getvalue())
        result = _extract_csv_from_zip(zip_path)
        assert result is not None
        assert b"a;b;c" in result
        zip_path.unlink(missing_ok=True)

    def test_path_traversal_rejected(self) -> None:
        """ZIP with path traversal names should be rejected."""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("../../etc/passwd.csv", "malicious;data\n")
        zip_path = Path("/tmp/test-rfb-traversal.zip")
        zip_path.write_bytes(buf.getvalue())
        assert _extract_csv_from_zip(zip_path) is None
        zip_path.unlink(missing_ok=True)

    def test_absolute_path_rejected(self) -> None:
        """ZIP with absolute path should be rejected."""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("/etc/data.csv", "malicious;data\n")
        zip_path = Path("/tmp/test-rfb-absolute.zip")
        zip_path.write_bytes(buf.getvalue())
        assert _extract_csv_from_zip(zip_path) is None
        zip_path.unlink(missing_ok=True)

    def test_rejects_zip_above_uncompressed_limit(self, monkeypatch) -> None:
        monkeypatch.setattr("atlas_stf.rfb._runner._RFB_MAX_ZIP_UNCOMPRESSED_BYTES", 1)
        zip_path = Path("/tmp/test-rfb-too-large.zip")
        zip_path.write_bytes(_make_zip("a;b;c\n1;2;3\n"))
        assert _extract_csv_from_zip(zip_path) is None
        zip_path.unlink(missing_ok=True)


class TestDiscoverLatestMonth:
    def test_skips_webdav_without_token(self, monkeypatch) -> None:
        original = _runner._active_share_token[0]
        _runner._active_share_token[0] = ""
        try:
            with patch("atlas_stf.rfb._runner.httpx.Client") as client_cls:
                assert _discover_latest_month(timeout=5) is None
            client_cls.assert_not_called()
        finally:
            _runner._active_share_token[0] = original


class TestDownloadZip:
    @patch("atlas_stf.rfb._runner.httpx.stream")
    def test_rejects_oversized_stream(self, mock_stream, monkeypatch, tmp_path: Path) -> None:
        monkeypatch.setattr("atlas_stf.rfb._runner._RFB_MAX_DOWNLOAD_BYTES", 4)
        destination = tmp_path / "Socios0.zip"
        mock_stream.return_value = _FakeStreamResponse([b"12", b"345"])

        assert _download_zip("https://example.test/Socios0.zip", destination, timeout=5) is None
        assert not destination.exists()


class TestFetchRfbData:
    def test_dry_run(self, tmp_path: Path) -> None:
        config = RfbFetchConfig(
            output_dir=tmp_path / "rfb",
            minister_bio_path=tmp_path / "bio.json",
            party_path=tmp_path / "party.jsonl",
            counsel_path=tmp_path / "counsel.jsonl",
            dry_run=True,
        )
        result = fetch_rfb_data(config)
        assert result == config.output_dir

    @patch("atlas_stf.rfb._runner.httpx.head")
    @patch("atlas_stf.rfb._runner._download_zip")
    def test_full_run(self, mock_download, mock_head, tmp_path: Path) -> None:
        bio = tmp_path / "minister_bio.json"
        bio.write_text(json.dumps({"m1": {"minister_name": "JOSE DA SILVA"}}), encoding="utf-8")

        socios_csv = "12345678;2;JOSE DA SILVA;12345678901;49;20200101;0;0;0;0;0\n"
        socios_csv += "12345678;2;CO PARTNER;98765432100;22;20190501;0;0;0;0;0\n"
        socios_zip = _make_zip(socios_csv)

        empresas_csv = "12345678;EMPRESA XYZ LTDA;2062;;100000,50;03\n"
        empresas_zip = _make_zip(empresas_csv)

        # mock_download returns socios_zip for Socios URLs, empresas_zip for Empresas
        def side_effect(url: str, destination: Path, timeout: int) -> Path:
            if "Socios" in url:
                destination.write_bytes(socios_zip)
            else:
                destination.write_bytes(empresas_zip)
            return destination

        mock_download.side_effect = side_effect

        config = RfbFetchConfig(
            output_dir=tmp_path / "rfb",
            minister_bio_path=bio,
            party_path=tmp_path / "party.jsonl",
            counsel_path=tmp_path / "counsel.jsonl",
        )
        fetch_rfb_data(config)

        partners_path = config.output_dir / "partners_raw.jsonl"
        assert partners_path.exists()
        partners = [json.loads(line) for line in partners_path.read_text(encoding="utf-8").strip().split("\n")]
        assert len(partners) >= 1
        names = {p["partner_name_normalized"] for p in partners}
        assert "JOSE DA SILVA" in names

        companies_path = config.output_dir / "companies_raw.jsonl"
        assert companies_path.exists()
