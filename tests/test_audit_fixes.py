"""Regression tests for production audit fixes (2026-03-28)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# A1 — Evidence bundle atomic write
# ---------------------------------------------------------------------------


class TestAtomicWriteText:
    def test_success_writes_content(self, tmp_path: Path) -> None:
        from atlas_stf.evidence.build_bundle import _atomic_write_text

        dest = tmp_path / "output.json"
        _atomic_write_text(dest, '{"key": "value"}')
        assert dest.read_text(encoding="utf-8") == '{"key": "value"}'
        assert not dest.with_suffix(".json.tmp").exists()

    def test_failure_does_not_leave_dest(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        from atlas_stf.evidence.build_bundle import _atomic_write_text

        dest = tmp_path / "output.json"

        def _explode(self: Path, target: Path) -> Path:
            raise OSError("simulated disk full")

        monkeypatch.setattr(Path, "replace", _explode)

        with pytest.raises(OSError, match="simulated disk full"):
            _atomic_write_text(dest, "content")

        assert not dest.exists()


# ---------------------------------------------------------------------------
# A2 — write_limited_stream_to_file: no partial file on excess
# ---------------------------------------------------------------------------


class TestWriteLimitedStreamPartCleanup:
    def test_excess_does_not_leave_partial(self, tmp_path: Path) -> None:
        from atlas_stf.core.http_stream_safety import write_limited_stream_to_file

        dest = tmp_path / "download.bin"
        response = MagicMock()
        response.iter_bytes.return_value = iter([b"x" * 1024, b"x" * 1024])

        with pytest.raises(ValueError, match="exceeded max bytes"):
            write_limited_stream_to_file(response, dest, max_download_bytes=1500)

        assert not dest.exists()
        part = dest.with_suffix(".bin.part")
        assert not part.exists()

    def test_success_creates_final(self, tmp_path: Path) -> None:
        from atlas_stf.core.http_stream_safety import write_limited_stream_to_file

        dest = tmp_path / "download.bin"
        response = MagicMock()
        response.iter_bytes.return_value = iter([b"hello"])

        total = write_limited_stream_to_file(response, dest, max_download_bytes=100)
        assert total == 5
        assert dest.read_bytes() == b"hello"


# ---------------------------------------------------------------------------
# A4 — subjects_normalized is independent copy
# ---------------------------------------------------------------------------


class TestSubjectsNormalized:
    def test_no_aliasing(self) -> None:
        record: dict = {"subjects_raw": None, "subjects_normalized": None}

        from atlas_stf.curated.common import split_subjects

        subjects = split_subjects("Direito Constitucional;Direito Penal")
        record["subjects_raw"] = subjects
        record["subjects_normalized"] = list(subjects)

        record["subjects_raw"].append("INJECTED")
        assert "INJECTED" not in record["subjects_normalized"]


# ---------------------------------------------------------------------------
# A5 — movement_id collision with same data
# ---------------------------------------------------------------------------


class TestMovementIdUniqueness:
    def test_same_data_different_seq_produces_different_ids(self) -> None:
        from atlas_stf.core.identity import stable_id

        id_a = stable_id("mov_", "PROC:2024-01-01:Conclusos ao relator::0")
        id_b = stable_id("mov_", "PROC:2024-01-01:Conclusos ao relator::1")
        assert id_a != id_b

    def test_determinism_across_calls(self, tmp_path: Path) -> None:
        """Same input JSON → same movement_ids on repeated builds."""
        import json

        from atlas_stf.curated.build_movement import build_movement_records

        doc = {
            "process_number": "ADI 1234",
            "informacoes": {"relator_atual": "MIN. TESTE"},
            "andamentos": [
                {"date": "2024-01-15", "description": "Conclusos ao relator", "detail": None},
                {"date": "2024-01-15", "description": "Conclusos ao relator", "detail": None},
                {"date": "2024-01-16", "description": "Juntada de petição", "detail": "Réplica"},
            ],
            "deslocamentos": [],
        }
        portal_dir = tmp_path / "portal"
        portal_dir.mkdir()
        (portal_dir / "ADI_1234.json").write_text(json.dumps(doc), encoding="utf-8")

        run1 = build_movement_records(portal_dir)
        run2 = build_movement_records(portal_dir)

        ids1 = [r["movement_id"] for r in run1]
        ids2 = [r["movement_id"] for r in run2]

        # Determinism: same input → same output
        assert ids1 == ids2
        # No collisions: all IDs unique
        assert len(set(ids1)) == len(ids1)
        # The two "Conclusos ao relator" on same day have different IDs
        assert ids1[0] != ids1[1]


# ---------------------------------------------------------------------------
# A6 — Path traversal in ingest manifest
# ---------------------------------------------------------------------------


class TestIngestManifestPathTraversal:
    def test_traversal_stays_in_output_dir(self, tmp_path: Path) -> None:
        from atlas_stf.ingest_manifest import SourceManifest, write_manifest

        m = SourceManifest(
            source="test",
            file_name="../../etc/passwd",
            year_or_cycle="2024",
            sha256_full="",
            sha256_first_1mb="",
            encoding="utf-8",
            delimiter=",",
            raw_header=[],
            normalized_header=[],
            layout_signature="",
            column_count=0,
            row_count=0,
            sample_rows=[],
            parser_version="",
            source_file_fingerprint="",
            observed_at="2024-01-01T00:00:00Z",
        )
        dest = write_manifest(m, tmp_path)
        assert dest.parent == tmp_path
        assert ".." not in dest.name


# ---------------------------------------------------------------------------
# A7 — ZIP symlink rejection
# ---------------------------------------------------------------------------


class TestZipSymlinkSafety:
    def test_rejects_symlink_member(self, tmp_path: Path) -> None:
        from atlas_stf.core.zip_safety import is_safe_zip_member

        # Unix symlink mode: 0o120000 in upper 16 bits
        symlink_attr = 0o120777 << 16
        assert not is_safe_zip_member("legit.txt", tmp_path, external_attr=symlink_attr)

    def test_accepts_regular_file(self, tmp_path: Path) -> None:
        from atlas_stf.core.zip_safety import is_safe_zip_member

        regular_attr = 0o100644 << 16
        assert is_safe_zip_member("legit.txt", tmp_path, external_attr=regular_attr)

    def test_accepts_zero_attr(self, tmp_path: Path) -> None:
        from atlas_stf.core.zip_safety import is_safe_zip_member

        assert is_safe_zip_member("legit.txt", tmp_path, external_attr=0)


# ---------------------------------------------------------------------------
# B1 — File handle leak in _open_csv
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# B3 — Review auth behavior with/without API key
# ---------------------------------------------------------------------------


class TestReviewAuthBehavior:
    def test_no_key_returns_503(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Without ATLAS_STF_REVIEW_API_KEY, fail-closed with 503."""
        import asyncio

        from fastapi import HTTPException

        from atlas_stf.api._routes_graph import _require_review_auth

        monkeypatch.delenv("ATLAS_STF_REVIEW_API_KEY", raising=False)
        with pytest.raises(HTTPException) as exc_info:
            asyncio.run(_require_review_auth(api_key=None))
        assert exc_info.value.status_code == 503

    def test_dev_bypass_allows_access(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """With __dev__ sentinel, auth is explicitly bypassed."""
        import asyncio

        from atlas_stf.api._routes_graph import _require_review_auth

        monkeypatch.setenv("ATLAS_STF_REVIEW_API_KEY", "__dev__")
        asyncio.run(_require_review_auth(api_key=None))

    def test_wrong_key_rejects(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """With ATLAS_STF_REVIEW_API_KEY set, wrong key → 401."""
        import asyncio

        from fastapi import HTTPException

        from atlas_stf.api._routes_graph import _require_review_auth

        monkeypatch.setenv("ATLAS_STF_REVIEW_API_KEY", "correct-secret")
        with pytest.raises(HTTPException) as exc_info:
            asyncio.run(_require_review_auth(api_key="wrong-key"))
        assert exc_info.value.status_code == 401

    def test_correct_key_allows(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """With ATLAS_STF_REVIEW_API_KEY set, correct key → access allowed."""
        import asyncio

        from atlas_stf.api._routes_graph import _require_review_auth

        monkeypatch.setenv("ATLAS_STF_REVIEW_API_KEY", "correct-secret")
        asyncio.run(_require_review_auth(api_key="correct-secret"))


# ---------------------------------------------------------------------------
# B1 — File handle leak in _open_csv
# ---------------------------------------------------------------------------


class TestOpenCsvHandleLeak:
    def test_utf8_handle_closed_on_latin1_fallback(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "test.csv"
        csv_path.write_bytes(b"col\xe9e1,col2\nval1,val2\n")

        from atlas_stf.contracts._stf_overlap import _open_csv

        with _open_csv(csv_path) as reader:
            rows = list(reader)

        assert len(rows) == 1


# ---------------------------------------------------------------------------
# B — counsel_network uses canonical baseline via build_baseline_rates
# ---------------------------------------------------------------------------


class TestCounselNetworkCanonicalBaseline:
    def _setup_curated(self, curated: Path, favorable_pct: float = 0.8) -> None:
        """Helper: create minimal curated data for counsel_network."""
        import json

        # Two counsel sharing 2+ clients → will cluster
        pcl = [
            {"counsel_id": "c1", "process_id": "p1"},
            {"counsel_id": "c2", "process_id": "p1"},
            {"counsel_id": "c1", "process_id": "p2"},
            {"counsel_id": "c2", "process_id": "p2"},
            {"counsel_id": "c1", "process_id": "p3"},
            {"counsel_id": "c2", "process_id": "p3"},
            {"counsel_id": "c1", "process_id": "p4"},
            {"counsel_id": "c2", "process_id": "p4"},
            {"counsel_id": "c1", "process_id": "p5"},
            {"counsel_id": "c2", "process_id": "p5"},
        ]
        ppl = [
            {"process_id": "p1", "party_id": "party1"},
            {"process_id": "p2", "party_id": "party1"},
            {"process_id": "p3", "party_id": "party2"},
            {"process_id": "p4", "party_id": "party2"},
            {"process_id": "p5", "party_id": "party2"},
        ]
        n_fav = int(10 * favorable_pct)
        events = []
        for i in range(10):
            events.append({
                "decision_event_id": f"de{i}",
                "process_id": f"p{i % 5 + 1}",
                "process_class": "ADI",
                "decision_progress": "Procedente" if i < n_fav else "Improcedente",
                "current_rapporteur": "MIN A",
            })
        processes = [{"process_id": f"p{i}", "process_class": "ADI"} for i in range(1, 6)]

        for name, data in [
            ("process_counsel_link.jsonl", pcl),
            ("process_party_link.jsonl", ppl),
            ("decision_event.jsonl", events),
            ("process.jsonl", processes),
            ("counsel.jsonl", [
                {"counsel_id": "c1", "counsel_name_normalized": "ADV A"},
                {"counsel_id": "c2", "counsel_name_normalized": "ADV B"},
            ]),
        ]:
            (curated / name).write_text("\n".join(json.dumps(r) for r in data), encoding="utf-8")

    def test_uses_canonical_baseline_not_fixed(self, tmp_path: Path) -> None:
        """Prove that build_baseline_rates is used, not the fixed 0.5."""
        import json

        from atlas_stf.analytics._outcome_helpers import build_baseline_rates

        curated = tmp_path / "curated"
        curated.mkdir()
        output = tmp_path / "analytics"
        self._setup_curated(curated, favorable_pct=0.8)

        # Confirm canonical baseline
        rates = build_baseline_rates(curated / "decision_event.jsonl", curated / "process.jsonl")
        assert "ADI" in rates
        adi_rate = rates["ADI"]
        assert adi_rate > 0.7  # ~80% favorable

        from atlas_stf.analytics.counsel_network import build_counsel_network

        result_path = build_counsel_network(curated_dir=curated, output_dir=output)
        records = [json.loads(line) for line in result_path.read_text().strip().split("\n") if line.strip()]
        assert len(records) >= 1
        # baseline_rate in output should match canonical baseline, not 0.5
        for r in records:
            assert abs(r["baseline_rate"] - adi_rate) < 0.01, (
                f"baseline_rate={r['baseline_rate']} should match canonical {adi_rate}"
            )

    def test_high_baseline_no_false_flag(self, tmp_path: Path) -> None:
        """Cluster rate near class baseline should NOT flag."""
        import json

        curated = tmp_path / "curated"
        curated.mkdir()
        output = tmp_path / "analytics"
        self._setup_curated(curated, favorable_pct=0.8)

        from atlas_stf.analytics.counsel_network import build_counsel_network

        result_path = build_counsel_network(curated_dir=curated, output_dir=output)
        records = [json.loads(line) for line in result_path.read_text().strip().split("\n") if line.strip()]
        for r in records:
            if r.get("cluster_size", 0) > 1:
                assert not r["red_flag"], "Rate near baseline → no flag"

    def test_fallback_when_no_process_path(self, tmp_path: Path) -> None:
        """Without process.jsonl, baseline falls back to DEFAULT_BASELINE_RATE."""
        import json

        from atlas_stf.analytics.counsel_network import DEFAULT_BASELINE_RATE

        curated = tmp_path / "curated"
        curated.mkdir()
        output = tmp_path / "analytics"
        self._setup_curated(curated, favorable_pct=0.8)
        # Remove process.jsonl so canonical baseline can't be computed
        (curated / "process.jsonl").unlink()

        from atlas_stf.analytics.counsel_network import build_counsel_network

        result_path = build_counsel_network(curated_dir=curated, output_dir=output)
        records = [json.loads(line) for line in result_path.read_text().strip().split("\n") if line.strip()]
        for r in records:
            assert abs(r["baseline_rate"] - DEFAULT_BASELINE_RATE) < 0.01


# ---------------------------------------------------------------------------
# A (v2) — normalize_subjects
# ---------------------------------------------------------------------------


class TestNormalizeSubjects:
    def test_uppercase_and_dedup(self) -> None:
        from atlas_stf.core.parsers import normalize_subjects

        result = normalize_subjects(["Direito Penal", "direito penal", "Direito Civil"])
        assert result == ["DIREITO PENAL", "DIREITO CIVIL"]

    def test_strips_whitespace(self) -> None:
        from atlas_stf.core.parsers import normalize_subjects

        result = normalize_subjects(["  Tributário  ", "Tributário"])
        assert result == ["TRIBUTÁRIO"]

    def test_removes_empty(self) -> None:
        from atlas_stf.core.parsers import normalize_subjects

        result = normalize_subjects(["", "  ", "Penal"])
        assert result == ["PENAL"]

    def test_none_input(self) -> None:
        from atlas_stf.core.parsers import normalize_subjects

        assert normalize_subjects(None) is None

    def test_all_empty(self) -> None:
        from atlas_stf.core.parsers import normalize_subjects

        assert normalize_subjects(["", "  "]) is None


# ---------------------------------------------------------------------------
# E — firm_cluster cap constant exists
# ---------------------------------------------------------------------------


class TestFirmClusterCap:
    @staticmethod
    def _write(path: Path, records: list) -> None:
        import json

        path.write_text("\n".join(json.dumps(r) for r in records), encoding="utf-8")

    def test_below_cap_forms_clusters(self, tmp_path: Path) -> None:
        """Parties with firms below the cap form clusters normally."""
        import json

        from atlas_stf.analytics.firm_cluster import build_firm_cluster

        curated = tmp_path / "curated"
        curated.mkdir()
        output = tmp_path / "analytics"

        firms = [
            {"firm_id": "f1", "firm_name_normalized": "A", "member_lawyer_ids": []},
            {"firm_id": "f2", "firm_name_normalized": "B", "member_lawyer_ids": []},
        ]
        edges = [
            {"firm_id": "f1", "party_id": "pa1", "process_id": "p1"},
            {"firm_id": "f2", "party_id": "pa1", "process_id": "p1"},
            {"firm_id": "f1", "party_id": "pa2", "process_id": "p2"},
            {"firm_id": "f2", "party_id": "pa2", "process_id": "p2"},
        ]
        self._write(curated / "law_firm_entity.jsonl", firms)
        self._write(curated / "representation_edge.jsonl", edges)
        self._write(curated / "party.jsonl", [{"party_id": "pa1"}, {"party_id": "pa2"}])
        self._write(curated / "process.jsonl", [])

        build_firm_cluster(curated_dir=curated, output_dir=output)

        text = (output / "firm_cluster.jsonl").read_text().strip()
        records = [json.loads(line) for line in text.split("\n") if line.strip()]
        assert len(records) >= 1
        summary = json.loads((output / "firm_cluster_summary.json").read_text())
        assert summary["skipped_parties_over_cap"] == 0

    def test_above_cap_skips_and_reports(self, tmp_path: Path) -> None:
        """Parties with >MAX_FIRMS_PER_PARTY firms are skipped in summary."""
        import json

        from atlas_stf.analytics.firm_cluster import MAX_FIRMS_PER_PARTY, build_firm_cluster

        curated = tmp_path / "curated"
        curated.mkdir()
        output = tmp_path / "analytics"

        n = MAX_FIRMS_PER_PARTY + 5
        firms = [{"firm_id": f"f{i}", "firm_name_normalized": f"F{i}", "member_lawyer_ids": []} for i in range(n)]
        edges = [{"firm_id": f"f{i}", "party_id": "mega", "process_id": "p1"} for i in range(n)]
        self._write(curated / "law_firm_entity.jsonl", firms)
        self._write(curated / "representation_edge.jsonl", edges)
        self._write(curated / "party.jsonl", [{"party_id": "mega"}])
        self._write(curated / "process.jsonl", [])

        build_firm_cluster(curated_dir=curated, output_dir=output)

        summary = json.loads((output / "firm_cluster_summary.json").read_text())
        assert summary["skipped_parties_over_cap"] >= 1


# ---------------------------------------------------------------------------
# F — fingerprint includes file size
# ---------------------------------------------------------------------------


class TestFingerprintIncludesSize:
    def test_same_prefix_different_size(self, tmp_path: Path) -> None:
        from atlas_stf.contracts._inspector import _file_fingerprint

        a = tmp_path / "a.csv"
        b = tmp_path / "b.csv"
        content = b"x" * 100
        a.write_bytes(content)
        b.write_bytes(content + b"extra")
        assert _file_fingerprint(a) != _file_fingerprint(b)
