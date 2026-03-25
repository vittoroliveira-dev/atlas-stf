"""Tests for datajud/_runner.py — manifest-based, no legacy checkpoint."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from atlas_stf.datajud._config import DatajudFetchConfig
from atlas_stf.datajud._runner import discover_indices, fetch_origin_data, fetch_single_index


def _make_process_jsonl(tmp_path: Path, records: list[dict]) -> Path:
    process_path = tmp_path / "process.jsonl"
    process_path.write_text(
        "\n".join(json.dumps(r) for r in records),
        encoding="utf-8",
    )
    return process_path


class TestDiscoverIndices:
    def test_discovers_from_process(self, tmp_path: Path) -> None:
        process_path = _make_process_jsonl(
            tmp_path,
            [
                {
                    "process_id": "p1",
                    "origin_court_or_body": "TRIBUNAL REGIONAL FEDERAL",
                    "origin_description": "SAO PAULO",
                },
                {
                    "process_id": "p2",
                    "origin_court_or_body": "TRIBUNAL DE JUSTICA ESTADUAL",
                    "origin_description": "RIO DE JANEIRO",
                },
            ],
        )
        indices = discover_indices(process_path)
        assert "api_publica_trf3" in indices
        assert "api_publica_tjrj" in indices

    def test_deduplicates(self, tmp_path: Path) -> None:
        records = [
            {
                "process_id": f"p{i}",
                "origin_court_or_body": "TRIBUNAL REGIONAL FEDERAL",
                "origin_description": "SAO PAULO",
            }
            for i in range(5)
        ]
        process_path = _make_process_jsonl(tmp_path, records)
        indices = discover_indices(process_path)
        assert indices.count("api_publica_trf3") == 1

    def test_stable_between_runs(self, tmp_path: Path) -> None:
        """unit_id stability: same input → same indices."""
        records = [{"process_id": "p1", "origin_court_or_body": "STJ", "origin_description": ""}]
        path = _make_process_jsonl(tmp_path, records)
        run1 = discover_indices(path)
        run2 = discover_indices(path)
        assert run1 == run2

    def test_empty_process_returns_empty(self, tmp_path: Path) -> None:
        path = _make_process_jsonl(tmp_path, [{"process_id": "p1"}])
        assert discover_indices(path) == []


class TestFetchSingleIndex:
    @patch("atlas_stf.datajud._runner.DatajudClient")
    def test_writes_json_and_returns_result(self, mock_cls: MagicMock, tmp_path: Path) -> None:
        mock_client = MagicMock()
        mock_client.search.return_value = {
            "hits": {"total": {"value": 500}},
            "aggregations": {
                "top_assuntos": {"buckets": [{"key": "Civil", "doc_count": 200}]},
                "top_orgaos": {"buckets": []},
                "classes": {"buckets": []},
            },
        }

        result = fetch_single_index(mock_client, "api_publica_stj", tmp_path)
        assert result["index"] == "api_publica_stj"
        assert result["total_processes"] == 500

        out_file = tmp_path / "api_publica_stj.json"
        assert out_file.exists()
        data = json.loads(out_file.read_text("utf-8"))
        assert data["total_processes"] == 500


class TestFetchOriginData:
    def test_dry_run(self, tmp_path: Path) -> None:
        process_path = _make_process_jsonl(
            tmp_path,
            [
                {
                    "process_id": "p1",
                    "origin_court_or_body": "TRIBUNAL REGIONAL FEDERAL",
                    "origin_description": "SAO PAULO",
                },
            ],
        )
        output_dir = tmp_path / "output"
        config = DatajudFetchConfig(api_key="test", process_path=process_path, output_dir=output_dir, dry_run=True)
        result = fetch_origin_data(config)
        assert result == output_dir
        assert not list(output_dir.glob("api_publica_*.json"))

    @patch("atlas_stf.datajud._runner.DatajudClient")
    def test_full_flow_writes_manifest(self, mock_client_cls: MagicMock, tmp_path: Path) -> None:
        process_path = _make_process_jsonl(
            tmp_path,
            [
                {"process_id": "p1", "origin_court_or_body": "STJ", "origin_description": ""},
            ],
        )
        output_dir = tmp_path / "output"

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.search.return_value = {
            "hits": {"total": {"value": 1000}},
            "aggregations": {
                "top_assuntos": {"buckets": [{"key": "Direito Civil", "doc_count": 500}]},
                "top_orgaos": {"buckets": [{"key": "1a Turma", "doc_count": 200}]},
                "classes": {"buckets": [{"key": "Recurso Especial", "doc_count": 800}]},
            },
        }
        mock_client_cls.return_value = mock_client

        config = DatajudFetchConfig(api_key="test", process_path=process_path, output_dir=output_dir)
        fetch_origin_data(config)

        # JSON output exists
        result_file = output_dir / "api_publica_stj.json"
        assert result_file.exists()
        data = json.loads(result_file.read_text("utf-8"))
        assert data["index"] == "api_publica_stj"
        assert data["total_processes"] == 1000

        # Manifest exists (no legacy checkpoint)
        manifest_file = output_dir / "_manifest_datajud.json"
        assert manifest_file.exists()
        manifest_data = json.loads(manifest_file.read_text("utf-8"))
        assert manifest_data["source"] == "datajud"
        assert any("stj" in uid for uid in manifest_data["units"])

        # No legacy checkpoint
        assert not (output_dir / "_checkpoint.json").exists()

    @patch("atlas_stf.datajud._runner.DatajudClient")
    def test_manifest_skip_committed(self, mock_client_cls: MagicMock, tmp_path: Path) -> None:
        """Committed units in manifest are skipped (no API calls)."""
        process_path = _make_process_jsonl(
            tmp_path,
            [
                {"process_id": "p1", "origin_court_or_body": "STJ", "origin_description": ""},
            ],
        )
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        # Pre-seed manifest with committed unit
        from atlas_stf.fetch._manifest_model import FetchUnit, RemoteState, SourceManifest, build_unit_id
        from atlas_stf.fetch._manifest_store import save_manifest_locked

        uid = build_unit_id("datajud", "api_publica_stj")
        manifest = SourceManifest(source="datajud")
        manifest.units[uid] = FetchUnit(
            unit_id=uid,
            source="datajud",
            label="DataJud api_publica_stj",
            remote_url="",
            remote_state=RemoteState(url=""),
            local_path=str(output_dir / "api_publica_stj.json"),
            status="committed",
        )
        save_manifest_locked(manifest, output_dir)

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        config = DatajudFetchConfig(api_key="test", process_path=process_path, output_dir=output_dir)
        fetch_origin_data(config)

        # API should not be called — unit was already committed
        mock_client.search.assert_not_called()

    def test_no_legacy_checkpoint_import(self) -> None:
        """Ensure _load_checkpoint and _save_checkpoint are no longer in the module."""
        import atlas_stf.datajud._runner as runner_module

        assert not hasattr(runner_module, "_load_checkpoint")
        assert not hasattr(runner_module, "_save_checkpoint")


class TestFetchPlan:
    def test_plan_generates_datajud_items(self, tmp_path: Path) -> None:
        """fetch plan discovers DataJud units correctly."""
        from atlas_stf.fetch._manifest_planner import generate_plan

        process_path = _make_process_jsonl(
            tmp_path,
            [
                {"process_id": "p1", "origin_court_or_body": "STJ", "origin_description": ""},
            ],
        )
        base_dir = tmp_path / "raw"
        base_dir.mkdir()
        (base_dir / "datajud").mkdir()

        plan = generate_plan(
            sources=["datajud"],
            base_dir=base_dir,
            discovery_kwargs={"datajud": {"process_path": process_path}},
        )

        datajud_items = [i for i in plan.items if i.source == "datajud"]
        assert len(datajud_items) >= 1
        assert all(i.unit_id.startswith("datajud:") for i in datajud_items)

    def test_plan_id_is_deterministic(self, tmp_path: Path) -> None:
        """Same input → same plan_id (volatile fields excluded from fingerprint)."""
        from atlas_stf.fetch._manifest_planner import generate_plan

        process_path = _make_process_jsonl(
            tmp_path,
            [
                {"process_id": "p1", "origin_court_or_body": "STJ", "origin_description": ""},
            ],
        )
        base_dir = tmp_path / "raw"
        base_dir.mkdir()
        (base_dir / "datajud").mkdir()
        kwargs: dict = {"datajud": {"process_path": process_path}}

        plan1 = generate_plan(sources=["datajud"], base_dir=base_dir, discovery_kwargs=kwargs)
        plan2 = generate_plan(sources=["datajud"], base_dir=base_dir, discovery_kwargs=kwargs)
        assert plan1.plan_id == plan2.plan_id

    def test_generate_plan_is_read_only(self, tmp_path: Path) -> None:
        """generate_plan does not write to disk."""
        from atlas_stf.fetch._manifest_planner import generate_plan

        process_path = _make_process_jsonl(
            tmp_path,
            [
                {"process_id": "p1", "origin_court_or_body": "STJ", "origin_description": ""},
            ],
        )
        base_dir = tmp_path / "raw"
        base_dir.mkdir()
        dj_dir = base_dir / "datajud"
        dj_dir.mkdir()

        files_before = set(dj_dir.iterdir())
        generate_plan(
            sources=["datajud"], base_dir=base_dir, discovery_kwargs={"datajud": {"process_path": process_path}}
        )
        files_after = set(dj_dir.iterdir())
        assert files_before == files_after
