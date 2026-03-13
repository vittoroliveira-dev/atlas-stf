"""Tests for datajud/_runner.py."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from atlas_stf.datajud._config import DatajudFetchConfig
from atlas_stf.datajud._runner import _discover_indices, fetch_origin_data


class TestDiscoverIndices:
    def test_discovers_from_process(self, tmp_path: Path) -> None:
        process_path = tmp_path / "process.jsonl"
        process_path.write_text(
            json.dumps(
                {
                    "process_id": "p1",
                    "origin_court_or_body": "TRIBUNAL REGIONAL FEDERAL",
                    "origin_description": "SAO PAULO",
                }
            )
            + "\n"
            + json.dumps(
                {
                    "process_id": "p2",
                    "origin_court_or_body": "TRIBUNAL DE JUSTICA ESTADUAL",
                    "origin_description": "RIO DE JANEIRO",
                }
            )
            + "\n",
            encoding="utf-8",
        )
        indices = _discover_indices(process_path)
        assert "api_publica_trf3" in indices
        assert "api_publica_tjrj" in indices

    def test_deduplicates(self, tmp_path: Path) -> None:
        process_path = tmp_path / "process.jsonl"
        lines = [
            json.dumps(
                {
                    "process_id": f"p{i}",
                    "origin_court_or_body": "TRIBUNAL REGIONAL FEDERAL",
                    "origin_description": "SAO PAULO",
                }
            )
            for i in range(5)
        ]
        process_path.write_text("\n".join(lines), encoding="utf-8")
        indices = _discover_indices(process_path)
        assert indices.count("api_publica_trf3") == 1


class TestFetchOriginData:
    def test_dry_run(self, tmp_path: Path) -> None:
        process_path = tmp_path / "process.jsonl"
        process_path.write_text(
            json.dumps(
                {
                    "process_id": "p1",
                    "origin_court_or_body": "TRIBUNAL REGIONAL FEDERAL",
                    "origin_description": "SAO PAULO",
                }
            ),
            encoding="utf-8",
        )
        output_dir = tmp_path / "output"
        config = DatajudFetchConfig(
            api_key="test",
            process_path=process_path,
            output_dir=output_dir,
            dry_run=True,
        )
        result = fetch_origin_data(config)
        assert result == output_dir
        assert not list(output_dir.glob("api_publica_*.json"))

    @patch("atlas_stf.datajud._runner.DatajudClient")
    def test_full_flow(self, mock_client_cls: MagicMock, tmp_path: Path) -> None:
        process_path = tmp_path / "process.jsonl"
        process_path.write_text(
            json.dumps(
                {
                    "process_id": "p1",
                    "origin_court_or_body": "STJ",
                    "origin_description": "",
                }
            ),
            encoding="utf-8",
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

        config = DatajudFetchConfig(
            api_key="test",
            process_path=process_path,
            output_dir=output_dir,
        )
        fetch_origin_data(config)

        result_file = output_dir / "api_publica_stj.json"
        assert result_file.exists()
        data = json.loads(result_file.read_text(encoding="utf-8"))
        assert data["index"] == "api_publica_stj"
        assert data["total_processes"] == 1000

    @patch("atlas_stf.datajud._runner.DatajudClient")
    def test_checkpoint_skip(self, mock_client_cls: MagicMock, tmp_path: Path) -> None:
        process_path = tmp_path / "process.jsonl"
        process_path.write_text(
            json.dumps({"process_id": "p1", "origin_court_or_body": "STJ", "origin_description": ""}),
            encoding="utf-8",
        )
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        checkpoint = output_dir / "_checkpoint.json"
        checkpoint.write_text(json.dumps({"completed": ["api_publica_stj"]}), encoding="utf-8")

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        config = DatajudFetchConfig(api_key="test", process_path=process_path, output_dir=output_dir)
        fetch_origin_data(config)

        mock_client.search.assert_not_called()
