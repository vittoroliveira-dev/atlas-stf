"""Tests for analytics/origin_context.py."""

from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics.origin_context import build_origin_context


class TestBuildOriginContext:
    def _setup(self, tmp_path: Path) -> tuple[Path, Path, Path]:
        datajud_dir = tmp_path / "datajud"
        datajud_dir.mkdir()
        process_path = tmp_path / "process.jsonl"
        output_dir = tmp_path / "output"

        datajud_dir.joinpath("api_publica_tjsp.json").write_text(
            json.dumps(
                {
                    "index": "api_publica_tjsp",
                    "tribunal_label": "TJSP",
                    "total_processes": 2000000,
                    "top_assuntos": [{"nome": "Direito Civil", "count": 500000}],
                    "top_orgaos_julgadores": [{"nome": "1a Camara", "count": 10000}],
                    "class_distribution": [{"nome": "Apelacao", "count": 100000}],
                }
            ),
            encoding="utf-8",
        )

        process_path.write_text(
            "\n".join(
                [
                    json.dumps(
                        {
                            "process_id": f"p{i}",
                            "origin_court_or_body": "TRIBUNAL DE JUSTICA ESTADUAL",
                            "origin_description": "SAO PAULO",
                        }
                    )
                    for i in range(3)
                ]
            ),
            encoding="utf-8",
        )

        return datajud_dir, process_path, output_dir

    def test_produces_jsonl(self, tmp_path: Path) -> None:
        datajud_dir, process_path, output_dir = self._setup(tmp_path)
        result = build_origin_context(
            datajud_dir=datajud_dir,
            process_path=process_path,
            output_dir=output_dir,
        )
        assert result.exists()
        lines = result.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 1
        record = json.loads(lines[0])
        assert record["origin_index"] == "api_publica_tjsp"
        assert record["datajud_total_processes"] == 2000000
        assert record["stf_process_count"] == 3
        assert record["stf_share_pct"] > 0

    def test_produces_summary(self, tmp_path: Path) -> None:
        datajud_dir, process_path, output_dir = self._setup(tmp_path)
        build_origin_context(
            datajud_dir=datajud_dir,
            process_path=process_path,
            output_dir=output_dir,
        )
        summary_path = output_dir / "origin_context_summary.json"
        assert summary_path.exists()
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        assert summary["origin_count"] == 1

    def test_empty_datajud_dir(self, tmp_path: Path) -> None:
        datajud_dir = tmp_path / "empty_datajud"
        datajud_dir.mkdir()
        process_path = tmp_path / "process.jsonl"
        process_path.write_text("{}\n", encoding="utf-8")
        output_dir = tmp_path / "output"

        result = build_origin_context(
            datajud_dir=datajud_dir,
            process_path=process_path,
            output_dir=output_dir,
        )
        assert result == output_dir / "origin_context.jsonl"
        assert result.exists()
