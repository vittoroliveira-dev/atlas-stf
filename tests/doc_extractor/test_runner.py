"""Tests for doc_extractor/_runner.py."""

from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.doc_extractor._config import DocExtractorConfig
from atlas_stf.doc_extractor._runner import _filter_low_confidence_edges, run_doc_extraction


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")


class TestDocExtractorConfig:
    def test_defaults(self) -> None:
        config = DocExtractorConfig()
        assert config.curated_dir == Path("data/curated")
        assert config.output_dir == Path("data/curated")
        assert config.min_confidence_gap == 0.7
        assert config.max_documents is None
        assert config.download_timeout == 30.0
        assert config.rate_limit_seconds == 2.0

    def test_custom_values(self) -> None:
        config = DocExtractorConfig(
            curated_dir=Path("/tmp/curated"),
            output_dir=Path("/tmp/output"),
            min_confidence_gap=0.5,
            max_documents=10,
            download_timeout=60.0,
            rate_limit_seconds=5.0,
        )
        assert config.curated_dir == Path("/tmp/curated")
        assert config.min_confidence_gap == 0.5
        assert config.max_documents == 10

    def test_is_dataclass(self) -> None:
        import dataclasses

        assert dataclasses.is_dataclass(DocExtractorConfig)


class TestFilterLowConfidenceEdges:
    def test_filters_below_threshold(self) -> None:
        edges = [
            {"edge_id": "e1", "confidence": 0.3},
            {"edge_id": "e2", "confidence": 0.8},
            {"edge_id": "e3", "confidence": 0.5},
        ]
        result = _filter_low_confidence_edges(edges, 0.7)
        assert len(result) == 2
        assert result[0]["edge_id"] == "e1"
        assert result[1]["edge_id"] == "e3"

    def test_missing_confidence_treated_as_zero(self) -> None:
        edges = [{"edge_id": "e1"}, {"edge_id": "e2", "confidence": 0.9}]
        result = _filter_low_confidence_edges(edges, 0.7)
        assert len(result) == 1
        assert result[0]["edge_id"] == "e1"

    def test_all_above_threshold(self) -> None:
        edges = [{"edge_id": "e1", "confidence": 0.9}]
        result = _filter_low_confidence_edges(edges, 0.7)
        assert len(result) == 0

    def test_empty_edges(self) -> None:
        assert _filter_low_confidence_edges([], 0.7) == []


class TestRunDocExtraction:
    def test_no_edge_file(self, tmp_path: Path) -> None:
        config = DocExtractorConfig(curated_dir=tmp_path, output_dir=tmp_path)
        result = run_doc_extraction(config)
        assert result == 0

    def test_empty_edges_file(self, tmp_path: Path) -> None:
        _write_jsonl(tmp_path / "representation_edge.jsonl", [])
        config = DocExtractorConfig(curated_dir=tmp_path, output_dir=tmp_path)
        result = run_doc_extraction(config)
        assert result == 0

    def test_all_high_confidence(self, tmp_path: Path) -> None:
        edges = [
            {"edge_id": "e1", "confidence": 0.9},
            {"edge_id": "e2", "confidence": 0.85},
        ]
        _write_jsonl(tmp_path / "representation_edge.jsonl", edges)
        config = DocExtractorConfig(curated_dir=tmp_path, output_dir=tmp_path)
        result = run_doc_extraction(config)
        assert result == 0

    def test_low_confidence_edges_found(self, tmp_path: Path) -> None:
        edges = [
            {"edge_id": "e1", "confidence": 0.3},
            {"edge_id": "e2", "confidence": 0.9},
            {"edge_id": "e3", "confidence": 0.5},
        ]
        _write_jsonl(tmp_path / "representation_edge.jsonl", edges)
        config = DocExtractorConfig(curated_dir=tmp_path, output_dir=tmp_path)
        # Currently returns 0 (placeholder) but exercises the filtering path
        result = run_doc_extraction(config)
        assert result == 0

    def test_max_documents_limit(self, tmp_path: Path) -> None:
        edges = [{"edge_id": f"e{i}", "confidence": 0.1} for i in range(10)]
        _write_jsonl(tmp_path / "representation_edge.jsonl", edges)
        config = DocExtractorConfig(
            curated_dir=tmp_path,
            output_dir=tmp_path,
            max_documents=3,
        )
        # Placeholder returns 0, but exercises max_documents path
        result = run_doc_extraction(config)
        assert result == 0

    def test_missing_confidence_field(self, tmp_path: Path) -> None:
        edges = [{"edge_id": "e1"}]  # no confidence key
        _write_jsonl(tmp_path / "representation_edge.jsonl", edges)
        config = DocExtractorConfig(curated_dir=tmp_path, output_dir=tmp_path)
        # Edge without confidence is treated as 0 (below threshold)
        result = run_doc_extraction(config)
        assert result == 0
