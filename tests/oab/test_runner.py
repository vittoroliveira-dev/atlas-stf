"""Tests for oab/_runner.py."""

from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.oab._config import OabValidationConfig
from atlas_stf.oab._runner import _extract_oab_entries, _read_jsonl, _write_jsonl, run_oab_validation


def _write_lawyer_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")


class TestReadJsonl:
    def test_reads_records(self, tmp_path: Path) -> None:
        path = tmp_path / "test.jsonl"
        path.write_text('{"a": 1}\n{"b": 2}\n', encoding="utf-8")
        records = _read_jsonl(path)
        assert len(records) == 2
        assert records[0] == {"a": 1}
        assert records[1] == {"b": 2}

    def test_returns_empty_for_missing_file(self, tmp_path: Path) -> None:
        path = tmp_path / "missing.jsonl"
        records = _read_jsonl(path)
        assert records == []

    def test_skips_blank_lines(self, tmp_path: Path) -> None:
        path = tmp_path / "test.jsonl"
        path.write_text('{"a": 1}\n\n{"b": 2}\n\n', encoding="utf-8")
        records = _read_jsonl(path)
        assert len(records) == 2


class TestWriteJsonl:
    def test_writes_records(self, tmp_path: Path) -> None:
        path = tmp_path / "out.jsonl"
        _write_jsonl([{"x": 1}, {"y": 2}], path)
        lines = path.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 2
        assert json.loads(lines[0]) == {"x": 1}

    def test_creates_parent_dirs(self, tmp_path: Path) -> None:
        path = tmp_path / "sub" / "dir" / "out.jsonl"
        _write_jsonl([{"z": 3}], path)
        assert path.exists()


class TestExtractOabEntries:
    def test_extracts_pending_entries(self) -> None:
        records = [
            {"oab_number": "12345", "oab_state": "SP", "oab_status": None},
            {"oab_number": "67890", "oab_state": "RJ", "oab_status": "ativo"},
            {"oab_number": "111", "oab_state": "MG", "oab_status": None},
        ]
        entries = _extract_oab_entries(records)
        assert len(entries) == 2
        assert entries[0] == (0, "12345", "SP")
        assert entries[1] == (2, "111", "MG")

    def test_skips_missing_oab_number(self) -> None:
        records = [
            {"oab_state": "SP", "oab_status": None},
            {"oab_number": None, "oab_state": "RJ", "oab_status": None},
            {"oab_number": "", "oab_state": "MG", "oab_status": None},
        ]
        entries = _extract_oab_entries(records)
        assert entries == []

    def test_skips_missing_oab_state(self) -> None:
        records = [{"oab_number": "12345", "oab_status": None}]
        entries = _extract_oab_entries(records)
        assert entries == []

    def test_empty_records(self) -> None:
        assert _extract_oab_entries([]) == []


class TestRunOabValidation:
    def test_empty_file(self, tmp_path: Path) -> None:
        curated = tmp_path / "curated"
        curated.mkdir()
        _write_lawyer_jsonl(curated / "lawyer_entity.jsonl", [])
        config = OabValidationConfig(curated_dir=curated, output_dir=curated)
        count = run_oab_validation(config)
        assert count == 0

    def test_no_file(self, tmp_path: Path) -> None:
        curated = tmp_path / "curated"
        curated.mkdir()
        config = OabValidationConfig(curated_dir=curated, output_dir=curated)
        count = run_oab_validation(config)
        assert count == 0

    def test_no_pending_oab(self, tmp_path: Path) -> None:
        curated = tmp_path / "curated"
        curated.mkdir()
        _write_lawyer_jsonl(
            curated / "lawyer_entity.jsonl",
            [{"name": "JOAO", "oab_number": "12345", "oab_state": "SP", "oab_status": "ativo"}],
        )
        config = OabValidationConfig(curated_dir=curated, output_dir=curated)
        count = run_oab_validation(config)
        assert count == 0

    def test_null_provider(self, tmp_path: Path) -> None:
        curated = tmp_path / "curated"
        output = tmp_path / "output"
        curated.mkdir()
        _write_lawyer_jsonl(
            curated / "lawyer_entity.jsonl",
            [
                {"name": "JOAO", "oab_number": "12345", "oab_state": "SP", "oab_status": None},
                {"name": "MARIA", "oab_number": "67890", "oab_state": "RJ", "oab_status": None},
            ],
        )
        config = OabValidationConfig(curated_dir=curated, output_dir=output, provider="null")
        count = run_oab_validation(config)
        assert count == 2

        result_path = output / "lawyer_entity.jsonl"
        assert result_path.exists()
        results = [json.loads(line) for line in result_path.read_text().strip().split("\n")]
        assert len(results) == 2
        assert results[0]["oab_source"] == "null"
        assert results[0]["oab_validation_method"] == "none"
        assert results[1]["oab_source"] == "null"

    def test_format_provider(self, tmp_path: Path) -> None:
        curated = tmp_path / "curated"
        output = tmp_path / "output"
        curated.mkdir()
        _write_lawyer_jsonl(
            curated / "lawyer_entity.jsonl",
            [
                {"name": "JOAO", "oab_number": "12345", "oab_state": "SP", "oab_status": None},
                {"name": "INVALID", "oab_number": "1234567", "oab_state": "XX", "oab_status": None},
            ],
        )
        config = OabValidationConfig(curated_dir=curated, output_dir=output, provider="format")
        count = run_oab_validation(config)
        assert count == 2

        result_path = output / "lawyer_entity.jsonl"
        results = [json.loads(line) for line in result_path.read_text().strip().split("\n")]
        assert results[0]["oab_status"] == "format_valid"
        assert results[0]["oab_source"] == "format_only"
        assert results[1]["oab_status"] is None  # invalid format

    def test_writes_updated_file(self, tmp_path: Path) -> None:
        curated = tmp_path / "curated"
        output = tmp_path / "output"
        curated.mkdir()
        _write_lawyer_jsonl(
            curated / "lawyer_entity.jsonl",
            [
                {"name": "A", "oab_number": "1", "oab_state": "SP", "oab_status": None, "extra": "keep"},
                {"name": "B", "oab_number": "2", "oab_state": "RJ", "oab_status": "ativo"},
            ],
        )
        config = OabValidationConfig(curated_dir=curated, output_dir=output, provider="null")
        run_oab_validation(config)

        result_path = output / "lawyer_entity.jsonl"
        results = [json.loads(line) for line in result_path.read_text().strip().split("\n")]
        # First record updated with validation fields
        assert results[0]["oab_source"] == "null"
        assert results[0]["extra"] == "keep"  # original field preserved
        # Second record unchanged (already validated)
        assert results[1]["oab_status"] == "ativo"
        assert "oab_source" not in results[1]

    def test_batch_processing(self, tmp_path: Path) -> None:
        curated = tmp_path / "curated"
        output = tmp_path / "output"
        curated.mkdir()
        records = [
            {"name": f"LAWYER_{i}", "oab_number": str(i), "oab_state": "SP", "oab_status": None} for i in range(75)
        ]
        _write_lawyer_jsonl(curated / "lawyer_entity.jsonl", records)
        config = OabValidationConfig(
            curated_dir=curated,
            output_dir=output,
            provider="null",
            batch_size=30,
        )
        count = run_oab_validation(config)
        assert count == 75

    def test_lawyers_without_oab(self, tmp_path: Path) -> None:
        curated = tmp_path / "curated"
        output = tmp_path / "output"
        curated.mkdir()
        _write_lawyer_jsonl(
            curated / "lawyer_entity.jsonl",
            [
                {"name": "NO OAB", "oab_number": None, "oab_state": None, "oab_status": None},
                {"name": "PARTIAL", "oab_number": "123", "oab_state": None, "oab_status": None},
            ],
        )
        config = OabValidationConfig(curated_dir=curated, output_dir=output, provider="null")
        count = run_oab_validation(config)
        assert count == 0
