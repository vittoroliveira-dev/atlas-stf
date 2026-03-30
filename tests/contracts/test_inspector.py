"""Unit tests for atlas_stf.contracts._inspector.

Covers all public helpers and the two main inspection entry-points:
inspect_csv() and inspect_jsonl().  All I/O uses tmp_path; no real data
files are required.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from atlas_stf.contracts._inspector import (
    _Col,
    _count_lines_fast,
    _detect_encoding,
    _detect_line_ending,
    _file_fingerprint,
    _infer_type,
    _normalize_col,
    inspect_csv,
    inspect_jsonl,
    inspect_jsonl_partitioned,
)

# ---------------------------------------------------------------------------
# _file_fingerprint
# ---------------------------------------------------------------------------


class TestFileFingerprint:
    def test_returns_hex_string(self, tmp_path: Path) -> None:
        p = tmp_path / "f.bin"
        p.write_bytes(b"hello world")
        result = _file_fingerprint(p)
        assert isinstance(result, str)
        assert len(result) == 64  # SHA-256 hex digest

    def test_matches_manual_sha256(self, tmp_path: Path) -> None:
        content = b"atlas stf test data"
        p = tmp_path / "data.bin"
        p.write_bytes(content)
        h = hashlib.sha256()
        h.update(str(len(content)).encode())
        h.update(content)
        assert _file_fingerprint(p) == h.hexdigest()

    def test_only_reads_first_1mb(self, tmp_path: Path) -> None:
        # Write exactly 2 MB; fingerprint uses size + first 1 MB.
        chunk = b"x" * 1_048_576
        full = chunk + b"y" * 1_048_576
        p = tmp_path / "big.bin"
        p.write_bytes(full)
        h = hashlib.sha256()
        h.update(str(len(full)).encode())
        h.update(chunk)
        assert _file_fingerprint(p) == h.hexdigest()

    def test_empty_file(self, tmp_path: Path) -> None:
        p = tmp_path / "empty.bin"
        p.write_bytes(b"")
        h = hashlib.sha256()
        h.update(b"0")
        h.update(b"")
        assert _file_fingerprint(p) == h.hexdigest()


# ---------------------------------------------------------------------------
# _detect_encoding
# ---------------------------------------------------------------------------


class TestDetectEncoding:
    def test_utf8_file(self, tmp_path: Path) -> None:
        p = tmp_path / "utf8.txt"
        p.write_bytes("São Paulo — açaí".encode("utf-8"))
        assert _detect_encoding(p) == "utf-8"

    def test_latin1_file(self, tmp_path: Path) -> None:
        p = tmp_path / "latin1.txt"
        # \xe3 = ã in latin-1; invalid in UTF-8 standalone
        p.write_bytes(b"S\xe3o Paulo")
        assert _detect_encoding(p) == "latin-1"

    def test_ascii_is_utf8(self, tmp_path: Path) -> None:
        p = tmp_path / "ascii.txt"
        p.write_bytes(b"plain ascii text 123")
        assert _detect_encoding(p) == "utf-8"


# ---------------------------------------------------------------------------
# _detect_line_ending
# ---------------------------------------------------------------------------


class TestDetectLineEnding:
    def test_lf(self, tmp_path: Path) -> None:
        p = tmp_path / "lf.txt"
        p.write_bytes(b"line1\nline2\nline3\n")
        assert _detect_line_ending(p) == "lf"

    def test_crlf(self, tmp_path: Path) -> None:
        p = tmp_path / "crlf.txt"
        p.write_bytes(b"line1\r\nline2\r\nline3\r\n")
        assert _detect_line_ending(p) == "crlf"

    def test_cr(self, tmp_path: Path) -> None:
        p = tmp_path / "cr.txt"
        # Pure CR only (old Mac-style); must NOT contain \r\n
        p.write_bytes(b"line1\rline2\rline3\r")
        assert _detect_line_ending(p) == "cr"

    def test_empty_file_defaults_lf(self, tmp_path: Path) -> None:
        p = tmp_path / "empty.txt"
        p.write_bytes(b"")
        assert _detect_line_ending(p) == "lf"


# ---------------------------------------------------------------------------
# _normalize_col
# ---------------------------------------------------------------------------


class TestNormalizeCol:
    def test_ascii_lowercase(self) -> None:
        assert _normalize_col("Name") == "name"

    def test_strips_accents(self) -> None:
        assert _normalize_col("Ação") == "acao"

    def test_spaces_become_underscores(self) -> None:
        assert _normalize_col("First Name") == "first_name"

    def test_punctuation_becomes_underscore(self) -> None:
        assert _normalize_col("col-name/value") == "col_name_value"

    def test_leading_trailing_underscores_stripped(self) -> None:
        assert _normalize_col("_col_") == "col"
        assert _normalize_col("  col  ") == "col"

    def test_cedilla_and_tilde(self) -> None:
        assert _normalize_col("Situação Cadastral") == "situacao_cadastral"

    def test_all_uppercase(self) -> None:
        assert _normalize_col("CNPJ_BASICO") == "cnpj_basico"

    def test_digits_preserved(self) -> None:
        assert _normalize_col("col2024") == "col2024"


# ---------------------------------------------------------------------------
# _infer_type
# ---------------------------------------------------------------------------


class TestInferType:
    def test_empty_list(self) -> None:
        assert _infer_type([]) == "empty"

    def test_all_whitespace_values(self) -> None:
        assert _infer_type(["   ", "\t", ""]) == "empty"

    def test_integer(self) -> None:
        # "0" and "1" are in _BOOLEANS, so use values that are unambiguously integers
        assert _infer_type(["2", "-3", "100", "42"]) == "integer"

    def test_float(self) -> None:
        assert _infer_type(["1.5", "2,3", "-0.7"]) == "float"

    def test_date_iso(self) -> None:
        assert _infer_type(["2024-01-01", "2023-12-31", "2000-06-15"]) == "date"

    def test_date_br_format(self) -> None:
        assert _infer_type(["01/01/2024", "31/12/2023"]) == "date"

    def test_boolean_true_false(self) -> None:
        assert _infer_type(["true", "false", "True", "False"]) == "boolean"

    def test_boolean_sim_nao(self) -> None:
        assert _infer_type(["sim", "não", "nao"]) == "boolean"

    def test_boolean_zero_one(self) -> None:
        assert _infer_type(["0", "1", "0", "1"]) == "boolean"

    def test_string(self) -> None:
        assert _infer_type(["hello", "world", "foo bar"]) == "string"

    def test_mixed_below_threshold(self) -> None:
        # 5 strings + 1 integer → strings = 83% → "string" (above 80 %)
        values = ["hello", "world", "foo", "bar", "baz", "1"]
        result = _infer_type(values)
        assert result == "string"

    def test_mixed_exactly_split(self) -> None:
        # 5 integers + 5 strings → neither dominates at 80%
        values = ["1", "2", "3", "4", "5", "a", "b", "c", "d", "e"]
        assert _infer_type(values) == "mixed"

    def test_ignores_blank_entries_in_probe(self) -> None:
        # Blank values are not counted; "0"/"1" are _BOOLEANS, so use unambiguous integers
        assert _infer_type(["2", "3", "42", ""]) == "integer"


# ---------------------------------------------------------------------------
# _Col.observe / _Col.profile
# ---------------------------------------------------------------------------


class TestCol:
    def test_null_tracking(self) -> None:
        col = _Col()
        col.observe(None)
        col.observe(None)
        col.observe("hello")
        assert col.null_count == 2
        assert col.count == 3

    def test_empty_string_tracking(self) -> None:
        col = _Col()
        col.observe("  ")
        col.observe("")
        col.observe("value")
        assert col.empty_count == 2

    def test_min_max_length(self) -> None:
        col = _Col()
        col.observe("hi")
        col.observe("hello world")
        col.observe("x")
        assert col.min_len == 1
        assert col.max_len == 11

    def test_distinct_counting(self) -> None:
        col = _Col()
        for v in ["a", "b", "a", "c", "b"]:
            col.observe(v)
        assert len(col.distinct) == 3

    def test_distinct_saturation(self) -> None:
        col = _Col()
        for i in range(10_001):
            col.observe(str(i))
        assert col.distinct_saturated is True

    def test_samples_collected_up_to_limit(self) -> None:
        col = _Col()
        for i in range(10):
            col.observe(f"val{i}")
        assert len(col.samples) == 5  # _SAMPLE_VALUES_COUNT

    def test_samples_no_duplicates(self) -> None:
        col = _Col()
        for _ in range(20):
            col.observe("same")
        assert col.samples == ["same"]

    def test_samples_skips_blank(self) -> None:
        col = _Col()
        col.observe("  ")
        col.observe("real")
        assert col.samples == ["real"]

    def test_type_probe_capped_at_200(self) -> None:
        col = _Col()
        for i in range(300):
            col.observe(str(i))
        assert len(col.type_probe) == 200

    def test_profile_null_rate(self) -> None:
        col = _Col()
        col.observe(None)
        col.observe("x")
        col.observe("y")
        p = col.profile("campo", 0, None)
        assert p["null_rate"] == pytest.approx(1 / 3, abs=1e-4)

    def test_profile_empty_rate(self) -> None:
        col = _Col()
        col.observe("")
        col.observe("val")
        p = col.profile("campo", 0, None)
        assert p["empty_rate"] == pytest.approx(0.5, abs=1e-4)

    def test_profile_keys_present(self) -> None:
        col = _Col()
        col.observe("test")
        p = col.profile("MeuCampo", 3, "nota de teste")
        required_keys = {
            "position",
            "observed_column_name",
            "normalized_column_name",
            "observed_type",
            "sample_values",
            "null_rate",
            "empty_rate",
            "distinct_count_estimate",
            "distinct_saturated",
            "min_length",
            "max_length",
            "notes",
            "suspected_semantic_drift",
            "suspected_alias_group",
            "extraction_confidence",
        }
        assert required_keys.issubset(p.keys())

    def test_profile_normalizes_col_name(self) -> None:
        col = _Col()
        col.observe("x")
        p = col.profile("Situação Cadastral", 0, None)
        assert p["normalized_column_name"] == "situacao_cadastral"

    def test_profile_empty_col_returns_zeros(self) -> None:
        col = _Col()
        p = col.profile("vazio", 0, None)
        assert p["null_rate"] == 0.0
        assert p["empty_rate"] == 0.0

    def test_profile_notes_passed_through(self) -> None:
        col = _Col()
        col.observe("x")
        p = col.profile("col", 0, "informação extra")
        assert p["notes"] == "informação extra"


# ---------------------------------------------------------------------------
# inspect_csv
# ---------------------------------------------------------------------------


def _write_csv(path: Path, rows: list[list[str]], delimiter: str = ";") -> None:
    lines = [delimiter.join(row) for row in rows]
    path.write_text("\n".join(lines), encoding="utf-8")


class TestInspectCsv:
    def test_basic_structure(self, tmp_path: Path) -> None:
        p = tmp_path / "data.csv"
        _write_csv(p, [["id", "name", "value"], ["1", "Alice", "10.5"], ["2", "Bob", "20.0"]])
        result = inspect_csv(p, source="test", year_or_cycle="2024", project_root=tmp_path)

        assert result["format"] == "csv"
        assert result["source"] == "test"
        assert result["year_or_cycle"] == "2024"
        assert result["total_records"] == 2
        assert result["sample_coverage"] == 1.0
        assert result["encoding_detected"] == "utf-8"
        assert len(result["columns"]) == 3

    def test_column_names_in_result(self, tmp_path: Path) -> None:
        p = tmp_path / "data.csv"
        _write_csv(p, [["cnpj", "razao_social"], ["12345", "Empresa Ltda"]])
        result = inspect_csv(p, source="rfb", year_or_cycle="2023", project_root=tmp_path)
        col_names = [c["observed_column_name"] for c in result["columns"]]
        assert col_names == ["cnpj", "razao_social"]

    def test_file_path_relative(self, tmp_path: Path) -> None:
        sub = tmp_path / "sub"
        sub.mkdir()
        p = sub / "data.csv"
        _write_csv(p, [["a"], ["1"]])
        result = inspect_csv(p, source="s", year_or_cycle="2024", project_root=tmp_path)
        assert result["file_path_relative"] == "sub/data.csv"

    def test_custom_delimiter(self, tmp_path: Path) -> None:
        p = tmp_path / "pipe.csv"
        p.write_text("a|b\n1|2\n3|4\n", encoding="utf-8")
        result = inspect_csv(p, source="s", year_or_cycle="2024", delimiter="|", project_root=tmp_path)
        assert result["delimiter"] == "|"
        assert len(result["columns"]) == 2

    def test_notes_applied_to_columns(self, tmp_path: Path) -> None:
        p = tmp_path / "data.csv"
        _write_csv(p, [["cpf", "nome"], ["111", "Ana"]])
        notes = {"cpf": "identificador fiscal"}
        result = inspect_csv(p, source="s", year_or_cycle="2024", project_root=tmp_path, notes=notes)
        cpf_col = next(c for c in result["columns"] if c["observed_column_name"] == "cpf")
        assert cpf_col["notes"] == "identificador fiscal"

    def test_meta_block_present(self, tmp_path: Path) -> None:
        p = tmp_path / "data.csv"
        _write_csv(p, [["x"], ["1"]])
        result = inspect_csv(p, source="s", year_or_cycle="2024", project_root=tmp_path)
        assert "_meta" in result
        assert "generated_at" in result["_meta"]
        assert result["_meta"]["generator_version"] == "1.0.0"

    def test_file_size_bytes(self, tmp_path: Path) -> None:
        p = tmp_path / "data.csv"
        _write_csv(p, [["col"], ["value"]])
        result = inspect_csv(p, source="s", year_or_cycle="2024", project_root=tmp_path)
        assert result["file_size_bytes"] == p.stat().st_size

    def test_fingerprint_present(self, tmp_path: Path) -> None:
        p = tmp_path / "data.csv"
        _write_csv(p, [["col"], ["v"]])
        result = inspect_csv(p, source="s", year_or_cycle="2024", project_root=tmp_path)
        assert len(result["file_fingerprint_sha256_1mb"]) == 64

    def test_null_rate_for_short_rows(self, tmp_path: Path) -> None:
        # Row with fewer columns than header — missing fields become None
        p = tmp_path / "data.csv"
        p.write_text("a;b;c\n1;2\n3;4;5\n", encoding="utf-8")
        result = inspect_csv(p, source="s", year_or_cycle="2024", project_root=tmp_path)
        col_c = next(c for c in result["columns"] if c["observed_column_name"] == "c")
        assert col_c["null_rate"] == pytest.approx(0.5, abs=1e-4)

    def test_integer_type_inferred(self, tmp_path: Path) -> None:
        p = tmp_path / "data.csv"
        _write_csv(p, [["id"], ["1"], ["2"], ["3"], ["4"], ["5"]])
        result = inspect_csv(p, source="s", year_or_cycle="2024", project_root=tmp_path)
        assert result["columns"][0]["observed_type"] == "integer"

    def test_header_present_true(self, tmp_path: Path) -> None:
        p = tmp_path / "data.csv"
        _write_csv(p, [["col"], ["val"]])
        result = inspect_csv(p, source="s", year_or_cycle="2024", project_root=tmp_path)
        assert result["header_present"] is True


# ---------------------------------------------------------------------------
# inspect_jsonl  (mocking _count_lines_fast to avoid wc -l dependency)
# ---------------------------------------------------------------------------


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")


class TestInspectJsonl:
    def _inspect(self, path: Path, project_root: Path, **kwargs) -> dict:
        """Wrapper that mocks _count_lines_fast with actual line count."""
        line_count = sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
        with patch("atlas_stf.contracts._inspector._count_lines_fast", return_value=line_count):
            return inspect_jsonl(path, project_root=project_root, **kwargs)

    def test_basic_structure(self, tmp_path: Path) -> None:
        p = tmp_path / "data.jsonl"
        records = [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]
        _write_jsonl(p, records)
        result = self._inspect(p, tmp_path, source="test", year_or_cycle="2024")

        assert result["format"] == "jsonl"
        assert result["source"] == "test"
        assert result["total_records"] == 2
        assert result["sample_size"] == 2
        assert result["sample_coverage"] == 1.0
        assert result["header_present"] is False
        assert result["delimiter"] is None

    def test_columns_sorted_alphabetically(self, tmp_path: Path) -> None:
        p = tmp_path / "data.jsonl"
        _write_jsonl(p, [{"z": "1", "a": "2", "m": "3"}])
        result = self._inspect(p, tmp_path, source="s", year_or_cycle="2024")
        col_names = [c["observed_column_name"] for c in result["columns"]]
        assert col_names == sorted(col_names)

    def test_null_for_missing_keys(self, tmp_path: Path) -> None:
        p = tmp_path / "data.jsonl"
        records = [{"a": "x", "b": "y"}, {"a": "z"}]
        _write_jsonl(p, records)
        result = self._inspect(p, tmp_path, source="s", year_or_cycle="2024")
        col_b = next(c for c in result["columns"] if c["observed_column_name"] == "b")
        assert col_b["null_rate"] == pytest.approx(0.5, abs=1e-4)

    def test_partition_key_counts(self, tmp_path: Path) -> None:
        p = tmp_path / "data.jsonl"
        records = [{"year": "2022", "val": "a"}, {"year": "2022", "val": "b"}, {"year": "2023", "val": "c"}]
        _write_jsonl(p, records)
        result = self._inspect(p, tmp_path, source="s", year_or_cycle="all", partition_key="year")
        assert "partition_key" in result
        assert result["partition_key"] == "year"
        assert result["partition_values_sampled"]["2022"] == 2
        assert result["partition_values_sampled"]["2023"] == 1

    def test_no_partition_key_section_when_absent(self, tmp_path: Path) -> None:
        p = tmp_path / "data.jsonl"
        _write_jsonl(p, [{"a": "1"}])
        result = self._inspect(p, tmp_path, source="s", year_or_cycle="2024")
        assert "partition_key" not in result
        assert "partition_values_sampled" not in result

    def test_meta_block_present(self, tmp_path: Path) -> None:
        p = tmp_path / "data.jsonl"
        _write_jsonl(p, [{"x": "1"}])
        result = self._inspect(p, tmp_path, source="s", year_or_cycle="2024")
        assert "_meta" in result
        assert result["_meta"]["generator_version"] == "1.0.0"

    def test_file_path_relative(self, tmp_path: Path) -> None:
        sub = tmp_path / "raw" / "tse"
        sub.mkdir(parents=True)
        p = sub / "donations.jsonl"
        _write_jsonl(p, [{"amount": "100"}])
        result = self._inspect(p, tmp_path, source="tse", year_or_cycle="2024")
        assert result["file_path_relative"] == "raw/tse/donations.jsonl"

    def test_encoding_detected(self, tmp_path: Path) -> None:
        p = tmp_path / "data.jsonl"
        _write_jsonl(p, [{"nome": "João"}])
        result = self._inspect(p, tmp_path, source="s", year_or_cycle="2024")
        assert result["encoding_detected"] == "utf-8"

    def test_skip_blank_lines(self, tmp_path: Path) -> None:
        p = tmp_path / "data.jsonl"
        p.write_text('{"a": "1"}\n\n{"a": "2"}\n', encoding="utf-8")
        # line_count counts non-blank lines only (2), but _count_lines_fast
        # counts all lines; we patch it with the actual non-blank count.
        with patch("atlas_stf.contracts._inspector._count_lines_fast", return_value=2):
            result = inspect_jsonl(p, project_root=tmp_path, source="s", year_or_cycle="2024")
        assert result["sample_size"] == 2

    def test_invalid_json_lines_skipped(self, tmp_path: Path) -> None:
        p = tmp_path / "data.jsonl"
        p.write_text('{"a": "ok"}\nnot_json\n{"a": "also_ok"}\n', encoding="utf-8")
        with patch("atlas_stf.contracts._inspector._count_lines_fast", return_value=3):
            result = inspect_jsonl(p, project_root=tmp_path, source="s", year_or_cycle="2024")
        assert result["sample_size"] == 2

    def test_medium_confidence_when_sampled(self, tmp_path: Path) -> None:
        # Simulate subsampling by reporting a total larger than actual lines
        records = [{"x": str(i)} for i in range(10)]
        p = tmp_path / "data.jsonl"
        _write_jsonl(p, records)
        # With total=100 and sample_size=20_000, step=max(1,100//20000)=1,
        # so all lines are read.  To force coverage < 1.0 we need total >> sampled.
        # Patch total to be large enough that step > 1.
        with patch("atlas_stf.contracts._inspector._count_lines_fast", return_value=100_000):
            result = inspect_jsonl(p, project_root=tmp_path, source="s", year_or_cycle="2024", sample_size=20_000)
        for col in result["columns"]:
            assert col["extraction_confidence"] == "medium"

    def test_notes_applied(self, tmp_path: Path) -> None:
        p = tmp_path / "data.jsonl"
        _write_jsonl(p, [{"cpf": "111.222.333-44"}])
        notes = {"cpf": "chave de junção"}
        result = self._inspect(p, tmp_path, source="s", year_or_cycle="2024", notes=notes)
        cpf_col = next(c for c in result["columns"] if c["observed_column_name"] == "cpf")
        assert cpf_col["notes"] == "chave de junção"


# ---------------------------------------------------------------------------
# _count_lines_fast (subprocess wc -l wrapper)
# ---------------------------------------------------------------------------


class TestCountLinesFast:
    def test_counts_lines(self, tmp_path: Path) -> None:
        p = tmp_path / "lines.txt"
        p.write_text("a\nb\nc\n", encoding="utf-8")
        result = _count_lines_fast(p)
        assert result == 3

    def test_single_line_no_trailing_newline(self, tmp_path: Path) -> None:
        p = tmp_path / "one.txt"
        p.write_text("only one line", encoding="utf-8")
        result = _count_lines_fast(p)
        assert result == 0  # wc -l counts newlines, not lines

    def test_empty_file(self, tmp_path: Path) -> None:
        p = tmp_path / "empty.txt"
        p.write_bytes(b"")
        result = _count_lines_fast(p)
        assert result == 0


# ---------------------------------------------------------------------------
# inspect_csv — row wider than header (extra columns branch)
# ---------------------------------------------------------------------------


class TestInspectCsvExtraColumns:
    def test_row_wider_than_header_creates_extra_accumulator(self, tmp_path: Path) -> None:
        # A data row has more columns than the header; the extra column should
        # be profiled as "_col_<i>" and not crash.
        p = tmp_path / "wide.csv"
        # header has 2 cols, one data row has 3
        p.write_text("a;b\n1;2;3\n4;5\n", encoding="utf-8")
        result = inspect_csv(p, source="s", year_or_cycle="2024", project_root=tmp_path)
        col_names = [c["observed_column_name"] for c in result["columns"]]
        assert "_col_2" in col_names

    def test_extra_column_value_observed(self, tmp_path: Path) -> None:
        p = tmp_path / "wide.csv"
        p.write_text("a;b\n10;20;extra_value\n", encoding="utf-8")
        result = inspect_csv(p, source="s", year_or_cycle="2024", project_root=tmp_path)
        extra = next(c for c in result["columns"] if c["observed_column_name"] == "_col_2")
        assert extra["sample_values"] == ["extra_value"]


# ---------------------------------------------------------------------------
# inspect_jsonl_partitioned
# ---------------------------------------------------------------------------


class TestInspectJsonlPartitioned:
    def test_returns_dict_keyed_by_partition_value(self, tmp_path: Path) -> None:
        p = tmp_path / "data.jsonl"
        records = [
            {"year": "2022", "name": "Alice"},
            {"year": "2022", "name": "Bob"},
            {"year": "2023", "name": "Carol"},
        ]
        _write_jsonl(p, records)
        result = inspect_jsonl_partitioned(p, source="s", project_root=tmp_path, partition_key="year")
        assert set(result.keys()) == {"2022", "2023"}

    def test_per_partition_record_count(self, tmp_path: Path) -> None:
        p = tmp_path / "data.jsonl"
        records = [{"yr": "A", "v": str(i)} for i in range(5)] + [{"yr": "B", "v": str(i)} for i in range(3)]
        _write_jsonl(p, records)
        result = inspect_jsonl_partitioned(p, source="s", project_root=tmp_path, partition_key="yr")
        assert result["A"]["total_records"] == 5
        assert result["B"]["total_records"] == 3

    def test_partition_coverage_full_when_within_limit(self, tmp_path: Path) -> None:
        p = tmp_path / "data.jsonl"
        records = [{"yr": "2024", "x": str(i)} for i in range(10)]
        _write_jsonl(p, records)
        result = inspect_jsonl_partitioned(
            p, source="s", project_root=tmp_path, partition_key="yr", max_per_partition=100
        )
        assert result["2024"]["sample_coverage"] == 1.0

    def test_partition_coverage_medium_when_exceeds_limit(self, tmp_path: Path) -> None:
        p = tmp_path / "data.jsonl"
        records = [{"yr": "2024", "x": str(i)} for i in range(20)]
        _write_jsonl(p, records)
        result = inspect_jsonl_partitioned(
            p, source="s", project_root=tmp_path, partition_key="yr", max_per_partition=5
        )
        # 20 records but only 5 sampled → coverage < 1.0 → medium confidence
        assert result["2024"]["sample_coverage"] < 1.0
        for col in result["2024"]["columns"]:
            assert col["extraction_confidence"] == "medium"

    def test_unknown_partition_key_becomes_unknown(self, tmp_path: Path) -> None:
        p = tmp_path / "data.jsonl"
        # Record missing the partition key
        _write_jsonl(p, [{"other": "value"}])
        result = inspect_jsonl_partitioned(p, source="s", project_root=tmp_path, partition_key="missing_key")
        assert "_unknown" in result

    def test_meta_block_per_partition(self, tmp_path: Path) -> None:
        p = tmp_path / "data.jsonl"
        _write_jsonl(p, [{"yr": "2024", "a": "x"}])
        result = inspect_jsonl_partitioned(p, source="s", project_root=tmp_path, partition_key="yr")
        assert "_meta" in result["2024"]
        assert result["2024"]["_meta"]["generator_version"] == "1.0.0"

    def test_null_for_missing_keys_across_records(self, tmp_path: Path) -> None:
        p = tmp_path / "data.jsonl"
        records = [
            {"yr": "2024", "a": "present", "b": "also"},
            {"yr": "2024", "a": "only_a"},
        ]
        _write_jsonl(p, records)
        result = inspect_jsonl_partitioned(p, source="s", project_root=tmp_path, partition_key="yr")
        col_b = next(c for c in result["2024"]["columns"] if c["observed_column_name"] == "b")
        assert col_b["null_rate"] == pytest.approx(0.5, abs=1e-4)

    def test_skip_blank_and_invalid_lines(self, tmp_path: Path) -> None:
        p = tmp_path / "data.jsonl"
        p.write_text('{"yr": "2024", "v": "ok"}\nnot_json\n\n{"yr": "2024", "v": "also"}\n', encoding="utf-8")
        result = inspect_jsonl_partitioned(p, source="s", project_root=tmp_path, partition_key="yr")
        assert result["2024"]["total_records"] == 2
