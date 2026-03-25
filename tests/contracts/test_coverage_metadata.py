"""Tests for contracts/_coverage.py — field availability and coverage metadata."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from atlas_stf.contracts._coverage import (
    _field_availability_from_inventory,
    _is_low_reliability,
    _layout_signature_from_inventory,
    build_coverage_metadata,
    write_coverage_metadata,
)
from atlas_stf.ingest_manifest import normalize_header_for_signature

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_col(name: str, null_rate: float = 0.0, empty_rate: float = 0.0) -> dict[str, Any]:
    return {"observed_column_name": name, "null_rate": null_rate, "empty_rate": empty_rate}


def _make_inventory(
    source: str = "tse",
    year_or_cycle: str = "2022",
    columns: list[dict[str, Any]] | None = None,
    file_name: str = "donations_raw.jsonl",
) -> dict[str, Any]:
    return {
        "source": source,
        "file_name": file_name,
        "year_or_cycle": year_or_cycle,
        "columns": columns if columns is not None else [],
    }


# ---------------------------------------------------------------------------
# _is_low_reliability
# ---------------------------------------------------------------------------


class TestIsLowReliability:
    def test_both_zero_is_reliable(self) -> None:
        col = _make_col("cpf", null_rate=0.0, empty_rate=0.0)
        assert not _is_low_reliability(col)

    def test_exactly_threshold_is_not_low(self) -> None:
        # threshold is strictly greater than 0.50, so exactly 0.50 is NOT low
        col = _make_col("cpf", null_rate=0.25, empty_rate=0.25)
        assert not _is_low_reliability(col)

    def test_just_above_threshold_is_low(self) -> None:
        col = _make_col("cpf", null_rate=0.26, empty_rate=0.25)
        assert _is_low_reliability(col)

    def test_null_rate_alone_exceeds_threshold(self) -> None:
        col = _make_col("cpf", null_rate=0.80, empty_rate=0.0)
        assert _is_low_reliability(col)

    def test_empty_rate_alone_exceeds_threshold(self) -> None:
        col = _make_col("cpf", null_rate=0.0, empty_rate=0.99)
        assert _is_low_reliability(col)

    def test_combined_rates_exceeds_threshold(self) -> None:
        col = _make_col("cpf", null_rate=0.30, empty_rate=0.30)
        assert _is_low_reliability(col)

    def test_missing_rates_treated_as_zero(self) -> None:
        # keys absent — should default to 0.0
        col: dict[str, Any] = {"observed_column_name": "x"}
        assert not _is_low_reliability(col)

    def test_none_rates_treated_as_zero(self) -> None:
        col: dict[str, Any] = {"observed_column_name": "x", "null_rate": None, "empty_rate": None}
        assert not _is_low_reliability(col)


# ---------------------------------------------------------------------------
# _layout_signature_from_inventory
# ---------------------------------------------------------------------------


class TestLayoutSignatureFromInventory:
    def test_empty_inventory_returns_empty_string(self) -> None:
        inv = _make_inventory(columns=[])
        assert _layout_signature_from_inventory(inv) == ""

    def test_no_columns_key_returns_empty_string(self) -> None:
        assert _layout_signature_from_inventory({}) == ""

    def test_single_column_returns_sha256_hex(self) -> None:
        inv = _make_inventory(columns=[_make_col("cpf")])
        sig = _layout_signature_from_inventory(inv)
        assert len(sig) == 64
        assert all(c in "0123456789abcdef" for c in sig)

    def test_matches_normalize_header_for_signature(self) -> None:
        cols = ["cpf", "nome", "valor"]
        inv = _make_inventory(columns=[_make_col(c) for c in cols])
        expected = normalize_header_for_signature(cols)
        assert _layout_signature_from_inventory(inv) == expected

    def test_different_column_sets_produce_different_signatures(self) -> None:
        inv_a = _make_inventory(columns=[_make_col("cpf"), _make_col("nome")])
        inv_b = _make_inventory(columns=[_make_col("cnpj"), _make_col("razao")])
        assert _layout_signature_from_inventory(inv_a) != _layout_signature_from_inventory(inv_b)

    def test_column_order_affects_signature(self) -> None:
        inv_a = _make_inventory(columns=[_make_col("a"), _make_col("b")])
        inv_b = _make_inventory(columns=[_make_col("b"), _make_col("a")])
        assert _layout_signature_from_inventory(inv_a) != _layout_signature_from_inventory(inv_b)


# ---------------------------------------------------------------------------
# _field_availability_from_inventory
# ---------------------------------------------------------------------------


class TestFieldAvailabilityFromInventory:
    def test_no_columns_returns_empty(self) -> None:
        inv = _make_inventory(columns=[])
        assert _field_availability_from_inventory(inv, "donations_raw.jsonl") == []

    def test_reliable_column_not_included(self) -> None:
        col = _make_col("cpf", null_rate=0.10, empty_rate=0.10)
        inv = _make_inventory(columns=[col])
        entries = _field_availability_from_inventory(inv, "donations_raw.jsonl")
        assert entries == []

    def test_low_reliability_column_included(self) -> None:
        col = _make_col("nome", null_rate=0.60, empty_rate=0.0)
        inv = _make_inventory(source="tse", year_or_cycle="2022", columns=[col])
        entries = _field_availability_from_inventory(inv, "donations_raw.jsonl")
        assert len(entries) == 1
        e = entries[0]
        assert e["source"] == "tse"
        assert e["field"] == "nome"
        assert e["year_or_cycle"] == "2022"
        assert e["file_name"] == "donations_raw.jsonl"
        assert e["status"] == "source_present_low_reliability"
        assert e["origin_of_status"] == "observed_from_raw"

    def test_layout_signature_matches_columns(self) -> None:
        cols = [_make_col("cpf"), _make_col("nome", null_rate=0.90)]
        inv = _make_inventory(columns=cols)
        entries = _field_availability_from_inventory(inv, "x.jsonl")
        # only the low-reliability col is returned
        assert len(entries) == 1
        expected_sig = normalize_header_for_signature(["cpf", "nome"])
        assert entries[0]["layout_signature"] == expected_sig

    def test_multiple_low_reliability_columns(self) -> None:
        cols = [
            _make_col("a", null_rate=0.0, empty_rate=0.0),
            _make_col("b", null_rate=0.80, empty_rate=0.0),
            _make_col("c", null_rate=0.0, empty_rate=0.90),
        ]
        inv = _make_inventory(columns=cols)
        entries = _field_availability_from_inventory(inv, "f.jsonl")
        names = [e["field"] for e in entries]
        assert names == ["b", "c"]

    def test_missing_source_field_defaults_to_empty(self) -> None:
        col = _make_col("x", null_rate=0.99)
        inv = {"year_or_cycle": "2020", "columns": [col]}
        entries = _field_availability_from_inventory(inv, "f.jsonl")
        assert entries[0]["source"] == ""


# ---------------------------------------------------------------------------
# build_coverage_metadata — end-to-end with temp directory
# ---------------------------------------------------------------------------


class TestBuildCoverageMetadata:
    def test_empty_observed_dir_returns_base_structure(self, tmp_path: Path) -> None:
        observed = tmp_path / "observed"
        observed.mkdir()
        result = build_coverage_metadata(observed)
        assert result["schema_version"] == "1.0"
        assert "generated_at" in result
        assert "field_availability" in result
        assert "coverage_gaps" in result
        assert isinstance(result["field_availability"], list)
        # no files → no entries
        assert result["field_availability"] == []

    def test_nonexistent_observed_dir_returns_base_structure(self, tmp_path: Path) -> None:
        observed = tmp_path / "does_not_exist"
        result = build_coverage_metadata(observed)
        assert result["schema_version"] == "1.0"
        assert result["field_availability"] == []

    def test_design_gaps_always_present(self, tmp_path: Path) -> None:
        observed = tmp_path / "observed"
        observed.mkdir()
        result = build_coverage_metadata(observed)
        gaps = result["coverage_gaps"]
        assert any(g["source"] == "tse" for g in gaps)
        tse_gap = next(g for g in gaps if g["source"] == "tse")
        assert tse_gap["scope"] == "campaign_expenses"
        assert tse_gap["blocking"] is True

    def test_tse_by_year_inventory_contributes_entries(self, tmp_path: Path) -> None:
        observed = tmp_path / "observed"
        tse_by_year = observed / "tse" / "by_year"
        tse_by_year.mkdir(parents=True)

        inv = _make_inventory(
            source="tse",
            year_or_cycle="2022",
            columns=[
                _make_col("cpf", null_rate=0.0),
                _make_col("nome_doador", null_rate=0.70),
            ],
        )
        (tse_by_year / "donations_raw_2022.json").write_text(
            json.dumps(inv), encoding="utf-8"
        )
        result = build_coverage_metadata(observed)
        availability = result["field_availability"]
        assert len(availability) == 1
        assert availability[0]["field"] == "nome_doador"
        assert availability[0]["year_or_cycle"] == "2022"
        # File label: stem donations_raw_2022 → parts ["donations_raw", "2022"] → donations_raw.jsonl
        assert availability[0]["file_name"] == "donations_raw.jsonl"

    def test_by_year_file_label_derivation_single_part(self, tmp_path: Path) -> None:
        observed = tmp_path / "observed"
        tse_by_year = observed / "tse" / "by_year"
        tse_by_year.mkdir(parents=True)

        inv = _make_inventory(
            source="tse",
            year_or_cycle="2022",
            columns=[_make_col("x", null_rate=0.99)],
        )
        # Stem with no underscore suffix — falls through to "{stem}.jsonl"
        (tse_by_year / "snapshot.json").write_text(json.dumps(inv), encoding="utf-8")
        result = build_coverage_metadata(observed)
        assert result["field_availability"][0]["file_name"] == "snapshot.jsonl"

    def test_top_level_inventory_contributes_entries(self, tmp_path: Path) -> None:
        observed = tmp_path / "observed"
        cgu_dir = observed / "cgu"
        cgu_dir.mkdir(parents=True)

        inv = {
            "source": "cgu",
            "file_name": "ceis.jsonl",
            "year_or_cycle": "",
            "columns": [
                _make_col("cnpj", null_rate=0.0),
                _make_col("nome_sancionado", null_rate=0.80),
            ],
        }
        (cgu_dir / "cgu_inventory.json").write_text(json.dumps(inv), encoding="utf-8")
        result = build_coverage_metadata(observed)
        availability = result["field_availability"]
        assert any(e["field"] == "nome_sancionado" and e["source"] == "cgu" for e in availability)

    def test_files_starting_with_underscore_are_skipped(self, tmp_path: Path) -> None:
        observed = tmp_path / "observed"
        observed.mkdir()

        inv = _make_inventory(
            source="cgu",
            columns=[_make_col("field", null_rate=0.99)],
        )
        (observed / "_cross_file_report.json").write_text(json.dumps(inv), encoding="utf-8")
        result = build_coverage_metadata(observed)
        assert result["field_availability"] == []

    def test_invalid_json_inventory_is_skipped(self, tmp_path: Path) -> None:
        observed = tmp_path / "observed"
        tse_by_year = observed / "tse" / "by_year"
        tse_by_year.mkdir(parents=True)
        (tse_by_year / "bad_2022.json").write_text("NOT JSON {{", encoding="utf-8")
        result = build_coverage_metadata(observed)
        assert result["field_availability"] == []

    def test_reconciliation_keys_present(self, tmp_path: Path) -> None:
        observed = tmp_path / "observed"
        observed.mkdir()
        result = build_coverage_metadata(observed)
        assert result["reconciliation_keys"] == [
            "source",
            "file_name",
            "year_or_cycle",
            "layout_signature",
        ]


# ---------------------------------------------------------------------------
# write_coverage_metadata
# ---------------------------------------------------------------------------


class TestWriteCoverageMetadata:
    def test_writes_to_correct_path(self, tmp_path: Path) -> None:
        metadata = {"schema_version": "1.0", "field_availability": [], "coverage_gaps": []}
        out_path = write_coverage_metadata(metadata, tmp_path)
        assert out_path == tmp_path / "_coverage_metadata.json"
        assert out_path.exists()

    def test_content_is_valid_json(self, tmp_path: Path) -> None:
        metadata: dict[str, Any] = {
            "schema_version": "1.0",
            "generated_at": "2026-01-01T00:00:00+00:00",
            "field_availability": [{"source": "tse", "field": "nome"}],
            "coverage_gaps": [],
        }
        out_path = write_coverage_metadata(metadata, tmp_path)
        loaded = json.loads(out_path.read_text(encoding="utf-8"))
        assert loaded["schema_version"] == "1.0"
        assert loaded["field_availability"][0]["field"] == "nome"

    def test_creates_parent_dir_if_missing(self, tmp_path: Path) -> None:
        nested = tmp_path / "a" / "b" / "c"
        metadata: dict[str, Any] = {"x": 1}
        out_path = write_coverage_metadata(metadata, nested)
        assert out_path.exists()

    def test_returns_path_instance(self, tmp_path: Path) -> None:
        out = write_coverage_metadata({}, tmp_path)
        assert isinstance(out, Path)

    def test_non_ascii_content_preserved(self, tmp_path: Path) -> None:
        metadata: dict[str, Any] = {"note": "Informação com acentuação: ç, ã, é"}
        out_path = write_coverage_metadata(metadata, tmp_path)
        raw = out_path.read_text(encoding="utf-8")
        assert "acentuação" in raw
        # ensure_ascii=False means the characters are not escaped
        assert "\\u" not in raw
