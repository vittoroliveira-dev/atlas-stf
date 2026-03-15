"""Tests for RFB reference table parsing and loading."""

from __future__ import annotations

import io
import json
from pathlib import Path

from atlas_stf.rfb._config import RFB_REFERENCE_TABLES
from atlas_stf.rfb._reference import (
    _parse_two_column_csv_text,
    load_all_reference_tables,
    load_reference_table,
)


class TestParseTwoColumnCsvText:
    def test_valid_csv(self) -> None:
        stream = io.StringIO("01;Administrador\n02;Diretor\n03;Conselheiro\n")
        result = _parse_two_column_csv_text(stream)
        assert result == {"01": "Administrador", "02": "Diretor", "03": "Conselheiro"}

    def test_empty_values_skipped(self) -> None:
        stream = io.StringIO(";Sem codigo\n02;\n;;\n04;Valido\n")
        result = _parse_two_column_csv_text(stream)
        # Row 1: code is empty -> skipped.  Row 2: code "02", desc empty -> kept.
        # Row 3: both empty -> skipped.  Row 4: valid.
        assert "04" in result
        assert result["04"] == "Valido"
        assert result.get("02") == ""
        assert "" not in result

    def test_iso_8859_1_content(self) -> None:
        """Latin-1 characters survive when the stream is already decoded."""
        stream = io.StringIO("01;Administra\u00e7\u00e3o\n02;Promo\u00e7\u00e3o\n")
        result = _parse_two_column_csv_text(stream)
        assert result["01"] == "Administra\u00e7\u00e3o"
        assert result["02"] == "Promo\u00e7\u00e3o"


class TestLoadReferenceTable:
    def test_missing_file(self, tmp_path: Path) -> None:
        missing = tmp_path / "does_not_exist.json"
        result = load_reference_table(missing)
        assert result == {}

    def test_valid_json(self, tmp_path: Path) -> None:
        table_path = tmp_path / "qualificacoes.json"
        payload = {"49": "S\u00f3cio-Administrador", "22": "S\u00f3cio"}
        table_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        result = load_reference_table(table_path)
        assert result == payload


class TestLoadAllReferenceTables:
    def test_loads_all_five_tables(self, tmp_path: Path) -> None:
        for table_name in RFB_REFERENCE_TABLES:
            path = tmp_path / f"{table_name.lower()}.json"
            path.write_text(json.dumps({"01": f"desc_{table_name}"}), encoding="utf-8")

        result = load_all_reference_tables(tmp_path)
        assert len(result) == 5
        for table_name in RFB_REFERENCE_TABLES:
            key = table_name.lower()
            assert key in result
            assert result[key]["01"] == f"desc_{table_name}"
