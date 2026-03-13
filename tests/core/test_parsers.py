"""Tests for core parser functions."""

import math

import pytest

from atlas_stf.core.parsers import (
    as_optional_str,
    first_non_null,
    infer_process_number,
    is_missing,
    parse_bool_collegiate,
    parse_decision_year,
    split_party_names,
    split_subjects,
)


class TestIsMissing:
    @pytest.mark.parametrize(
        "value,expected",
        [
            (None, True),
            (float("nan"), True),
            ("", False),
            ("text", False),
            (0, False),
            (False, False),
        ],
    )
    def test_values(self, value, expected):
        assert is_missing(value) == expected


class TestAsOptionalStr:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("hello", "hello"),
            ("  hello  ", "hello"),
            ("", None),
            ("   ", None),
            (None, None),
            (float("nan"), None),
            (42, "42"),
        ],
    )
    def test_values(self, value, expected):
        assert as_optional_str(value) == expected


class TestFirstNonNull:
    def test_returns_first_present(self):
        row = {"a": None, "b": "val_b", "c": "val_c"}
        assert first_non_null(row, "a", "b", "c") == "val_b"

    def test_returns_none_when_all_missing(self):
        row = {"a": None}
        assert first_non_null(row, "a", "b") is None

    def test_nan_is_skipped(self):
        row = {"a": math.nan, "b": "ok"}
        assert first_non_null(row, "a", "b") == "ok"


class TestSplitSubjects:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("Direito Civil|Direito Penal", ["Direito Civil", "Direito Penal"]),
            ("Único", ["Único"]),
            (None, None),
            ("", None),
            ("  |  ", None),
        ],
    )
    def test_splitting(self, value, expected):
        assert split_subjects(value) == expected


class TestParseBoolCollegiate:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("COLEGIADA", True),
            ("Decisão Colegiada", True),
            ("MONOCRÁTICA", False),
            ("MONOCR", False),
            (None, None),
            ("OUTRO", None),
        ],
    )
    def test_parsing(self, value, expected):
        assert parse_bool_collegiate(value) == expected


class TestParseDecisionYear:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("2024", 2024),
            ("abc", None),
            (None, None),
            ("", None),
        ],
    )
    def test_parsing(self, value, expected):
        assert parse_decision_year(value) == expected


class TestSplitPartyNames:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("AUTOR VS RÉU", ["AUTOR", "RÉU"]),
            ("A X B", ["A", "B"]),
            (None, []),
            ("ÚNICO", ["ÚNICO"]),
        ],
    )
    def test_splitting(self, value, expected):
        assert split_party_names(value) == expected


class TestInferProcessNumber:
    def test_from_processo_field(self):
        assert infer_process_number({"processo": "ADI 1234"}) == "ADI 1234"

    def test_from_classe_and_numero(self):
        assert infer_process_number({"classe": "ADI", "no_do_processo": "1234"}) == "ADI 1234"

    def test_from_processo_paradigma(self):
        assert infer_process_number({"processo_paradigma": "RE 5678"}) == "RE 5678"

    def test_returns_none_when_empty(self):
        assert infer_process_number({}) is None
