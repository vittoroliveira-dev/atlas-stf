"""Tests for _cleaners module."""

import pandas as pd

from atlas_stf.staging._cleaners import (
    clean_x000d,
    normalize_residual_nulls,
    standardize_column_names,
    strip_whitespace,
)


class TestStripWhitespace:
    def test_strips_leading_trailing(self):
        df = pd.DataFrame({"a": ["  foo  ", "bar  ", "  baz"], "b": [1, 2, 3]})
        result = strip_whitespace(df)
        assert list(result["a"]) == ["foo", "bar", "baz"]

    def test_ignores_non_string_columns(self):
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        result = strip_whitespace(df)
        assert list(result["a"]) == [1, 2]


class TestCleanX000d:
    def test_replaces_x000d_with_space(self):
        df = pd.DataFrame({"a": ["hello_x000D_world", "clean"]})
        result, count = clean_x000d(df)
        assert result["a"].iloc[0] == "hello world"
        assert count == 1

    def test_handles_multiple_occurrences(self):
        df = pd.DataFrame({"a": ["a_x000D_b_x000D_c"]})
        result, count = clean_x000d(df)
        assert result["a"].iloc[0] == "a b c"
        assert count == 1

    def test_no_x000d_returns_zero(self):
        df = pd.DataFrame({"a": ["clean", "data"]})
        _, count = clean_x000d(df)
        assert count == 0


class TestNormalizeResidualNulls:
    def test_converts_dash_to_na(self):
        df = pd.DataFrame({"a": ["-", "value", "-"]})
        result, count = normalize_residual_nulls(df)
        assert pd.isna(result["a"].iloc[0])
        assert result["a"].iloc[1] == "value"
        assert count == 2

    def test_converts_ni_to_na(self):
        df = pd.DataFrame({"a": ["*NI*", "ok"]})
        result, count = normalize_residual_nulls(df)
        assert pd.isna(result["a"].iloc[0])
        assert count == 1


class TestStandardizeColumnNames:
    def test_removes_accents(self):
        df = pd.DataFrame({"Órgão Origem": [1]})
        result, mapping = standardize_column_names(df)
        assert "orgao_origem" in result.columns

    def test_snake_case(self):
        df = pd.DataFrame({"Data da autuação": [1]})
        result, mapping = standardize_column_names(df)
        assert "data_da_autuacao" in result.columns

    def test_handles_special_chars(self):
        df = pd.DataFrame({"Nº do processo": [1]})
        result, _ = standardize_column_names(df)
        assert "no_do_processo" in result.columns

    def test_handles_trailing_quote(self):
        df = pd.DataFrame({"Tempo admissibilidade tema str'": [1]})
        result, _ = standardize_column_names(df)
        col = list(result.columns)[0]
        assert not col.endswith("'")
        assert col == "tempo_admissibilidade_tema_str"

    def test_handles_duplicate_names(self):
        df = pd.DataFrame({"A B": [1], "A-B": [2]})
        result, _ = standardize_column_names(df)
        assert len(result.columns) == 2
        assert len(set(result.columns)) == 2
