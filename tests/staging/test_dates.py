"""Tests for _dates module."""

import pandas as pd

from atlas_stf.staging._dates import normalize_all_dates, normalize_date_column


class TestNormalizeDateColumn:
    def test_yyyy_mm_dd_hh_mm_ss(self):
        s = pd.Series(["2026-03-05 14:30:00"])
        result = normalize_date_column(s)
        assert result.iloc[0] == "2026-03-05"

    def test_dd_mm_yyyy_hh_mm_ss(self):
        s = pd.Series(["07/11/2002 10:00:00"])
        result = normalize_date_column(s)
        assert result.iloc[0] == "2002-11-07"

    def test_dd_mm_yyyy(self):
        s = pd.Series(["06/01/2003"])
        result = normalize_date_column(s)
        assert result.iloc[0] == "2003-01-06"

    def test_d_m_yyyy_no_zero_pad(self):
        s = pd.Series(["6/6/2025"])
        result = normalize_date_column(s)
        assert result.iloc[0] == "2025-06-06"

    def test_already_yyyy_mm_dd(self):
        s = pd.Series(["2026-01-27"])
        result = normalize_date_column(s)
        assert result.iloc[0] == "2026-01-27"

    def test_na_values_preserved(self):
        s = pd.Series([pd.NA, None, ""])
        result = normalize_date_column(s)
        assert pd.isna(result.iloc[0])
        assert pd.isna(result.iloc[1])
        assert pd.isna(result.iloc[2])

    def test_mixed_formats_in_column(self):
        s = pd.Series(["2026-03-05 14:30:00", "06/01/2003", "6/6/2025"])
        result = normalize_date_column(s)
        assert result.iloc[0] == "2026-03-05"
        assert result.iloc[1] == "2003-01-06"
        assert result.iloc[2] == "2025-06-06"

    def test_invalid_date_returns_null(self):
        s = pd.Series(["32/01/2026"])
        result = normalize_date_column(s)
        assert pd.isna(result.iloc[0])

    def test_unrecognized_format_returns_null(self):
        s = pd.Series(["2026/31/01"])
        result = normalize_date_column(s)
        assert pd.isna(result.iloc[0])


class TestNormalizeAllDates:
    def test_normalizes_specified_columns(self):
        df = pd.DataFrame(
            {
                "Data": ["06/01/2003", "07/02/2004"],
                "Nome": ["foo", "bar"],
            }
        )
        result, count = normalize_all_dates(df, ["Data"])
        assert result["Data"].iloc[0] == "2003-01-06"
        assert result["Nome"].iloc[0] == "foo"
        assert count == 2

    def test_skips_missing_columns(self):
        df = pd.DataFrame({"A": [1]})
        result, count = normalize_all_dates(df, ["NonExistent"])
        assert count == 0

    def test_invalid_dates_are_cleared_and_counted_as_changed(self):
        df = pd.DataFrame({"Data": ["32/01/2026", "07/02/2004"]})
        result, count = normalize_all_dates(df, ["Data"])
        assert pd.isna(result["Data"].iloc[0])
        assert result["Data"].iloc[1] == "2004-02-07"
        assert count == 2
