"""Tests for shared helpers in analytics._temporal_utils (parse_date, percentile)."""

from __future__ import annotations

from datetime import datetime

from atlas_stf.analytics._temporal_utils import parse_date, percentile


class TestParseDate:
    def test_valid_iso(self) -> None:
        result = parse_date("2026-03-27")
        assert result == datetime(2026, 3, 27)

    def test_truncates_to_10_chars(self) -> None:
        result = parse_date("2026-03-27T14:30:00Z")
        assert result == datetime(2026, 3, 27)

    def test_none_input(self) -> None:
        assert parse_date(None) is None

    def test_empty_string(self) -> None:
        assert parse_date("") is None

    def test_non_string(self) -> None:
        assert parse_date(12345) is None  # type: ignore[arg-type]

    def test_invalid_date(self) -> None:
        assert parse_date("not-a-date") is None

    def test_short_string(self) -> None:
        # Slicing "abc" to [:10] gives "abc", strptime fails → None
        assert parse_date("abc") is None

    def test_partial_date(self) -> None:
        assert parse_date("2026-13-01") is None  # month 13


class TestPercentile:
    def test_empty_list(self) -> None:
        assert percentile([], 50) == 0.0

    def test_single_element(self) -> None:
        assert percentile([10.0], 50) == 10.0

    def test_median_odd(self) -> None:
        assert percentile([1.0, 2.0, 3.0], 50) == 2.0

    def test_p0(self) -> None:
        assert percentile([1.0, 2.0, 3.0], 0) == 1.0

    def test_p100(self) -> None:
        assert percentile([1.0, 2.0, 3.0], 100) == 3.0

    def test_interpolation(self) -> None:
        result = percentile([10.0, 20.0, 30.0, 40.0], 25)
        assert abs(result - 17.5) < 0.01

    def test_p5_p95(self) -> None:
        # The actual use case in decision_velocity
        values = sorted([float(i) for i in range(100)])
        p5 = percentile(values, 5)
        p95 = percentile(values, 95)
        assert p5 < p95
        assert 4.0 <= p5 <= 5.0
        assert 94.0 <= p95 <= 95.0
