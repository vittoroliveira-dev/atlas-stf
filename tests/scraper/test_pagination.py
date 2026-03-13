"""Tests for _pagination: month partition generation."""

from atlas_stf.scraper._pagination import default_date_range, generate_month_partitions


class TestGenerateMonthPartitions:
    def test_single_month(self) -> None:
        result = generate_month_partitions("2024-03-01", "2024-03-31")
        assert result == [("2024-03", "2024-03-01", "2024-03-31")]

    def test_partial_month(self) -> None:
        result = generate_month_partitions("2024-03-15", "2024-03-20")
        assert result == [("2024-03", "2024-03-01", "2024-03-20")]

    def test_cross_year(self) -> None:
        result = generate_month_partitions("2023-11-01", "2024-02-29")
        assert len(result) == 4
        assert result[0][0] == "2023-11"
        assert result[1][0] == "2023-12"
        assert result[2][0] == "2024-01"
        assert result[3] == ("2024-02", "2024-02-01", "2024-02-29")

    def test_clamps_end(self) -> None:
        result = generate_month_partitions("2024-06-01", "2024-06-15")
        assert result == [("2024-06", "2024-06-01", "2024-06-15")]

    def test_two_months(self) -> None:
        result = generate_month_partitions("2024-01-01", "2024-02-28")
        assert len(result) == 2
        assert result[0] == ("2024-01", "2024-01-01", "2024-01-31")
        assert result[1] == ("2024-02", "2024-02-01", "2024-02-28")


class TestDefaultDateRange:
    def test_returns_tuple(self) -> None:
        start, end = default_date_range()
        assert start == "2000-01-01"
        assert len(end) == 10  # yyyy-MM-dd
