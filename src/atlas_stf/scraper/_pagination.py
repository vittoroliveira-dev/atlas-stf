"""Monthly partition generation for incremental scraping."""

from __future__ import annotations

import calendar
from datetime import date


def generate_month_partitions(start: str, end: str) -> list[tuple[str, str, str]]:
    """Return [(label, gte, lte), ...] for each month in [start, end].

    Parameters
    ----------
    start : str  ``yyyy-MM-dd``
    end : str    ``yyyy-MM-dd``

    Returns
    -------
    list of (label, date_gte, date_lte)
        e.g. [("2024-01", "2024-01-01", "2024-01-31"), ...]
    """
    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end)

    partitions: list[tuple[str, str, str]] = []
    current = start_date.replace(day=1)

    while current <= end_date:
        label = current.strftime("%Y-%m")
        month_start = current.isoformat()
        last_day = calendar.monthrange(current.year, current.month)[1]
        month_end_date = current.replace(day=last_day)
        # Clamp to actual end_date
        if month_end_date > end_date:
            month_end_date = end_date
        partitions.append((label, month_start, month_end_date.isoformat()))

        # Advance to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    return partitions


def default_date_range() -> tuple[str, str]:
    """Return (start, end) covering 2000-01-01 to today."""
    return ("2000-01-01", date.today().isoformat())
