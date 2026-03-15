"""Configuration for STF ministerial agenda extractor."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path


@dataclass
class AgendaFetchConfig:
    """Configuration for the agenda fetcher."""

    output_dir: Path = field(default_factory=lambda: Path("data/raw/agenda"))
    start_year: int = 2024
    start_month: int = 1
    end_year: int | None = None
    end_month: int | None = None
    rate_limit_seconds: float = 1.0
    max_retries: int = 3
    retry_delay_seconds: float = 5.0
    timeout_seconds: float = 30.0
    dry_run: bool = False

    def __post_init__(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)


GRAPHQL_BASE_URL = "https://noticias.stf.jus.br/"

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

# Fixed national holidays (month, day)
FIXED_HOLIDAYS: list[tuple[int, int]] = [
    (1, 1), (4, 21), (5, 1), (9, 7), (10, 12), (11, 2), (11, 15), (12, 25),
]

# Court recess periods by year — (start_month, start_day, end_month, end_day)
# Fallback when official calendar unavailable
RECESS_PERIODS: dict[str, list[tuple[int, int, int, int]]] = {
    "default": [
        (7, 2, 7, 31),    # recesso forense
        (12, 20, 12, 31), # recesso fim de ano
        (1, 1, 1, 31),    # recesso janeiro
    ],
}

# STF president mapping — who holds presidency at a given date
STF_PRESIDENTS: list[dict[str, str]] = [
    {
        "minister_slug": "barroso",
        "minister_name": "MIN. LUIS ROBERTO BARROSO",
        "start_date": "2023-10-02",
        "end_date": "2025-10-01",
    },
]


def resolve_president_at(event_date: date) -> dict[str, str] | None:
    """Return the president record for a given date, or None."""
    for p in STF_PRESIDENTS:
        start = date.fromisoformat(p["start_date"])
        end = date.fromisoformat(p["end_date"])
        if start <= event_date <= end:
            return p
    return None
