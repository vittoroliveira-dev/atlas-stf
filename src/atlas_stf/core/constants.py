"""Shared domain constants, types and pure helpers used across layers.

Convention: this module accepts ONLY stdlib-pure items that are genuinely
shared between two or more layers.  It must NOT grow into a catch-all.
Pure textual-representation helpers (e.g. collegiate_label) are allowed
when they are used by multiple layers and have zero I/O.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

# --- Temporal analysis windows ---
ROLLING_WINDOW_MONTHS = 6
EVENT_WINDOW_DAYS = 180

# --- Case filtering ---
CollegiateFilter = Literal["all", "colegiado", "monocratico"]


@dataclass(frozen=True)
class QueryFilters:
    minister: str | None = None
    period: str | None = None
    collegiate: CollegiateFilter = "all"
    judging_body: str | None = None
    process_class: str | None = None


# --- Pure textual representation (shared by serving + api) ---
def collegiate_label(value: bool | None) -> str:
    if value is True:
        return "Colegial"
    if value is False:
        return "Monocrático"
    return "INCERTO"
