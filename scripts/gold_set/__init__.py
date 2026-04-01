"""Gold set pipeline: generation, adjudication, and reporting."""

from ._builder import (
    adjudicated_count,
    apply_human_review,
    check_required_strata,
    generate_gold_set,
    write_gold_set,
)
from ._constants import ALL_STRATA, MINIMUM_GOLD_SET_SIZE, REQUIRED_STRATA
from ._summary import GoldSetSummary, build_summary, print_summary, write_summary_json

__all__ = [
    "ALL_STRATA",
    "GoldSetSummary",
    "MINIMUM_GOLD_SET_SIZE",
    "REQUIRED_STRATA",
    "adjudicated_count",
    "apply_human_review",
    "build_summary",
    "check_required_strata",
    "generate_gold_set",
    "print_summary",
    "write_gold_set",
    "write_summary_json",
]
