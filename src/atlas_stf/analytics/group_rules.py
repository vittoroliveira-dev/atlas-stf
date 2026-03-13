"""Deterministic group-comparability rules.

Canonical definitions live in atlas_stf.core.rules.
This module re-exports for backward compatibility.
"""

from ..core.rules import (
    MAX_CASE_COUNT,
    MIN_CASE_COUNT,
    RULE_VERSION,
    GroupKey,
    classify_group_size,
    classify_judging_body_category,
)

__all__ = [
    "GroupKey",
    "MAX_CASE_COUNT",
    "MIN_CASE_COUNT",
    "RULE_VERSION",
    "classify_group_size",
    "classify_judging_body_category",
]
