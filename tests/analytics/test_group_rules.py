from __future__ import annotations

from atlas_stf.analytics.group_rules import classify_group_size


def test_classify_group_size_below_min():
    status, reason = classify_group_size(4)
    assert status == "insufficient_cases"
    assert reason == "below_min_case_count"


def test_classify_group_size_valid():
    status, reason = classify_group_size(5)
    assert status == "valid"
    assert reason is None
