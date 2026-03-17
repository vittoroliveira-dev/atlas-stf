from __future__ import annotations

__all__ = ["MATCH_STRATEGY_TO_CONFIDENCE", "match_confidence"]

MATCH_STRATEGY_TO_CONFIDENCE: dict[str, str] = {
    "tax_id": "deterministic",
    "alias": "exact_name",
    "exact": "exact_name",
    "canonical_name": "exact_name",
    "jaccard": "fuzzy",
    "levenshtein": "fuzzy",
    "ambiguous": "nominal_review_needed",
}


def match_confidence(strategy: str | None) -> str:
    if not strategy:
        return "unknown"
    return MATCH_STRATEGY_TO_CONFIDENCE.get(strategy, "unknown")
