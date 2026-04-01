"""Name analysis helpers and stratum classification."""

from __future__ import annotations

import hashlib

from ._constants import PJ_INDICATORS, SEED

# ---------------------------------------------------------------------------
# Name analysis
# ---------------------------------------------------------------------------


def is_pj(name: str) -> bool:
    """Check if a name looks like a corporate/institutional entity."""
    return bool(set(name.upper().split()) & PJ_INDICATORS)


def first_token(name: str) -> str:
    parts = name.strip().split()
    return parts[0] if parts else ""


def levenshtein(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ac in enumerate(a, start=1):
        curr = [i]
        for j, bc in enumerate(b, start=1):
            cost = 0 if ac == bc else 1
            curr.append(min(prev[j] + 1, curr[j - 1] + 1, prev[j - 1] + cost))
        prev = curr
    return prev[-1]


def tokens_are_reorder(donor: str, entity: str) -> bool:
    return set(donor.split()) == set(entity.split()) and donor != entity


def name_containment(donor: str, entity: str) -> bool:
    d_tokens = set(donor.split())
    e_tokens = set(entity.split())
    return d_tokens < e_tokens or e_tokens < d_tokens


# ---------------------------------------------------------------------------
# Stratum classification
# ---------------------------------------------------------------------------


def classify_donation_party(rec: dict) -> str | None:
    """Classify a donation_match record (entity_type=party) into a stratum."""
    strategy = rec.get("match_strategy", "")
    score = rec.get("match_score", 0)
    has_cpf = bool(rec.get("donor_cpf_cnpj"))

    if strategy == "exact":
        return "exact_with_cpf" if has_cpf else "exact_name_fallback"
    if strategy == "canonical_name":
        return "canonical_with_cpf" if has_cpf else "canonical_no_cpf"
    if strategy == "jaccard":
        return "jaccard_high" if score > 0.85 else "jaccard_borderline"
    if strategy == "levenshtein":
        return "levenshtein_dist1" if score <= 1.0 else "levenshtein_dist2"
    return None


def classify_counsel(rec: dict) -> str | None:
    """Classify a donation_match record (entity_type=counsel)."""
    if rec.get("match_strategy") in ("exact", "canonical_name", "jaccard", "levenshtein"):
        return "counsel_match"
    return None


def classify_sanction(rec: dict) -> str | None:
    if rec.get("match_strategy") in ("exact", "canonical_name", "jaccard", "levenshtein"):
        return "sanction_match"
    return None


def classify_scl(rec: dict) -> str | None:
    if rec.get("link_degree") == 2:
        return "scl_degree2"
    return None


# ---------------------------------------------------------------------------
# Deterministic sampling
# ---------------------------------------------------------------------------


def deterministic_hash(rec: dict) -> str:
    key = rec.get("match_id") or rec.get("link_id") or f"{rec.get('donor_identity_key', '')}|{rec.get('entity_id', '')}"
    return hashlib.sha256(f"{SEED}:{key}".encode()).hexdigest()


def sample_stratum(records: list[dict], target: int) -> list[dict]:
    if len(records) <= target:
        return sorted(records, key=deterministic_hash)
    return sorted(records, key=deterministic_hash)[:target]
