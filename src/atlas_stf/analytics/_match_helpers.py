"""Shared helpers for match analytics (sanction_match, donation_match, corporate_network, counsel_affinity)."""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..core.identity import (
    _tokenize_for_similarity,
    canonicalize_entity_name,
    jaccard_similarity,
    levenshtein_distance,
    normalize_entity_name,
    normalize_tax_id,
)
from ._match_io import read_jsonl

logger = logging.getLogger(__name__)


def degree_decay(link_degree: int) -> float:
    """Exponential decay for corporate link degree: full weight up to degree 2, halved per extra hop."""
    return 1.0 if link_degree <= 2 else 0.5 ** (link_degree - 2)


DEFAULT_ALIAS_PATH = Path("data/curated/entity_alias.jsonl")


@dataclass(frozen=True)
class MatchThresholds:
    """Tunable thresholds for the entity matching cascade."""

    jaccard_min: float = 0.8
    levenshtein_max: int = 2
    length_prefilter_max: int = 2
    max_fuzzy_candidates: int = 10_000


DEFAULT_MATCH_THRESHOLDS = MatchThresholds()


@dataclass(frozen=True)
class EntityMatchResult:
    record: dict[str, Any]
    strategy: str
    score: float | None = None
    matched_alias: str | None = None
    matched_tax_id: str | None = None
    uncertainty_note: str | None = None
    candidate_count: int | None = None


@dataclass(frozen=True)
class EntityMatchIndex:
    records: list[dict[str, Any]]
    by_tax_id: dict[str, list[dict[str, Any]]]
    by_name: dict[str, list[dict[str, Any]]]
    by_canonical_name: dict[str, list[dict[str, Any]]]
    by_token: dict[str, set[int]]
    canonical_names: list[str | None]
    aliases: dict[str, list[dict[str, Any]]]


def build_party_index(party_path: Path) -> dict[str, dict[str, Any]]:
    """Index parties by normalized name -> party record."""
    index: dict[str, dict[str, Any]] = {}
    collisions = 0
    for record in read_jsonl(party_path):
        norm = normalize_entity_name(record.get("party_name_normalized") or record.get("party_name_raw", ""))
        if norm:
            if norm in index:
                collisions += 1
            else:
                index[norm] = record
    if collisions:
        logger.warning("build_party_index: %d name collisions (first record kept)", collisions)
    return index


def _record_name(record: dict[str, Any], name_field: str) -> str | None:
    return normalize_entity_name(record.get(name_field) or record.get("canonical_name_normalized"))


def _record_canonical_name(record: dict[str, Any], name_field: str) -> str | None:
    return canonicalize_entity_name(record.get("canonical_name_normalized") or record.get(name_field))


def load_alias_index(
    alias_path: Path = DEFAULT_ALIAS_PATH,
    *,
    entity_kind: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    if not alias_path.exists():
        return {}
    index: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in read_jsonl(alias_path):
        if not record.get("active", True):
            continue
        if entity_kind and record.get("entity_kind") not in {None, "", entity_kind}:
            continue
        alias = normalize_entity_name(record.get("alias_normalized"))
        if alias:
            index[alias].append(record)
    return dict(index)


def build_entity_match_index(
    records: list[dict[str, Any]],
    *,
    name_field: str,
    alias_path: Path = DEFAULT_ALIAS_PATH,
    entity_kind: str | None = None,
) -> EntityMatchIndex:
    by_tax_id: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_name: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_canonical_name: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_token: dict[str, set[int]] = defaultdict(set)
    canonical_names: list[str | None] = []
    for idx, record in enumerate(records):
        tax_id = normalize_tax_id(record.get("entity_tax_id"))
        if tax_id:
            by_tax_id[tax_id].append(record)
        name = _record_name(record, name_field)
        if name:
            by_name[name].append(record)
        canonical = _record_canonical_name(record, name_field)
        canonical_names.append(canonical)
        if canonical:
            by_canonical_name[canonical].append(record)
            for token in _tokenize_for_similarity(canonical):
                by_token[token].add(idx)
    return EntityMatchIndex(
        records=records,
        by_tax_id=dict(by_tax_id),
        by_name=dict(by_name),
        by_canonical_name=dict(by_canonical_name),
        by_token=dict(by_token),
        canonical_names=canonical_names,
        aliases=load_alias_index(alias_path, entity_kind=entity_kind),
    )


def _resolve_unique_candidate(
    candidates: list[dict[str, Any]],
    *,
    strategy: str,
    score: float | None = None,
    matched_alias: str | None = None,
    matched_tax_id: str | None = None,
) -> EntityMatchResult | None:
    if not candidates:
        return None
    if len(candidates) > 1:
        return None
    return EntityMatchResult(
        record=candidates[0],
        strategy=strategy,
        score=score,
        matched_alias=matched_alias,
        matched_tax_id=matched_tax_id,
    )


def _collect_fuzzy_candidates(
    canonical_name: str,
    index: EntityMatchIndex,
    max_candidates: int,
) -> set[int]:
    """Pre-filter candidate indices via token intersection (lossless for Jaccard >= 0.5)."""
    query_tokens = _tokenize_for_similarity(canonical_name)
    sorted_sets: list[tuple[int, set[int]]] = []
    for token in query_tokens:
        token_set = index.by_token.get(token)
        if token_set:
            sorted_sets.append((len(token_set), token_set))
    sorted_sets.sort(key=lambda x: x[0])
    candidate_indices: set[int] = set()
    if len(sorted_sets) >= 2:
        candidate_indices = sorted_sets[0][1] & sorted_sets[1][1]
        if not candidate_indices:
            candidate_indices = set(sorted_sets[0][1])
    elif sorted_sets:
        candidate_indices = set(sorted_sets[0][1])
    if len(candidate_indices) > max_candidates:
        candidate_indices = set()
    return candidate_indices


def match_entity_record(
    *,
    query_name: Any,
    query_tax_id: Any = None,
    index: EntityMatchIndex,
    name_field: str,
    thresholds: MatchThresholds | None = None,
) -> EntityMatchResult | None:
    t = thresholds or DEFAULT_MATCH_THRESHOLDS
    normalized_name = normalize_entity_name(query_name)
    canonical_name = canonicalize_entity_name(query_name)
    tax_id = normalize_tax_id(query_tax_id)

    if tax_id:
        candidates = index.by_tax_id.get(tax_id, [])
        match = _resolve_unique_candidate(candidates, strategy="tax_id", score=1.0, matched_tax_id=tax_id)
        if match is not None:
            return match

    if normalized_name:
        alias_matches = index.aliases.get(normalized_name, [])
        if len(alias_matches) == 1:
            alias = alias_matches[0]
            alias_tax_id = normalize_tax_id(alias.get("entity_tax_id"))
            if alias_tax_id:
                match = _resolve_unique_candidate(
                    index.by_tax_id.get(alias_tax_id, []),
                    strategy="alias",
                    score=1.0,
                    matched_alias=normalized_name,
                    matched_tax_id=alias_tax_id,
                )
                if match is not None:
                    return match
            alias_canonical = canonicalize_entity_name(alias.get("canonical_name_normalized"))
            if alias_canonical:
                match = _resolve_unique_candidate(
                    index.by_canonical_name.get(alias_canonical, []),
                    strategy="alias",
                    score=1.0,
                    matched_alias=normalized_name,
                )
                if match is not None:
                    return match

    if normalized_name:
        match = _resolve_unique_candidate(index.by_name.get(normalized_name, []), strategy="exact", score=1.0)
        if match is not None:
            return match

    if canonical_name:
        match = _resolve_unique_candidate(
            index.by_canonical_name.get(canonical_name, []),
            strategy="canonical_name",
            score=1.0,
        )
        if match is not None:
            return match

    if canonical_name:
        candidate_indices = _collect_fuzzy_candidates(canonical_name, index, t.max_fuzzy_candidates)

        # Jaccard similarity (only meaningful for multi-word names)
        if " " in canonical_name:
            scored: list[tuple[float, dict[str, Any]]] = []
            for idx in candidate_indices:
                record = index.records[idx]
                candidate_name = index.canonical_names[idx]
                if not candidate_name:
                    continue
                score = jaccard_similarity(canonical_name, candidate_name)
                if score >= t.jaccard_min:
                    scored.append((score, record))
            if scored:
                scored.sort(key=lambda item: item[0], reverse=True)
                best_score = scored[0][0]
                best_records = [record for score, record in scored if score == best_score]
                match = _resolve_unique_candidate(best_records, strategy="jaccard", score=best_score)
                if match is not None:
                    return match
                return EntityMatchResult(
                    record=best_records[0],
                    strategy="ambiguous",
                    score=best_score,
                    uncertainty_note="multiple_candidates_same_jaccard_score",
                    candidate_count=len(best_records),
                )

        # Levenshtein distance (token-filtered candidates)
        canonical_len = len(canonical_name)
        scored_distance: list[tuple[int, dict[str, Any]]] = []
        for idx in candidate_indices:
            candidate_name = index.canonical_names[idx]
            if not candidate_name:
                continue
            if abs(len(candidate_name) - canonical_len) > t.length_prefilter_max:
                continue
            distance = levenshtein_distance(canonical_name, candidate_name)
            if distance <= t.levenshtein_max:
                scored_distance.append((distance, index.records[idx]))
        if scored_distance:
            scored_distance.sort(key=lambda item: item[0])
            best_distance = scored_distance[0][0]
            best_records = [record for distance, record in scored_distance if distance == best_distance]
            match = _resolve_unique_candidate(
                best_records,
                strategy="levenshtein",
                score=float(best_distance),
            )
            if match is not None:
                return match
            return EntityMatchResult(
                record=best_records[0],
                strategy="ambiguous",
                score=float(best_distance),
                uncertainty_note="multiple_candidates_same_levenshtein_distance",
                candidate_count=len(best_records),
            )

    return None


# Re-exports for backward compatibility — existing importers use _match_helpers as entry point.
from ._match_io import iter_jsonl  # noqa: E402, F401
from ._outcome_helpers import (  # noqa: E402, F401
    build_baseline_rates,
    build_baseline_rates_stratified,
    build_counsel_client_map_from_links,
    build_counsel_process_map,
    build_party_process_map,
    build_process_class_map,
    build_process_jb_category_map,
    build_process_outcomes,
    classify_outcome,
    compute_favorable_rate,
    compute_favorable_rate_role_aware,
    compute_favorable_rate_substantive,
    lookup_baseline_rate,
)
