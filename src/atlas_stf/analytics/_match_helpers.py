"""Shared helpers for match analytics (sanction_match, donation_match, corporate_network, counsel_affinity)."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from collections.abc import Iterator
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
from ..core.rules import classify_outcome_for_party, classify_outcome_materiality, classify_outcome_raw

logger = logging.getLogger(__name__)
DEFAULT_ALIAS_PATH = Path("data/curated/entity_alias.jsonl")
_MAX_FUZZY_CANDIDATES = 10_000


@dataclass(frozen=True)
class EntityMatchResult:
    record: dict[str, Any]
    strategy: str
    score: float | None = None
    matched_alias: str | None = None
    matched_tax_id: str | None = None
    uncertainty_note: str | None = None


@dataclass(frozen=True)
class EntityMatchIndex:
    records: list[dict[str, Any]]
    by_tax_id: dict[str, list[dict[str, Any]]]
    by_name: dict[str, list[dict[str, Any]]]
    by_canonical_name: dict[str, list[dict[str, Any]]]
    by_token: dict[str, set[int]]
    canonical_names: list[str | None]
    aliases: dict[str, list[dict[str, Any]]]


def iter_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    """Yield JSONL records one at a time (never loads full file)."""
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    return list(iter_jsonl(path))


def build_party_index(party_path: Path) -> dict[str, dict[str, Any]]:
    """Index parties by normalized name -> party record."""
    index: dict[str, dict[str, Any]] = {}
    for record in read_jsonl(party_path):
        norm = normalize_entity_name(record.get("party_name_normalized") or record.get("party_name_raw", ""))
        if norm:
            index.setdefault(norm, record)
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


def match_entity_record(
    *,
    query_name: Any,
    query_tax_id: Any = None,
    index: EntityMatchIndex,
    name_field: str,
) -> EntityMatchResult | None:
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
        query_tokens = _tokenize_for_similarity(canonical_name)
        # Frequency-aware candidate pre-filter: sort token sets by ascending
        # frequency and intersect the 2 rarest.  For Jaccard >= 0.8 any valid
        # match must share the query's rarest tokens, making this lossless.
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
        if len(candidate_indices) > _MAX_FUZZY_CANDIDATES:
            candidate_indices = set()

        # Jaccard similarity (only meaningful for multi-word names)
        if " " in canonical_name:
            scored: list[tuple[float, dict[str, Any]]] = []
            for idx in candidate_indices:
                record = index.records[idx]
                candidate_name = index.canonical_names[idx]
                if not candidate_name:
                    continue
                score = jaccard_similarity(canonical_name, candidate_name)
                if score >= 0.8:
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
                )

        # Levenshtein distance (token-filtered candidates)
        canonical_len = len(canonical_name)
        scored_distance: list[tuple[int, dict[str, Any]]] = []
        for idx in candidate_indices:
            candidate_name = index.canonical_names[idx]
            if not candidate_name:
                continue
            if abs(len(candidate_name) - canonical_len) > 2:
                continue
            distance = levenshtein_distance(canonical_name, candidate_name)
            if distance <= 2:
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
            )

    return None


def classify_outcome(decision_progress: str) -> str | None:
    """Classify a decision_progress string as canonical outcome or None."""
    return classify_outcome_raw(decision_progress)


def compute_favorable_rate(outcomes: list[str]) -> float | None:
    """Compute rate of favorable outcomes from a list of decision_progress values."""
    classified = [(o, classify_outcome(o)) for o in outcomes]
    classifiable = [c for c in classified if c[1] in {"favorable", "unfavorable"}]
    if not classifiable:
        return None
    favorable = sum(1 for _, cls in classifiable if cls == "favorable")
    return favorable / len(classifiable)


def compute_favorable_rate_substantive(
    outcomes_with_roles: list[tuple[str, str | None]],
) -> tuple[float | None, int]:
    """Compute favorable rate considering only substantive decisions.

    Filters out provisional, procedural, and unknown decisions before
    classifying favorable/unfavorable with role awareness.

    Returns (rate, n_substantive) where rate is None if no substantive
    decisions are classifiable.
    """
    substantive_pairs = [
        (progress, role)
        for progress, role in outcomes_with_roles
        if classify_outcome_materiality(progress) == "substantive"
    ]
    if not substantive_pairs:
        return None, 0

    classified = [classify_outcome_for_party(progress, role) for progress, role in substantive_pairs]
    classifiable = [c for c in classified if c in {"favorable", "unfavorable"}]
    if not classifiable:
        return None, len(substantive_pairs)

    favorable = sum(1 for c in classifiable if c == "favorable")
    return favorable / len(classifiable), len(classifiable)


def compute_favorable_rate_role_aware(
    outcomes_with_roles: list[tuple[str, str | None]],
) -> float | None:
    """Compute favorable rate considering party role in each process."""
    classified = [classify_outcome_for_party(progress, role) for progress, role in outcomes_with_roles]
    classifiable = [c for c in classified if c in {"favorable", "unfavorable"}]
    if not classifiable:
        return None
    favorable = sum(1 for c in classifiable if c == "favorable")
    return favorable / len(classifiable)


def build_party_process_map(
    process_party_link_path: Path, party_id_to_name: dict[str, str]
) -> dict[str, list[tuple[str, str | None]]]:
    """Map party_name_normalized -> list of (process_id, role_in_case)."""
    result: dict[str, list[tuple[str, str | None]]] = defaultdict(list)
    for record in read_jsonl(process_party_link_path):
        party_id = record.get("party_id", "")
        process_id = record.get("process_id", "")
        role = record.get("role_in_case")
        if party_id in party_id_to_name and process_id:
            result[party_id_to_name[party_id]].append((process_id, role))
    return dict(result)


def build_counsel_process_map(
    process_counsel_link_path: Path, counsel_id_to_name: dict[str, str]
) -> dict[str, list[tuple[str, str | None]]]:
    """Map counsel_name_normalized -> list of (process_id, side_in_case)."""
    result: dict[str, list[tuple[str, str | None]]] = defaultdict(list)
    for record in read_jsonl(process_counsel_link_path):
        counsel_id = record.get("counsel_id", "")
        process_id = record.get("process_id", "")
        side = record.get("side_in_case")
        if counsel_id in counsel_id_to_name and process_id:
            result[counsel_id_to_name[counsel_id]].append((process_id, side))
    return dict(result)


def build_process_outcomes(decision_event_path: Path) -> dict[str, list[str]]:
    """Map process_id -> list of decision_progress values."""
    result: dict[str, list[str]] = defaultdict(list)
    for record in read_jsonl(decision_event_path):
        process_id = record.get("process_id")
        progress = record.get("decision_progress")
        if process_id and progress:
            result[process_id].append(progress)
    return dict(result)


def build_baseline_rates(
    decision_event_path: Path,
    process_path: Path,
) -> dict[str, float]:
    """Compute baseline favorable rate per process_class."""
    class_map: dict[str, str] = {}
    for record in read_jsonl(process_path):
        pid = record.get("process_id")
        pc = record.get("process_class")
        if pid and pc:
            class_map[pid] = pc

    class_outcomes: dict[str, list[str]] = defaultdict(list)
    for record in read_jsonl(decision_event_path):
        pid = record.get("process_id")
        progress = record.get("decision_progress")
        if pid and progress and pid in class_map:
            class_outcomes[class_map[pid]].append(progress)

    rates: dict[str, float] = {}
    for pc, outcomes in class_outcomes.items():
        rate = compute_favorable_rate(outcomes)
        if rate is not None:
            rates[pc] = rate
    return rates


def build_process_class_map(process_path: Path) -> dict[str, str]:
    """Map process_id -> process_class."""
    result: dict[str, str] = {}
    for record in read_jsonl(process_path):
        pid = record.get("process_id")
        pc = record.get("process_class")
        if pid and pc:
            result[pid] = pc
    return result


def build_counsel_client_map_from_links(
    process_party_link_path: Path,
    process_counsel_link_path: Path,
    party_id_to_name: dict[str, str],
    counsel_id_to_name: dict[str, str],
) -> dict[str, set[str]]:
    """Map counsel_name_normalized -> set of party_name_normalized using link tables."""
    proc_parties: dict[str, set[str]] = defaultdict(set)
    for record in read_jsonl(process_party_link_path):
        pid = record.get("party_id", "")
        proc = record.get("process_id", "")
        if pid in party_id_to_name and proc:
            proc_parties[proc].add(party_id_to_name[pid])

    result: dict[str, set[str]] = defaultdict(set)
    for record in read_jsonl(process_counsel_link_path):
        cid = record.get("counsel_id", "")
        proc = record.get("process_id", "")
        if cid in counsel_id_to_name and proc and proc in proc_parties:
            result[counsel_id_to_name[cid]].update(proc_parties[proc])

    return dict(result)
