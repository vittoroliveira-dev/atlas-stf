"""Outcome classification and baseline rate helpers for match analytics."""

from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path

from ..core.rules import (
    classify_judging_body_category,
    classify_outcome_for_party,
    classify_outcome_materiality,
    classify_outcome_raw,
)
from ._match_io import read_jsonl
from .baseline import MIN_RELIABLE_SIZE

__all__ = [
    "build_baseline_rates",
    "build_baseline_rates_stratified",
    "build_counsel_client_map_from_links",
    "build_counsel_process_map",
    "build_party_process_map",
    "build_process_class_map",
    "build_process_jb_category_map",
    "build_process_outcomes",
    "classify_outcome",
    "compute_favorable_rate",
    "compute_favorable_rate_role_aware",
    "compute_favorable_rate_substantive",
    "lookup_baseline_rate",
]


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


def build_baseline_rates_stratified(
    decision_event_path: Path,
    process_path: Path,
) -> tuple[dict[tuple[str, str], float], dict[str, float]]:
    """Compute baseline favorable rate stratified by (process_class, jb_category).

    Returns (stratified_rates, fallback_rates) where:
    - stratified_rates: only cells with >= MIN_RELIABLE_SIZE events
    - fallback_rates: per process_class (identical to build_baseline_rates)
    """
    class_map: dict[str, str] = {}
    for record in read_jsonl(process_path):
        pid = record.get("process_id")
        pc = record.get("process_class")
        if pid and pc:
            class_map[pid] = pc

    stratified_outcomes: dict[tuple[str, str], list[str]] = defaultdict(list)
    class_outcomes: dict[str, list[str]] = defaultdict(list)

    for record in read_jsonl(decision_event_path):
        pid = record.get("process_id")
        progress = record.get("decision_progress")
        if not (pid and progress and pid in class_map):
            continue
        pc = class_map[pid]
        jb_cat = classify_judging_body_category(
            record.get("judging_body"),
            record.get("is_collegiate"),
        )
        stratified_outcomes[(pc, jb_cat)].append(progress)
        class_outcomes[pc].append(progress)

    # Fallback rates: identical logic to build_baseline_rates
    fallback_rates: dict[str, float] = {}
    for pc, outcomes in class_outcomes.items():
        rate = compute_favorable_rate(outcomes)
        if rate is not None:
            fallback_rates[pc] = rate

    # Stratified rates: only cells with enough observations
    stratified_rates: dict[tuple[str, str], float] = {}
    for key, outcomes in stratified_outcomes.items():
        if len(outcomes) < MIN_RELIABLE_SIZE:
            continue
        rate = compute_favorable_rate(outcomes)
        if rate is not None:
            stratified_rates[key] = rate

    return stratified_rates, fallback_rates


def build_process_jb_category_map(decision_event_path: Path) -> dict[str, str]:
    """Map process_id -> predominant judging body category."""
    counters: dict[str, Counter[str]] = defaultdict(Counter)
    for record in read_jsonl(decision_event_path):
        pid = record.get("process_id")
        if not pid:
            continue
        jb_cat = classify_judging_body_category(
            record.get("judging_body"),
            record.get("is_collegiate"),
        )
        counters[pid][jb_cat] += 1

    return {pid: counter.most_common(1)[0][0] for pid, counter in counters.items()}


def lookup_baseline_rate(
    stratified_rates: dict[tuple[str, str], float],
    fallback_rates: dict[str, float],
    process_class: str,
    jb_category: str,
) -> float | None:
    """Look up baseline rate: stratified cell first, then class-level fallback."""
    rate = stratified_rates.get((process_class, jb_category))
    if rate is not None:
        return rate
    return fallback_rates.get(process_class)


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
