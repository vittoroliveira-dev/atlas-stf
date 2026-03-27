"""Single-pass loaders for sanction corporate link: process + decision_event data."""

from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path

from ..core.rules import classify_judging_body_category
from ._match_helpers import compute_favorable_rate
from ._match_io import iter_jsonl
from .baseline import MIN_RELIABLE_SIZE


def load_process_class_map(
    process_path: Path,
) -> dict[str, str]:
    """Read process.jsonl once and return process_id → process_class.

    Replaces two sequential calls:
      - build_process_class_map(process_path)
      - the process.jsonl read inside build_baseline_rates_stratified(…, process_path)
    The returned dict is reused for both purposes.
    """
    class_map: dict[str, str] = {}
    for record in iter_jsonl(process_path):
        pid = record.get("process_id")
        pc = record.get("process_class")
        if pid and pc:
            class_map[pid] = pc
    return class_map


def load_decision_event_data(
    decision_event_path: Path,
    class_map: dict[str, str],
) -> tuple[
    dict[str, list[str]],
    dict[tuple[str, str], float],
    dict[str, float],
    dict[str, str],
]:
    """Read decision_event.jsonl once and return all derived structures.

    Replaces three sequential calls:
      - build_process_outcomes(decision_event_path)
      - the decision_event.jsonl read inside build_baseline_rates_stratified(…)
      - build_process_jb_category_map(decision_event_path)

    Returns:
        process_outcomes   – process_id → [decision_progress]  (build_process_outcomes)
        stratified_rates   – (process_class, jb_category) → baseline rate
        fallback_rates     – process_class → baseline rate       (build_baseline_rates_stratified)
        process_jb_map     – process_id → modal jb_category      (build_process_jb_category_map)
    """
    outcomes: dict[str, list[str]] = defaultdict(list)
    stratified_outcomes: dict[tuple[str, str], list[str]] = defaultdict(list)
    class_outcomes: dict[str, list[str]] = defaultdict(list)
    jb_counters: dict[str, Counter[str]] = defaultdict(Counter)

    for record in iter_jsonl(decision_event_path):
        pid = record.get("process_id")
        progress = record.get("decision_progress")

        if not pid:
            continue

        jb_cat = classify_judging_body_category(
            record.get("judging_body"),
            record.get("is_collegiate"),
        )
        jb_counters[pid][jb_cat] += 1

        if progress:
            outcomes[pid].append(progress)
            if pid in class_map:
                pc = class_map[pid]
                stratified_outcomes[(pc, jb_cat)].append(progress)
                class_outcomes[pc].append(progress)

    # Fallback rates: per process_class
    fallback_rates: dict[str, float] = {}
    for pc, poutcomes in class_outcomes.items():
        rate = compute_favorable_rate(poutcomes)
        if rate is not None:
            fallback_rates[pc] = rate

    # Stratified rates: only cells with enough observations
    stratified_rates: dict[tuple[str, str], float] = {}
    for key, poutcomes in stratified_outcomes.items():
        if len(poutcomes) < MIN_RELIABLE_SIZE:
            continue
        rate = compute_favorable_rate(poutcomes)
        if rate is not None:
            stratified_rates[key] = rate

    process_jb_map = {
        pid: counter.most_common(1)[0][0] for pid, counter in jb_counters.items()
    }

    return dict(outcomes), stratified_rates, fallback_rates, process_jb_map
