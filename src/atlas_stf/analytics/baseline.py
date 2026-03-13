"""Build baseline metrics for valid comparison groups."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.rules import classify_outcome_raw
from ..core.stats import beta_binomial_posterior_mean
from ..schema_validate import validate_records

BASELINE_SCHEMA = Path("schemas/baseline.schema.json")
SUMMARY_SCHEMA = Path("schemas/baseline_summary.schema.json")
DEFAULT_GROUP_PATH = Path("data/analytics/comparison_group.jsonl")
DEFAULT_LINK_PATH = Path("data/analytics/decision_event_group_link.jsonl")
DEFAULT_DECISION_EVENT_PATH = Path("data/curated/decision_event.jsonl")
DEFAULT_OUTPUT_PATH = Path("data/analytics/baseline.jsonl")
DEFAULT_SUMMARY_PATH = Path("data/analytics/baseline_summary.json")
LAPLACE_ALPHA = 1.0
LAPLACE_BETA = 1.0
MIN_RELIABLE_SIZE = 10


@dataclass(frozen=True)
class BaselineRecord:
    baseline_id: str
    comparison_group_id: str
    rule_version: str
    event_count: int
    process_count: int
    favorable_rate: float | None
    low_confidence: bool
    expected_decision_progress_distribution: dict[str, int]
    expected_rapporteur_distribution: dict[str, int]
    expected_judging_body_distribution: dict[str, int]
    expected_progress_by_class: dict[str, dict[str, int]]
    observed_period_start: str | None
    observed_period_end: str | None
    generated_at: str
    notes: str | None
    loo_rapporteur_distributions: dict[str, dict[str, Any]] | None = None


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def _stable_baseline_id(group_id: str) -> str:
    import hashlib

    return "base_" + hashlib.sha256(group_id.encode("utf-8")).hexdigest()[:16]


def _clean_counter(counter: Counter) -> dict[str, int]:
    return {key: int(value) for key, value in counter.items() if key}


def _compute_favorable_rate(events: list[dict[str, Any]]) -> float | None:
    favorable_count = 0
    classifiable_count = 0
    for event in events:
        decision_progress = event.get("decision_progress")
        if not decision_progress:
            continue
        outcome = classify_outcome_raw(str(decision_progress))
        if outcome not in {"favorable", "unfavorable"}:
            continue
        classifiable_count += 1
        if outcome == "favorable":
            favorable_count += 1
    if classifiable_count == 0:
        return None
    return beta_binomial_posterior_mean(
        favorable_count,
        classifiable_count,
        alpha=LAPLACE_ALPHA,
        beta=LAPLACE_BETA,
    )


def _build_notes(*, low_confidence: bool) -> str:
    note = (
        "Baseline derived from valid comparison groups; favorable_rate uses "
        f"Beta-Binomial smoothing (alpha={LAPLACE_ALPHA:.1f}, beta={LAPLACE_BETA:.1f})."
    )
    if low_confidence:
        return f"{note} low_confidence=true because process_count < {MIN_RELIABLE_SIZE}."
    return note


def build_baseline(
    comparison_group_path: Path = DEFAULT_GROUP_PATH,
    link_path: Path = DEFAULT_LINK_PATH,
    decision_event_path: Path = DEFAULT_DECISION_EVENT_PATH,
    output_path: Path = DEFAULT_OUTPUT_PATH,
    summary_path: Path = DEFAULT_SUMMARY_PATH,
    *,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> tuple[Path, Path]:
    if on_progress:
        on_progress(0, 3, "Baseline: Carregando dados...")
    groups = [row for row in _read_jsonl(comparison_group_path) if row.get("status") == "valid"]
    links = _read_jsonl(link_path)
    events = {row["decision_event_id"]: row for row in _read_jsonl(decision_event_path)}

    group_meta = {row["comparison_group_id"]: row for row in groups}
    group_events: dict[str, list[dict[str, Any]]] = defaultdict(list)
    group_processes: dict[str, set[str]] = defaultdict(set)

    for link in links:
        group_id = link["comparison_group_id"]
        event = events.get(link["decision_event_id"])
        if group_id not in group_meta or event is None:
            continue
        group_events[group_id].append(event)
        group_processes[group_id].add(link["process_id"])

    timestamp = datetime.now(timezone.utc).isoformat()
    baseline_records: list[dict[str, Any]] = []
    if on_progress:
        on_progress(1, 3, "Baseline: Calculando métricas...")
    for group_id, meta in group_meta.items():
        event_rows = group_events.get(group_id, [])
        if not event_rows:
            continue
        progress_counter: Counter[str] = Counter()
        rapporteur_counter: Counter[str] = Counter()
        judging_body_counter: Counter[str] = Counter()
        dates: list[str] = []

        for event in event_rows:
            if event.get("decision_progress"):
                progress_counter[str(event["decision_progress"])] += 1
            if event.get("current_rapporteur"):
                rapporteur_counter[str(event["current_rapporteur"])] += 1
            if event.get("judging_body"):
                judging_body_counter[str(event["judging_body"])] += 1
            if event.get("decision_date"):
                dates.append(str(event["decision_date"]))

        # Stratify progress distribution by process_class from group criteria
        progress_by_class: dict[str, dict[str, int]] = {}
        group_class = (meta.get("selection_criteria") or {}).get("process_class")
        if group_class and progress_counter:
            progress_by_class[str(group_class)] = _clean_counter(progress_counter)
        process_count = len(group_processes[group_id])
        low_confidence = process_count < MIN_RELIABLE_SIZE

        # Build leave-one-out distributions per rapporteur
        loo_distributions: dict[str, dict[str, Any]] | None = None
        rapporteur_list = list(rapporteur_counter.keys())
        if len(rapporteur_list) >= 2 and len(event_rows) >= MIN_RELIABLE_SIZE:
            loo_distributions = {}
            for rap in rapporteur_list:
                loo_events = [e for e in event_rows if str(e.get("current_rapporteur") or "") != rap]
                if not loo_events:
                    continue
                loo_progress: Counter[str] = Counter()
                loo_rapporteur: Counter[str] = Counter()
                loo_jb: Counter[str] = Counter()
                for e in loo_events:
                    if e.get("decision_progress"):
                        loo_progress[str(e["decision_progress"])] += 1
                    if e.get("current_rapporteur"):
                        loo_rapporteur[str(e["current_rapporteur"])] += 1
                    if e.get("judging_body"):
                        loo_jb[str(e["judging_body"])] += 1
                loo_distributions[rap] = {
                    "event_count": len(loo_events),
                    "favorable_rate": _compute_favorable_rate(loo_events),
                    "expected_decision_progress_distribution": _clean_counter(loo_progress),
                    "expected_rapporteur_distribution": _clean_counter(loo_rapporteur),
                    "expected_judging_body_distribution": _clean_counter(loo_jb),
                }

        record = asdict(
            BaselineRecord(
                baseline_id=_stable_baseline_id(group_id),
                comparison_group_id=group_id,
                rule_version=meta["rule_version"],
                event_count=len(event_rows),
                process_count=process_count,
                favorable_rate=_compute_favorable_rate(event_rows),
                low_confidence=low_confidence,
                expected_decision_progress_distribution=_clean_counter(progress_counter),
                expected_rapporteur_distribution=_clean_counter(rapporteur_counter),
                expected_judging_body_distribution=_clean_counter(judging_body_counter),
                expected_progress_by_class=progress_by_class,
                observed_period_start=min(dates) if dates else None,
                observed_period_end=max(dates) if dates else None,
                generated_at=timestamp,
                notes=_build_notes(low_confidence=low_confidence),
                loo_rapporteur_distributions=loo_distributions,
            )
        )
        baseline_records.append(record)

    validate_records(baseline_records, BASELINE_SCHEMA)
    if on_progress:
        on_progress(2, 3, "Baseline: Gravando resultados...")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        for record in baseline_records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    summary = {
        "generated_at": timestamp,
        "baseline_count": len(baseline_records),
        "group_count_considered": len(groups),
        "event_count_linked": sum(record["event_count"] for record in baseline_records),
    }
    validate_records([summary], SUMMARY_SCHEMA)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    if on_progress:
        on_progress(3, 3, "Baseline: Concluído")
    return output_path, summary_path
