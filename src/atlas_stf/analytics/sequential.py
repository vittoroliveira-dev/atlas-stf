"""Sequential analysis: autocorrelation and streak effects in judicial decisions."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.rules import classify_outcome_raw
from ..core.stats import autocorrelation_lag1
from ..schema_validate import validate_records
from ._atomic_io import AtomicJsonlWriter
from ._match_io import read_jsonl as _read_jsonl

DEFAULT_DECISION_EVENT_PATH = Path("data/curated/decision_event.jsonl")
DEFAULT_OUTPUT_DIR = Path("data/analytics")
SCHEMA_PATH = Path("schemas/sequential_analysis.schema.json")
SUMMARY_SCHEMA_PATH = Path("schemas/sequential_analysis_summary.schema.json")
MIN_DECISIONS_FOR_ANALYSIS = 50


@dataclass(frozen=True)
class SequentialAnalysisRecord:
    rapporteur: str
    decision_year: int
    n_decisions: int
    n_favorable: int
    n_unfavorable: int
    autocorrelation_lag1: float
    streak_effect_3: float | None
    streak_effect_5: float | None
    base_favorable_rate: float
    post_streak_favorable_rate_3: float | None
    post_streak_favorable_rate_5: float | None
    sequential_bias_flag: bool


def _classify_outcome(decision_progress: str | None) -> int | None:
    """Map decision_progress to binary: 1=favorable, 0=unfavorable, None=excluded."""
    if not decision_progress:
        return None
    outcome = classify_outcome_raw(decision_progress)
    if outcome == "favorable":
        return 1
    if outcome == "unfavorable":
        return 0
    return None


def _streak_effect(series: list[int], streak_len: int) -> tuple[float | None, float | None]:
    """Compute P(favorable | last streak_len were favorable) and the difference from base rate.

    Returns (streak_effect, post_streak_rate) or (None, None) if insufficient data.
    """
    if len(series) < streak_len + 1:
        return None, None

    base_favorable = sum(series) / len(series)
    post_streak_count = 0
    post_streak_favorable = 0

    for i in range(streak_len, len(series)):
        window = series[i - streak_len : i]
        if all(v == 1 for v in window):
            post_streak_count += 1
            if series[i] == 1:
                post_streak_favorable += 1

    if post_streak_count < 3:
        return None, None

    post_rate = post_streak_favorable / post_streak_count
    effect = round(post_rate - base_favorable, 6)
    return effect, round(post_rate, 6)


def build_sequential_analysis(
    *,
    decision_event_path: Path = DEFAULT_DECISION_EVENT_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    min_decisions: int = MIN_DECISIONS_FOR_ANALYSIS,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    if on_progress:
        on_progress(0, 3, "Sequential: Carregando dados...")
    events = _read_jsonl(decision_event_path)

    if on_progress:
        on_progress(1, 3, "Sequential: Analisando sequências...")
    groups: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)

    for event in events:
        rapporteur = event.get("current_rapporteur")
        year = event.get("decision_year")
        date = event.get("decision_date")
        progress = event.get("decision_progress")
        if not rapporteur or not year or not date or not progress:
            continue
        outcome = _classify_outcome(str(progress))
        if outcome is None:
            continue
        groups[(str(rapporteur), int(year))].append(
            {
                "decision_date": str(date),
                "decision_event_id": str(event.get("decision_event_id") or ""),
                "outcome": outcome,
            }
        )

    records: list[dict[str, Any]] = []
    for (rapporteur, year), group_events in groups.items():
        if len(group_events) < min_decisions:
            continue

        sorted_events = sorted(group_events, key=lambda x: (x["decision_date"], x["decision_event_id"]))
        series = [e["outcome"] for e in sorted_events]
        n_favorable = sum(series)
        n_unfavorable = len(series) - n_favorable
        base_rate = round(n_favorable / len(series), 6)

        ac = autocorrelation_lag1(series)
        effect_3, post_rate_3 = _streak_effect(series, 3)
        effect_5, post_rate_5 = _streak_effect(series, 5)

        bias_flag = abs(ac) > 0.1

        records.append(
            asdict(
                SequentialAnalysisRecord(
                    rapporteur=rapporteur,
                    decision_year=year,
                    n_decisions=len(series),
                    n_favorable=n_favorable,
                    n_unfavorable=n_unfavorable,
                    autocorrelation_lag1=ac,
                    streak_effect_3=effect_3,
                    streak_effect_5=effect_5,
                    base_favorable_rate=base_rate,
                    post_streak_favorable_rate_3=post_rate_3,
                    post_streak_favorable_rate_5=post_rate_5,
                    sequential_bias_flag=bias_flag,
                )
            )
        )

    if on_progress:
        on_progress(2, 3, "Sequential: Gravando resultados...")
    validate_records(records, SCHEMA_PATH)

    output_path = output_dir / "sequential_analysis.jsonl"
    with AtomicJsonlWriter(output_path) as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_analyses": len(records),
        "bias_flagged_count": sum(1 for r in records if r["sequential_bias_flag"]),
        "min_decisions": min_decisions,
    }
    validate_records([summary], SUMMARY_SCHEMA_PATH)
    summary_path = output_dir / "sequential_analysis_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    if on_progress:
        on_progress(3, 3, "Sequential: Concluído")
    return output_path
