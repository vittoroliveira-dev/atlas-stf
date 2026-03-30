"""Build rapporteur change analytics: detect redistribution patterns."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.identity import stable_id
from ..schema_validate import validate_records
from ._atomic_io import AtomicJsonlWriter
from ._match_io import read_jsonl
from ._outcome_helpers import build_process_class_map, compute_favorable_rate

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path("schemas/rapporteur_change.schema.json")
SUMMARY_SCHEMA_PATH = Path("schemas/rapporteur_change_summary.schema.json")
DEFAULT_DECISION_EVENT_PATH = Path("data/curated/decision_event.jsonl")
DEFAULT_PROCESS_PATH = Path("data/curated/process.jsonl")
DEFAULT_OUTPUT_DIR = Path("data/analytics")


def _build_process_events(
    decision_event_path: Path,
) -> dict[str, list[dict[str, Any]]]:
    """Group decision events by process_id, sorted by date."""
    process_events: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in read_jsonl(decision_event_path):
        pid = record.get("process_id")
        if not pid:
            continue
        process_events[str(pid)].append(record)

    for events in process_events.values():
        events.sort(
            key=lambda e: (
                str(e.get("decision_date") or ""),
                str(e.get("decision_event_id") or ""),
            )
        )
    return dict(process_events)


RED_FLAG_DELTA_THRESHOLD = 0.15
MIN_CASES_FOR_RED_FLAG = 3


def build_rapporteur_changes(
    *,
    decision_event_path: Path = DEFAULT_DECISION_EVENT_PATH,
    process_path: Path = DEFAULT_PROCESS_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Build rapporteur change analytics: detect redistribution + post-change favorability."""
    output_dir.mkdir(parents=True, exist_ok=True)

    if on_progress:
        on_progress(0, 3, "Rapporteur Change: Carregando dados...")

    process_events = _build_process_events(decision_event_path)
    process_classes = build_process_class_map(process_path)

    if on_progress:
        on_progress(1, 3, "Rapporteur Change: Detectando mudanças...")

    now_iso = datetime.now(timezone.utc).isoformat()
    records: list[dict[str, Any]] = []

    # Also collect per-rapporteur baselines for post-change comparison
    rapporteur_outcomes: dict[str, list[str]] = defaultdict(list)
    for events in process_events.values():
        for event in events:
            rapporteur = event.get("current_rapporteur")
            progress = event.get("decision_progress")
            if rapporteur and progress:
                rapporteur_outcomes[str(rapporteur)].append(str(progress))

    rapporteur_baselines: dict[str, float | None] = {
        rap: compute_favorable_rate(outcomes) for rap, outcomes in rapporteur_outcomes.items()
    }

    for pid, events in process_events.items():
        if len(events) < 2:
            continue

        prev_rapporteur: str | None = None
        for i, event in enumerate(events):
            curr_rapporteur = event.get("current_rapporteur")
            if not curr_rapporteur:
                continue

            if prev_rapporteur and curr_rapporteur != prev_rapporteur:
                # Rapporteur changed!
                # Compute post-change outcomes for new rapporteur in this process
                post_change_outcomes: list[str] = []
                for subsequent_event in events[i:]:
                    progress = subsequent_event.get("decision_progress")
                    if progress and subsequent_event.get("current_rapporteur") == curr_rapporteur:
                        post_change_outcomes.append(str(progress))

                post_change_rate = compute_favorable_rate(post_change_outcomes)
                new_rapporteur_baseline = rapporteur_baselines.get(str(curr_rapporteur))

                delta_vs_baseline: float | None = None
                if post_change_rate is not None and new_rapporteur_baseline is not None:
                    delta_vs_baseline = post_change_rate - new_rapporteur_baseline

                red_flag = (
                    delta_vs_baseline is not None
                    and delta_vs_baseline > RED_FLAG_DELTA_THRESHOLD
                    and len(post_change_outcomes) >= MIN_CASES_FOR_RED_FLAG
                )

                decision_event_id = str(event.get("decision_event_id") or "")
                change_id = stable_id(
                    "rchg-",
                    f"{pid}:{prev_rapporteur}:{curr_rapporteur}:{event.get('decision_date', '')}:{decision_event_id}",
                )
                records.append(
                    {
                        "change_id": change_id,
                        "process_id": pid,
                        "process_class": process_classes.get(pid),
                        "previous_rapporteur": str(prev_rapporteur),
                        "new_rapporteur": str(curr_rapporteur),
                        "change_date": event.get("decision_date"),
                        "decision_event_id": decision_event_id,
                        "post_change_decision_count": len(post_change_outcomes),
                        "post_change_favorable_rate": post_change_rate,
                        "new_rapporteur_baseline_rate": new_rapporteur_baseline,
                        "delta_vs_baseline": (round(delta_vs_baseline, 6) if delta_vs_baseline is not None else None),
                        "red_flag": red_flag,
                        "generated_at": now_iso,
                    }
                )

            prev_rapporteur = curr_rapporteur

    if on_progress:
        on_progress(2, 3, "Rapporteur Change: Gravando resultados...")

    validate_records(records, SCHEMA_PATH)

    output_path = output_dir / "rapporteur_change.jsonl"
    with AtomicJsonlWriter(output_path) as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    red_flag_count = sum(1 for r in records if r["red_flag"])
    summary = {
        "generated_at": now_iso,
        "total_changes": len(records),
        "red_flag_count": red_flag_count,
        "processes_with_changes": len({r["process_id"] for r in records}),
    }
    validate_records([summary], SUMMARY_SCHEMA_PATH)
    summary_path = output_dir / "rapporteur_change_summary.json"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    logger.info(
        "Built rapporteur change: %d changes (%d red flags)",
        len(records),
        red_flag_count,
    )
    if on_progress:
        on_progress(3, 3, "Rapporteur Change: Concluído")
    return output_path
