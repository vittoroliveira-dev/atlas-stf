"""Build counsel affinity analytics: detect anomalous minister-counsel pairs."""

from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.identity import stable_id
from ..core.rules import classify_outcome_materiality, classify_outcome_raw
from ._match_helpers import (
    build_process_class_map,
    compute_favorable_rate,
    read_jsonl,
)

logger = logging.getLogger(__name__)

RED_FLAG_DELTA_THRESHOLD = 0.15
MIN_CASES_FOR_RED_FLAG = 5


def _build_rapporteur_map(decision_event_path: Path) -> dict[str, str]:
    """Map process_id -> rapporteur from the latest dated event."""
    result: dict[str, str] = {}
    latest_keys: dict[str, tuple[str, str, int]] = {}
    for position, record in enumerate(read_jsonl(decision_event_path)):
        pid = record.get("process_id")
        rapporteur = record.get("current_rapporteur")
        if pid and rapporteur:
            event_key = (
                str(record.get("decision_date") or ""),
                str(record.get("decision_event_id") or ""),
                position,
            )
            current_key = latest_keys.get(pid)
            if current_key is None or event_key >= current_key:
                latest_keys[pid] = event_key
                result[pid] = rapporteur
    return result


def _build_counsel_id_to_name(counsel_path: Path) -> dict[str, str]:
    """Map counsel_id -> counsel_name_normalized."""
    result: dict[str, str] = {}
    for record in read_jsonl(counsel_path):
        cid = record.get("counsel_id", "")
        name = record.get("counsel_name_normalized", "")
        if cid and name:
            result[cid] = name
    return result


def _build_counsel_process_map(
    process_counsel_link_path: Path,
) -> dict[str, list[str]]:
    """Map counsel_id -> list of process_ids."""
    result: dict[str, list[str]] = defaultdict(list)
    for record in read_jsonl(process_counsel_link_path):
        cid = record.get("counsel_id", "")
        pid = record.get("process_id", "")
        if cid and pid:
            result[cid].append(pid)
    return dict(result)


def _build_process_outcomes_map(
    decision_event_path: Path,
) -> dict[str, list[str]]:
    """Map process_id -> list of decision_progress values."""
    result: dict[str, list[str]] = defaultdict(list)
    for record in read_jsonl(decision_event_path):
        pid = record.get("process_id")
        progress = record.get("decision_progress")
        if pid and progress:
            result[pid].append(progress)
    return dict(result)


def _compute_minister_baseline(
    rapporteur: str,
    rapporteur_map: dict[str, str],
    process_outcomes: dict[str, list[str]],
    process_class_map: dict[str, str],
    target_classes: set[str],
) -> float | None:
    """Compute baseline favorable rate for a minister across given process classes."""
    outcomes: list[str] = []
    for pid, rap in rapporteur_map.items():
        if rap != rapporteur:
            continue
        pc = process_class_map.get(pid)
        if pc and pc in target_classes:
            outcomes.extend(process_outcomes.get(pid, []))
    return compute_favorable_rate(outcomes)


def _compute_counsel_baseline(
    counsel_id: str,
    counsel_process_map: dict[str, list[str]],
    process_outcomes: dict[str, list[str]],
    process_class_map: dict[str, str],
    target_classes: set[str],
) -> float | None:
    """Compute baseline favorable rate for a counsel across given process classes."""
    outcomes: list[str] = []
    for pid in counsel_process_map.get(counsel_id, []):
        pc = process_class_map.get(pid)
        if pc and pc in target_classes:
            outcomes.extend(process_outcomes.get(pid, []))
    return compute_favorable_rate(outcomes)


def build_counsel_affinity(
    *,
    decision_event_path: Path = Path("data/curated/decision_event.jsonl"),
    process_path: Path = Path("data/curated/process.jsonl"),
    counsel_path: Path = Path("data/curated/counsel.jsonl"),
    process_counsel_link_path: Path = Path("data/curated/process_counsel_link.jsonl"),
    output_dir: Path = Path("data/analytics"),
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Build counsel affinity analytics: detect anomalous minister-counsel pairs."""
    output_dir.mkdir(parents=True, exist_ok=True)

    if on_progress:
        on_progress(0, 3, "Affinity: Carregando dados...")
    rapporteur_map = _build_rapporteur_map(decision_event_path)
    counsel_id_to_name = _build_counsel_id_to_name(counsel_path)
    counsel_process_map = _build_counsel_process_map(process_counsel_link_path)
    process_outcomes = _build_process_outcomes_map(decision_event_path)
    process_class_map = build_process_class_map(process_path)

    # Build pairs: (rapporteur, counsel_id) -> set of process_ids
    pair_processes: dict[tuple[str, str], set[str]] = defaultdict(set)
    for cid, pids in counsel_process_map.items():
        if cid not in counsel_id_to_name:
            continue
        for pid in pids:
            rapporteur = rapporteur_map.get(pid)
            if rapporteur:
                pair_processes[(rapporteur, cid)].add(pid)

    if on_progress:
        on_progress(1, 3, "Affinity: Analisando pares...")
    now_iso = datetime.now(timezone.utc).isoformat()
    affinities: list[dict[str, Any]] = []

    for (rapporteur, counsel_id), shared_pids in pair_processes.items():
        if len(shared_pids) < 2:
            continue

        # Compute pair favorable rate
        pair_outcomes: list[str] = []
        pair_classes: list[str] = []
        for pid in shared_pids:
            pair_outcomes.extend(process_outcomes.get(pid, []))
            pc = process_class_map.get(pid)
            if pc:
                pair_classes.append(pc)

        pair_rate = compute_favorable_rate(pair_outcomes)
        if pair_rate is None:
            continue

        # Substantive rate: only decisions with materiality == "substantive"
        substantive_outcomes = [o for o in pair_outcomes if classify_outcome_materiality(o) == "substantive"]
        pair_rate_substantive = compute_favorable_rate(substantive_outcomes) if substantive_outcomes else None
        n_substantive = len(substantive_outcomes)

        # Count favorable/unfavorable
        favorable_count = 0
        unfavorable_count = 0
        for outcome in pair_outcomes:
            cls = classify_outcome_raw(outcome)
            if cls == "favorable":
                favorable_count += 1
            elif cls == "unfavorable":
                unfavorable_count += 1

        target_classes = set(pair_classes)
        if not target_classes:
            continue

        minister_baseline = _compute_minister_baseline(
            rapporteur, rapporteur_map, process_outcomes, process_class_map, target_classes
        )
        counsel_baseline = _compute_counsel_baseline(
            counsel_id, counsel_process_map, process_outcomes, process_class_map, target_classes
        )

        delta_vs_minister: float | None = None
        delta_vs_counsel: float | None = None
        if minister_baseline is not None:
            delta_vs_minister = pair_rate - minister_baseline
        if counsel_baseline is not None:
            delta_vs_counsel = pair_rate - counsel_baseline

        # Red flag logic
        max_delta = max(
            delta_vs_minister if delta_vs_minister is not None else -1.0,
            delta_vs_counsel if delta_vs_counsel is not None else -1.0,
        )
        red_flag = max_delta > RED_FLAG_DELTA_THRESHOLD and len(shared_pids) >= MIN_CASES_FOR_RED_FLAG

        # Substantive red flag
        red_flag_substantive: bool | None = None
        if pair_rate_substantive is not None and n_substantive >= MIN_CASES_FOR_RED_FLAG:
            sub_delta_minister = (
                (pair_rate_substantive - minister_baseline) if minister_baseline is not None else -1.0
            )
            sub_delta_counsel = (
                (pair_rate_substantive - counsel_baseline) if counsel_baseline is not None else -1.0
            )
            max_sub_delta = max(sub_delta_minister, sub_delta_counsel)
            red_flag_substantive = max_sub_delta > RED_FLAG_DELTA_THRESHOLD

        # Top process classes
        class_counter = Counter(pair_classes)
        top_classes = [cls for cls, _ in class_counter.most_common(5)]

        affinity_id = stable_id("ca-", f"{rapporteur}:{counsel_id}")
        affinities.append(
            {
                "affinity_id": affinity_id,
                "rapporteur": rapporteur,
                "counsel_id": counsel_id,
                "counsel_name_normalized": counsel_id_to_name.get(counsel_id, ""),
                "shared_case_count": len(shared_pids),
                "favorable_count": favorable_count,
                "unfavorable_count": unfavorable_count,
                "pair_favorable_rate": pair_rate,
                "pair_favorable_rate_substantive": pair_rate_substantive,
                "substantive_decision_count": n_substantive,
                "minister_baseline_favorable_rate": minister_baseline,
                "counsel_baseline_favorable_rate": counsel_baseline,
                "pair_delta_vs_minister": delta_vs_minister,
                "pair_delta_vs_counsel": delta_vs_counsel,
                "red_flag": red_flag,
                "red_flag_substantive": red_flag_substantive,
                "top_process_classes": top_classes,
                "generated_at": now_iso,
            }
        )

    if on_progress:
        on_progress(2, 3, "Affinity: Gravando resultados...")
    # Write affinity records
    output_path = output_dir / "counsel_affinity.jsonl"
    with output_path.open("w", encoding="utf-8") as fh:
        for a in affinities:
            fh.write(json.dumps(a, ensure_ascii=False) + "\n")

    # Write summary
    summary = {
        "total_pairs_analyzed": len(affinities),
        "red_flag_count": sum(1 for a in affinities if a["red_flag"]),
        "ministers_with_red_flags": len({a["rapporteur"] for a in affinities if a["red_flag"]}),
        "generated_at": now_iso,
    }
    summary_path = output_dir / "counsel_affinity_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info(
        "Built counsel affinity: %d pairs (%d red flags)",
        len(affinities),
        summary["red_flag_count"],
    )
    if on_progress:
        on_progress(3, 3, "Affinity: Concluído")
    return output_path
