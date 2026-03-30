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
from ..core.progress import PhaseSpec, ProgressTracker
from ..core.rules import classify_outcome_materiality, classify_outcome_raw
from ..schema_validate import validate_records
from ._atomic_io import AtomicJsonlWriter
from ._match_helpers import (
    build_process_class_map,
    compute_favorable_rate,
    read_jsonl,
)

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path("schemas/counsel_affinity.schema.json")
SUMMARY_SCHEMA_PATH = Path("schemas/counsel_affinity_summary.schema.json")
RED_FLAG_DELTA_THRESHOLD = 0.15
# Minimum shared cases to trigger a red flag.  Set to 5 (vs 3 in
# sanction_match/donation_match) because counsel-affinity measures an
# indirect, pairwise relationship (minister × counsel) whose favorable
# rate is statistically less stable than direct entity matches.  Fewer
# than 5 shared processes produce volatile rates that would generate
# excessive false-positive red flags.
MIN_CASES_FOR_RED_FLAG = 5

# ---------------------------------------------------------------------------
# Institutional counsel classification
# ---------------------------------------------------------------------------
# Primary source: representation_edge.jsonl role_type = "public_attorney",
# bridged to counsel entities via lawyer_entity name matching.
#
# Fallback: name-prefix heuristic for counsel that don't appear in the
# representation data.  Explicitly marked as low-confidence.
# ---------------------------------------------------------------------------

_INSTITUTIONAL_PREFIXES = (
    "PROCURADOR-GERAL",
    "PROCURADOR GERAL",
    "PROCURADORIA",
    "ADVOGADO-GERAL",
    "ADVOGADO GERAL",
    "ADVOCACIA-GERAL",
    "ADVOCACIA GERAL",
    "DEFENSOR PUBLICO",
    "DEFENSOR-GERAL",
    "DEFENSORIA",
    "MINISTERIO PUBLICO",
)


def build_structural_institutional_set(
    representation_edge_path: Path,
    lawyer_entity_path: Path,
) -> set[str]:
    """Build a set of normalized names classified as public_attorney.

    Uses the ``role_type`` field from ``representation_edge.jsonl`` (parsed
    from court record labels, e.g. "PROC.") and bridges to counsel names via
    ``lawyer_entity.jsonl``.  This is a data-driven classification, not a
    name-prefix heuristic.
    """
    public_attorney_ids: set[str] = set()
    if representation_edge_path.exists():
        for rec in read_jsonl(representation_edge_path):
            if rec.get("role_type") == "public_attorney":
                eid = rec.get("representative_entity_id", "")
                if eid:
                    public_attorney_ids.add(eid)

    public_attorney_names: set[str] = set()
    if lawyer_entity_path.exists() and public_attorney_ids:
        for rec in read_jsonl(lawyer_entity_path):
            lid = rec.get("lawyer_id", "")
            if lid in public_attorney_ids:
                name = rec.get("lawyer_name_normalized", "")
                if name:
                    public_attorney_names.add(name.upper())

    return public_attorney_names


def classify_institutional(
    name: str,
    structural_set: set[str],
) -> tuple[bool, str]:
    """Classify whether a counsel is an institutional (mandatory) representative.

    Returns ``(is_institutional, source)`` where source is one of:
    - ``"structural"`` — classified via representation_edge role_type
    - ``"fallback:name_prefix"`` — classified via name prefix heuristic
    - ``"private"`` — not institutional
    """
    upper = name.upper()
    if upper in structural_set:
        return True, "structural"
    if any(upper.startswith(prefix) for prefix in _INSTITUTIONAL_PREFIXES):
        return True, "fallback:name_prefix"
    return False, "private"


def is_institutional_counsel(name: str, structural_set: set[str] | None = None) -> bool:
    """Return True if the counsel is an institutional representative.

    When ``structural_set`` is provided, uses structural classification first.
    Falls back to name-prefix heuristic.
    """
    if structural_set is not None:
        result, _ = classify_institutional(name, structural_set)
        return result
    upper = name.upper()
    return any(upper.startswith(prefix) for prefix in _INSTITUTIONAL_PREFIXES)


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


def _build_rapporteur_pids_index(
    rapporteur_map: dict[str, str],
) -> dict[str, list[str]]:
    """Invert rapporteur_map to rapporteur → list[process_id] (O(n) once)."""
    index: dict[str, list[str]] = defaultdict(list)
    for pid, rap in rapporteur_map.items():
        index[rap].append(pid)
    return dict(index)


def _compute_minister_baseline(
    rapporteur: str,
    rapporteur_pids: dict[str, list[str]],
    process_outcomes: dict[str, list[str]],
    process_class_map: dict[str, str],
    target_classes: frozenset[str],
    cache: dict[tuple[str, frozenset[str]], float | None],
) -> float | None:
    """Compute baseline favorable rate for a minister across given process classes."""
    key = (rapporteur, target_classes)
    if key in cache:
        return cache[key]
    outcomes: list[str] = []
    for pid in rapporteur_pids.get(rapporteur, ()):
        pc = process_class_map.get(pid)
        if pc and pc in target_classes:
            outcomes.extend(process_outcomes.get(pid, []))
    result = compute_favorable_rate(outcomes)
    cache[key] = result
    return result


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
    representation_edge_path: Path = Path("data/curated/representation_edge.jsonl"),
    lawyer_entity_path: Path = Path("data/curated/lawyer_entity.jsonl"),
    output_dir: Path = Path("data/analytics"),
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Build counsel affinity analytics: detect anomalous minister-counsel pairs."""
    output_dir.mkdir(parents=True, exist_ok=True)

    tracker = ProgressTracker(
        phases=[
            PhaseSpec("Affinity: Carregando dados", weight=0.05),
            PhaseSpec("Affinity: Construindo pares", weight=0.05),
            PhaseSpec("Affinity: Analisando pares", weight=0.85),
            PhaseSpec("Affinity: Gravando resultados", weight=0.05),
        ],
        callback=on_progress,
    )

    tracker.begin_phase("Affinity: Carregando dados")
    rapporteur_map = _build_rapporteur_map(decision_event_path)
    rapporteur_pids = _build_rapporteur_pids_index(rapporteur_map)
    counsel_id_to_name = _build_counsel_id_to_name(counsel_path)
    counsel_process_map = _build_counsel_process_map(process_counsel_link_path)
    process_outcomes = _build_process_outcomes_map(decision_event_path)
    process_class_map = build_process_class_map(process_path)
    structural_institutional = build_structural_institutional_set(
        representation_edge_path, lawyer_entity_path,
    )
    logger.info(
        "Institutional classification: %d structural names, prefix fallback active",
        len(structural_institutional),
    )
    minister_baseline_cache: dict[tuple[str, frozenset[str]], float | None] = {}
    tracker.complete_phase()

    # Build pairs: (rapporteur, counsel_id) -> set of process_ids
    tracker.begin_phase("Affinity: Construindo pares", total=len(counsel_process_map), unit="advogados")
    pair_processes: dict[tuple[str, str], set[str]] = defaultdict(set)
    for i, (cid, pids) in enumerate(counsel_process_map.items()):
        if cid not in counsel_id_to_name:
            tracker.advance()
            continue
        for pid in pids:
            rapporteur = rapporteur_map.get(pid)
            if rapporteur:
                pair_processes[(rapporteur, cid)].add(pid)
        tracker.advance()
    tracker.complete_phase()
    tracker.begin_phase("Affinity: Analisando pares", total=len(pair_processes), unit="pares")
    now_iso = datetime.now(timezone.utc).isoformat()
    affinities: list[dict[str, Any]] = []

    for (rapporteur, counsel_id), shared_pids in pair_processes.items():
        tracker.advance()
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
            rapporteur, rapporteur_pids, process_outcomes, process_class_map, frozenset(target_classes),
            minister_baseline_cache,
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
        counsel_name = counsel_id_to_name.get(counsel_id, "")
        institutional, institutional_source = classify_institutional(
            counsel_name, structural_institutional,
        )
        red_flag = (
            max_delta > RED_FLAG_DELTA_THRESHOLD
            and len(shared_pids) >= MIN_CASES_FOR_RED_FLAG
            and not institutional
        )

        # Substantive red flag
        red_flag_substantive: bool | None = None
        if pair_rate_substantive is not None and n_substantive >= MIN_CASES_FOR_RED_FLAG and not institutional:
            sub_delta_minister = (pair_rate_substantive - minister_baseline) if minister_baseline is not None else -1.0
            sub_delta_counsel = (pair_rate_substantive - counsel_baseline) if counsel_baseline is not None else -1.0
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
                "institutional": institutional,
                "institutional_source": institutional_source,
                "institutional_confidence": (
                    "high" if institutional_source == "structural"
                    else "low" if institutional_source == "fallback:name_prefix"
                    else None
                ),
                "top_process_classes": top_classes,
                "generated_at": now_iso,
            }
        )

    tracker.complete_phase()

    tracker.begin_phase("Affinity: Gravando resultados")
    validate_records(affinities, SCHEMA_PATH)
    # Write affinity records
    output_path = output_dir / "counsel_affinity.jsonl"
    with AtomicJsonlWriter(output_path) as fh:
        for a in affinities:
            fh.write(json.dumps(a, ensure_ascii=False) + "\n")

    # Institutional classification metrics
    inst_sources = Counter(a["institutional_source"] for a in affinities)
    total_institutional = sum(1 for a in affinities if a["institutional"])
    structural_count = inst_sources.get("structural", 0)
    fallback_count = inst_sources.get("fallback:name_prefix", 0)
    structural_pct = (structural_count / total_institutional * 100) if total_institutional else 100.0
    fallback_pct = (fallback_count / total_institutional * 100) if total_institutional else 0.0

    FALLBACK_WARN_THRESHOLD = 20.0
    if fallback_pct > FALLBACK_WARN_THRESHOLD:
        logger.warning(
            "Institutional classification: %.1f%% using prefix fallback (%d/%d) — "
            "exceeds %.0f%% threshold. Consider enriching representation_edge coverage.",
            fallback_pct,
            fallback_count,
            total_institutional,
            FALLBACK_WARN_THRESHOLD,
        )

    # Write summary
    summary = {
        "total_pairs_analyzed": len(affinities),
        "red_flag_count": sum(1 for a in affinities if a["red_flag"]),
        "ministers_with_red_flags": len({a["rapporteur"] for a in affinities if a["red_flag"]}),
        "institutional_classification": {
            "total_institutional": total_institutional,
            "structural": structural_count,
            "fallback_prefix": fallback_count,
            "private": inst_sources.get("private", 0),
            "structural_coverage_pct": round(structural_pct, 1),
            "fallback_pct": round(fallback_pct, 1),
        },
        "generated_at": now_iso,
    }
    validate_records([summary], SUMMARY_SCHEMA_PATH)
    summary_path = output_dir / "counsel_affinity_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info(
        "Built counsel affinity: %d pairs (%d red flags, %d institutional [%d structural, %d prefix fallback])",
        len(affinities),
        summary["red_flag_count"],
        total_institutional,
        structural_count,
        fallback_count,
    )
    tracker.complete_phase()
    return output_path
