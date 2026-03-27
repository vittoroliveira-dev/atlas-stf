"""Build rapporteur decision profiles with chi-square deviation tests."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.rules import classify_outcome_raw, derive_thematic_key
from ..core.stats import chi_square_p_value_approx, chi_square_statistic
from ..schema_validate import validate_records
from ._atomic_io import AtomicJsonlWriter
from ._match_io import read_jsonl as _read_jsonl

# Process classes of Plenário original jurisdiction that should be collegiate
EXPECTED_COLLEGIATE_CLASSES: frozenset[str] = frozenset({"ADI", "ADC", "ADPF", "ADO"})
MONOCRATIC_DELTA_THRESHOLD = 0.10

DEFAULT_DECISION_EVENT_PATH = Path("data/curated/decision_event.jsonl")
DEFAULT_PROCESS_PATH = Path("data/curated/process.jsonl")
DEFAULT_OUTPUT_DIR = Path("data/analytics")
SCHEMA_PATH = Path("schemas/rapporteur_profile.schema.json")
SUMMARY_SCHEMA_PATH = Path("schemas/rapporteur_profile_summary.schema.json")
MIN_GROUP_SIZE = 30


@dataclass(frozen=True)
class RapporteurProfileRecord:
    rapporteur: str
    process_class: str
    thematic_key: str
    decision_year: int
    event_count: int
    progress_distribution: dict[str, int]
    group_progress_distribution: dict[str, int]
    group_event_count: int
    chi2_statistic: float | None
    p_value_approx: float | None
    deviation_flag: bool
    deviation_direction: str
    monocratic_event_count: int = 0
    monocratic_favorable_count: int = 0
    monocratic_unfavorable_count: int = 0
    collegiate_event_count: int = 0
    collegiate_favorable_count: int = 0
    collegiate_unfavorable_count: int = 0
    monocratic_favorable_rate: float | None = None
    collegiate_favorable_rate: float | None = None
    monocratic_blocking_flag: bool = False


def _load_process_context(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    context: dict[str, dict[str, str]] = {}
    for row in _read_jsonl(path):
        pid = str(row.get("process_id") or "").strip()
        if not pid:
            continue
        pc = str(row.get("process_class") or "").strip()
        subjects = row.get("subjects_normalized")
        branch = row.get("branch_of_law")
        tk = derive_thematic_key(
            subjects if isinstance(subjects, list) else None,
            branch,
            fallback="",
        )
        if pc or tk:
            context[pid] = {"process_class": pc, "thematic_key": tk}
    return context


def _deviation_direction(
    rapporteur_dist: dict[str, int],
    group_dist: dict[str, int],
    rapporteur_total: int,
    group_total: int,
) -> str:
    if not rapporteur_dist or rapporteur_total == 0 or group_total == 0:
        return "indeterminado"
    max_delta_key = ""
    max_delta = 0.0
    for key in rapporteur_dist:
        rap_rate = rapporteur_dist[key] / rapporteur_total
        grp_rate = group_dist.get(key, 0) / group_total
        delta = rap_rate - grp_rate
        if abs(delta) > abs(max_delta):
            max_delta = delta
            max_delta_key = key
    if max_delta > 0:
        return f"sobre-representado em {max_delta_key}"
    if max_delta < 0:
        return f"sub-representado em {max_delta_key}"
    return "indeterminado"


def build_rapporteur_profiles(
    *,
    decision_event_path: Path = DEFAULT_DECISION_EVENT_PATH,
    process_path: Path = DEFAULT_PROCESS_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    min_group_size: int = MIN_GROUP_SIZE,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    if on_progress:
        on_progress(0, 3, "Rapporteur: Carregando dados...")
    events = _read_jsonl(decision_event_path)
    process_ctx = _load_process_context(process_path)

    if on_progress:
        on_progress(1, 3, "Rapporteur: Analisando perfis...")
    groups: dict[tuple[str, str, int], list[dict[str, Any]]] = defaultdict(list)

    for event in events:
        pid = str(event.get("process_id") or "").strip()
        ctx = process_ctx.get(pid, {})
        pc = ctx.get("process_class", "")
        tk = ctx.get("thematic_key", "")
        year = event.get("decision_year")
        rapporteur = event.get("current_rapporteur")
        progress = event.get("decision_progress")
        if not pc or not year or not rapporteur or not progress:
            continue
        groups[(pc, tk or "INCERTO", int(year))].append(event)

    records: list[dict[str, Any]] = []
    for (pc, tk, year), group_events in groups.items():
        if len(group_events) < min_group_size:
            continue

        group_progress: Counter[str] = Counter()
        rapporteur_events: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for evt in group_events:
            prog = str(evt["decision_progress"])
            rap = str(evt["current_rapporteur"])
            group_progress[prog] += 1
            rapporteur_events[rap].append(evt)

        group_total = len(group_events)
        all_progress_keys = sorted(group_progress.keys())

        for rap, rap_evts in rapporteur_events.items():
            rap_progress: Counter[str] = Counter(str(e["decision_progress"]) for e in rap_evts)
            rap_total = len(rap_evts)

            # Monocratic vs collegiate outcome analysis
            mono_fav = 0
            mono_unfav = 0
            mono_total = 0
            col_fav = 0
            col_unfav = 0
            col_total = 0
            for evt in rap_evts:
                outcome = classify_outcome_raw(str(evt.get("decision_progress") or ""))
                is_collegiate = evt.get("is_collegiate")
                if is_collegiate is False:
                    mono_total += 1
                    if outcome == "favorable":
                        mono_fav += 1
                    elif outcome == "unfavorable":
                        mono_unfav += 1
                elif is_collegiate is True:
                    col_total += 1
                    if outcome == "favorable":
                        col_fav += 1
                    elif outcome == "unfavorable":
                        col_unfav += 1

            mono_classifiable = mono_fav + mono_unfav
            col_classifiable = col_fav + col_unfav
            mono_rate = mono_fav / mono_classifiable if mono_classifiable > 0 else None
            col_rate = col_fav / col_classifiable if col_classifiable > 0 else None

            # Flag if monocratic favorable rate exceeds collegiate by threshold
            mono_blocking = False
            if (
                mono_rate is not None
                and col_rate is not None
                and mono_classifiable >= 5
                and col_classifiable >= 5
                and (mono_rate - col_rate) > MONOCRATIC_DELTA_THRESHOLD
            ):
                mono_blocking = True
            # Also flag if process class should be collegiate but decided monocratically
            if pc in EXPECTED_COLLEGIATE_CLASSES and mono_total > 0 and col_total == 0:
                mono_blocking = True

            if rap_total < 5 or len(all_progress_keys) < 2:
                records.append(
                    asdict(
                        RapporteurProfileRecord(
                            rapporteur=rap,
                            process_class=pc,
                            thematic_key=tk,
                            decision_year=year,
                            event_count=rap_total,
                            progress_distribution=dict(rap_progress),
                            group_progress_distribution=dict(group_progress),
                            group_event_count=group_total,
                            chi2_statistic=None,
                            p_value_approx=None,
                            deviation_flag=False,
                            deviation_direction="insuficiente",
                            monocratic_event_count=mono_total,
                            monocratic_favorable_count=mono_fav,
                            monocratic_unfavorable_count=mono_unfav,
                            collegiate_event_count=col_total,
                            collegiate_favorable_count=col_fav,
                            collegiate_unfavorable_count=col_unfav,
                            monocratic_favorable_rate=mono_rate,
                            collegiate_favorable_rate=col_rate,
                            monocratic_blocking_flag=mono_blocking,
                        )
                    )
                )
                continue

            observed = [float(rap_progress.get(k, 0)) for k in all_progress_keys]
            expected = [float(group_progress[k]) * rap_total / group_total for k in all_progress_keys]

            # Skip categories with zero expected
            filtered_obs = []
            filtered_exp = []
            for o, e in zip(observed, expected):
                if e > 0:
                    filtered_obs.append(o)
                    filtered_exp.append(e)

            if len(filtered_obs) < 2:
                chi2, p_val, flag = None, None, False
            else:
                chi2 = chi_square_statistic(filtered_obs, filtered_exp)
                df = len(filtered_obs) - 1
                p_val = chi_square_p_value_approx(chi2, df)
                flag = p_val <= 0.01

            direction = (
                _deviation_direction(
                    dict(rap_progress),
                    dict(group_progress),
                    rap_total,
                    group_total,
                )
                if flag
                else "sem desvio significativo"
            )

            records.append(
                asdict(
                    RapporteurProfileRecord(
                        rapporteur=rap,
                        process_class=pc,
                        thematic_key=tk,
                        decision_year=year,
                        event_count=rap_total,
                        progress_distribution=dict(rap_progress),
                        group_progress_distribution=dict(group_progress),
                        group_event_count=group_total,
                        chi2_statistic=chi2,
                        p_value_approx=p_val,
                        deviation_flag=flag,
                        deviation_direction=direction,
                        monocratic_event_count=mono_total,
                        monocratic_favorable_count=mono_fav,
                        monocratic_unfavorable_count=mono_unfav,
                        collegiate_event_count=col_total,
                        collegiate_favorable_count=col_fav,
                        collegiate_unfavorable_count=col_unfav,
                        monocratic_favorable_rate=mono_rate,
                        collegiate_favorable_rate=col_rate,
                        monocratic_blocking_flag=mono_blocking,
                    )
                )
            )

    if on_progress:
        on_progress(2, 3, "Rapporteur: Gravando resultados...")
    validate_records(records, SCHEMA_PATH)

    output_path = output_dir / "rapporteur_profile.jsonl"
    with AtomicJsonlWriter(output_path) as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_profiles": len(records),
        "deviation_count": sum(1 for r in records if r["deviation_flag"]),
        "min_group_size": min_group_size,
    }
    validate_records([summary], SUMMARY_SCHEMA_PATH)
    summary_path = output_dir / "rapporteur_profile_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    if on_progress:
        on_progress(3, 3, "Rapporteur: Concluído")
    return output_path
