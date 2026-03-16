"""Audit randomness of case assignment (rapporteur distribution)."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.stats import chi_square_p_value_approx, chi_square_statistic
from ..schema_validate import validate_records
from ._atomic_io import AtomicJsonlWriter

DEFAULT_DECISION_EVENT_PATH = Path("data/curated/decision_event.jsonl")
DEFAULT_PROCESS_PATH = Path("data/curated/process.jsonl")
DEFAULT_OUTPUT_DIR = Path("data/analytics")
SCHEMA_PATH = Path("schemas/assignment_audit.schema.json")
SUMMARY_SCHEMA_PATH = Path("schemas/assignment_audit_summary.schema.json")
MIN_EVENTS_FOR_AUDIT = 50


@dataclass(frozen=True)
class AssignmentAuditRecord:
    process_class: str
    decision_year: int
    rapporteur_count: int
    event_count: int
    rapporteur_distribution: dict[str, int]
    chi2_statistic: float
    p_value_approx: float
    uniformity_flag: bool
    most_overrepresented_rapporteur: str | None
    most_underrepresented_rapporteur: str | None


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def _process_class_by_process_id(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    result: dict[str, str] = {}
    for row in _read_jsonl(path):
        process_id = str(row.get("process_id") or "").strip()
        process_class = str(row.get("process_class") or "").strip()
        if process_id and process_class:
            result[process_id] = process_class
    return result


def build_assignment_audit(
    *,
    decision_event_path: Path = DEFAULT_DECISION_EVENT_PATH,
    process_path: Path = DEFAULT_PROCESS_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    min_events: int = MIN_EVENTS_FOR_AUDIT,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    if on_progress:
        on_progress(0, 3, "Assignment: Carregando dados...")
    events = _read_jsonl(decision_event_path)
    process_classes = _process_class_by_process_id(process_path)

    if on_progress:
        on_progress(1, 3, "Assignment: Auditando distribuição...")
    groups: dict[tuple[str, int], Counter[str]] = defaultdict(Counter)

    for event in events:
        process_id = str(event.get("process_id") or "").strip()
        pc = event.get("process_class") or process_classes.get(process_id)
        year = event.get("decision_year")
        rapporteur = event.get("current_rapporteur")
        if not pc or not year or not rapporteur:
            continue
        groups[(str(pc), int(year))][str(rapporteur)] += 1

    records: list[dict[str, Any]] = []
    for (pc, year), counter in groups.items():
        total = sum(counter.values())
        if total < min_events:
            continue
        n_rapporteurs = len(counter)
        if n_rapporteurs < 2:
            continue

        expected_per = total / n_rapporteurs
        observed = [float(v) for v in counter.values()]
        expected = [expected_per] * n_rapporteurs

        chi2 = chi_square_statistic(observed, expected)
        df = n_rapporteurs - 1
        p_val = chi_square_p_value_approx(chi2, df)
        uniform = p_val > 0.05

        sorted_rapporteurs = sorted(counter.items(), key=lambda x: x[1], reverse=True)
        most_over = sorted_rapporteurs[0][0] if sorted_rapporteurs else None
        most_under = sorted_rapporteurs[-1][0] if sorted_rapporteurs else None

        records.append(
            asdict(
                AssignmentAuditRecord(
                    process_class=pc,
                    decision_year=year,
                    rapporteur_count=n_rapporteurs,
                    event_count=total,
                    rapporteur_distribution=dict(counter),
                    chi2_statistic=chi2,
                    p_value_approx=p_val,
                    uniformity_flag=uniform,
                    most_overrepresented_rapporteur=most_over,
                    most_underrepresented_rapporteur=most_under,
                )
            )
        )

    if on_progress:
        on_progress(2, 3, "Assignment: Gravando resultados...")
    validate_records(records, SCHEMA_PATH)

    output_path = output_dir / "assignment_audit.jsonl"
    with AtomicJsonlWriter(output_path) as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_audits": len(records),
        "uniform_count": sum(1 for r in records if r["uniformity_flag"]),
        "non_uniform_count": sum(1 for r in records if not r["uniformity_flag"]),
        "min_events": min_events,
    }
    validate_records([summary], SUMMARY_SCHEMA_PATH)
    summary_path = output_dir / "assignment_audit_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    if on_progress:
        on_progress(3, 3, "Assignment: Concluído")
    return output_path
