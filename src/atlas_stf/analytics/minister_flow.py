"""Build monthly decision-event flow summaries for a minister."""

from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from ..core.rules import derive_thematic_key
from ..schema_validate import validate_records

FLOW_SCHEMA = Path("schemas/minister_flow.schema.json")
DEFAULT_DECISION_EVENT_PATH = Path("data/curated/decision_event.jsonl")
DEFAULT_ALERT_PATH = Path("data/analytics/outlier_alert.jsonl")
DEFAULT_PROCESS_PATH = Path("data/curated/process.jsonl")
THEMATIC_KEY_RULE = "first_subject_normalized_else_branch_of_law"
MIN_THEMATIC_EVENTS_FOR_COMPARATIVE_READ = 5
MIN_THEMATIC_ACTIVE_DAYS_FOR_COMPARATIVE_READ = 3
MIN_THEMATIC_HISTORICAL_EVENTS_FOR_COMPARATIVE_READ = 20


@dataclass(frozen=True)
class DailyCount:
    decision_date: str
    event_count: int
    delta_vs_historical_average: float | None
    ratio_vs_historical_average: float | None


@dataclass(frozen=True)
class SegmentFlowRecord:
    segment_value: str
    event_count: int
    process_count: int
    active_day_count: int
    historical_event_count: int
    historical_active_day_count: int
    historical_average_events_per_active_day: float | None
    daily_counts: list[dict[str, Any]]


@dataclass(frozen=True)
class MinisterFlowRecord:
    minister_query: str
    minister_match_mode: str
    minister_reference: str
    period: str
    collegiate_filter: str
    status: str
    event_count: int
    process_count: int
    active_day_count: int
    first_decision_date: str | None
    last_decision_date: str | None
    historical_reference_period_start: str | None
    historical_reference_period_end: str | None
    historical_event_count: int
    historical_active_day_count: int
    historical_average_events_per_active_day: float | None
    linked_alert_count: int
    thematic_key_rule: str
    thematic_source_distribution: dict[str, int]
    historical_thematic_source_distribution: dict[str, int]
    thematic_flow_interpretation_status: str
    thematic_flow_interpretation_reasons: list[str]
    decision_type_distribution: dict[str, int]
    decision_progress_distribution: dict[str, int]
    judging_body_distribution: dict[str, int]
    process_class_distribution: dict[str, int]
    thematic_distribution: dict[str, int]
    collegiate_distribution: dict[str, int]
    daily_counts: list[dict[str, Any]]
    decision_type_flow: list[dict[str, Any]]
    decision_progress_flow: list[dict[str, Any]]
    judging_body_flow: list[dict[str, Any]]
    process_class_flow: list[dict[str, Any]]
    thematic_flow: list[dict[str, Any]]


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "minister"


def _clean_counter(counter: Counter[str]) -> dict[str, int]:
    return {key: int(value) for key, value in counter.items() if key}


def _default_output_path(output_dir: Path, minister: str, period: str) -> Path:
    return output_dir / f"minister_flow__{_slugify(minister)}__{period}.json"


def _load_process_context(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}

    context: dict[str, dict[str, str]] = {}
    for row in _read_jsonl(path):
        process_id = str(row.get("process_id") or "").strip()
        if not process_id:
            continue

        process_class = str(row.get("process_class") or "").strip()
        subjects_normalized = row.get("subjects_normalized")
        branch_of_law = row.get("branch_of_law")
        thematic_key = derive_thematic_key(
            subjects_normalized if isinstance(subjects_normalized, list) else None,
            branch_of_law,
            fallback="",
        )
        payload_source = ""
        if thematic_key:
            is_from_subjects = isinstance(subjects_normalized, list) and any(
                str(v or "").strip() == thematic_key for v in subjects_normalized
            )
            if is_from_subjects:
                payload_source = "subjects_normalized_first"
            elif branch_of_law and str(branch_of_law).strip() == thematic_key:
                payload_source = "branch_of_law_fallback"

        payload: dict[str, str] = {}
        if process_class:
            payload["process_class"] = process_class
        if thematic_key:
            payload["thematic_key"] = thematic_key
        if payload_source:
            payload["thematic_source"] = payload_source
        if payload:
            context[process_id] = payload
    return context


def _build_thematic_interpretation(
    *,
    event_count: int,
    active_day_count: int,
    historical_event_count: int,
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if event_count == 0:
        reasons.append("no_events_in_period")
    if event_count < MIN_THEMATIC_EVENTS_FOR_COMPARATIVE_READ:
        reasons.append(f"event_count_lt_{MIN_THEMATIC_EVENTS_FOR_COMPARATIVE_READ}")
    if active_day_count < MIN_THEMATIC_ACTIVE_DAYS_FOR_COMPARATIVE_READ:
        reasons.append(f"active_day_count_lt_{MIN_THEMATIC_ACTIVE_DAYS_FOR_COMPARATIVE_READ}")
    if historical_event_count < MIN_THEMATIC_HISTORICAL_EVENTS_FOR_COMPARATIVE_READ:
        reasons.append(f"historical_event_count_lt_{MIN_THEMATIC_HISTORICAL_EVENTS_FOR_COMPARATIVE_READ}")
    return ("inconclusivo", reasons) if reasons else ("comparativo", [])


def _build_daily_counts(
    daily_counter: Counter[str],
    historical_daily_counter: Counter[str],
) -> tuple[list[dict[str, Any]], float | None]:
    historical_average = None
    if historical_daily_counter:
        historical_average = round(
            sum(historical_daily_counter.values()) / len(historical_daily_counter),
            6,
        )

    sorted_days = sorted(daily_counter.items())
    return [
        asdict(
            DailyCount(
                decision_date=decision_date,
                event_count=count,
                delta_vs_historical_average=(
                    round(count - historical_average, 6) if historical_average is not None else None
                ),
                ratio_vs_historical_average=(
                    round(count / historical_average, 6) if historical_average not in (None, 0) else None
                ),
            )
        )
        for decision_date, count in sorted_days
    ], historical_average


def _build_segment_flow(
    *,
    matched_events: list[dict[str, Any]],
    historical_events: list[dict[str, Any]],
    segment_field: str,
) -> list[dict[str, Any]]:
    segment_values = {
        str(row.get(segment_field)) for row in matched_events + historical_events if row.get(segment_field)
    }
    records: list[dict[str, Any]] = []
    for segment_value in sorted(segment_values):
        segment_events = [row for row in matched_events if row.get(segment_field) == segment_value]
        if not segment_events:
            continue
        segment_historical_events = [row for row in historical_events if row.get(segment_field) == segment_value]
        daily_counter: Counter[str] = Counter(str(row["decision_date"]) for row in segment_events)
        historical_daily_counter: Counter[str] = Counter(str(row["decision_date"]) for row in segment_historical_events)
        daily_counts, historical_average = _build_daily_counts(
            daily_counter,
            historical_daily_counter,
        )
        records.append(
            asdict(
                SegmentFlowRecord(
                    segment_value=segment_value,
                    event_count=len(segment_events),
                    process_count=len({str(row["process_id"]) for row in segment_events if row.get("process_id")}),
                    active_day_count=len(daily_counter),
                    historical_event_count=len(segment_historical_events),
                    historical_active_day_count=len(historical_daily_counter),
                    historical_average_events_per_active_day=historical_average,
                    daily_counts=daily_counts,
                )
            )
        )
    return records


def build_minister_flow(
    *,
    minister: str,
    year: int,
    month: int,
    collegiate_filter: str = "all",
    decision_event_path: Path = DEFAULT_DECISION_EVENT_PATH,
    process_path: Path = DEFAULT_PROCESS_PATH,
    alert_path: Path | None = DEFAULT_ALERT_PATH,
    output_path: Path | None = None,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    if on_progress:
        on_progress(0, 3, "Minister Flow: Carregando dados...")
    period = f"{year:04d}-{month:02d}"
    period_start = f"{period}-01"
    minister_query = minister.strip()
    minister_query_upper = minister_query.upper()
    events = _read_jsonl(decision_event_path)
    process_context = _load_process_context(process_path)
    minister_events = [
        row
        for row in events
        if minister_query_upper in str(row.get("current_rapporteur") or "").upper() and row.get("decision_date")
    ]
    if collegiate_filter == "colegiado":
        minister_events = [row for row in minister_events if bool(row.get("is_collegiate"))]
    elif collegiate_filter == "monocratico":
        minister_events = [row for row in minister_events if not bool(row.get("is_collegiate"))]

    matched_events = [row for row in minister_events if str(row.get("decision_date") or "").startswith(period)]
    historical_events = [row for row in minister_events if str(row.get("decision_date") or "") < period_start]
    enriched_matched_events: list[dict[str, Any]] = []
    enriched_historical_events: list[dict[str, Any]] = []

    minister_reference = "INCERTO"
    if matched_events:
        minister_reference = str(matched_events[0].get("current_rapporteur") or "INCERTO")

    event_ids = {str(row["decision_event_id"]) for row in matched_events}
    linked_alert_count = 0
    if alert_path is not None and alert_path.exists() and event_ids:
        linked_alert_count = sum(
            1 for row in _read_jsonl(alert_path) if str(row.get("decision_event_id") or "") in event_ids
        )

    decision_type_counter: Counter[str] = Counter()
    decision_progress_counter: Counter[str] = Counter()
    judging_body_counter: Counter[str] = Counter()
    process_class_counter: Counter[str] = Counter()
    thematic_counter: Counter[str] = Counter()
    thematic_source_counter: Counter[str] = Counter()
    historical_thematic_source_counter: Counter[str] = Counter()
    collegiate_counter: Counter[str] = Counter()
    daily_counter: Counter[str] = Counter()
    historical_daily_counter: Counter[str] = Counter()
    process_ids: set[str] = set()

    if on_progress:
        on_progress(1, 3, "Minister Flow: Agregando fluxos...")
    for row in matched_events:
        process_id = str(row.get("process_id") or "").strip()
        enriched_row = row
        if process_id and process_id in process_context:
            enriched_row = {**row, **process_context[process_id]}
        enriched_matched_events.append(enriched_row)
        if enriched_row.get("decision_type"):
            decision_type_counter[str(enriched_row["decision_type"])] += 1
        if enriched_row.get("decision_progress"):
            decision_progress_counter[str(enriched_row["decision_progress"])] += 1
        if enriched_row.get("judging_body"):
            judging_body_counter[str(enriched_row["judging_body"])] += 1
        if enriched_row.get("process_class"):
            process_class_counter[str(enriched_row["process_class"])] += 1
        if enriched_row.get("thematic_key"):
            thematic_counter[str(enriched_row["thematic_key"])] += 1
        if enriched_row.get("thematic_source"):
            thematic_source_counter[str(enriched_row["thematic_source"])] += 1
        collegiate_counter["colegiado" if enriched_row.get("is_collegiate") else "monocratico"] += 1
        if enriched_row.get("decision_date"):
            daily_counter[str(enriched_row["decision_date"])] += 1
        if enriched_row.get("process_id"):
            process_ids.add(str(enriched_row["process_id"]))

    for row in historical_events:
        process_id = str(row.get("process_id") or "").strip()
        enriched_row = row
        if process_id and process_id in process_context:
            enriched_row = {**row, **process_context[process_id]}
        enriched_historical_events.append(enriched_row)
        if enriched_row.get("decision_date"):
            historical_daily_counter[str(enriched_row["decision_date"])] += 1
        if enriched_row.get("thematic_source"):
            historical_thematic_source_counter[str(enriched_row["thematic_source"])] += 1

    daily_counts, historical_average = _build_daily_counts(
        daily_counter,
        historical_daily_counter,
    )
    thematic_flow_status, thematic_flow_reasons = _build_thematic_interpretation(
        event_count=len(matched_events),
        active_day_count=len(daily_counter),
        historical_event_count=sum(historical_daily_counter.values()),
    )
    sorted_days = sorted(daily_counter.items())
    historical_days = sorted(historical_daily_counter.items())
    record = asdict(
        MinisterFlowRecord(
            minister_query=minister_query,
            minister_match_mode="contains_casefold",
            minister_reference=minister_reference,
            period=period,
            collegiate_filter=collegiate_filter,
            status="ok" if matched_events else "empty",
            event_count=len(matched_events),
            process_count=len(process_ids),
            active_day_count=len(daily_counter),
            first_decision_date=sorted_days[0][0] if sorted_days else None,
            last_decision_date=sorted_days[-1][0] if sorted_days else None,
            historical_reference_period_start=historical_days[0][0] if historical_days else None,
            historical_reference_period_end=historical_days[-1][0] if historical_days else None,
            historical_event_count=sum(historical_daily_counter.values()),
            historical_active_day_count=len(historical_daily_counter),
            historical_average_events_per_active_day=historical_average,
            linked_alert_count=linked_alert_count,
            thematic_key_rule=THEMATIC_KEY_RULE,
            thematic_source_distribution=_clean_counter(thematic_source_counter),
            historical_thematic_source_distribution=_clean_counter(historical_thematic_source_counter),
            thematic_flow_interpretation_status=thematic_flow_status,
            thematic_flow_interpretation_reasons=thematic_flow_reasons,
            decision_type_distribution=_clean_counter(decision_type_counter),
            decision_progress_distribution=_clean_counter(decision_progress_counter),
            judging_body_distribution=_clean_counter(judging_body_counter),
            process_class_distribution=_clean_counter(process_class_counter),
            thematic_distribution=_clean_counter(thematic_counter),
            collegiate_distribution=_clean_counter(collegiate_counter),
            daily_counts=daily_counts,
            decision_type_flow=_build_segment_flow(
                matched_events=enriched_matched_events,
                historical_events=enriched_historical_events,
                segment_field="decision_type",
            ),
            decision_progress_flow=_build_segment_flow(
                matched_events=enriched_matched_events,
                historical_events=enriched_historical_events,
                segment_field="decision_progress",
            ),
            judging_body_flow=_build_segment_flow(
                matched_events=enriched_matched_events,
                historical_events=enriched_historical_events,
                segment_field="judging_body",
            ),
            process_class_flow=_build_segment_flow(
                matched_events=enriched_matched_events,
                historical_events=enriched_historical_events,
                segment_field="process_class",
            ),
            thematic_flow=_build_segment_flow(
                matched_events=enriched_matched_events,
                historical_events=enriched_historical_events,
                segment_field="thematic_key",
            ),
        )
    )

    if on_progress:
        on_progress(2, 3, "Minister Flow: Gravando resultados...")
    validate_records([record], FLOW_SCHEMA)
    final_output = output_path or _default_output_path(Path("data/analytics"), minister_query, period)
    final_output.parent.mkdir(parents=True, exist_ok=True)
    final_output.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
    if on_progress:
        on_progress(3, 3, "Minister Flow: Concluído")
    return final_output
