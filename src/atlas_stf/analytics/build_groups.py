"""Build comparison groups and decision-event links."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.identity import infer_process_class_from_number
from ..core.rules import (
    MAX_CASE_COUNT,
    MIN_CASE_COUNT,
    RULE_VERSION,
    GroupKey,
    classify_group_size,
    classify_judging_body_category,
    derive_thematic_key,
)
from ..schema_validate import validate_records

PROCESS_SCHEMA = Path("schemas/comparison_group.schema.json")
LINK_SCHEMA = Path("schemas/decision_event_group_link.schema.json")
SUMMARY_SCHEMA = Path("schemas/comparison_group_summary.schema.json")
DEFAULT_PROCESS_PATH = Path("data/curated/process.jsonl")
DEFAULT_DECISION_EVENT_PATH = Path("data/curated/decision_event.jsonl")
DEFAULT_OUTPUT_DIR = Path("data/analytics")


@dataclass(frozen=True)
class ComparisonGroupRecord:
    comparison_group_id: str
    rule_version: str
    selection_criteria: dict[str, Any]
    time_window: str
    case_count: int
    baseline_notes: str | None
    status: str
    blocked_reason: str | None
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class DecisionEventGroupLinkRecord:
    decision_event_id: str
    comparison_group_id: str
    process_id: str
    linked_at: str


def _load_process_context(path: Path, relevant_process_ids: set[str]) -> dict[str, dict[str, Any]]:
    context: dict[str, dict[str, Any]] = {}
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            row = json.loads(line)
            pid = row["process_id"]
            if pid not in relevant_process_ids:
                continue
            process_class = row.get("process_class") or infer_process_class_from_number(row.get("process_number"))
            thematic_key = derive_thematic_key(row.get("subjects_normalized"), row.get("branch_of_law"), fallback="")
            context[pid] = {
                "process_class": process_class,
                "thematic_key": thematic_key,
            }
    return context


def _read_decision_events(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh]


def _build_group_key(decision_event: dict[str, Any], process_ctx: dict[str, Any]) -> GroupKey | None:
    process_class = process_ctx.get("process_class")
    thematic_key = process_ctx.get("thematic_key")
    decision_type = decision_event.get("decision_type")
    decision_year = decision_event.get("decision_year")

    if (
        process_class is None
        or thematic_key is None
        or decision_type is None
        or decision_year is None
    ):
        return None

    judging_body_category = classify_judging_body_category(
        decision_event.get("judging_body"),
        decision_event.get("is_collegiate"),
    )

    return GroupKey(
        process_class=str(process_class),
        thematic_key=str(thematic_key),
        decision_type=str(decision_type),
        judging_body_category=judging_body_category,
        decision_year=int(decision_year),
    )


def _group_id_from_key(key: GroupKey) -> str:
    payload = json.dumps(key.to_dict(), ensure_ascii=False, sort_keys=True)
    import hashlib

    return "grp_" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _write_jsonl(records: list[dict[str, Any]], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    return output_path


def build_groups(
    process_path: Path = DEFAULT_PROCESS_PATH,
    decision_event_path: Path = DEFAULT_DECISION_EVENT_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    *,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> tuple[Path, Path, Path]:
    if on_progress:
        on_progress(0, 3, "Groups: Carregando dados...")
    decision_events = _read_decision_events(decision_event_path)
    relevant_process_ids = {row["process_id"] for row in decision_events}
    process_context = _load_process_context(process_path, relevant_process_ids)

    if on_progress:
        on_progress(1, 3, "Groups: Classificando eventos...")
    grouped_events: dict[GroupKey, list[dict[str, Any]]] = defaultdict(list)
    skipped_events = 0
    for event in decision_events:
        process_ctx = process_context.get(event["process_id"])
        if not process_ctx:
            skipped_events += 1
            continue
        key = _build_group_key(event, process_ctx)
        if key is None:
            skipped_events += 1
            continue
        grouped_events[key].append(event)

    timestamp = datetime.now(timezone.utc).isoformat()
    group_records: list[dict[str, Any]] = []
    link_records: list[dict[str, Any]] = []

    for key, events in grouped_events.items():
        comparison_group_id = _group_id_from_key(key)
        status, blocked_reason = classify_group_size(len(events))
        group_record = asdict(
            ComparisonGroupRecord(
                comparison_group_id=comparison_group_id,
                rule_version=RULE_VERSION,
                selection_criteria=key.to_dict(),
                time_window=str(key.decision_year),
                case_count=len(events),
                baseline_notes=(
                    f"Initial comparability rule with min_cases={MIN_CASE_COUNT} and max_cases={MAX_CASE_COUNT}"
                ),
                status=status,
                blocked_reason=blocked_reason,
                created_at=timestamp,
                updated_at=timestamp,
            )
        )
        group_records.append(group_record)

        if status != "valid":
            continue
        for event in events:
            link_records.append(
                asdict(
                    DecisionEventGroupLinkRecord(
                        decision_event_id=event["decision_event_id"],
                        comparison_group_id=comparison_group_id,
                        process_id=event["process_id"],
                        linked_at=timestamp,
                    )
                )
            )

    summary = {
        "generated_at": timestamp,
        "rule_version": RULE_VERSION,
        "group_count": len(group_records),
        "valid_group_count": sum(1 for record in group_records if record["status"] == "valid"),
        "linked_event_count": len(link_records),
        "skipped_event_count": skipped_events,
    }

    if on_progress:
        on_progress(2, 3, "Groups: Gravando resultados...")
    validate_records(group_records, PROCESS_SCHEMA)
    validate_records(link_records, LINK_SCHEMA)
    validate_records([summary], SUMMARY_SCHEMA)

    groups_path = _write_jsonl(group_records, output_dir / "comparison_group.jsonl")
    links_path = _write_jsonl(link_records, output_dir / "decision_event_group_link.jsonl")
    summary_path = output_dir / "comparison_group_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    if on_progress:
        on_progress(3, 3, "Groups: Concluído")
    return groups_path, links_path, summary_path
