"""Date parsing utilities and constants for temporal analysis."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

from ..core.rules import classify_outcome_raw
from ._match_helpers import build_process_class_map
from ._match_io import read_jsonl

DEFAULT_DECISION_EVENT_PATH = Path("data/curated/decision_event.jsonl")
DEFAULT_PROCESS_PATH = Path("data/curated/process.jsonl")
DEFAULT_MINISTER_BIO_PATH = Path("data/curated/minister_bio.json")
DEFAULT_PARTY_PATH = Path("data/curated/party.jsonl")
DEFAULT_COUNSEL_PATH = Path("data/curated/counsel.jsonl")
DEFAULT_PROCESS_PARTY_LINK_PATH = Path("data/curated/process_party_link.jsonl")
DEFAULT_PROCESS_COUNSEL_LINK_PATH = Path("data/curated/process_counsel_link.jsonl")
DEFAULT_EXTERNAL_EVENTS_DIR = Path("data/raw/external_events")
DEFAULT_RFB_DIR = Path("data/raw/rfb")
DEFAULT_OUTPUT_DIR = Path("data/analytics")
SCHEMA_PATH = Path("schemas/temporal_analysis.schema.json")
SUMMARY_SCHEMA_PATH = Path("schemas/temporal_analysis_summary.schema.json")

MIN_EVENT_WINDOW_DECISIONS = 5
CUSUM_DRIFT = 0.25
BREAKPOINT_SCORE_THRESHOLD = 2.0


def _parse_iso_date(value: Any) -> date | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _parse_rfb_date(value: Any) -> date | None:
    if not isinstance(value, str) or len(value) != 8 or not value.isdigit():
        return None
    try:
        return date(int(value[:4]), int(value[4:6]), int(value[6:8]))
    except ValueError:
        return None


def _month_start(value: date) -> date:
    return date(value.year, value.month, 1)


def _month_key(value: date) -> str:
    return value.strftime("%Y-%m")


def _month_range(start: date, end: date) -> list[date]:
    months: list[date] = []
    current = _month_start(start)
    stop = _month_start(end)
    while current <= stop:
        months.append(current)
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    return months


def _round(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value, 6)


def _read_external_events(external_events_dir: Path) -> list[dict[str, Any]]:
    if not external_events_dir.exists():
        return []
    rows: list[dict[str, Any]] = []
    for path in sorted(external_events_dir.glob("*.jsonl")):
        rows.extend(read_jsonl(path))
    return rows


def _classifiable_events(decision_event_path: Path, process_path: Path) -> list[dict[str, Any]]:
    process_class_map = build_process_class_map(process_path)
    rows: list[dict[str, Any]] = []
    for record in read_jsonl(decision_event_path):
        rapporteur = record.get("current_rapporteur")
        decision_date = _parse_iso_date(record.get("decision_date"))
        outcome = classify_outcome_raw(str(record.get("decision_progress", "")))
        if not rapporteur or decision_date is None or outcome not in {"favorable", "unfavorable"}:
            continue
        rows.append(
            {
                "decision_event_id": str(record.get("decision_event_id", "")),
                "process_id": str(record.get("process_id", "")),
                "rapporteur": str(rapporteur),
                "decision_date": decision_date,
                "decision_year": decision_date.year,
                "decision_month": _month_key(decision_date),
                "month_start": _month_start(decision_date),
                "process_class": process_class_map.get(str(record.get("process_id", ""))),
                "outcome": outcome,
            }
        )
    return rows
