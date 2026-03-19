"""Event window analysis for temporal analysis."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

from ..core.constants import EVENT_WINDOW_DAYS
from ..core.identity import normalize_entity_name, stable_id
from ._temporal_utils import (
    MIN_EVENT_WINDOW_DECISIONS,
    _parse_iso_date,
    _read_external_events,
    _round,
)


def _window_stats(events: list[dict[str, Any]]) -> tuple[int, int, int, float | None]:
    favorable = sum(1 for event in events if event["outcome"] == "favorable")
    unfavorable = sum(1 for event in events if event["outcome"] == "unfavorable")
    total = favorable + unfavorable
    return total, favorable, unfavorable, (favorable / total if total else None)


def _build_event_window_records(
    events: list[dict[str, Any]], external_events_dir: Path, generated_at: str
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for external_event in _read_external_events(external_events_dir):
        event_date = _parse_iso_date(external_event.get("event_date"))
        event_scope = external_event.get("event_scope")
        if event_date is None or event_scope not in {"global", "minister"}:
            continue
        minister_name = external_event.get("minister_name")
        minister_normalized = normalize_entity_name(minister_name) if minister_name else None
        relevant_events = [
            event
            for event in events
            if event_scope == "global" or normalize_entity_name(event["rapporteur"]) == minister_normalized
        ]
        before_events = [
            event
            for event in relevant_events
            if event_date - timedelta(days=EVENT_WINDOW_DAYS) <= event["decision_date"] < event_date
        ]
        after_events = [
            event
            for event in relevant_events
            if event_date < event["decision_date"] <= event_date + timedelta(days=EVENT_WINDOW_DAYS)
        ]
        before_count, before_favorable, before_unfavorable, before_rate = _window_stats(before_events)
        after_count, after_favorable, after_unfavorable, after_rate = _window_stats(after_events)
        status = (
            "comparativo"
            if before_count >= MIN_EVENT_WINDOW_DECISIONS and after_count >= MIN_EVENT_WINDOW_DECISIONS
            else "inconclusivo"
        )
        delta = (after_rate - before_rate) if before_rate is not None and after_rate is not None else None
        records.append(
            {
                "analysis_kind": "event_window",
                "record_id": stable_id("tmp_", str(external_event.get("event_id", ""))),
                "rapporteur": minister_name,
                "event_id": str(external_event.get("event_id", "")),
                "event_type": str(external_event.get("event_type", "")),
                "event_scope": str(event_scope),
                "event_date": event_date.isoformat(),
                "event_title": str(external_event.get("title", "")),
                "source": str(external_event.get("source", "")),
                "source_url": external_event.get("source_url"),
                "status": status,
                "before_decision_count": before_count,
                "before_favorable_rate": _round(before_rate),
                "after_decision_count": after_count,
                "after_favorable_rate": _round(after_rate),
                "delta_before_after": _round(delta),
                "decision_count": before_count + after_count,
                "favorable_count": before_favorable + after_favorable,
                "unfavorable_count": before_unfavorable + after_unfavorable,
                "generated_at": generated_at,
            }
        )
    return records
