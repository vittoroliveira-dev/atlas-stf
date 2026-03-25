"""Loaders for agenda serving tables."""

from __future__ import annotations

import json
from datetime import time as time_cls
from pathlib import Path

from ._builder_utils import _coerce_bool, _coerce_float, _coerce_int, _parse_date, _parse_datetime, _read_jsonl
from ._models_agenda import ServingAgendaCoverage, ServingAgendaEvent, ServingAgendaExposure


def load_agenda_events(curated_dir: Path) -> list[ServingAgendaEvent]:
    path = curated_dir / "agenda_event.jsonl"
    if not path.exists():
        return []
    results: list[ServingAgendaEvent] = []
    seen: set[str] = set()
    for r in _read_jsonl(path):
        eid = str(r.get("agenda_event_id") or r.get("event_id") or "")
        if eid in seen:
            continue
        seen.add(eid)
        tv = None
        ts = r.get("event_time_local")
        if isinstance(ts, str) and ts:
            try:
                p = ts.split(":")
                tv = time_cls(int(p[0]), int(p[1]))
            except ValueError, IndexError:
                pass
        results.append(
            ServingAgendaEvent(
                event_id=eid,
                minister_slug=str(r.get("minister_slug", "")),
                minister_name=str(r.get("minister_name", "")),
                owner_scope=str(r.get("owner_scope", "")),
                owner_role=str(r.get("owner_role", "")),
                event_date=_parse_date(r.get("event_date")),
                event_time_local=tv,
                event_datetime_start=_parse_datetime(r.get("event_datetime_start")),
                source_time_raw=r.get("source_time_raw"),
                event_title=str(r.get("event_title", "")),
                event_description=r.get("event_description"),
                event_category=str(r.get("event_category", "")),
                meeting_nature=str(r.get("meeting_nature", "")),
                has_process_ref=_coerce_bool(r.get("has_process_ref")),
                contains_public_actor=_coerce_bool(r.get("contains_public_actor")),
                contains_private_actor=_coerce_bool(r.get("contains_private_actor")),
                actor_count=_coerce_int(r.get("actor_count")),
                classification_confidence=_coerce_float(r.get("classification_confidence")) or 0.0,
                relevance_track=str(r.get("relevance_track", "none")),
                process_refs_json=json.dumps(r.get("process_refs", []), ensure_ascii=False),
                participants_json=json.dumps(
                    {
                        "participants_raw": r.get("participants_raw", []),
                        "participant_entities": r.get("participant_entities", []),
                    },
                    ensure_ascii=False,
                ),
                participant_resolution_confidence=_coerce_float(r.get("participant_resolution_confidence")),
                organizations_json=json.dumps(r.get("organizations_raw", []), ensure_ascii=False),
                process_id=r.get("process_id"),
                process_class=r.get("process_class"),
                is_own_process=r.get("is_own_process"),
                minister_case_role=r.get("minister_case_role"),
                institutional_role_bias_flag=_coerce_bool(r.get("institutional_role_bias_flag")),
            )
        )
    return results


def load_agenda_coverage(curated_dir: Path) -> list[ServingAgendaCoverage]:
    path = curated_dir / "agenda_coverage.jsonl"
    if not path.exists():
        return []
    results: list[ServingAgendaCoverage] = []
    seen: set[str] = set()
    for r in _read_jsonl(path):
        cid = str(r.get("coverage_id", ""))
        if cid in seen:
            continue
        seen.add(cid)
        results.append(
            ServingAgendaCoverage(
                coverage_id=cid,
                minister_slug=str(r.get("minister_slug", "")),
                minister_name=str(r.get("minister_name", "")),
                owner_scope=str(r.get("owner_scope", "ministerial")),
                year=_coerce_int(r.get("year")),
                month=_coerce_int(r.get("month")),
                publication_observed=_coerce_bool(r.get("publication_observed")),
                event_count=_coerce_int(r.get("event_count")),
                days_with_events=_coerce_int(r.get("days_with_events")),
                business_days_in_month=_coerce_int(r.get("business_days_in_month")),
                coverage_ratio=_coerce_float(r.get("coverage_ratio")) or 0.0,
                institutional_core_count=_coerce_int(r.get("institutional_core_count")),
                institutional_external_actor_count=_coerce_int(r.get("institutional_external_actor_count")),
                private_advocacy_count=_coerce_int(r.get("private_advocacy_count")),
                unclear_count=_coerce_int(r.get("unclear_count")),
                track_a_count=_coerce_int(r.get("track_a_count")),
                track_b_count=_coerce_int(r.get("track_b_count")),
                court_recess_flag=_coerce_bool(r.get("court_recess_flag")),
                vacation_or_leave_flag=_coerce_bool(r.get("vacation_or_leave_flag")),
                publication_gap_flag=_coerce_bool(r.get("publication_gap_flag")),
                comparability_tier=str(r.get("comparability_tier", "low")),
                coverage_quality_note=r.get("coverage_quality_note"),
            )
        )
    return results


def load_agenda_exposures(analytics_dir: Path) -> list[ServingAgendaExposure]:
    path = analytics_dir / "agenda_exposure.jsonl"
    if not path.exists():
        return []
    results: list[ServingAgendaExposure] = []
    seen: set[str] = set()
    for r in _read_jsonl(path):
        eid = str(r.get("exposure_id", ""))
        if eid in seen:
            continue
        seen.add(eid)
        results.append(
            ServingAgendaExposure(
                exposure_id=eid,
                agenda_event_id=str(r.get("agenda_event_id", "")),
                minister_slug=str(r.get("minister_slug", "")),
                process_id=r.get("process_id"),
                process_class=r.get("process_class"),
                agenda_date=_parse_date(r.get("agenda_date")),
                decision_date=_parse_date(r.get("decision_date")),
                days_between=r.get("days_between"),
                window=str(r.get("window", "none")),
                is_own_process=_coerce_bool(r.get("is_own_process")),
                minister_case_role=r.get("minister_case_role"),
                event_category=str(r.get("agenda_event_category", r.get("event_category", ""))),
                meeting_nature=str(r.get("agenda_meeting_nature", r.get("meeting_nature", ""))),
                event_title=r.get("agenda_event_title"),
                decision_type=r.get("decision_type"),
                baseline_rate=_coerce_float(r.get("baseline_rate")),
                rate_ratio=_coerce_float(r.get("rate_ratio")),
                priority_score=_coerce_float(r.get("priority_score")) or 0.0,
                priority_tier=str(r.get("priority_tier", "low")),
                priority_tier_override_reason=r.get("priority_tier_override_reason"),
                coverage_comparability=str(r.get("coverage_comparability", "low")),
            )
        )
    return results
