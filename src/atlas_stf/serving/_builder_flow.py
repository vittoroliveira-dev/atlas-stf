"""Materialized minister flows for serving database.

Replaces ~136K sequential SQL queries with in-memory filtering + parallel computation.
"""

from __future__ import annotations

import logging
import multiprocessing
import os
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from hashlib import sha256
from typing import Literal

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..core.constants import QueryFilters, collegiate_label
from ._builder_utils import _json_text
from .models import ServingAlert, ServingCase, ServingMinisterFlow

logger = logging.getLogger(__name__)

# Module-level state shared with forked workers (copy-on-write).
_worker_cases: list[_CaseRow] = []
_worker_alert_ids: frozenset[str] = frozenset()

FLOW_SHAPES: tuple[tuple[str, ...], ...] = (
    (),
    ("minister",),
    ("judging_body",),
    ("process_class",),
    ("minister", "judging_body"),
    ("minister", "process_class"),
    ("judging_body", "process_class"),
    ("minister", "judging_body", "process_class"),
)


@dataclass(frozen=True)
class _CaseRow:
    """Lightweight projection of ServingCase for in-memory flow computation."""

    decision_event_id: str
    process_id: str
    decision_date: date | None
    period: str | None
    current_rapporteur: str | None
    judging_body: str | None
    process_class: str | None
    is_collegiate: bool | None
    decision_type: str | None
    decision_progress: str | None
    thematic_key: str | None


def _minister_flow_key(filters: QueryFilters) -> str:
    signature = "|".join(
        [
            filters.minister or "",
            filters.period or "",
            filters.collegiate,
            filters.judging_body or "",
            filters.process_class or "",
        ]
    )
    return sha256(signature.encode("utf-8")).hexdigest()


def _matches_filter(case: _CaseRow, filters: QueryFilters) -> bool:
    if filters.minister:
        rapporteur = (case.current_rapporteur or "").lower()
        if filters.minister.lower() not in rapporteur:
            return False
    if filters.period and case.period != filters.period:
        return False
    if filters.collegiate == "colegiado" and case.is_collegiate is not True:
        return False
    if filters.collegiate == "monocratico" and case.is_collegiate is not False:
        return False
    if filters.judging_body and case.judging_body != filters.judging_body:
        return False
    if filters.process_class and case.process_class != filters.process_class:
        return False
    return True


def _group_counter(cases: list[_CaseRow], selector) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for case in cases:
        counter[selector(case) or "INCERTO"] += 1
    return dict(sorted(counter.items(), key=lambda item: (-item[1], item[0])))


def _daily_points(cases: list[_CaseRow], historical_average: float) -> list[dict]:
    by_day: Counter[str] = Counter()
    for case in cases:
        if case.decision_date:
            by_day[case.decision_date.isoformat()] += 1
    points: list[dict] = []
    for day, count in sorted(by_day.items()):
        delta = count - historical_average
        ratio = (count / historical_average) if historical_average > 0 else 0.0
        points.append(
            {
                "date": day,
                "event_count": count,
                "delta_vs_historical_average": round(delta, 6),
                "ratio_vs_historical_average": round(ratio, 6),
            }
        )
    return points


def _segment_flow(
    monthly_cases: list[_CaseRow],
    historical_cases: list[_CaseRow],
    selector,
) -> list[dict]:
    monthly_groups: dict[str, list[_CaseRow]] = defaultdict(list)
    historical_groups: dict[str, list[_CaseRow]] = defaultdict(list)
    for case in monthly_cases:
        monthly_groups[selector(case) or "INCERTO"].append(case)
    for case in historical_cases:
        historical_groups[selector(case) or "INCERTO"].append(case)

    flows: list[dict] = []
    for value, segment_cases in sorted(monthly_groups.items(), key=lambda item: (-len(item[1]), item[0])):
        hist_cases = historical_groups.get(value, [])
        hist_days = {c.decision_date for c in hist_cases if c.decision_date}
        hist_average = len(hist_cases) / len(hist_days) if hist_days else 0.0
        seg_days = {c.decision_date for c in segment_cases if c.decision_date}
        flows.append(
            {
                "segment_value": value,
                "event_count": len(segment_cases),
                "process_count": len({c.process_id for c in segment_cases}),
                "active_day_count": len(seg_days),
                "historical_event_count": len(hist_cases),
                "historical_active_day_count": len(hist_days),
                "historical_average_events_per_active_day": round(hist_average, 6),
                "daily_counts": _daily_points(segment_cases, hist_average),
            }
        )
    return flows


def _interpret_thematic_flow(
    monthly_cases: list[_CaseRow],
    historical_cases: list[_CaseRow],
) -> tuple[Literal["comparativo", "inconclusivo"], list[str]]:
    reasons: list[str] = []
    if not monthly_cases:
        reasons.append("no_events_in_period")
    if len(monthly_cases) < 5:
        reasons.append("event_count_lt_5")
    if len({c.decision_date for c in monthly_cases if c.decision_date}) < 3:
        reasons.append("active_day_count_lt_3")
    if len(historical_cases) < 20:
        reasons.append("historical_event_count_lt_20")
    return ("comparativo" if not reasons else "inconclusivo", reasons)


def _compute_flow(
    all_cases: list[_CaseRow],
    alert_event_ids: frozenset[str],
    filters: QueryFilters,
) -> dict:
    """Compute a minister flow entirely in-memory (no SQL)."""
    monthly_cases = [c for c in all_cases if _matches_filter(c, filters)]
    monthly_cases.sort(key=lambda c: (c.decision_date or date.min, c.decision_event_id))

    historical_cases: list[_CaseRow] = []
    historical_start: date | None = None
    if filters.period:
        historical_start = date.fromisoformat(f"{filters.period}-01")
        hist_filters = QueryFilters(
            minister=filters.minister,
            collegiate=filters.collegiate,
            judging_body=filters.judging_body,
            process_class=filters.process_class,
        )
        historical_cases = [
            c
            for c in all_cases
            if _matches_filter(c, hist_filters) and c.decision_date is not None and c.decision_date < historical_start
        ]
        historical_cases.sort(key=lambda c: (c.decision_date or date.min,))

    monthly_days = {c.decision_date for c in monthly_cases if c.decision_date}
    historical_days = {c.decision_date for c in historical_cases if c.decision_date}
    historical_average = len(historical_cases) / len(historical_days) if historical_days else 0.0

    linked_alert_count = sum(1 for c in monthly_cases if c.decision_event_id in alert_event_ids)

    thematic_status, thematic_reasons = _interpret_thematic_flow(monthly_cases, historical_cases)

    monthly_dates = [c.decision_date for c in monthly_cases if c.decision_date]
    historical_dates = [c.decision_date for c in historical_cases if c.decision_date]

    return {
        "minister_query": filters.minister or "",
        "minister_match_mode": "contains_casefold",
        "minister_reference": filters.minister,
        "period": filters.period or "",
        "status": "empty" if not monthly_cases else "ok",
        "event_count": len(monthly_cases),
        "process_count": len({c.process_id for c in monthly_cases}),
        "active_day_count": len(monthly_days),
        "first_decision_date": min(monthly_dates) if monthly_dates else None,
        "last_decision_date": max(monthly_dates) if monthly_dates else None,
        "historical_reference_period_start": min(historical_dates) if historical_dates else None,
        "historical_reference_period_end": max(historical_dates) if historical_dates else None,
        "historical_event_count": len(historical_cases),
        "historical_active_day_count": len(historical_days),
        "historical_average_events_per_active_day": round(historical_average, 6),
        "linked_alert_count": linked_alert_count,
        "thematic_key_rule": "first_subject_normalized_else_branch_of_law",
        "thematic_source_distribution": {"serving_process_thematic_key": len(monthly_cases)} if monthly_cases else {},
        "historical_thematic_source_distribution": (
            {"serving_process_thematic_key": len(historical_cases)} if historical_cases else {}
        ),
        "thematic_flow_interpretation_status": thematic_status,
        "thematic_flow_interpretation_reasons": thematic_reasons,
        "decision_type_distribution": _group_counter(monthly_cases, lambda c: c.decision_type),
        "decision_progress_distribution": _group_counter(monthly_cases, lambda c: c.decision_progress),
        "judging_body_distribution": _group_counter(monthly_cases, lambda c: c.judging_body),
        "collegiate_distribution": _group_counter(monthly_cases, lambda c: collegiate_label(c.is_collegiate)),
        "process_class_distribution": _group_counter(monthly_cases, lambda c: c.process_class),
        "thematic_distribution": _group_counter(monthly_cases, lambda c: c.thematic_key),
        "daily_counts": _daily_points(monthly_cases, historical_average),
        "decision_type_flow": _segment_flow(monthly_cases, historical_cases, lambda c: c.decision_type),
        "judging_body_flow": _segment_flow(monthly_cases, historical_cases, lambda c: c.judging_body),
        "decision_progress_flow": _segment_flow(monthly_cases, historical_cases, lambda c: c.decision_progress),
        "process_class_flow": _segment_flow(monthly_cases, historical_cases, lambda c: c.process_class),
        "thematic_flow": _segment_flow(monthly_cases, historical_cases, lambda c: c.thematic_key),
    }


def _worker_compute_flow(item: tuple[str, QueryFilters]) -> tuple[str, QueryFilters, dict]:
    """Multiprocessing worker — uses module-level state inherited via fork."""
    key, filters = item
    payload = _compute_flow(_worker_cases, _worker_alert_ids, filters)
    return key, filters, payload


def _flow_to_model(key: str, filters: QueryFilters, payload: dict) -> ServingMinisterFlow:
    return ServingMinisterFlow(
        flow_key=key,
        minister_name=filters.minister,
        period=filters.period or "",
        collegiate_filter=filters.collegiate,
        judging_body=filters.judging_body,
        process_class=filters.process_class,
        minister_query=payload["minister_query"],
        minister_match_mode=payload["minister_match_mode"],
        minister_reference=payload["minister_reference"],
        status=payload["status"],
        event_count=payload["event_count"],
        process_count=payload["process_count"],
        active_day_count=payload["active_day_count"],
        first_decision_date=payload["first_decision_date"],
        last_decision_date=payload["last_decision_date"],
        historical_reference_period_start=payload["historical_reference_period_start"],
        historical_reference_period_end=payload["historical_reference_period_end"],
        historical_event_count=payload["historical_event_count"],
        historical_active_day_count=payload["historical_active_day_count"],
        historical_average_events_per_active_day=payload["historical_average_events_per_active_day"],
        linked_alert_count=payload["linked_alert_count"],
        thematic_key_rule=payload["thematic_key_rule"],
        thematic_flow_interpretation_status=payload["thematic_flow_interpretation_status"],
        thematic_source_distribution_json=_json_text(payload["thematic_source_distribution"]),
        historical_thematic_source_distribution_json=_json_text(payload["historical_thematic_source_distribution"]),
        thematic_flow_interpretation_reasons_json=_json_text(payload["thematic_flow_interpretation_reasons"]),
        decision_type_distribution_json=_json_text(payload["decision_type_distribution"]),
        decision_progress_distribution_json=_json_text(payload["decision_progress_distribution"]),
        judging_body_distribution_json=_json_text(payload["judging_body_distribution"]),
        collegiate_distribution_json=_json_text(payload["collegiate_distribution"]),
        process_class_distribution_json=_json_text(payload["process_class_distribution"]),
        thematic_distribution_json=_json_text(payload["thematic_distribution"]),
        daily_counts_json=_json_text(payload["daily_counts"]),
        decision_type_flow_json=_json_text(payload["decision_type_flow"]),
        judging_body_flow_json=_json_text(payload["judging_body_flow"]),
        decision_progress_flow_json=_json_text(payload["decision_progress_flow"]),
        process_class_flow_json=_json_text(payload["process_class_flow"]),
        thematic_flow_json=_json_text(payload["thematic_flow"]),
    )


def _materialize_minister_flows(session: Session) -> list[ServingMinisterFlow]:
    # Phase 1: Pre-load all data into memory (one-time cost).
    logger.info("Minister flows: loading cases into memory...")
    orm_cases = list(session.scalars(select(ServingCase)).all())
    all_cases = [
        _CaseRow(
            decision_event_id=c.decision_event_id or "",
            process_id=c.process_id or "",
            decision_date=c.decision_date,
            period=c.period,
            current_rapporteur=c.current_rapporteur,
            judging_body=c.judging_body,
            process_class=c.process_class,
            is_collegiate=c.is_collegiate,
            decision_type=c.decision_type,
            decision_progress=c.decision_progress,
            thematic_key=c.thematic_key,
        )
        for c in orm_cases
    ]
    del orm_cases

    alert_event_ids = frozenset(eid for eid in session.scalars(select(ServingAlert.decision_event_id)) if eid)
    logger.info("Minister flows: %d cases, %d alert events loaded", len(all_cases), len(alert_event_ids))

    # Phase 2: Enumerate all filter combinations in-memory.
    periods = sorted({c.period for c in all_cases if c.period}, reverse=True)
    periods_with_all: list[str | None] = [None, *periods]
    tasks: list[tuple[str, QueryFilters]] = []
    seen_keys: set[str] = set()

    for period in periods_with_all:
        period_cases = all_cases if period is None else [c for c in all_cases if c.period == period]
        for collegiate in ("all", "colegiado", "monocratico"):
            col_cases = period_cases
            if collegiate == "colegiado":
                col_cases = [c for c in period_cases if c.is_collegiate is True]
            elif collegiate == "monocratico":
                col_cases = [c for c in period_cases if c.is_collegiate is False]

            for shape in FLOW_SHAPES:
                if not shape:
                    filters = QueryFilters(period=period, collegiate=collegiate)
                    key = _minister_flow_key(filters)
                    if key not in seen_keys:
                        seen_keys.add(key)
                        tasks.append((key, filters))
                    continue

                field_getters = {
                    "minister": lambda c: c.current_rapporteur,
                    "judging_body": lambda c: c.judging_body,
                    "process_class": lambda c: c.process_class,
                }
                getters = [(field, field_getters[field]) for field in shape]
                combos: set[tuple[str | None, ...]] = set()
                for case in col_cases:
                    vals = tuple(getter(case) for _, getter in getters)
                    if all(v is not None for v in vals):
                        combos.add(vals)

                for combo in sorted(combos):
                    values = dict(zip(shape, combo, strict=True))
                    filters = QueryFilters(
                        minister=values.get("minister"),
                        period=period,
                        collegiate=collegiate,
                        judging_body=values.get("judging_body"),
                        process_class=values.get("process_class"),
                    )
                    key = _minister_flow_key(filters)
                    if key not in seen_keys:
                        seen_keys.add(key)
                        tasks.append((key, filters))

    logger.info("Minister flows: %d unique filter combinations to compute", len(tasks))

    # Phase 3: Compute flows in parallel using fork-based multiprocessing.
    # Workers inherit all_cases + alert_event_ids via copy-on-write (no serialization).
    global _worker_cases, _worker_alert_ids  # noqa: PLW0603
    _worker_cases = all_cases
    _worker_alert_ids = alert_event_ids

    max_workers = min(
        os.cpu_count() or 4,
        int(os.environ.get("ATLAS_FLOW_WORKERS", str(min(4, os.cpu_count() or 1)))),
    )

    # Log memory before pool creation so operators can diagnose OOM.
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith(("VmRSS:", "VmPeak:")):
                    logger.info("Minister flows pre-pool: %s", line.strip())
    except OSError:
        pass

    logger.info("Minister flows: computing with %d workers...", max_workers)
    results: dict[str, ServingMinisterFlow] = {}
    try:
        if max_workers <= 1:
            # Serial path: avoids fork() overhead and OOM risk from copy-on-write.
            done = 0
            for task in tasks:
                key, filters, payload = _worker_compute_flow(task)
                results[key] = _flow_to_model(key, filters, payload)
                done += 1
                if done % 5000 == 0:
                    logger.info("Minister flows: %d / %d computed", done, len(tasks))
        else:
            chunksize = max(1, len(tasks) // (max_workers * 4))
            ctx = multiprocessing.get_context("fork")
            with ctx.Pool(processes=max_workers) as pool:
                done = 0
                for key, filters, payload in pool.imap_unordered(_worker_compute_flow, tasks, chunksize=chunksize):
                    results[key] = _flow_to_model(key, filters, payload)
                    done += 1
                    if done % 5000 == 0:
                        logger.info("Minister flows: %d / %d computed", done, len(tasks))
    finally:
        _worker_cases = []
        _worker_alert_ids = frozenset()

    logger.info("Minister flows: %d flows materialized", len(results))
    return list(results.values())
