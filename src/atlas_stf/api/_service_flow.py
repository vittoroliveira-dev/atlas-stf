"""Minister flow service — pre-materialized lookup.

Contract:
- **Exact match** on ``minister_query`` is always attempted first.
- If exact match fails, **textual fallback** (case-insensitive substring via
  ``_normalized_like``) is attempted.
- The payload signals which path was taken via ``minister_match_mode``:
    - ``"exact"`` — canonical key matched directly.
    - ``"textual"`` — single textual match found.
    - ``"ambiguous"`` — multiple textual matches; flow not returned.
    - ``"unresolved"`` — no match at all.
"""

from __future__ import annotations

from typing import Any, cast

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..serving.models import ServingMinisterFlow
from ._filters import QueryFilters, _normalized_like, resolve_filters
from ._json_helpers import parse_json_dict, parse_json_list
from .schemas import MinisterFlowResponse


def _empty_minister_flow(
    filters: QueryFilters,
    *,
    minister_query: str | None,
    match_mode: str = "unresolved",
    match_count: int = 0,
    candidates: list[str] | None = None,
) -> MinisterFlowResponse:
    return MinisterFlowResponse(
        minister_query=minister_query or "",
        minister_match_mode=match_mode,
        minister_match_count=match_count,
        minister_candidates=candidates,
        minister_reference=None,
        period=filters.period or "",
        status="empty",
        collegiate_filter=filters.collegiate,
        event_count=0,
        process_count=0,
        active_day_count=0,
        historical_event_count=0,
        historical_active_day_count=0,
        historical_average_events_per_active_day=0.0,
        linked_alert_count=0,
        thematic_key_rule="first_subject_normalized_else_branch_of_law",
        thematic_source_distribution={},
        historical_thematic_source_distribution={},
        thematic_flow_interpretation_status="inconclusivo",
        thematic_flow_interpretation_reasons=["minister_lookup_not_materialized"],
        decision_type_distribution={},
        decision_progress_distribution={},
        judging_body_distribution={},
        collegiate_distribution={},
        process_class_distribution={},
        thematic_distribution={},
        daily_counts=[],
        decision_type_flow=[],
        judging_body_flow=[],
        decision_progress_flow=[],
        process_class_flow=[],
        thematic_flow=[],
    )


def _minister_flow_from_row(
    row: ServingMinisterFlow,
    *,
    minister_query: str | None,
    match_mode: str,
) -> MinisterFlowResponse:
    return MinisterFlowResponse(
        minister_query=minister_query or row.minister_query,
        minister_match_mode=match_mode,
        minister_match_count=1,
        minister_candidates=None,
        minister_reference=row.minister_reference,
        period=row.period,
        status=cast(Any, row.status),
        collegiate_filter=cast(Any, row.collegiate_filter),
        event_count=row.event_count,
        process_count=row.process_count,
        active_day_count=row.active_day_count,
        first_decision_date=row.first_decision_date,
        last_decision_date=row.last_decision_date,
        historical_reference_period_start=row.historical_reference_period_start,
        historical_reference_period_end=row.historical_reference_period_end,
        historical_event_count=row.historical_event_count,
        historical_active_day_count=row.historical_active_day_count,
        historical_average_events_per_active_day=row.historical_average_events_per_active_day,
        linked_alert_count=row.linked_alert_count,
        thematic_key_rule=row.thematic_key_rule,
        thematic_source_distribution=parse_json_dict(row.thematic_source_distribution_json),
        historical_thematic_source_distribution=parse_json_dict(row.historical_thematic_source_distribution_json),
        thematic_flow_interpretation_status=cast(Any, row.thematic_flow_interpretation_status),
        thematic_flow_interpretation_reasons=parse_json_list(row.thematic_flow_interpretation_reasons_json),
        decision_type_distribution=parse_json_dict(row.decision_type_distribution_json),
        decision_progress_distribution=parse_json_dict(row.decision_progress_distribution_json),
        judging_body_distribution=parse_json_dict(row.judging_body_distribution_json),
        collegiate_distribution=parse_json_dict(row.collegiate_distribution_json),
        process_class_distribution=parse_json_dict(row.process_class_distribution_json),
        thematic_distribution=parse_json_dict(row.thematic_distribution_json),
        daily_counts=parse_json_list(row.daily_counts_json),
        decision_type_flow=parse_json_list(row.decision_type_flow_json),
        judging_body_flow=parse_json_list(row.judging_body_flow_json),
        decision_progress_flow=parse_json_list(row.decision_progress_flow_json),
        process_class_flow=parse_json_list(row.process_class_flow_json),
        thematic_flow=parse_json_list(row.thematic_flow_json),
    )


def _materialized_minister_flow(session: Session, filters: QueryFilters) -> MinisterFlowResponse:
    minister = filters.minister or ""
    base_where = [
        ServingMinisterFlow.period == (filters.period or ""),
        ServingMinisterFlow.collegiate_filter == filters.collegiate,
        ServingMinisterFlow.judging_body.is_(filters.judging_body)
        if filters.judging_body is None
        else ServingMinisterFlow.judging_body == filters.judging_body,
        ServingMinisterFlow.process_class.is_(filters.process_class)
        if filters.process_class is None
        else ServingMinisterFlow.process_class == filters.process_class,
    ]

    # ── Fast path: exact match on canonical key ──
    row = session.scalar(
        select(ServingMinisterFlow).where(
            ServingMinisterFlow.minister_query == minister,
            *base_where,
        )
    )
    if row is not None:
        return _minister_flow_from_row(row, minister_query=filters.minister, match_mode="exact")

    # ── Slow path: textual search with disambiguation ──
    if not minister:
        return _empty_minister_flow(filters, minister_query=filters.minister)

    # Count distinct minister_query values that match the textual input.
    like_filter = _normalized_like(ServingMinisterFlow.minister_query, minister)
    match_count = session.scalar(
        select(func.count(ServingMinisterFlow.minister_query.distinct())).where(
            like_filter,
            *base_where,
        )
    ) or 0

    if match_count == 0:
        return _empty_minister_flow(filters, minister_query=filters.minister)

    if match_count > 1:
        # Ambiguous: multiple ministers match — return sorted candidates.
        candidates = sorted(
            v
            for v in session.scalars(
                select(ServingMinisterFlow.minister_query.distinct()).where(
                    like_filter,
                    *base_where,
                )
            )
            if v
        )
        return _empty_minister_flow(
            filters,
            minister_query=filters.minister,
            match_mode="ambiguous",
            match_count=match_count,
            candidates=candidates[:10],
        )

    # Exactly one minister matches — deterministic textual resolution.
    row = session.scalar(
        select(ServingMinisterFlow).where(like_filter, *base_where)
    )
    if row is None:
        return _empty_minister_flow(filters, minister_query=filters.minister)
    return _minister_flow_from_row(row, minister_query=filters.minister, match_mode="textual")


def get_minister_flow(session: Session, raw_filters: QueryFilters) -> MinisterFlowResponse:
    resolved = resolve_filters(session, raw_filters)
    return _materialized_minister_flow(session, resolved.filters)
