from __future__ import annotations

import json
from typing import Any, cast

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..serving.models import ServingCase, ServingMinisterFlow
from ._filters import QueryFilters, _normalized_like, resolve_filters
from .schemas import MinisterFlowResponse


def _parse_json_dict(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except TypeError, json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_json_list(raw: str | None) -> list[Any]:
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except TypeError, json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def _resolve_materialized_minister_name(session: Session, minister: str | None) -> tuple[str | None, bool]:
    if not minister:
        return None, True
    matches = [
        value
        for value in session.scalars(
            select(ServingCase.current_rapporteur)
            .where(
                ServingCase.current_rapporteur.is_not(None),
                _normalized_like(ServingCase.current_rapporteur, minister),
            )
            .distinct()
            .order_by(ServingCase.current_rapporteur)
        )
        if value
    ]
    return (matches[0], True) if len(matches) == 1 else (None, False)


def _empty_minister_flow(filters: QueryFilters, *, minister_query: str | None) -> MinisterFlowResponse:
    return MinisterFlowResponse(
        minister_query=minister_query or "",
        minister_match_mode="unresolved",
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


def _minister_flow_from_row(row: ServingMinisterFlow, *, minister_query: str | None) -> MinisterFlowResponse:
    return MinisterFlowResponse(
        minister_query=minister_query or row.minister_query,
        minister_match_mode=row.minister_match_mode,
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
        thematic_source_distribution=_parse_json_dict(row.thematic_source_distribution_json),
        historical_thematic_source_distribution=_parse_json_dict(row.historical_thematic_source_distribution_json),
        thematic_flow_interpretation_status=cast(Any, row.thematic_flow_interpretation_status),
        thematic_flow_interpretation_reasons=_parse_json_list(row.thematic_flow_interpretation_reasons_json),
        decision_type_distribution=_parse_json_dict(row.decision_type_distribution_json),
        decision_progress_distribution=_parse_json_dict(row.decision_progress_distribution_json),
        judging_body_distribution=_parse_json_dict(row.judging_body_distribution_json),
        collegiate_distribution=_parse_json_dict(row.collegiate_distribution_json),
        process_class_distribution=_parse_json_dict(row.process_class_distribution_json),
        thematic_distribution=_parse_json_dict(row.thematic_distribution_json),
        daily_counts=_parse_json_list(row.daily_counts_json),
        decision_type_flow=_parse_json_list(row.decision_type_flow_json),
        judging_body_flow=_parse_json_list(row.judging_body_flow_json),
        decision_progress_flow=_parse_json_list(row.decision_progress_flow_json),
        process_class_flow=_parse_json_list(row.process_class_flow_json),
        thematic_flow=_parse_json_list(row.thematic_flow_json),
    )


def _materialized_minister_flow(session: Session, filters: QueryFilters) -> MinisterFlowResponse:
    minister_name, resolved = _resolve_materialized_minister_name(session, filters.minister)
    if not resolved:
        return _empty_minister_flow(filters, minister_query=filters.minister)

    row = session.scalar(
        select(ServingMinisterFlow).where(
            ServingMinisterFlow.period == (filters.period or ""),
            ServingMinisterFlow.collegiate_filter == filters.collegiate,
            ServingMinisterFlow.minister_name.is_(minister_name)
            if minister_name is None
            else ServingMinisterFlow.minister_name == minister_name,
            ServingMinisterFlow.judging_body.is_(filters.judging_body)
            if filters.judging_body is None
            else ServingMinisterFlow.judging_body == filters.judging_body,
            ServingMinisterFlow.process_class.is_(filters.process_class)
            if filters.process_class is None
            else ServingMinisterFlow.process_class == filters.process_class,
        )
    )
    if row is None:
        return _empty_minister_flow(filters, minister_query=filters.minister)
    return _minister_flow_from_row(row, minister_query=filters.minister)


def get_minister_flow(session: Session, raw_filters: QueryFilters) -> MinisterFlowResponse:
    resolved = resolve_filters(session, raw_filters)
    return _materialized_minister_flow(session, resolved.filters)
