from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date
from typing import Iterable, Literal

from sqlalchemy import distinct, func, select
from sqlalchemy.orm import Session

from ..serving.models import (
    ServingAlert,
    ServingCase,
    ServingCounsel,
    ServingParty,
    ServingProcessCounsel,
    ServingProcessParty,
)
from ._filters import CaseSelector, EntityKind, QueryFilters, _apply_case_filters
from ._formatters import _collegiate_label, _format_date
from ._service_flow import _materialized_minister_flow
from .schemas import (
    DailyPoint,
    EntitySummaryItem,
    MinisterFlowResponse,
    MinisterProfileItem,
    SegmentFlowItem,
)


def _group_counter(cases: Iterable[ServingCase], selector: CaseSelector) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for case in cases:
        counter[selector(case) or "INCERTO"] += 1
    return dict(sorted(counter.items(), key=lambda item: (-item[1], item[0])))


def _daily_points(cases: list[ServingCase], historical_average: float) -> list[DailyPoint]:
    by_day: Counter[str] = Counter(_format_date(case.decision_date) for case in cases)
    points: list[DailyPoint] = []
    for day, count in sorted(by_day.items()):
        delta = count - historical_average
        ratio = (count / historical_average) if historical_average > 0 else 0.0
        points.append(
            DailyPoint(
                date=day,
                event_count=count,
                delta_vs_historical_average=round(delta, 6),
                ratio_vs_historical_average=round(ratio, 6),
            )
        )
    return points


def _segment_flow(
    monthly_cases: list[ServingCase],
    historical_cases: list[ServingCase],
    selector: CaseSelector,
) -> list[SegmentFlowItem]:
    monthly_groups: dict[str, list[ServingCase]] = defaultdict(list)
    historical_groups: dict[str, list[ServingCase]] = defaultdict(list)

    for case in monthly_cases:
        monthly_groups[selector(case) or "INCERTO"].append(case)
    for case in historical_cases:
        historical_groups[selector(case) or "INCERTO"].append(case)

    flows: list[SegmentFlowItem] = []
    for value, segment_cases in sorted(monthly_groups.items(), key=lambda item: (-len(item[1]), item[0])):
        hist_cases = historical_groups.get(value, [])
        hist_days = {case.decision_date for case in hist_cases if case.decision_date}
        hist_average = len(hist_cases) / len(hist_days) if hist_days else 0.0
        flows.append(
            SegmentFlowItem(
                segment_value=value,
                event_count=len(segment_cases),
                process_count=len({case.process_id for case in segment_cases}),
                active_day_count=len({case.decision_date for case in segment_cases if case.decision_date}),
                historical_event_count=len(hist_cases),
                historical_active_day_count=len(hist_days),
                historical_average_events_per_active_day=round(hist_average, 6),
                daily_counts=_daily_points(segment_cases, hist_average),
            )
        )
    return flows


def _interpret_thematic_flow(
    monthly_cases: list[ServingCase],
    historical_cases: list[ServingCase],
) -> tuple[Literal["comparativo", "inconclusivo"], list[str]]:
    reasons: list[str] = []
    if not monthly_cases:
        reasons.append("no_events_in_period")
    if len(monthly_cases) < 5:
        reasons.append("event_count_lt_5")
    if len({case.decision_date for case in monthly_cases if case.decision_date}) < 3:
        reasons.append("active_day_count_lt_3")
    if len(historical_cases) < 20:
        reasons.append("historical_event_count_lt_20")
    return ("comparativo" if not reasons else "inconclusivo", reasons)


def build_minister_flow(session: Session, filters: QueryFilters) -> MinisterFlowResponse:
    monthly_stmt = _apply_case_filters(select(ServingCase), filters)
    monthly_stmt = monthly_stmt.order_by(ServingCase.decision_date.asc(), ServingCase.decision_event_id.asc())
    monthly_cases: list[ServingCase] = list(session.scalars(monthly_stmt).all())

    historical_cases: list[ServingCase] = []
    historical_start: date | None = None
    if filters.period:
        historical_start = date.fromisoformat(f"{filters.period}-01")
        historical_filters = QueryFilters(
            minister=filters.minister,
            collegiate=filters.collegiate,
            judging_body=filters.judging_body,
            process_class=filters.process_class,
        )
        historical_stmt = _apply_case_filters(select(ServingCase), historical_filters)
        historical_stmt = historical_stmt.where(
            ServingCase.decision_date.is_not(None),
            ServingCase.decision_date < historical_start,
        )
        historical_stmt = historical_stmt.order_by(ServingCase.decision_date.asc())
        historical_cases = list(session.scalars(historical_stmt).all())

    monthly_days = {case.decision_date for case in monthly_cases if case.decision_date}
    historical_days = {case.decision_date for case in historical_cases if case.decision_date}
    historical_average = len(historical_cases) / len(historical_days) if historical_days else 0.0

    case_ids = [case.decision_event_id for case in monthly_cases]
    linked_alert_count = 0
    if case_ids:
        linked_alert_count = (
            session.scalar(
                select(func.count()).select_from(ServingAlert).where(ServingAlert.decision_event_id.in_(case_ids))
            )
            or 0
        )

    thematic_status, thematic_reasons = _interpret_thematic_flow(monthly_cases, historical_cases)

    return MinisterFlowResponse(
        minister_query=filters.minister or "",
        minister_reference=filters.minister,
        period=filters.period or "",
        status="empty" if not monthly_cases else "ok",
        collegiate_filter=filters.collegiate,
        event_count=len(monthly_cases),
        process_count=len({case.process_id for case in monthly_cases}),
        active_day_count=len(monthly_days),
        first_decision_date=min((case.decision_date for case in monthly_cases if case.decision_date), default=None),
        last_decision_date=max((case.decision_date for case in monthly_cases if case.decision_date), default=None),
        historical_reference_period_start=min(
            (case.decision_date for case in historical_cases if case.decision_date),
            default=None,
        ),
        historical_reference_period_end=max(
            (case.decision_date for case in historical_cases if case.decision_date),
            default=None,
        ),
        historical_event_count=len(historical_cases),
        historical_active_day_count=len(historical_days),
        historical_average_events_per_active_day=round(historical_average, 6),
        linked_alert_count=linked_alert_count,
        thematic_key_rule="first_subject_normalized_else_branch_of_law",
        thematic_source_distribution=({"serving_process_thematic_key": len(monthly_cases)} if monthly_cases else {}),
        historical_thematic_source_distribution=(
            {"serving_process_thematic_key": len(historical_cases)} if historical_cases else {}
        ),
        thematic_flow_interpretation_status=thematic_status,
        thematic_flow_interpretation_reasons=thematic_reasons,
        decision_type_distribution=_group_counter(monthly_cases, lambda case: case.decision_type),
        decision_progress_distribution=_group_counter(monthly_cases, lambda case: case.decision_progress),
        judging_body_distribution=_group_counter(monthly_cases, lambda case: case.judging_body),
        collegiate_distribution=_group_counter(monthly_cases, lambda case: _collegiate_label(case.is_collegiate)),
        process_class_distribution=_group_counter(monthly_cases, lambda case: case.process_class),
        thematic_distribution=_group_counter(monthly_cases, lambda case: case.thematic_key),
        daily_counts=_daily_points(monthly_cases, historical_average),
        decision_type_flow=_segment_flow(monthly_cases, historical_cases, lambda case: case.decision_type),
        judging_body_flow=_segment_flow(monthly_cases, historical_cases, lambda case: case.judging_body),
        decision_progress_flow=_segment_flow(monthly_cases, historical_cases, lambda case: case.decision_progress),
        process_class_flow=_segment_flow(monthly_cases, historical_cases, lambda case: case.process_class),
        thematic_flow=_segment_flow(monthly_cases, historical_cases, lambda case: case.thematic_key),
    )


def _entity_lookup_rows(session: Session, kind: EntityKind):
    if kind == "counsel":
        rows = session.execute(
            select(
                ServingCounsel.counsel_id,
                ServingCounsel.counsel_name_raw,
                ServingCounsel.counsel_name_normalized,
            )
        ).all()
        return {row.counsel_id: (row.counsel_name_raw, row.counsel_name_normalized) for row in rows}
    rows = session.execute(
        select(
            ServingParty.party_id,
            ServingParty.party_name_raw,
            ServingParty.party_name_normalized,
        )
    ).all()
    return {row.party_id: (row.party_name_raw, row.party_name_normalized) for row in rows}


def _entity_link_rows(session: Session, kind: EntityKind, process_ids: set[str]):
    if not process_ids:
        return []
    if kind == "counsel":
        stmt = select(
            ServingProcessCounsel.process_id,
            ServingProcessCounsel.counsel_id,
            ServingProcessCounsel.side_in_case,
        ).where(ServingProcessCounsel.process_id.in_(process_ids))
    else:
        stmt = select(
            ServingProcessParty.process_id,
            ServingProcessParty.party_id,
            ServingProcessParty.role_in_case,
        ).where(ServingProcessParty.process_id.in_(process_ids))
    return session.execute(stmt).all()


def _entity_listing_page(
    session: Session,
    kind: EntityKind,
    filters: QueryFilters,
    *,
    page: int,
    page_size: int,
) -> tuple[int, list[EntitySummaryItem]]:
    selected_cases = _apply_case_filters(
        select(ServingCase.process_id, ServingCase.decision_event_id),
        filters,
    ).subquery()

    if kind == "counsel":
        entity_model = ServingCounsel
        entity_id_col = ServingCounsel.counsel_id
        entity_raw_col = ServingCounsel.counsel_name_raw
        entity_normalized_col = ServingCounsel.counsel_name_normalized
        link_model = ServingProcessCounsel
        link_entity_col = ServingProcessCounsel.counsel_id
        link_role_col = ServingProcessCounsel.side_in_case
    else:
        entity_model = ServingParty
        entity_id_col = ServingParty.party_id
        entity_raw_col = ServingParty.party_name_raw
        entity_normalized_col = ServingParty.party_name_normalized
        link_model = ServingProcessParty
        link_entity_col = ServingProcessParty.party_id
        link_role_col = ServingProcessParty.role_in_case

    total = (
        session.scalar(
            select(func.count(distinct(link_entity_col)))
            .select_from(link_model)
            .join(
                selected_cases,
                selected_cases.c.process_id == link_model.process_id,
            )
        )
        or 0
    )

    if total == 0:
        return 0, []

    aggregate_stmt = (
        select(
            entity_id_col.label("entity_id"),
            entity_raw_col.label("name_raw"),
            entity_normalized_col.label("name_normalized"),
            func.count(selected_cases.c.decision_event_id).label("associated_event_count"),
            func.count(distinct(selected_cases.c.process_id)).label("distinct_process_count"),
        )
        .select_from(link_model)
        .join(selected_cases, selected_cases.c.process_id == link_model.process_id)
        .join(entity_model, entity_id_col == link_entity_col)
        .group_by(entity_id_col, entity_raw_col, entity_normalized_col)
        .order_by(
            func.count(selected_cases.c.decision_event_id).desc(),
            func.count(distinct(selected_cases.c.process_id)).desc(),
            entity_normalized_col.asc(),
        )
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    aggregate_rows = session.execute(aggregate_stmt).all()
    if not aggregate_rows:
        return total, []

    entity_ids = [row.entity_id for row in aggregate_rows]
    role_stmt = (
        select(link_entity_col, link_role_col)
        .select_from(link_model)
        .join(selected_cases, selected_cases.c.process_id == link_model.process_id)
        .where(link_entity_col.in_(entity_ids))
    )
    role_rows = session.execute(role_stmt).all()
    roles_by_entity: dict[str, set[str]] = defaultdict(set)
    for entity_id, role_label in role_rows:
        if role_label:
            roles_by_entity[entity_id].add(role_label)

    items = [
        EntitySummaryItem(
            id=row.entity_id,
            name_raw=row.name_raw,
            name_normalized=row.name_normalized,
            associated_event_count=row.associated_event_count,
            distinct_process_count=row.distinct_process_count,
            relation_level="decision_derived",
            role_labels=sorted(roles_by_entity[row.entity_id]),
        )
        for row in aggregate_rows
    ]
    return total, items


def _top_entities(
    session: Session,
    kind: EntityKind,
    filters: QueryFilters,
    limit: int = 8,
) -> list[EntitySummaryItem]:
    _, items = _entity_listing_page(session, kind, filters, page=1, page_size=limit)
    return items


def _minister_profiles(session: Session, filters: QueryFilters, limit: int = 6) -> list[MinisterProfileItem]:
    if not filters.period:
        return []
    profile_filters = QueryFilters(
        period=filters.period,
        collegiate=filters.collegiate,
        judging_body=filters.judging_body,
        process_class=filters.process_class,
    )
    stmt = _apply_case_filters(
        select(ServingCase.current_rapporteur, func.count())
        .where(ServingCase.current_rapporteur.is_not(None))
        .group_by(ServingCase.current_rapporteur),
        profile_filters,
    ).order_by(func.count().desc(), ServingCase.current_rapporteur.asc())
    ministers = [row[0] for row in session.execute(stmt).all()[:limit] if row[0]]

    profiles: list[MinisterProfileItem] = []
    for minister in ministers:
        flow = _materialized_minister_flow(
            session,
            QueryFilters(
                minister=minister,
                period=filters.period,
                collegiate=filters.collegiate,
                judging_body=filters.judging_body,
                process_class=filters.process_class,
            ),
        )
        profiles.append(
            MinisterProfileItem(
                minister=minister,
                period=filters.period,
                collegiate=filters.collegiate,
                event_count=flow.event_count,
                historical_average=flow.historical_average_events_per_active_day,
                linked_alert_count=flow.linked_alert_count,
                process_classes=list(flow.process_class_distribution.keys())[:3],
                themes=list(flow.thematic_distribution.keys())[:3],
            )
        )
    return profiles
