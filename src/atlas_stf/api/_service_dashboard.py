from __future__ import annotations

from sqlalchemy import distinct, func, select
from sqlalchemy.orm import Session

from ..serving.models import ServingAlert, ServingCase
from ._aggregation import _minister_profiles, _top_entities
from ._filters import QueryFilters, _apply_case_filters, resolve_filters
from ._formatters import _alert_to_summary, _case_to_summary, _source_files
from ._service_flow import _materialized_minister_flow
from ._service_ml import _ensemble_score_for, _load_ml_outlier_map
from .schemas import DashboardResponse, MetricsSummary


def _selected_case_subquery(filters: QueryFilters):
    return _apply_case_filters(select(ServingCase.process_id, ServingCase.decision_event_id), filters).subquery()


def _filtered_alert_kpis(session: Session, filters: QueryFilters) -> tuple[int, int, float]:
    stmt = (
        select(
            func.count().label("alert_count"),
            func.count(distinct(ServingAlert.comparison_group_id)).label("group_count"),
            func.avg(ServingAlert.alert_score).label("avg_score"),
        )
        .select_from(ServingAlert)
        .join(ServingCase, ServingCase.decision_event_id == ServingAlert.decision_event_id)
    )
    stmt = _apply_case_filters(stmt, filters)
    row = session.execute(stmt).one()
    return (
        row.alert_count or 0,
        row.group_count or 0,
        float(row.avg_score or 0.0),
    )


def get_dashboard(session: Session, raw_filters: QueryFilters) -> DashboardResponse:
    resolved = resolve_filters(session, raw_filters)
    flow = _materialized_minister_flow(session, resolved.filters)

    filtered_cases_stmt = _apply_case_filters(select(ServingCase), resolved.filters)
    filtered_cases_stmt = filtered_cases_stmt.order_by(
        ServingCase.decision_date.desc(),
        ServingCase.decision_event_id.desc(),
    )
    filtered_cases = session.scalars(filtered_cases_stmt.limit(24)).all()

    alert_stmt = select(ServingAlert, ServingCase).join(
        ServingCase,
        ServingCase.decision_event_id == ServingAlert.decision_event_id,
    )
    alert_stmt = _apply_case_filters(alert_stmt, resolved.filters)
    alert_stmt = alert_stmt.order_by(ServingAlert.alert_score.desc(), ServingAlert.alert_id.asc())
    top_alert_rows = session.execute(alert_stmt.limit(12)).all()
    top_alert_ml_map = _load_ml_outlier_map(
        session,
        [alert.decision_event_id for alert, _ in top_alert_rows],
    )

    selected_cases = _selected_case_subquery(resolved.filters)
    total_events = session.scalar(select(func.count()).select_from(selected_cases)) or 0
    total_processes = session.scalar(select(func.count(distinct(selected_cases.c.process_id)))) or 0

    alert_count, group_count, avg_score = _filtered_alert_kpis(session, resolved.filters)

    return DashboardResponse(
        filters=resolved.options,
        flow=flow,
        kpis=MetricsSummary(
            alert_count=alert_count,
            valid_group_count=group_count,
            baseline_count=group_count,
            average_alert_score=avg_score,
            selected_events=total_events,
            selected_processes=total_processes,
        ),
        source_files=_source_files(session),
        minister_profiles=_minister_profiles(session, resolved.filters),
        top_alerts=[
            _alert_to_summary(
                alert,
                case,
                ensemble_score=_ensemble_score_for(top_alert_ml_map, alert.decision_event_id),
            )
            for alert, case in top_alert_rows
        ],
        case_rows=[_case_to_summary(case) for case in filtered_cases],
        top_counsels=_top_entities(session, "counsel", resolved.filters),
        top_parties=_top_entities(session, "party", resolved.filters),
    )
