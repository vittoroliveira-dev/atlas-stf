from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..serving.models import (
    ServingAlert,
    ServingCase,
    ServingCounsel,
    ServingMlOutlierScore,
    ServingParty,
    ServingProcessCounsel,
    ServingProcessParty,
)
from ._filters import EntityKind, QueryFilters, _apply_case_filters, _paginate, resolve_filters
from ._formatters import _alert_to_summary, _case_to_summary, _source_files
from ._service_flow import _materialized_minister_flow
from ._service_ml import _ensemble_score_for, _load_ml_outlier_map, _to_ml_outlier_response
from .schemas import (
    AlertSummaryItem,
    CaseDetailResponse,
    EntitySummaryItem,
    MlOutlierScoreResponse,
    PaginatedAlertsResponse,
    PaginatedCasesResponse,
)


def _selected_case_subquery(filters: QueryFilters):
    return _apply_case_filters(select(ServingCase.process_id, ServingCase.decision_event_id), filters).subquery()


def _entity_case_ids(session: Session, kind: EntityKind, entity_id: str) -> set[str]:
    if kind == "counsel":
        stmt = select(ServingProcessCounsel.process_id).where(ServingProcessCounsel.counsel_id == entity_id)
    else:
        stmt = select(ServingProcessParty.process_id).where(ServingProcessParty.party_id == entity_id)
    return set(session.scalars(stmt).all())


def _entity_roles(
    session: Session,
    kind: EntityKind,
    entity_id: str,
    *,
    process_ids: set[str] | None = None,
) -> list[str]:
    if process_ids is not None and not process_ids:
        return []
    if kind == "counsel":
        stmt = select(ServingProcessCounsel.side_in_case).where(ServingProcessCounsel.counsel_id == entity_id)
        if process_ids is not None:
            stmt = stmt.where(ServingProcessCounsel.process_id.in_(process_ids))
    else:
        stmt = select(ServingProcessParty.role_in_case).where(ServingProcessParty.party_id == entity_id)
        if process_ids is not None:
            stmt = stmt.where(ServingProcessParty.process_id.in_(process_ids))
    values = session.scalars(stmt).all()
    return sorted({value for value in values if value})


def get_alerts(session: Session, raw_filters: QueryFilters, page: int, page_size: int) -> PaginatedAlertsResponse:
    from ._aggregation import _top_entities

    resolved = resolve_filters(session, raw_filters)
    flow = _materialized_minister_flow(session, resolved.filters)
    page, page_size = _paginate(page, page_size)

    base_stmt = select(ServingAlert, ServingCase).join(
        ServingCase,
        ServingCase.decision_event_id == ServingAlert.decision_event_id,
    )
    base_stmt = _apply_case_filters(base_stmt, resolved.filters)
    total = session.scalar(select(func.count()).select_from(base_stmt.subquery())) or 0
    rows = session.execute(
        base_stmt.order_by(
            (ServingAlert.alert_score + ServingAlert.risk_signal_count * 0.05).desc(),
            ServingAlert.alert_id.asc(),
        )
        .offset((page - 1) * page_size)
        .limit(page_size)
    ).all()
    ml_outlier_map = _load_ml_outlier_map(
        session,
        [alert.decision_event_id for alert, _ in rows],
    )

    return PaginatedAlertsResponse(
        filters=resolved.options,
        flow=flow,
        source_files=_source_files(session),
        total=total,
        page=page,
        page_size=page_size,
        items=[
            _alert_to_summary(
                alert,
                case,
                ensemble_score=_ensemble_score_for(ml_outlier_map, alert.decision_event_id),
            )
            for alert, case in rows
        ],
        top_counsels=_top_entities(session, "counsel", resolved.filters),
        top_parties=_top_entities(session, "party", resolved.filters),
    )


def get_alert_detail(session: Session, alert_id: str) -> AlertSummaryItem | None:
    row = session.execute(
        select(ServingAlert, ServingCase)
        .join(ServingCase, ServingCase.decision_event_id == ServingAlert.decision_event_id)
        .where(ServingAlert.alert_id == alert_id)
    ).first()
    if row is None:
        return None
    alert, case = row
    ml_outlier = session.scalar(
        select(ServingMlOutlierScore).where(ServingMlOutlierScore.decision_event_id == alert.decision_event_id)
    )
    return _alert_to_summary(
        alert,
        case,
        ensemble_score=ml_outlier.ensemble_score if ml_outlier is not None else None,
    )


def get_cases(session: Session, raw_filters: QueryFilters, page: int, page_size: int) -> PaginatedCasesResponse:
    resolved = resolve_filters(session, raw_filters)
    flow = _materialized_minister_flow(session, resolved.filters)
    page, page_size = _paginate(page, page_size)
    stmt = _apply_case_filters(select(ServingCase), resolved.filters)
    total = session.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    stmt = stmt.order_by(ServingCase.decision_date.desc(), ServingCase.decision_event_id.asc())
    items = session.scalars(stmt.offset((page - 1) * page_size).limit(page_size)).all()
    return PaginatedCasesResponse(
        filters=resolved.options,
        flow=flow,
        source_files=_source_files(session),
        total=total,
        page=page,
        page_size=page_size,
        items=[_case_to_summary(item) for item in items],
    )


def get_case_detail(session: Session, raw_filters: QueryFilters, decision_event_id: str) -> CaseDetailResponse:
    resolved = resolve_filters(session, raw_filters)
    flow = _materialized_minister_flow(session, resolved.filters)
    # Contract: case detail by decision_event_id is an exact lookup — the case
    # is identified uniquely by its PK, independent of the active period/minister
    # filter.  Filters are used only for the surrounding context (flow, options).
    case = session.scalar(
        select(ServingCase).where(ServingCase.decision_event_id == decision_event_id)
    )
    if case is None:
        return CaseDetailResponse(
            filters=resolved.options,
            flow=flow,
            source_files=_source_files(session),
            case_item=None,
            ml_outlier_analysis=None,
            related_alerts=[],
            counsels=[],
            parties=[],
        )

    ml_outlier = session.scalar(
        select(ServingMlOutlierScore).where(ServingMlOutlierScore.decision_event_id == decision_event_id)
    )

    related_alert_rows = session.execute(
        select(ServingAlert, ServingCase)
        .join(ServingCase, ServingCase.decision_event_id == ServingAlert.decision_event_id)
        .where(ServingAlert.decision_event_id == decision_event_id)
        .order_by(ServingAlert.alert_score.desc())
    ).all()
    related_alert_ml_map = _load_ml_outlier_map(
        session,
        [alert.decision_event_id for alert, _ in related_alert_rows],
    )

    counsel_rows = session.execute(
        select(
            ServingCounsel.counsel_id,
            ServingCounsel.counsel_name_raw,
            ServingCounsel.counsel_name_normalized,
        )
        .join(ServingProcessCounsel, ServingProcessCounsel.counsel_id == ServingCounsel.counsel_id)
        .where(ServingProcessCounsel.process_id == case.process_id)
        .distinct()
        .order_by(ServingCounsel.counsel_name_normalized.asc())
    ).all()
    counsels = [
        EntitySummaryItem(
            id=row.counsel_id,
            name_raw=row.counsel_name_raw,
            name_normalized=row.counsel_name_normalized,
            associated_event_count=1,
            distinct_process_count=1,
            relation_level="process_level",
            role_labels=_entity_roles(session, "counsel", row.counsel_id, process_ids={case.process_id}),
        )
        for row in counsel_rows
    ]

    party_rows = session.execute(
        select(
            ServingParty.party_id,
            ServingParty.party_name_raw,
            ServingParty.party_name_normalized,
        )
        .join(ServingProcessParty, ServingProcessParty.party_id == ServingParty.party_id)
        .where(ServingProcessParty.process_id == case.process_id)
        .distinct()
        .order_by(ServingParty.party_name_normalized.asc())
    ).all()
    parties = [
        EntitySummaryItem(
            id=row.party_id,
            name_raw=row.party_name_raw,
            name_normalized=row.party_name_normalized,
            associated_event_count=1,
            distinct_process_count=1,
            relation_level="process_level",
            role_labels=_entity_roles(session, "party", row.party_id, process_ids={case.process_id}),
        )
        for row in party_rows
    ]

    return CaseDetailResponse(
        filters=resolved.options,
        flow=flow,
        source_files=_source_files(session),
        case_item=_case_to_summary(case),
        ml_outlier_analysis=_to_ml_outlier_response(ml_outlier) if ml_outlier is not None else None,
        related_alerts=[
            _alert_to_summary(
                alert,
                joined_case,
                ensemble_score=_ensemble_score_for(related_alert_ml_map, alert.decision_event_id),
            )
            for alert, joined_case in related_alert_rows
        ],
        counsels=counsels,
        parties=parties,
    )


def get_case_ml_outlier(
    session: Session,
    raw_filters: QueryFilters,
    decision_event_id: str,
) -> MlOutlierScoreResponse | None:
    # Contract: ml-outlier is identified by decision_event_id (PK) — context
    # filters must not exclude the case itself.
    case_exists = session.scalar(
        select(ServingCase.decision_event_id).where(
            ServingCase.decision_event_id == decision_event_id
        )
    )
    if case_exists is None:
        return None

    ml_outlier = session.scalar(
        select(ServingMlOutlierScore).where(ServingMlOutlierScore.decision_event_id == decision_event_id)
    )
    if ml_outlier is None:
        return None
    return _to_ml_outlier_response(ml_outlier)


def get_related_alerts_for_case(
    session: Session, decision_event_id: str, *, limit: int = 100
) -> list[AlertSummaryItem]:
    rows = session.execute(
        select(ServingAlert, ServingCase)
        .join(ServingCase, ServingCase.decision_event_id == ServingAlert.decision_event_id)
        .where(ServingAlert.decision_event_id == decision_event_id)
        .order_by(ServingAlert.alert_score.desc())
        .limit(limit)
    ).all()
    ml_outlier_map = _load_ml_outlier_map(
        session,
        [alert.decision_event_id for alert, _ in rows],
    )
    return [
        _alert_to_summary(
            alert,
            case,
            ensemble_score=_ensemble_score_for(ml_outlier_map, alert.decision_event_id),
        )
        for alert, case in rows
    ]
