from __future__ import annotations

from sqlalchemy import distinct, func, select
from sqlalchemy.orm import Session

from ..serving.models import (
    ServingCase,
    ServingCounsel,
    ServingParty,
)
from ._aggregation import _entity_listing_page
from ._filters import EntityKind, QueryFilters, _apply_case_filters, _paginate, resolve_filters
from ._formatters import _case_to_summary, _source_files
from ._service_alerts_cases import _entity_case_ids, _entity_roles
from .schemas import (
    EntityDetailResponse,
    EntitySummaryItem,
    MinisterCorrelationItem,
    PaginatedEntitiesResponse,
)


def _entity_listing(
    session: Session,
    kind: EntityKind,
    raw_filters: QueryFilters,
    page: int,
    page_size: int,
    minister: str | None = None,
) -> PaginatedEntitiesResponse:
    resolved = resolve_filters(session, raw_filters)
    filters = resolved.filters
    if minister is not None:
        filters = QueryFilters(
            minister=minister,
            period=resolved.filters.period,
            collegiate=resolved.filters.collegiate,
            judging_body=resolved.filters.judging_body,
            process_class=resolved.filters.process_class,
        )
        resolved = resolve_filters(session, filters)
    page, page_size = _paginate(page, page_size)
    total, items = _entity_listing_page(session, kind, filters, page=page, page_size=page_size)
    return PaginatedEntitiesResponse(
        filters=resolved.options,
        total=total,
        page=page,
        page_size=page_size,
        items=items,
    )


def get_counsels(session: Session, raw_filters: QueryFilters, page: int, page_size: int) -> PaginatedEntitiesResponse:
    return _entity_listing(session, "counsel", raw_filters, page, page_size)


def get_parties(session: Session, raw_filters: QueryFilters, page: int, page_size: int) -> PaginatedEntitiesResponse:
    return _entity_listing(session, "party", raw_filters, page, page_size)


def _entity_detail(
    session: Session,
    kind: EntityKind,
    entity_id: str,
    raw_filters: QueryFilters,
) -> EntityDetailResponse | None:
    resolved = resolve_filters(session, raw_filters, auto_select_period=False)
    if kind == "counsel":
        entity = session.get(ServingCounsel, entity_id)
        name_raw_attr = "counsel_name_raw"
        name_norm_attr = "counsel_name_normalized"
    else:
        entity = session.get(ServingParty, entity_id)
        name_raw_attr = "party_name_raw"
        name_norm_attr = "party_name_normalized"
    if entity is None:
        return None

    process_ids = _entity_case_ids(session, kind, entity_id)
    filtered_case_base = _apply_case_filters(
        select(ServingCase).where(ServingCase.process_id.in_(process_ids)),
        resolved.filters,
    )
    filtered_case_rows = session.execute(
        _apply_case_filters(
            select(ServingCase.process_id, ServingCase.current_rapporteur).where(
                ServingCase.process_id.in_(process_ids)
            ),
            resolved.filters,
        )
    ).all()
    scoped_process_ids = {row.process_id for row in filtered_case_rows}

    # Accurate counts BEFORE applying limit
    total_event_count = session.scalar(select(func.count()).select_from(filtered_case_base.subquery())) or 0
    total_distinct_processes = (
        session.scalar(select(func.count(distinct(filtered_case_base.subquery().c.process_id)))) or 0
    )

    case_stmt = filtered_case_base.order_by(ServingCase.decision_date.desc(), ServingCase.decision_event_id.asc())
    cases = session.scalars(case_stmt.limit(50)).all()

    minister_stmt = (
        select(
            ServingCase.current_rapporteur,
            func.count(),
            func.count(distinct(ServingCase.process_id)),
        )
        .where(
            ServingCase.process_id.in_(process_ids),
            ServingCase.current_rapporteur.is_not(None),
        )
        .group_by(ServingCase.current_rapporteur)
    )
    minister_stmt = _apply_case_filters(minister_stmt, resolved.filters)
    minister_stmt = minister_stmt.order_by(func.count().desc(), ServingCase.current_rapporteur.asc())
    minister_rows = session.execute(minister_stmt).all()

    return EntityDetailResponse(
        filters=resolved.options,
        entity=EntitySummaryItem(
            id=entity_id,
            name_raw=getattr(entity, name_raw_attr),
            name_normalized=getattr(entity, name_norm_attr),
            associated_event_count=total_event_count,
            distinct_process_count=total_distinct_processes,
            relation_level="decision_derived" if cases else "process_level",
            role_labels=_entity_roles(session, kind, entity_id, process_ids=scoped_process_ids),
        ),
        ministers=[
            MinisterCorrelationItem(
                minister=row[0],
                associated_event_count=row[1],
                distinct_process_count=row[2],
                relation_level="decision_derived",
                role_labels=_entity_roles(
                    session,
                    kind,
                    entity_id,
                    process_ids={
                        case_row.process_id for case_row in filtered_case_rows if case_row.current_rapporteur == row[0]
                    },
                ),
            )
            for row in minister_rows
        ],
        cases=[_case_to_summary(case) for case in cases],
        source_files=_source_files(session),
    )


def get_counsel_detail(session: Session, counsel_id: str, raw_filters: QueryFilters) -> EntityDetailResponse | None:
    return _entity_detail(session, "counsel", counsel_id, raw_filters)


def get_party_detail(session: Session, party_id: str, raw_filters: QueryFilters) -> EntityDetailResponse | None:
    return _entity_detail(session, "party", party_id, raw_filters)


def get_minister_counsels(
    session: Session,
    minister: str,
    raw_filters: QueryFilters,
    page: int,
    page_size: int,
) -> PaginatedEntitiesResponse:
    return _entity_listing(session, "counsel", raw_filters, page, page_size, minister=minister)


def get_minister_parties(
    session: Session,
    minister: str,
    raw_filters: QueryFilters,
    page: int,
    page_size: int,
) -> PaginatedEntitiesResponse:
    return _entity_listing(session, "party", raw_filters, page, page_size, minister=minister)


def get_counsel_ministers(
    session: Session,
    counsel_id: str,
    raw_filters: QueryFilters,
    *,
    limit: int = 100,
) -> list[MinisterCorrelationItem]:
    detail = get_counsel_detail(session, counsel_id, raw_filters)
    return detail.ministers[:limit] if detail else []


def get_party_ministers(
    session: Session,
    party_id: str,
    raw_filters: QueryFilters,
    *,
    limit: int = 100,
) -> list[MinisterCorrelationItem]:
    detail = get_party_detail(session, party_id, raw_filters)
    return detail.ministers[:limit] if detail else []
