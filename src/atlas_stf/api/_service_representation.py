"""Query functions for representation network endpoints."""

from __future__ import annotations

from typing import cast

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..serving._models_representation import (
    ServingLawFirmEntity,
    ServingLawyerEntity,
    ServingRepresentationEdge,
    ServingRepresentationEvent,
)
from ._filters import _normalized_like
from ._schemas_representation import (
    FirmDetailResponse,
    LawFirmEntityItem,
    LawyerDetailResponse,
    LawyerEntityItem,
    PaginatedFirmsResponse,
    PaginatedLawyersResponse,
    ProcessRepresentationResponse,
    RepresentationEdgeItem,
    RepresentationEventItem,
    RepresentationNetworkSummary,
)


def _lawyer_to_item(row: ServingLawyerEntity) -> LawyerEntityItem:
    return LawyerEntityItem(
        lawyer_id=row.lawyer_id,
        lawyer_name_raw=row.lawyer_name_raw,
        lawyer_name_normalized=row.lawyer_name_normalized,
        canonical_name_normalized=row.canonical_name_normalized,
        oab_number=row.oab_number,
        oab_state=row.oab_state,
        oab_status=row.oab_status,
        oab_source=row.oab_source,
        identity_key=row.identity_key,
        identity_strategy=row.identity_strategy,
        firm_id=row.firm_id,
        process_count=row.process_count,
        event_count=row.event_count,
        first_seen_date=str(row.first_seen_date) if row.first_seen_date else None,
        last_seen_date=str(row.last_seen_date) if row.last_seen_date else None,
    )


def _firm_to_item(row: ServingLawFirmEntity) -> LawFirmEntityItem:
    return LawFirmEntityItem(
        firm_id=row.firm_id,
        firm_name_raw=row.firm_name_raw,
        firm_name_normalized=row.firm_name_normalized,
        canonical_name_normalized=row.canonical_name_normalized,
        cnpj=row.cnpj,
        cnpj_valid=row.cnpj_valid,
        cnsa_number=row.cnsa_number,
        identity_key=row.identity_key,
        identity_strategy=row.identity_strategy,
        member_count=row.member_count,
        process_count=row.process_count,
        first_seen_date=str(row.first_seen_date) if row.first_seen_date else None,
        last_seen_date=str(row.last_seen_date) if row.last_seen_date else None,
    )


def _edge_to_item(row: ServingRepresentationEdge) -> RepresentationEdgeItem:
    return RepresentationEdgeItem(
        edge_id=row.edge_id,
        process_id=row.process_id,
        representative_entity_id=row.representative_entity_id,
        representative_kind=row.representative_kind,
        role_type=row.role_type,
        lawyer_id=row.lawyer_id,
        firm_id=row.firm_id,
        party_id=row.party_id,
        event_count=row.event_count,
        start_date=str(row.start_date) if row.start_date else None,
        end_date=str(row.end_date) if row.end_date else None,
        confidence=row.confidence,
    )


def _event_to_item(row: ServingRepresentationEvent) -> RepresentationEventItem:
    return RepresentationEventItem(
        event_id=row.event_id,
        process_id=row.process_id,
        edge_id=row.edge_id,
        lawyer_id=row.lawyer_id,
        firm_id=row.firm_id,
        event_type=row.event_type,
        event_date=str(row.event_date) if row.event_date else None,
        event_description=row.event_description,
        protocol_number=row.protocol_number,
        document_type=row.document_type,
        source_system=row.source_system,
        confidence=row.confidence,
    )


def get_lawyers(
    session: Session,
    page: int,
    page_size: int,
    *,
    name: str | None = None,
    oab_state: str | None = None,
    firm_id: str | None = None,
) -> PaginatedLawyersResponse:
    stmt = select(ServingLawyerEntity)
    count_stmt = select(func.count()).select_from(ServingLawyerEntity)

    if name:
        stmt = stmt.where(_normalized_like(ServingLawyerEntity.lawyer_name_normalized, name))
        count_stmt = count_stmt.where(_normalized_like(ServingLawyerEntity.lawyer_name_normalized, name))
    if oab_state:
        stmt = stmt.where(ServingLawyerEntity.oab_state == oab_state.upper())
        count_stmt = count_stmt.where(ServingLawyerEntity.oab_state == oab_state.upper())
    if firm_id:
        stmt = stmt.where(ServingLawyerEntity.firm_id == firm_id)
        count_stmt = count_stmt.where(ServingLawyerEntity.firm_id == firm_id)

    total = session.execute(count_stmt).scalar_one()
    stmt = stmt.order_by(
        ServingLawyerEntity.process_count.desc(),
        ServingLawyerEntity.lawyer_id.asc(),
    )
    rows = cast(
        list[ServingLawyerEntity],
        session.scalars(stmt.offset((page - 1) * page_size).limit(page_size)).all(),
    )

    return PaginatedLawyersResponse(
        total=total,
        page=page,
        page_size=page_size,
        items=[_lawyer_to_item(r) for r in rows],
    )


def get_lawyer_detail(session: Session, lawyer_id: str) -> LawyerDetailResponse | None:
    lawyer = session.get(ServingLawyerEntity, lawyer_id)
    if lawyer is None:
        return None

    edges = cast(
        list[ServingRepresentationEdge],
        session.scalars(
            select(ServingRepresentationEdge)
            .where(ServingRepresentationEdge.lawyer_id == lawyer_id)
            .order_by(ServingRepresentationEdge.start_date.desc(), ServingRepresentationEdge.edge_id.asc())
            .limit(200)
        ).all(),
    )
    events = cast(
        list[ServingRepresentationEvent],
        session.scalars(
            select(ServingRepresentationEvent)
            .where(ServingRepresentationEvent.lawyer_id == lawyer_id)
            .order_by(ServingRepresentationEvent.event_date.desc(), ServingRepresentationEvent.event_id.asc())
            .limit(200)
        ).all(),
    )

    return LawyerDetailResponse(
        lawyer=_lawyer_to_item(lawyer),
        edges=[_edge_to_item(e) for e in edges],
        events=[_event_to_item(e) for e in events],
    )


def get_firms(
    session: Session,
    page: int,
    page_size: int,
    *,
    name: str | None = None,
) -> PaginatedFirmsResponse:
    stmt = select(ServingLawFirmEntity)
    count_stmt = select(func.count()).select_from(ServingLawFirmEntity)

    if name:
        stmt = stmt.where(_normalized_like(ServingLawFirmEntity.firm_name_normalized, name))
        count_stmt = count_stmt.where(_normalized_like(ServingLawFirmEntity.firm_name_normalized, name))

    total = session.execute(count_stmt).scalar_one()
    stmt = stmt.order_by(
        ServingLawFirmEntity.process_count.desc(),
        ServingLawFirmEntity.firm_id.asc(),
    )
    rows = cast(
        list[ServingLawFirmEntity],
        session.scalars(stmt.offset((page - 1) * page_size).limit(page_size)).all(),
    )

    return PaginatedFirmsResponse(
        total=total,
        page=page,
        page_size=page_size,
        items=[_firm_to_item(r) for r in rows],
    )


def get_firm_detail(session: Session, firm_id: str) -> FirmDetailResponse | None:
    firm = session.get(ServingLawFirmEntity, firm_id)
    if firm is None:
        return None

    lawyers = cast(
        list[ServingLawyerEntity],
        session.scalars(
            select(ServingLawyerEntity)
            .where(ServingLawyerEntity.firm_id == firm_id)
            .order_by(ServingLawyerEntity.process_count.desc(), ServingLawyerEntity.lawyer_id.asc())
            .limit(200)
        ).all(),
    )

    return FirmDetailResponse(
        firm=_firm_to_item(firm),
        lawyers=[_lawyer_to_item(row) for row in lawyers],
    )


def get_process_representation(session: Session, process_id: str) -> ProcessRepresentationResponse:
    edges = cast(
        list[ServingRepresentationEdge],
        session.scalars(
            select(ServingRepresentationEdge)
            .where(ServingRepresentationEdge.process_id == process_id)
            .order_by(ServingRepresentationEdge.edge_id.asc())
            .limit(200)
        ).all(),
    )
    events = cast(
        list[ServingRepresentationEvent],
        session.scalars(
            select(ServingRepresentationEvent)
            .where(ServingRepresentationEvent.process_id == process_id)
            .order_by(ServingRepresentationEvent.event_date.desc(), ServingRepresentationEvent.event_id.asc())
            .limit(500)
        ).all(),
    )

    return ProcessRepresentationResponse(
        process_id=process_id,
        edges=[_edge_to_item(e) for e in edges],
        events=[_event_to_item(e) for e in events],
    )


def get_representation_events(
    session: Session,
    page: int,
    page_size: int,
    *,
    lawyer_id: str | None = None,
    firm_id: str | None = None,
    event_type: str | None = None,
) -> tuple[int, list[RepresentationEventItem]]:
    stmt = select(ServingRepresentationEvent)
    count_stmt = select(func.count()).select_from(ServingRepresentationEvent)

    if lawyer_id:
        stmt = stmt.where(ServingRepresentationEvent.lawyer_id == lawyer_id)
        count_stmt = count_stmt.where(ServingRepresentationEvent.lawyer_id == lawyer_id)
    if firm_id:
        stmt = stmt.where(ServingRepresentationEvent.firm_id == firm_id)
        count_stmt = count_stmt.where(ServingRepresentationEvent.firm_id == firm_id)
    if event_type:
        stmt = stmt.where(ServingRepresentationEvent.event_type == event_type)
        count_stmt = count_stmt.where(ServingRepresentationEvent.event_type == event_type)

    total = session.execute(count_stmt).scalar_one()
    stmt = stmt.order_by(
        ServingRepresentationEvent.event_date.desc(),
        ServingRepresentationEvent.event_id.asc(),
    )
    rows = cast(
        list[ServingRepresentationEvent],
        session.scalars(stmt.offset((page - 1) * page_size).limit(page_size)).all(),
    )

    return total, [_event_to_item(r) for r in rows]


def get_representation_summary(session: Session) -> RepresentationNetworkSummary:
    total_lawyers = session.scalar(select(func.count()).select_from(ServingLawyerEntity)) or 0
    total_firms = session.scalar(select(func.count()).select_from(ServingLawFirmEntity)) or 0
    total_edges = session.scalar(select(func.count()).select_from(ServingRepresentationEdge)) or 0
    total_events = session.scalar(select(func.count()).select_from(ServingRepresentationEvent)) or 0
    lawyers_with_oab = (
        session.scalar(
            select(func.count()).select_from(ServingLawyerEntity).where(ServingLawyerEntity.oab_number.is_not(None))
        )
        or 0
    )
    lawyers_with_firm = (
        session.scalar(
            select(func.count()).select_from(ServingLawyerEntity).where(ServingLawyerEntity.firm_id.is_not(None))
        )
        or 0
    )

    return RepresentationNetworkSummary(
        total_lawyers=total_lawyers,
        total_firms=total_firms,
        total_edges=total_edges,
        total_events=total_events,
        lawyers_with_oab=lawyers_with_oab,
        lawyers_with_firm=lawyers_with_firm,
    )
