"""Query functions for sanction-related endpoints."""

from __future__ import annotations

from typing import cast

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..serving.models import ServingCounselSanctionProfile, ServingSanctionMatch
from .schemas import (
    CounselSanctionProfileItem,
    PaginatedSanctionsResponse,
    SanctionMatchItem,
    SanctionRedFlagsResponse,
)


def _row_to_match_item(row: ServingSanctionMatch) -> SanctionMatchItem:
    return SanctionMatchItem(
        match_id=row.match_id,
        entity_type=row.entity_type,
        party_id=row.party_id,
        counsel_id=row.party_id if row.entity_type == "counsel" else None,
        party_name_normalized=row.party_name_normalized,
        sanction_source=row.sanction_source,
        sanction_id=row.sanction_id,
        sanctioning_body=row.sanctioning_body,
        sanction_type=row.sanction_type,
        sanction_start_date=row.sanction_start_date,
        sanction_end_date=row.sanction_end_date,
        sanction_description=row.sanction_description,
        stf_case_count=row.stf_case_count,
        favorable_rate=row.favorable_rate,
        baseline_favorable_rate=row.baseline_favorable_rate,
        favorable_rate_delta=row.favorable_rate_delta,
        red_flag=row.red_flag,
        match_strategy=row.match_strategy,
        match_score=row.match_score,
        match_confidence=row.match_confidence,
    )


def _row_to_counsel_item(row: ServingCounselSanctionProfile) -> CounselSanctionProfileItem:
    return CounselSanctionProfileItem(
        counsel_id=row.counsel_id,
        counsel_name_normalized=row.counsel_name_normalized,
        sanctioned_client_count=row.sanctioned_client_count,
        total_client_count=row.total_client_count,
        sanctioned_client_rate=row.sanctioned_client_rate,
        sanctioned_favorable_rate=row.sanctioned_favorable_rate,
        overall_favorable_rate=row.overall_favorable_rate,
        red_flag=row.red_flag,
    )


def get_sanctions(
    session: Session,
    page: int,
    page_size: int,
    *,
    source: str | None = None,
    red_flag_only: bool = False,
    entity_type: str | None = None,
) -> PaginatedSanctionsResponse:
    stmt = select(ServingSanctionMatch)
    count_stmt = select(func.count()).select_from(ServingSanctionMatch)

    if entity_type:
        stmt = stmt.where(ServingSanctionMatch.entity_type == entity_type)
        count_stmt = count_stmt.where(ServingSanctionMatch.entity_type == entity_type)
    if source:
        stmt = stmt.where(ServingSanctionMatch.sanction_source == source)
        count_stmt = count_stmt.where(ServingSanctionMatch.sanction_source == source)
    if red_flag_only:
        stmt = stmt.where(ServingSanctionMatch.red_flag.is_(True))
        count_stmt = count_stmt.where(ServingSanctionMatch.red_flag.is_(True))

    total = session.execute(count_stmt).scalar_one()
    stmt = stmt.order_by(
        ServingSanctionMatch.red_flag.desc(),
        ServingSanctionMatch.favorable_rate_delta.desc(),
        ServingSanctionMatch.match_id.asc(),
    )
    rows = cast(
        list[ServingSanctionMatch],
        session.scalars(stmt.offset((page - 1) * page_size).limit(page_size)).all(),
    )

    return PaginatedSanctionsResponse(
        total=total,
        page=page,
        page_size=page_size,
        items=[_row_to_match_item(r) for r in rows],
    )


def get_party_sanctions(session: Session, party_id: str, *, limit: int = 100) -> list[SanctionMatchItem]:
    rows = cast(
        list[ServingSanctionMatch],
        session.scalars(
            select(ServingSanctionMatch)
            .where(ServingSanctionMatch.party_id == party_id)
            .order_by(
                ServingSanctionMatch.red_flag.desc(),
                ServingSanctionMatch.favorable_rate_delta.desc(),
                ServingSanctionMatch.match_id.asc(),
            )
            .limit(limit)
        ).all(),
    )
    return [_row_to_match_item(r) for r in rows]


def get_counsel_sanction_profile(session: Session, counsel_id: str) -> CounselSanctionProfileItem | None:
    row = session.get(ServingCounselSanctionProfile, counsel_id)
    if row is None:
        return None
    return _row_to_counsel_item(row)


def get_sanction_red_flags(session: Session, *, limit: int = 100) -> SanctionRedFlagsResponse:
    total_party_flags = (
        session.scalar(
            select(func.count()).select_from(ServingSanctionMatch).where(ServingSanctionMatch.red_flag.is_(True))
        )
        or 0
    )
    party_flags = cast(
        list[ServingSanctionMatch],
        session.scalars(
            select(ServingSanctionMatch)
            .where(ServingSanctionMatch.red_flag.is_(True))
            .order_by(
                ServingSanctionMatch.favorable_rate_delta.desc(),
                ServingSanctionMatch.match_id.asc(),
            )
            .limit(limit)
        ).all(),
    )
    total_counsel_flags = (
        session.scalar(
            select(func.count())
            .select_from(ServingCounselSanctionProfile)
            .where(ServingCounselSanctionProfile.red_flag.is_(True))
        )
        or 0
    )
    counsel_flags = cast(
        list[ServingCounselSanctionProfile],
        session.scalars(
            select(ServingCounselSanctionProfile)
            .where(ServingCounselSanctionProfile.red_flag.is_(True))
            .order_by(ServingCounselSanctionProfile.counsel_id.asc())
            .limit(limit)
        ).all(),
    )
    return SanctionRedFlagsResponse(
        party_flags=[_row_to_match_item(r) for r in party_flags],
        counsel_flags=[_row_to_counsel_item(r) for r in counsel_flags],
        total_party_flags=total_party_flags,
        total_counsel_flags=total_counsel_flags,
    )
