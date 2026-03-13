"""Query functions for corporate network endpoints."""

from __future__ import annotations

import json
from typing import cast

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..serving.models import ServingCorporateConflict
from ._filters import _normalized_like
from .schemas import (
    CorporateConflictItem,
    CorporateConflictRedFlagsResponse,
    PaginatedCorporateConflictsResponse,
)


def _parse_json_list(raw: str | None) -> list:
    if not raw:
        return []
    try:
        return json.loads(raw)
    except json.JSONDecodeError, TypeError:
        return []


def _row_to_item(row: ServingCorporateConflict) -> CorporateConflictItem:
    return CorporateConflictItem(
        conflict_id=row.conflict_id,
        minister_name=row.minister_name,
        company_cnpj_basico=row.company_cnpj_basico,
        company_name=row.company_name,
        minister_qualification=row.minister_qualification,
        linked_entity_type=row.linked_entity_type,
        linked_entity_id=row.linked_entity_id,
        linked_entity_name=row.linked_entity_name,
        entity_qualification=row.entity_qualification,
        shared_process_ids=_parse_json_list(row.shared_process_ids_json),
        shared_process_count=row.shared_process_count,
        favorable_rate=row.favorable_rate,
        baseline_favorable_rate=row.baseline_favorable_rate,
        favorable_rate_delta=row.favorable_rate_delta,
        risk_score=row.risk_score,
        decay_factor=row.decay_factor,
        red_flag=row.red_flag,
        link_chain=row.link_chain,
        link_degree=row.link_degree,
    )


def get_corporate_conflicts(
    session: Session,
    page: int,
    page_size: int,
    *,
    minister: str | None = None,
    red_flag_only: bool = False,
    link_degree: int | None = None,
) -> PaginatedCorporateConflictsResponse:
    stmt = select(ServingCorporateConflict)
    count_stmt = select(func.count()).select_from(ServingCorporateConflict)

    if minister:
        stmt = stmt.where(_normalized_like(ServingCorporateConflict.minister_name, minister))
        count_stmt = count_stmt.where(_normalized_like(ServingCorporateConflict.minister_name, minister))
    if red_flag_only:
        stmt = stmt.where(ServingCorporateConflict.red_flag.is_(True))
        count_stmt = count_stmt.where(ServingCorporateConflict.red_flag.is_(True))
    if link_degree is not None:
        stmt = stmt.where(ServingCorporateConflict.link_degree == link_degree)
        count_stmt = count_stmt.where(ServingCorporateConflict.link_degree == link_degree)

    total = session.execute(count_stmt).scalar_one()
    stmt = stmt.order_by(
        ServingCorporateConflict.red_flag.desc(),
        ServingCorporateConflict.risk_score.desc().nullslast(),
        ServingCorporateConflict.favorable_rate_delta.desc(),
        ServingCorporateConflict.conflict_id.asc(),
    )
    rows = cast(
        list[ServingCorporateConflict],
        session.scalars(stmt.offset((page - 1) * page_size).limit(page_size)).all(),
    )

    return PaginatedCorporateConflictsResponse(
        total=total,
        page=page,
        page_size=page_size,
        items=[_row_to_item(r) for r in rows],
    )


def get_corporate_conflict_red_flags(session: Session, *, limit: int = 100) -> CorporateConflictRedFlagsResponse:
    total = (
        session.scalar(
            select(func.count())
            .select_from(ServingCorporateConflict)
            .where(ServingCorporateConflict.red_flag.is_(True))
        )
        or 0
    )
    rows = cast(
        list[ServingCorporateConflict],
        session.scalars(
            select(ServingCorporateConflict)
            .where(ServingCorporateConflict.red_flag.is_(True))
            .order_by(
                ServingCorporateConflict.risk_score.desc().nullslast(),
                ServingCorporateConflict.favorable_rate_delta.desc(),
                ServingCorporateConflict.conflict_id.asc(),
            )
            .limit(limit)
        ).all(),
    )
    return CorporateConflictRedFlagsResponse(
        items=[_row_to_item(r) for r in rows],
        total=total,
    )


def get_minister_corporate_conflicts(
    session: Session, minister: str, *, limit: int = 100
) -> list[CorporateConflictItem]:
    rows = cast(
        list[ServingCorporateConflict],
        session.scalars(
            select(ServingCorporateConflict)
            .where(_normalized_like(ServingCorporateConflict.minister_name, minister))
            .order_by(
                ServingCorporateConflict.red_flag.desc(),
                ServingCorporateConflict.risk_score.desc().nullslast(),
                ServingCorporateConflict.favorable_rate_delta.desc(),
                ServingCorporateConflict.conflict_id.asc(),
            )
            .limit(limit)
        ).all(),
    )
    return [_row_to_item(r) for r in rows]
