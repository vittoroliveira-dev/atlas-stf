"""Query functions for rapporteur change endpoints."""

from __future__ import annotations

from typing import cast

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..serving.models import ServingRapporteurChange
from ._filters import _normalized_like
from ._schemas_velocity import (
    PaginatedRapporteurChangeResponse,
    RapporteurChangeItem,
    RapporteurChangeRedFlagsResponse,
)


def _row_to_item(row: ServingRapporteurChange) -> RapporteurChangeItem:
    return RapporteurChangeItem(
        change_id=row.change_id,
        process_id=row.process_id,
        process_class=row.process_class,
        previous_rapporteur=row.previous_rapporteur,
        new_rapporteur=row.new_rapporteur,
        change_date=row.change_date,
        decision_event_id=row.decision_event_id,
        post_change_decision_count=row.post_change_decision_count,
        post_change_favorable_rate=row.post_change_favorable_rate,
        new_rapporteur_baseline_rate=row.new_rapporteur_baseline_rate,
        delta_vs_baseline=row.delta_vs_baseline,
        red_flag=row.red_flag,
    )


def get_rapporteur_changes(
    session: Session,
    page: int,
    page_size: int,
    *,
    minister: str | None = None,
    red_flag_only: bool = False,
) -> PaginatedRapporteurChangeResponse:
    stmt = select(ServingRapporteurChange)
    count_stmt = select(func.count()).select_from(ServingRapporteurChange)

    if minister:
        new_match = _normalized_like(ServingRapporteurChange.new_rapporteur, minister)
        prev_match = _normalized_like(ServingRapporteurChange.previous_rapporteur, minister)
        stmt = stmt.where(new_match | prev_match)
        count_stmt = count_stmt.where(new_match | prev_match)
    if red_flag_only:
        stmt = stmt.where(ServingRapporteurChange.red_flag.is_(True))
        count_stmt = count_stmt.where(ServingRapporteurChange.red_flag.is_(True))

    total = session.execute(count_stmt).scalar_one()
    stmt = stmt.order_by(
        ServingRapporteurChange.red_flag.desc(),
        ServingRapporteurChange.delta_vs_baseline.desc(),
        ServingRapporteurChange.change_id.asc(),
    )
    rows = cast(
        list[ServingRapporteurChange],
        session.scalars(stmt.offset((page - 1) * page_size).limit(page_size)).all(),
    )

    return PaginatedRapporteurChangeResponse(
        total=total,
        page=page,
        page_size=page_size,
        items=[_row_to_item(r) for r in rows],
    )


def get_rapporteur_change_red_flags(session: Session, *, limit: int = 100) -> RapporteurChangeRedFlagsResponse:
    total = (
        session.scalar(
            select(func.count()).select_from(ServingRapporteurChange).where(ServingRapporteurChange.red_flag.is_(True))
        )
        or 0
    )
    rows = cast(
        list[ServingRapporteurChange],
        session.scalars(
            select(ServingRapporteurChange)
            .where(ServingRapporteurChange.red_flag.is_(True))
            .order_by(
                ServingRapporteurChange.delta_vs_baseline.desc(),
                ServingRapporteurChange.change_id.asc(),
            )
            .limit(limit)
        ).all(),
    )
    return RapporteurChangeRedFlagsResponse(
        items=[_row_to_item(r) for r in rows],
        total=total,
    )
