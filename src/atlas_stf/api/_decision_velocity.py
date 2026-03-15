"""Query functions for decision velocity endpoints."""

from __future__ import annotations

from typing import cast

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..serving.models import ServingDecisionVelocity
from ._filters import _normalized_like
from ._schemas_velocity import (
    DecisionVelocityItem,
    DecisionVelocityRedFlagsResponse,
    PaginatedDecisionVelocityResponse,
)


def _row_to_item(row: ServingDecisionVelocity) -> DecisionVelocityItem:
    return DecisionVelocityItem(
        velocity_id=row.velocity_id,
        decision_event_id=row.decision_event_id,
        process_id=row.process_id,
        current_rapporteur=row.current_rapporteur,
        decision_date=row.decision_date,
        filing_date=row.filing_date,
        days_to_decision=row.days_to_decision,
        process_class=row.process_class,
        thematic_key=row.thematic_key,
        decision_year=row.decision_year,
        group_size=row.group_size,
        p5_days=row.p5_days,
        p10_days=row.p10_days,
        median_days=row.median_days,
        p90_days=row.p90_days,
        p95_days=row.p95_days,
        velocity_flag=row.velocity_flag,
        velocity_z_score=row.velocity_z_score,
    )


def get_decision_velocities(
    session: Session,
    page: int,
    page_size: int,
    *,
    minister: str | None = None,
    flag_only: bool = False,
    velocity_flag: str | None = None,
    process_class: str | None = None,
) -> PaginatedDecisionVelocityResponse:
    stmt = select(ServingDecisionVelocity)
    count_stmt = select(func.count()).select_from(ServingDecisionVelocity)

    if minister:
        stmt = stmt.where(_normalized_like(ServingDecisionVelocity.current_rapporteur, minister))
        count_stmt = count_stmt.where(_normalized_like(ServingDecisionVelocity.current_rapporteur, minister))
    if flag_only:
        stmt = stmt.where(ServingDecisionVelocity.velocity_flag.is_not(None))
        count_stmt = count_stmt.where(ServingDecisionVelocity.velocity_flag.is_not(None))
    if velocity_flag:
        stmt = stmt.where(ServingDecisionVelocity.velocity_flag == velocity_flag)
        count_stmt = count_stmt.where(ServingDecisionVelocity.velocity_flag == velocity_flag)
    if process_class:
        stmt = stmt.where(ServingDecisionVelocity.process_class == process_class)
        count_stmt = count_stmt.where(ServingDecisionVelocity.process_class == process_class)

    total = session.execute(count_stmt).scalar_one()
    stmt = stmt.order_by(
        ServingDecisionVelocity.days_to_decision.asc(),
        ServingDecisionVelocity.velocity_id.asc(),
    )
    rows = cast(
        list[ServingDecisionVelocity],
        session.scalars(stmt.offset((page - 1) * page_size).limit(page_size)).all(),
    )

    return PaginatedDecisionVelocityResponse(
        total=total,
        page=page,
        page_size=page_size,
        items=[_row_to_item(r) for r in rows],
    )


def get_decision_velocity_flags(session: Session, *, limit: int = 100) -> DecisionVelocityRedFlagsResponse:
    total = (
        session.scalar(
            select(func.count())
            .select_from(ServingDecisionVelocity)
            .where(ServingDecisionVelocity.velocity_flag.is_not(None))
        )
        or 0
    )
    rows = cast(
        list[ServingDecisionVelocity],
        session.scalars(
            select(ServingDecisionVelocity)
            .where(ServingDecisionVelocity.velocity_flag.is_not(None))
            .order_by(
                ServingDecisionVelocity.days_to_decision.asc(),
                ServingDecisionVelocity.velocity_id.asc(),
            )
            .limit(limit)
        ).all(),
    )
    return DecisionVelocityRedFlagsResponse(
        items=[_row_to_item(r) for r in rows],
        total=total,
    )
