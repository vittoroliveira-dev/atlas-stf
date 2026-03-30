"""Query functions for counsel affinity endpoints."""

from __future__ import annotations

from typing import cast

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..serving.models import ServingCounselAffinity
from ._filters import _normalized_like
from ._json_helpers import parse_json_list
from .schemas import (
    CounselAffinityItem,
    CounselAffinityRedFlagsResponse,
    PaginatedCounselAffinityResponse,
)


def _row_to_item(row: ServingCounselAffinity) -> CounselAffinityItem:
    return CounselAffinityItem(
        affinity_id=row.affinity_id,
        rapporteur=row.rapporteur,
        counsel_id=row.counsel_id,
        counsel_name_normalized=row.counsel_name_normalized,
        shared_case_count=row.shared_case_count,
        favorable_count=row.favorable_count,
        unfavorable_count=row.unfavorable_count,
        pair_favorable_rate=row.pair_favorable_rate,
        minister_baseline_favorable_rate=row.minister_baseline_favorable_rate,
        counsel_baseline_favorable_rate=row.counsel_baseline_favorable_rate,
        pair_delta_vs_minister=row.pair_delta_vs_minister,
        pair_delta_vs_counsel=row.pair_delta_vs_counsel,
        red_flag=row.red_flag,
        top_process_classes=parse_json_list(row.top_process_classes_json),
    )


def get_counsel_affinities(
    session: Session,
    page: int,
    page_size: int,
    *,
    minister: str | None = None,
    red_flag_only: bool = False,
) -> PaginatedCounselAffinityResponse:
    stmt = select(ServingCounselAffinity)
    count_stmt = select(func.count()).select_from(ServingCounselAffinity)

    if minister:
        stmt = stmt.where(_normalized_like(ServingCounselAffinity.rapporteur, minister))
        count_stmt = count_stmt.where(_normalized_like(ServingCounselAffinity.rapporteur, minister))
    if red_flag_only:
        stmt = stmt.where(ServingCounselAffinity.red_flag.is_(True))
        count_stmt = count_stmt.where(ServingCounselAffinity.red_flag.is_(True))

    total = session.execute(count_stmt).scalar_one()
    stmt = stmt.order_by(
        ServingCounselAffinity.red_flag.desc(),
        ServingCounselAffinity.pair_delta_vs_minister.desc(),
        ServingCounselAffinity.affinity_id.asc(),
    )
    rows = cast(
        list[ServingCounselAffinity],
        session.scalars(stmt.offset((page - 1) * page_size).limit(page_size)).all(),
    )

    return PaginatedCounselAffinityResponse(
        total=total,
        page=page,
        page_size=page_size,
        items=[_row_to_item(r) for r in rows],
    )


def get_counsel_affinity_red_flags(session: Session, *, limit: int = 100) -> CounselAffinityRedFlagsResponse:
    total = (
        session.scalar(
            select(func.count()).select_from(ServingCounselAffinity).where(ServingCounselAffinity.red_flag.is_(True))
        )
        or 0
    )
    rows = cast(
        list[ServingCounselAffinity],
        session.scalars(
            select(ServingCounselAffinity)
            .where(ServingCounselAffinity.red_flag.is_(True))
            .order_by(
                ServingCounselAffinity.pair_delta_vs_minister.desc(),
                ServingCounselAffinity.affinity_id.asc(),
            )
            .limit(limit)
        ).all(),
    )
    return CounselAffinityRedFlagsResponse(
        items=[_row_to_item(r) for r in rows],
        total=total,
    )


def get_minister_counsel_affinities(session: Session, minister: str, *, limit: int = 100) -> list[CounselAffinityItem]:
    rows = cast(
        list[ServingCounselAffinity],
        session.scalars(
            select(ServingCounselAffinity)
            .where(_normalized_like(ServingCounselAffinity.rapporteur, minister))
            .order_by(
                ServingCounselAffinity.red_flag.desc(),
                ServingCounselAffinity.pair_delta_vs_minister.desc(),
                ServingCounselAffinity.affinity_id.asc(),
            )
            .limit(limit)
        ).all(),
    )
    return [_row_to_item(r) for r in rows]


def get_counsel_minister_affinities(
    session: Session, counsel_id: str, *, limit: int = 100
) -> list[CounselAffinityItem]:
    rows = cast(
        list[ServingCounselAffinity],
        session.scalars(
            select(ServingCounselAffinity)
            .where(ServingCounselAffinity.counsel_id == counsel_id)
            .order_by(
                ServingCounselAffinity.red_flag.desc(),
                ServingCounselAffinity.pair_delta_vs_minister.desc(),
                ServingCounselAffinity.affinity_id.asc(),
            )
            .limit(limit)
        ).all(),
    )
    return [_row_to_item(r) for r in rows]
