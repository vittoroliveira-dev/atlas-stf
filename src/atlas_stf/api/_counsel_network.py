"""Query functions for counsel network cluster endpoints."""

from __future__ import annotations

import json
import logging
from typing import cast

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..serving.models import ServingCounselNetworkCluster
from ._schemas_velocity import (
    CounselNetworkClusterItem,
    CounselNetworkRedFlagsResponse,
    PaginatedCounselNetworkResponse,
)

logger = logging.getLogger(__name__)


def _parse_json_list(raw: str | None) -> list:
    if not raw:
        return []
    try:
        return json.loads(raw)
    except json.JSONDecodeError, TypeError:
        logger.warning("Failed to parse JSON list: %r", raw[:200] if raw else raw)
        return []


def _row_to_item(row: ServingCounselNetworkCluster) -> CounselNetworkClusterItem:
    return CounselNetworkClusterItem(
        cluster_id=row.cluster_id,
        counsel_ids=_parse_json_list(row.counsel_ids_json),
        counsel_names=_parse_json_list(row.counsel_names_json),
        cluster_size=row.cluster_size,
        shared_client_count=row.shared_client_count,
        shared_process_count=row.shared_process_count,
        minister_names=_parse_json_list(row.minister_names_json),
        cluster_favorable_rate=row.cluster_favorable_rate,
        cluster_case_count=row.cluster_case_count,
        red_flag=row.red_flag,
    )


def get_counsel_network_clusters(
    session: Session,
    page: int,
    page_size: int,
    *,
    red_flag_only: bool = False,
) -> PaginatedCounselNetworkResponse:
    stmt = select(ServingCounselNetworkCluster)
    count_stmt = select(func.count()).select_from(ServingCounselNetworkCluster)

    if red_flag_only:
        stmt = stmt.where(ServingCounselNetworkCluster.red_flag.is_(True))
        count_stmt = count_stmt.where(ServingCounselNetworkCluster.red_flag.is_(True))

    total = session.execute(count_stmt).scalar_one()
    stmt = stmt.order_by(
        ServingCounselNetworkCluster.red_flag.desc(),
        ServingCounselNetworkCluster.cluster_size.desc(),
        ServingCounselNetworkCluster.cluster_id.asc(),
    )
    rows = cast(
        list[ServingCounselNetworkCluster],
        session.scalars(stmt.offset((page - 1) * page_size).limit(page_size)).all(),
    )

    return PaginatedCounselNetworkResponse(
        total=total,
        page=page,
        page_size=page_size,
        items=[_row_to_item(r) for r in rows],
    )


def get_counsel_network_red_flags(session: Session, *, limit: int = 100) -> CounselNetworkRedFlagsResponse:
    total = (
        session.scalar(
            select(func.count())
            .select_from(ServingCounselNetworkCluster)
            .where(ServingCounselNetworkCluster.red_flag.is_(True))
        )
        or 0
    )
    rows = cast(
        list[ServingCounselNetworkCluster],
        session.scalars(
            select(ServingCounselNetworkCluster)
            .where(ServingCounselNetworkCluster.red_flag.is_(True))
            .order_by(
                ServingCounselNetworkCluster.cluster_size.desc(),
                ServingCounselNetworkCluster.cluster_id.asc(),
            )
            .limit(limit)
        ).all(),
    )
    return CounselNetworkRedFlagsResponse(
        items=[_row_to_item(r) for r in rows],
        total=total,
    )
