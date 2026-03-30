"""Query functions for economic groups endpoint."""

from __future__ import annotations

from typing import cast

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..serving.models import ServingEconomicGroup
from ._json_helpers import parse_json_list
from .schemas import (
    EconomicGroupItem,
    PaginatedEconomicGroupResponse,
)


def _row_to_item(row: ServingEconomicGroup) -> EconomicGroupItem:
    return EconomicGroupItem(
        group_id=row.group_id,
        member_cnpjs=parse_json_list(row.member_cnpjs_json),
        razoes_sociais=parse_json_list(row.razoes_sociais_json),
        member_count=row.member_count,
        total_capital_social=row.total_capital_social,
        cnae_labels=parse_json_list(row.cnae_labels_json),
        ufs=parse_json_list(row.ufs_json),
        active_establishment_count=row.active_establishment_count,
        total_establishment_count=row.total_establishment_count,
        is_law_firm_group=row.is_law_firm_group,
        law_firm_member_count=row.law_firm_member_count,
        law_firm_member_ratio=row.law_firm_member_ratio,
        has_minister_partner=row.has_minister_partner,
        has_party_partner=row.has_party_partner,
        has_counsel_partner=row.has_counsel_partner,
    )


def get_economic_groups(
    session: Session,
    page: int,
    page_size: int,
    *,
    minister_only: bool = False,
    party_only: bool = False,
    counsel_only: bool = False,
    law_firm_only: bool = False,
) -> PaginatedEconomicGroupResponse:
    stmt = select(ServingEconomicGroup)
    count_stmt = select(func.count()).select_from(ServingEconomicGroup)

    if minister_only:
        stmt = stmt.where(ServingEconomicGroup.has_minister_partner.is_(True))
        count_stmt = count_stmt.where(ServingEconomicGroup.has_minister_partner.is_(True))
    if party_only:
        stmt = stmt.where(ServingEconomicGroup.has_party_partner.is_(True))
        count_stmt = count_stmt.where(ServingEconomicGroup.has_party_partner.is_(True))
    if counsel_only:
        stmt = stmt.where(ServingEconomicGroup.has_counsel_partner.is_(True))
        count_stmt = count_stmt.where(ServingEconomicGroup.has_counsel_partner.is_(True))
    if law_firm_only:
        stmt = stmt.where(ServingEconomicGroup.is_law_firm_group.is_(True))
        count_stmt = count_stmt.where(ServingEconomicGroup.is_law_firm_group.is_(True))

    total = session.execute(count_stmt).scalar_one()
    stmt = stmt.order_by(
        ServingEconomicGroup.member_count.desc(),
        ServingEconomicGroup.group_id.asc(),
    )
    rows = cast(
        list[ServingEconomicGroup],
        session.scalars(stmt.offset((page - 1) * page_size).limit(page_size)).all(),
    )

    return PaginatedEconomicGroupResponse(
        total=total,
        page=page,
        page_size=page_size,
        items=[_row_to_item(r) for r in rows],
    )


def get_economic_group_by_id(
    session: Session,
    group_id: str,
) -> EconomicGroupItem | None:
    row = session.get(ServingEconomicGroup, group_id)
    if row is None:
        return None
    return _row_to_item(row)
