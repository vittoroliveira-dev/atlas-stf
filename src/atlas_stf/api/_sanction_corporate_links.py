"""Query functions for sanction corporate link endpoints."""

from __future__ import annotations

import json
from typing import Literal, cast

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..serving._models_analytics import ServingSanctionCorporateLink
from .schemas import (
    PaginatedSanctionCorporateLinksResponse,
    SanctionCorporateLinkItem,
    SanctionCorporateLinkRedFlagsResponse,
)


def _row_to_item(row: ServingSanctionCorporateLink) -> SanctionCorporateLinkItem:
    return SanctionCorporateLinkItem(
        link_id=row.link_id,
        sanction_id=row.sanction_id,
        sanction_source=row.sanction_source,
        sanction_entity_name=row.sanction_entity_name,
        sanction_entity_tax_id=row.sanction_entity_tax_id,
        sanction_type=row.sanction_type,
        bridge_company_cnpj_basico=row.bridge_company_cnpj_basico,
        bridge_company_name=row.bridge_company_name,
        bridge_link_basis=row.bridge_link_basis,
        bridge_confidence=row.bridge_confidence,
        bridge_partner_role=row.bridge_partner_role,
        bridge_qualification_code=row.bridge_qualification_code,
        bridge_qualification_label=row.bridge_qualification_label,
        economic_group_id=row.economic_group_id,
        economic_group_member_count=row.economic_group_member_count,
        is_law_firm_group=row.is_law_firm_group,
        stf_entity_type=row.stf_entity_type,
        stf_entity_id=row.stf_entity_id,
        stf_entity_name=row.stf_entity_name,
        stf_match_strategy=row.stf_match_strategy,
        stf_match_score=row.stf_match_score,
        stf_match_confidence=row.stf_match_confidence,
        matched_alias=row.matched_alias,
        matched_tax_id=row.matched_tax_id,
        uncertainty_note=row.uncertainty_note,
        link_degree=row.link_degree,
        stf_process_count=row.stf_process_count,
        favorable_rate=row.favorable_rate,
        baseline_favorable_rate=row.baseline_favorable_rate,
        favorable_rate_delta=row.favorable_rate_delta,
        risk_score=row.risk_score,
        red_flag=row.red_flag,
        red_flag_power=row.red_flag_power,
        red_flag_confidence=cast(Literal["high", "moderate", "low"] | None, row.red_flag_confidence),
        evidence_chain=json.loads(row.evidence_chain_json) if row.evidence_chain_json else [],
        source_datasets=json.loads(row.source_datasets_json) if row.source_datasets_json else [],
    )


def get_sanction_corporate_links(
    session: Session,
    page: int,
    page_size: int,
    *,
    sanction_source: str | None = None,
    red_flag_only: bool = False,
    min_degree: int | None = None,
    max_degree: int | None = None,
) -> PaginatedSanctionCorporateLinksResponse:
    stmt = select(ServingSanctionCorporateLink)
    count_stmt = select(func.count()).select_from(ServingSanctionCorporateLink)

    if sanction_source:
        stmt = stmt.where(ServingSanctionCorporateLink.sanction_source == sanction_source)
        count_stmt = count_stmt.where(ServingSanctionCorporateLink.sanction_source == sanction_source)
    if red_flag_only:
        stmt = stmt.where(ServingSanctionCorporateLink.red_flag.is_(True))
        count_stmt = count_stmt.where(ServingSanctionCorporateLink.red_flag.is_(True))
    if min_degree is not None:
        stmt = stmt.where(ServingSanctionCorporateLink.link_degree >= min_degree)
        count_stmt = count_stmt.where(ServingSanctionCorporateLink.link_degree >= min_degree)
    if max_degree is not None:
        stmt = stmt.where(ServingSanctionCorporateLink.link_degree <= max_degree)
        count_stmt = count_stmt.where(ServingSanctionCorporateLink.link_degree <= max_degree)

    total = session.execute(count_stmt).scalar_one()
    stmt = stmt.order_by(
        ServingSanctionCorporateLink.red_flag.desc(),
        ServingSanctionCorporateLink.risk_score.desc(),
        ServingSanctionCorporateLink.link_id.asc(),
    )
    rows = cast(
        list[ServingSanctionCorporateLink],
        session.scalars(stmt.offset((page - 1) * page_size).limit(page_size)).all(),
    )
    return PaginatedSanctionCorporateLinksResponse(
        total=total, page=page, page_size=page_size, items=[_row_to_item(r) for r in rows]
    )


def get_sanction_corporate_link_red_flags(
    session: Session, *, limit: int = 100
) -> SanctionCorporateLinkRedFlagsResponse:
    count = (
        session.scalar(
            select(func.count())
            .select_from(ServingSanctionCorporateLink)
            .where(ServingSanctionCorporateLink.red_flag.is_(True))
        )
        or 0
    )
    rows = cast(
        list[ServingSanctionCorporateLink],
        session.scalars(
            select(ServingSanctionCorporateLink)
            .where(ServingSanctionCorporateLink.red_flag.is_(True))
            .order_by(
                ServingSanctionCorporateLink.risk_score.desc(),
                ServingSanctionCorporateLink.link_id.asc(),
            )
            .limit(limit)
        ).all(),
    )
    return SanctionCorporateLinkRedFlagsResponse(items=[_row_to_item(r) for r in rows], total=count)


def get_party_sanction_corporate_links(
    session: Session, party_id: str, *, limit: int = 100
) -> list[SanctionCorporateLinkItem]:
    rows = cast(
        list[ServingSanctionCorporateLink],
        session.scalars(
            select(ServingSanctionCorporateLink)
            .where(ServingSanctionCorporateLink.stf_entity_id == party_id)
            .order_by(
                ServingSanctionCorporateLink.red_flag.desc(),
                ServingSanctionCorporateLink.risk_score.desc(),
                ServingSanctionCorporateLink.link_id.asc(),
            )
            .limit(limit)
        ).all(),
    )
    return [_row_to_item(r) for r in rows]
