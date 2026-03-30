"""Query functions for compound risk endpoints."""

from __future__ import annotations

import json
import logging
from typing import Any, cast

from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..serving.models import ServingCompoundRisk
from ._filters import _normalized_like
from ._json_helpers import parse_json_list
from .schemas import (
    CompoundRiskCompanyItem,
    CompoundRiskHeatmapCell,
    CompoundRiskHeatmapEntity,
    CompoundRiskHeatmapResponse,
    CompoundRiskItem,
    CompoundRiskRedFlagsResponse,
    PaginatedCompoundRiskResponse,
)

logger = logging.getLogger(__name__)


def _parse_company_items(raw: str | None) -> list[CompoundRiskCompanyItem]:
    items: list[CompoundRiskCompanyItem] = []
    for company in parse_json_list(raw):
        if not isinstance(company, dict):
            continue
        try:
            items.append(CompoundRiskCompanyItem.model_validate(company))
        except ValidationError:
            logger.debug("Skipping invalid company item: %s", company)
    return items


def _parse_signal_details(raw: str | None) -> dict[str, dict[str, Any]] | None:
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) and parsed else None
    except TypeError, json.JSONDecodeError:
        return None


def _row_to_item(row: ServingCompoundRisk) -> CompoundRiskItem:
    return CompoundRiskItem(
        pair_id=row.pair_id,
        minister_name=row.minister_name,
        entity_type=row.entity_type,
        entity_id=row.entity_id,
        entity_name=row.entity_name,
        signal_count=row.signal_count,
        signals=[str(value) for value in parse_json_list(row.signals_json)],
        red_flag=row.red_flag,
        shared_process_count=row.shared_process_count,
        shared_process_ids=[str(value) for value in parse_json_list(row.shared_process_ids_json)],
        alert_count=row.alert_count,
        alert_ids=[str(value) for value in parse_json_list(row.alert_ids_json)],
        max_alert_score=row.max_alert_score,
        max_rate_delta=row.max_rate_delta,
        sanction_match_count=row.sanction_match_count,
        sanction_sources=[str(value) for value in parse_json_list(row.sanction_sources_json)],
        donation_match_count=row.donation_match_count,
        donation_total_brl=row.donation_total_brl,
        corporate_conflict_count=row.corporate_conflict_count,
        corporate_conflict_ids=[str(value) for value in parse_json_list(row.corporate_conflict_ids_json)],
        corporate_companies=_parse_company_items(row.corporate_companies_json),
        affinity_count=row.affinity_count,
        affinity_ids=[str(value) for value in parse_json_list(row.affinity_ids_json)],
        top_process_classes=[str(value) for value in parse_json_list(row.top_process_classes_json)],
        supporting_party_ids=[str(value) for value in parse_json_list(row.supporting_party_ids_json)],
        supporting_party_names=[str(value) for value in parse_json_list(row.supporting_party_names_json)],
        signal_details=_parse_signal_details(row.signal_details_json),
        earliest_year=row.earliest_year,
        latest_year=row.latest_year,
        sanction_corporate_link_count=row.sanction_corporate_link_count,
        sanction_corporate_link_ids=[str(v) for v in parse_json_list(row.sanction_corporate_link_ids_json)],
        sanction_corporate_min_degree=row.sanction_corporate_min_degree,
        adjusted_rate_delta=row.adjusted_rate_delta,
        has_law_firm_group=row.has_law_firm_group,
        donor_group_has_minister_partner=row.donor_group_has_minister_partner,
        donor_group_has_party_partner=row.donor_group_has_party_partner,
        donor_group_has_counsel_partner=row.donor_group_has_counsel_partner,
        min_link_degree_to_minister=row.min_link_degree_to_minister,
    )


def _filtered_stmt(
    *,
    minister: str | None = None,
    entity_type: str | None = None,
    red_flag_only: bool = False,
):
    stmt = select(ServingCompoundRisk)
    if minister:
        stmt = stmt.where(_normalized_like(ServingCompoundRisk.minister_name, minister))
    if entity_type:
        stmt = stmt.where(ServingCompoundRisk.entity_type == entity_type)
    if red_flag_only:
        stmt = stmt.where(ServingCompoundRisk.red_flag.is_(True))
    return stmt


def get_compound_risks(
    session: Session,
    page: int,
    page_size: int,
    *,
    minister: str | None = None,
    entity_type: str | None = None,
    red_flag_only: bool = False,
) -> PaginatedCompoundRiskResponse:
    stmt = _filtered_stmt(minister=minister, entity_type=entity_type, red_flag_only=red_flag_only)
    total = session.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    rows = cast(
        list[ServingCompoundRisk],
        session.scalars(
            stmt.order_by(
                ServingCompoundRisk.signal_count.desc(),
                ServingCompoundRisk.adjusted_rate_delta.desc(),
                ServingCompoundRisk.max_alert_score.desc(),
                ServingCompoundRisk.shared_process_count.desc(),
                ServingCompoundRisk.minister_name.asc(),
                ServingCompoundRisk.entity_name.asc(),
            )
            .offset((page - 1) * page_size)
            .limit(page_size)
        ).all(),
    )
    return PaginatedCompoundRiskResponse(
        total=total,
        page=page,
        page_size=page_size,
        items=[_row_to_item(row) for row in rows],
    )


def get_compound_risk_red_flags(
    session: Session,
    *,
    minister: str | None = None,
    entity_type: str | None = None,
    limit: int = 100,
) -> CompoundRiskRedFlagsResponse:
    total = (
        session.scalar(
            select(func.count()).select_from(
                _filtered_stmt(minister=minister, entity_type=entity_type, red_flag_only=True).subquery()
            )
        )
        or 0
    )
    rows = cast(
        list[ServingCompoundRisk],
        session.scalars(
            _filtered_stmt(minister=minister, entity_type=entity_type, red_flag_only=True)
            .order_by(
                ServingCompoundRisk.signal_count.desc(),
                ServingCompoundRisk.adjusted_rate_delta.desc(),
                ServingCompoundRisk.max_alert_score.desc(),
                ServingCompoundRisk.shared_process_count.desc(),
                ServingCompoundRisk.minister_name.asc(),
                ServingCompoundRisk.entity_name.asc(),
            )
            .limit(limit)
        ).all(),
    )
    return CompoundRiskRedFlagsResponse(items=[_row_to_item(row) for row in rows], total=total)


def get_compound_risk_heatmap(
    session: Session,
    *,
    limit: int = 20,
    minister: str | None = None,
    entity_type: str | None = None,
    red_flag_only: bool = False,
) -> CompoundRiskHeatmapResponse:
    stmt = _filtered_stmt(minister=minister, entity_type=entity_type, red_flag_only=red_flag_only)
    total = session.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    rows = cast(
        list[ServingCompoundRisk],
        session.scalars(
            stmt.order_by(
                ServingCompoundRisk.signal_count.desc(),
                ServingCompoundRisk.adjusted_rate_delta.desc(),
                ServingCompoundRisk.max_alert_score.desc(),
                ServingCompoundRisk.shared_process_count.desc(),
                ServingCompoundRisk.minister_name.asc(),
                ServingCompoundRisk.entity_name.asc(),
            ).limit(limit)
        ).all(),
    )
    items = [_row_to_item(row) for row in rows]

    ministers: list[str] = []
    seen_ministers: set[str] = set()
    entities: list[CompoundRiskHeatmapEntity] = []
    seen_entities: set[tuple[str, str]] = set()
    cells: list[CompoundRiskHeatmapCell] = []

    for item in items:
        if item.minister_name not in seen_ministers:
            seen_ministers.add(item.minister_name)
            ministers.append(item.minister_name)
        entity_key = (item.entity_type, item.entity_id)
        if entity_key not in seen_entities:
            seen_entities.add(entity_key)
            entities.append(
                CompoundRiskHeatmapEntity(
                    entity_type=item.entity_type,
                    entity_id=item.entity_id,
                    entity_name=item.entity_name,
                )
            )
        cells.append(
            CompoundRiskHeatmapCell(
                pair_id=item.pair_id,
                minister_name=item.minister_name,
                entity_type=item.entity_type,
                entity_id=item.entity_id,
                signal_count=item.signal_count,
                signals=item.signals,
                red_flag=item.red_flag,
                max_alert_score=item.max_alert_score,
                max_rate_delta=item.max_rate_delta,
                adjusted_rate_delta=item.adjusted_rate_delta,
            )
        )

    return CompoundRiskHeatmapResponse(
        pair_count=total,
        display_limit=limit,
        ministers=ministers,
        entities=entities,
        cells=cells,
    )
