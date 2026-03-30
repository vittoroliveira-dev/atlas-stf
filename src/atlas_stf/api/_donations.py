"""Query functions for donation-related endpoints."""

from __future__ import annotations

import json
from typing import Literal, cast

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..serving.models import ServingCounselDonationProfile, ServingDonationEvent, ServingDonationMatch
from .schemas import (
    CounselDonationProfileItem,
    DonationEventItem,
    DonationMatchItem,
    DonationRedFlagsResponse,
    PaginatedDonationEventsResponse,
    PaginatedDonationsResponse,
)


def _parse_json_list(raw: str | None) -> list:
    if not raw:
        return []
    try:
        return json.loads(raw)
    except json.JSONDecodeError, TypeError:
        return []


def _compute_match_subtotals(
    session: Session, match_ids: list[str],
) -> dict[str, tuple[float, int]]:
    """Compute (sum, count) of donation events per match_id."""
    if not match_ids:
        return {}
    rows = session.execute(
        select(
            ServingDonationEvent.match_id,
            func.coalesce(func.sum(ServingDonationEvent.donation_amount), 0.0),
            func.count(),
        )
        .where(ServingDonationEvent.match_id.in_(match_ids))
        .group_by(ServingDonationEvent.match_id)
    ).all()
    return {mid: (total, cnt) for mid, total, cnt in rows}


def _row_to_match_item(
    row: ServingDonationMatch,
    subtotals: dict[str, tuple[float, int]] | None = None,
) -> DonationMatchItem:
    entity_id = row.entity_id or row.party_id
    return DonationMatchItem(
        match_id=row.match_id,
        entity_type=row.entity_type,
        entity_id=entity_id,
        party_id=row.party_id,
        counsel_id=entity_id if row.entity_type == "counsel" else None,
        party_name_normalized=row.party_name_normalized,
        donor_cpf_cnpj=row.donor_cpf_cnpj,
        donor_name_normalized=row.donor_name_normalized,
        donor_name_originator=row.donor_name_originator,
        total_donated_brl=row.total_donated_brl,
        donation_count=row.donation_count,
        matched_events_total_brl=subtotals[row.match_id][0] if subtotals and row.match_id in subtotals else None,
        matched_events_count=subtotals[row.match_id][1] if subtotals and row.match_id in subtotals else None,
        election_years=_parse_json_list(row.election_years_json),
        parties_donated_to=_parse_json_list(row.parties_donated_to_json),
        candidates_donated_to=_parse_json_list(row.candidates_donated_to_json),
        positions_donated_to=_parse_json_list(row.positions_donated_to_json),
        stf_case_count=row.stf_case_count,
        favorable_rate=row.favorable_rate,
        favorable_rate_substantive=row.favorable_rate_substantive,
        substantive_decision_count=row.substantive_decision_count,
        baseline_favorable_rate=row.baseline_favorable_rate,
        favorable_rate_delta=row.favorable_rate_delta,
        red_flag=row.red_flag,
        red_flag_substantive=row.red_flag_substantive,
        red_flag_power=row.red_flag_power,
        red_flag_confidence=cast(Literal["high", "moderate", "low"] | None, row.red_flag_confidence),
        match_strategy=row.match_strategy,
        match_score=row.match_score,
        match_confidence=row.match_confidence,
        matched_alias=row.matched_alias or None,
        matched_tax_id=row.matched_tax_id or None,
        uncertainty_note=row.uncertainty_note or None,
        donor_identity_key=row.donor_identity_key or None,
        donor_document_type=row.donor_document_type,
        donor_tax_id_normalized=row.donor_tax_id_normalized,
        donor_cnpj_basico=row.donor_cnpj_basico,
        donor_company_name=row.donor_company_name,
        economic_group_id=row.economic_group_id,
        economic_group_member_count=row.economic_group_member_count,
        is_law_firm_group=row.is_law_firm_group,
        donor_group_has_minister_partner=row.donor_group_has_minister_partner,
        donor_group_has_party_partner=row.donor_group_has_party_partner,
        donor_group_has_counsel_partner=row.donor_group_has_counsel_partner,
        min_link_degree_to_minister=row.min_link_degree_to_minister,
        corporate_link_red_flag=row.corporate_link_red_flag,
        resource_types_observed=_parse_json_list(row.resource_types_observed_json),
        first_donation_date=row.first_donation_date,
        last_donation_date=row.last_donation_date,
        active_election_year_count=row.active_election_year_count,
        max_single_donation_brl=row.max_single_donation_brl,
        avg_donation_brl=row.avg_donation_brl,
        top_candidate_share=row.top_candidate_share,
        top_party_share=row.top_party_share,
        top_state_share=row.top_state_share,
        donation_year_span=row.donation_year_span,
        recent_donation_flag=row.recent_donation_flag,
    )


def _row_to_counsel_item(row: ServingCounselDonationProfile) -> CounselDonationProfileItem:
    return CounselDonationProfileItem(
        counsel_id=row.counsel_id,
        counsel_name_normalized=row.counsel_name_normalized,
        donor_client_count=row.donor_client_count,
        total_client_count=row.total_client_count,
        donor_client_rate=row.donor_client_rate,
        donor_client_favorable_rate=row.donor_client_favorable_rate,
        overall_favorable_rate=row.overall_favorable_rate,
        red_flag=row.red_flag,
    )


def get_donations(
    session: Session,
    page: int,
    page_size: int,
    *,
    red_flag_only: bool = False,
    entity_type: str | None = None,
) -> PaginatedDonationsResponse:
    stmt = select(ServingDonationMatch)
    count_stmt = select(func.count()).select_from(ServingDonationMatch)

    if entity_type:
        stmt = stmt.where(ServingDonationMatch.entity_type == entity_type)
        count_stmt = count_stmt.where(ServingDonationMatch.entity_type == entity_type)
    if red_flag_only:
        stmt = stmt.where(ServingDonationMatch.red_flag.is_(True))
        count_stmt = count_stmt.where(ServingDonationMatch.red_flag.is_(True))

    total = session.execute(count_stmt).scalar_one()
    stmt = stmt.order_by(
        ServingDonationMatch.red_flag.desc(),
        ServingDonationMatch.total_donated_brl.desc(),
        ServingDonationMatch.match_id.asc(),
    )
    rows = cast(
        list[ServingDonationMatch],
        session.scalars(stmt.offset((page - 1) * page_size).limit(page_size)).all(),
    )
    subtotals = _compute_match_subtotals(session, [r.match_id for r in rows])

    return PaginatedDonationsResponse(
        total=total,
        page=page,
        page_size=page_size,
        items=[_row_to_match_item(r, subtotals) for r in rows],
    )


def get_party_donations(session: Session, party_id: str, *, limit: int = 100) -> list[DonationMatchItem]:
    rows = cast(
        list[ServingDonationMatch],
        session.scalars(
            select(ServingDonationMatch)
            .where(ServingDonationMatch.party_id == party_id)
            .order_by(
                ServingDonationMatch.red_flag.desc(),
                ServingDonationMatch.total_donated_brl.desc(),
                ServingDonationMatch.match_id.asc(),
            )
            .limit(limit)
        ).all(),
    )
    subtotals = _compute_match_subtotals(session, [r.match_id for r in rows])
    return [_row_to_match_item(r, subtotals) for r in rows]


def get_counsel_donation_profile(session: Session, counsel_id: str) -> CounselDonationProfileItem | None:
    row = session.get(ServingCounselDonationProfile, counsel_id)
    if row is None:
        return None
    return _row_to_counsel_item(row)


def get_donation_red_flags(session: Session, *, limit: int = 100) -> DonationRedFlagsResponse:
    total_party_flags = (
        session.scalar(
            select(func.count()).select_from(ServingDonationMatch).where(ServingDonationMatch.red_flag.is_(True))
        )
        or 0
    )
    party_flags = cast(
        list[ServingDonationMatch],
        session.scalars(
            select(ServingDonationMatch)
            .where(ServingDonationMatch.red_flag.is_(True))
            .order_by(
                ServingDonationMatch.total_donated_brl.desc(),
                ServingDonationMatch.match_id.asc(),
            )
            .limit(limit)
        ).all(),
    )
    total_counsel_flags = (
        session.scalar(
            select(func.count())
            .select_from(ServingCounselDonationProfile)
            .where(ServingCounselDonationProfile.red_flag.is_(True))
        )
        or 0
    )
    counsel_flags = cast(
        list[ServingCounselDonationProfile],
        session.scalars(
            select(ServingCounselDonationProfile)
            .where(ServingCounselDonationProfile.red_flag.is_(True))
            .order_by(ServingCounselDonationProfile.counsel_id.asc())
            .limit(limit)
        ).all(),
    )
    party_subtotals = _compute_match_subtotals(session, [r.match_id for r in party_flags])
    return DonationRedFlagsResponse(
        party_flags=[_row_to_match_item(r, party_subtotals) for r in party_flags],
        counsel_flags=[_row_to_counsel_item(r) for r in counsel_flags],
        total_party_flags=total_party_flags,
        total_counsel_flags=total_counsel_flags,
    )


def _row_to_event_item(row: ServingDonationEvent) -> DonationEventItem:
    return DonationEventItem(
        event_id=row.event_id,
        match_id=row.match_id,
        election_year=row.election_year,
        donation_date=row.donation_date.isoformat() if row.donation_date else None,
        donation_amount=row.donation_amount,
        candidate_name=row.candidate_name,
        party_abbrev=row.party_abbrev,
        position=row.position,
        state=row.state,
        donor_name=row.donor_name,
        donor_name_originator=row.donor_name_originator,
        donor_cpf_cnpj=row.donor_cpf_cnpj,
        donation_description=row.donation_description,
        donor_identity_key=row.donor_identity_key or None,
        resource_type_category=row.resource_type_category,
        resource_type_subtype=row.resource_type_subtype,
        resource_classification_confidence=row.resource_classification_confidence,
        resource_classification_rule=row.resource_classification_rule,
        source_file=row.source_file or None,
        collected_at=row.collected_at or None,
        source_url=row.source_url or None,
        ingest_run_id=row.ingest_run_id or None,
        record_hash=row.record_hash or None,
    )


def get_donation_events(
    session: Session,
    match_id: str,
    page: int,
    page_size: int,
) -> PaginatedDonationEventsResponse:
    stmt = select(ServingDonationEvent).where(ServingDonationEvent.match_id == match_id)
    count_stmt = select(func.count()).select_from(ServingDonationEvent).where(ServingDonationEvent.match_id == match_id)
    total = session.execute(count_stmt).scalar_one()
    rows = cast(
        list[ServingDonationEvent],
        session.scalars(
            stmt.order_by(
                ServingDonationEvent.election_year.desc(),
                ServingDonationEvent.donation_date.desc(),
                ServingDonationEvent.event_id.asc(),
            )
            .offset((page - 1) * page_size)
            .limit(page_size)
        ).all(),
    )
    return PaginatedDonationEventsResponse(
        total=total,
        page=page,
        page_size=page_size,
        items=[_row_to_event_item(r) for r in rows],
    )
