"""Query functions for agenda endpoints."""

from __future__ import annotations

import json
from datetime import date
from typing import cast

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..serving._models_agenda import ServingAgendaCoverage, ServingAgendaEvent, ServingAgendaExposure
from ._schemas_agenda import (
    AgendaCoverageItem,
    AgendaEventItem,
    AgendaExposureItem,
    AgendaMinisterSummary,
    AgendaSummaryResponse,
    PaginatedAgendaEventsResponse,
    PaginatedAgendaExposuresResponse,
)

_DISCLAIMER = (
    "Cobertura parcial por disponibilidade publica de agenda. "
    "Ausencia de registro nao significa ausencia de contato. "
    "Dados servem para priorizacao investigativa, nao para inferencia causal."
)


def _safe_json(val: str | None) -> list:
    if not val:
        return []
    try:
        return json.loads(val)
    except json.JSONDecodeError, TypeError:
        return []


def _ev_item(r: ServingAgendaEvent) -> AgendaEventItem:
    return AgendaEventItem(
        event_id=r.event_id,
        minister_slug=r.minister_slug,
        minister_name=r.minister_name,
        owner_scope=r.owner_scope,
        owner_role=r.owner_role,
        event_date=str(r.event_date) if r.event_date else "",
        event_time_local=str(r.event_time_local) if r.event_time_local else None,
        source_time_raw=r.source_time_raw,
        event_title=r.event_title,
        event_description=r.event_description,
        event_category=r.event_category,
        meeting_nature=r.meeting_nature,
        has_process_ref=r.has_process_ref,
        classification_confidence=r.classification_confidence,
        relevance_track=r.relevance_track,
        process_refs=_safe_json(r.process_refs_json),
        process_id=r.process_id,
        process_class=r.process_class,
        is_own_process=r.is_own_process,
        minister_case_role=r.minister_case_role,
        contains_public_actor=r.contains_public_actor,
        contains_private_actor=r.contains_private_actor,
        actor_count=r.actor_count,
        institutional_role_bias_flag=r.institutional_role_bias_flag,
    )


def _exp_item(r: ServingAgendaExposure) -> AgendaExposureItem:
    return AgendaExposureItem(
        exposure_id=r.exposure_id,
        agenda_event_id=r.agenda_event_id,
        minister_slug=r.minister_slug,
        process_id=r.process_id,
        process_class=r.process_class,
        agenda_date=str(r.agenda_date) if r.agenda_date else "",
        decision_date=str(r.decision_date) if r.decision_date else None,
        days_between=r.days_between,
        window=r.window,
        is_own_process=r.is_own_process,
        event_category=r.event_category,
        meeting_nature=r.meeting_nature,
        event_title=r.event_title,
        decision_type=r.decision_type,
        priority_score=r.priority_score,
        priority_tier=r.priority_tier,
        priority_tier_override_reason=r.priority_tier_override_reason,
        coverage_comparability=r.coverage_comparability,
    )


def get_agenda_events(
    session: Session,
    page: int,
    page_size: int,
    *,
    minister_slug: str | None = None,
    event_category: str | None = None,
    has_process_ref: bool | None = None,
    owner_scope: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> PaginatedAgendaEventsResponse:
    stmt = select(ServingAgendaEvent)
    cnt = select(func.count()).select_from(ServingAgendaEvent)
    if minister_slug:
        stmt = stmt.where(ServingAgendaEvent.minister_slug == minister_slug)
        cnt = cnt.where(ServingAgendaEvent.minister_slug == minister_slug)
    if event_category:
        stmt = stmt.where(ServingAgendaEvent.event_category == event_category)
        cnt = cnt.where(ServingAgendaEvent.event_category == event_category)
    if has_process_ref is not None:
        stmt = stmt.where(ServingAgendaEvent.has_process_ref == has_process_ref)
        cnt = cnt.where(ServingAgendaEvent.has_process_ref == has_process_ref)
    if owner_scope:
        stmt = stmt.where(ServingAgendaEvent.owner_scope == owner_scope)
        cnt = cnt.where(ServingAgendaEvent.owner_scope == owner_scope)
    if date_from:
        stmt = stmt.where(ServingAgendaEvent.event_date >= date.fromisoformat(date_from))
        cnt = cnt.where(ServingAgendaEvent.event_date >= date.fromisoformat(date_from))
    if date_to:
        stmt = stmt.where(ServingAgendaEvent.event_date <= date.fromisoformat(date_to))
        cnt = cnt.where(ServingAgendaEvent.event_date <= date.fromisoformat(date_to))
    total = session.execute(cnt).scalar_one()
    rows = cast(
        list[ServingAgendaEvent],
        session.scalars(
            stmt.order_by(ServingAgendaEvent.event_date.desc(), ServingAgendaEvent.event_id.asc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        ).all(),
    )
    return PaginatedAgendaEventsResponse(total=total, page=page, page_size=page_size, items=[_ev_item(r) for r in rows])


def get_agenda_event_detail(session: Session, event_id: str) -> AgendaEventItem | None:
    r = session.get(ServingAgendaEvent, event_id)
    return _ev_item(r) if r else None


def get_agenda_ministers(session: Session) -> list[AgendaMinisterSummary]:
    rows = cast(
        list[ServingAgendaCoverage],
        session.scalars(select(ServingAgendaCoverage).where(ServingAgendaCoverage.owner_scope == "ministerial")).all(),
    )
    by_slug: dict[str, list[ServingAgendaCoverage]] = {}
    for r in rows:
        by_slug.setdefault(r.minister_slug, []).append(r)
    result: list[AgendaMinisterSummary] = []
    for slug, covs in sorted(by_slug.items()):
        total_ev = sum(c.event_count for c in covs)
        priv = sum(c.private_advocacy_count for c in covs)
        ta = sum(c.track_a_count for c in covs)
        avg_r = sum(c.coverage_ratio for c in covs) / len(covs) if covs else 0.0
        result.append(
            AgendaMinisterSummary(
                minister_slug=slug,
                minister_name=covs[0].minister_name if covs else "",
                total_events=total_ev,
                private_advocacy_count=priv,
                track_a_count=ta,
                coverage_months=len(covs),
                avg_coverage_ratio=round(avg_r, 4),
            )
        )
    return result


def get_agenda_minister_detail(session: Session, slug: str, page: int, page_size: int) -> dict | None:
    covs = cast(
        list[ServingAgendaCoverage],
        session.scalars(select(ServingAgendaCoverage).where(ServingAgendaCoverage.minister_slug == slug)).all(),
    )
    if not covs:
        return None
    total = session.execute(
        select(func.count()).select_from(ServingAgendaEvent).where(ServingAgendaEvent.minister_slug == slug)
    ).scalar_one()
    evs = cast(
        list[ServingAgendaEvent],
        session.scalars(
            select(ServingAgendaEvent)
            .where(ServingAgendaEvent.minister_slug == slug)
            .order_by(ServingAgendaEvent.event_date.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        ).all(),
    )
    exps = cast(
        list[ServingAgendaExposure],
        session.scalars(
            select(ServingAgendaExposure)
            .where(ServingAgendaExposure.minister_slug == slug)
            .order_by(ServingAgendaExposure.agenda_date.desc())
            .limit(100)
        ).all(),
    )
    return {
        "minister_name": covs[0].minister_name,
        "events": {"total": total, "page": page, "page_size": page_size, "items": [_ev_item(e) for e in evs]},
        "exposures": [_exp_item(e) for e in exps],
        "coverages": [
            AgendaCoverageItem(
                coverage_id=c.coverage_id,
                minister_slug=c.minister_slug,
                minister_name=c.minister_name,
                year=c.year,
                month=c.month,
                event_count=c.event_count,
                days_with_events=c.days_with_events,
                coverage_ratio=c.coverage_ratio,
                comparability_tier=c.comparability_tier,
                court_recess_flag=c.court_recess_flag,
                publication_gap_flag=c.publication_gap_flag,
            ).model_dump()
            for c in covs
        ],
    }


def get_agenda_exposures(
    session: Session,
    page: int,
    page_size: int,
    *,
    minister_slug: str | None = None,
    priority_tier: str | None = None,
    process_id: str | None = None,
    window: str | None = None,
) -> PaginatedAgendaExposuresResponse:
    stmt = select(ServingAgendaExposure)
    cnt = select(func.count()).select_from(ServingAgendaExposure)
    if minister_slug:
        stmt = stmt.where(ServingAgendaExposure.minister_slug == minister_slug)
        cnt = cnt.where(ServingAgendaExposure.minister_slug == minister_slug)
    if priority_tier:
        stmt = stmt.where(ServingAgendaExposure.priority_tier == priority_tier)
        cnt = cnt.where(ServingAgendaExposure.priority_tier == priority_tier)
    if process_id:
        stmt = stmt.where(ServingAgendaExposure.process_id == process_id)
        cnt = cnt.where(ServingAgendaExposure.process_id == process_id)
    if window:
        stmt = stmt.where(ServingAgendaExposure.window == window)
        cnt = cnt.where(ServingAgendaExposure.window == window)
    total = session.execute(cnt).scalar_one()
    rows = cast(
        list[ServingAgendaExposure],
        session.scalars(
            stmt.order_by(ServingAgendaExposure.priority_score.desc(), ServingAgendaExposure.exposure_id.asc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        ).all(),
    )
    return PaginatedAgendaExposuresResponse(
        total=total,
        page=page,
        page_size=page_size,
        items=[_exp_item(r) for r in rows],
    )


def get_agenda_summary(session: Session) -> AgendaSummaryResponse:
    te = session.scalar(select(func.count()).select_from(ServingAgendaEvent)) or 0
    tm = (
        session.scalar(
            select(func.count()).select_from(ServingAgendaEvent).where(ServingAgendaEvent.owner_scope == "ministerial")
        )
        or 0
    )
    tp = (
        session.scalar(
            select(func.count())
            .select_from(ServingAgendaEvent)
            .where(ServingAgendaEvent.event_category == "private_advocacy")
        )
        or 0
    )
    tr = (
        session.scalar(
            select(func.count()).select_from(ServingAgendaEvent).where(ServingAgendaEvent.has_process_ref.is_(True))
        )
        or 0
    )
    mc = (
        session.scalar(
            select(func.count(func.distinct(ServingAgendaCoverage.minister_slug))).select_from(ServingAgendaCoverage)
        )
        or 0
    )
    tex = session.scalar(select(func.count()).select_from(ServingAgendaExposure)) or 0
    hp = (
        session.scalar(
            select(func.count()).select_from(ServingAgendaExposure).where(ServingAgendaExposure.priority_tier == "high")
        )
        or 0
    )
    return AgendaSummaryResponse(
        total_events=te,
        total_ministerial_events=tm,
        total_private_advocacy=tp,
        total_with_process_ref=tr,
        ministers_covered=mc,
        total_exposures=tex,
        high_priority_exposures=hp,
        methodology_note=(
            "Temporal proximity between public ministerial agenda and judicial decisions. "
            "Intra-minister baselines only. Partial coverage since Jan/2024."
        ),
        disclaimer=_DISCLAIMER,
    )
