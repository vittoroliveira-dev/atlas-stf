from __future__ import annotations

from collections import Counter

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..core.constants import EVENT_WINDOW_DAYS, ROLLING_WINDOW_MONTHS
from ..serving.models import ServingTemporalAnalysis
from ._filters import _normalized_like
from .temporal_schemas import (
    TemporalAnalysisMinisterResponse,
    TemporalAnalysisOverviewResponse,
    TemporalCorporateLinkItem,
    TemporalEventItem,
    TemporalMinisterSummary,
    TemporalMonthlyItem,
    TemporalOverviewSummary,
    TemporalSeasonalityItem,
    TemporalYoyItem,
)


def _minister_stmt(minister: str):
    return _normalized_like(ServingTemporalAnalysis.rapporteur, minister)


def _match_rapporteur(session: Session, minister: str) -> str | None:
    exact = session.scalar(
        select(ServingTemporalAnalysis.rapporteur)
        .where(ServingTemporalAnalysis.rapporteur == minister)
        .limit(1)
    )
    if exact:
        return exact
    row = session.execute(
        select(ServingTemporalAnalysis.rapporteur, func.count())
        .where(
            ServingTemporalAnalysis.rapporteur.is_not(None),
            _minister_stmt(minister),
        )
        .group_by(ServingTemporalAnalysis.rapporteur)
        .order_by(func.count().desc(), ServingTemporalAnalysis.rapporteur.asc())
    ).first()
    return row[0] if row else None


def _monthly_item(row: ServingTemporalAnalysis) -> TemporalMonthlyItem:
    return TemporalMonthlyItem(
        record_id=row.record_id,
        rapporteur=row.rapporteur,
        decision_month=row.decision_month,
        decision_year=row.decision_year,
        decision_count=row.decision_count,
        favorable_count=row.favorable_count,
        unfavorable_count=row.unfavorable_count,
        favorable_rate=row.favorable_rate,
        rolling_favorable_rate_6m=row.rolling_favorable_rate_6m,
        breakpoint_score=row.breakpoint_score,
        breakpoint_flag=row.breakpoint_flag,
        generated_at=row.generated_at,
    )


def _yoy_item(row: ServingTemporalAnalysis) -> TemporalYoyItem:
    current_rate = row.current_favorable_rate if row.current_favorable_rate is not None else row.favorable_rate
    return TemporalYoyItem(
        record_id=row.record_id,
        rapporteur=row.rapporteur,
        process_class=row.process_class,
        decision_year=row.decision_year,
        decision_count=row.decision_count,
        favorable_count=row.favorable_count,
        unfavorable_count=row.unfavorable_count,
        current_favorable_rate=current_rate,
        favorable_rate=row.favorable_rate,
        prior_decision_count=row.prior_decision_count,
        prior_favorable_rate=row.prior_favorable_rate,
        delta_vs_prior_year=row.delta_vs_prior_year,
        generated_at=row.generated_at,
    )


def _seasonality_item(row: ServingTemporalAnalysis) -> TemporalSeasonalityItem:
    return TemporalSeasonalityItem(
        record_id=row.record_id,
        rapporteur=row.rapporteur,
        month_of_year=row.month_of_year,
        decision_count=row.decision_count,
        favorable_count=row.favorable_count,
        unfavorable_count=row.unfavorable_count,
        favorable_rate=row.favorable_rate,
        delta_vs_overall=row.delta_vs_overall,
        generated_at=row.generated_at,
    )


def _event_item(row: ServingTemporalAnalysis) -> TemporalEventItem:
    return TemporalEventItem(
        record_id=row.record_id,
        rapporteur=row.rapporteur,
        event_id=row.event_id,
        event_type=row.event_type,
        event_scope=row.event_scope,
        event_date=row.event_date,
        event_title=row.event_title,
        source=row.source,
        source_url=row.source_url,
        status=row.status,
        before_decision_count=row.before_decision_count,
        before_favorable_rate=row.before_favorable_rate,
        after_decision_count=row.after_decision_count,
        after_favorable_rate=row.after_favorable_rate,
        delta_before_after=row.delta_before_after,
        decision_count=row.decision_count,
        favorable_count=row.favorable_count,
        unfavorable_count=row.unfavorable_count,
        generated_at=row.generated_at,
    )


def _corporate_link_item(row: ServingTemporalAnalysis) -> TemporalCorporateLinkItem:
    return TemporalCorporateLinkItem(
        record_id=row.record_id,
        rapporteur=row.rapporteur,
        linked_entity_type=row.linked_entity_type,
        linked_entity_id=row.linked_entity_id,
        linked_entity_name=row.linked_entity_name,
        company_cnpj_basico=row.company_cnpj_basico,
        company_name=row.company_name,
        link_degree=row.link_degree,
        link_chain=row.link_chain,
        link_start_date=row.link_start_date,
        link_status=row.link_status,
        decision_count=row.decision_count,
        favorable_count=row.favorable_count,
        unfavorable_count=row.unfavorable_count,
        favorable_rate=row.favorable_rate,
        generated_at=row.generated_at,
    )


def get_temporal_analysis_overview(
    session: Session,
    *,
    minister: str | None = None,
    process_class: str | None = None,
    analysis_kind: str | None = None,
    event_type: str | None = None,
) -> TemporalAnalysisOverviewResponse:
    stmt = select(ServingTemporalAnalysis)
    if minister:
        stmt = stmt.where(_minister_stmt(minister))
    if process_class:
        stmt = stmt.where(ServingTemporalAnalysis.process_class == process_class)
    if analysis_kind:
        stmt = stmt.where(ServingTemporalAnalysis.analysis_kind == analysis_kind)
    if event_type:
        stmt = stmt.where(ServingTemporalAnalysis.event_type == event_type)

    rows = session.scalars(stmt.order_by(ServingTemporalAnalysis.record_id)).all()
    counts_by_kind = Counter(row.analysis_kind for row in rows)
    ministers = {row.rapporteur for row in rows if row.rapporteur}
    events = {row.event_id for row in rows if row.analysis_kind == "event_window" and row.event_id}

    monthly_rows = [row for row in rows if row.analysis_kind == "monthly_minister"]
    seasonality_rows = [row for row in rows if row.analysis_kind == "seasonality"]
    event_rows = [row for row in rows if row.analysis_kind == "event_window"]

    by_rapporteur: dict[str, list[ServingTemporalAnalysis]] = {}
    for row in rows:
        if not row.rapporteur:
            continue
        by_rapporteur.setdefault(row.rapporteur, []).append(row)

    minister_summaries = sorted(
        [
            TemporalMinisterSummary(
                rapporteur=rapporteur,
                record_count=len(group),
                breakpoint_count=sum(
                    1 for item in group if item.analysis_kind == "monthly_minister" and bool(item.breakpoint_flag)
                ),
                latest_decision_month=max(
                    (
                        item.decision_month
                        for item in group
                        if item.analysis_kind == "monthly_minister" and item.decision_month
                    ),
                    default=None,
                ),
                latest_breakpoint_month=max(
                    (
                        item.decision_month
                        for item in group
                        if item.analysis_kind == "monthly_minister"
                        and bool(item.breakpoint_flag)
                        and item.decision_month
                    ),
                    default=None,
                ),
            )
            for rapporteur, group in by_rapporteur.items()
        ],
        key=lambda item: (-item.breakpoint_count, -item.record_count, item.rapporteur),
    )

    breakpoints = sorted(
        [_monthly_item(row) for row in monthly_rows if bool(row.breakpoint_flag)],
        key=lambda item: (
            -(item.breakpoint_score or 0.0),
            item.decision_month or "",
            item.rapporteur or "",
        ),
    )

    seasonality = sorted(
        [_seasonality_item(row) for row in seasonality_rows],
        key=lambda item: (item.month_of_year or 0, item.rapporteur or ""),
    )

    event_items = sorted(
        [_event_item(row) for row in event_rows],
        key=lambda item: (
            item.event_date or (item.generated_at.date() if item.generated_at else None),
            item.event_id or "",
        ),
        reverse=True,
    )

    return TemporalAnalysisOverviewResponse(
        summary=TemporalOverviewSummary(
            total_records=len(rows),
            counts_by_kind=dict(sorted(counts_by_kind.items())),
            ministers_covered=len(ministers),
            events_covered=len(events),
            rolling_window_months=ROLLING_WINDOW_MONTHS,
            event_window_days=EVENT_WINDOW_DAYS,
        ),
        minister_summaries=minister_summaries,
        breakpoints=breakpoints,
        seasonality=seasonality,
        events=event_items,
    )


def get_temporal_analysis_minister(
    session: Session,
    minister: str,
) -> TemporalAnalysisMinisterResponse:
    rapporteur = _match_rapporteur(session, minister)
    if rapporteur is None:
        return TemporalAnalysisMinisterResponse(
            minister=minister,
            rapporteur=None,
            monthly=[],
            yoy=[],
            seasonality=[],
            events=[],
            corporate_links=[],
        )

    rows = session.scalars(
        select(ServingTemporalAnalysis)
        .where(ServingTemporalAnalysis.rapporteur == rapporteur)
        .order_by(
            ServingTemporalAnalysis.decision_year.asc(),
            ServingTemporalAnalysis.decision_month.asc(),
            ServingTemporalAnalysis.month_of_year.asc(),
            ServingTemporalAnalysis.event_date.asc(),
            ServingTemporalAnalysis.link_start_date.asc(),
            ServingTemporalAnalysis.process_class.asc(),
            ServingTemporalAnalysis.record_id.asc(),
        )
    ).all()

    monthly = [_monthly_item(row) for row in rows if row.analysis_kind == "monthly_minister"]
    yoy = [_yoy_item(row) for row in rows if row.analysis_kind == "yoy_process_class"]
    seasonality = [_seasonality_item(row) for row in rows if row.analysis_kind == "seasonality"]
    events = sorted(
        [_event_item(row) for row in rows if row.analysis_kind == "event_window"],
        key=lambda item: (
            item.event_date or (item.generated_at.date() if item.generated_at else None),
            item.event_id or "",
        ),
        reverse=True,
    )
    corporate_links = sorted(
        [_corporate_link_item(row) for row in rows if row.analysis_kind == "corporate_link_timeline"],
        key=lambda item: (
            item.link_start_date or (item.generated_at.date() if item.generated_at else None),
            item.link_degree or 0,
        ),
    )

    return TemporalAnalysisMinisterResponse(
        minister=minister,
        rapporteur=rapporteur,
        monthly=monthly,
        yoy=yoy,
        seasonality=seasonality,
        events=events,
        corporate_links=corporate_links,
    )
