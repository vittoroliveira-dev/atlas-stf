from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..serving.models import (
    ServingAssignmentAudit,
    ServingMinisterBio,
    ServingOriginContext,
    ServingRapporteurProfile,
    ServingSequentialAnalysis,
)
from ._filters import _normalized_like
from ._formatters import _metrics, _source_files
from ._json_helpers import parse_json_dict, parse_json_list
from .schemas import (
    AssignmentAuditResponse,
    HealthResponse,
    MinisterBioResponse,
    OriginContextItem,
    OriginContextResponse,
    RapporteurProfileResponse,
    SequentialAnalysisResponse,
    SourcesAuditResponse,
)


def get_sources_audit(session: Session) -> SourcesAuditResponse:
    return SourcesAuditResponse(source_files=_source_files(session), metrics=_metrics(session))


def get_minister_profile_data(
    session: Session,
    minister: str,
    *,
    limit: int = 100,
) -> list[RapporteurProfileResponse]:
    stmt = (
        select(ServingRapporteurProfile)
        .where(
            _normalized_like(ServingRapporteurProfile.rapporteur, minister),
        )
        .order_by(ServingRapporteurProfile.decision_year.desc())
    )
    rows = session.scalars(stmt.limit(limit)).all()
    return [
        RapporteurProfileResponse(
            rapporteur=row.rapporteur,
            process_class=row.process_class,
            thematic_key=row.thematic_key,
            decision_year=row.decision_year,
            event_count=row.event_count,
            chi2_statistic=row.chi2_statistic,
            p_value_approx=row.p_value_approx,
            deviation_flag=row.deviation_flag,
            deviation_direction=row.deviation_direction,
            progress_distribution=parse_json_dict(row.progress_distribution_json),
            group_progress_distribution=parse_json_dict(row.group_progress_distribution_json),
        )
        for row in rows
    ]


def get_minister_sequential(
    session: Session,
    minister: str,
    *,
    limit: int = 100,
) -> list[SequentialAnalysisResponse]:
    stmt = (
        select(ServingSequentialAnalysis)
        .where(
            _normalized_like(ServingSequentialAnalysis.rapporteur, minister),
        )
        .order_by(ServingSequentialAnalysis.decision_year.desc())
    )
    rows = session.scalars(stmt.limit(limit)).all()
    return [
        SequentialAnalysisResponse(
            rapporteur=row.rapporteur,
            decision_year=row.decision_year,
            n_decisions=row.n_decisions,
            autocorrelation_lag1=row.autocorrelation_lag1,
            streak_effect_3=row.streak_effect_3,
            streak_effect_5=row.streak_effect_5,
            base_favorable_rate=row.base_favorable_rate,
            post_streak_favorable_rate_3=row.post_streak_favorable_rate_3,
            post_streak_favorable_rate_5=row.post_streak_favorable_rate_5,
            sequential_bias_flag=row.sequential_bias_flag,
        )
        for row in rows
    ]


def get_assignment_audit(session: Session, *, limit: int = 100) -> list[AssignmentAuditResponse]:
    stmt = select(ServingAssignmentAudit).order_by(
        ServingAssignmentAudit.decision_year.desc(),
        ServingAssignmentAudit.process_class.asc(),
    )
    rows = session.scalars(stmt.limit(limit)).all()
    return [
        AssignmentAuditResponse(
            process_class=row.process_class,
            decision_year=row.decision_year,
            rapporteur_count=row.rapporteur_count,
            event_count=row.event_count,
            chi2_statistic=row.chi2_statistic,
            p_value_approx=row.p_value_approx,
            uniformity_flag=row.uniformity_flag,
            most_overrepresented_rapporteur=row.most_overrepresented_rapporteur,
            most_underrepresented_rapporteur=row.most_underrepresented_rapporteur,
            rapporteur_distribution=parse_json_dict(row.rapporteur_distribution_json),
        )
        for row in rows
    ]


def get_minister_bio(session: Session, minister: str) -> MinisterBioResponse | None:
    stmt = select(ServingMinisterBio).where(
        _normalized_like(ServingMinisterBio.minister_name, minister),
    )
    row = session.scalars(stmt).first()
    if row is None:
        return None
    return MinisterBioResponse(
        minister_name=row.minister_name,
        appointment_date=row.appointment_date,
        appointing_president=row.appointing_president,
        birth_date=row.birth_date,
        birth_state=row.birth_state,
        career_summary=row.career_summary,
        political_party_history=(
            parse_json_list(row.political_party_history_json) if row.political_party_history_json else None
        ),
        known_connections=(parse_json_list(row.known_connections_json) if row.known_connections_json else None),
        news_references=(parse_json_list(row.news_references_json) if row.news_references_json else None),
    )


def get_origin_context(session: Session, state: str | None = None) -> OriginContextResponse:
    stmt = select(ServingOriginContext).order_by(ServingOriginContext.stf_process_count.desc())
    if state:
        stmt = stmt.where(ServingOriginContext.state == state.upper())
    rows = session.scalars(stmt).all()
    items = [
        OriginContextItem(
            origin_index=row.origin_index,
            tribunal_label=row.tribunal_label,
            state=row.state,
            datajud_total_processes=row.datajud_total_processes,
            stf_process_count=row.stf_process_count,
            stf_share_pct=row.stf_share_pct,
            top_assuntos=parse_json_list(row.top_assuntos_json),
            top_orgaos_julgadores=parse_json_list(row.top_orgaos_julgadores_json),
            class_distribution=parse_json_list(row.class_distribution_json),
        )
        for row in rows
    ]
    return OriginContextResponse(items=items, total=len(items))


def get_health(database_url: str) -> HealthResponse:
    backend = database_url.split(":", 1)[0] if ":" in database_url else database_url
    return HealthResponse(status="ok", database_backend=backend)
