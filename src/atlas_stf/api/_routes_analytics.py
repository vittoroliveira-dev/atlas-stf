from __future__ import annotations

from typing import Annotated

from fastapi import FastAPI, HTTPException, Query

from .schemas import (
    AssignmentAuditResponse,
    MinisterBioResponse,
    OriginContextResponse,
    RapporteurProfileResponse,
    SequentialAnalysisResponse,
    SourcesAuditResponse,
)
from .service import (
    get_assignment_audit,
    get_minister_bio,
    get_minister_profile_data,
    get_minister_sequential,
    get_origin_context,
    get_sources_audit,
    get_temporal_analysis_minister,
    get_temporal_analysis_overview,
)
from .temporal_schemas import TemporalAnalysisMinisterResponse, TemporalAnalysisOverviewResponse

ListLimit = Annotated[int, Query(ge=1, le=500)]


def register_analytics_routes(
    app: FastAPI,
    factory: ...,
    build_filters: ...,
    get_base_filters: ...,
) -> None:
    @app.get("/ministers/{minister}/profile", response_model=list[RapporteurProfileResponse])
    def minister_profile(minister: str, limit: ListLimit = 100) -> list[RapporteurProfileResponse]:
        with factory() as session:
            return get_minister_profile_data(session, minister, limit=limit)

    @app.get("/ministers/{minister}/sequential", response_model=list[SequentialAnalysisResponse])
    def minister_sequential(minister: str, limit: ListLimit = 100) -> list[SequentialAnalysisResponse]:
        with factory() as session:
            return get_minister_sequential(session, minister, limit=limit)

    @app.get("/ministers/{minister}/bio", response_model=MinisterBioResponse)
    def minister_bio(minister: str) -> MinisterBioResponse:
        with factory() as session:
            bio = get_minister_bio(session, minister)
        if bio is None:
            raise HTTPException(status_code=404, detail="minister_bio_not_found")
        return bio

    @app.get("/audit/assignment", response_model=list[AssignmentAuditResponse])
    def assignment_audit(limit: ListLimit = 100) -> list[AssignmentAuditResponse]:
        with factory() as session:
            return get_assignment_audit(session, limit=limit)

    @app.get("/temporal-analysis", response_model=TemporalAnalysisOverviewResponse)
    def temporal_analysis_overview(
        minister: str | None = Query(default=None),
        process_class: str | None = Query(default=None),
        analysis_kind: str | None = Query(default=None),
        event_type: str | None = Query(default=None),
    ) -> TemporalAnalysisOverviewResponse:
        with factory() as session:
            return get_temporal_analysis_overview(
                session,
                minister=minister,
                process_class=process_class,
                analysis_kind=analysis_kind,
                event_type=event_type,
            )

    @app.get("/temporal-analysis/{minister}", response_model=TemporalAnalysisMinisterResponse)
    def temporal_analysis_minister(minister: str) -> TemporalAnalysisMinisterResponse:
        with factory() as session:
            return get_temporal_analysis_minister(session, minister)

    @app.get("/origin-context", response_model=OriginContextResponse)
    def origin_context(
        state: str | None = Query(default=None),
    ) -> OriginContextResponse:
        with factory() as session:
            return get_origin_context(session, state)

    @app.get("/origin-context/{state}", response_model=OriginContextResponse)
    def origin_context_by_state(state: str) -> OriginContextResponse:
        with factory() as session:
            return get_origin_context(session, state)

    @app.get("/sources/audit", response_model=SourcesAuditResponse)
    def sources_audit() -> SourcesAuditResponse:
        with factory() as session:
            return get_sources_audit(session)
