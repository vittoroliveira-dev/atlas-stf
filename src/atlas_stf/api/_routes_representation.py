"""Routes for representation network endpoints."""

from __future__ import annotations

from typing import Annotated

from fastapi import FastAPI, HTTPException, Query

from ._schemas_representation import (
    FirmDetailResponse,
    LawyerDetailResponse,
    PaginatedFirmsResponse,
    PaginatedLawyersResponse,
    ProcessRepresentationResponse,
    RepresentationNetworkSummary,
)

PositiveInt = Annotated[int, Query(ge=1)]
PageSize = Annotated[int, Query(ge=1, le=100)]


def register_representation_routes(
    app: FastAPI,
    factory: ...,
    build_filters: ...,
    get_base_filters: ...,
) -> None:
    @app.get("/representation/lawyers", response_model=PaginatedLawyersResponse)
    def representation_lawyers(
        page: PositiveInt = 1,
        page_size: PageSize = 20,
        name: str | None = Query(default=None),
        oab_state: str | None = Query(default=None, pattern=r"^[A-Za-z]{2}$"),
        firm_id: str | None = Query(default=None),
    ) -> PaginatedLawyersResponse:
        from ._service_representation import get_lawyers

        with factory() as session:
            return get_lawyers(session, page, page_size, name=name, oab_state=oab_state, firm_id=firm_id)

    @app.get("/representation/lawyers/{lawyer_id}", response_model=LawyerDetailResponse)
    def representation_lawyer_detail(lawyer_id: str) -> LawyerDetailResponse:
        from ._service_representation import get_lawyer_detail

        with factory() as session:
            result = get_lawyer_detail(session, lawyer_id)
        if result is None:
            raise HTTPException(status_code=404, detail="lawyer_not_found")
        return result

    @app.get("/representation/firms", response_model=PaginatedFirmsResponse)
    def representation_firms(
        page: PositiveInt = 1,
        page_size: PageSize = 20,
        name: str | None = Query(default=None),
    ) -> PaginatedFirmsResponse:
        from ._service_representation import get_firms

        with factory() as session:
            return get_firms(session, page, page_size, name=name)

    @app.get("/representation/firms/{firm_id}", response_model=FirmDetailResponse)
    def representation_firm_detail(firm_id: str) -> FirmDetailResponse:
        from ._service_representation import get_firm_detail

        with factory() as session:
            result = get_firm_detail(session, firm_id)
        if result is None:
            raise HTTPException(status_code=404, detail="firm_not_found")
        return result

    @app.get("/representation/process/{process_id}", response_model=ProcessRepresentationResponse)
    def representation_process(process_id: str) -> ProcessRepresentationResponse:
        from ._service_representation import get_process_representation

        with factory() as session:
            return get_process_representation(session, process_id)

    @app.get("/representation/events", response_model=dict)
    def representation_events(
        page: PositiveInt = 1,
        page_size: PageSize = 20,
        lawyer_id: str | None = Query(default=None),
        firm_id: str | None = Query(default=None),
        event_type: str | None = Query(default=None),
    ) -> dict:
        from ._service_representation import get_representation_events

        with factory() as session:
            total, items = get_representation_events(
                session, page, page_size, lawyer_id=lawyer_id, firm_id=firm_id, event_type=event_type
            )
        return {"total": total, "page": page, "page_size": page_size, "items": [i.model_dump() for i in items]}

    @app.get("/representation/summary", response_model=RepresentationNetworkSummary)
    def representation_summary() -> RepresentationNetworkSummary:
        from ._service_representation import get_representation_summary

        with factory() as session:
            return get_representation_summary(session)
