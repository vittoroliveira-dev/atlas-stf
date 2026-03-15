"""Routes for agenda ministerial endpoints."""

from __future__ import annotations

from typing import Annotated

from fastapi import FastAPI, HTTPException, Query

from ._schemas_agenda import (
    AgendaEventItem,
    AgendaMinisterSummary,
    AgendaSummaryResponse,
    PaginatedAgendaEventsResponse,
    PaginatedAgendaExposuresResponse,
)

PositiveInt = Annotated[int, Query(ge=1)]
PageSize = Annotated[int, Query(ge=1, le=100)]


def register_agenda_routes(app: FastAPI, factory: ..., build_filters: ..., get_base_filters: ...) -> None:
    @app.get("/agenda/events", response_model=PaginatedAgendaEventsResponse)
    def agenda_events(
        page: PositiveInt = 1, page_size: PageSize = 20,
        minister_slug: str | None = Query(default=None),
        event_category: str | None = Query(default=None),
        has_process_ref: bool | None = Query(default=None),
        owner_scope: str | None = Query(default=None),
        date_from: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
        date_to: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    ) -> PaginatedAgendaEventsResponse:
        from ._service_agenda import get_agenda_events
        with factory() as session:
            return get_agenda_events(session, page, page_size,
                minister_slug=minister_slug, event_category=event_category,
                has_process_ref=has_process_ref, owner_scope=owner_scope,
                date_from=date_from, date_to=date_to)

    @app.get("/agenda/events/{event_id}", response_model=AgendaEventItem)
    def agenda_event_detail(event_id: str) -> AgendaEventItem:
        from ._service_agenda import get_agenda_event_detail
        with factory() as session:
            r = get_agenda_event_detail(session, event_id)
        if r is None:
            raise HTTPException(status_code=404, detail="agenda_event_not_found")
        return r

    @app.get("/agenda/ministers", response_model=list[AgendaMinisterSummary])
    def agenda_ministers() -> list[AgendaMinisterSummary]:
        from ._service_agenda import get_agenda_ministers
        with factory() as session:
            return get_agenda_ministers(session)

    @app.get("/agenda/ministers/{slug}", response_model=dict)
    def agenda_minister_detail(slug: str, page: PositiveInt = 1, page_size: PageSize = 20) -> dict:
        from ._service_agenda import get_agenda_minister_detail
        with factory() as session:
            r = get_agenda_minister_detail(session, slug, page, page_size)
        if r is None:
            raise HTTPException(status_code=404, detail="minister_not_found")
        return r

    @app.get("/agenda/exposures", response_model=PaginatedAgendaExposuresResponse)
    def agenda_exposures(
        page: PositiveInt = 1, page_size: PageSize = 20,
        minister_slug: str | None = Query(default=None),
        priority_tier: str | None = Query(default=None),
        process_id: str | None = Query(default=None),
        window: str | None = Query(default=None),
    ) -> PaginatedAgendaExposuresResponse:
        from ._service_agenda import get_agenda_exposures
        with factory() as session:
            return get_agenda_exposures(session, page, page_size,
                minister_slug=minister_slug, priority_tier=priority_tier,
                process_id=process_id, window=window)

    @app.get("/agenda/summary", response_model=AgendaSummaryResponse)
    def agenda_summary() -> AgendaSummaryResponse:
        from ._service_agenda import get_agenda_summary
        with factory() as session:
            return get_agenda_summary(session)
