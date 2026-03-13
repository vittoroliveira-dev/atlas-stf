from __future__ import annotations

from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query

from .schemas import EntityDetailResponse, MinisterCorrelationItem, PaginatedEntitiesResponse
from .service import (
    QueryFilters,
    get_counsel_detail,
    get_counsel_ministers,
    get_counsels,
    get_minister_counsels,
    get_minister_parties,
    get_parties,
    get_party_detail,
    get_party_ministers,
)

PositiveInt = Annotated[int, Query(ge=1)]
PageSize = Annotated[int, Query(ge=1, le=100)]
ListLimit = Annotated[int, Query(ge=1, le=500)]


def register_entities_routes(
    app: FastAPI,
    factory: ...,
    build_filters: ...,
    get_base_filters: ...,
) -> None:
    @app.get("/counsels", response_model=PaginatedEntitiesResponse)
    def counsels(
        minister: str | None = Query(default=None),
        base_filters: QueryFilters = Depends(get_base_filters),
        page: PositiveInt = 1,
        page_size: PageSize = 20,
    ) -> PaginatedEntitiesResponse:
        filters = build_filters(
            minister=minister,
            period=base_filters.period,
            collegiate=base_filters.collegiate,
            judging_body=base_filters.judging_body,
            process_class=base_filters.process_class,
        )
        with factory() as session:
            return get_counsels(session, filters, page, page_size)

    @app.get("/counsels/{counsel_id}", response_model=EntityDetailResponse)
    def counsel_detail(
        counsel_id: str,
        minister: str | None = Query(default=None),
        base_filters: QueryFilters = Depends(get_base_filters),
    ) -> EntityDetailResponse:
        filters = build_filters(
            minister=minister,
            period=base_filters.period,
            collegiate=base_filters.collegiate,
            judging_body=base_filters.judging_body,
            process_class=base_filters.process_class,
        )
        with factory() as session:
            detail = get_counsel_detail(session, counsel_id, filters)
        if detail is None:
            raise HTTPException(status_code=404, detail="counsel_not_found")
        return detail

    @app.get("/counsels/{counsel_id}/ministers", response_model=list[MinisterCorrelationItem])
    def counsel_ministers(
        counsel_id: str,
        minister: str | None = Query(default=None),
        base_filters: QueryFilters = Depends(get_base_filters),
        limit: ListLimit = 100,
    ) -> list[MinisterCorrelationItem]:
        filters = build_filters(
            minister=minister,
            period=base_filters.period,
            collegiate=base_filters.collegiate,
            judging_body=base_filters.judging_body,
            process_class=base_filters.process_class,
        )
        with factory() as session:
            detail = get_counsel_detail(session, counsel_id, filters)
            if detail is None:
                raise HTTPException(status_code=404, detail="counsel_not_found")
            return get_counsel_ministers(session, counsel_id, filters, limit=limit)

    @app.get("/ministers/{minister}/counsels", response_model=PaginatedEntitiesResponse)
    def minister_counsels(
        minister: str,
        base_filters: QueryFilters = Depends(get_base_filters),
        page: PositiveInt = 1,
        page_size: PageSize = 20,
    ) -> PaginatedEntitiesResponse:
        filters = build_filters(
            minister=minister,
            period=base_filters.period,
            collegiate=base_filters.collegiate,
            judging_body=base_filters.judging_body,
            process_class=base_filters.process_class,
        )
        with factory() as session:
            return get_minister_counsels(session, minister, filters, page, page_size)

    @app.get("/parties", response_model=PaginatedEntitiesResponse)
    def parties(
        minister: str | None = Query(default=None),
        base_filters: QueryFilters = Depends(get_base_filters),
        page: PositiveInt = 1,
        page_size: PageSize = 20,
    ) -> PaginatedEntitiesResponse:
        filters = build_filters(
            minister=minister,
            period=base_filters.period,
            collegiate=base_filters.collegiate,
            judging_body=base_filters.judging_body,
            process_class=base_filters.process_class,
        )
        with factory() as session:
            return get_parties(session, filters, page, page_size)

    @app.get("/parties/{party_id}", response_model=EntityDetailResponse)
    def party_detail(
        party_id: str,
        minister: str | None = Query(default=None),
        base_filters: QueryFilters = Depends(get_base_filters),
    ) -> EntityDetailResponse:
        filters = build_filters(
            minister=minister,
            period=base_filters.period,
            collegiate=base_filters.collegiate,
            judging_body=base_filters.judging_body,
            process_class=base_filters.process_class,
        )
        with factory() as session:
            detail = get_party_detail(session, party_id, filters)
        if detail is None:
            raise HTTPException(status_code=404, detail="party_not_found")
        return detail

    @app.get("/parties/{party_id}/ministers", response_model=list[MinisterCorrelationItem])
    def party_ministers(
        party_id: str,
        minister: str | None = Query(default=None),
        base_filters: QueryFilters = Depends(get_base_filters),
        limit: ListLimit = 100,
    ) -> list[MinisterCorrelationItem]:
        filters = build_filters(
            minister=minister,
            period=base_filters.period,
            collegiate=base_filters.collegiate,
            judging_body=base_filters.judging_body,
            process_class=base_filters.process_class,
        )
        with factory() as session:
            detail = get_party_detail(session, party_id, filters)
            if detail is None:
                raise HTTPException(status_code=404, detail="party_not_found")
            return get_party_ministers(session, party_id, filters, limit=limit)

    @app.get("/ministers/{minister}/parties", response_model=PaginatedEntitiesResponse)
    def minister_parties(
        minister: str,
        base_filters: QueryFilters = Depends(get_base_filters),
        page: PositiveInt = 1,
        page_size: PageSize = 20,
    ) -> PaginatedEntitiesResponse:
        filters = build_filters(
            minister=minister,
            period=base_filters.period,
            collegiate=base_filters.collegiate,
            judging_body=base_filters.judging_body,
            process_class=base_filters.process_class,
        )
        with factory() as session:
            return get_minister_parties(session, minister, filters, page, page_size)
