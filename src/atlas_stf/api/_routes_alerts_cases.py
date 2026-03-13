from __future__ import annotations

from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query

from .schemas import (
    AlertSummaryItem,
    CaseDetailResponse,
    MlOutlierScoreResponse,
    PaginatedAlertsResponse,
    PaginatedCasesResponse,
)
from .service import (
    QueryFilters,
    get_alert_detail,
    get_alerts,
    get_case_detail,
    get_case_ml_outlier,
    get_cases,
    get_related_alerts_for_case,
)

PositiveInt = Annotated[int, Query(ge=1)]
PageSize = Annotated[int, Query(ge=1, le=100)]
ListLimit = Annotated[int, Query(ge=1, le=500)]


def register_alerts_cases_routes(
    app: FastAPI,
    factory: ...,
    build_filters: ...,
    get_base_filters: ...,
) -> None:
    @app.get("/alerts", response_model=PaginatedAlertsResponse)
    def alerts(
        minister: str | None = Query(default=None),
        base_filters: QueryFilters = Depends(get_base_filters),
        page: PositiveInt = 1,
        page_size: PageSize = 20,
    ) -> PaginatedAlertsResponse:
        filters = build_filters(
            minister=minister,
            period=base_filters.period,
            collegiate=base_filters.collegiate,
            judging_body=base_filters.judging_body,
            process_class=base_filters.process_class,
        )
        with factory() as session:
            return get_alerts(session, filters, page, page_size)

    @app.get("/alerts/{alert_id}", response_model=AlertSummaryItem)
    def alert_detail(
        alert_id: str,
    ) -> AlertSummaryItem:
        with factory() as session:
            item = get_alert_detail(session, alert_id)
        if item is None:
            raise HTTPException(status_code=404, detail="alert_not_found")
        return item

    @app.get("/cases", response_model=PaginatedCasesResponse)
    def cases(
        minister: str | None = Query(default=None),
        base_filters: QueryFilters = Depends(get_base_filters),
        page: PositiveInt = 1,
        page_size: PageSize = 20,
    ) -> PaginatedCasesResponse:
        filters = build_filters(
            minister=minister,
            period=base_filters.period,
            collegiate=base_filters.collegiate,
            judging_body=base_filters.judging_body,
            process_class=base_filters.process_class,
        )
        with factory() as session:
            return get_cases(session, filters, page, page_size)

    @app.get("/cases/{decision_event_id}", response_model=CaseDetailResponse)
    def case_detail(
        decision_event_id: str,
        minister: str | None = Query(default=None),
        base_filters: QueryFilters = Depends(get_base_filters),
    ) -> CaseDetailResponse:
        # Contract: the case must belong to the current filtered recorte.
        filters = build_filters(
            minister=minister,
            period=base_filters.period,
            collegiate=base_filters.collegiate,
            judging_body=base_filters.judging_body,
            process_class=base_filters.process_class,
        )
        with factory() as session:
            detail = get_case_detail(session, filters, decision_event_id)
        if detail.case_item is None:
            raise HTTPException(status_code=404, detail="case_not_found")
        return detail

    @app.get("/cases/{decision_event_id}/ml-outlier", response_model=MlOutlierScoreResponse)
    def case_ml_outlier(
        decision_event_id: str,
        minister: str | None = Query(default=None),
        base_filters: QueryFilters = Depends(get_base_filters),
    ) -> MlOutlierScoreResponse:
        filters = build_filters(
            minister=minister,
            period=base_filters.period,
            collegiate=base_filters.collegiate,
            judging_body=base_filters.judging_body,
            process_class=base_filters.process_class,
        )
        with factory() as session:
            detail = get_case_ml_outlier(session, filters, decision_event_id)
        if detail is None:
            raise HTTPException(status_code=404, detail="ml_outlier_not_found")
        return detail

    @app.get("/cases/{decision_event_id}/related-alerts", response_model=list[AlertSummaryItem])
    def related_alerts(
        decision_event_id: str,
        limit: ListLimit = 100,
    ) -> list[AlertSummaryItem]:
        with factory() as session:
            return get_related_alerts_for_case(session, decision_event_id, limit=limit)
