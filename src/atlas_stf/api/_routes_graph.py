"""Routes for graph, investigation, and review endpoints.

The review decision POST endpoint is an explicit exception to ADR-004
(GET-only read-only API).  It exists solely for the calibration/review
workflow where human reviewers confirm or reject flagged items.
"""

from __future__ import annotations

import os
import secrets
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query, Security
from fastapi.security import APIKeyHeader

from ._schemas_graph import (
    BuildMetricsResponse,
    GraphEdgeItem,
    GraphNeighborResponse,
    GraphNodeItem,
    InvestigationDetailResponse,
    InvestigationSummary,
    PaginatedInvestigationsResponse,
    PaginatedNodesResponse,
    PaginatedPathsResponse,
    PaginatedReviewResponse,
    PaginatedScoresResponse,
    ReviewDecisionRequest,
    ReviewQueueItem,
)

PositiveInt = Annotated[int, Query(ge=1)]
PageSize = Annotated[int, Query(ge=1, le=100)]
TopLimit = Annotated[int, Query(ge=1, le=500)]


_REVIEW_API_KEY_HEADER = APIKeyHeader(name="X-Review-API-Key", auto_error=False)


_REVIEW_AUTH_DEV_BYPASS = "__dev__"


def _get_review_api_key() -> str:
    """Read the review API key from environment. Empty string = not configured."""
    return os.getenv("ATLAS_STF_REVIEW_API_KEY", "")


async def _require_review_auth(
    api_key: str | None = Security(_REVIEW_API_KEY_HEADER),
) -> None:
    """Dependency that enforces API key auth on write endpoints.

    Fail-closed by default: when ``ATLAS_STF_REVIEW_API_KEY`` is not set,
    returns 503 (review endpoint unavailable).

    Set to ``__dev__`` to explicitly opt in to unauthenticated access
    during development/testing.  In production, set to a random secret
    and pass it via ``X-Review-API-Key`` header.
    """
    expected = _get_review_api_key()
    if not expected:
        raise HTTPException(
            status_code=503,
            detail="review_endpoint_unavailable_no_api_key_configured",
        )
    if expected == _REVIEW_AUTH_DEV_BYPASS:
        return  # explicit dev/test bypass
    if not api_key or not secrets.compare_digest(api_key, expected):
        raise HTTPException(status_code=401, detail="invalid_or_missing_review_api_key")


def register_graph_routes(
    app: FastAPI,
    factory: ...,
    build_filters: ...,
    get_base_filters: ...,
) -> None:
    # ------------------------------------------------------------------
    # Graph core
    # ------------------------------------------------------------------

    @app.get("/graph/search", response_model=PaginatedNodesResponse)
    def graph_search(
        query: str | None = Query(default=None),
        node_type: str | None = Query(default=None),
        page: PositiveInt = 1,
        page_size: PageSize = 20,
    ) -> PaginatedNodesResponse:
        from ._service_graph import search_nodes

        with factory() as session:
            return search_nodes(session, query, node_type, page, page_size)

    @app.get("/graph/nodes/{node_id}", response_model=GraphNodeItem)
    def graph_node_detail(node_id: str) -> GraphNodeItem:
        from ._service_graph import get_node

        with factory() as session:
            result = get_node(session, node_id)
        if result is None:
            raise HTTPException(status_code=404, detail="node_not_found")
        return result

    @app.get("/graph/edges/{edge_id}", response_model=GraphEdgeItem)
    def graph_edge_detail(edge_id: str) -> GraphEdgeItem:
        from ._service_graph import get_edge

        with factory() as session:
            result = get_edge(session, edge_id)
        if result is None:
            raise HTTPException(status_code=404, detail="edge_not_found")
        return result

    @app.get("/graph/neighbors/{node_id}", response_model=GraphNeighborResponse)
    def graph_neighbors(
        node_id: str,
        edge_types: str | None = Query(default=None, description="Comma-separated edge types"),
        evidence_strength_min: str | None = Query(default=None),
        exclude_truncated: bool = Query(default=True),
        depth: Annotated[int, Query(ge=1, le=1)] = 1,
    ) -> GraphNeighborResponse:
        from ._service_graph import get_neighbors

        parsed_edge_types = [t.strip() for t in edge_types.split(",") if t.strip()] if edge_types else None
        with factory() as session:
            result = get_neighbors(
                session,
                node_id,
                edge_types=parsed_edge_types,
                evidence_strength_min=evidence_strength_min,
                exclude_truncated=exclude_truncated,
                depth=depth,
            )
        if result is None:
            raise HTTPException(status_code=404, detail="node_not_found")
        return result

    @app.get("/graph/paths", response_model=PaginatedPathsResponse)
    def graph_paths(
        start_id: str = Query(...),
        end_id: str = Query(...),
        mode: str | None = Query(default="strict"),
        page: PositiveInt = 1,
        page_size: PageSize = 20,
    ) -> PaginatedPathsResponse:
        from ._service_graph import get_paths

        with factory() as session:
            return get_paths(session, start_id, end_id, mode, page, page_size)

    @app.get("/graph/explain/{entity_id}", response_model=InvestigationSummary)
    def graph_explain(entity_id: str) -> InvestigationSummary:
        from ._service_graph import explain_entity

        with factory() as session:
            result = explain_entity(session, entity_id)
        if result is None:
            raise HTTPException(status_code=404, detail="entity_not_found")
        return result

    @app.get("/graph/scores", response_model=PaginatedScoresResponse)
    def graph_scores(
        mode: str | None = Query(default="broad"),
        min_signals: Annotated[int, Query(ge=0)] = 2,
        page: PositiveInt = 1,
        page_size: PageSize = 20,
    ) -> PaginatedScoresResponse:
        from ._service_graph import get_scores

        with factory() as session:
            return get_scores(session, mode, min_signals, page, page_size)

    @app.get("/graph/metrics", response_model=BuildMetricsResponse)
    def graph_metrics() -> BuildMetricsResponse:
        from ._service_graph import get_build_metrics

        with factory() as session:
            return get_build_metrics(session)

    # ------------------------------------------------------------------
    # Investigations
    # ------------------------------------------------------------------

    @app.get("/investigations/top", response_model=PaginatedInvestigationsResponse)
    def investigations_top(
        mode: str | None = Query(default="broad"),
        min_signals: Annotated[int, Query(ge=0)] = 2,
        limit: TopLimit = 100,
        page: PositiveInt = 1,
    ) -> PaginatedInvestigationsResponse:
        from ._service_graph_review import get_top_investigations

        page_size = min(limit, 500)
        with factory() as session:
            return get_top_investigations(session, mode, min_signals, page, page_size)

    @app.get("/investigations/entity/{entity_id}", response_model=InvestigationDetailResponse)
    def investigation_entity_detail(entity_id: str) -> InvestigationDetailResponse:
        from ._service_graph_review import get_investigation_by_entity

        with factory() as session:
            result = get_investigation_by_entity(session, entity_id)
        if result is None:
            raise HTTPException(status_code=404, detail="entity_not_found")
        return result

    # ------------------------------------------------------------------
    # Review queue
    # ------------------------------------------------------------------

    @app.get("/review/queue", response_model=PaginatedReviewResponse)
    def review_queue(
        status: str | None = Query(
            default="pending",
            pattern="^(pending|confirmed_relevant|false_positive|needs_more_data|deferred)$",
        ),
        queue_type: str | None = Query(default=None, pattern="^(calibration|investigation)$"),
        tier: str | None = Query(default=None, pattern="^(high|medium|low)$"),
        page: PositiveInt = 1,
        page_size: PageSize = 20,
    ) -> PaginatedReviewResponse:
        from ._service_graph_review import get_review_queue

        with factory() as session:
            return get_review_queue(session, status, queue_type, tier, page, page_size)

    @app.post("/review/decision", response_model=ReviewQueueItem, dependencies=[Depends(_require_review_auth)])
    def review_decision(body: ReviewDecisionRequest) -> ReviewQueueItem:
        """Record a human review decision.

        Exception to ADR-004 (GET-only): this POST endpoint exists for the
        calibration/review workflow where reviewers confirm or reject flagged
        items.  It is the only write endpoint in the system.
        """
        from ._service_graph_review import record_review_decision

        with factory() as session:
            try:
                result = record_review_decision(session, body.item_id, body.status, body.notes)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
        if result is None:
            raise HTTPException(status_code=404, detail="review_item_not_found")
        return result
