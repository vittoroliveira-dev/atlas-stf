"""Service layer for investigation and review queue queries."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import cast

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from atlas_stf.serving._models_graph import (
    ServingEvidenceBundle,
    ServingGraphEdge,
    ServingGraphNode,
    ServingGraphPathCandidate,
    ServingGraphScore,
    ServingReviewQueue,
)

from ._schemas_graph import (
    InvestigationDetailResponse,
    InvestigationSummary,
    PaginatedInvestigationsResponse,
    PaginatedReviewResponse,
    ReviewQueueItem,
)
from ._service_graph import (
    _bundle_to_item,
    _edge_to_item,
    _node_to_item,
    _path_to_item,
    _score_filter_stmts,
    _score_to_item,
)

_VALID_REVIEW_STATUSES = frozenset(
    {
        "confirmed_relevant",
        "false_positive",
        "needs_more_data",
        "deferred",
    }
)


# ---------------------------------------------------------------------------
# Investigation functions
# ---------------------------------------------------------------------------


def get_top_investigations(
    session: Session,
    mode: str | None,
    min_signals: int,
    page: int,
    page_size: int,
) -> PaginatedInvestigationsResponse:
    """Top entities by operational_priority."""
    score_stmt, count_stmt = _score_filter_stmts(mode, min_signals)

    total = session.execute(count_stmt).scalar_one()
    score_stmt = score_stmt.order_by(
        ServingGraphScore.operational_priority.desc(),
        ServingGraphScore.entity_id.asc(),
    )
    scores = cast(
        list[ServingGraphScore],
        session.scalars(score_stmt.offset((page - 1) * page_size).limit(page_size)).all(),
    )

    # Batch-load related data for the page
    entity_ids = [s.entity_id for s in scores if s.entity_id]
    nodes_map: dict[str, ServingGraphNode] = {}
    bundles_map: dict[str, ServingEvidenceBundle] = {}
    if entity_ids:
        node_rows = cast(
            list[ServingGraphNode],
            session.scalars(select(ServingGraphNode).where(ServingGraphNode.entity_id.in_(entity_ids))).all(),
        )
        for n in node_rows:
            if n.entity_id and n.entity_id not in nodes_map:
                nodes_map[n.entity_id] = n

        bundle_rows = cast(
            list[ServingEvidenceBundle],
            session.scalars(select(ServingEvidenceBundle).where(ServingEvidenceBundle.entity_id.in_(entity_ids))).all(),
        )
        for b in bundle_rows:
            if b.entity_id and b.entity_id not in bundles_map:
                bundles_map[b.entity_id] = b

    items: list[InvestigationSummary] = []
    for s in scores:
        eid = s.entity_id or ""
        node = nodes_map.get(eid)
        bundle = bundles_map.get(eid)
        items.append(
            InvestigationSummary(
                entity_id=eid,
                entity_label=node.canonical_label if node else None,
                node_type=node.node_type if node else None,
                score=_score_to_item(s),
                bundle=_bundle_to_item(bundle) if bundle else None,
                edge_count=0,  # omitted in list view for performance
                signal_count=bundle.signal_count if bundle else 0,
            )
        )

    return PaginatedInvestigationsResponse(total=total, page=page, page_size=page_size, items=items)


def get_investigation_by_entity(
    session: Session,
    entity_id: str,
) -> InvestigationDetailResponse | None:
    """Full investigation detail: node, score, bundle, paths, edges."""
    node = cast(
        ServingGraphNode | None,
        session.scalars(
            select(ServingGraphNode)
            .where(ServingGraphNode.entity_id == entity_id)
            .order_by(ServingGraphNode.node_id.asc())
            .limit(1)
        ).first(),
    )
    if node is None:
        return None

    score_row = cast(
        ServingGraphScore | None,
        session.scalars(
            select(ServingGraphScore)
            .where(ServingGraphScore.entity_id == entity_id)
            .order_by(ServingGraphScore.operational_priority.desc(), ServingGraphScore.score_id.asc())
            .limit(1)
        ).first(),
    )
    bundle_rows = cast(
        list[ServingEvidenceBundle],
        session.scalars(
            select(ServingEvidenceBundle)
            .where(ServingEvidenceBundle.entity_id == entity_id)
            .order_by(ServingEvidenceBundle.bundle_id.asc())
            .limit(50)
        ).all(),
    )
    edges = cast(
        list[ServingGraphEdge],
        session.scalars(
            select(ServingGraphEdge)
            .where(or_(ServingGraphEdge.src_node_id == node.node_id, ServingGraphEdge.dst_node_id == node.node_id))
            .order_by(ServingGraphEdge.edge_id.asc())
            .limit(500)
        ).all(),
    )
    paths = cast(
        list[ServingGraphPathCandidate],
        session.scalars(
            select(ServingGraphPathCandidate)
            .where(
                or_(
                    ServingGraphPathCandidate.start_node_id == node.node_id,
                    ServingGraphPathCandidate.end_node_id == node.node_id,
                )
            )
            .order_by(ServingGraphPathCandidate.total_cost.asc(), ServingGraphPathCandidate.path_id.asc())
            .limit(100)
        ).all(),
    )

    return InvestigationDetailResponse(
        entity_id=entity_id,
        node=_node_to_item(node),
        score=_score_to_item(score_row) if score_row else None,
        bundles=[_bundle_to_item(b) for b in bundle_rows],
        edges=[_edge_to_item(e) for e in edges],
        paths=[_path_to_item(p) for p in paths],
    )


# ---------------------------------------------------------------------------
# Review queue functions
# ---------------------------------------------------------------------------


def _review_to_item(row: ServingReviewQueue) -> ReviewQueueItem:
    reason = row.review_reason or ""
    reason_lower = reason.lower()
    if row.priority_tier == "low" or "fuzzy" in reason_lower or "truncated" in reason_lower:
        queue_type = "calibration"
    else:
        queue_type = "investigation"

    return ReviewQueueItem(
        item_id=row.item_id,
        entity_id=row.entity_id,
        path_id=row.path_id,
        bundle_id=row.bundle_id,
        priority_score=row.priority_score,
        priority_tier=row.priority_tier,
        review_reason=row.review_reason,
        status=row.status,
        queue_type=queue_type,
    )


def get_review_queue(
    session: Session,
    status: str | None,
    queue_type: str | None,
    tier: str | None,
    page: int,
    page_size: int,
) -> PaginatedReviewResponse:
    """Paginated review items with queue_type classification."""
    stmt = select(ServingReviewQueue)
    count_stmt = select(func.count()).select_from(ServingReviewQueue)

    if status:
        stmt = stmt.where(ServingReviewQueue.status == status)
        count_stmt = count_stmt.where(ServingReviewQueue.status == status)
    if tier:
        stmt = stmt.where(ServingReviewQueue.priority_tier == tier)
        count_stmt = count_stmt.where(ServingReviewQueue.priority_tier == tier)
    if queue_type == "calibration":
        calibration_filter = or_(
            ServingReviewQueue.priority_tier == "low",
            func.lower(func.coalesce(ServingReviewQueue.review_reason, "")).like("%fuzzy%"),
            func.lower(func.coalesce(ServingReviewQueue.review_reason, "")).like("%truncated%"),
        )
        stmt = stmt.where(calibration_filter)
        count_stmt = count_stmt.where(calibration_filter)
    elif queue_type == "investigation":
        investigation_filter = ~or_(
            ServingReviewQueue.priority_tier == "low",
            func.lower(func.coalesce(ServingReviewQueue.review_reason, "")).like("%fuzzy%"),
            func.lower(func.coalesce(ServingReviewQueue.review_reason, "")).like("%truncated%"),
        )
        stmt = stmt.where(investigation_filter)
        count_stmt = count_stmt.where(investigation_filter)

    total = session.execute(count_stmt).scalar_one()
    stmt = stmt.order_by(ServingReviewQueue.priority_score.desc(), ServingReviewQueue.item_id.asc())
    rows = cast(
        list[ServingReviewQueue],
        session.scalars(stmt.offset((page - 1) * page_size).limit(page_size)).all(),
    )
    return PaginatedReviewResponse(
        total=total, page=page, page_size=page_size, items=[_review_to_item(r) for r in rows]
    )


def record_review_decision(
    session: Session,
    item_id: str,
    status: str,
    notes: str | None,
) -> ReviewQueueItem | None:
    """Update review item status. Returns updated item or None if not found.

    This is the ONLY write operation in the API -- an explicit exception to
    ADR-004 (GET-only read-only) for the review/calibration workflow.
    """
    if status not in _VALID_REVIEW_STATUSES:
        raise ValueError(f"Invalid status: {status!r}. Must be one of: {sorted(_VALID_REVIEW_STATUSES)}")

    row = session.get(ServingReviewQueue, item_id)
    if row is None:
        return None

    row.status = status
    row.review_notes = notes
    row.reviewed_at = datetime.now(timezone.utc)
    session.commit()

    return _review_to_item(row)
