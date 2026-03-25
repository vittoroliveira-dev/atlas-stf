"""Service layer for graph queries (nodes, edges, neighbors, paths, scores)."""

from __future__ import annotations

import json
from typing import Any, cast

from sqlalchemy import Select, func, or_, select
from sqlalchemy.orm import Session

from atlas_stf.serving._models_graph import (
    ServingEvidenceBundle,
    ServingGraphEdge,
    ServingGraphNode,
    ServingGraphPathCandidate,
    ServingGraphScore,
    ServingModuleAvailability,
)

from ._filters import _normalized_like
from ._schemas_graph import (
    BuildMetricsResponse,
    EvidenceBundleItem,
    GraphEdgeItem,
    GraphNeighborResponse,
    GraphNodeItem,
    GraphPathItem,
    GraphScoreItem,
    InvestigationSummary,
    PaginatedNodesResponse,
    PaginatedPathsResponse,
    PaginatedScoresResponse,
)

# ---------------------------------------------------------------------------
# JSON deserialization helpers
# ---------------------------------------------------------------------------


def _safe_json_loads(raw: str | None) -> Any:
    """Parse a JSON string, returning None on failure or None input."""
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError, TypeError:
        return None


def _safe_json_list(raw: str | None) -> list:
    result = _safe_json_loads(raw)
    return result if isinstance(result, list) else []


def _safe_json_dict(raw: str | None) -> dict | None:
    result = _safe_json_loads(raw)
    return result if isinstance(result, dict) else None


# ---------------------------------------------------------------------------
# ORM -> Pydantic converters
# ---------------------------------------------------------------------------


def _node_to_item(row: ServingGraphNode) -> GraphNodeItem:
    return GraphNodeItem(
        node_id=row.node_id,
        node_type=row.node_type,
        canonical_label=row.canonical_label,
        entity_id=row.entity_id,
        entity_identifier=row.entity_identifier,
        entity_identifier_type=row.entity_identifier_type,
        entity_identifier_quality=row.entity_identifier_quality,
        active_flag=row.active_flag,
    )


def _edge_to_item(row: ServingGraphEdge) -> GraphEdgeItem:
    return GraphEdgeItem(
        edge_id=row.edge_id,
        src_node_id=row.src_node_id,
        dst_node_id=row.dst_node_id,
        edge_type=row.edge_type,
        confidence_score=row.confidence_score,
        evidence_strength=row.evidence_strength,
        match_strategy=row.match_strategy,
        match_score=row.match_score,
        traversal_policy=row.traversal_policy,
        truncated_flag=row.truncated_flag,
        weight=row.weight,
        explanation=_safe_json_dict(row.explanation_json),
    )


def _score_to_item(row: ServingGraphScore) -> GraphScoreItem:
    return GraphScoreItem(
        score_id=row.score_id,
        entity_id=row.entity_id,
        traversal_mode=row.traversal_mode,
        signal_registry=row.signal_registry,
        documentary_score=row.documentary_score,
        statistical_score=row.statistical_score,
        network_score=row.network_score,
        temporal_score=row.temporal_score,
        fuzzy_penalty=row.fuzzy_penalty,
        truncation_penalty=row.truncation_penalty,
        singleton_penalty=row.singleton_penalty,
        missing_identifier_penalty=row.missing_identifier_penalty,
        raw_score=row.raw_score,
        calibrated_score=row.calibrated_score,
        operational_priority=row.operational_priority,
        explanation=_safe_json_dict(row.explanation_json),
    )


def _path_to_item(row: ServingGraphPathCandidate) -> GraphPathItem:
    return GraphPathItem(
        path_id=row.path_id,
        start_node_id=row.start_node_id,
        end_node_id=row.end_node_id,
        path_length=row.path_length,
        total_cost=row.total_cost,
        min_confidence=row.min_confidence,
        min_evidence_strength=row.min_evidence_strength,
        traversal_mode=row.traversal_mode,
        has_truncated_edge=row.has_truncated_edge,
        has_fuzzy_edge=row.has_fuzzy_edge,
        edges=_safe_json_list(row.path_edges_json),
    )


def _bundle_to_item(row: ServingEvidenceBundle) -> EvidenceBundleItem:
    return EvidenceBundleItem(
        bundle_id=row.bundle_id,
        entity_id=row.entity_id,
        bundle_type=row.bundle_type,
        signal_count=row.signal_count,
        signal_types=_safe_json_list(row.signal_types_json),
        summary_text=row.summary_text,
        evidence=_safe_json_list(row.evidence_json),
    )


def _score_filter_stmts(
    mode: str | None,
    min_signals: int,
) -> tuple[Select, Select]:
    """Build score select + count statements with optional mode/min_signals filters."""
    base_where = []
    if mode:
        base_where.append(ServingGraphScore.traversal_mode == mode)

    if min_signals > 0:
        stmt = (
            select(ServingGraphScore)
            .join(
                ServingEvidenceBundle,
                ServingGraphScore.entity_id == ServingEvidenceBundle.entity_id,
                isouter=True,
            )
            .where(func.coalesce(ServingEvidenceBundle.signal_count, 0) >= min_signals, *base_where)
        )
        count_stmt = (
            select(func.count(ServingGraphScore.score_id))
            .join(
                ServingEvidenceBundle,
                ServingGraphScore.entity_id == ServingEvidenceBundle.entity_id,
                isouter=True,
            )
            .where(func.coalesce(ServingEvidenceBundle.signal_count, 0) >= min_signals, *base_where)
        )
    else:
        stmt = select(ServingGraphScore).where(*base_where) if base_where else select(ServingGraphScore)
        count_stmt = (
            select(func.count()).select_from(ServingGraphScore).where(*base_where)
            if base_where
            else select(func.count()).select_from(ServingGraphScore)
        )
    return stmt, count_stmt


# ---------------------------------------------------------------------------
# Service functions — graph core
# ---------------------------------------------------------------------------


def search_nodes(
    session: Session,
    query: str | None,
    node_type: str | None,
    page: int,
    page_size: int,
) -> PaginatedNodesResponse:
    """LIKE search on canonical_label, optionally filtered by node_type."""
    stmt = select(ServingGraphNode)
    count_stmt = select(func.count()).select_from(ServingGraphNode)

    if query:
        stmt = stmt.where(_normalized_like(ServingGraphNode.canonical_label, query))
        count_stmt = count_stmt.where(_normalized_like(ServingGraphNode.canonical_label, query))
    if node_type:
        stmt = stmt.where(ServingGraphNode.node_type == node_type)
        count_stmt = count_stmt.where(ServingGraphNode.node_type == node_type)

    total = session.execute(count_stmt).scalar_one()
    stmt = stmt.order_by(ServingGraphNode.canonical_label.asc(), ServingGraphNode.node_id.asc())
    rows = cast(
        list[ServingGraphNode],
        session.scalars(stmt.offset((page - 1) * page_size).limit(page_size)).all(),
    )
    return PaginatedNodesResponse(total=total, page=page, page_size=page_size, items=[_node_to_item(r) for r in rows])


def get_node(session: Session, node_id: str) -> GraphNodeItem | None:
    """Single node by primary key."""
    row = session.get(ServingGraphNode, node_id)
    return _node_to_item(row) if row is not None else None


def get_edge(session: Session, edge_id: str) -> GraphEdgeItem | None:
    """Single edge by primary key, with parsed explanation_json."""
    row = session.get(ServingGraphEdge, edge_id)
    return _edge_to_item(row) if row is not None else None


def get_neighbors(
    session: Session,
    node_id: str,
    *,
    edge_types: list[str] | None = None,
    evidence_strength_min: str | None = None,
    exclude_truncated: bool = True,
    depth: int = 1,
) -> GraphNeighborResponse | None:
    """Neighbors of a node + connecting edges. depth=1 only."""
    center = session.get(ServingGraphNode, node_id)
    if center is None:
        return None

    _ = depth  # accepted but capped at 1

    edge_stmt = select(ServingGraphEdge).where(
        or_(ServingGraphEdge.src_node_id == node_id, ServingGraphEdge.dst_node_id == node_id)
    )
    if edge_types:
        edge_stmt = edge_stmt.where(ServingGraphEdge.edge_type.in_(edge_types))
    if evidence_strength_min:
        _STRENGTH_ORDER = {"documentary": 4, "statistical": 3, "fuzzy": 2, "none": 1}
        min_level = _STRENGTH_ORDER.get(evidence_strength_min, 0)
        allowed = [k for k, v in _STRENGTH_ORDER.items() if v >= min_level]
        edge_stmt = edge_stmt.where(ServingGraphEdge.evidence_strength.in_(allowed))
    if exclude_truncated:
        edge_stmt = edge_stmt.where(ServingGraphEdge.truncated_flag == False)  # noqa: E712

    edge_stmt = edge_stmt.order_by(ServingGraphEdge.edge_id.asc()).limit(500)
    edges = cast(list[ServingGraphEdge], session.scalars(edge_stmt).all())

    neighbor_ids: set[str] = set()
    for e in edges:
        if e.src_node_id != node_id:
            neighbor_ids.add(e.src_node_id)
        if e.dst_node_id != node_id:
            neighbor_ids.add(e.dst_node_id)

    neighbors: list[GraphNodeItem] = []
    if neighbor_ids:
        rows = cast(
            list[ServingGraphNode],
            session.scalars(
                select(ServingGraphNode)
                .where(ServingGraphNode.node_id.in_(sorted(neighbor_ids)))
                .order_by(ServingGraphNode.node_id.asc())
            ).all(),
        )
        neighbors = [_node_to_item(r) for r in rows]

    return GraphNeighborResponse(
        center_node=_node_to_item(center),
        neighbors=neighbors,
        edges=[_edge_to_item(e) for e in edges],
    )


def get_paths(
    session: Session,
    start_id: str,
    end_id: str,
    mode: str | None,
    page: int,
    page_size: int,
) -> PaginatedPathsResponse:
    """Paths between two nodes, filtered by traversal_mode."""
    stmt = select(ServingGraphPathCandidate).where(
        ServingGraphPathCandidate.start_node_id == start_id,
        ServingGraphPathCandidate.end_node_id == end_id,
    )
    count_stmt = (
        select(func.count())
        .select_from(ServingGraphPathCandidate)
        .where(
            ServingGraphPathCandidate.start_node_id == start_id,
            ServingGraphPathCandidate.end_node_id == end_id,
        )
    )

    if mode:
        stmt = stmt.where(ServingGraphPathCandidate.traversal_mode == mode)
        count_stmt = count_stmt.where(ServingGraphPathCandidate.traversal_mode == mode)

    total = session.execute(count_stmt).scalar_one()
    stmt = stmt.order_by(ServingGraphPathCandidate.total_cost.asc(), ServingGraphPathCandidate.path_id.asc())
    rows = cast(
        list[ServingGraphPathCandidate],
        session.scalars(stmt.offset((page - 1) * page_size).limit(page_size)).all(),
    )
    return PaginatedPathsResponse(total=total, page=page, page_size=page_size, items=[_path_to_item(r) for r in rows])


def explain_entity(session: Session, entity_id: str) -> InvestigationSummary | None:
    """Node + bundle + score + edge count for an entity."""
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
    bundle_row = cast(
        ServingEvidenceBundle | None,
        session.scalars(
            select(ServingEvidenceBundle)
            .where(ServingEvidenceBundle.entity_id == entity_id)
            .order_by(ServingEvidenceBundle.bundle_id.asc())
            .limit(1)
        ).first(),
    )
    edge_count = session.execute(
        select(func.count())
        .select_from(ServingGraphEdge)
        .where(or_(ServingGraphEdge.src_node_id == node.node_id, ServingGraphEdge.dst_node_id == node.node_id))
    ).scalar_one()

    return InvestigationSummary(
        entity_id=entity_id,
        entity_label=node.canonical_label,
        node_type=node.node_type,
        score=_score_to_item(score_row) if score_row else None,
        bundle=_bundle_to_item(bundle_row) if bundle_row else None,
        edge_count=edge_count,
        signal_count=bundle_row.signal_count if bundle_row else 0,
    )


def get_scores(
    session: Session,
    mode: str | None,
    min_signals: int,
    page: int,
    page_size: int,
) -> PaginatedScoresResponse:
    """Paginated scores, optionally filtered by traversal_mode and min signals."""
    stmt, count_stmt = _score_filter_stmts(mode, min_signals)
    total = session.execute(count_stmt).scalar_one()
    stmt = stmt.order_by(ServingGraphScore.operational_priority.desc(), ServingGraphScore.score_id.asc())
    rows = cast(
        list[ServingGraphScore],
        session.scalars(stmt.offset((page - 1) * page_size).limit(page_size)).all(),
    )
    return PaginatedScoresResponse(total=total, page=page, page_size=page_size, items=[_score_to_item(r) for r in rows])


def get_build_metrics(session: Session) -> BuildMetricsResponse:
    """Compute aggregate metrics from graph tables."""
    total_nodes = session.execute(select(func.count()).select_from(ServingGraphNode)).scalar_one()
    total_edges = session.execute(select(func.count()).select_from(ServingGraphEdge)).scalar_one()
    total_scores = session.execute(select(func.count()).select_from(ServingGraphScore)).scalar_one()

    truncated_edges = session.execute(
        select(func.count()).select_from(ServingGraphEdge).where(ServingGraphEdge.truncated_flag == True)  # noqa: E712
    ).scalar_one()
    fuzzy_edges = session.execute(
        select(func.count()).select_from(ServingGraphEdge).where(ServingGraphEdge.match_strategy == "fuzzy")
    ).scalar_one()
    deterministic_edges = total_edges - fuzzy_edges

    pct_deterministic = (deterministic_edges / total_edges * 100.0) if total_edges > 0 else 0.0
    pct_fuzzy = (fuzzy_edges / total_edges * 100.0) if total_edges > 0 else 0.0
    pct_truncated = (truncated_edges / total_edges * 100.0) if total_edges > 0 else 0.0

    top100_stmt = (
        select(ServingGraphScore)
        .order_by(ServingGraphScore.operational_priority.desc(), ServingGraphScore.score_id.asc())
        .limit(100)
    )
    top100 = cast(list[ServingGraphScore], session.scalars(top100_stmt).all())
    n_top = len(top100)
    strict_clean = sum(1 for s in top100 if s.fuzzy_penalty == 0.0 and s.truncation_penalty == 0.0)
    single_signal = sum(1 for s in top100 if s.signal_registry == "single_signal")
    pct_top100_strict_clean = (strict_clean / n_top * 100.0) if n_top > 0 else 0.0
    pct_top100_single_signal = (single_signal / n_top * 100.0) if n_top > 0 else 0.0

    modules = cast(
        list[ServingModuleAvailability],
        session.scalars(select(ServingModuleAvailability)).all(),
    )
    modules_available = sum(1 for m in modules if m.status == "available" and m.record_count > 0)
    modules_empty = sum(1 for m in modules if m.status == "available" and m.record_count == 0)
    modules_missing = sum(1 for m in modules if m.status != "available")

    return BuildMetricsResponse(
        pct_deterministic_edges=round(pct_deterministic, 2),
        pct_fuzzy_edges=round(pct_fuzzy, 2),
        pct_truncated_edges=round(pct_truncated, 2),
        pct_top100_strict_clean=round(pct_top100_strict_clean, 2),
        pct_top100_single_signal=round(pct_top100_single_signal, 2),
        total_nodes=total_nodes,
        total_edges=total_edges,
        total_scores=total_scores,
        modules_available=modules_available,
        modules_empty=modules_empty,
        modules_missing=modules_missing,
    )
