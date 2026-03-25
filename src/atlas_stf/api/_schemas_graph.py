"""Pydantic schemas for graph, investigation, and review endpoints."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class GraphNodeItem(BaseModel):
    node_id: str
    node_type: str
    canonical_label: str | None = None
    entity_id: str | None = None
    entity_identifier: str | None = None
    entity_identifier_type: str | None = None
    entity_identifier_quality: str | None = None
    active_flag: bool = True


class GraphEdgeItem(BaseModel):
    edge_id: str
    src_node_id: str
    dst_node_id: str
    edge_type: str
    confidence_score: float | None = None
    evidence_strength: str | None = None
    match_strategy: str | None = None
    match_score: float | None = None
    traversal_policy: str | None = None
    truncated_flag: bool = False
    weight: float = 1.0
    explanation: dict | None = None  # parsed from explanation_json


class GraphScoreItem(BaseModel):
    score_id: str
    entity_id: str | None = None
    traversal_mode: str | None = None
    signal_registry: str | None = None
    documentary_score: float = 0.0
    statistical_score: float = 0.0
    network_score: float = 0.0
    temporal_score: float = 0.0
    fuzzy_penalty: float = 0.0
    truncation_penalty: float = 0.0
    singleton_penalty: float = 0.0
    missing_identifier_penalty: float = 0.0
    raw_score: float = 0.0
    calibrated_score: float = 0.0
    operational_priority: float = 0.0
    explanation: dict | None = None


class GraphNeighborResponse(BaseModel):
    center_node: GraphNodeItem
    neighbors: list[GraphNodeItem]
    edges: list[GraphEdgeItem]


class GraphPathItem(BaseModel):
    path_id: str
    start_node_id: str
    end_node_id: str
    path_length: int
    total_cost: float | None = None
    min_confidence: float | None = None
    min_evidence_strength: str | None = None
    traversal_mode: str | None = None
    has_truncated_edge: bool = False
    has_fuzzy_edge: bool = False
    edges: list[str] = []  # edge_ids


class EvidenceBundleItem(BaseModel):
    bundle_id: str
    entity_id: str | None = None
    bundle_type: str | None = None
    signal_count: int = 0
    signal_types: list[str] = []
    summary_text: str | None = None
    evidence: list[dict] = []


class ReviewQueueItem(BaseModel):
    item_id: str
    entity_id: str | None = None
    path_id: str | None = None
    bundle_id: str | None = None
    priority_score: float = 0.0
    priority_tier: str | None = None
    review_reason: str | None = None
    status: str = "pending"
    queue_type: str | None = None  # calibration | investigation


class InvestigationSummary(BaseModel):
    entity_id: str
    entity_label: str | None = None
    node_type: str | None = None
    score: GraphScoreItem | None = None
    bundle: EvidenceBundleItem | None = None
    edge_count: int = 0
    signal_count: int = 0


class ReviewDecisionRequest(BaseModel):
    item_id: str
    status: Literal["confirmed_relevant", "false_positive", "needs_more_data", "deferred"]
    notes: str | None = None


class BuildMetricsResponse(BaseModel):
    pct_deterministic_edges: float
    pct_fuzzy_edges: float
    pct_truncated_edges: float
    pct_top100_strict_clean: float
    pct_top100_single_signal: float
    total_nodes: int
    total_edges: int
    total_scores: int
    modules_available: int
    modules_empty: int
    modules_missing: int


# Paginated responses


class PaginatedNodesResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[GraphNodeItem]


class PaginatedEdgesResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[GraphEdgeItem]


class PaginatedScoresResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[GraphScoreItem]


class PaginatedPathsResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[GraphPathItem]


class PaginatedReviewResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[ReviewQueueItem]


class PaginatedInvestigationsResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[InvestigationSummary]
