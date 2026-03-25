from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .models import Base

__all__ = [
    "ServingGraphNode",
    "ServingGraphEdge",
    "ServingGraphPathCandidate",
    "ServingGraphScore",
    "ServingEvidenceBundle",
    "ServingReviewQueue",
    "ServingModuleAvailability",
]


# ---------------------------------------------------------------------------
# Graph: nodes and edges
# ---------------------------------------------------------------------------


class ServingGraphNode(Base):
    __tablename__ = "serving_graph_node"

    node_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    node_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    canonical_label: Mapped[str | None] = mapped_column(String(512))
    entity_id: Mapped[str | None] = mapped_column(String(64), index=True)
    source_table: Mapped[str] = mapped_column(String(128), nullable=False)
    source_pk: Mapped[str] = mapped_column(String(64), nullable=False)
    provenance_json: Mapped[str | None] = mapped_column(Text())
    entity_identifier: Mapped[str | None] = mapped_column(String(256))
    entity_identifier_type: Mapped[str | None] = mapped_column(String(64))
    entity_identifier_quality: Mapped[str | None] = mapped_column(String(32))
    active_flag: Mapped[bool] = mapped_column(Boolean, default=True)
    first_seen_at: Mapped[datetime | None] = mapped_column(DateTime)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime)


Index(
    "ix_serving_graph_node_type_entity",
    ServingGraphNode.node_type,
    ServingGraphNode.entity_id,
)


class ServingGraphEdge(Base):
    __tablename__ = "serving_graph_edge"

    edge_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    src_node_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    dst_node_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    edge_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    directionality: Mapped[str] = mapped_column(String(16), default="directed")
    source_system: Mapped[str | None] = mapped_column(String(64))
    source_table: Mapped[str | None] = mapped_column(String(128))
    source_pk: Mapped[str | None] = mapped_column(String(64))
    source_record_fingerprint: Mapped[str | None] = mapped_column(String(64))
    confidence_score: Mapped[float | None] = mapped_column(Float)
    evidence_strength: Mapped[str | None] = mapped_column(String(32))
    match_strategy: Mapped[str | None] = mapped_column(String(64))
    match_score: Mapped[float | None] = mapped_column(Float)
    traversal_policy: Mapped[str | None] = mapped_column(String(32))
    truncated_flag: Mapped[bool] = mapped_column(Boolean, default=False)
    truncation_reason: Mapped[str | None] = mapped_column(Text())
    manual_review_required: Mapped[bool] = mapped_column(Boolean, default=False)
    weight: Mapped[float] = mapped_column(Float, default=1.0)
    path_cost: Mapped[float] = mapped_column(Float, default=1.0)
    occurred_at: Mapped[datetime | None] = mapped_column(DateTime)
    interval_start: Mapped[datetime | None] = mapped_column(DateTime)
    interval_end: Mapped[datetime | None] = mapped_column(DateTime)
    explanation_json: Mapped[str | None] = mapped_column(Text())


Index(
    "ix_serving_graph_edge_type",
    ServingGraphEdge.edge_type,
)

Index(
    "ix_serving_graph_edge_traversal",
    ServingGraphEdge.traversal_policy,
)


# ---------------------------------------------------------------------------
# Graph: path candidates
# ---------------------------------------------------------------------------


class ServingGraphPathCandidate(Base):
    __tablename__ = "serving_graph_path_candidate"

    path_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    start_node_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    end_node_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    path_length: Mapped[int] = mapped_column(Integer, nullable=False)
    path_edges_json: Mapped[str | None] = mapped_column(Text())
    total_cost: Mapped[float | None] = mapped_column(Float)
    min_confidence: Mapped[float | None] = mapped_column(Float)
    min_evidence_strength: Mapped[str | None] = mapped_column(String(32))
    traversal_mode: Mapped[str | None] = mapped_column(String(16))
    has_truncated_edge: Mapped[bool] = mapped_column(Boolean, default=False)
    has_fuzzy_edge: Mapped[bool] = mapped_column(Boolean, default=False)
    explanation_json: Mapped[str | None] = mapped_column(Text())
    generated_at: Mapped[datetime | None] = mapped_column(DateTime)


# ---------------------------------------------------------------------------
# Graph: decomposable scores
# ---------------------------------------------------------------------------


class ServingGraphScore(Base):
    """Decomposable score per entity or path — enables transparent ranking."""

    __tablename__ = "serving_graph_score"

    score_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    entity_id: Mapped[str | None] = mapped_column(String(64), index=True)
    path_id: Mapped[str | None] = mapped_column(String(64), index=True)
    bundle_id: Mapped[str | None] = mapped_column(String(64))

    # Score components
    documentary_score: Mapped[float] = mapped_column(Float, default=0.0)
    statistical_score: Mapped[float] = mapped_column(Float, default=0.0)
    network_score: Mapped[float] = mapped_column(Float, default=0.0)
    temporal_score: Mapped[float] = mapped_column(Float, default=0.0)

    # Penalties (subtracted from raw)
    fuzzy_penalty: Mapped[float] = mapped_column(Float, default=0.0)
    truncation_penalty: Mapped[float] = mapped_column(Float, default=0.0)
    singleton_penalty: Mapped[float] = mapped_column(Float, default=0.0)
    missing_identifier_penalty: Mapped[float] = mapped_column(Float, default=0.0)

    # Aggregates
    raw_score: Mapped[float] = mapped_column(Float, default=0.0, index=True)
    calibrated_score: Mapped[float] = mapped_column(Float, default=0.0, index=True)
    operational_priority: Mapped[float] = mapped_column(Float, default=0.0, index=True)

    # Classification
    traversal_mode: Mapped[str | None] = mapped_column(String(16))  # strict | broad
    signal_registry: Mapped[str | None] = mapped_column(String(32))  # single_signal | multi_signal
    explanation_json: Mapped[str | None] = mapped_column(Text())
    generated_at: Mapped[datetime | None] = mapped_column(DateTime)


# ---------------------------------------------------------------------------
# Evidence bundles
# ---------------------------------------------------------------------------


class ServingEvidenceBundle(Base):
    __tablename__ = "serving_evidence_bundle"

    bundle_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    path_id: Mapped[str | None] = mapped_column(String(64), index=True)
    entity_id: Mapped[str | None] = mapped_column(String(64), index=True)
    bundle_type: Mapped[str | None] = mapped_column(String(64))
    signal_count: Mapped[int] = mapped_column(Integer, default=0)
    signal_types_json: Mapped[str | None] = mapped_column(Text())
    summary_text: Mapped[str | None] = mapped_column(Text())
    evidence_json: Mapped[str | None] = mapped_column(Text())
    generated_at: Mapped[datetime | None] = mapped_column(DateTime)


# ---------------------------------------------------------------------------
# Review queue
# ---------------------------------------------------------------------------


class ServingReviewQueue(Base):
    __tablename__ = "serving_review_queue"

    item_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    entity_id: Mapped[str | None] = mapped_column(String(64), index=True)
    path_id: Mapped[str | None] = mapped_column(String(64))
    bundle_id: Mapped[str | None] = mapped_column(String(64))
    priority_score: Mapped[float] = mapped_column(Float, default=0.0, index=True)
    priority_tier: Mapped[str | None] = mapped_column(String(16))
    review_reason: Mapped[str | None] = mapped_column(Text())
    status: Mapped[str] = mapped_column(String(32), default="pending", index=True)
    assigned_to: Mapped[str | None] = mapped_column(String(128))
    created_at: Mapped[datetime | None] = mapped_column(DateTime)
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime)
    review_notes: Mapped[str | None] = mapped_column(Text())


# ---------------------------------------------------------------------------
# Module availability
# ---------------------------------------------------------------------------


class ServingModuleAvailability(Base):
    __tablename__ = "serving_module_availability"

    module_name: Mapped[str] = mapped_column(String(128), primary_key=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    record_count: Mapped[int] = mapped_column(Integer, default=0)
    source_file: Mapped[str | None] = mapped_column(String(256))
    status_reason: Mapped[str | None] = mapped_column(Text())
    checked_at: Mapped[datetime | None] = mapped_column(DateTime)
