"""Camada E scoring: decomposable scores, strict/broad traversal, ranking.

Reads graph nodes, edges, evidence bundles and review queue items to produce
``ServingGraphScore`` records with transparent, decomposable scoring.

Strict mode: only deterministic evidence, no truncated/fuzzy edges.
Broad mode: includes statistical, fuzzy, and inferred evidence with penalties.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from ._models_graph import (
    ServingEvidenceBundle,
    ServingGraphEdge,
    ServingGraphScore,
    ServingReviewQueue,
)

logger = logging.getLogger(__name__)
_J = json.dumps
_BATCH = 5000

# ---------------------------------------------------------------------------
# Score weights — heuristic defaults, not empirically calibrated
# ---------------------------------------------------------------------------
#
# Rationale: weights reflect epistemological confidence in the evidence type.
# Deterministic evidence (tax_id match, PK join) is the most reliable signal;
# fuzzy/inferred evidence is the least.  The 3:2:1.5:1:0.5 graduation is a
# first-pass heuristic that preserves this ordering without empirical tuning.
#
# Calibration plan (when ground truth is available):
#   1. Collect manual review decisions (investigate / dismiss) for top-100
#      ranked entities from the /review/queue endpoint.
#   2. Compute NDCG@k and Precision@k for the current weight vector.
#   3. Grid-search alternative weight vectors holding structural monotonicity
#      (deterministic >= statistical >= composite >= fuzzy >= truncated/inferred).
#   4. Accept new weights only if NDCG@k improves >= 5% without degrading
#      Precision@10.
#
# Until calibration is performed, these values should be treated as
# reasonable defaults, not optimized parameters.

_EVIDENCE_WEIGHTS: dict[str, float] = {
    "deterministic": 3.0,  # highest: matched by validated tax ID or primary key
    "statistical": 2.0,  # chi-square / rate anomaly with sufficient sample
    "composite": 1.5,  # converging multi-signal (compound risk)
    "fuzzy": 1.0,  # name similarity without deterministic anchor
    "truncated": 0.5,  # economic group edge truncated at membership limit
    "inferred": 0.5,  # cross-entity propagation (counsel via party's signal)
}

# Penalties — subtracted from raw score to discount unreliable evidence paths.
_PENALTY_FUZZY = 0.3  # per fuzzy/inferred edge: discounts imprecise matching
_PENALTY_TRUNCATION = 0.5  # per truncated edge: membership list was capped
_PENALTY_SINGLETON = 1.0  # single-signal entity: no convergence of evidence
_PENALTY_MISSING_ID = 0.5  # no deterministic identifier: entity may be mislinked

# Single source of truth for which edge types enter the scoring pipeline.
# _edge_score_contribution() and compute_graph_scores() both derive from
# this set.  Adding a new risk edge type here is sufficient to include it
# in scoring — no second registration point.
RISK_EDGE_TYPES: frozenset[str] = frozenset({
    "sanction_hits_entity",
    "donation_associated_with_entity",
    "case_has_compound_signal",
    "sanction_links_corporate_path",
    "minister_linked_to_company",
    "entity_in_economic_group",
})

# Subset of RISK_EDGE_TYPES that contribute to the network score category
# (indirect linkage, weighted at 0.5× base).
_NETWORK_EDGE_TYPES: frozenset[str] = frozenset({
    "sanction_links_corporate_path",
    "minister_linked_to_company",
    "entity_in_economic_group",
})

# NOTE: temporal_score is structurally present but NOT populated in this
# version.  It will be wired to agenda_exposure proximity and decision
# velocity signals once those edges carry occurred_at timestamps.
# Until then, temporal_score = 0.0 for all entities.


def _sid(entity_id: str, mode: str) -> str:
    return f"gs_{hashlib.sha256(f'score:{entity_id}:{mode}'.encode()).hexdigest()[:16]}"


# ---------------------------------------------------------------------------
# Edge classification helpers
# ---------------------------------------------------------------------------


def _is_strict_edge(e: ServingGraphEdge) -> bool:
    """Edge qualifies for strict traversal.

    Current implementation uses ``traversal_policy`` set by the graph builder
    (``_builder_graph.py``), which assigns policies based on local heuristics
    per edge type.  Future improvement: derive policy systematically from
    ``join_matrix.json`` governance to avoid divergence between governance
    declarations and runtime execution.
    """
    return (
        e.evidence_strength == "deterministic"
        and e.traversal_policy == "strict_allowed"
        and not e.truncated_flag
    )


def _edge_score_contribution(e: ServingGraphEdge) -> dict[str, float]:
    """Decompose an edge's contribution into score categories."""
    w = _EVIDENCE_WEIGHTS.get(e.evidence_strength or "", 1.0)
    base = (e.confidence_score or 0.5) * w

    documentary = base if e.evidence_strength == "deterministic" else 0.0
    statistical = base if e.evidence_strength in ("statistical", "composite") else 0.0
    # Network contribution: corporate-path edges contribute at half weight
    # because they represent indirect linkage (company → partner → entity),
    # not direct evidence of misconduct.
    network = base * 0.5 if e.edge_type in _NETWORK_EDGE_TYPES else 0.0
    fuzzy_pen = _PENALTY_FUZZY if e.evidence_strength in ("fuzzy", "inferred") else 0.0
    trunc_pen = _PENALTY_TRUNCATION if e.truncated_flag else 0.0

    return {
        "documentary": documentary,
        "statistical": statistical,
        "network": network,
        "fuzzy_penalty": fuzzy_pen,
        "truncation_penalty": trunc_pen,
    }


# ---------------------------------------------------------------------------
# Score computation per entity
# ---------------------------------------------------------------------------


def _compute_entity_score(
    entity_id: str,
    edges: list[ServingGraphEdge],
    bundle: ServingEvidenceBundle | None,
    mode: str,
) -> ServingGraphScore:
    """Compute decomposable score for one entity in one traversal mode."""
    now = datetime.now(timezone.utc)

    # Filter edges by mode
    if mode == "strict":
        valid = [e for e in edges if _is_strict_edge(e)]
    else:
        valid = list(edges)

    # Accumulate score components
    doc = stat = net = temp = 0.0
    fuzz_pen = trunc_pen = 0.0
    strengths: list[str] = []
    edge_details: list[dict[str, Any]] = []

    for e in valid:
        contrib = _edge_score_contribution(e)
        doc += contrib["documentary"]
        stat += contrib["statistical"]
        net += contrib["network"]
        fuzz_pen += contrib["fuzzy_penalty"]
        trunc_pen += contrib["truncation_penalty"]
        if e.evidence_strength:
            strengths.append(e.evidence_strength)
        edge_details.append(
            {
                "edge_id": e.edge_id,
                "type": e.edge_type,
                "strength": e.evidence_strength,
                "confidence": e.confidence_score,
                "truncated": e.truncated_flag,
            }
        )

    # Signal classification
    signal_count = bundle.signal_count if bundle else len(valid)
    is_singleton = signal_count <= 1
    signal_registry = "single_signal" if is_singleton else "multi_signal"

    # Singleton penalty: single-signal entities don't compete with convergence
    sing_pen = _PENALTY_SINGLETON if is_singleton else 0.0

    # Missing identifier penalty
    has_deterministic = any(s == "deterministic" for s in strengths)
    miss_id_pen = _PENALTY_MISSING_ID if not has_deterministic else 0.0

    # Raw score = sum of positives
    raw = doc + stat + net + temp

    # Calibrated = raw - penalties
    calibrated = max(raw - fuzz_pen - trunc_pen - sing_pen - miss_id_pen, 0.0)

    # Operational priority: calibrated × signal count (capped at 10).
    # Entities with more converging signals rank higher.  The cap at 10
    # prevents extreme outliers from dominating the review queue.
    boost = min(signal_count, 10)
    operational = calibrated * boost if calibrated > 0 else 0.0

    explanation = {
        "mode": mode,
        "edges_considered": len(valid),
        "edges_total": len(edges),
        "signal_count": signal_count,
        "signal_registry": signal_registry,
        "score_components": {
            "documentary": round(doc, 3),
            "statistical": round(stat, 3),
            "network": round(net, 3),
            "temporal": round(temp, 3),
        },
        "penalties": {
            "fuzzy": round(fuzz_pen, 3),
            "truncation": round(trunc_pen, 3),
            "singleton": round(sing_pen, 3),
            "missing_identifier": round(miss_id_pen, 3),
        },
        "raw_score": round(raw, 3),
        "calibrated_score": round(calibrated, 3),
        "operational_priority": round(operational, 3),
        "edges": edge_details[:20],  # cap for readability
    }

    return ServingGraphScore(
        score_id=_sid(entity_id, mode),
        entity_id=entity_id,
        path_id=None,
        bundle_id=bundle.bundle_id if bundle else None,
        documentary_score=round(doc, 4),
        statistical_score=round(stat, 4),
        network_score=round(net, 4),
        temporal_score=round(temp, 4),
        fuzzy_penalty=round(fuzz_pen, 4),
        truncation_penalty=round(trunc_pen, 4),
        singleton_penalty=round(sing_pen, 4),
        missing_identifier_penalty=round(miss_id_pen, 4),
        raw_score=round(raw, 4),
        calibrated_score=round(calibrated, 4),
        operational_priority=round(operational, 4),
        traversal_mode=mode,
        signal_registry=signal_registry,
        explanation_json=_J(explanation, ensure_ascii=False),
        generated_at=now,
    )


# ---------------------------------------------------------------------------
# Review queue enrichment
# ---------------------------------------------------------------------------


def _enrich_review_queue(
    session: Session,
    scores: list[ServingGraphScore],
) -> None:
    """Update review queue items with computed scores."""
    score_by_entity: dict[str, ServingGraphScore] = {}
    for s in scores:
        if s.entity_id and s.traversal_mode == "broad":
            score_by_entity[s.entity_id] = s

    items = list(session.scalars(select(ServingReviewQueue)))
    for item in items:
        if item.entity_id and item.entity_id in score_by_entity:
            sc = score_by_entity[item.entity_id]
            item.priority_score = sc.operational_priority
            # Tier thresholds: heuristic, not calibrated.
            # high (>=20): multi-signal with strong evidence — prioritize for review
            # medium (>=5): moderate evidence — review when capacity allows
            # low (<5): weak or singleton — background queue
            tier = "high" if sc.operational_priority >= 20 else "medium" if sc.operational_priority >= 5 else "low"
            item.priority_tier = tier


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def compute_graph_scores(session: Session) -> dict[str, int]:
    """Compute decomposable scores for all entities with risk evidence.

    Produces two scores per entity: strict and broad.
    Updates review queue priorities based on broad scores.
    Returns counts.
    """
    t0 = time.monotonic()

    # Load all risk-related edges
    edges = list(session.scalars(select(ServingGraphEdge)))
    bundles = list(session.scalars(select(ServingEvidenceBundle)))

    # Group edges by dst_node (entity receiving risk signals)
    edges_by_entity: dict[str, list[ServingGraphEdge]] = {}
    for e in edges:
        if e.edge_type in RISK_EDGE_TYPES:
            edges_by_entity.setdefault(e.dst_node_id, []).append(e)

    bundle_by_entity: dict[str, ServingEvidenceBundle] = {}
    for b in bundles:
        if b.entity_id:
            bundle_by_entity[b.entity_id] = b

    # Compute scores for each entity in both modes
    scores: list[ServingGraphScore] = []
    for entity_nid, entity_edges in edges_by_entity.items():
        bundle = bundle_by_entity.get(entity_nid)
        for mode in ("strict", "broad"):
            sc = _compute_entity_score(entity_nid, entity_edges, bundle, mode)
            scores.append(sc)

    # Persist scores
    for i in range(0, len(scores), _BATCH):
        session.add_all(scores[i : i + _BATCH])
    session.flush()

    # Enrich review queue with computed priorities
    _enrich_review_queue(session, scores)
    session.flush()

    strict_count = sum(1 for s in scores if s.traversal_mode == "strict")
    broad_count = sum(1 for s in scores if s.traversal_mode == "broad")
    logger.info(
        "Scoring: %d scores (%d strict, %d broad) for %d entities in %.1fs",
        len(scores),
        strict_count,
        broad_count,
        len(edges_by_entity),
        time.monotonic() - t0,
    )
    return {"scores": len(scores), "strict": strict_count, "broad": broad_count}
