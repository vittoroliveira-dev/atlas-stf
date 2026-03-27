"""Graph metadata builders: module availability, path candidates, evidence bundles, review queue, and orchestrator."""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.orm import Session

from ._builder_graph_nodes import (
    _BATCH,
    _J,
    _bid,
    _build_edges,
    _build_nodes,
    _pid,
)
from ._models_graph import (
    ServingEvidenceBundle,
    ServingGraphEdge,
    ServingGraphNode,
    ServingGraphPathCandidate,
    ServingModuleAvailability,
    ServingReviewQueue,
)

logger = logging.getLogger(__name__)

_MODULE_CHECKS: list[tuple[str, str, str | None]] = [
    ("representation_graph", "serving_representation_edge", "representation_edge.jsonl"),
    ("representation_recurrence", "serving_representation_event", "representation_event.jsonl"),
    ("firm_cluster", "serving_counsel_network_cluster", "counsel_network_cluster.jsonl"),
    ("sanction_match", "serving_sanction_match", "sanction_match.jsonl"),
    ("donation_match", "serving_donation_match", "donation_match.jsonl"),
    ("corporate_network", "serving_corporate_conflict", "corporate_network.jsonl"),
    ("economic_groups", "serving_economic_group", "economic_group.jsonl"),
    ("compound_risk", "serving_compound_risk", "compound_risk.jsonl"),
    ("sanction_corporate_link", "serving_sanction_corporate_link", "sanction_corporate_link.jsonl"),
    ("counsel_affinity", "serving_counsel_affinity", "counsel_affinity.jsonl"),
    ("temporal_analysis", "serving_temporal_analysis", "temporal_analysis.jsonl"),
    ("agenda", "agenda_event", "agenda_event.jsonl"),
    ("minister_bios", "serving_minister_bio", None),
]


def _build_module_availability(
    session: Session,
    analytics_dir: Path | None,
    curated_dir: Path | None,
) -> list[ServingModuleAvailability]:
    """Check each module's serving table row count and source file presence."""
    mods: list[ServingModuleAvailability] = []
    now = datetime.now(timezone.utc)
    dirs = [d for d in (analytics_dir, curated_dir) if d]

    for name, tbl, src in _MODULE_CHECKS:
        try:
            cnt = session.execute(text(f"SELECT COUNT(*) FROM {tbl}")).scalar() or 0  # noqa: S608
        except Exception:
            cnt = 0

        fp: str | None = None
        found = False
        if src:
            for d in dirs:
                c = d / src
                if c.exists():
                    fp, found = str(c), True
                    break

        if cnt > 0:
            st, reason = "available", f"{cnt} records loaded"
        elif found and cnt == 0:
            st, reason = "empty_after_build", f"Source exists ({fp}) but 0 records in {tbl}"
        elif src is None:
            st, reason = "missing_source", f"No source file configured for {name}"
        else:
            st, reason = "missing_source", f"Source file {src} not found in {dirs}"

        mods.append(
            ServingModuleAvailability(
                module_name=name,
                status=st,
                record_count=cnt,
                source_file=fp,
                status_reason=reason,
                checked_at=now,
            )
        )

    logger.info("Graph D4: %d modules (%d available)", len(mods), sum(1 for m in mods if m.status == "available"))
    return mods


def _build_path_candidates(
    nodes: dict[str, ServingGraphNode],
    edges: list[ServingGraphEdge],
) -> list[ServingGraphPathCandidate]:
    """Build 1-hop path candidates from risk edges to case nodes.

    Finds edges where src or dst is a risk-related node (sanction, donation,
    compound) and the other end connects to a case node. These are the
    simplest reviewable paths.
    """
    now = datetime.now(timezone.utc)
    risk_edge_types = {
        "sanction_hits_entity",
        "donation_associated_with_entity",
        "case_has_compound_signal",
        "sanction_links_corporate_path",
        "minister_linked_to_company",
    }
    paths: list[ServingGraphPathCandidate] = []
    seen: set[str] = set()

    for e in edges:
        if e.edge_type not in risk_edge_types:
            continue
        pid = _pid(e.src_node_id, e.dst_node_id)
        if pid in seen:
            continue
        seen.add(pid)
        paths.append(
            ServingGraphPathCandidate(
                path_id=pid,
                start_node_id=e.src_node_id,
                end_node_id=e.dst_node_id,
                path_length=1,
                path_edges_json=_J([e.edge_id]),
                total_cost=e.path_cost,
                min_confidence=e.confidence_score,
                min_evidence_strength=e.evidence_strength,
                traversal_mode="strict" if e.traversal_policy == "strict_allowed" else "broad",
                has_truncated_edge=e.truncated_flag,
                has_fuzzy_edge=e.evidence_strength in ("fuzzy", "truncated"),
                explanation_json=e.explanation_json,
                generated_at=now,
            )
        )

    logger.info("Graph paths: %d 1-hop candidates from risk edges", len(paths))
    return paths


def _build_evidence_bundles(
    nodes: dict[str, ServingGraphNode],
    edges: list[ServingGraphEdge],
) -> list[ServingEvidenceBundle]:
    """Bundle risk evidence per entity by aggregating connected risk edges."""
    now = datetime.now(timezone.utc)
    risk_types = {
        "sanction_hits_entity",
        "donation_associated_with_entity",
        "case_has_compound_signal",
        "sanction_links_corporate_path",
        "minister_linked_to_company",
    }
    # Group risk edges by entity (dst_node_id for most risk edges)
    entity_signals: dict[str, list[ServingGraphEdge]] = {}
    for e in edges:
        if e.edge_type not in risk_types:
            continue
        entity_signals.setdefault(e.dst_node_id, []).append(e)

    bundles: list[ServingEvidenceBundle] = []
    for entity_nid, signals in entity_signals.items():
        node = nodes.get(entity_nid)
        entity_id = node.entity_id if node else entity_nid
        types_seen = sorted({s.edge_type for s in signals})
        bundles.append(
            ServingEvidenceBundle(
                bundle_id=_bid(entity_nid, "risk"),
                path_id=None,
                entity_id=entity_id,
                bundle_type="risk_convergence",
                signal_count=len(signals),
                signal_types_json=_J(types_seen),
                summary_text=f"{len(signals)} risk signals ({len(types_seen)} types) for entity {entity_id}",
                evidence_json=_J([
                    {"edge_id": s.edge_id, "type": s.edge_type, "strength": s.evidence_strength}
                    for s in signals[:50]
                ]),
                generated_at=now,
            )
        )

    logger.info("Graph evidence: %d bundles for %d entities", len(bundles), len(entity_signals))
    return bundles


def _build_review_queue(
    bundles: list[ServingEvidenceBundle],
    paths: list[ServingGraphPathCandidate],
) -> list[ServingReviewQueue]:
    """Populate review queue from evidence bundles with multiple signals."""
    now = datetime.now(timezone.utc)
    items: list[ServingReviewQueue] = []

    for b in bundles:
        if b.signal_count < 2:
            continue  # single-signal entities are not interesting for review

        score = float(b.signal_count)
        tier = "high" if b.signal_count >= 5 else "medium" if b.signal_count >= 3 else "low"
        items.append(
            ServingReviewQueue(
                item_id=f"rq_{hashlib.sha256(f'review:{b.bundle_id}'.encode()).hexdigest()[:16]}",
                entity_id=b.entity_id,
                path_id=None,
                bundle_id=b.bundle_id,
                priority_score=score,
                priority_tier=tier,
                review_reason=f"{b.signal_count} converging risk signals ({b.bundle_type})",
                status="pending",
                assigned_to=None,
                created_at=now,
                reviewed_at=None,
                review_notes=None,
            )
        )

    # Also queue paths with truncated or fuzzy edges
    for p in paths:
        if not p.has_truncated_edge and not p.has_fuzzy_edge:
            continue
        reason_parts = []
        if p.has_truncated_edge:
            reason_parts.append("truncated edge in path")
        if p.has_fuzzy_edge:
            reason_parts.append("fuzzy evidence in path")
        items.append(
            ServingReviewQueue(
                item_id=f"rq_{hashlib.sha256(f'review:path:{p.path_id}'.encode()).hexdigest()[:16]}",
                entity_id=None,
                path_id=p.path_id,
                bundle_id=None,
                priority_score=1.0,
                priority_tier="low",
                review_reason="; ".join(reason_parts),
                status="pending",
                assigned_to=None,
                created_at=now,
                reviewed_at=None,
                review_notes=None,
            )
        )

    from_bundles = sum(1 for i in items if i.bundle_id)
    from_paths = sum(1 for i in items if i.path_id and not i.bundle_id)
    logger.info("Graph review queue: %d items (%d from bundles, %d from paths)", len(items), from_bundles, from_paths)
    return items


def materialize_graph(
    session: Session,
    *,
    analytics_dir: Path | None = None,
    curated_dir: Path | None = None,
) -> dict[str, int]:
    """Materialize graph nodes, edges, paths, evidence, review queue, and module availability.

    Returns dict with counts.
    """
    t0 = time.monotonic()
    nodes = _build_nodes(session)
    raw = _build_edges(session)

    seen: set[str] = set()
    edges: list[ServingGraphEdge] = []
    for e in raw:
        if e.edge_id not in seen:
            seen.add(e.edge_id)
            edges.append(e)

    paths = _build_path_candidates(nodes, edges)
    bundles = _build_evidence_bundles(nodes, edges)
    queue = _build_review_queue(bundles, paths)
    mods = _build_module_availability(session, analytics_dir, curated_dir)

    # Flush in batches
    nl = list(nodes.values())
    for batch in (nl, edges, paths, bundles, queue, mods):
        for i in range(0, len(batch), _BATCH):
            session.add_all(batch[i : i + _BATCH])
        session.flush()

    c = {
        "nodes": len(nl),
        "edges": len(edges),
        "paths": len(paths),
        "bundles": len(bundles),
        "review_queue": len(queue),
        "modules": len(mods),
    }
    logger.info(
        "Graph: %d nodes, %d edges, %d paths, %d bundles, %d review items, %d modules in %.1fs",
        *c.values(),
        time.monotonic() - t0,
    )
    return c
