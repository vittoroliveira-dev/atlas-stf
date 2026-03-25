"""Graph materialization: nodes and edges from existing serving tables."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from ._models_graph import (
    ServingEvidenceBundle,
    ServingGraphEdge,
    ServingGraphNode,
    ServingGraphPathCandidate,
    ServingModuleAvailability,
    ServingReviewQueue,
)
from .models import (
    ServingCase,
    ServingCompoundRisk,
    ServingCorporateConflict,
    ServingCounsel,
    ServingDonationMatch,
    ServingEconomicGroup,
    ServingLawFirmEntity,
    ServingLawyerEntity,
    ServingParty,
    ServingProcessCounsel,
    ServingProcessParty,
    ServingRepresentationEdge,
    ServingSanctionCorporateLink,
    ServingSanctionMatch,
)

logger = logging.getLogger(__name__)
_BATCH = 5000
_J = json.dumps  # shorthand used throughout


def _nid(t: str, pk: str) -> str:
    return f"gn_{hashlib.sha256(f'{t}:{pk}'.encode()).hexdigest()[:16]}"


def _eid(t: str, a: str, b: str) -> str:
    return f"ge_{hashlib.sha256(f'{t}:{a}:{b}'.encode()).hexdigest()[:16]}"


def _node(
    nid: str,
    *,
    nt: str,
    lbl: str | None,
    eid: str,
    tbl: str,
    pk: str,
    ident: str | None = None,
    it: str = "internal_id",
    q: str = "internal",
    now: datetime,
) -> ServingGraphNode:
    return ServingGraphNode(
        node_id=nid,
        node_type=nt,
        canonical_label=lbl,
        entity_id=eid,
        source_table=tbl,
        source_pk=pk,
        entity_identifier=ident or eid,
        entity_identifier_type=it,
        entity_identifier_quality=q,
        active_flag=True,
        first_seen_at=now,
        last_seen_at=now,
        provenance_json=_J({"source_table": tbl}, ensure_ascii=False),
    )


def _edge(
    eid: str,
    *,
    s: str,
    d: str,
    et: str,
    tbl: str,
    pk: str,
    sys: str | None = None,
    cf: float | None = None,
    es: str | None = None,
    ms: str | None = None,
    msc: float | None = None,
    pol: str = "strict_allowed",
    tr: bool = False,
    trr: str | None = None,
    w: float = 1.0,
    pc: float = 1.0,
    ex: dict[str, Any] | None = None,
) -> ServingGraphEdge:
    return ServingGraphEdge(
        edge_id=eid,
        src_node_id=s,
        dst_node_id=d,
        edge_type=et,
        directionality="directed",
        source_system=sys,
        source_table=tbl,
        source_pk=pk,
        confidence_score=cf,
        evidence_strength=es,
        match_strategy=ms,
        match_score=msc,
        traversal_policy=pol,
        truncated_flag=tr,
        truncation_reason=trr,
        weight=w,
        path_cost=pc,
        explanation_json=_J(ex, ensure_ascii=False) if ex else None,
    )


def _build_nodes(session: Session) -> dict[str, ServingGraphNode]:
    """Nodes from lawyers, firms, parties, counsels, cases, and economic groups."""
    n: dict[str, ServingGraphNode] = {}
    now = datetime.now(timezone.utc)

    for r in session.scalars(select(ServingLawyerEntity)):
        k = _nid("counsel", r.lawyer_id)
        if k not in n:
            oab = bool(r.oab_number)
            n[k] = _node(
                k,
                nt="counsel",
                lbl=r.lawyer_name_normalized or r.lawyer_name_raw,
                eid=r.lawyer_id,
                tbl="serving_lawyer_entity",
                pk=r.lawyer_id,
                ident=r.oab_number or r.lawyer_id,
                it="oab" if oab else "internal_id",
                q="validated" if oab else "internal",
                now=now,
            )

    for r in session.scalars(select(ServingLawFirmEntity)):
        k = _nid("law_firm", r.firm_id)
        if k not in n:
            vc = bool(r.cnpj and r.cnpj_valid)
            n[k] = _node(
                k,
                nt="law_firm",
                lbl=r.firm_name_normalized or r.firm_name_raw,
                eid=r.firm_id,
                tbl="serving_law_firm_entity",
                pk=r.firm_id,
                ident=r.cnpj if vc else r.firm_id,
                it="cnpj_validated" if vc else "internal_id",
                q="validated" if vc else "internal",
                now=now,
            )

    for r in session.scalars(select(ServingParty)):
        k = _nid("party", r.party_id)
        if k not in n:
            n[k] = _node(
                k, nt="party", lbl=r.party_name_normalized, eid=r.party_id, tbl="serving_party", pk=r.party_id, now=now
            )

    # Counsels from serving_counsel (skip overlaps with lawyer_entity)
    labels: set[str] = {v.canonical_label for v in n.values() if v.node_type == "counsel" and v.canonical_label}
    for r in session.scalars(select(ServingCounsel)):
        k = _nid("counsel", r.counsel_id)
        if k in n or r.counsel_name_normalized in labels:
            continue
        labels.add(r.counsel_name_normalized)
        n[k] = _node(
            k,
            nt="counsel",
            lbl=r.counsel_name_normalized,
            eid=r.counsel_id,
            tbl="serving_counsel",
            pk=r.counsel_id,
            now=now,
        )

    # Cases (distinct by process_id)
    pids: set[str] = set()
    for pid, pnum in session.execute(select(ServingCase.process_id, ServingCase.process_number).distinct()):
        if pid in pids:
            continue
        pids.add(pid)
        k = _nid("case", pid)
        if k not in n:
            n[k] = _node(
                k,
                nt="case",
                lbl=pnum or pid,
                eid=pid,
                tbl="serving_case",
                pk=pid,
                it="process_number",
                q="deterministic",
                now=now,
            )

    # Economic groups (D2 nodes folded here to avoid extra function)
    for r in session.scalars(select(ServingEconomicGroup)):
        k = _nid("economic_group", r.group_id)
        if k in n:
            continue
        lbl = r.group_id
        if r.razoes_sociais_json:
            try:
                ns = json.loads(r.razoes_sociais_json)
                if isinstance(ns, list) and ns:
                    lbl = ns[0]
            except json.JSONDecodeError, IndexError:
                pass
        n[k] = _node(
            k, nt="economic_group", lbl=lbl, eid=r.group_id, tbl="serving_economic_group", pk=r.group_id, now=now
        )

    logger.info("Graph nodes: %d total", len(n))
    return n


def _build_edges(session: Session) -> list[ServingGraphEdge]:
    """Build all graph edges: representation, corporate, and risk."""
    e: list[ServingGraphEdge] = []

    # D1: Representation edges
    for r in session.scalars(select(ServingRepresentationEdge)):
        kind = r.representative_kind or "counsel"
        if kind in ("lawyer", "counsel"):
            src, et = _nid("counsel", r.lawyer_id or r.representative_entity_id), "counsel_represents_party"
        else:
            src, et = _nid("law_firm", r.firm_id or r.representative_entity_id), "firm_represents_party"
        if not r.party_id:
            continue
        e.append(
            _edge(
                _eid(et, r.edge_id, r.party_id),
                s=src,
                d=_nid("party", r.party_id),
                et=et,
                tbl="serving_representation_edge",
                pk=r.edge_id,
                sys="representation",
                cf=r.confidence,
                es="deterministic",
            )
        )

    for r in session.scalars(select(ServingProcessCounsel)):
        e.append(
            _edge(
                _eid("case_has_counsel", r.process_id, r.counsel_id),
                s=_nid("case", r.process_id),
                d=_nid("counsel", r.counsel_id),
                et="case_has_counsel",
                tbl="serving_process_counsel",
                pk=r.link_id,
                sys="curated",
                es="deterministic",
            )
        )

    for r in session.scalars(select(ServingProcessParty)):
        e.append(
            _edge(
                _eid("case_has_party", r.process_id, r.party_id),
                s=_nid("case", r.process_id),
                d=_nid("party", r.party_id),
                et="case_has_party",
                tbl="serving_process_party",
                pk=r.link_id,
                sys="curated",
                es="deterministic",
            )
        )

    logger.info("Graph D1 edges: %d representation", len(e))
    mark = len(e)

    # D2: Corporate edges
    for r in session.scalars(select(ServingCorporateConflict)):
        ind = r.link_degree > 1
        e.append(
            _edge(
                _eid("minister_linked_to_company", r.conflict_id, r.company_cnpj_basico),
                s=_nid("case", r.linked_entity_id),
                d=_nid("economic_group", r.economic_group_id or r.company_cnpj_basico),
                et="minister_linked_to_company",
                tbl="serving_corporate_conflict",
                pk=r.conflict_id,
                sys="corporate_network",
                cf=r.risk_score,
                es="deterministic" if r.linked_entity_type == "party" else "statistical",
                pol="broad_only" if ind else "strict_allowed",
                tr=ind,
                w=r.risk_score or 1.0,
                pc=float(r.link_degree),
                ex={"minister": r.minister_name, "company": r.company_name, "degree": r.link_degree},
            )
        )

    for r in session.scalars(select(ServingSanctionCorporateLink)):
        deg, tr = r.link_degree or 2, (r.link_degree or 2) > 2
        e.append(
            _edge(
                _eid("sanction_links_corporate_path", r.link_id, r.stf_entity_id),
                s=_nid("party", r.stf_entity_id),
                d=_nid("economic_group", r.economic_group_id or r.bridge_company_cnpj_basico),
                et="sanction_links_corporate_path",
                tbl="serving_sanction_corporate_link",
                pk=r.link_id,
                sys="sanction_corporate_link",
                cf=r.risk_score,
                es="truncated" if tr else "statistical",
                ms=r.stf_match_strategy,
                msc=r.stf_match_score,
                pol="broad_only",
                tr=tr,
                trr=f"link_degree={deg}" if tr else None,
                w=r.risk_score or 1.0,
                pc=float(deg),
            )
        )

    logger.info("Graph D2 edges: %d corporate", len(e) - mark)
    mark = len(e)

    # D3: Risk signal edges
    for r in session.scalars(select(ServingSanctionMatch)):
        st = (r.match_strategy or "").lower()
        det = "tax_id" in st or "cpf" in st or "cnpj" in st
        e.append(
            _edge(
                _eid("sanction_hits_entity", r.match_id, r.party_id),
                s=_nid("sanction", r.match_id),
                d=_nid("party", r.party_id),
                et="sanction_hits_entity",
                tbl="serving_sanction_match",
                pk=r.match_id,
                sys=r.sanction_source,
                cf=r.match_score,
                es="deterministic" if det else "statistical",
                ms=r.match_strategy,
                msc=r.match_score,
            )
        )

    for r in session.scalars(select(ServingDonationMatch)):
        doc = bool(r.donor_cpf_cnpj and r.donor_cpf_cnpj.strip())
        e.append(
            _edge(
                _eid("donation_associated_with_entity", r.match_id, r.party_id),
                s=_nid("donation", r.match_id),
                d=_nid("party", r.party_id),
                et="donation_associated_with_entity",
                tbl="serving_donation_match",
                pk=r.match_id,
                sys="tse",
                cf=r.match_score,
                es="deterministic" if doc else "statistical",
                ms=r.match_strategy,
                msc=r.match_score,
                w=r.total_donated_brl or 1.0,
            )
        )

    for r in session.scalars(select(ServingCompoundRisk)):
        wt = float(r.signal_count) if r.signal_count else 1.0
        e.append(
            _edge(
                _eid("case_has_compound_signal", r.pair_id, r.entity_id),
                s=_nid("case", r.minister_name),
                d=_nid("party", r.entity_id),
                et="case_has_compound_signal",
                tbl="serving_compound_risk",
                pk=r.pair_id,
                sys="compound_risk",
                cf=r.max_alert_score,
                es="composite",
                pol="broad_only",
                w=wt,
                pc=1.0 / wt if wt > 0 else 1.0,
                ex={"minister": r.minister_name, "entity": r.entity_name, "signal_count": r.signal_count},
            )
        )

    logger.info("Graph D3 edges: %d risk", len(e) - mark)
    return e


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


def _pid(a: str, b: str) -> str:
    return f"gp_{hashlib.sha256(f'path:{a}:{b}'.encode()).hexdigest()[:16]}"


def _bid(entity_id: str, bundle_type: str) -> str:
    return f"eb_{hashlib.sha256(f'bundle:{entity_id}:{bundle_type}'.encode()).hexdigest()[:16]}"


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
