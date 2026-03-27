"""Graph node and edge builders: ID helpers, model constructors, and D1/D2/D3 edge sets."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from ._models_graph import (
    ServingGraphEdge,
    ServingGraphNode,
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


def _pid(a: str, b: str) -> str:
    return f"gp_{hashlib.sha256(f'path:{a}:{b}'.encode()).hexdigest()[:16]}"


def _bid(entity_id: str, bundle_type: str) -> str:
    return f"eb_{hashlib.sha256(f'bundle:{entity_id}:{bundle_type}'.encode()).hexdigest()[:16]}"


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
