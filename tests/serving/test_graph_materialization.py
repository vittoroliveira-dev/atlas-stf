"""Tests for graph materialization (Camada D)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from atlas_stf.serving._builder_graph import _nid, materialize_graph
from atlas_stf.serving._models_graph import (
    ServingEvidenceBundle,
    ServingGraphEdge,
    ServingGraphNode,
    ServingGraphPathCandidate,
    ServingModuleAvailability,
    ServingReviewQueue,
)
from atlas_stf.serving.models import (
    Base,
    ServingCase,
    ServingCounsel,
    ServingParty,
    ServingProcessParty,
)

_VALID_NODE_TYPES = {"case", "party", "counsel", "law_firm", "economic_group", "sanction", "donation"}
_VALID_EDGE_TYPES = {
    "case_has_party",
    "case_has_counsel",
    "counsel_represents_party",
    "firm_represents_party",
    "minister_linked_to_company",
    "sanction_links_corporate_path",
    "sanction_hits_entity",
    "donation_associated_with_entity",
    "case_has_compound_signal",
}


@pytest.fixture
def graph_db(tmp_path):
    """In-memory SQLite with all tables created and minimal seed data."""
    db_path = tmp_path / "test_graph.db"
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)

    now = datetime.now(timezone.utc)
    with Session(engine) as session:
        with session.begin():
            session.add(
                ServingCase(
                    decision_event_id="de_test1",
                    process_id="proc_test1",
                    process_class="ADI",
                    thematic_key="direito_constitucional",
                    current_rapporteur="MINISTRO TESTE",
                    decision_date=now.date(),
                    period="2024-01",
                )
            )
            session.add(
                ServingParty(
                    party_id="party_test1",
                    party_name_raw="EMPRESA TESTE LTDA",
                    party_name_normalized="EMPRESA TESTE LTDA",
                )
            )
            session.add(
                ServingCounsel(
                    counsel_id="csl_test1",
                    counsel_name_raw="ADVOGADO TESTE",
                    counsel_name_normalized="ADVOGADO TESTE",
                )
            )

    yield engine
    engine.dispose()


def test_graph_nodes_created(graph_db):
    """materialize_graph cria nós para case, party e counsel do seed."""
    with Session(graph_db) as session:
        with session.begin():
            materialize_graph(session)

    with Session(graph_db) as session:
        nodes = list(session.scalars(select(ServingGraphNode)))

    assert len(nodes) >= 3
    types = {n.node_type for n in nodes}
    assert "case" in types
    assert "party" in types
    assert "counsel" in types


def test_graph_node_ids_deterministic(graph_db):
    """IDs de nós são determinísticos: duas execuções geram os mesmos node_ids."""
    with Session(graph_db) as session:
        with session.begin():
            materialize_graph(session)

    with Session(graph_db) as session:
        first_ids = {r.node_id for r in session.scalars(select(ServingGraphNode))}

    # Limpa e roda novamente
    with Session(graph_db) as session:
        with session.begin():
            session.execute(ServingGraphNode.__table__.delete())
            session.execute(ServingGraphEdge.__table__.delete())
            session.execute(ServingGraphPathCandidate.__table__.delete())
            session.execute(ServingEvidenceBundle.__table__.delete())
            session.execute(ServingReviewQueue.__table__.delete())
            session.execute(ServingModuleAvailability.__table__.delete())

    with Session(graph_db) as session:
        with session.begin():
            materialize_graph(session)

    with Session(graph_db) as session:
        second_ids = {r.node_id for r in session.scalars(select(ServingGraphNode))}

    assert first_ids == second_ids


def test_graph_edges_from_process_links(graph_db):
    """Aresta case_has_party criada a partir de ServingProcessParty."""
    with Session(graph_db) as session:
        with session.begin():
            session.add(
                ServingProcessParty(
                    link_id="pp_test1",
                    process_id="proc_test1",
                    party_id="party_test1",
                    role_in_case="REQTE.(S)",
                    source_id="juris",
                )
            )

    with Session(graph_db) as session:
        with session.begin():
            materialize_graph(session)

    with Session(graph_db) as session:
        edges = list(session.scalars(select(ServingGraphEdge).where(ServingGraphEdge.edge_type == "case_has_party")))

    assert len(edges) >= 1
    edge = edges[0]
    assert edge.src_node_id == _nid("case", "proc_test1")
    assert edge.dst_node_id == _nid("party", "party_test1")


def test_module_availability_populated(graph_db):
    """ServingModuleAvailability tem entradas após materialização."""
    with Session(graph_db) as session:
        with session.begin():
            materialize_graph(session)

    with Session(graph_db) as session:
        mods = list(session.scalars(select(ServingModuleAvailability)))

    assert len(mods) > 0
    statuses = {m.status for m in mods}
    assert statuses <= {"available", "missing_source", "empty_after_build", "degraded", "disabled"}


def test_empty_db_no_crash(tmp_path):
    """Banco vazio não causa erro; materialize retorna 0 nós e 0 arestas."""
    engine = create_engine(f"sqlite:///{tmp_path / 'empty.db'}")
    Base.metadata.create_all(engine)
    try:
        with Session(engine) as session:
            with session.begin():
                counts = materialize_graph(session)
        assert counts["nodes"] == 0
        assert counts["edges"] == 0
        assert counts["modules"] > 0  # _MODULE_CHECKS sempre gera entradas
    finally:
        engine.dispose()


def test_node_types_are_valid(graph_db):
    """Todos os node_type gerados pertencem ao conjunto esperado."""
    with Session(graph_db) as session:
        with session.begin():
            materialize_graph(session)

    with Session(graph_db) as session:
        types = {r.node_type for r in session.scalars(select(ServingGraphNode))}

    assert types <= _VALID_NODE_TYPES


def test_edge_types_are_valid(graph_db):
    """Todos os edge_type gerados pertencem ao conjunto esperado."""
    with Session(graph_db) as session:
        with session.begin():
            session.add(
                ServingProcessParty(
                    link_id="pp_edge_check",
                    process_id="proc_test1",
                    party_id="party_test1",
                    role_in_case="REQTE.(S)",
                    source_id="juris",
                )
            )

    with Session(graph_db) as session:
        with session.begin():
            materialize_graph(session)

    with Session(graph_db) as session:
        types = {r.edge_type for r in session.scalars(select(ServingGraphEdge))}

    assert types <= _VALID_EDGE_TYPES


def test_graph_counts_returned(graph_db):
    """materialize_graph retorna dict com todas as chaves de contagem."""
    with Session(graph_db) as session:
        with session.begin():
            counts = materialize_graph(session)

    assert isinstance(counts, dict)
    for key in ("nodes", "edges", "paths", "bundles", "review_queue", "modules"):
        assert key in counts, f"Missing key: {key}"
        assert isinstance(counts[key], int)
    assert counts["nodes"] >= 3  # case + party + counsel do seed


def test_path_candidates_created(graph_db):
    """Risk edges generate path candidates."""
    from atlas_stf.serving.models import ServingSanctionMatch

    with Session(graph_db) as session:
        with session.begin():
            session.add(
                ServingSanctionMatch(
                    match_id="sm_test1",
                    party_id="party_test1",
                    party_name_normalized="EMPRESA TESTE LTDA",
                    sanction_source="ceis",
                    sanction_id="s001",
                    match_score=0.95,
                    match_confidence="high",
                    red_flag=True,
                )
            )

    with Session(graph_db) as session:
        with session.begin():
            counts = materialize_graph(session)

    assert counts["paths"] >= 1
    with Session(graph_db) as session:
        paths = list(session.scalars(select(ServingGraphPathCandidate)))
    assert len(paths) >= 1
    assert all(p.path_length == 1 for p in paths)


def test_evidence_bundles_created(graph_db):
    """Entities with risk edges get evidence bundles."""
    from atlas_stf.serving.models import ServingSanctionMatch

    with Session(graph_db) as session:
        with session.begin():
            session.add(
                ServingSanctionMatch(
                    match_id="sm_bundle1",
                    party_id="party_test1",
                    party_name_normalized="EMPRESA TESTE LTDA",
                    sanction_source="ceis",
                    sanction_id="s002",
                    match_score=0.9,
                    match_confidence="high",
                    red_flag=True,
                )
            )

    with Session(graph_db) as session:
        with session.begin():
            counts = materialize_graph(session)

    assert counts["bundles"] >= 1
    with Session(graph_db) as session:
        bundles = list(session.scalars(select(ServingEvidenceBundle)))
    assert len(bundles) >= 1
    assert all(b.bundle_type == "risk_convergence" for b in bundles)


def test_review_queue_populated_for_multi_signal(graph_db):
    """Entities with ≥2 risk signals get review queue items."""
    from atlas_stf.serving.models import ServingCompoundRisk, ServingSanctionMatch

    with Session(graph_db) as session:
        with session.begin():
            session.add(
                ServingSanctionMatch(
                    match_id="sm_rq1",
                    party_id="party_test1",
                    party_name_normalized="EMPRESA TESTE LTDA",
                    sanction_source="ceis",
                    sanction_id="s003",
                    match_score=0.9,
                    match_confidence="high",
                    red_flag=True,
                )
            )
            session.add(
                ServingCompoundRisk(
                    pair_id="cr_rq1",
                    minister_name="MINISTRO TESTE",
                    entity_type="party",
                    entity_id="party_test1",
                    entity_name="EMPRESA TESTE LTDA",
                    signal_count=3,
                    red_flag=True,
                )
            )

    with Session(graph_db) as session:
        with session.begin():
            counts = materialize_graph(session)

    assert counts["review_queue"] >= 1
    with Session(graph_db) as session:
        items = list(session.scalars(select(ServingReviewQueue)))
    assert len(items) >= 1
    assert all(i.status == "pending" for i in items)


def test_module_availability_status_granularity(graph_db):
    """Module statuses use granular classifications."""
    with Session(graph_db) as session:
        with session.begin():
            materialize_graph(session)

    with Session(graph_db) as session:
        mods = list(session.scalars(select(ServingModuleAvailability)))

    # At least some modules should be missing_source (no JSONL files in test)
    statuses = {m.status for m in mods}
    assert "missing_source" in statuses, f"Expected missing_source in {statuses}"
    # All status_reason fields should be populated
    assert all(m.status_reason for m in mods)
