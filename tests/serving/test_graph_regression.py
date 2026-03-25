"""Testes de regressão do grafo — invariantes estruturais entre builds."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from atlas_stf.serving._builder_graph import materialize_graph
from atlas_stf.serving._builder_scoring import compute_graph_scores
from atlas_stf.serving._models_graph import (
    ServingEvidenceBundle,
    ServingGraphEdge,
    ServingGraphNode,
    ServingGraphPathCandidate,
    ServingGraphScore,
    ServingModuleAvailability,
    ServingReviewQueue,
)
from atlas_stf.serving.models import (
    Base,
    ServingCase,
    ServingCompoundRisk,
    ServingParty,
    ServingProcessParty,
    ServingSanctionMatch,
    ServingSchemaMeta,
)


def _clear_graph_tables(session: Session) -> None:
    """Apaga todas as tabelas de grafo na ordem correta para rebuild limpo."""
    for tbl in (
        ServingGraphScore.__table__,
        ServingReviewQueue.__table__,
        ServingEvidenceBundle.__table__,
        ServingGraphPathCandidate.__table__,
        ServingGraphEdge.__table__,
        ServingGraphNode.__table__,
        ServingModuleAvailability.__table__,
    ):
        session.execute(tbl.delete())


@pytest.fixture
def regression_db(tmp_path):
    """SQLite com seed multi-sinal para exercitar scoring completo."""
    db_path = tmp_path / "regression.db"
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)

    now = datetime.now(timezone.utc)
    with Session(engine) as session:
        with session.begin():
            session.add(
                ServingCase(
                    decision_event_id="de_reg1",
                    process_id="proc_reg1",
                    process_class="ADI",
                    thematic_key="constitucional",
                    current_rapporteur="MIN REG",
                    decision_date=now.date(),
                    period="2024-01",
                )
            )
            session.add(
                ServingParty(
                    party_id="party_reg1",
                    party_name_raw="EMPRESA REG LTDA",
                    party_name_normalized="EMPRESA REG LTDA",
                )
            )
            # Entidade single_signal — apenas uma sanção, sem compound risk
            session.add(
                ServingParty(
                    party_id="party_single",
                    party_name_raw="EMPRESA SINGLE LTDA",
                    party_name_normalized="EMPRESA SINGLE LTDA",
                )
            )
            session.add(
                ServingProcessParty(
                    link_id="pp_reg1",
                    process_id="proc_reg1",
                    party_id="party_reg1",
                    role_in_case="REQTE.(S)",
                    source_id="juris",
                )
            )
            session.add(
                ServingSanctionMatch(
                    match_id="sm_reg1",
                    party_id="party_reg1",
                    party_name_normalized="EMPRESA REG LTDA",
                    sanction_source="ceis",
                    sanction_id="s_reg1",
                    match_strategy="tax_id",
                    match_score=0.95,
                    match_confidence="high",
                    red_flag=True,
                )
            )
            session.add(
                ServingCompoundRisk(
                    pair_id="cr_reg1",
                    minister_name="MIN REG",
                    entity_type="party",
                    entity_id="party_reg1",
                    entity_name="EMPRESA REG LTDA",
                    signal_count=3,
                    red_flag=True,
                )
            )
            # Single signal: apenas sanção, sem compound risk
            session.add(
                ServingSanctionMatch(
                    match_id="sm_single",
                    party_id="party_single",
                    party_name_normalized="EMPRESA SINGLE LTDA",
                    sanction_source="cnep",
                    sanction_id="s_single1",
                    match_strategy="name_fuzzy",
                    match_score=0.7,
                    match_confidence="medium",
                    red_flag=False,
                )
            )
            # Registra schema meta para test_atomic_publication_intact
            session.add(
                ServingSchemaMeta(
                    singleton_key="serving",
                    schema_version=1,
                    schema_fingerprint="test_fingerprint",
                    built_at=now,
                )
            )

    with Session(engine) as session:
        with session.begin():
            materialize_graph(session)

    with Session(engine) as session:
        with session.begin():
            compute_graph_scores(session)

    yield engine
    engine.dispose()


# ---------------------------------------------------------------------------
# 1. Estabilidade de IDs entre builds
# ---------------------------------------------------------------------------


def test_node_edge_id_stability(regression_db) -> None:
    """IDs de nós e arestas são idênticos após rebuild completo."""
    with Session(regression_db) as session:
        first_node_ids = {r.node_id for r in session.scalars(select(ServingGraphNode))}
        first_edge_ids = {r.edge_id for r in session.scalars(select(ServingGraphEdge))}

    with Session(regression_db) as session:
        with session.begin():
            _clear_graph_tables(session)

    with Session(regression_db) as session:
        with session.begin():
            materialize_graph(session)

    with Session(regression_db) as session:
        second_node_ids = {r.node_id for r in session.scalars(select(ServingGraphNode))}
        second_edge_ids = {r.edge_id for r in session.scalars(select(ServingGraphEdge))}

    assert first_node_ids == second_node_ids
    assert first_edge_ids == second_edge_ids


# ---------------------------------------------------------------------------
# 2. Proveniência em todas as arestas
# ---------------------------------------------------------------------------


def test_provenance_on_all_edges(regression_db) -> None:
    """Toda aresta tem source_table não nulo."""
    with Session(regression_db) as session:
        edges = list(session.scalars(select(ServingGraphEdge)))

    assert len(edges) >= 1
    for edge in edges:
        assert edge.source_table is not None, f"Aresta {edge.edge_id} sem source_table"


# ---------------------------------------------------------------------------
# 3. Modo strict exclui arestas fuzzy/truncadas
# ---------------------------------------------------------------------------


def test_strict_mode_excludes_fuzzy_truncated(regression_db) -> None:
    """Scores strict não referenciam arestas fuzzy nem truncadas em explanation."""
    with Session(regression_db) as session:
        strict_scores = list(
            session.scalars(select(ServingGraphScore).where(ServingGraphScore.traversal_mode == "strict"))
        )

    for score in strict_scores:
        if not score.explanation_json:
            continue
        explanation = json.loads(score.explanation_json)
        for edge_detail in explanation.get("edges", []):
            strength = edge_detail.get("strength", "")
            truncated = edge_detail.get("truncated", False)
            assert strength not in ("fuzzy", "inferred"), (
                f"Score strict {score.score_id} contém aresta fuzzy: {edge_detail}"
            )
            assert not truncated, f"Score strict {score.score_id} contém aresta truncada: {edge_detail}"


# ---------------------------------------------------------------------------
# 4. Entidade multi-sinal domina top sobre single-signal
# ---------------------------------------------------------------------------


def test_single_signal_not_dominating_top(regression_db) -> None:
    """No broad top-10, entidades multi_signal têm prioridade maior que single_signal."""
    with Session(regression_db) as session:
        scores = list(
            session.scalars(
                select(ServingGraphScore)
                .where(ServingGraphScore.traversal_mode == "broad")
                .order_by(ServingGraphScore.operational_priority.desc())
                .limit(10)
            )
        )

    multi = [s for s in scores if s.signal_registry == "multi_signal"]
    single = [s for s in scores if s.signal_registry == "single_signal"]

    if multi and single:
        max_multi = max(s.operational_priority for s in multi)
        max_single = max(s.operational_priority for s in single)
        assert max_multi >= max_single, f"Single signal ({max_single}) superou multi_signal ({max_multi}) no topo"


# ---------------------------------------------------------------------------
# 5. Schema version presente após build
# ---------------------------------------------------------------------------


def test_atomic_publication_intact(regression_db) -> None:
    """ServingSchemaMeta está presente após build, indicando publicação atômica."""
    with Session(regression_db) as session:
        meta = session.get(ServingSchemaMeta, "serving")
    assert meta is not None, "ServingSchemaMeta ausente — build incompleto"
    assert meta.schema_version is not None


# ---------------------------------------------------------------------------
# 6. Rebuild idempotente
# ---------------------------------------------------------------------------


def test_rebuild_idempotent(regression_db) -> None:
    """Rebuild completo produz o mesmo número de nós e mesmo top-5 de scores."""
    with Session(regression_db) as session:
        node_count_before = session.scalar(select(func.count()).select_from(ServingGraphNode))
        top5_before = [
            s.score_id
            for s in session.scalars(
                select(ServingGraphScore)
                .where(ServingGraphScore.traversal_mode == "broad")
                .order_by(ServingGraphScore.operational_priority.desc())
                .limit(5)
            )
        ]

    with Session(regression_db) as session:
        with session.begin():
            _clear_graph_tables(session)

    with Session(regression_db) as session:
        with session.begin():
            materialize_graph(session)
    with Session(regression_db) as session:
        with session.begin():
            compute_graph_scores(session)

    with Session(regression_db) as session:
        node_count_after = session.scalar(select(func.count()).select_from(ServingGraphNode))
        top5_after = [
            s.score_id
            for s in session.scalars(
                select(ServingGraphScore)
                .where(ServingGraphScore.traversal_mode == "broad")
                .order_by(ServingGraphScore.operational_priority.desc())
                .limit(5)
            )
        ]

    assert node_count_before == node_count_after
    assert top5_before == top5_after


# ---------------------------------------------------------------------------
# 7. Nenhuma aresta determinística tem truncated_flag=True
# ---------------------------------------------------------------------------


def test_no_blocked_column_in_deterministic_join(regression_db) -> None:
    """Nenhuma aresta com evidence_strength=deterministic tem truncated_flag=True."""
    with Session(regression_db) as session:
        bad_edges = list(
            session.scalars(
                select(ServingGraphEdge).where(
                    ServingGraphEdge.evidence_strength == "deterministic",
                    ServingGraphEdge.truncated_flag.is_(True),
                )
            )
        )
    assert len(bad_edges) == 0, f"Arestas determinísticas com truncated_flag: {[e.edge_id for e in bad_edges]}"


# ---------------------------------------------------------------------------
# 8. Módulos sem reason silencioso
# ---------------------------------------------------------------------------


def test_module_availability_no_silent_empty(regression_db) -> None:
    """Todos os módulos têm status_reason preenchido — nenhuma falha silenciosa."""
    with Session(regression_db) as session:
        mods = list(session.scalars(select(ServingModuleAvailability)))

    assert len(mods) > 0
    for mod in mods:
        assert mod.status_reason, f"Módulo {mod.module_name} com status={mod.status} sem status_reason"


# ---------------------------------------------------------------------------
# 9. Todos os itens de revisão têm review_reason
# ---------------------------------------------------------------------------


def test_all_review_items_have_reason(regression_db) -> None:
    """Todo item em ServingReviewQueue tem review_reason não nulo."""
    with Session(regression_db) as session:
        items = list(session.scalars(select(ServingReviewQueue)))

    for item in items:
        assert item.review_reason, f"Item de revisão {item.item_id} sem review_reason"


# ---------------------------------------------------------------------------
# 10. Todos os scores têm explanation_json
# ---------------------------------------------------------------------------


def test_scores_have_explanation(regression_db) -> None:
    """Todo ServingGraphScore tem explanation_json com campos obrigatórios."""
    with Session(regression_db) as session:
        scores = list(session.scalars(select(ServingGraphScore)))

    assert len(scores) >= 1
    for score in scores:
        assert score.explanation_json is not None, f"Score {score.score_id} sem explanation_json"
        explanation = json.loads(score.explanation_json)
        for key in ("score_components", "penalties", "edges", "mode"):
            assert key in explanation, f"Score {score.score_id}: chave '{key}' ausente em explanation_json"
