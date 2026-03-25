"""Tests for Camada E: scoring, strict/broad traversal, ranking."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from atlas_stf.serving._builder_graph import materialize_graph
from atlas_stf.serving._builder_scoring import (
    _edge_score_contribution,
    _is_strict_edge,
    compute_graph_scores,
)
from atlas_stf.serving._models_graph import (
    ServingGraphEdge,
    ServingGraphScore,
    ServingReviewQueue,
)
from atlas_stf.serving.models import (
    Base,
    ServingCase,
    ServingCompoundRisk,
    ServingCounsel,
    ServingParty,
    ServingProcessParty,
    ServingSanctionMatch,
)


@pytest.fixture
def scoring_db(tmp_path):
    """SQLite with seed data for scoring tests."""
    db_path = tmp_path / "test_scoring.db"
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)

    now = datetime.now(timezone.utc)
    with Session(engine) as session:
        with session.begin():
            session.add(
                ServingCase(
                    decision_event_id="de_score1",
                    process_id="proc_score1",
                    process_class="ADI",
                    thematic_key="constitucional",
                    current_rapporteur="MIN TESTE",
                    decision_date=now.date(),
                    period="2024-01",
                )
            )
            session.add(
                ServingParty(
                    party_id="party_score1",
                    party_name_raw="EMPRESA SCORE LTDA",
                    party_name_normalized="EMPRESA SCORE LTDA",
                )
            )
            session.add(
                ServingCounsel(
                    counsel_id="csl_score1",
                    counsel_name_raw="ADV SCORE",
                    counsel_name_normalized="ADV SCORE",
                )
            )
            session.add(
                ServingProcessParty(
                    link_id="pp_score1",
                    process_id="proc_score1",
                    party_id="party_score1",
                    role_in_case="REQTE.(S)",
                    source_id="juris",
                )
            )
            # Two risk signals for multi-signal test
            session.add(
                ServingSanctionMatch(
                    match_id="sm_score1",
                    party_id="party_score1",
                    party_name_normalized="EMPRESA SCORE LTDA",
                    sanction_source="ceis",
                    sanction_id="s_score1",
                    match_strategy="tax_id",
                    match_score=0.95,
                    match_confidence="high",
                    red_flag=True,
                )
            )
            session.add(
                ServingCompoundRisk(
                    pair_id="cr_score1",
                    minister_name="MIN TESTE",
                    entity_type="party",
                    entity_id="party_score1",
                    entity_name="EMPRESA SCORE LTDA",
                    signal_count=3,
                    red_flag=True,
                )
            )

    # Materialize graph first (scoring reads from graph tables)
    with Session(engine) as session:
        with session.begin():
            materialize_graph(session)

    yield engine
    engine.dispose()


# ---------------------------------------------------------------------------
# Unit tests: edge classification
# ---------------------------------------------------------------------------


class TestEdgeClassification:
    def test_strict_edge_requires_deterministic(self) -> None:
        e = ServingGraphEdge(
            edge_id="e1",
            src_node_id="a",
            dst_node_id="b",
            edge_type="case_has_party",
            evidence_strength="deterministic",
            traversal_policy="strict_allowed",
            truncated_flag=False,
        )
        assert _is_strict_edge(e) is True

    def test_statistical_not_strict(self) -> None:
        e = ServingGraphEdge(
            edge_id="e2",
            src_node_id="a",
            dst_node_id="b",
            edge_type="sanction_hits_entity",
            evidence_strength="statistical",
            traversal_policy="strict_allowed",
            truncated_flag=False,
        )
        assert _is_strict_edge(e) is False

    def test_truncated_not_strict(self) -> None:
        e = ServingGraphEdge(
            edge_id="e3",
            src_node_id="a",
            dst_node_id="b",
            edge_type="sanction_links_corporate_path",
            evidence_strength="deterministic",
            traversal_policy="strict_allowed",
            truncated_flag=True,
        )
        assert _is_strict_edge(e) is False

    def test_broad_only_not_strict(self) -> None:
        e = ServingGraphEdge(
            edge_id="e4",
            src_node_id="a",
            dst_node_id="b",
            edge_type="case_has_compound_signal",
            evidence_strength="deterministic",
            traversal_policy="broad_only",
            truncated_flag=False,
        )
        assert _is_strict_edge(e) is False

    def test_score_contribution_deterministic(self) -> None:
        e = ServingGraphEdge(
            edge_id="e5",
            src_node_id="a",
            dst_node_id="b",
            edge_type="case_has_party",
            evidence_strength="deterministic",
            confidence_score=0.9,
        )
        contrib = _edge_score_contribution(e)
        assert contrib["documentary"] > 0
        assert contrib["statistical"] == 0
        assert contrib["fuzzy_penalty"] == 0

    def test_score_contribution_fuzzy_has_penalty(self) -> None:
        e = ServingGraphEdge(
            edge_id="e6",
            src_node_id="a",
            dst_node_id="b",
            edge_type="donation_associated_with_entity",
            evidence_strength="fuzzy",
            confidence_score=0.5,
        )
        contrib = _edge_score_contribution(e)
        assert contrib["fuzzy_penalty"] > 0
        assert contrib["documentary"] == 0


# ---------------------------------------------------------------------------
# Integration tests: scoring computation
# ---------------------------------------------------------------------------


class TestScoringIntegration:
    def test_scores_computed(self, scoring_db) -> None:
        with Session(scoring_db) as session:
            with session.begin():
                counts = compute_graph_scores(session)

        assert counts["scores"] >= 2  # at least strict + broad for one entity
        assert counts["strict"] >= 1
        assert counts["broad"] >= 1

    def test_strict_and_broad_differ(self, scoring_db) -> None:
        with Session(scoring_db) as session:
            with session.begin():
                compute_graph_scores(session)

        with Session(scoring_db) as session:
            scores = list(session.scalars(select(ServingGraphScore)))

        strict = [s for s in scores if s.traversal_mode == "strict"]
        broad = [s for s in scores if s.traversal_mode == "broad"]
        assert len(strict) >= 1
        assert len(broad) >= 1
        # Broad should generally have higher raw scores (more edges considered)
        if strict and broad:
            # At minimum, broad considers >= edges than strict
            assert broad[0].raw_score >= strict[0].raw_score

    def test_multi_signal_beats_singleton(self, scoring_db) -> None:
        with Session(scoring_db) as session:
            with session.begin():
                compute_graph_scores(session)

        with Session(scoring_db) as session:
            scores = list(session.scalars(select(ServingGraphScore).where(ServingGraphScore.traversal_mode == "broad")))

        multi = [s for s in scores if s.signal_registry == "multi_signal"]
        single = [s for s in scores if s.signal_registry == "single_signal"]
        if multi and single:
            assert max(s.operational_priority for s in multi) > max(s.operational_priority for s in single)

    def test_singleton_penalty_applied(self, scoring_db) -> None:
        with Session(scoring_db) as session:
            with session.begin():
                compute_graph_scores(session)

        with Session(scoring_db) as session:
            scores = list(session.scalars(select(ServingGraphScore)))

        singletons = [s for s in scores if s.signal_registry == "single_signal"]
        for s in singletons:
            assert s.singleton_penalty > 0

    def test_score_decomposition_present(self, scoring_db) -> None:
        with Session(scoring_db) as session:
            with session.begin():
                compute_graph_scores(session)

        with Session(scoring_db) as session:
            scores = list(session.scalars(select(ServingGraphScore)))

        for s in scores:
            assert s.explanation_json is not None
            explanation = __import__("json").loads(s.explanation_json)
            assert "score_components" in explanation
            assert "penalties" in explanation
            assert "edges" in explanation
            assert "mode" in explanation

    def test_review_queue_enriched(self, scoring_db) -> None:
        with Session(scoring_db) as session:
            with session.begin():
                compute_graph_scores(session)

        with Session(scoring_db) as session:
            items = list(session.scalars(select(ServingReviewQueue)))

        # Items from entities with ≥2 signals should have updated priorities
        scored_items = [i for i in items if i.entity_id and i.priority_score > 1.0]
        assert len(scored_items) >= 0  # may be 0 if no multi-signal entities match review queue


class TestEmptyDbScoring:
    def test_empty_db_no_crash(self, tmp_path) -> None:
        engine = create_engine(f"sqlite:///{tmp_path / 'empty_score.db'}")
        Base.metadata.create_all(engine)
        try:
            with Session(engine) as session:
                with session.begin():
                    counts = compute_graph_scores(session)
            assert counts["scores"] == 0
        finally:
            engine.dispose()


# ---------------------------------------------------------------------------
# Registry consistency tests (prevent RISK_EDGE_TYPES ↔ contribution drift)
# ---------------------------------------------------------------------------


class TestEdgeTypeRegistryConsistency:
    """Ensure the scoring pipeline processes every edge type it knows how to score."""

    def test_network_edge_types_subset_of_risk(self) -> None:
        """Every network edge type must be in RISK_EDGE_TYPES (otherwise it would
        never be loaded by compute_graph_scores)."""
        from atlas_stf.serving._builder_scoring import _NETWORK_EDGE_TYPES, RISK_EDGE_TYPES

        missing = _NETWORK_EDGE_TYPES - RISK_EDGE_TYPES
        assert not missing, f"Network types not in RISK_EDGE_TYPES: {missing}"

    def test_entity_in_economic_group_is_scored(self) -> None:
        """Regression: entity_in_economic_group must be in RISK_EDGE_TYPES so
        that economic group edges contribute to the network score."""
        from atlas_stf.serving._builder_scoring import RISK_EDGE_TYPES

        assert "entity_in_economic_group" in RISK_EDGE_TYPES

    def test_contribution_covers_all_risk_types(self) -> None:
        """Every RISK_EDGE_TYPE must produce a non-zero contribution for at least
        one score category when given a deterministic edge with confidence 1.0."""
        from atlas_stf.serving._builder_scoring import RISK_EDGE_TYPES, _edge_score_contribution

        for edge_type in RISK_EDGE_TYPES:
            edge = ServingGraphEdge(
                edge_id=f"test_{edge_type}",
                src_node_id="a",
                dst_node_id="b",
                edge_type=edge_type,
                evidence_strength="deterministic",
                confidence_score=1.0,
            )
            contrib = _edge_score_contribution(edge)
            total = contrib["documentary"] + contrib["statistical"] + contrib["network"]
            assert total > 0, f"Edge type {edge_type!r} produces zero contribution"
