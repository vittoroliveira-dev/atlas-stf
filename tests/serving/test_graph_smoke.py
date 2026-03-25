"""Smoke tests for graph/scoring table population.

Verifies that the serving DB has non-zero counts in graph-related tables
after a serving build.  These tests read the live serving DB (or skip if
not present) and check minimum population thresholds.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine, func, inspect, select
from sqlalchemy.orm import Session

_DB_PATH = Path("data/serving/atlas_stf.db")
_DB_URL = os.getenv("ATLAS_STF_DATABASE_URL", f"sqlite:///{_DB_PATH}")


@pytest.fixture(scope="module")
def session() -> Session:
    if not _DB_PATH.exists() and "sqlite" in _DB_URL:
        pytest.skip("Serving DB not found — run 'make serving-build' first")
    engine = create_engine(_DB_URL)
    with Session(engine) as s:
        yield s


def _has_table(session: Session, table_name: str) -> bool:
    insp = inspect(session.bind)
    return table_name in insp.get_table_names()


def _require_table(session: Session, table_name: str) -> None:
    if not _has_table(session, table_name):
        pytest.skip(f"Table {table_name!r} not in DB — serving-build may need graph builders")


def _count(session: Session, model: type) -> int:
    return session.scalar(select(func.count()).select_from(model)) or 0


# ---------------------------------------------------------------------------
# Graph tables minimum counts
# ---------------------------------------------------------------------------


class TestGraphNodePopulation:
    @pytest.fixture(autouse=True)
    def _check_table(self, session: Session) -> None:
        _require_table(session, "serving_graph_node")

    def test_has_nodes(self, session: Session) -> None:
        from atlas_stf.serving._models_graph import ServingGraphNode

        count = _count(session, ServingGraphNode)
        assert count > 0, "ServingGraphNode is empty — graph builder did not run"

    def test_has_multiple_node_types(self, session: Session) -> None:
        from atlas_stf.serving._models_graph import ServingGraphNode

        types = session.scalars(select(ServingGraphNode.node_type).distinct()).all()
        assert len(types) >= 3, f"Expected ≥3 node types, got {len(types)}: {types}"


class TestGraphEdgePopulation:
    @pytest.fixture(autouse=True)
    def _check_table(self, session: Session) -> None:
        _require_table(session, "serving_graph_edge")

    def test_has_edges(self, session: Session) -> None:
        from atlas_stf.serving._models_graph import ServingGraphEdge

        count = _count(session, ServingGraphEdge)
        assert count > 0, "ServingGraphEdge is empty — graph builder did not create edges"

    def test_has_multiple_edge_types(self, session: Session) -> None:
        from atlas_stf.serving._models_graph import ServingGraphEdge

        types = session.scalars(select(ServingGraphEdge.edge_type).distinct()).all()
        assert len(types) >= 2, f"Expected ≥2 edge types, got {len(types)}: {types}"

    def test_has_deterministic_edges(self, session: Session) -> None:
        from atlas_stf.serving._models_graph import ServingGraphEdge

        count = session.scalar(
            select(func.count()).select_from(ServingGraphEdge).where(
                ServingGraphEdge.evidence_strength == "deterministic"
            )
        ) or 0
        assert count > 0, "No deterministic edges found — data linkage broken"


class TestGraphScorePopulation:
    @pytest.fixture(autouse=True)
    def _check_table(self, session: Session) -> None:
        _require_table(session, "serving_graph_score")

    def test_has_scores(self, session: Session) -> None:
        from atlas_stf.serving._models_graph import ServingGraphScore

        count = _count(session, ServingGraphScore)
        assert count > 0, "ServingGraphScore is empty — scoring did not run"

    def test_scores_have_positive_priority(self, session: Session) -> None:
        from atlas_stf.serving._models_graph import ServingGraphScore

        max_priority = session.scalar(select(func.max(ServingGraphScore.operational_priority))) or 0.0
        assert max_priority > 0, "All scores have 0 operational_priority"


class TestGraphPathPopulation:
    @pytest.fixture(autouse=True)
    def _check_table(self, session: Session) -> None:
        _require_table(session, "serving_graph_path_candidate")

    def test_has_paths(self, session: Session) -> None:
        from atlas_stf.serving._models_graph import ServingGraphPathCandidate

        count = _count(session, ServingGraphPathCandidate)
        assert count > 0, "ServingGraphPathCandidate is empty — path computation did not run"


class TestEvidenceBundlePopulation:
    @pytest.fixture(autouse=True)
    def _check_table(self, session: Session) -> None:
        _require_table(session, "serving_evidence_bundle")

    def test_has_bundles(self, session: Session) -> None:
        from atlas_stf.serving._models_graph import ServingEvidenceBundle

        count = _count(session, ServingEvidenceBundle)
        assert count > 0, "ServingEvidenceBundle is empty"

    def test_bundles_have_signals(self, session: Session) -> None:
        from atlas_stf.serving._models_graph import ServingEvidenceBundle

        max_signals = session.scalar(select(func.max(ServingEvidenceBundle.signal_count))) or 0
        assert max_signals >= 2, f"Max signal_count={max_signals}, expected ≥2 for meaningful bundles"


class TestModuleAvailability:
    @pytest.fixture(autouse=True)
    def _check_table(self, session: Session) -> None:
        _require_table(session, "serving_module_availability")

    def test_has_modules(self, session: Session) -> None:
        from atlas_stf.serving._models_graph import ServingModuleAvailability

        count = _count(session, ServingModuleAvailability)
        assert count > 0, "ServingModuleAvailability is empty"

    def test_no_all_missing(self, session: Session) -> None:
        from atlas_stf.serving._models_graph import ServingModuleAvailability

        available = session.scalar(
            select(func.count()).select_from(ServingModuleAvailability).where(
                ServingModuleAvailability.status == "available"
            )
        ) or 0
        assert available > 0, "No modules marked as available"


# ---------------------------------------------------------------------------
# Cross-table consistency
# ---------------------------------------------------------------------------


class TestGraphConsistency:
    @pytest.fixture(autouse=True)
    def _check_tables(self, session: Session) -> None:
        _require_table(session, "serving_graph_node")
        _require_table(session, "serving_graph_edge")
        _require_table(session, "serving_graph_score")

    def test_node_count_ge_edge_unique_nodes(self, session: Session) -> None:
        """Every node referenced by an edge must exist in the node table."""
        from atlas_stf.serving._models_graph import ServingGraphEdge, ServingGraphNode

        node_ids = set(session.scalars(select(ServingGraphNode.node_id)).all())
        src_ids = set(session.scalars(select(ServingGraphEdge.src_node_id).distinct()).all())
        dst_ids = set(session.scalars(select(ServingGraphEdge.dst_node_id).distinct()).all())
        edge_node_ids = src_ids | dst_ids
        orphan = edge_node_ids - node_ids
        assert not orphan, f"{len(orphan)} edge node_ids not found in node table (sample: {list(orphan)[:5]})"

    def test_score_entity_ids_exist_as_nodes(self, session: Session) -> None:
        """Every scored entity must have a corresponding node."""
        from atlas_stf.serving._models_graph import ServingGraphNode, ServingGraphScore

        scored_entities = set(session.scalars(select(ServingGraphScore.entity_id).distinct()).all())
        node_entities = set(session.scalars(select(ServingGraphNode.entity_id).distinct()).all())
        orphan = scored_entities - node_entities
        assert not orphan, f"{len(orphan)} scored entity_ids not found in nodes (sample: {list(orphan)[:5]})"
