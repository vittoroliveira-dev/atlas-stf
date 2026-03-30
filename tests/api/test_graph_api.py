"""API tests for graph, investigation, and review endpoints."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from atlas_stf.api.app import create_app
from atlas_stf.serving._builder_graph import _nid, materialize_graph
from atlas_stf.serving._builder_scoring import compute_graph_scores
from atlas_stf.serving.models import (
    Base,
    ServingCase,
    ServingCompoundRisk,
    ServingCounsel,
    ServingParty,
    ServingProcessParty,
    ServingSanctionMatch,
)


@pytest.fixture(scope="module")
def graph_client(tmp_path_factory):
    """TestClient com tabelas de grafo populadas e scoring calculado."""
    import os

    prev = os.environ.get("ATLAS_STF_REVIEW_API_KEY")
    os.environ["ATLAS_STF_REVIEW_API_KEY"] = "__dev__"
    tmp = tmp_path_factory.mktemp("graph_api")
    db_path = tmp / "test.db"
    db_url = f"sqlite:///{db_path}"
    engine = create_engine(db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)

    now = datetime.now(timezone.utc)
    with Session(engine) as s:
        with s.begin():
            s.add(
                ServingCase(
                    decision_event_id="de1",
                    process_id="proc1",
                    process_class="ADI",
                    thematic_key="const",
                    current_rapporteur="MIN A",
                    decision_date=now.date(),
                    period="2024-01",
                )
            )
            s.add(
                ServingParty(
                    party_id="p1",
                    party_name_raw="EMPRESA A",
                    party_name_normalized="EMPRESA A",
                )
            )
            s.add(
                ServingCounsel(
                    counsel_id="c1",
                    counsel_name_raw="ADV A",
                    counsel_name_normalized="ADV A",
                )
            )
            s.add(
                ServingProcessParty(
                    link_id="pp1",
                    process_id="proc1",
                    party_id="p1",
                    role_in_case="REQTE",
                    source_id="juris",
                )
            )
            s.add(
                ServingSanctionMatch(
                    match_id="sm1",
                    party_id="p1",
                    party_name_normalized="EMPRESA A",
                    sanction_source="ceis",
                    sanction_id="s1",
                    match_strategy="tax_id",
                    match_score=0.95,
                    match_confidence="high",
                    red_flag=True,
                )
            )
            s.add(
                ServingCompoundRisk(
                    pair_id="cr1",
                    minister_name="MIN A",
                    entity_type="party",
                    entity_id="p1",
                    entity_name="EMPRESA A",
                    signal_count=3,
                    red_flag=True,
                )
            )

    with Session(engine) as s:
        with s.begin():
            materialize_graph(s)

    with Session(engine) as s:
        with s.begin():
            compute_graph_scores(s)

    app = create_app(database_url=db_url)
    client = TestClient(app, raise_server_exceptions=False)
    yield client
    engine.dispose()
    if prev is None:
        os.environ.pop("ATLAS_STF_REVIEW_API_KEY", None)
    else:
        os.environ["ATLAS_STF_REVIEW_API_KEY"] = prev


# ---------------------------------------------------------------------------
# Graph: nós
# ---------------------------------------------------------------------------


def test_graph_search(graph_client: TestClient) -> None:
    resp = graph_client.get("/graph/search", params={"query": "EMPRESA"})
    assert resp.status_code == 200
    body = resp.json()
    assert "items" in body
    assert len(body["items"]) >= 1


def test_graph_node_detail(graph_client: TestClient) -> None:
    node_id = _nid("party", "p1")
    resp = graph_client.get(f"/graph/nodes/{node_id}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["node_type"] == "party"
    assert body["node_id"] == node_id


def test_graph_node_not_found(graph_client: TestClient) -> None:
    resp = graph_client.get("/graph/nodes/nonexistent_node_id_xyz")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "node_not_found"


def test_graph_edge_detail(graph_client: TestClient) -> None:
    search = graph_client.get("/graph/search", params={"query": "EMPRESA"})
    assert search.status_code == 200
    node_id = _nid("party", "p1")
    neighbors = graph_client.get(f"/graph/neighbors/{node_id}")
    assert neighbors.status_code == 200
    edges = neighbors.json().get("edges", [])
    if not edges:
        pytest.skip("Nenhuma aresta disponível para testar edge detail")
    edge_id = edges[0]["edge_id"]
    resp = graph_client.get(f"/graph/edges/{edge_id}")
    assert resp.status_code == 200
    body = resp.json()
    assert "edge_type" in body
    assert body["edge_id"] == edge_id


# ---------------------------------------------------------------------------
# Graph: vizinhos e caminhos
# ---------------------------------------------------------------------------


def test_graph_neighbors(graph_client: TestClient) -> None:
    node_id = _nid("party", "p1")
    resp = graph_client.get(f"/graph/neighbors/{node_id}", params={"exclude_truncated": "true"})
    assert resp.status_code == 200
    body = resp.json()
    assert "center_node" in body
    assert "neighbors" in body
    assert isinstance(body["neighbors"], list)


def test_graph_neighbors_node_not_found(graph_client: TestClient) -> None:
    resp = graph_client.get("/graph/neighbors/nonexistent_node_xyz")
    assert resp.status_code == 404


def test_graph_paths(graph_client: TestClient) -> None:
    start_id = _nid("case", "proc1")
    end_id = _nid("party", "p1")
    resp = graph_client.get("/graph/paths", params={"start_id": start_id, "end_id": end_id})
    assert resp.status_code == 200
    body = resp.json()
    assert "items" in body
    assert isinstance(body["items"], list)


# ---------------------------------------------------------------------------
# Graph: scores e métricas
# ---------------------------------------------------------------------------


def test_graph_scores(graph_client: TestClient) -> None:
    resp = graph_client.get("/graph/scores", params={"mode": "broad", "min_signals": 0})
    assert resp.status_code == 200
    body = resp.json()
    assert "items" in body
    if body["items"]:
        item = body["items"][0]
        assert "raw_score" in item
        assert "operational_priority" in item
        assert "traversal_mode" in item


def test_graph_metrics(graph_client: TestClient) -> None:
    resp = graph_client.get("/graph/metrics")
    assert resp.status_code == 200
    body = resp.json()
    assert "pct_deterministic_edges" in body
    assert "total_nodes" in body
    assert "total_edges" in body
    assert body["total_nodes"] >= 1


# ---------------------------------------------------------------------------
# Investigations
# ---------------------------------------------------------------------------


def test_investigations_top(graph_client: TestClient) -> None:
    resp = graph_client.get("/investigations/top", params={"mode": "broad", "min_signals": 0})
    assert resp.status_code == 200
    body = resp.json()
    assert "items" in body
    assert isinstance(body["items"], list)


def test_investigation_entity(graph_client: TestClient) -> None:
    resp = graph_client.get("/investigations/entity/p1")
    assert resp.status_code == 200
    body = resp.json()
    assert "node" in body
    assert "score" in body
    assert "bundles" in body


def test_investigation_entity_not_found(graph_client: TestClient) -> None:
    resp = graph_client.get("/investigations/entity/entidade_inexistente_xyz")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Review queue
# ---------------------------------------------------------------------------


def test_review_queue(graph_client: TestClient) -> None:
    resp = graph_client.get("/review/queue", params={"status": "pending"})
    assert resp.status_code == 200
    body = resp.json()
    assert "items" in body
    assert isinstance(body["items"], list)


def test_review_decision(graph_client: TestClient) -> None:
    # Busca um item pendente existente antes de tentar registrar decisão
    queue_resp = graph_client.get("/review/queue", params={"status": "pending", "page_size": 1})
    assert queue_resp.status_code == 200
    items = queue_resp.json().get("items", [])
    if not items:
        pytest.skip("Nenhum item pendente na fila de revisão")
    item_id = items[0]["item_id"]
    resp = graph_client.post(
        "/review/decision",
        json={"item_id": item_id, "status": "confirmed_relevant", "notes": "Teste automatizado"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["item_id"] == item_id
    assert body["status"] == "confirmed_relevant"


def test_review_decision_invalid_status(graph_client: TestClient) -> None:
    resp = graph_client.post(
        "/review/decision",
        json={"item_id": "qualquer_id", "status": "status_invalido"},
    )
    # Pydantic Literal validation rejects before handler → 422
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# SEC-005: tier/status enum validation
# ---------------------------------------------------------------------------


def test_review_queue_rejects_invalid_tier(graph_client: TestClient) -> None:
    resp = graph_client.get("/review/queue", params={"tier": "critical"})
    assert resp.status_code == 422


def test_review_queue_accepts_valid_tier(graph_client: TestClient) -> None:
    for tier in ("high", "medium", "low"):
        resp = graph_client.get("/review/queue", params={"tier": tier})
        assert resp.status_code == 200, f"tier={tier} should be accepted"


def test_review_queue_rejects_invalid_status(graph_client: TestClient) -> None:
    resp = graph_client.get("/review/queue", params={"status": "bogus_status"})
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# SEC-006: review decision auth
# ---------------------------------------------------------------------------


def test_review_decision_503_when_key_unset(tmp_path_factory, monkeypatch) -> None:
    """When ATLAS_STF_REVIEW_API_KEY is not set, endpoint returns 503 (fail-closed)."""
    monkeypatch.delenv("ATLAS_STF_REVIEW_API_KEY", raising=False)
    tmp = tmp_path_factory.mktemp("auth_503")
    db_path = tmp / "auth503.db"
    db_url = f"sqlite:///{db_path}"
    engine = create_engine(db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    engine.dispose()

    app = create_app(database_url=db_url)
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.post(
        "/review/decision",
        json={"item_id": "nonexistent", "status": "confirmed_relevant"},
    )
    assert resp.status_code == 503


def test_review_decision_rejects_wrong_key(tmp_path_factory, monkeypatch) -> None:
    """When ATLAS_STF_REVIEW_API_KEY is set, wrong/missing key → 401."""
    monkeypatch.setenv("ATLAS_STF_REVIEW_API_KEY", "test-secret-key-12345")
    tmp = tmp_path_factory.mktemp("auth_test")
    db_path = tmp / "auth.db"
    db_url = f"sqlite:///{db_path}"
    engine = create_engine(db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    engine.dispose()

    app = create_app(database_url=db_url)
    client = TestClient(app)

    # No key → 401
    resp = client.post(
        "/review/decision",
        json={"item_id": "x", "status": "confirmed_relevant"},
    )
    assert resp.status_code == 401

    # Wrong key → 401
    resp = client.post(
        "/review/decision",
        json={"item_id": "x", "status": "confirmed_relevant"},
        headers={"X-Review-API-Key": "wrong-key"},
    )
    assert resp.status_code == 401

    # Correct key → passes auth (404 because item doesn't exist)
    resp = client.post(
        "/review/decision",
        json={"item_id": "x", "status": "confirmed_relevant"},
        headers={"X-Review-API-Key": "test-secret-key-12345"},
    )
    assert resp.status_code == 404
