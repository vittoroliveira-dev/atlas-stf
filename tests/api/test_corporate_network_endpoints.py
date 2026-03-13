"""Tests for corporate network API endpoints."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from atlas_stf.api.app import create_app
from atlas_stf.serving.models import (
    Base,
    ServingCorporateConflict,
    ServingMetric,
    ServingSchemaMeta,
)
from tests.api.conftest import managed_engine


@pytest.fixture()
def client(tmp_path) -> TestClient:
    db_path = tmp_path / "test.db"
    db_url = f"sqlite:///{db_path}"
    with managed_engine(db_url) as engine:
        Base.metadata.create_all(engine)

        with Session(engine) as session:
            with session.begin():
                session.add(
                    ServingCorporateConflict(
                        conflict_id="cn-abc123",
                        minister_name="MIN. TESTE",
                        company_cnpj_basico="12345678",
                        company_name="EMPRESA XYZ LTDA",
                        minister_qualification="49",
                        linked_entity_type="party",
                        linked_entity_id="p1",
                        linked_entity_name="AUTOR A",
                        entity_qualification="22",
                        shared_process_ids_json=json.dumps(["proc_1", "proc_2", "proc_3"]),
                        shared_process_count=3,
                        favorable_rate=0.8,
                        baseline_favorable_rate=0.5,
                        favorable_rate_delta=0.3,
                        risk_score=0.3,
                        decay_factor=1.0,
                        red_flag=True,
                        link_chain="MIN. TESTE -> EMPRESA XYZ LTDA -> AUTOR A",
                        link_degree=1,
                        generated_at=datetime.now(timezone.utc),
                    )
                )
                session.add(
                    ServingCorporateConflict(
                        conflict_id="cn-def456",
                        minister_name="MIN. OUTRO",
                        company_cnpj_basico="87654321",
                        company_name="OUTRA EMPRESA SA",
                        linked_entity_type="counsel",
                        linked_entity_id="c1",
                        linked_entity_name="ADV B",
                        shared_process_count=1,
                        risk_score=0.0,
                        decay_factor=1.0,
                        red_flag=False,
                    )
                )
                session.add(
                    ServingCorporateConflict(
                        conflict_id="cn-ghi789",
                        minister_name="MIN. TESTE",
                        company_cnpj_basico="55555555",
                        company_name="EMPRESA E LTDA",
                        minister_qualification="49",
                        linked_entity_type="party",
                        linked_entity_id="p1",
                        linked_entity_name="AUTOR A",
                        entity_qualification="22",
                        shared_process_ids_json=json.dumps(["proc_1", "proc_2", "proc_3"]),
                        shared_process_count=3,
                        favorable_rate=1.0,
                        baseline_favorable_rate=0.5,
                        favorable_rate_delta=0.5,
                        risk_score=0.25,
                        decay_factor=0.5,
                        red_flag=True,
                        link_chain="MIN. TESTE -> EMPRESA XYZ LTDA -> EMPRESA C LTDA -> EMPRESA E LTDA -> AUTOR A",
                        link_degree=3,
                        generated_at=datetime.now(timezone.utc),
                    )
                )
                session.add(ServingMetric(key="alert_count", value_integer=0))
                session.add(ServingMetric(key="avg_alert_score", value_float=0.0))
                session.add(ServingMetric(key="valid_group_count", value_integer=0))
                session.add(ServingMetric(key="baseline_count", value_integer=0))
                session.add(
                    ServingSchemaMeta(
                        singleton_key="serving",
                        schema_version=1,
                        schema_fingerprint="test",
                        built_at=datetime.now(timezone.utc),
                    )
                )

    app = create_app(database_url=db_url)
    with TestClient(app) as client:
        yield client


class TestCorporateNetworkEndpoints:
    def test_list(self, client: TestClient) -> None:
        resp = client.get("/corporate-network", params={"page": 1, "page_size": 10})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 3
        assert len(data["items"]) == 3

    def test_filter_minister(self, client: TestClient) -> None:
        resp = client.get("/corporate-network", params={"page": 1, "page_size": 10, "minister": "TESTE"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert {item["minister_name"] for item in data["items"]} == {"MIN. TESTE"}

    def test_filter_minister_treats_percent_as_literal(self, client: TestClient) -> None:
        resp = client.get("/corporate-network", params={"page": 1, "page_size": 10, "minister": "%"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 0
        assert data["items"] == []

    def test_filter_red_flag_only(self, client: TestClient) -> None:
        resp = client.get("/corporate-network", params={"page": 1, "page_size": 10, "red_flag_only": True})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert all(item["red_flag"] is True for item in data["items"])

    def test_red_flags_endpoint(self, client: TestClient) -> None:
        resp = client.get("/corporate-network/red-flags")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert {item["conflict_id"] for item in data["items"]} == {"cn-abc123", "cn-ghi789"}

    def test_red_flags_endpoint_honors_limit(self, client: TestClient) -> None:
        resp = client.get("/corporate-network/red-flags", params={"limit": 1})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert len(data["items"]) == 1

    def test_minister_detail(self, client: TestClient) -> None:
        resp = client.get("/ministers/TESTE/corporate-conflicts")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        assert {item["minister_name"] for item in data} == {"MIN. TESTE"}

    def test_minister_detail_honors_limit(self, client: TestClient) -> None:
        resp = client.get("/ministers/TESTE/corporate-conflicts", params={"limit": 1})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1

    def test_minister_detail_empty(self, client: TestClient) -> None:
        resp = client.get("/ministers/DESCONHECIDO/corporate-conflicts")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_json_fields(self, client: TestClient) -> None:
        resp = client.get("/corporate-network", params={"page": 1, "page_size": 10})
        item = [i for i in resp.json()["items"] if i["conflict_id"] == "cn-abc123"][0]
        assert item["shared_process_ids"] == ["proc_1", "proc_2", "proc_3"]
        assert item["link_chain"] == "MIN. TESTE -> EMPRESA XYZ LTDA -> AUTOR A"

    def test_pagination(self, client: TestClient) -> None:
        resp = client.get("/corporate-network", params={"page": 1, "page_size": 1})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 3
        assert len(data["items"]) == 1

    def test_filter_link_degree_3(self, client: TestClient) -> None:
        resp = client.get("/corporate-network", params={"page": 1, "page_size": 10, "link_degree": 3})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert data["items"][0]["conflict_id"] == "cn-ghi789"
        assert data["items"][0]["decay_factor"] == 0.5
        assert data["items"][0]["risk_score"] == 0.25
