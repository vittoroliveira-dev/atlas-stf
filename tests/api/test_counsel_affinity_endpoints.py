"""Tests for counsel affinity API endpoints."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from atlas_stf.api.app import create_app
from atlas_stf.serving.models import (
    Base,
    ServingCounselAffinity,
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
                    ServingCounselAffinity(
                        affinity_id="ca-abc123",
                        rapporteur="MIN. TESTE",
                        counsel_id="c1",
                        counsel_name_normalized="ADV SILVA",
                        shared_case_count=10,
                        favorable_count=9,
                        unfavorable_count=1,
                        pair_favorable_rate=0.9,
                        minister_baseline_favorable_rate=0.55,
                        counsel_baseline_favorable_rate=0.6,
                        pair_delta_vs_minister=0.35,
                        pair_delta_vs_counsel=0.3,
                        red_flag=True,
                        top_process_classes_json=json.dumps(["ADI", "ADPF"]),
                        generated_at=datetime.now(timezone.utc),
                    )
                )
                session.add(
                    ServingCounselAffinity(
                        affinity_id="ca-def456",
                        rapporteur="MIN. OUTRO",
                        counsel_id="c2",
                        counsel_name_normalized="ADV COSTA",
                        shared_case_count=3,
                        favorable_count=1,
                        unfavorable_count=2,
                        pair_favorable_rate=0.33,
                        red_flag=False,
                    )
                )
                session.add(
                    ServingCounselAffinity(
                        affinity_id="ca-ghi789",
                        rapporteur="MIN. TESTE",
                        counsel_id="c3",
                        counsel_name_normalized="ADV ROCHA",
                        shared_case_count=8,
                        favorable_count=7,
                        unfavorable_count=1,
                        pair_favorable_rate=0.875,
                        minister_baseline_favorable_rate=0.55,
                        counsel_baseline_favorable_rate=0.58,
                        pair_delta_vs_minister=0.325,
                        pair_delta_vs_counsel=0.295,
                        red_flag=True,
                        top_process_classes_json=json.dumps(["RE"]),
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


class TestCounselAffinityEndpoints:
    def test_list(self, client: TestClient) -> None:
        resp = client.get("/counsel-affinity", params={"page": 1, "page_size": 10})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 3
        assert len(data["items"]) == 3

    def test_filter_minister(self, client: TestClient) -> None:
        resp = client.get("/counsel-affinity", params={"page": 1, "page_size": 10, "minister": "TESTE"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert {item["rapporteur"] for item in data["items"]} == {"MIN. TESTE"}

    def test_filter_minister_treats_percent_as_literal(self, client: TestClient) -> None:
        resp = client.get("/counsel-affinity", params={"page": 1, "page_size": 10, "minister": "%"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 0
        assert data["items"] == []

    def test_filter_red_flag_only(self, client: TestClient) -> None:
        resp = client.get("/counsel-affinity", params={"page": 1, "page_size": 10, "red_flag_only": True})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert all(item["red_flag"] is True for item in data["items"])

    def test_red_flags_endpoint(self, client: TestClient) -> None:
        resp = client.get("/counsel-affinity/red-flags")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert {item["affinity_id"] for item in data["items"]} == {"ca-abc123", "ca-ghi789"}

    def test_red_flags_endpoint_honors_limit(self, client: TestClient) -> None:
        resp = client.get("/counsel-affinity/red-flags", params={"limit": 1})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert len(data["items"]) == 1

    def test_minister_detail(self, client: TestClient) -> None:
        resp = client.get("/ministers/TESTE/counsel-affinity")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        assert {item["rapporteur"] for item in data} == {"MIN. TESTE"}

    def test_minister_detail_honors_limit(self, client: TestClient) -> None:
        resp = client.get("/ministers/TESTE/counsel-affinity", params={"limit": 1})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1

    def test_counsel_detail(self, client: TestClient) -> None:
        resp = client.get("/counsels/c1/minister-affinity")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["counsel_id"] == "c1"

    def test_counsel_detail_honors_limit(self, client: TestClient) -> None:
        resp = client.get("/counsels/c1/minister-affinity", params={"limit": 1})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1

    def test_counsel_detail_empty(self, client: TestClient) -> None:
        resp = client.get("/counsels/unknown/minister-affinity")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_json_fields(self, client: TestClient) -> None:
        resp = client.get("/counsel-affinity", params={"page": 1, "page_size": 10})
        item = [i for i in resp.json()["items"] if i["affinity_id"] == "ca-abc123"][0]
        assert item["top_process_classes"] == ["ADI", "ADPF"]
        assert item["pair_delta_vs_minister"] == 0.35
