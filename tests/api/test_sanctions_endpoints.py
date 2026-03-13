"""Tests for sanctions API endpoints."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from atlas_stf.api.app import create_app
from atlas_stf.serving.models import (
    Base,
    ServingCounselSanctionProfile,
    ServingMetric,
    ServingSanctionMatch,
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
                    ServingSanctionMatch(
                        match_id="sm-abc123",
                        party_id="p1",
                        party_name_normalized="ACME CORP",
                        sanction_source="ceis",
                        sanction_id="100",
                        sanctioning_body="CGU",
                        sanction_type="Inidoneidade",
                        sanction_start_date="2020-01-01",
                        sanction_end_date=None,
                        sanction_description="Test sanction",
                        stf_case_count=5,
                        favorable_rate=0.8,
                        baseline_favorable_rate=0.5,
                        favorable_rate_delta=0.3,
                        red_flag=True,
                        matched_at=datetime.now(timezone.utc),
                    )
                )
                session.add(
                    ServingSanctionMatch(
                        match_id="sm-def456",
                        party_id="p2",
                        party_name_normalized="XYZ LTDA",
                        sanction_source="cnep",
                        sanction_id="200",
                        stf_case_count=2,
                        red_flag=False,
                    )
                )
                session.add(
                    ServingSanctionMatch(
                        match_id="sm-ghi789",
                        party_id="p3",
                        party_name_normalized="OMEGA SA",
                        sanction_source="ceis",
                        sanction_id="300",
                        sanctioning_body="CGU",
                        sanction_type="Suspensão",
                        sanction_start_date="2021-01-01",
                        sanction_end_date=None,
                        sanction_description="Second red flag",
                        stf_case_count=3,
                        favorable_rate=0.75,
                        baseline_favorable_rate=0.5,
                        favorable_rate_delta=0.25,
                        red_flag=True,
                        matched_at=datetime.now(timezone.utc),
                    )
                )
                session.add(
                    ServingSanctionMatch(
                        match_id="sm-counsel1",
                        entity_type="counsel",
                        party_id="c10",
                        party_name_normalized="ADV SANTOS",
                        sanction_source="ceis",
                        sanction_id="400",
                        stf_case_count=3,
                        favorable_rate=0.7,
                        baseline_favorable_rate=0.5,
                        favorable_rate_delta=0.2,
                        red_flag=True,
                    )
                )
                session.add(
                    ServingCounselSanctionProfile(
                        counsel_id="c1",
                        counsel_name_normalized="ADV SILVA",
                        sanctioned_client_count=2,
                        total_client_count=10,
                        sanctioned_client_rate=0.2,
                        sanctioned_favorable_rate=0.9,
                        overall_favorable_rate=0.6,
                        red_flag=True,
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


class TestSanctionsEndpoints:
    def test_list_sanctions(self, client: TestClient) -> None:
        resp = client.get("/sanctions", params={"page": 1, "page_size": 10})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 4
        assert len(data["items"]) == 4

    def test_filter_by_source(self, client: TestClient) -> None:
        resp = client.get("/sanctions", params={"page": 1, "page_size": 10, "source": "ceis"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 3
        assert {item["sanction_source"] for item in data["items"]} == {"ceis"}

    def test_filter_by_source_rejects_invalid_value(self, client: TestClient) -> None:
        resp = client.get("/sanctions", params={"page": 1, "page_size": 10, "source": "javascript"})
        assert resp.status_code == 422

    def test_filter_red_flag_only(self, client: TestClient) -> None:
        resp = client.get("/sanctions", params={"page": 1, "page_size": 10, "red_flag_only": True})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 3
        assert all(item["red_flag"] is True for item in data["items"])

    def test_filter_entity_type_party(self, client: TestClient) -> None:
        resp = client.get("/sanctions", params={"page": 1, "page_size": 10, "entity_type": "party"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 3
        assert all(item["entity_type"] == "party" for item in data["items"])

    def test_filter_entity_type_counsel(self, client: TestClient) -> None:
        resp = client.get("/sanctions", params={"page": 1, "page_size": 10, "entity_type": "counsel"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert data["items"][0]["entity_type"] == "counsel"
        assert data["items"][0]["match_id"] == "sm-counsel1"
        assert data["items"][0]["counsel_id"] == "c10"
        assert data["items"][0]["party_id"] == "c10"

    def test_red_flags(self, client: TestClient) -> None:
        resp = client.get("/sanctions/red-flags")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_party_flags"] == 3
        assert data["total_counsel_flags"] == 1
        assert {item["match_id"] for item in data["party_flags"]} == {"sm-abc123", "sm-ghi789", "sm-counsel1"}
        assert data["counsel_flags"][0]["counsel_id"] == "c1"

    def test_red_flags_honors_limit(self, client: TestClient) -> None:
        resp = client.get("/sanctions/red-flags", params={"limit": 1})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_party_flags"] == 3
        assert len(data["party_flags"]) == 1

    def test_party_sanctions(self, client: TestClient) -> None:
        resp = client.get("/parties/p1/sanctions")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["party_id"] == "p1"

    def test_party_sanctions_empty(self, client: TestClient) -> None:
        resp = client.get("/parties/unknown/sanctions")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_counsel_sanction_profile(self, client: TestClient) -> None:
        resp = client.get("/counsels/c1/sanction-profile")
        assert resp.status_code == 200
        data = resp.json()
        assert data["counsel_id"] == "c1"
        assert data["red_flag"] is True

    def test_counsel_sanction_profile_not_found(self, client: TestClient) -> None:
        resp = client.get("/counsels/unknown/sanction-profile")
        assert resp.status_code == 404
