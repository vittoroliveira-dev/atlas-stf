"""Tests for donations API endpoints."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from atlas_stf.api.app import create_app
from atlas_stf.serving.models import (
    Base,
    ServingCounselDonationProfile,
    ServingDonationMatch,
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
                    ServingDonationMatch(
                        match_id="dm-abc123",
                        party_id="p1",
                        party_name_normalized="ACME CORP",
                        donor_cpf_cnpj="12345678000199",
                        total_donated_brl=50000.0,
                        donation_count=3,
                        election_years_json=json.dumps([2018, 2022]),
                        parties_donated_to_json=json.dumps(["PT", "MDB"]),
                        candidates_donated_to_json=json.dumps(["FULANO"]),
                        positions_donated_to_json=json.dumps(["SENADOR"]),
                        stf_case_count=5,
                        favorable_rate=0.8,
                        baseline_favorable_rate=0.5,
                        favorable_rate_delta=0.3,
                        red_flag=True,
                        matched_at=datetime.now(timezone.utc),
                    )
                )
                session.add(
                    ServingDonationMatch(
                        match_id="dm-def456",
                        party_id="p2",
                        party_name_normalized="XYZ LTDA",
                        donor_cpf_cnpj="98765432000111",
                        total_donated_brl=10000.0,
                        donation_count=1,
                        stf_case_count=2,
                        red_flag=False,
                    )
                )
                session.add(
                    ServingDonationMatch(
                        match_id="dm-ghi789",
                        party_id="p3",
                        party_name_normalized="OMEGA SA",
                        donor_cpf_cnpj="11111111000111",
                        total_donated_brl=25000.0,
                        donation_count=2,
                        stf_case_count=4,
                        favorable_rate=0.7,
                        baseline_favorable_rate=0.5,
                        favorable_rate_delta=0.2,
                        red_flag=True,
                    )
                )
                session.add(
                    ServingDonationMatch(
                        match_id="dm-counsel1",
                        entity_type="counsel",
                        party_id="c10",
                        party_name_normalized="ADV SANTOS",
                        donor_cpf_cnpj="55555555000100",
                        total_donated_brl=5000.0,
                        donation_count=1,
                        stf_case_count=2,
                        red_flag=False,
                    )
                )
                session.add(
                    ServingCounselDonationProfile(
                        counsel_id="c1",
                        counsel_name_normalized="ADV SILVA",
                        donor_client_count=2,
                        total_client_count=10,
                        donor_client_rate=0.2,
                        donor_client_favorable_rate=0.9,
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


class TestDonationsEndpoints:
    def test_list_donations(self, client: TestClient) -> None:
        resp = client.get("/donations", params={"page": 1, "page_size": 10})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 4
        assert len(data["items"]) == 4

    def test_filter_red_flag_only(self, client: TestClient) -> None:
        resp = client.get("/donations", params={"page": 1, "page_size": 10, "red_flag_only": True})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert all(item["red_flag"] is True for item in data["items"])

    def test_donation_json_fields(self, client: TestClient) -> None:
        resp = client.get("/donations", params={"page": 1, "page_size": 10})
        data = resp.json()
        item = next(i for i in data["items"] if i["match_id"] == "dm-abc123")
        assert item["election_years"] == [2018, 2022]
        assert item["parties_donated_to"] == ["PT", "MDB"]

    def test_filter_entity_type_party(self, client: TestClient) -> None:
        resp = client.get("/donations", params={"page": 1, "page_size": 10, "entity_type": "party"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 3
        assert all(item["entity_type"] == "party" for item in data["items"])

    def test_filter_entity_type_counsel(self, client: TestClient) -> None:
        resp = client.get("/donations", params={"page": 1, "page_size": 10, "entity_type": "counsel"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert data["items"][0]["entity_type"] == "counsel"
        assert data["items"][0]["match_id"] == "dm-counsel1"
        assert data["items"][0]["counsel_id"] == "c10"
        assert data["items"][0]["party_id"] == "c10"

    def test_red_flags(self, client: TestClient) -> None:
        resp = client.get("/donations/red-flags")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_party_flags"] == 2
        assert data["total_counsel_flags"] == 1
        assert {item["match_id"] for item in data["party_flags"]} == {"dm-abc123", "dm-ghi789"}
        assert data["counsel_flags"][0]["counsel_id"] == "c1"

    def test_red_flags_honors_limit(self, client: TestClient) -> None:
        resp = client.get("/donations/red-flags", params={"limit": 1})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_party_flags"] == 2
        assert len(data["party_flags"]) == 1

    def test_party_donations(self, client: TestClient) -> None:
        resp = client.get("/parties/p1/donations")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["party_id"] == "p1"

    def test_party_donations_empty(self, client: TestClient) -> None:
        resp = client.get("/parties/unknown/donations")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_counsel_donation_profile(self, client: TestClient) -> None:
        resp = client.get("/counsels/c1/donation-profile")
        assert resp.status_code == 200
        data = resp.json()
        assert data["counsel_id"] == "c1"
        assert data["red_flag"] is True

    def test_counsel_donation_profile_not_found(self, client: TestClient) -> None:
        resp = client.get("/counsels/unknown/donation-profile")
        assert resp.status_code == 404
