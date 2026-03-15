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
    ServingDonationEvent,
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
                session.add(
                    ServingDonationEvent(
                        event_id="de-evt1",
                        match_id="dm-abc123",
                        election_year=2022,
                        donation_amount=30000.0,
                        candidate_name="FULANO",
                        party_abbrev="PT",
                        position="SENADOR",
                        state="SP",
                        donor_name="ACME CORP",
                        donor_cpf_cnpj="12345678000199",
                        donation_description="Doacao em dinheiro",
                    )
                )
                session.add(
                    ServingDonationEvent(
                        event_id="de-evt2",
                        match_id="dm-abc123",
                        election_year=2018,
                        donation_amount=20000.0,
                        candidate_name="CICLANO",
                        party_abbrev="MDB",
                        position="DEPUTADO",
                        state="RJ",
                        donor_name="ACME CORP",
                        donor_cpf_cnpj="12345678000199",
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

    def test_donation_events(self, client: TestClient) -> None:
        resp = client.get("/donations/dm-abc123/events", params={"page": 1, "page_size": 10})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert len(data["items"]) == 2
        # Ordered by year desc
        assert data["items"][0]["election_year"] == 2022
        assert data["items"][0]["donation_amount"] == 30000.0
        assert data["items"][1]["election_year"] == 2018

    def test_donation_events_empty(self, client: TestClient) -> None:
        resp = client.get("/donations/nonexistent/events")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 0
        assert data["items"] == []

    def test_donation_match_has_audit_fields(self, client: TestClient) -> None:
        """P4: audit fields should be exposed in the API response."""
        resp = client.get("/donations", params={"page": 1, "page_size": 10})
        data = resp.json()
        item = data["items"][0]
        # These fields exist in the schema even if null
        assert "favorable_rate_substantive" in item
        assert "substantive_decision_count" in item
        assert "red_flag_substantive" in item
        assert "matched_alias" in item
        assert "matched_tax_id" in item
        assert "uncertainty_note" in item
        assert "entity_id" in item
        assert "donor_name_normalized" in item
        assert "donor_name_originator" in item

    def test_entity_id_does_not_break_entity_type_filter(self, client: TestClient) -> None:
        """P5: entity_id column must not break entity_type filters or serialization."""
        for et in ("party", "counsel"):
            resp = client.get("/donations", params={"page": 1, "page_size": 10, "entity_type": et})
            assert resp.status_code == 200
            data = resp.json()
            for item in data["items"]:
                assert item["entity_type"] == et
                assert item["entity_id"]  # never empty

    def test_counsel_entity_id_equals_counsel_id(self, client: TestClient) -> None:
        """P5: for counsel matches, entity_id and counsel_id must be the same."""
        resp = client.get("/donations", params={"entity_type": "counsel"})
        data = resp.json()
        for item in data["items"]:
            assert item["counsel_id"] == item["entity_id"]

    def test_donation_events_pagination(self, client: TestClient) -> None:
        """P3: events endpoint must paginate correctly."""
        # Page 1, size 1 — should get 1 item, total 2
        resp = client.get("/donations/dm-abc123/events", params={"page": 1, "page_size": 1})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert len(data["items"]) == 1
        assert data["page"] == 1

        # Page 2, size 1 — should get 1 item
        resp2 = client.get("/donations/dm-abc123/events", params={"page": 2, "page_size": 1})
        data2 = resp2.json()
        assert len(data2["items"]) == 1
        assert data2["items"][0]["event_id"] != data["items"][0]["event_id"]

    def test_donation_events_only_returns_own_match(self, client: TestClient) -> None:
        """P3: events endpoint must only return events for the given match_id."""
        resp = client.get("/donations/dm-abc123/events")
        data = resp.json()
        for item in data["items"]:
            assert item["match_id"] == "dm-abc123"

        resp2 = client.get("/donations/dm-def456/events")
        assert resp2.json()["total"] == 0
