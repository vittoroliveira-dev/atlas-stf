"""Tests for payment counterparties API endpoints."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from atlas_stf.api.app import create_app
from atlas_stf.serving.models import (
    Base,
    ServingMetric,
    ServingPaymentCounterparty,
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
                    ServingPaymentCounterparty(
                        counterparty_id="pc-aaa111",
                        counterparty_identity_key="tax:12345678000190",
                        identity_basis="tax_id",
                        counterparty_name="FORNECEDOR ALPHA",
                        counterparty_tax_id="12345678000190",
                        counterparty_tax_id_normalized="12345678000190",
                        counterparty_document_type="cnpj",
                        total_received_brl=50000.0,
                        payment_count=10,
                        election_years_json=json.dumps([2020, 2022]),
                        payer_parties_json=json.dumps(["PT", "MDB"]),
                        payer_actor_type="party_org",
                        first_payment_date="2020-03-01",
                        last_payment_date="2022-11-15",
                        states_json=json.dumps(["SP", "RJ"]),
                        cnae_codes_json=json.dumps(["4110700"]),
                        provenance_json=json.dumps(
                            {
                                "source_file_count": 3,
                                "ingest_run_count": 2,
                                "first_collected_at": "2026-01-01T00:00:00",
                                "last_collected_at": "2026-03-01T00:00:00",
                            }
                        ),
                        generated_at=datetime.now(timezone.utc),
                    )
                )
                session.add(
                    ServingPaymentCounterparty(
                        counterparty_id="pc-bbb222",
                        counterparty_identity_key="name:FORNECEDOR BETA",
                        identity_basis="name_fallback",
                        counterparty_name="FORNECEDOR BETA",
                        counterparty_tax_id="",
                        counterparty_tax_id_normalized="",
                        counterparty_document_type="",
                        total_received_brl=10000.0,
                        payment_count=3,
                        election_years_json=json.dumps([2022]),
                        payer_parties_json=json.dumps(["PSDB"]),
                        payer_actor_type="party_org",
                        states_json=json.dumps(["MG"]),
                        generated_at=datetime.now(timezone.utc),
                    )
                )
                session.add(
                    ServingPaymentCounterparty(
                        counterparty_id="pc-ccc333",
                        counterparty_identity_key="tax:98765432000100",
                        identity_basis="tax_id",
                        counterparty_name="FORNECEDOR GAMMA",
                        counterparty_tax_id="98765432000100",
                        counterparty_tax_id_normalized="98765432000100",
                        counterparty_document_type="cnpj",
                        total_received_brl=25000.0,
                        payment_count=5,
                        payer_actor_type="party_org",
                        generated_at=datetime.now(timezone.utc),
                    )
                )
                session.add(
                    ServingSchemaMeta(
                        singleton_key="serving",
                        schema_version=10,
                        schema_fingerprint="test",
                        built_at=datetime.now(timezone.utc),
                    )
                )
                session.add(ServingMetric(key="alert_count", value_integer=0))

        app = create_app(database_url=db_url)
        yield TestClient(app)


class TestPagination:
    """Pagination works correctly."""

    def test_page_1(self, client: TestClient) -> None:
        resp = client.get("/payment-counterparties?page=1&page_size=2")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 3
        assert data["page"] == 1
        assert data["page_size"] == 2
        assert len(data["items"]) == 2

    def test_page_2(self, client: TestClient) -> None:
        resp = client.get("/payment-counterparties?page=2&page_size=2")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["items"]) == 1


class TestResponseSchema:
    """Response contains all expected fields."""

    def test_full_schema(self, client: TestClient) -> None:
        resp = client.get("/payment-counterparties?page=1&page_size=1")
        assert resp.status_code == 200
        item = resp.json()["items"][0]
        assert item["counterparty_id"] == "pc-aaa111"
        assert item["counterparty_identity_key"] == "tax:12345678000190"
        assert item["identity_basis"] == "tax_id"
        assert item["counterparty_name"] == "FORNECEDOR ALPHA"
        assert item["counterparty_tax_id"] == "12345678000190"
        assert item["counterparty_tax_id_normalized"] == "12345678000190"
        assert item["counterparty_document_type"] == "cnpj"
        assert item["total_received_brl"] == 50000.0
        assert item["payment_count"] == 10
        assert item["election_years"] == [2020, 2022]
        assert item["payer_parties"] == ["PT", "MDB"]
        assert item["payer_actor_type"] == "party_org"
        assert item["first_payment_date"] == "2020-03-01"
        assert item["last_payment_date"] == "2022-11-15"
        assert item["states"] == ["SP", "RJ"]
        assert item["cnae_codes"] == ["4110700"]
        assert item["provenance"]["source_file_count"] == 3


class TestOrdering:
    """Results ordered by total_received_brl DESC."""

    def test_order_by_amount_desc(self, client: TestClient) -> None:
        resp = client.get("/payment-counterparties?page=1&page_size=10")
        assert resp.status_code == 200
        items = resp.json()["items"]
        amounts = [it["total_received_brl"] for it in items]
        assert amounts == sorted(amounts, reverse=True)
