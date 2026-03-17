"""Tests for sanction corporate links API endpoints."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from atlas_stf.api.app import create_app
from atlas_stf.serving.models import (
    Base,
    ServingMetric,
    ServingSanctionCorporateLink,
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
                session.add_all(
                    [
                        ServingSanctionCorporateLink(
                            link_id="scl-abc123",
                            sanction_id="s1",
                            sanction_source="ceis",
                            sanction_entity_name="EMPRESA RUIM",
                            sanction_entity_tax_id="12345678000195",
                            sanction_type="inidoneidade",
                            bridge_company_cnpj_basico="12345678",
                            bridge_company_name="EMPRESA RUIM LTDA",
                            bridge_link_basis="exact_cnpj_basico",
                            bridge_confidence="deterministic",
                            stf_entity_type="party",
                            stf_entity_id="p1",
                            stf_entity_name="JOAO DA SILVA",
                            stf_match_strategy="tax_id",
                            stf_match_score=1.0,
                            stf_match_confidence="deterministic",
                            link_degree=2,
                            stf_process_count=5,
                            favorable_rate=0.8,
                            baseline_favorable_rate=0.5,
                            favorable_rate_delta=0.3,
                            risk_score=0.3,
                            red_flag=True,
                            red_flag_power=0.85,
                            red_flag_confidence="high",
                            evidence_chain_json='["Sancao CEIS: EMPRESA RUIM", "Co-socio JOAO DA SILVA"]',
                            source_datasets_json='["ceis", "rfb_socios"]',
                            record_hash="abc123hash",
                            generated_at=datetime(2026, 3, 15, tzinfo=timezone.utc),
                        ),
                        ServingSanctionCorporateLink(
                            link_id="scl-def456",
                            sanction_id="s2",
                            sanction_source="cvm",
                            sanction_entity_name="EMPRESA IRREGULAR",
                            sanction_entity_tax_id="99887766000155",
                            sanction_type="multa",
                            bridge_company_cnpj_basico="99887766",
                            bridge_company_name="EMPRESA IRREGULAR SA",
                            bridge_link_basis="exact_cnpj_basico",
                            bridge_confidence="deterministic",
                            stf_entity_type="party",
                            stf_entity_id="p2",
                            stf_entity_name="MARIA OLIVEIRA",
                            stf_match_strategy="canonical_name",
                            stf_match_score=0.95,
                            stf_match_confidence="exact_name",
                            link_degree=3,
                            stf_process_count=2,
                            favorable_rate=0.4,
                            baseline_favorable_rate=0.5,
                            favorable_rate_delta=-0.1,
                            risk_score=0.05,
                            red_flag=False,
                            evidence_chain_json='["Sancao CVM: EMPRESA IRREGULAR"]',
                            source_datasets_json='["cvm", "rfb_socios"]',
                            record_hash="def456hash",
                            generated_at=datetime(2026, 3, 15, tzinfo=timezone.utc),
                        ),
                        ServingSchemaMeta(
                            singleton_key="serving",
                            schema_version=1,
                            schema_fingerprint="test",
                            built_at=datetime.now(timezone.utc),
                        ),
                        ServingMetric(key="alert_count", value_integer=0),
                    ]
                )
                session.flush()

    app = create_app(database_url=db_url)
    with TestClient(app) as tc:
        yield tc


class TestSanctionCorporateLinksEndpoints:
    def test_list_paginated(self, client: TestClient) -> None:
        resp = client.get("/sanction-corporate-links", params={"page": 1, "page_size": 10})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert len(data["items"]) == 2

    def test_filter_sanction_source(self, client: TestClient) -> None:
        resp = client.get("/sanction-corporate-links", params={"sanction_source": "ceis"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert data["items"][0]["sanction_source"] == "ceis"
        assert data["items"][0]["link_id"] == "scl-abc123"

    def test_filter_red_flag(self, client: TestClient) -> None:
        resp = client.get("/sanction-corporate-links", params={"red_flag_only": True})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert all(item["red_flag"] is True for item in data["items"])

    def test_filter_degree(self, client: TestClient) -> None:
        resp = client.get("/sanction-corporate-links", params={"min_degree": 3, "max_degree": 3})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert data["items"][0]["link_degree"] == 3
        assert data["items"][0]["link_id"] == "scl-def456"

    def test_red_flags_endpoint(self, client: TestClient) -> None:
        resp = client.get("/sanction-corporate-links/red-flags")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert all(item["red_flag"] is True for item in data["items"])
        assert data["items"][0]["link_id"] == "scl-abc123"

    def test_party_links(self, client: TestClient) -> None:
        resp = client.get("/parties/p1/sanction-corporate-links")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["stf_entity_id"] == "p1"
        assert data[0]["link_id"] == "scl-abc123"

    def test_party_links_empty(self, client: TestClient) -> None:
        resp = client.get("/parties/unknown/sanction-corporate-links")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_response_schema_fields(self, client: TestClient) -> None:
        resp = client.get("/sanction-corporate-links", params={"page": 1, "page_size": 1})
        assert resp.status_code == 200
        item = resp.json()["items"][0]
        assert item["evidence_chain"] == ["Sancao CEIS: EMPRESA RUIM", "Co-socio JOAO DA SILVA"]
        assert item["source_datasets"] == ["ceis", "rfb_socios"]
        assert item["red_flag_power"] == 0.85
        assert item["red_flag_confidence"] == "high"
        assert item["bridge_confidence"] == "deterministic"
