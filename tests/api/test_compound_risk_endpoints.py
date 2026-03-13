"""Tests for compound risk API endpoints."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from atlas_stf.api.app import create_app
from atlas_stf.serving.models import Base, ServingCompoundRisk, ServingMetric, ServingSchemaMeta
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
                        ServingCompoundRisk(
                            pair_id="cr-party",
                            minister_name="MIN. TESTE",
                            entity_type="party",
                            entity_id="p1",
                            entity_name="AUTOR A",
                            signal_count=4,
                            signals_json=json.dumps(["alert", "corporate", "donation", "sanction"]),
                            red_flag=True,
                            shared_process_count=2,
                            shared_process_ids_json=json.dumps(["proc_1", "proc_2"]),
                            alert_count=1,
                            alert_ids_json=json.dumps(["alert-1"]),
                            max_alert_score=0.92,
                            max_rate_delta=0.33,
                            sanction_match_count=1,
                            sanction_sources_json=json.dumps(["CGU"]),
                            donation_match_count=1,
                            donation_total_brl=100000.0,
                            corporate_conflict_count=1,
                            corporate_conflict_ids_json=json.dumps(["cn1"]),
                            corporate_companies_json=json.dumps(
                                [{"company_cnpj_basico": "12345678", "company_name": "EMPRESA X", "link_degree": 1}]
                            ),
                            affinity_count=0,
                            affinity_ids_json=json.dumps([]),
                            top_process_classes_json=json.dumps([]),
                            supporting_party_ids_json=json.dumps([]),
                            supporting_party_names_json=json.dumps([]),
                            generated_at=datetime.now(timezone.utc),
                        ),
                        ServingCompoundRisk(
                            pair_id="cr-counsel",
                            minister_name="MIN. TESTE",
                            entity_type="counsel",
                            entity_id="c1",
                            entity_name="ADV SILVA",
                            signal_count=3,
                            signals_json=json.dumps(["affinity", "alert", "donation"]),
                            red_flag=True,
                            shared_process_count=2,
                            shared_process_ids_json=json.dumps(["proc_1", "proc_2"]),
                            alert_count=1,
                            alert_ids_json=json.dumps(["alert-1"]),
                            max_alert_score=0.92,
                            max_rate_delta=0.21,
                            sanction_match_count=0,
                            sanction_sources_json=json.dumps([]),
                            donation_match_count=1,
                            donation_total_brl=100000.0,
                            corporate_conflict_count=0,
                            corporate_conflict_ids_json=json.dumps([]),
                            corporate_companies_json=json.dumps([]),
                            affinity_count=1,
                            affinity_ids_json=json.dumps(["ca1"]),
                            top_process_classes_json=json.dumps(["ADI"]),
                            supporting_party_ids_json=json.dumps(["p1"]),
                            supporting_party_names_json=json.dumps(["AUTOR A"]),
                            generated_at=datetime.now(timezone.utc),
                        ),
                        ServingCompoundRisk(
                            pair_id="cr-other",
                            minister_name="MIN. OUTRO",
                            entity_type="party",
                            entity_id="p2",
                            entity_name="REU B",
                            signal_count=1,
                            signals_json=json.dumps(["alert"]),
                            red_flag=False,
                            shared_process_count=1,
                            shared_process_ids_json=json.dumps(["proc_3"]),
                            alert_count=1,
                            alert_ids_json=json.dumps(["alert-2"]),
                            max_alert_score=0.61,
                            max_rate_delta=0.05,
                            sanction_match_count=0,
                            sanction_sources_json=json.dumps([]),
                            donation_match_count=0,
                            donation_total_brl=0.0,
                            corporate_conflict_count=0,
                            corporate_conflict_ids_json=json.dumps([]),
                            corporate_companies_json=json.dumps([]),
                            affinity_count=0,
                            affinity_ids_json=json.dumps([]),
                            top_process_classes_json=json.dumps([]),
                            supporting_party_ids_json=json.dumps([]),
                            supporting_party_names_json=json.dumps([]),
                            generated_at=datetime.now(timezone.utc),
                        ),
                    ]
                )
                session.add_all(
                    [
                        ServingMetric(key="alert_count", value_integer=0),
                        ServingMetric(key="avg_alert_score", value_float=0.0),
                        ServingMetric(key="valid_group_count", value_integer=0),
                        ServingMetric(key="baseline_count", value_integer=0),
                    ]
                )
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


class TestCompoundRiskEndpoints:
    def test_lists_pairs_ordered_by_signal_count(self, client: TestClient) -> None:
        response = client.get("/compound-risk", params={"page": 1, "page_size": 10})
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 3
        assert data["items"][0]["pair_id"] == "cr-party"
        assert data["items"][1]["pair_id"] == "cr-counsel"

    def test_filters_by_minister_and_entity_type(self, client: TestClient) -> None:
        response = client.get(
            "/compound-risk",
            params={"page": 1, "page_size": 10, "minister": "TESTE", "entity_type": "counsel"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["entity_id"] == "c1"

    def test_filter_minister_treats_percent_as_literal(self, client: TestClient) -> None:
        response = client.get(
            "/compound-risk",
            params={"page": 1, "page_size": 10, "minister": "%"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 0
        assert data["items"] == []

    def test_red_flags_endpoint_only_returns_compound_flags(self, client: TestClient) -> None:
        response = client.get("/compound-risk/red-flags")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 2
        assert {item["pair_id"] for item in data["items"]} == {"cr-party", "cr-counsel"}

    def test_red_flags_endpoint_honors_limit(self, client: TestClient) -> None:
        response = client.get("/compound-risk/red-flags", params={"limit": 1})
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 2
        assert len(data["items"]) == 1

    def test_heatmap_endpoint_materializes_cells(self, client: TestClient) -> None:
        response = client.get("/compound-risk/heatmap", params={"limit": 5})
        assert response.status_code == 200
        data = response.json()
        assert data["pair_count"] == 3
        assert data["ministers"] == ["MIN. TESTE", "MIN. OUTRO"]
        assert data["entities"][0]["entity_id"] == "p1"
        top_cell = next(cell for cell in data["cells"] if cell["pair_id"] == "cr-party")
        assert top_cell["signal_count"] == 4
        assert top_cell["signals"] == ["alert", "corporate", "donation", "sanction"]
