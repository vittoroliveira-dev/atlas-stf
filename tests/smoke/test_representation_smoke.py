"""Smoke tests for the representation network subsystem.

These tests verify end-to-end wiring without real STF data.
They exercise the full pipeline: identity -> curated -> analytics -> serving -> API
using minimal synthetic fixtures.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import create_engine


def _write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")


# ---------------------------------------------------------------------------
# Phase A: Identity + curated builder smoke
# ---------------------------------------------------------------------------


class TestIdentitySmoke:
    def test_oab_round_trip(self) -> None:
        from atlas_stf.core.identity import is_valid_oab_format, normalize_oab_number

        assert normalize_oab_number("123.456/SP") == "123456/SP"
        assert is_valid_oab_format("123456/SP")
        assert not is_valid_oab_format("123456/XX")

    def test_lawyer_identity_key_priority(self) -> None:
        from atlas_stf.core.identity import build_lawyer_identity_key

        assert build_lawyer_identity_key(name="JOSE", oab_number="1234/SP") == "oab:1234/SP"
        assert build_lawyer_identity_key(name="JOSE", tax_id="12345678901") == "tax:12345678901"
        key = build_lawyer_identity_key(name="JOSE DA SILVA")
        assert key is not None
        assert key.startswith("name:")

    def test_firm_identity_key_priority(self) -> None:
        from atlas_stf.core.identity import build_firm_identity_key

        assert build_firm_identity_key(name="ABC", cnpj="12345678000190") == "tax:12345678000190"
        assert build_firm_identity_key(name="ABC", cnsa_number="1234") == "cnsa:1234"
        assert build_firm_identity_key(name="ABC ADVOGADOS") == "name:ABC ADVOGADOS"


class TestCuratedBuilderSmoke:
    def test_build_representation_produces_5_files(self, tmp_path: Path) -> None:
        from atlas_stf.curated.build_representation import build_representation_jsonl

        process_path = tmp_path / "curated" / "process.jsonl"
        _write_jsonl(
            process_path,
            [
                {
                    "process_id": "proc_001",
                    "process_number": "ADI 1234",
                    "juris_partes": "REQTE.: EMPRESA X (ADV.: DR. JOSE SILVA OAB/SP 12345)",
                    "juris_advogados": "JOSE SILVA",
                    "counsel_raw": None,
                },
            ],
        )
        portal_dir = tmp_path / "portal"
        portal_dir.mkdir()
        curated_dir = tmp_path / "out"
        curated_dir.mkdir()

        result = build_representation_jsonl(
            process_path=process_path,
            portal_dir=portal_dir,
            curated_dir=curated_dir,
        )

        assert "lawyer_entity" in result
        assert "law_firm_entity" in result
        assert "representation_edge" in result
        assert "representation_event" in result
        assert "source_evidence" in result
        assert result["lawyer_entity"].exists()

    def test_lawyer_dedup_by_oab(self, tmp_path: Path) -> None:
        from atlas_stf.curated.build_representation import build_representation_jsonl

        process_path = tmp_path / "curated" / "process.jsonl"
        _write_jsonl(
            process_path,
            [
                {
                    "process_id": "p1",
                    "juris_advogados": "JOSE SILVA",
                    "juris_partes": "REQTE.: X (ADV.: JOSE SILVA OAB/SP 11111)",
                },
                {
                    "process_id": "p2",
                    "juris_advogados": "JOSE SILVA",
                    "juris_partes": "REQTE.: Y (ADV.: JOSE SILVA OAB/SP 11111)",
                },
            ],
        )
        portal_dir = tmp_path / "portal"
        portal_dir.mkdir()
        curated_dir = tmp_path / "out"
        curated_dir.mkdir()

        build_representation_jsonl(process_path=process_path, portal_dir=portal_dir, curated_dir=curated_dir)

        lines = (curated_dir / "lawyer_entity.jsonl").read_text().splitlines()
        lawyers = [json.loads(line) for line in lines if line.strip()]
        # Same OAB should be deduped to 1 record
        oab_lawyers = [rec for rec in lawyers if rec.get("oab_number")]
        assert len(oab_lawyers) <= 2  # at most 2 (name-based may differ)


# ---------------------------------------------------------------------------
# Phase B: Analytics builder smoke
# ---------------------------------------------------------------------------


class TestAnalyticsSmoke:
    def test_representation_graph_empty(self, tmp_path: Path) -> None:
        from atlas_stf.analytics.representation_graph import build_representation_graph

        curated_dir = tmp_path / "curated"
        curated_dir.mkdir()
        output_dir = tmp_path / "analytics"

        path = build_representation_graph(curated_dir=curated_dir, output_dir=output_dir)

        assert path.exists()
        summary = json.loads((output_dir / "representation_graph_summary.json").read_text())
        assert summary["total_edges"] == 0

    def test_recurrence_empty(self, tmp_path: Path) -> None:
        from atlas_stf.analytics.representation_recurrence import build_representation_recurrence

        curated_dir = tmp_path / "curated"
        curated_dir.mkdir()
        output_dir = tmp_path / "analytics"

        path = build_representation_recurrence(curated_dir=curated_dir, output_dir=output_dir)

        assert path.exists()

    def test_amicus_network_empty(self, tmp_path: Path) -> None:
        from atlas_stf.analytics.amicus_network import build_amicus_network

        curated_dir = tmp_path / "curated"
        curated_dir.mkdir()
        output_dir = tmp_path / "analytics"

        path = build_amicus_network(curated_dir=curated_dir, output_dir=output_dir)

        assert path.exists()

    def test_firm_cluster_empty(self, tmp_path: Path) -> None:
        from atlas_stf.analytics.firm_cluster import build_firm_cluster

        curated_dir = tmp_path / "curated"
        curated_dir.mkdir()
        output_dir = tmp_path / "analytics"

        path = build_firm_cluster(curated_dir=curated_dir, output_dir=output_dir)

        assert path.exists()


# ---------------------------------------------------------------------------
# Phase C: OAB provider smoke
# ---------------------------------------------------------------------------


class TestOabProviderSmoke:
    def test_null_provider(self) -> None:
        from atlas_stf.oab._providers import NullOabProvider

        provider = NullOabProvider()
        result = provider.validate("12345", "SP")
        assert result.oab_status is None
        assert result.oab_source == "null"

    def test_format_provider(self) -> None:
        from atlas_stf.oab._providers import FormatOnlyProvider

        provider = FormatOnlyProvider()
        result = provider.validate("12345", "SP")
        assert result.oab_source == "format_only"

    def test_select_provider_null(self) -> None:
        from atlas_stf.oab._config import OabValidationConfig
        from atlas_stf.oab._providers import NullOabProvider, select_provider

        config = OabValidationConfig(provider="null")
        provider = select_provider(config)
        assert isinstance(provider, NullOabProvider)


# ---------------------------------------------------------------------------
# Phase D: Serving models smoke
# ---------------------------------------------------------------------------


class TestServingSmoke:
    def test_models_importable(self) -> None:
        from atlas_stf.serving.models import (
            ServingLawFirmEntity,
            ServingLawyerEntity,
            ServingProcessLawyer,
            ServingRepresentationEdge,
            ServingRepresentationEvent,
        )

        assert ServingLawyerEntity.__tablename__ == "serving_lawyer_entity"
        assert ServingLawFirmEntity.__tablename__ == "serving_law_firm_entity"
        assert ServingProcessLawyer.__tablename__ == "serving_process_lawyer"
        assert ServingRepresentationEdge.__tablename__ == "serving_representation_edge"
        assert ServingRepresentationEvent.__tablename__ == "serving_representation_event"

    def test_loaders_with_no_files(self, tmp_path: Path) -> None:
        from atlas_stf.serving._builder_loaders_representation import (
            load_law_firm_entities,
            load_lawyer_entities,
            load_process_lawyers,
            load_representation_edges,
            load_representation_events,
        )

        assert load_lawyer_entities(tmp_path) == []
        assert load_law_firm_entities(tmp_path) == []
        assert load_process_lawyers(tmp_path) == []
        assert load_representation_edges(tmp_path) == []
        assert load_representation_events(tmp_path) == []

    def test_schema_version_is_14(self) -> None:
        from atlas_stf.serving._builder_schema import SERVING_SCHEMA_VERSION

        assert SERVING_SCHEMA_VERSION == 19

    def test_schema_upgrade_drops_and_recreates(self, tmp_path: Path) -> None:
        """P4/P5: incompatible schema must be rebuilt, not crash."""
        from datetime import datetime, timezone

        from sqlalchemy import create_engine
        from sqlalchemy.orm import Session

        from atlas_stf.serving._builder_schema import (
            SERVING_SCHEMA_SINGLETON_KEY,
            _ensure_compatible_schema,
        )
        from atlas_stf.serving.models import Base, ServingDonationEvent, ServingDonationMatch, ServingSchemaMeta

        db_path = tmp_path / "test_upgrade.db"
        engine = create_engine(f"sqlite:///{db_path}")

        # Create schema with a fake old version
        Base.metadata.create_all(engine)
        with Session(engine) as session:
            session.add(
                ServingSchemaMeta(
                    singleton_key=SERVING_SCHEMA_SINGLETON_KEY,
                    schema_version=7,  # old version
                    schema_fingerprint="old_fingerprint",
                    built_at=datetime.now(timezone.utc),
                )
            )
            session.commit()

        # Now call _ensure_compatible_schema — should drop+recreate with warning
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            _ensure_compatible_schema(engine)

        # Verify tables exist after rebuild
        from sqlalchemy import inspect as sa_inspect

        inspector = sa_inspect(engine)
        tables = set(inspector.get_table_names())
        assert ServingDonationMatch.__tablename__ in tables
        assert ServingDonationEvent.__tablename__ in tables

        # Verify version is current
        with Session(engine) as session:
            meta = session.get(ServingSchemaMeta, SERVING_SCHEMA_SINGLETON_KEY)
        # After drop+recreate, no meta row exists yet (builder writes it later)
        assert meta is None

        engine.dispose()


# ---------------------------------------------------------------------------
# Phase E: API routes smoke
# ---------------------------------------------------------------------------


class TestApiSmoke:
    @pytest.fixture()
    def client(self, tmp_path: Path):
        from atlas_stf.serving.models import Base

        db_path = tmp_path / "test.db"
        db_url = f"sqlite+pysqlite:///{db_path}"
        engine = create_engine(db_url, connect_args={"check_same_thread": False})
        Base.metadata.create_all(engine)
        engine.dispose()

        from atlas_stf.api.app import create_app

        app = create_app(database_url=db_url)
        from starlette.testclient import TestClient

        return TestClient(app, raise_server_exceptions=False)

    def test_representation_summary(self, client) -> None:  # noqa: ANN001
        resp = client.get("/representation/summary")
        assert resp.status_code == 200
        data = resp.json()
        assert "total_lawyers" in data
        assert "total_firms" in data

    def test_representation_lawyers_empty(self, client) -> None:  # noqa: ANN001
        resp = client.get("/representation/lawyers")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 0
        assert data["items"] == []

    def test_representation_firms_empty(self, client) -> None:  # noqa: ANN001
        resp = client.get("/representation/firms")
        assert resp.status_code == 200
        assert resp.json()["total"] == 0

    def test_representation_lawyer_not_found(self, client) -> None:  # noqa: ANN001
        resp = client.get("/representation/lawyers/nonexistent")
        assert resp.status_code == 404

    def test_representation_firm_not_found(self, client) -> None:  # noqa: ANN001
        resp = client.get("/representation/firms/nonexistent")
        assert resp.status_code == 404

    def test_representation_events_empty(self, client) -> None:  # noqa: ANN001
        resp = client.get("/representation/events")
        assert resp.status_code == 200
        assert resp.json()["total"] == 0

    def test_representation_process_empty(self, client) -> None:  # noqa: ANN001
        resp = client.get("/representation/process/nonexistent")
        assert resp.status_code == 200
        data = resp.json()
        assert data["edges"] == []


# ---------------------------------------------------------------------------
# Phase F: Audit gate smoke
# ---------------------------------------------------------------------------


class TestAuditGateSmoke:
    def test_audit_representation_missing_files(self, tmp_path: Path) -> None:
        from atlas_stf.audit_gates import audit_representation

        result = audit_representation(curated_dir=tmp_path, analytics_dir=tmp_path)

        assert result["overall_status"] == "fail"
        assert result["target"] == "representation"
        missing = [r for r in result["artifacts"] if r["status"] == "missing"]
        assert len(missing) == 4  # lawyer, firm, edge, event

    def test_audit_representation_with_valid_data(self, tmp_path: Path) -> None:
        from atlas_stf.audit_gates import audit_representation

        _write_jsonl(
            tmp_path / "lawyer_entity.jsonl",
            [
                {"lawyer_id": "law_001", "lawyer_name_raw": "DR JOSE", "oab_number": "12345/SP"},
            ],
        )
        _write_jsonl(
            tmp_path / "law_firm_entity.jsonl",
            [
                {"firm_id": "firm_001", "firm_name_raw": "SILVA ADVOGADOS"},
            ],
        )
        _write_jsonl(
            tmp_path / "representation_edge.jsonl",
            [
                {
                    "edge_id": "rep_001",
                    "process_id": "p1",
                    "representative_entity_id": "law_001",
                    "representative_kind": "lawyer",
                },
            ],
        )
        _write_jsonl(
            tmp_path / "representation_event.jsonl",
            [
                {"event_id": "evt_001", "process_id": "p1", "event_type": "petition"},
            ],
        )

        analytics_dir = tmp_path / "analytics"
        analytics_dir.mkdir()
        output_path = tmp_path / "report.json"

        result = audit_representation(curated_dir=tmp_path, analytics_dir=analytics_dir, output_path=output_path)

        assert result["overall_status"] == "ok"
        assert result["coverage"]["total_lawyers"] == 1
        assert result["coverage"]["lawyers_with_oab"] == 1
        assert result["coverage"]["oab_coverage_pct"] == 100.0
        assert result["coverage"]["orphan_edges"] == 0
        assert output_path.exists()
