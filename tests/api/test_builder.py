from __future__ import annotations

import json
import types

import pytest
from sqlalchemy import func, inspect, select, text
from sqlalchemy.orm import Session

from atlas_stf.serving._builder_loaders import load_cases
from atlas_stf.serving._builder_loaders_corporate import stream_economic_group_rows
from atlas_stf.serving.builder import (
    SERVING_SCHEMA_SINGLETON_KEY,
    SERVING_SCHEMA_VERSION,
    build_serving_database,
)
from atlas_stf.serving.models import (
    ServingAlert,
    ServingCase,
    ServingCompoundRisk,
    ServingCounsel,
    ServingMetric,
    ServingMinisterFlow,
    ServingParty,
    ServingProcessCounsel,
    ServingProcessParty,
    ServingSchemaMeta,
    ServingSourceAudit,
    ServingTemporalAnalysis,
)
from tests.api.conftest import managed_engine


def _write_jsonl(path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _write_json(path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _count(session: Session, model) -> int:
    return session.scalar(select(func.count()).select_from(model)) or 0


def _write_minimal_data(curated_dir, analytics_dir) -> None:
    """Write minimal valid data that passes critical table validation."""
    _write_jsonl(
        curated_dir / "process.jsonl",
        [
            {
                "process_id": "proc_1",
                "process_number": "ADI 1",
                "process_class": "ADI",
                "branch_of_law": "DIREITO ADMINISTRATIVO",
                "subjects_normalized": ["DIREITO ADMINISTRATIVO"],
                "origin_description": "DISTRITO FEDERAL",
                "juris_inteiro_teor_url": "https://example.com/adi1.pdf",
                "juris_doc_count": 2,
                "juris_has_acordao": True,
                "juris_has_decisao_monocratica": False,
            }
        ],
    )
    _write_jsonl(
        curated_dir / "decision_event.jsonl",
        [
            {
                "decision_event_id": "evt_1",
                "process_id": "proc_1",
                "decision_date": "2026-01-05",
                "current_rapporteur": "MIN. TESTE",
                "decision_type": "Decisao Final",
                "decision_progress": "Procedente",
                "decision_origin": "JULGAMENTO",
                "judging_body": "PLENO",
                "is_collegiate": True,
                "decision_note": "Decisao colegiada materializada.",
            }
        ],
    )
    _write_jsonl(
        curated_dir / "party.jsonl",
        [{"party_id": "party_1", "party_name_raw": "AUTOR A", "party_name_normalized": "AUTOR A", "notes": None}],
    )
    _write_jsonl(
        curated_dir / "process_party_link.jsonl",
        [
            {
                "link_id": "pp_1",
                "process_id": "proc_1",
                "party_id": "party_1",
                "role_in_case": "REQTE.(S)",
                "source_id": "juris",
            }
        ],
    )
    _write_jsonl(
        curated_dir / "counsel.jsonl",
        [
            {
                "counsel_id": "coun_1",
                "counsel_name_raw": "ADVOGADO A",
                "counsel_name_normalized": "ADVOGADO A",
                "notes": None,
            }
        ],
    )
    _write_jsonl(
        curated_dir / "process_counsel_link.jsonl",
        [
            {
                "link_id": "pc_1",
                "process_id": "proc_1",
                "counsel_id": "coun_1",
                "side_in_case": "REQTE.(S)",
                "source_id": "juris",
            }
        ],
    )
    _write_jsonl(
        analytics_dir / "outlier_alert.jsonl",
        [
            {
                "alert_id": "alert_1",
                "process_id": "proc_1",
                "decision_event_id": "evt_1",
                "comparison_group_id": "grp_1",
                "alert_type": "atipicidade",
                "alert_score": 0.87,
                "expected_pattern": "Esperado colegiado homogeneo.",
                "observed_pattern": "Observado desvio pontual.",
                "evidence_summary": "Desvio comparativo materializado.",
                "uncertainty_note": None,
                "status": "novo",
                "created_at": "2026-01-31T12:00:00",
                "updated_at": "2026-01-31T12:05:00",
            }
        ],
    )
    _write_json(analytics_dir / "outlier_alert_summary.json", {"alert_count": 1, "avg_score": 0.87})
    _write_json(analytics_dir / "comparison_group_summary.json", {"valid_group_count": 1})
    _write_json(analytics_dir / "baseline_summary.json", {"baseline_count": 1})


def test_build_serving_database_materializes_expected_rows(serving_db: str):
    with managed_engine(serving_db) as engine:
        with Session(engine) as session:
            assert _count(session, ServingCase) == 3
            assert _count(session, ServingAlert) == 1
            assert _count(session, ServingCounsel) == 2
            assert _count(session, ServingProcessCounsel) == 3
            assert _count(session, ServingParty) == 2
            assert _count(session, ServingProcessParty) == 2
            assert _count(session, ServingSchemaMeta) == 1
            assert _count(session, ServingSourceAudit) == 20
            assert _count(session, ServingMetric) == 4
            assert _count(session, ServingTemporalAnalysis) == 5
            assert _count(session, ServingMinisterFlow) > 0
            meta = session.get(ServingSchemaMeta, SERVING_SCHEMA_SINGLETON_KEY)
            assert meta is not None
            assert meta.schema_version == SERVING_SCHEMA_VERSION


def test_build_serving_database_materializes_minister_flow_lookup_rows(serving_db: str):
    with managed_engine(serving_db) as engine:
        with Session(engine) as session:
            minister_row = session.scalar(
                select(ServingMinisterFlow).where(
                    ServingMinisterFlow.period == "2026-01",
                    ServingMinisterFlow.collegiate_filter == "all",
                    ServingMinisterFlow.minister_name == "MIN. TESTE",
                    ServingMinisterFlow.judging_body.is_(None),
                    ServingMinisterFlow.process_class.is_(None),
                )
            )
            assert minister_row is not None
            assert minister_row.event_count == 1
            assert minister_row.minister_reference == "MIN. TESTE"

            aggregate_row = session.scalar(
                select(ServingMinisterFlow).where(
                    ServingMinisterFlow.period == "2026-01",
                    ServingMinisterFlow.collegiate_filter == "all",
                    ServingMinisterFlow.minister_name.is_(None),
                    ServingMinisterFlow.judging_body.is_(None),
                    ServingMinisterFlow.process_class.is_(None),
                )
            )
            assert aggregate_row is not None
            assert aggregate_row.event_count == 3


def test_build_serving_database_replaces_legacy_database(tmp_path):
    """Atomic publish replaces a pre-existing (legacy) database file."""
    legacy_db = tmp_path / "legacy-serving.db"
    legacy_url = f"sqlite+pysqlite:///{legacy_db}"
    with managed_engine(legacy_url) as engine:
        with engine.begin() as conn:
            conn.execute(text("create table serving_case (decision_event_id varchar(64) primary key)"))
            conn.execute(text("insert into serving_case (decision_event_id) values ('legacy_evt')"))

    curated_dir = tmp_path / "curated"
    analytics_dir = tmp_path / "analytics"
    _write_minimal_data(curated_dir, analytics_dir)

    build_serving_database(
        database_url=legacy_url,
        curated_dir=curated_dir,
        analytics_dir=analytics_dir,
    )

    with managed_engine(legacy_url) as rebuilt_engine:
        rebuilt_inspector = inspect(rebuilt_engine)
        assert "serving_schema_meta" in rebuilt_inspector.get_table_names()
        assert {column["name"] for column in rebuilt_inspector.get_columns("serving_case")} >= {
            "decision_event_id",
            "process_id",
        }
        with Session(rebuilt_engine) as session:
            assert _count(session, ServingCase) == 1
            assert session.get(ServingCase, "legacy_evt") is None
            meta = session.get(ServingSchemaMeta, SERVING_SCHEMA_SINGLETON_KEY)
            assert meta is not None
            assert meta.schema_version == SERVING_SCHEMA_VERSION


def test_load_cases_skips_rows_without_decision_event_id(tmp_path):
    curated_dir = tmp_path / "curated"
    _write_jsonl(
        curated_dir / "process.jsonl",
        [
            {
                "process_id": "proc_1",
                "process_number": "ADI 1",
                "process_class": "ADI",
                "branch_of_law": "DIREITO ADMINISTRATIVO",
                "subjects_normalized": ["DIREITO ADMINISTRATIVO"],
            }
        ],
    )
    _write_jsonl(
        curated_dir / "decision_event.jsonl",
        [
            {"decision_event_id": None, "process_id": "proc_1", "decision_date": "2026-01-05"},
            {"decision_event_id": "evt_1", "process_id": "proc_1", "decision_date": "2026-01-06"},
        ],
    )

    cases = load_cases(curated_dir)

    assert [case.decision_event_id for case in cases] == ["evt_1"]


def test_build_serving_database_rebuilds_compatible_schema_without_duplicate_metadata(serving_db: str, tmp_path):
    database_path = tmp_path / "serving.db"
    database_url = f"sqlite+pysqlite:///{database_path}"

    curated_dir = tmp_path / "curated"
    analytics_dir = tmp_path / "analytics"
    _write_minimal_data(curated_dir, analytics_dir)

    build_serving_database(database_url=database_url, curated_dir=curated_dir, analytics_dir=analytics_dir)
    build_serving_database(database_url=database_url, curated_dir=curated_dir, analytics_dir=analytics_dir)


def test_build_serving_database_loads_compound_risk_artifacts(tmp_path):
    database_path = tmp_path / "serving.db"
    database_url = f"sqlite+pysqlite:///{database_path}"
    curated_dir = tmp_path / "curated"
    analytics_dir = tmp_path / "analytics"
    _write_minimal_data(curated_dir, analytics_dir)

    _write_jsonl(
        analytics_dir / "compound_risk.jsonl",
        [
            {
                "pair_id": "cr1",
                "minister_name": "MIN. TESTE",
                "entity_type": "party",
                "entity_id": "p1",
                "entity_name": "AUTOR A",
                "signal_count": 3,
                "signals": ["alert", "donation", "sanction"],
                "red_flag": True,
                "shared_process_count": 1,
                "shared_process_ids": ["proc_1"],
                "alert_count": 1,
                "alert_ids": ["alert_1"],
                "max_alert_score": 0.87,
                "max_rate_delta": 0.31,
                "sanction_match_count": 1,
                "sanction_sources": ["CGU"],
                "donation_match_count": 1,
                "donation_total_brl": 1000.0,
                "corporate_conflict_count": 0,
                "corporate_conflict_ids": [],
                "corporate_companies": [],
                "affinity_count": 0,
                "affinity_ids": [],
                "top_process_classes": [],
                "supporting_party_ids": [],
                "supporting_party_names": [],
                "generated_at": "2026-01-31T12:00:00+00:00",
            }
        ],
    )
    _write_json(analytics_dir / "compound_risk_summary.json", {"pair_count": 1, "red_flag_count": 1, "top_pairs": []})

    build_serving_database(database_url=database_url, curated_dir=curated_dir, analytics_dir=analytics_dir)

    with managed_engine(database_url) as engine:
        with Session(engine) as session:
            rows = session.scalars(select(ServingCompoundRisk)).all()
            assert len(rows) == 1
            assert rows[0].pair_id == "cr1"
            assert rows[0].signals_json is not None
            assert json.loads(rows[0].signals_json) == ["alert", "donation", "sanction"]
        with Session(engine) as session:
            assert _count(session, ServingCase) == 1
            assert _count(session, ServingSchemaMeta) == 1
            meta = session.get(ServingSchemaMeta, SERVING_SCHEMA_SINGLETON_KEY)
            assert meta is not None
            assert meta.schema_version == SERVING_SCHEMA_VERSION


def test_build_does_not_publish_on_failure(tmp_path):
    """A failed build must not corrupt or remove the existing database."""
    database_path = tmp_path / "serving.db"
    database_url = f"sqlite+pysqlite:///{database_path}"
    curated_dir = tmp_path / "curated"
    analytics_dir = tmp_path / "analytics"

    # First build succeeds.
    _write_minimal_data(curated_dir, analytics_dir)
    build_serving_database(database_url=database_url, curated_dir=curated_dir, analytics_dir=analytics_dir)

    with managed_engine(database_url) as engine:
        with Session(engine) as session:
            assert _count(session, ServingCase) == 1

    # Remove required files to force failure on second build.
    (curated_dir / "decision_event.jsonl").unlink()

    with pytest.raises(FileNotFoundError):
        build_serving_database(database_url=database_url, curated_dir=curated_dir, analytics_dir=analytics_dir)

    # Original DB must remain valid and populated.
    assert database_path.exists()
    with managed_engine(database_url) as engine:
        with Session(engine) as session:
            assert _count(session, ServingCase) == 1
            assert _count(session, ServingSchemaMeta) == 1


def test_build_mid_phase_failure_does_not_publish(tmp_path, monkeypatch):
    """A failure AFTER the .build file is created must not replace the final DB.

    Injects failure during Phase 3 (entity loading) to exercise the error
    path after create_engine has already created the .build file on disk.
    """
    database_path = tmp_path / "serving.db"
    database_url = f"sqlite+pysqlite:///{database_path}"
    build_path = database_path.with_suffix(".db.build")
    curated_dir = tmp_path / "curated"
    analytics_dir = tmp_path / "analytics"

    # First build succeeds — creates a valid final DB.
    _write_minimal_data(curated_dir, analytics_dir)
    build_serving_database(database_url=database_url, curated_dir=curated_dir, analytics_dir=analytics_dir)
    original_size = database_path.stat().st_size
    assert original_size > 0

    # Inject failure into load_counsels (Phase 3) for second build.
    def _exploding_loader(*_args, **_kwargs):
        raise RuntimeError("Injected mid-build failure")

    from atlas_stf.serving import builder as builder_mod

    monkeypatch.setattr(builder_mod, "load_counsels", _exploding_loader)

    with pytest.raises(RuntimeError, match="Injected mid-build failure"):
        build_serving_database(database_url=database_url, curated_dir=curated_dir, analytics_dir=analytics_dir)

    # Final DB must be untouched — same size, still valid.
    assert database_path.exists()
    assert database_path.stat().st_size == original_size
    with managed_engine(database_url) as engine:
        with Session(engine) as session:
            assert _count(session, ServingCase) == 1

    # .build must be cleaned up by the error path — no orphan on disk.
    assert not build_path.exists(), ".build should be removed after mid-phase failure"


def test_economic_groups_streaming_does_not_accumulate(tmp_path):
    """stream_economic_group_rows is a generator, not a list."""
    analytics_dir = tmp_path / "analytics"
    _write_jsonl(
        analytics_dir / "economic_group.jsonl",
        [
            {
                "group_id": "eg_1",
                "member_cnpjs": ["11111111"],
                "razoes_sociais": ["EMPRESA A"],
                "member_count": 1,
                "total_capital_social": 100000.0,
                "cnae_labels": [],
                "ufs": ["DF"],
                "active_establishment_count": 1,
                "total_establishment_count": 1,
                "is_law_firm_group": False,
                "law_firm_member_count": 0,
                "law_firm_member_ratio": 0.0,
                "has_minister_partner": False,
                "has_party_partner": False,
                "has_counsel_partner": False,
                "generated_at": "2026-01-31T12:00:00+00:00",
            },
            {
                "group_id": "eg_2",
                "member_cnpjs": ["22222222"],
                "razoes_sociais": ["EMPRESA B"],
                "member_count": 2,
                "total_capital_social": 200000.0,
                "cnae_labels": [],
                "ufs": ["SP"],
                "active_establishment_count": 2,
                "total_establishment_count": 3,
                "is_law_firm_group": True,
                "law_firm_member_count": 1,
                "law_firm_member_ratio": 0.5,
                "has_minister_partner": True,
                "has_party_partner": False,
                "has_counsel_partner": True,
                "generated_at": "2026-01-31T12:00:00+00:00",
            },
            # Duplicate — must be skipped
            {
                "group_id": "eg_1",
                "member_cnpjs": ["11111111"],
                "razoes_sociais": ["EMPRESA A DUPLICADA"],
                "member_count": 1,
            },
        ],
    )

    result = stream_economic_group_rows(analytics_dir)
    assert isinstance(result, types.GeneratorType)

    rows = list(result)
    assert len(rows) == 2
    assert rows[0]["group_id"] == "eg_1"
    assert rows[1]["group_id"] == "eg_2"
    assert json.loads(rows[0]["member_cnpjs_json"]) == ["11111111"]
    assert rows[1]["is_law_firm_group"] is True
    assert rows[1]["law_firm_member_ratio"] == 0.5
    assert rows[1]["has_minister_partner"] is True


def test_build_validates_inputs_rejects_duplicate_pks(tmp_path):
    """Pre-build validation must reject files with duplicate primary keys."""
    curated_dir = tmp_path / "curated"
    analytics_dir = tmp_path / "analytics"
    _write_minimal_data(curated_dir, analytics_dir)

    # Inject duplicate decision_event_id into decision_event.jsonl.
    _write_jsonl(
        curated_dir / "decision_event.jsonl",
        [
            {
                "decision_event_id": "evt_1",
                "process_id": "proc_1",
                "decision_date": "2026-01-05",
                "current_rapporteur": "MIN. TESTE",
                "decision_type": "Decisao Final",
                "decision_progress": "Procedente",
                "decision_origin": "JULGAMENTO",
                "judging_body": "PLENO",
                "is_collegiate": True,
            },
            {
                "decision_event_id": "evt_1",
                "process_id": "proc_1",
                "decision_date": "2026-01-06",
                "current_rapporteur": "MIN. TESTE",
                "decision_type": "Decisao Final",
                "decision_progress": "Improcedente",
                "decision_origin": "JULGAMENTO",
                "judging_body": "PLENO",
                "is_collegiate": True,
            },
        ],
    )

    database_url = f"sqlite+pysqlite:///{tmp_path / 'serving.db'}"
    with pytest.raises(ValueError, match="conflicting"):
        build_serving_database(database_url=database_url, curated_dir=curated_dir, analytics_dir=analytics_dir)

    # Build file must not be left behind.
    build_path = (tmp_path / "serving.db").with_suffix(".db.build")
    assert not build_path.exists()


def test_build_validates_critical_tables(serving_db: str):
    """After a successful build all critical tables must have rows."""
    with managed_engine(serving_db) as engine:
        with Session(engine) as session:
            assert _count(session, ServingCase) >= 1
            assert _count(session, ServingAlert) >= 1
            assert _count(session, ServingCounsel) >= 1
            assert _count(session, ServingParty) >= 1
            assert _count(session, ServingMinisterFlow) >= 1
            assert _count(session, ServingSchemaMeta) >= 1
