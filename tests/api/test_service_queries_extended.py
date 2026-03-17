"""Tests for API service layer — extended queries and pagination tests."""

from __future__ import annotations

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from atlas_stf.api._filters import QueryFilters
from atlas_stf.api.service import (
    get_assignment_audit,
    get_counsel_detail,
    get_counsels,
    get_health,
    get_minister_bio,
    get_minister_sequential,
    get_origin_context,
    get_temporal_analysis_minister,
    get_temporal_analysis_overview,
)
from atlas_stf.serving.models import (
    Base,
    ServingAssignmentAudit,
    ServingCase,
    ServingCounsel,
    ServingMinisterBio,
    ServingMlOutlierScore,
    ServingOriginContext,
    ServingProcessCounsel,
    ServingSequentialAnalysis,
)
from tests.api.conftest import managed_engine


@pytest.fixture()
def session(serving_db: str):
    with managed_engine(serving_db) as engine:
        with Session(engine) as s:
            yield s


class TestServiceQueriesExtended:
    def test_get_minister_sequential(self, session: Session):
        result = get_minister_sequential(session, "TESTE")
        assert len(result) == 1
        assert result[0].sequential_bias_flag is True

    def test_get_minister_sequential_honors_limit(self, session: Session):
        session.add(
            ServingSequentialAnalysis(
                rapporteur="MIN. TESTE",
                decision_year=2025,
                n_decisions=9,
                autocorrelation_lag1=0.1,
                base_favorable_rate=0.5,
                sequential_bias_flag=False,
            )
        )
        session.commit()

        result = get_minister_sequential(session, "TESTE", limit=1)

        assert len(result) == 1
        assert result[0].decision_year == 2026

    def test_get_minister_sequential_treats_percent_as_literal(self, session: Session):
        result = get_minister_sequential(session, "%")
        assert result == []

    def test_get_assignment_audit(self, session: Session):
        result = get_assignment_audit(session)
        assert len(result) == 1
        assert result[0].process_class == "ADI"

    def test_get_assignment_audit_honors_limit(self, session: Session):
        session.add(
            ServingAssignmentAudit(
                process_class="RE",
                decision_year=2025,
                rapporteur_count=3,
                event_count=8,
                chi2_statistic=0.8,
                p_value_approx=0.4,
                uniformity_flag=True,
                rapporteur_distribution_json="{}",
            )
        )
        session.commit()

        result = get_assignment_audit(session, limit=1)

        assert len(result) == 1
        assert result[0].decision_year == 2026

    def test_get_assignment_audit_tolerates_invalid_json(self, session: Session):
        row = session.scalars(select(ServingAssignmentAudit)).first()
        assert row is not None
        row.rapporteur_distribution_json = "{invalid"
        session.commit()

        result = get_assignment_audit(session)

        assert len(result) == 1
        assert result[0].rapporteur_distribution == {}

    def test_get_minister_bio_tolerates_invalid_json(self, session: Session):
        session.add(
            ServingMinisterBio(
                minister_name="MIN. INVALIDO",
                political_party_history_json="{invalid",
                known_connections_json='{"wrong":"type"}',
                news_references_json='["fonte"]',
            )
        )
        session.commit()

        result = get_minister_bio(session, "INVALIDO")

        assert result is not None
        assert result.political_party_history == []
        assert result.known_connections == []
        assert result.news_references == ["fonte"]

    def test_get_minister_bio_treats_percent_as_literal(self, session: Session):
        result = get_minister_bio(session, "%")
        assert result is None

    def test_get_origin_context_tolerates_invalid_json(self, session: Session):
        session.add(
            ServingOriginContext(
                origin_index="origem-invalida",
                tribunal_label="TRIBUNAL X",
                state="DF",
                datajud_total_processes=10,
                stf_process_count=1,
                stf_share_pct=0.1,
                top_assuntos_json="{invalid",
                top_orgaos_julgadores_json='{"wrong":"type"}',
                class_distribution_json='[{"label":"ADI","value":1}]',
            )
        )
        session.commit()

        result = get_origin_context(session, "DF")

        assert result.total == 1
        assert result.items[0].top_assuntos == []
        assert result.items[0].top_orgaos_julgadores == []
        assert result.items[0].class_distribution == [{"label": "ADI", "value": 1}]

    def test_get_temporal_analysis_overview(self, session: Session):
        result = get_temporal_analysis_overview(session)
        assert result.summary.total_records == 5
        assert result.minister_summaries[0].rapporteur == "MIN. TESTE"
        assert result.events[0].event_id == "event_1"

    def test_get_temporal_analysis_minister(self, session: Session):
        result = get_temporal_analysis_minister(session, "TESTE")
        assert result.minister == "TESTE"
        assert result.monthly[0].breakpoint_flag is True
        assert result.corporate_links[0].link_start_date.isoformat() == "2025-03-01"

    def test_get_temporal_analysis_minister_treats_percent_as_literal(self, session: Session):
        result = get_temporal_analysis_minister(session, "%")
        assert result.rapporteur is None
        assert result.monthly == []

    def test_serving_loads_ml_outlier_scores(self, session: Session):
        rows = session.scalars(select(ServingMlOutlierScore)).all()
        assert len(rows) == 1
        assert rows[0].decision_event_id == "evt_1"
        assert rows[0].ensemble_score == pytest.approx(0.85)

    def test_get_health(self):
        result = get_health("sqlite+pysqlite:///test.db")
        assert result.status == "ok"
        assert result.database_backend == "sqlite+pysqlite"


def test_entity_detail_counts_not_truncated_at_50(tmp_path):
    """Bug 2 fix: entity_detail should report accurate counts even when >50 cases exist."""
    database_url = f"sqlite+pysqlite:///{tmp_path / 'entity-counts.db'}"
    with managed_engine(database_url) as engine:
        Base.metadata.create_all(engine)

        with Session(engine) as session:
            # Create 60 cases (more than the .limit(50))
            for i in range(60):
                session.add(
                    ServingCase(
                        decision_event_id=f"evt_{i:03d}",
                        process_id=f"proc_{i:03d}",
                        process_number=f"ADI {i}",
                        process_class="ADI",
                        branch_of_law="DIREITO ADMINISTRATIVO",
                        thematic_key="DIREITO ADMINISTRATIVO",
                        origin_description="DISTRITO FEDERAL",
                        inteiro_teor_url=None,
                        juris_doc_count=1,
                        juris_has_acordao=True,
                        juris_has_decisao_monocratica=False,
                        decision_date=None,
                        period="2026-01",
                        current_rapporteur="MIN. TESTE",
                        decision_type="Decisão Final",
                        decision_progress="Procedente",
                        decision_origin="JULGAMENTO",
                        judging_body="PLENO",
                        is_collegiate=True,
                        decision_note=None,
                    )
                )
            session.add(
                ServingCounsel(
                    counsel_id="coun_big",
                    counsel_name_raw="ADVOGADO BIG",
                    counsel_name_normalized="ADVOGADO BIG",
                    notes=None,
                )
            )
            session.add_all(
                [
                    ServingProcessCounsel(
                        link_id=f"pcb_{i:03d}",
                        process_id=f"proc_{i:03d}",
                        counsel_id="coun_big",
                        side_in_case="REQTE.(S)",
                        source_id="fixture",
                    )
                    for i in range(60)
                ]
            )
            session.commit()

            result = get_counsel_detail(session, "coun_big", QueryFilters(period="2026-01"))

    assert result is not None
    # The list of cases should be capped at 50
    assert len(result.cases) == 50
    # But the counts must reflect ALL 60 cases
    assert result.entity.associated_event_count == 60
    assert result.entity.distinct_process_count == 60


def test_get_counsels_paginates_beyond_5000_without_truncating_total(tmp_path):
    database_url = f"sqlite+pysqlite:///{tmp_path / 'entity-pagination.db'}"
    with managed_engine(database_url) as engine:
        Base.metadata.create_all(engine)

        with Session(engine) as session:
            session.add(
                ServingCase(
                    decision_event_id="evt_bulk",
                    process_id="proc_bulk",
                    process_number="ADI BULK",
                    process_class="ADI",
                    branch_of_law="DIREITO ADMINISTRATIVO",
                    thematic_key="DIREITO ADMINISTRATIVO",
                    origin_description="DISTRITO FEDERAL",
                    inteiro_teor_url=None,
                    juris_doc_count=1,
                    juris_has_acordao=True,
                    juris_has_decisao_monocratica=False,
                    decision_date=None,
                    period="2026-01",
                    current_rapporteur="MIN. TESTE",
                    decision_type="Decisão Final",
                    decision_progress="Procedente",
                    decision_origin="JULGAMENTO",
                    judging_body="PLENO",
                    is_collegiate=True,
                    decision_note="Caso sintético para paginação.",
                )
            )
            session.add_all(
                [
                    ServingCounsel(
                        counsel_id=f"coun_{index:04d}",
                        counsel_name_raw=f"ADVOGADO {index:04d}",
                        counsel_name_normalized=f"ADVOGADO {index:04d}",
                        notes=None,
                    )
                    for index in range(5001)
                ]
            )
            session.add_all(
                [
                    ServingProcessCounsel(
                        link_id=f"pc_{index:04d}",
                        process_id="proc_bulk",
                        counsel_id=f"coun_{index:04d}",
                        side_in_case="REQTE.(S)",
                        source_id="fixture",
                    )
                    for index in range(5001)
                ]
            )
            session.commit()

            first_page = get_counsels(session, QueryFilters(period="2026-01"), page=1, page_size=24)
            late_page = get_counsels(session, QueryFilters(period="2026-01"), page=209, page_size=24)

    assert first_page.total == 5001
    assert len(first_page.items) == 24
    assert late_page.total == 5001
    assert len(late_page.items) == 9
