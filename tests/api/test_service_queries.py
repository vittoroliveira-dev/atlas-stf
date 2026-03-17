"""Tests for API service layer — filters and core service queries."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from atlas_stf.api._filters import QueryFilters, _apply_case_filters, _paginate, resolve_filters
from atlas_stf.api.service import (
    get_alert_detail,
    get_alerts,
    get_case_detail,
    get_case_ml_outlier,
    get_cases,
    get_counsel_detail,
    get_counsels,
    get_dashboard,
    get_minister_flow,
    get_minister_profile_data,
    get_parties,
    get_related_alerts_for_case,
    get_sources_audit,
)
from atlas_stf.serving.models import (
    ServingAlert,
    ServingCase,
    ServingMinisterFlow,
    ServingRapporteurProfile,
)
from tests.api.conftest import managed_engine


@pytest.fixture()
def session(serving_db: str):
    with managed_engine(serving_db) as engine:
        with Session(engine) as s:
            yield s


class TestFilters:
    def test_paginate_clamps_values(self):
        assert _paginate(0, 200) == (1, 100)
        assert _paginate(-5, -1) == (1, 1)
        assert _paginate(3, 50) == (3, 50)

    def test_resolve_filters_defaults_to_first_period(self, session: Session):
        resolved = resolve_filters(session, QueryFilters())
        assert resolved.filters.period == "2026-01"

    def test_resolve_filters_invalid_period_falls_back(self, session: Session):
        resolved = resolve_filters(session, QueryFilters(period="9999-99"))
        assert resolved.filters.period == "2026-01"

    def test_resolve_filters_valid_minister(self, session: Session):
        resolved = resolve_filters(session, QueryFilters(minister="TESTE"))
        assert resolved.filters.minister == "TESTE"

    def test_apply_case_filters_minister(self, session: Session):
        stmt = _apply_case_filters(select(ServingCase), QueryFilters(minister="TESTE"))
        cases = session.scalars(stmt).all()
        assert len(cases) == 1
        assert cases[0].current_rapporteur == "MIN. TESTE"

    def test_apply_case_filters_collegiate(self, session: Session):
        stmt = _apply_case_filters(select(ServingCase), QueryFilters(collegiate="colegiado"))
        cases = session.scalars(stmt).all()
        assert all(c.is_collegiate for c in cases)

    def test_apply_case_filters_monocratico(self, session: Session):
        stmt = _apply_case_filters(select(ServingCase), QueryFilters(collegiate="monocratico"))
        cases = session.scalars(stmt).all()
        assert all(not c.is_collegiate for c in cases)


class TestServiceQueries:
    def test_get_dashboard(self, session: Session):
        result = get_dashboard(session, QueryFilters(period="2026-01"))
        assert result.kpis.alert_count == 1
        assert result.kpis.selected_events >= 1
        assert len(result.source_files) > 0

    def test_get_alerts(self, session: Session):
        result = get_alerts(session, QueryFilters(period="2026-01"), page=1, page_size=10)
        assert result.total >= 1
        assert result.items[0].alert_id == "alert_1"
        assert result.items[0].ensemble_score == pytest.approx(0.85)

    def test_get_alert_detail_found(self, session: Session):
        result = get_alert_detail(session, "alert_1")
        assert result is not None
        assert result.alert_id == "alert_1"
        assert result.ensemble_score == pytest.approx(0.85)

    def test_get_alert_detail_not_found(self, session: Session):
        assert get_alert_detail(session, "nonexistent") is None

    def test_get_cases(self, session: Session):
        result = get_cases(session, QueryFilters(period="2026-01"), page=1, page_size=10)
        assert result.total >= 1

    def test_get_case_detail(self, session: Session):
        result = get_case_detail(session, QueryFilters(period="2026-01"), "evt_1")
        assert result.case_item is not None
        assert result.ml_outlier_analysis is not None
        assert result.ml_outlier_analysis.decision_event_id == "evt_1"
        assert result.ml_outlier_analysis.ensemble_score == pytest.approx(0.85)
        assert len(result.counsels) >= 1
        assert len(result.parties) >= 1
        assert {item.id: item.role_labels for item in result.counsels} == {
            "coun_1": ["REQTE.(S)"],
            "coun_2": ["REQDO.(A/S)"],
        }
        assert {item.id: item.role_labels for item in result.parties} == {
            "party_1": ["REQTE.(S)"],
            "party_2": ["REQDO.(A/S)"],
        }

    def test_get_case_detail_not_found(self, session: Session):
        result = get_case_detail(session, QueryFilters(), "nonexistent")
        assert result.case_item is None
        assert result.ml_outlier_analysis is None

    def test_get_case_detail_respects_filters(self, session: Session):
        result = get_case_detail(session, QueryFilters(minister="TESTE", period="2026-01"), "evt_2")
        assert result.case_item is None
        assert result.ml_outlier_analysis is None
        assert result.related_alerts == []
        assert result.counsels == []
        assert result.parties == []

    def test_get_case_detail_returns_case_when_id_matches_filtered_recorte(self, session: Session):
        result = get_case_detail(session, QueryFilters(minister="TESTE", period="2026-01"), "evt_1")
        assert result.case_item is not None
        assert result.case_item.decision_event_id == "evt_1"
        assert result.case_item.process_id == "proc_1"
        assert result.filters.applied.minister == "TESTE"
        assert result.related_alerts[0].alert_id == "alert_1"
        assert result.related_alerts[0].ensemble_score == pytest.approx(0.85)

    def test_get_case_ml_outlier(self, session: Session):
        result = get_case_ml_outlier(session, QueryFilters(period="2026-01"), "evt_1")
        assert result is not None
        assert result.decision_event_id == "evt_1"
        assert result.ensemble_score == pytest.approx(0.85)

    def test_get_case_ml_outlier_respects_filters(self, session: Session):
        result = get_case_ml_outlier(session, QueryFilters(minister="TESTE", period="2026-01"), "evt_2")
        assert result is None

    def test_get_case_ml_outlier_returns_none_when_score_is_missing(self, session: Session):
        result = get_case_ml_outlier(session, QueryFilters(period="2026-01"), "evt_2")
        assert result is None

    def test_get_related_alerts(self, session: Session):
        result = get_related_alerts_for_case(session, "evt_1")
        assert len(result) >= 1
        assert result[0].ensemble_score == pytest.approx(0.85)

    def test_get_related_alerts_honors_limit(self, session: Session):
        session.add(
            ServingAlert(
                alert_id="alert_2",
                process_id="proc_1",
                decision_event_id="evt_1",
                comparison_group_id="grp_1",
                alert_type="atipicidade",
                alert_score=0.5,
                expected_pattern="Esperado",
                observed_pattern="Observado",
                evidence_summary="Resumo",
                status="novo",
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
        )
        session.commit()

        result = get_related_alerts_for_case(session, "evt_1", limit=1)

        assert len(result) == 1
        assert result[0].alert_id == "alert_1"

    def test_get_counsels(self, session: Session):
        result = get_counsels(session, QueryFilters(period="2026-01"), page=1, page_size=10)
        assert result.total >= 1

    def test_get_counsel_detail_scopes_role_labels_to_filtered_cases(self, session: Session):
        result = get_counsel_detail(session, "coun_1", QueryFilters(period="2026-01", process_class="ADI"))
        assert result is not None
        assert result.entity.role_labels == ["REQTE.(S)"]
        assert len(result.ministers) == 1
        assert result.ministers[0].minister == "MIN. TESTE"
        assert result.ministers[0].role_labels == ["REQTE.(S)"]

    def test_get_parties(self, session: Session):
        result = get_parties(session, QueryFilters(period="2026-01"), page=1, page_size=10)
        assert result.total >= 1

    def test_get_sources_audit(self, session: Session):
        result = get_sources_audit(session)
        assert len(result.source_files) > 0

    def test_get_minister_profile_data(self, session: Session):
        result = get_minister_profile_data(session, "TESTE")
        assert len(result) == 1
        assert result[0].rapporteur == "MIN. TESTE"
        assert result[0].process_class == "ADI"

    def test_get_minister_profile_data_honors_limit(self, session: Session):
        session.add(
            ServingRapporteurProfile(
                rapporteur="MIN. TESTE",
                process_class="RE",
                thematic_key="DIREITO CONSTITUCIONAL",
                decision_year=2025,
                event_count=2,
                deviation_flag=False,
                progress_distribution_json="{}",
                group_progress_distribution_json="{}",
            )
        )
        session.commit()

        result = get_minister_profile_data(session, "TESTE", limit=1)

        assert len(result) == 1
        assert result[0].decision_year == 2026

    def test_get_minister_profile_data_treats_percent_as_literal(self, session: Session):
        result = get_minister_profile_data(session, "%")
        assert result == []

    def test_get_minister_profile_data_tolerates_invalid_json(self, session: Session):
        row = session.scalars(select(ServingRapporteurProfile)).first()
        assert row is not None
        row.progress_distribution_json = "{invalid"
        row.group_progress_distribution_json = '["wrong-type"]'
        session.commit()

        result = get_minister_profile_data(session, "TESTE")

        assert len(result) == 1
        assert result[0].progress_distribution == {}
        assert result[0].group_progress_distribution == {}

    def test_get_minister_flow_uses_materialized_row(self, session: Session):
        result = get_minister_flow(session, QueryFilters(minister="TESTE", period="2026-01"))
        assert result.status == "ok"
        assert result.minister_reference == "MIN. TESTE"
        assert result.event_count == 1
        assert result.process_count == 1

    def test_get_minister_flow_tolerates_invalid_json(self, session: Session):
        row = session.scalars(
            select(ServingMinisterFlow).where(
                ServingMinisterFlow.minister_name == "MIN. TESTE",
                ServingMinisterFlow.period == "2026-01",
                ServingMinisterFlow.collegiate_filter == "all",
                ServingMinisterFlow.judging_body.is_(None),
                ServingMinisterFlow.process_class.is_(None),
            )
        ).first()
        assert row is not None
        row.thematic_source_distribution_json = "{invalid"
        row.thematic_flow_interpretation_reasons_json = '{"wrong":"type"}'
        row.daily_counts_json = "{invalid"
        session.commit()

        result = get_minister_flow(session, QueryFilters(minister="TESTE", period="2026-01"))

        assert result.thematic_source_distribution == {}
        assert result.thematic_flow_interpretation_reasons == []
        assert result.daily_counts == []

    def test_get_minister_flow_returns_empty_for_ambiguous_match(self, session: Session):
        result = get_minister_flow(session, QueryFilters(minister="TE", period="2026-01"))
        assert result.status == "empty"
        assert result.minister_reference is None
        assert result.event_count == 0
        assert result.process_count == 0
