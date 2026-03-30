"""Regression tests for API audit fixes.

Tests four specific bugs that were fixed:
1. Minister flow secondary resolution — query by minister_query directly
2. Temporal overview LIMIT truncation — no row limit on overview
3. Unicode/accent case-insensitive matching — py_lower UDF
4. Institutional counsel false positive — is_institutional_counsel()
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from atlas_stf.analytics.counsel_affinity import (
    classify_institutional,
    is_institutional_counsel,
)
from atlas_stf.api.app import create_app
from atlas_stf.serving.models import (
    Base,
    ServingCase,
    ServingDonationEvent,
    ServingDonationMatch,
    ServingMinisterFlow,
    ServingMlOutlierScore,
    ServingTemporalAnalysis,
)

# ---------------------------------------------------------------------------
# Accented minister names used across tests
# ---------------------------------------------------------------------------
_ACCENTED_MINISTERS = [
    "MIN. LUÍS ROBERTO BARROSO",
    "MIN. CÁRMEN LÚCIA",
    "MIN. FLÁVIO DINO",
]


def _json_text(obj: object) -> str:
    return json.dumps(obj, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def audit_db(tmp_path) -> str:
    """Build a serving DB with accented ministers and >1000 temporal records."""
    db_path = tmp_path / "audit_regression.db"
    db_url = f"sqlite:///{db_path}"
    engine = create_engine(db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)

    now = datetime.now(timezone.utc)
    cases: list[ServingCase] = []
    flows: list[ServingMinisterFlow] = []
    temporal_rows: list[ServingTemporalAnalysis] = []

    # --- Cases with accented minister names ---
    for idx, minister in enumerate(_ACCENTED_MINISTERS):
        cases.append(
            ServingCase(
                decision_event_id=f"evt_acc_{idx}",
                process_id=f"proc_acc_{idx}",
                process_class="ADI",
                thematic_key="DIREITO CONSTITUCIONAL",
                current_rapporteur=minister,
                decision_date=date(2026, 1, 10 + idx),
                period="2026-01",
                decision_type="Decisão Final",
                decision_progress="Procedente",
                judging_body="PLENO",
                is_collegiate=True,
            )
        )

    # --- Minister flow rows keyed by full accented minister name ---
    for idx, minister in enumerate(_ACCENTED_MINISTERS):
        flows.append(
            ServingMinisterFlow(
                flow_key=f"flow_acc_{idx}",
                minister_name=minister,
                period="2026-01",
                collegiate_filter="all",
                judging_body=None,
                process_class=None,
                minister_query=minister,
                minister_match_mode="contains_casefold",
                minister_reference=minister,
                status="ok",
                event_count=5,
                process_count=3,
                active_day_count=2,
                first_decision_date=date(2026, 1, 10),
                last_decision_date=date(2026, 1, 15),
                historical_event_count=10,
                historical_active_day_count=5,
                historical_average_events_per_active_day=2.0,
                linked_alert_count=0,
                thematic_key_rule="first_subject_normalized_else_branch_of_law",
                thematic_flow_interpretation_status="comparativo",
                thematic_source_distribution_json=_json_text({"subject": 1}),
                historical_thematic_source_distribution_json=_json_text({"subject": 1}),
                thematic_flow_interpretation_reasons_json=_json_text(["dados_suficientes"]),
                decision_type_distribution_json=_json_text({"Decisão Final": 5}),
                decision_progress_distribution_json=_json_text({"Procedente": 5}),
                judging_body_distribution_json=_json_text({"PLENO": 5}),
                collegiate_distribution_json=_json_text({"colegiado": 5}),
                process_class_distribution_json=_json_text({"ADI": 5}),
                thematic_distribution_json=_json_text({"DIREITO CONSTITUCIONAL": 5}),
                daily_counts_json=_json_text([]),
                decision_type_flow_json=_json_text([]),
                judging_body_flow_json=_json_text([]),
                decision_progress_flow_json=_json_text([]),
                process_class_flow_json=_json_text([]),
                thematic_flow_json=_json_text([]),
            )
        )

    # --- Temporal analysis: generate >1000 rows to test no-LIMIT regression ---
    distinct_ministers_for_temporal = [f"MIN. MINISTRO_{i:04d}" for i in range(50)]
    record_counter = 0
    for minister in distinct_ministers_for_temporal:
        # 24 monthly records per minister = 50*24 = 1200 rows
        for month in range(1, 25):
            year = 2025 if month <= 12 else 2026
            m = month if month <= 12 else month - 12
            temporal_rows.append(
                ServingTemporalAnalysis(
                    record_id=f"temp_{record_counter:06d}",
                    analysis_kind="monthly_minister",
                    rapporteur=minister,
                    decision_month=f"{year}-{m:02d}",
                    decision_year=year,
                    decision_count=6,
                    favorable_count=4,
                    unfavorable_count=2,
                    favorable_rate=0.666667,
                    rolling_favorable_rate_6m=0.55,
                    breakpoint_score=1.2,
                    breakpoint_flag=False,
                    generated_at=now,
                )
            )
            record_counter += 1

    # Also add accented minister temporal records
    for minister in _ACCENTED_MINISTERS:
        temporal_rows.append(
            ServingTemporalAnalysis(
                record_id=f"temp_{record_counter:06d}",
                analysis_kind="monthly_minister",
                rapporteur=minister,
                decision_month="2026-01",
                decision_year=2026,
                decision_count=8,
                favorable_count=6,
                unfavorable_count=2,
                favorable_rate=0.75,
                rolling_favorable_rate_6m=0.7,
                breakpoint_score=3.5,
                breakpoint_flag=True,
                generated_at=now,
            )
        )
        record_counter += 1

    # --- ML outlier score for cross-period test ---
    ml_outlier = ServingMlOutlierScore(
        decision_event_id="evt_acc_0",
        comparison_group_id="grp_test",
        ml_anomaly_score=-0.15,
        ml_rarity_score=0.9,
        ensemble_score=0.88,
        n_features=4,
        n_samples=20,
        generated_at=now,
    )

    # --- Donation match + events for subtotal test ---
    donation_match = ServingDonationMatch(
        match_id="dm_test_1",
        entity_type="party",
        entity_id="party_test",
        party_id="party_test",
        party_name_normalized="PARTIDO TESTE",
        donor_cpf_cnpj="12345678000199",
        donor_name_normalized="DOADOR TESTE",
        total_donated_brl=500000.0,
        donation_count=100,
        stf_case_count=3,
        red_flag=False,
    )
    donation_events = [
        ServingDonationEvent(
            event_id=f"de_test_{i}",
            match_id="dm_test_1",
            election_year=2020,
            donation_amount=1000.0 * (i + 1),
            donor_cpf_cnpj="12345678000199",
        )
        for i in range(5)
    ]
    # Events sum: 1000 + 2000 + 3000 + 4000 + 5000 = 15000.0

    with Session(engine) as s:
        with s.begin():
            s.add_all(cases)
            s.add_all(flows)
            s.add_all(temporal_rows)
            s.add(ml_outlier)
            s.add(donation_match)
            s.add_all(donation_events)

    engine.dispose()
    return db_url


@pytest.fixture()
def client(audit_db: str) -> TestClient:
    """TestClient with py_lower UDF registered via create_app."""
    app = create_app(database_url=audit_db)
    return TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# 1. is_institutional_counsel — pure function test
# ---------------------------------------------------------------------------


class TestInstitutionalCounsel:
    """Regression: institutional counsel false positive filter."""

    def test_procurador_geral_with_state(self):
        assert is_institutional_counsel("PROCURADOR-GERAL DO ESTADO DE RORAIMA") is True

    def test_procuradoria_geral_do_estado(self):
        assert is_institutional_counsel("PROCURADORIA-GERAL DO ESTADO") is True

    def test_advocacia_geral_da_uniao(self):
        assert is_institutional_counsel("ADVOCACIA-GERAL DA UNIAO") is True

    def test_defensor_publico_geral(self):
        assert is_institutional_counsel("DEFENSOR PUBLICO-GERAL") is True

    def test_defensoria_publica(self):
        assert is_institutional_counsel("DEFENSORIA PUBLICA") is True

    def test_ministerio_publico_federal(self):
        assert is_institutional_counsel("MINISTERIO PUBLICO FEDERAL") is True

    def test_procurador_geral_hyphen(self):
        assert is_institutional_counsel("PROCURADOR-GERAL DA REPUBLICA") is True

    def test_advogado_geral_da_uniao(self):
        assert is_institutional_counsel("ADVOGADO-GERAL DA UNIAO") is True

    def test_advogado_geral_no_hyphen(self):
        assert is_institutional_counsel("ADVOGADO GERAL DA UNIAO") is True

    def test_private_law_firm_is_false(self):
        assert is_institutional_counsel("JOAO DA SILVA ADVOCACIA") is False

    def test_firm_with_ampersand_is_false(self):
        assert is_institutional_counsel("ESCRITORIO SILVA & ASSOCIADOS") is False

    def test_individual_name_is_false(self):
        assert is_institutional_counsel("MARIA FERNANDA DOS SANTOS") is False

    def test_case_insensitive(self):
        assert is_institutional_counsel("procuradoria-geral do estado") is True
        assert is_institutional_counsel("Defensoria Publica da Uniao") is True

    def test_empty_string_is_false(self):
        assert is_institutional_counsel("") is False


# ---------------------------------------------------------------------------
# 2. py_lower UDF — accent-aware case-insensitive filtering via /cases
# ---------------------------------------------------------------------------


class TestPyLowerUdf:
    """Regression: func.lower() replaced by func.py_lower() for accent support.

    The /cases endpoint uses _apply_case_filters which calls _normalized_like
    with func.py_lower. This tests that accented characters are correctly
    lowercased by the Python UDF (SQLite native lower() ignores non-ASCII).
    """

    def test_filter_cases_by_lowercase_accented_carmen(self, client: TestClient):
        resp = client.get(
            "/cases",
            params={"minister": "cármen", "period": "__all__"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] >= 1, "py_lower should match lowercase accented 'cármen'"
        event_ids = {item["decision_event_id"] for item in data["items"]}
        assert "evt_acc_1" in event_ids

    def test_filter_cases_by_lowercase_accented_luis(self, client: TestClient):
        resp = client.get(
            "/cases",
            params={"minister": "luís", "period": "__all__"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] >= 1, "py_lower should match lowercase accented 'luís'"
        event_ids = {item["decision_event_id"] for item in data["items"]}
        assert "evt_acc_0" in event_ids

    def test_filter_cases_by_lowercase_accented_flavio(self, client: TestClient):
        resp = client.get(
            "/cases",
            params={"minister": "flávio", "period": "__all__"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] >= 1, "py_lower should match lowercase accented 'flávio'"
        event_ids = {item["decision_event_id"] for item in data["items"]}
        assert "evt_acc_2" in event_ids

    def test_filter_cases_uppercase_accent(self, client: TestClient):
        resp = client.get(
            "/cases",
            params={"minister": "CÁRMEN", "period": "__all__"},
        )
        assert resp.status_code == 200
        assert resp.json()["total"] >= 1, "py_lower should match uppercase accented 'CÁRMEN'"

    def test_filter_cases_no_match(self, client: TestClient):
        resp = client.get(
            "/cases",
            params={"minister": "INEXISTENTE", "period": "__all__"},
        )
        assert resp.status_code == 200
        assert resp.json()["total"] == 0


# ---------------------------------------------------------------------------
# 3. Temporal overview — no LIMIT truncation
# ---------------------------------------------------------------------------


class TestTemporalOverviewNoLimit:
    """Regression: overview loaded only 1000 rows, now loads ALL."""

    def test_overview_returns_all_records(self, client: TestClient):
        resp = client.get("/temporal-analysis")
        assert resp.status_code == 200
        data = resp.json()
        summary = data["summary"]
        # 50 ministers * 24 months + 3 accented ministers = 1203 records
        expected_total = 50 * 24 + len(_ACCENTED_MINISTERS)
        assert summary["total_records"] == expected_total, (
            f"Expected {expected_total} total records (no LIMIT), got {summary['total_records']}"
        )

    def test_overview_all_ministers_present(self, client: TestClient):
        resp = client.get("/temporal-analysis")
        assert resp.status_code == 200
        data = resp.json()
        summary = data["summary"]
        # 50 generic + 3 accented = 53 ministers
        expected_ministers = 50 + len(_ACCENTED_MINISTERS)
        assert summary["ministers_covered"] == expected_ministers, (
            f"Expected {expected_ministers} ministers, got {summary['ministers_covered']}"
        )

    def test_overview_counts_by_kind(self, client: TestClient):
        resp = client.get("/temporal-analysis")
        assert resp.status_code == 200
        data = resp.json()
        counts = data["summary"]["counts_by_kind"]
        assert counts["monthly_minister"] == 50 * 24 + len(_ACCENTED_MINISTERS)

    def test_overview_filter_by_minister(self, client: TestClient):
        resp = client.get(
            "/temporal-analysis",
            params={"minister": "MINISTRO_0001"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["summary"]["total_records"] == 24

    def test_overview_accented_minister_in_summaries(self, client: TestClient):
        resp = client.get("/temporal-analysis")
        assert resp.status_code == 200
        data = resp.json()
        summary_names = {s["rapporteur"] for s in data["minister_summaries"]}
        for minister in _ACCENTED_MINISTERS:
            assert minister in summary_names, f"Accented minister {minister} missing from overview summaries"


# ---------------------------------------------------------------------------
# 4. Minister flow direct lookup by minister_query
# ---------------------------------------------------------------------------


class TestMinisterFlowDirectLookup:
    """Regression: flow was resolving via ServingCase.current_rapporteur
    before querying ServingMinisterFlow. Now queries minister_query directly.

    The endpoint /ministers/{minister}/flow stores flows keyed by the full
    minister name as minister_query. Querying with the exact full name must
    return the materialized flow, not 'unresolved' or 'empty'.
    """

    def test_accented_minister_flow_resolved(self, client: TestClient):
        for minister in _ACCENTED_MINISTERS:
            resp = client.get(
                f"/ministers/{minister}/flow",
                params={"period": "2026-01"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] != "empty", f"Flow for '{minister}' should be 'ok', got '{data['status']}'"
            assert data["minister_match_mode"] != "unresolved", (
                f"minister_match_mode for '{minister}' should not be 'unresolved'"
            )

    def test_flow_event_count(self, client: TestClient):
        resp = client.get(
            f"/ministers/{_ACCENTED_MINISTERS[0]}/flow",
            params={"period": "2026-01"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["event_count"] == 5

    def test_flow_minister_reference(self, client: TestClient):
        minister = _ACCENTED_MINISTERS[1]
        resp = client.get(
            f"/ministers/{minister}/flow",
            params={"period": "2026-01"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["minister_reference"] == minister

    def test_flow_unknown_minister_is_empty(self, client: TestClient):
        resp = client.get(
            "/ministers/INEXISTENTE/flow",
            params={"period": "2026-01"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "empty"


# ---------------------------------------------------------------------------
# 5. Temporal minister endpoint with accented name
# ---------------------------------------------------------------------------


class TestTemporalMinisterAccented:
    """Temporal analysis per-minister must resolve accented names via py_lower."""

    def test_minister_exact_match_with_accent(self, client: TestClient):
        resp = client.get(f"/temporal-analysis/{_ACCENTED_MINISTERS[0]}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["rapporteur"] == _ACCENTED_MINISTERS[0]
        assert len(data["monthly"]) == 1

    def test_minister_partial_match_with_accent(self, client: TestClient):
        resp = client.get("/temporal-analysis/LUÍS")
        assert resp.status_code == 200
        data = resp.json()
        assert data["rapporteur"] is not None
        assert "LUÍS" in data["rapporteur"]

    def test_minister_unknown_returns_empty(self, client: TestClient):
        resp = client.get("/temporal-analysis/INEXISTENTE")
        assert resp.status_code == 200
        data = resp.json()
        assert data["rapporteur"] is None
        assert data["monthly"] == []


# ---------------------------------------------------------------------------
# 6. Flow textual search fallback (unified contract)
# ---------------------------------------------------------------------------


class TestFlowContract:
    """Flow endpoint contract: exact → textual fallback → ambiguous.

    The payload's ``minister_match_mode`` signals which path was taken:
    - ``exact``: canonical key matched.
    - ``textual``: single textual match.
    - ``ambiguous``: multiple matches, no data returned.
    - ``unresolved``: no match at all.
    """

    def test_flow_exact_match_mode(self, client: TestClient):
        resp = client.get(
            f"/ministers/{_ACCENTED_MINISTERS[0]}/flow",
            params={"period": "2026-01"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["minister_match_mode"] == "exact"
        assert data["minister_match_count"] == 1
        assert data["minister_candidates"] is None
        assert data["minister_reference"] == _ACCENTED_MINISTERS[0]

    def test_flow_textual_single_match(self, client: TestClient):
        resp = client.get(
            "/ministers/BARROSO/flow",
            params={"period": "2026-01"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["minister_match_mode"] == "textual"
        assert data["minister_match_count"] == 1

    def test_flow_textual_lowercase_accented(self, client: TestClient):
        resp = client.get(
            "/ministers/flávio/flow",
            params={"period": "2026-01"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["minister_match_mode"] == "textual"

    def test_flow_ambiguous_returns_candidates(self, client: TestClient):
        # "MIN." matches all 3 accented ministers — ambiguous
        resp = client.get(
            "/ministers/MIN./flow",
            params={"period": "2026-01"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "empty"
        assert data["minister_match_mode"] == "ambiguous"
        assert data["minister_match_count"] == 3
        assert isinstance(data["minister_candidates"], list)
        assert len(data["minister_candidates"]) == 3
        # Candidates must be sorted deterministically
        assert data["minister_candidates"] == sorted(data["minister_candidates"])

    def test_flow_unresolved_for_unknown(self, client: TestClient):
        resp = client.get(
            "/ministers/INEXISTENTE/flow",
            params={"period": "2026-01"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "empty"
        assert data["minister_match_mode"] == "unresolved"
        assert data["minister_match_count"] == 0
        assert data["minister_candidates"] is None


# ---------------------------------------------------------------------------
# 7. Structural institutional classification
# ---------------------------------------------------------------------------


class TestStructuralInstitutionalClassification:
    """classify_institutional uses structural set first, prefix as fallback."""

    def test_structural_set_takes_priority(self):
        structural = {"PROCURADOR-GERAL DO ESTADO DE RORAIMA"}
        is_inst, source = classify_institutional(
            "PROCURADOR-GERAL DO ESTADO DE RORAIMA", structural,
        )
        assert is_inst is True
        assert source == "structural"

    def test_prefix_fallback_when_not_in_structural(self):
        structural: set[str] = set()  # empty structural set
        is_inst, source = classify_institutional(
            "PROCURADOR-GERAL DO ESTADO DE RORAIMA", structural,
        )
        assert is_inst is True
        assert source == "fallback:name_prefix"

    def test_private_counsel_not_institutional(self):
        structural = {"PROCURADOR-GERAL DO ESTADO DE RORAIMA"}
        is_inst, source = classify_institutional(
            "JOAO DA SILVA ADVOCACIA", structural,
        )
        assert is_inst is False
        assert source == "private"

    def test_structural_overrides_name_pattern(self):
        # A name that starts with institutional prefix BUT is in structural set
        structural = {"ADVOCACIA-GERAL DA UNIAO"}
        is_inst, source = classify_institutional(
            "ADVOCACIA-GERAL DA UNIAO", structural,
        )
        assert is_inst is True
        assert source == "structural"  # structural takes priority, not prefix

    def test_case_insensitive_structural(self):
        structural = {"DEFENSORIA PUBLICA DA UNIAO"}
        is_inst, source = classify_institutional(
            "defensoria publica da uniao", structural,
        )
        assert is_inst is True
        assert source == "structural"

    def test_structural_confidence_high(self):
        structural = {"PROCURADOR-GERAL DO ESTADO DE RORAIMA"}
        is_inst, source = classify_institutional(
            "PROCURADOR-GERAL DO ESTADO DE RORAIMA", structural,
        )
        assert is_inst is True
        assert source == "structural"
        # Confidence mapping: structural → high
        confidence = "high" if source == "structural" else "low" if source == "fallback:name_prefix" else None
        assert confidence == "high"

    def test_fallback_confidence_low(self):
        structural: set[str] = set()
        is_inst, source = classify_institutional(
            "PROCURADOR-GERAL DO ESTADO DE RORAIMA", structural,
        )
        assert is_inst is True
        assert source == "fallback:name_prefix"
        confidence = "high" if source == "structural" else "low" if source == "fallback:name_prefix" else None
        assert confidence == "low"


# ---------------------------------------------------------------------------
# 8. Normalization policy: case-insensitive but NOT accent-insensitive
# ---------------------------------------------------------------------------


class TestNormalizationPolicy:
    """The API is case-insensitive but NOT accent-insensitive.

    'LUÍS' matches 'luís' (case difference) but does NOT match 'luis'
    (missing accent).
    """

    def test_case_difference_matches(self, client: TestClient):
        resp = client.get(
            "/cases",
            params={"minister": "luís roberto barroso", "period": "__all__"},
        )
        assert resp.status_code == 200
        assert resp.json()["total"] >= 1

    def test_accent_difference_does_not_match(self, client: TestClient):
        resp = client.get(
            "/cases",
            params={"minister": "luis roberto barroso", "period": "__all__"},
        )
        assert resp.status_code == 200
        assert resp.json()["total"] == 0, (
            "Without accent ('luis' vs 'luís') should NOT match — policy is case-insensitive only"
        )


# ---------------------------------------------------------------------------
# 9. Lookup by ID ignores context filters (transversal guardrail)
# ---------------------------------------------------------------------------


class TestLookupByIdIgnoresContextFilters:
    """Endpoints that receive a unique PK must find the entity regardless of
    period/minister/collegiate filters.  Filters should only affect surrounding
    context (flow, options), never the primary entity.

    This is a transversal regression guard — same class of bug that affected
    /cases/{id} and /cases/{id}/ml-outlier.
    """

    def test_case_detail_found_despite_wrong_period(self, client: TestClient):
        """Case from period 2026-01 must be found even when ?period=9999-99."""
        resp = client.get(
            "/cases/evt_acc_0",
            params={"period": "9999-99", "minister": "INEXISTENTE"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["case_item"] is not None, (
            "Case detail by PK must not be excluded by context filters"
        )
        assert data["case_item"]["decision_event_id"] == "evt_acc_0"

    def test_case_ml_outlier_found_despite_wrong_period(self, client: TestClient):
        """ML-outlier must return data when the case has ML scores, regardless
        of period filter.  This directly validates the endpoint contract — not
        a proxy check via another endpoint."""
        resp = client.get(
            "/cases/evt_acc_0/ml-outlier",
            params={"period": "9999-99", "minister": "INEXISTENTE"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["decision_event_id"] == "evt_acc_0"
        assert data["ensemble_score"] == pytest.approx(0.88)

    def test_alert_detail_no_context_filters(self, client: TestClient):
        """Alert detail by alert_id has no context filter parameters at all."""
        resp = client.get("/alerts/nonexistent_alert")
        assert resp.status_code == 404  # not found by ID, not by filter


# ---------------------------------------------------------------------------
# 10. Donation payload semantic scope
# ---------------------------------------------------------------------------


class TestDonationPayloadSemantics:
    """Donation payload must distinguish global totals from match subtotals."""

    def test_donation_scope_field_present(self, client: TestClient):
        resp = client.get("/donations", params={"page": 1, "page_size": 10})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] >= 1
        item = data["items"][0]
        assert item["donation_scope"] == "donor_global"

    def test_matched_events_subtotal_present(self, client: TestClient):
        resp = client.get("/donations", params={"page": 1, "page_size": 10})
        assert resp.status_code == 200
        item = resp.json()["items"][0]
        assert "matched_events_total_brl" in item
        assert "matched_events_count" in item

    def test_matched_events_subtotal_equals_events_sum(self, client: TestClient):
        """matched_events_total_brl must equal the sum of donation events."""
        resp = client.get("/donations", params={"page": 1, "page_size": 10})
        assert resp.status_code == 200
        item = resp.json()["items"][0]
        match_id = item["match_id"]

        # Fetch events for this match
        events_resp = client.get(
            f"/donations/{match_id}/events",
            params={"page": 1, "page_size": 100},
        )
        assert events_resp.status_code == 200
        events = events_resp.json()

        events_sum = sum(e["donation_amount"] for e in events["items"])
        assert item["matched_events_total_brl"] == pytest.approx(events_sum), (
            f"matched_events_total_brl ({item['matched_events_total_brl']}) "
            f"must equal sum of events ({events_sum})"
        )
        assert item["matched_events_count"] == events["total"]

    def test_global_vs_match_distinction(self, client: TestClient):
        """Global total must differ from match subtotal (test data has 500k global, 15k match)."""
        resp = client.get("/donations", params={"page": 1, "page_size": 10})
        assert resp.status_code == 200
        item = resp.json()["items"][0]
        assert item["total_donated_brl"] == pytest.approx(500000.0)
        assert item["matched_events_total_brl"] == pytest.approx(15000.0)
        assert item["donation_count"] == 100  # global count
        assert item["matched_events_count"] == 5  # match count
