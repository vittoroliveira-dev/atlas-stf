from __future__ import annotations

import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
import pytest

from atlas_stf.api import _routes_core as routes_core
from atlas_stf.api import create_app


@asynccontextmanager
async def _get_client(serving_db: str) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(database_url=serving_db)
    async with _get_client_for_app(app) as client:
        yield client


@asynccontextmanager
async def _get_client_for_app(app) -> AsyncIterator[httpx.AsyncClient]:
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            yield client


@pytest.mark.anyio
async def test_health_endpoint(serving_db: str):
    async with _get_client(serving_db) as client:
        response = await client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.json()["database_backend"] == "sqlite+pysqlite"


@pytest.mark.anyio
async def test_openapi_metadata_exposes_consistent_api_version(serving_db: str):
    async with _get_client(serving_db) as client:
        response = await client.get("/openapi.json")
    assert response.status_code == 200
    payload = response.json()
    assert payload["info"]["title"] == "Atlas STF API"
    assert payload["info"]["version"] == "1.0.0"


@pytest.mark.anyio
async def test_filters_options_endpoint(serving_db: str):
    async with _get_client(serving_db) as client:
        response = await client.get("/filters/options")
    assert response.status_code == 200
    payload = response.json()
    assert payload["periods"] == ["2026-01"]
    assert "MIN. TESTE" in payload["ministers"]


@pytest.mark.anyio
async def test_dashboard_endpoint(serving_db: str):
    async with _get_client(serving_db) as client:
        response = await client.get(
            "/dashboard",
            params={"minister": "TESTE", "period": "2026-01", "collegiate": "colegiado"},
        )
    assert response.status_code == 200
    payload = response.json()
    assert payload["flow"]["event_count"] == 1
    assert payload["kpis"]["selected_events"] == 1
    assert payload["top_counsels"][0]["id"] == "coun_1"
    assert payload["top_parties"][0]["id"] == "party_1"


@pytest.mark.anyio
async def test_dashboard_endpoint_uses_short_ttl_cache(serving_db: str, monkeypatch: pytest.MonkeyPatch):
    calls = {"count": 0}
    original = routes_core.get_dashboard

    def counting_get_dashboard(session, filters):
        calls["count"] += 1
        return original(session, filters)

    monkeypatch.setenv("ATLAS_STF_DASHBOARD_CACHE_TTL_SECONDS", "30")
    monkeypatch.setattr(routes_core, "get_dashboard", counting_get_dashboard)

    async with _get_client(serving_db) as client:
        first = await client.get("/dashboard", params={"minister": "TESTE", "period": "2026-01"})
        second = await client.get("/dashboard", params={"minister": "TESTE", "period": "2026-01"})

    assert first.status_code == 200
    assert second.status_code == 200
    assert calls["count"] == 1


@pytest.mark.anyio
async def test_dashboard_minister_filter_treats_percent_as_literal(serving_db: str):
    async with _get_client(serving_db) as client:
        response = await client.get("/dashboard", params={"minister": "%", "period": "2026-01"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["flow"]["status"] == "empty"
    assert payload["kpis"]["selected_events"] == 0


@pytest.mark.anyio
async def test_dashboard_rejects_invalid_period_format(serving_db: str):
    async with _get_client(serving_db) as client:
        response = await client.get("/dashboard", params={"period": "2026/01"})
    assert response.status_code == 422


@pytest.mark.anyio
async def test_minister_flow_endpoint(serving_db: str):
    async with _get_client(serving_db) as client:
        response = await client.get("/ministers/TESTE/flow", params={"period": "2026-01"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["minister_reference"] == "MIN. TESTE"
    assert payload["event_count"] == 1


@pytest.mark.anyio
async def test_minister_flow_endpoint_returns_empty_for_ambiguous_match(serving_db: str):
    async with _get_client(serving_db) as client:
        response = await client.get("/ministers/TE/flow", params={"period": "2026-01"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "empty"
    assert payload["minister_reference"] is None
    assert payload["event_count"] == 0


@pytest.mark.anyio
async def test_case_detail_endpoint(serving_db: str):
    async with _get_client(serving_db) as client:
        response = await client.get(
            "/cases/evt_1",
            params={"minister": "TESTE", "period": "2026-01"},
        )
    assert response.status_code == 200
    payload = response.json()
    assert payload["case_item"]["process_id"] == "proc_1"
    assert payload["ml_outlier_analysis"]["decision_event_id"] == "evt_1"
    assert payload["ml_outlier_analysis"]["ensemble_score"] == pytest.approx(0.85)
    assert len(payload["related_alerts"]) == 1
    assert payload["related_alerts"][0]["ensemble_score"] == pytest.approx(0.85)
    assert {item["id"] for item in payload["counsels"]} == {"coun_1", "coun_2"}


@pytest.mark.anyio
async def test_case_detail_returns_404_when_case_is_outside_filters(serving_db: str):
    async with _get_client(serving_db) as client:
        response = await client.get(
            "/cases/evt_2",
            params={"minister": "TESTE", "period": "2026-01"},
        )
    assert response.status_code == 404
    assert response.json()["detail"] == "case_not_found"


@pytest.mark.anyio
async def test_case_ml_outlier_endpoint(serving_db: str):
    async with _get_client(serving_db) as client:
        response = await client.get(
            "/cases/evt_1/ml-outlier",
            params={"minister": "TESTE", "period": "2026-01"},
        )
    assert response.status_code == 200
    payload = response.json()
    assert payload["decision_event_id"] == "evt_1"
    assert payload["comparison_group_id"] == "grp_1"
    assert payload["ensemble_score"] == pytest.approx(0.85)


@pytest.mark.anyio
async def test_case_ml_outlier_returns_404_when_case_is_outside_filters(serving_db: str):
    async with _get_client(serving_db) as client:
        response = await client.get(
            "/cases/evt_2/ml-outlier",
            params={"minister": "TESTE", "period": "2026-01"},
        )
    assert response.status_code == 404
    assert response.json()["detail"] == "ml_outlier_not_found"


@pytest.mark.anyio
async def test_alerts_endpoint_exposes_ensemble_score(serving_db: str):
    async with _get_client(serving_db) as client:
        response = await client.get("/alerts", params={"period": "2026-01"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["items"][0]["ensemble_score"] == pytest.approx(0.85)


@pytest.mark.anyio
async def test_case_detail_returns_404_for_unknown_id(serving_db: str):
    async with _get_client(serving_db) as client:
        response = await client.get("/cases/evt_missing")
    assert response.status_code == 404


@pytest.mark.anyio
async def test_counsel_detail_endpoint(serving_db: str):
    async with _get_client(serving_db) as client:
        response = await client.get("/counsels/coun_1", params={"period": "2026-01", "process_class": "ADI"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["entity"]["id"] == "coun_1"
    assert payload["entity"]["role_labels"] == ["REQTE.(S)"]
    assert payload["ministers"][0]["minister"] == "MIN. TESTE"
    assert payload["ministers"][0]["role_labels"] == ["REQTE.(S)"]


@pytest.mark.anyio
async def test_party_listing_endpoint(serving_db: str):
    async with _get_client(serving_db) as client:
        response = await client.get("/parties", params={"period": "2026-01"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 2
    assert payload["items"][0]["associated_event_count"] == 1


@pytest.mark.anyio
async def test_minister_profile_endpoint(serving_db: str):
    async with _get_client(serving_db) as client:
        response = await client.get("/ministers/TESTE/profile")
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["rapporteur"] == "MIN. TESTE"


@pytest.mark.anyio
async def test_minister_sequential_endpoint(serving_db: str):
    async with _get_client(serving_db) as client:
        response = await client.get("/ministers/TESTE/sequential")
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["sequential_bias_flag"] is True


@pytest.mark.anyio
async def test_assignment_audit_endpoint(serving_db: str):
    async with _get_client(serving_db) as client:
        response = await client.get("/audit/assignment")
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["process_class"] == "ADI"


@pytest.mark.anyio
async def test_counsel_ministers_endpoint_honors_limit(serving_db: str):
    async with _get_client(serving_db) as client:
        response = await client.get("/counsels/coun_1/ministers", params={"limit": 1})
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1


@pytest.mark.anyio
async def test_temporal_analysis_overview_endpoint(serving_db: str):
    async with _get_client(serving_db) as client:
        response = await client.get("/temporal-analysis")
    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["total_records"] == 5
    assert payload["minister_summaries"][0]["rapporteur"] == "MIN. TESTE"
    assert payload["breakpoints"][0]["breakpoint_flag"] is True


@pytest.mark.anyio
async def test_temporal_analysis_minister_endpoint(serving_db: str):
    async with _get_client(serving_db) as client:
        response = await client.get("/temporal-analysis/TESTE")
    assert response.status_code == 200
    payload = response.json()
    assert payload["minister"] == "TESTE"
    assert payload["monthly"][0]["decision_month"] == "2026-01"
    assert payload["yoy"][0]["process_class"] == "ADI"
    assert payload["events"][0]["event_id"] == "event_1"


@pytest.mark.anyio
async def test_cors_is_disabled_by_default(serving_db: str):
    async with _get_client(serving_db) as client:
        response = await client.get("/health", headers={"Origin": "https://example.com"})
    assert response.status_code == 200
    assert "access-control-allow-origin" not in response.headers


@pytest.mark.anyio
async def test_rate_limit_returns_429_when_limit_is_exceeded(serving_db: str, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ATLAS_STF_RATE_LIMIT_MAX_REQUESTS", "2")
    monkeypatch.setenv("ATLAS_STF_RATE_LIMIT_WINDOW_SECONDS", "60")
    async with _get_client(serving_db) as client:
        first = await client.get("/filters/options")
        second = await client.get("/filters/options")
        third = await client.get("/filters/options")
    assert first.status_code == 200
    assert second.status_code == 200
    assert third.status_code == 429
    assert third.json()["detail"] == "rate_limited"
    assert third.headers["X-RateLimit-Limit"] == "2"
    assert third.headers["X-RateLimit-Remaining"] == "0"
    assert int(third.headers["Retry-After"]) >= 1


@pytest.mark.anyio
async def test_rate_limit_ignores_forwarded_headers_by_default(serving_db: str, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ATLAS_STF_RATE_LIMIT_MAX_REQUESTS", "2")
    monkeypatch.setenv("ATLAS_STF_RATE_LIMIT_WINDOW_SECONDS", "60")
    monkeypatch.delenv("ATLAS_STF_TRUST_PROXY_HEADERS", raising=False)
    async with _get_client(serving_db) as client:
        first = await client.get("/filters/options", headers={"X-Forwarded-For": "1.1.1.1"})
        second = await client.get("/filters/options", headers={"X-Forwarded-For": "2.2.2.2"})
        third = await client.get("/filters/options", headers={"X-Forwarded-For": "3.3.3.3"})
    assert first.status_code == 200
    assert second.status_code == 200
    assert third.status_code == 429


@pytest.mark.anyio
async def test_rate_limit_can_trust_forwarded_headers_when_explicitly_enabled(
    serving_db: str,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("ATLAS_STF_RATE_LIMIT_MAX_REQUESTS", "2")
    monkeypatch.setenv("ATLAS_STF_RATE_LIMIT_WINDOW_SECONDS", "60")
    monkeypatch.setenv("ATLAS_STF_TRUST_PROXY_HEADERS", "true")
    async with _get_client(serving_db) as client:
        first = await client.get("/filters/options", headers={"X-Forwarded-For": "1.1.1.1"})
        second = await client.get("/filters/options", headers={"X-Forwarded-For": "2.2.2.2"})
        third = await client.get("/filters/options", headers={"X-Forwarded-For": "3.3.3.3"})
    assert first.status_code == 200
    assert second.status_code == 200
    assert third.status_code == 200


@pytest.mark.anyio
async def test_health_endpoint_is_exempt_from_rate_limit(serving_db: str, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ATLAS_STF_RATE_LIMIT_MAX_REQUESTS", "1")
    monkeypatch.setenv("ATLAS_STF_RATE_LIMIT_WINDOW_SECONDS", "60")
    async with _get_client(serving_db) as client:
        first = await client.get("/health")
        second = await client.get("/health")
    assert first.status_code == 200
    assert second.status_code == 200


@pytest.mark.anyio
async def test_cors_allows_only_configured_origins(serving_db: str, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ATLAS_STF_CORS_ORIGINS", "https://example.com")
    async with _get_client(serving_db) as client:
        response = await client.get("/health", headers={"Origin": "https://example.com"})
    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "https://example.com"


@pytest.mark.anyio
async def test_cors_preflight_uses_explicit_allow_headers(serving_db: str, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ATLAS_STF_CORS_ORIGINS", "https://example.com")
    async with _get_client(serving_db) as client:
        response = await client.options(
            "/health",
            headers={
                "Origin": "https://example.com",
                "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "content-type",
            },
        )
    assert response.status_code == 200
    assert response.headers["access-control-allow-headers"] == "Accept, Accept-Language, Content-Language, Content-Type"


@pytest.mark.anyio
async def test_request_timeout_middleware_returns_504_for_slow_request(
    serving_db: str, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("ATLAS_STF_REQUEST_TIMEOUT_SECONDS", "0.01")
    app = create_app(database_url=serving_db)

    @app.get("/_slow")
    def slow_route():
        time.sleep(0.05)
        return {"status": "ok"}

    async with _get_client_for_app(app) as client:
        response = await client.get("/_slow")

    assert response.status_code == 504
    assert response.json()["detail"] == "request_timeout"
