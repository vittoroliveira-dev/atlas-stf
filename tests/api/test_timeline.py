from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from atlas_stf.api import create_app
from atlas_stf.serving.builder import build_serving_database
from atlas_stf.serving.models import ServingMovement, ServingSessionEvent
from tests.api.conftest import managed_engine


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


@asynccontextmanager
async def _get_client(database_url: str) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(database_url=database_url)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            yield client


@pytest.fixture()
def timeline_db(tmp_path: Path) -> str:
    """Build a serving database with movements and session events."""
    curated_dir = tmp_path / "curated"
    analytics_dir = tmp_path / "analytics"

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
                "first_distribution_date": "2025-06-01",
            },
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
            },
        ],
    )
    _write_jsonl(
        curated_dir / "party.jsonl",
        [{"party_id": "party_1", "party_name_raw": "AUTOR A", "party_name_normalized": "AUTOR A", "notes": None}],
    )
    _write_jsonl(
        curated_dir / "process_party_link.jsonl",
        [{"link_id": "pp_1", "process_id": "proc_1", "party_id": "party_1", "role_in_case": "REQTE.(S)"}],
    )
    _write_jsonl(
        curated_dir / "counsel.jsonl",
        [{"counsel_id": "coun_1", "counsel_name_raw": "ADV A", "counsel_name_normalized": "ADV A", "notes": None}],
    )
    _write_jsonl(
        curated_dir / "process_counsel_link.jsonl",
        [{"link_id": "pc_1", "process_id": "proc_1", "counsel_id": "coun_1", "side_in_case": "REQTE.(S)"}],
    )
    _write_jsonl(
        curated_dir / "movement.jsonl",
        [
            {
                "movement_id": "mov_1",
                "process_id": "proc_1",
                "source_system": "stf_portal",
                "tpu_code": 123,
                "tpu_name": "Distribuicao",
                "movement_category": "distribuicao",
                "movement_raw_description": "Distribuido por sorteio",
                "movement_date": "2025-06-01",
                "movement_detail": "Detalhe",
                "rapporteur_at_event": "MIN. TESTE",
                "tpu_match_confidence": "exact",
                "normalization_method": "tpu_v2",
            },
            {
                "movement_id": "mov_2",
                "process_id": "proc_1",
                "source_system": "stf_portal",
                "tpu_code": 456,
                "tpu_name": "Julgamento",
                "movement_category": "julgamento",
                "movement_raw_description": "Julgado em sessao plenaria",
                "movement_date": "2026-01-05",
                "rapporteur_at_event": "MIN. TESTE",
                "tpu_match_confidence": "exact",
                "normalization_method": "tpu_v2",
            },
        ],
    )
    _write_jsonl(
        curated_dir / "session_event.jsonl",
        [
            {
                "session_event_id": "sess_1",
                "process_id": "proc_1",
                "movement_id": "mov_2",
                "source_system": "stf_portal",
                "session_type": "plenario",
                "event_type": "julgamento",
                "event_date": "2026-01-05",
                "rapporteur_at_event": "MIN. TESTE",
                "vista_duration_days": None,
            },
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
                "expected_pattern": "Esperado.",
                "observed_pattern": "Observado.",
                "evidence_summary": "Desvio.",
                "status": "novo",
                "created_at": "2026-01-31T12:00:00",
                "updated_at": "2026-01-31T12:05:00",
            },
        ],
    )
    _write_json(analytics_dir / "outlier_alert_summary.json", {"alert_count": 1, "avg_score": 0.87})
    _write_json(analytics_dir / "comparison_group_summary.json", {"valid_group_count": 1})
    _write_json(analytics_dir / "baseline_summary.json", {"baseline_count": 1})

    database_url = f"sqlite+pysqlite:///{tmp_path / 'serving.db'}"
    build_serving_database(database_url=database_url, curated_dir=curated_dir, analytics_dir=analytics_dir)
    return database_url


def test_builder_loads_movements_and_session_events(timeline_db: str):
    with managed_engine(timeline_db) as engine:
        with Session(engine) as session:
            movement_count = session.scalar(select(func.count()).select_from(ServingMovement)) or 0
            assert movement_count == 2
            session_event_count = session.scalar(select(func.count()).select_from(ServingSessionEvent)) or 0
            assert session_event_count == 1


def test_builder_loads_first_distribution_date(timeline_db: str):
    from atlas_stf.serving.models import ServingCase

    with managed_engine(timeline_db) as engine:
        with Session(engine) as session:
            case = session.get(ServingCase, "evt_1")
            assert case is not None
            assert case.first_distribution_date == "2025-06-01"


def test_builder_handles_missing_timeline_files(tmp_path: Path):
    curated_dir = tmp_path / "curated"
    analytics_dir = tmp_path / "analytics"

    _write_jsonl(
        curated_dir / "process.jsonl",
        [{"process_id": "proc_1", "process_number": "ADI 1", "process_class": "ADI", "subjects_normalized": []}],
    )
    _write_jsonl(
        curated_dir / "decision_event.jsonl",
        [{"decision_event_id": "evt_1", "process_id": "proc_1", "decision_date": "2026-01-05"}],
    )
    _write_jsonl(
        curated_dir / "party.jsonl",
        [{"party_id": "party_1", "party_name_raw": "AUTOR A", "party_name_normalized": "AUTOR A", "notes": None}],
    )
    _write_jsonl(
        curated_dir / "process_party_link.jsonl",
        [{"link_id": "pp_1", "process_id": "proc_1", "party_id": "party_1", "role_in_case": "REQTE.(S)"}],
    )
    _write_jsonl(
        curated_dir / "counsel.jsonl",
        [{"counsel_id": "coun_1", "counsel_name_raw": "ADV A", "counsel_name_normalized": "ADV A", "notes": None}],
    )
    _write_jsonl(
        curated_dir / "process_counsel_link.jsonl",
        [{"link_id": "pc_1", "process_id": "proc_1", "counsel_id": "coun_1", "side_in_case": "REQTE.(S)"}],
    )
    _write_jsonl(
        analytics_dir / "outlier_alert.jsonl",
        [
            {
                "alert_id": "a1",
                "process_id": "proc_1",
                "decision_event_id": "evt_1",
                "comparison_group_id": "g1",
                "alert_type": "atipicidade",
                "alert_score": 0.5,
                "expected_pattern": "E.",
                "observed_pattern": "O.",
                "evidence_summary": "S.",
                "status": "novo",
                "created_at": "2026-01-31T12:00:00",
                "updated_at": "2026-01-31T12:05:00",
            },
        ],
    )
    _write_json(analytics_dir / "outlier_alert_summary.json", {"alert_count": 1, "avg_score": 0.5})
    _write_json(analytics_dir / "comparison_group_summary.json", {"valid_group_count": 1})
    _write_json(analytics_dir / "baseline_summary.json", {"baseline_count": 1})

    # No movement.jsonl or session_event.jsonl => should still build successfully
    database_url = f"sqlite+pysqlite:///{tmp_path / 'serving.db'}"
    result = build_serving_database(database_url=database_url, curated_dir=curated_dir, analytics_dir=analytics_dir)
    assert result.case_count == 1

    with managed_engine(database_url) as engine:
        with Session(engine) as session:
            assert (session.scalar(select(func.count()).select_from(ServingMovement)) or 0) == 0
            assert (session.scalar(select(func.count()).select_from(ServingSessionEvent)) or 0) == 0


@pytest.mark.anyio
async def test_timeline_endpoint_returns_empty_when_no_data(timeline_db: str):
    async with _get_client(timeline_db) as client:
        response = await client.get("/caso/nonexistent/timeline")
    assert response.status_code == 200
    payload = response.json()
    assert payload["process_id"] == "nonexistent"
    assert payload["movements"] == []
    assert payload["session_events"] == []
    assert payload["total_movements"] == 0
    assert payload["total_session_events"] == 0


@pytest.mark.anyio
async def test_timeline_endpoint_returns_movements_and_sessions(timeline_db: str):
    async with _get_client(timeline_db) as client:
        response = await client.get("/caso/proc_1/timeline")
    assert response.status_code == 200
    payload = response.json()
    assert payload["process_id"] == "proc_1"
    assert payload["total_movements"] == 2
    assert payload["total_session_events"] == 1

    movements = payload["movements"]
    assert movements[0]["movement_id"] == "mov_1"
    assert movements[0]["tpu_code"] == 123
    assert movements[0]["movement_category"] == "distribuicao"
    assert movements[0]["movement_date"] == "2025-06-01"
    assert movements[0]["rapporteur_at_event"] == "MIN. TESTE"
    assert movements[0]["tpu_match_confidence"] == "exact"
    assert movements[0]["normalization_method"] == "tpu_v2"
    assert movements[1]["movement_id"] == "mov_2"
    assert movements[1]["movement_date"] == "2026-01-05"

    session_events = payload["session_events"]
    assert session_events[0]["session_event_id"] == "sess_1"
    assert session_events[0]["movement_id"] == "mov_2"
    assert session_events[0]["session_type"] == "plenario"
    assert session_events[0]["event_type"] == "julgamento"
    assert session_events[0]["event_date"] == "2026-01-05"


@pytest.mark.anyio
async def test_sessions_endpoint_returns_session_events(timeline_db: str):
    async with _get_client(timeline_db) as client:
        response = await client.get("/caso/proc_1/sessions")
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["session_event_id"] == "sess_1"
    assert payload[0]["process_id"] == "proc_1"
    assert payload[0]["source_system"] == "stf_portal"


@pytest.mark.anyio
async def test_sessions_endpoint_returns_empty_for_unknown_process(timeline_db: str):
    async with _get_client(timeline_db) as client:
        response = await client.get("/caso/nonexistent/sessions")
    assert response.status_code == 200
    payload = response.json()
    assert payload == []
