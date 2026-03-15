"""Tests for economic group serving layer (builder + model)."""

from __future__ import annotations

import json

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from atlas_stf.serving._builder_loaders_corporate import load_economic_groups
from atlas_stf.serving.builder import build_serving_database
from atlas_stf.serving.models import ServingEconomicGroup
from tests.api.conftest import managed_engine


def _write_jsonl(path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _write_json(path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _count(session: Session, model) -> int:
    return session.scalar(select(func.count()).select_from(model)) or 0


class TestLoadEconomicGroups:
    def test_loads_records_from_jsonl(self, tmp_path) -> None:
        analytics_dir = tmp_path / "analytics"
        _write_jsonl(
            analytics_dir / "economic_group.jsonl",
            [
                {
                    "group_id": "eg-abc123",
                    "member_cnpjs": ["11111111", "22222222"],
                    "razoes_sociais": ["EMPRESA A LTDA", "EMPRESA B LTDA"],
                    "member_count": 2,
                    "total_capital_social": 150000.0,
                    "cnae_labels": ["Advocacia"],
                    "ufs": ["SP", "RJ"],
                    "active_establishment_count": 3,
                    "total_establishment_count": 5,
                    "is_law_firm_group": True,
                    "has_minister_partner": True,
                    "has_party_partner": False,
                    "has_counsel_partner": True,
                    "generated_at": "2026-01-31T12:00:00+00:00",
                },
            ],
        )
        groups = load_economic_groups(analytics_dir)
        assert len(groups) == 1
        g = groups[0]
        assert g.group_id == "eg-abc123"
        assert g.member_count == 2
        assert g.is_law_firm_group is True
        assert g.has_minister_partner is True
        assert json.loads(g.member_cnpjs_json or "[]") == ["11111111", "22222222"]

    def test_missing_file_returns_empty(self, tmp_path) -> None:
        analytics_dir = tmp_path / "analytics"
        analytics_dir.mkdir(parents=True, exist_ok=True)
        groups = load_economic_groups(analytics_dir)
        assert groups == []

    def test_deduplicates_by_group_id(self, tmp_path) -> None:
        analytics_dir = tmp_path / "analytics"
        _write_jsonl(
            analytics_dir / "economic_group.jsonl",
            [
                {"group_id": "eg-dup", "member_cnpjs": ["11111111"], "member_count": 1},
                {"group_id": "eg-dup", "member_cnpjs": ["11111111"], "member_count": 1},
            ],
        )
        groups = load_economic_groups(analytics_dir)
        assert len(groups) == 1


class TestBuildServingDatabaseWithEconomicGroups:
    def test_materializes_economic_group_rows(self, tmp_path) -> None:
        database_url = f"sqlite+pysqlite:///{tmp_path / 'serving.db'}"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"

        # Minimal curated data required by the builder
        _write_jsonl(
            curated_dir / "process.jsonl",
            [{"process_id": "proc_1", "process_number": "ADI 1", "process_class": "ADI", "subjects_normalized": []}],
        )
        _write_jsonl(
            curated_dir / "decision_event.jsonl",
            [{"decision_event_id": "evt_1", "process_id": "proc_1", "decision_date": "2026-01-05"}],
        )
        _write_jsonl(curated_dir / "party.jsonl", [])
        _write_jsonl(curated_dir / "process_party_link.jsonl", [])
        _write_jsonl(curated_dir / "counsel.jsonl", [])
        _write_jsonl(curated_dir / "process_counsel_link.jsonl", [])
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
                    "evidence_summary": "Evidencia.",
                    "status": "novo",
                    "created_at": "2026-01-31T12:00:00",
                    "updated_at": "2026-01-31T12:05:00",
                }
            ],
        )
        _write_json(analytics_dir / "outlier_alert_summary.json", {"alert_count": 1, "avg_score": 0.87})
        _write_json(analytics_dir / "comparison_group_summary.json", {"valid_group_count": 1})
        _write_json(analytics_dir / "baseline_summary.json", {"baseline_count": 1})

        # Economic group data
        _write_jsonl(
            analytics_dir / "economic_group.jsonl",
            [
                {
                    "group_id": "eg-test1",
                    "member_cnpjs": ["11111111", "22222222"],
                    "razoes_sociais": ["EMPRESA A LTDA", "EMPRESA B LTDA"],
                    "member_count": 2,
                    "total_capital_social": 200000.0,
                    "cnae_labels": [],
                    "ufs": ["SP"],
                    "active_establishment_count": 1,
                    "total_establishment_count": 2,
                    "is_law_firm_group": False,
                    "has_minister_partner": True,
                    "has_party_partner": False,
                    "has_counsel_partner": False,
                    "generated_at": "2026-01-31T12:00:00+00:00",
                },
            ],
        )
        _write_json(analytics_dir / "economic_group_summary.json", {"total_groups": 1})

        build_serving_database(database_url=database_url, curated_dir=curated_dir, analytics_dir=analytics_dir)

        with managed_engine(database_url) as engine:
            with Session(engine) as session:
                count = _count(session, ServingEconomicGroup)
                assert count == 1
                row = session.get(ServingEconomicGroup, "eg-test1")
                assert row is not None
                assert row.member_count == 2
                assert row.has_minister_partner is True
                assert row.is_law_firm_group is False
                assert json.loads(row.member_cnpjs_json or "[]") == ["11111111", "22222222"]
                assert json.loads(row.razoes_sociais_json or "[]") == ["EMPRESA A LTDA", "EMPRESA B LTDA"]

    def test_no_economic_group_file_still_builds(self, tmp_path) -> None:
        """Builder completes without error when economic_group.jsonl is absent."""
        database_url = f"sqlite+pysqlite:///{tmp_path / 'serving.db'}"
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
        _write_jsonl(curated_dir / "party.jsonl", [])
        _write_jsonl(curated_dir / "process_party_link.jsonl", [])
        _write_jsonl(curated_dir / "counsel.jsonl", [])
        _write_jsonl(curated_dir / "process_counsel_link.jsonl", [])
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
                    "evidence_summary": "Evidencia.",
                    "status": "novo",
                    "created_at": "2026-01-31T12:00:00",
                    "updated_at": "2026-01-31T12:05:00",
                }
            ],
        )
        _write_json(analytics_dir / "outlier_alert_summary.json", {"alert_count": 1, "avg_score": 0.87})
        _write_json(analytics_dir / "comparison_group_summary.json", {"valid_group_count": 1})
        _write_json(analytics_dir / "baseline_summary.json", {"baseline_count": 1})

        build_serving_database(database_url=database_url, curated_dir=curated_dir, analytics_dir=analytics_dir)

        with managed_engine(database_url) as engine:
            with Session(engine) as session:
                count = _count(session, ServingEconomicGroup)
                assert count == 0
