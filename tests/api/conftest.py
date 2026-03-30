from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session

from atlas_stf.serving.builder import build_serving_database
from atlas_stf.serving.models import ServingMlOutlierScore


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _register_py_lower(dbapi_conn, _connection_record):
    dbapi_conn.create_function("py_lower", 1, lambda v: v.lower() if isinstance(v, str) else v)


@contextmanager
def managed_engine(database_url: str) -> Iterator:
    engine = create_engine(database_url)
    event.listen(engine, "connect", _register_py_lower)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture()
def serving_db(tmp_path: Path) -> str:
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
                "juris_inteiro_teor_url": "https://example.com/adi1.pdf",
                "juris_doc_count": 2,
                "juris_has_acordao": True,
                "juris_has_decisao_monocratica": False,
            },
            {
                "process_id": "proc_2",
                "process_number": "RCL 2",
                "process_class": "RCL",
                "branch_of_law": "DIREITO TRIBUTÁRIO",
                "subjects_normalized": ["DIREITO TRIBUTÁRIO"],
                "origin_description": "SAO PAULO",
                "juris_inteiro_teor_url": None,
                "juris_doc_count": 1,
                "juris_has_acordao": False,
                "juris_has_decisao_monocratica": True,
            },
            {
                "process_id": "proc_3",
                "process_number": "RE 3",
                "process_class": "RE",
                "branch_of_law": "DIREITO CONSTITUCIONAL",
                "subjects_normalized": ["DIREITO CONSTITUCIONAL"],
                "origin_description": "RIO DE JANEIRO",
                "juris_inteiro_teor_url": None,
                "juris_doc_count": 1,
                "juris_has_acordao": True,
                "juris_has_decisao_monocratica": False,
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
                "decision_type": "Decisão Final",
                "decision_progress": "Procedente",
                "decision_origin": "JULGAMENTO",
                "judging_body": "PLENO",
                "is_collegiate": True,
                "decision_note": "Decisão colegiada materializada.",
            },
            {
                "decision_event_id": "evt_2",
                "process_id": "proc_2",
                "decision_date": "2026-01-12",
                "current_rapporteur": "MIN. OUTRO",
                "decision_type": "Decisão Liminar",
                "decision_progress": "Deferido",
                "decision_origin": "DECISÃO",
                "judging_body": "MONOCRÁTICA",
                "is_collegiate": False,
                "decision_note": "Decisão monocrática materializada.",
            },
            {
                "decision_event_id": "evt_3",
                "process_id": "proc_3",
                "decision_date": "2026-01-18",
                "current_rapporteur": "MIN. TERCEIRO",
                "decision_type": "Acórdão",
                "decision_progress": "Parcialmente procedente",
                "decision_origin": "JULGAMENTO",
                "judging_body": "TURMA",
                "is_collegiate": True,
                "decision_note": "Segundo contexto para a mesma entidade.",
            },
        ],
    )
    _write_jsonl(
        curated_dir / "party.jsonl",
        [
            {
                "party_id": "party_1",
                "party_name_raw": "AUTOR A",
                "party_name_normalized": "AUTOR A",
                "notes": None,
            },
            {
                "party_id": "party_2",
                "party_name_raw": "REU B",
                "party_name_normalized": "REU B",
                "notes": None,
            },
        ],
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
            },
            {
                "link_id": "pp_2",
                "process_id": "proc_1",
                "party_id": "party_2",
                "role_in_case": "REQDO.(A/S)",
                "source_id": "juris",
            },
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
            },
            {
                "counsel_id": "coun_2",
                "counsel_name_raw": "ADVOGADO B",
                "counsel_name_normalized": "ADVOGADO B",
                "notes": None,
            },
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
            },
            {
                "link_id": "pc_2",
                "process_id": "proc_1",
                "counsel_id": "coun_2",
                "side_in_case": "REQDO.(A/S)",
                "source_id": "juris",
            },
            {
                "link_id": "pc_3",
                "process_id": "proc_3",
                "counsel_id": "coun_1",
                "side_in_case": "AM. CURIAE.",
                "source_id": "juris",
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
                "expected_pattern": "Esperado colegiado homogêneo.",
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
    _write_json(analytics_dir / "comparison_group_summary.json", {"valid_group_count": 3})
    _write_json(analytics_dir / "baseline_summary.json", {"baseline_count": 2})
    _write_jsonl(
        analytics_dir / "rapporteur_profile.jsonl",
        [
            {
                "rapporteur": "MIN. TESTE",
                "process_class": "ADI",
                "thematic_key": "DIREITO ADMINISTRATIVO",
                "decision_year": 2026,
                "event_count": 1,
                "progress_distribution": {"Procedente": 1},
                "group_progress_distribution": {"Procedente": 1, "Improcedente": 1},
                "group_event_count": 2,
                "chi2_statistic": 1.0,
                "p_value_approx": 0.31,
                "deviation_flag": False,
                "deviation_direction": "sem desvio significativo",
            }
        ],
    )
    _write_json(analytics_dir / "rapporteur_profile_summary.json", {"total_profiles": 1, "deviation_count": 0})
    _write_jsonl(
        analytics_dir / "sequential_analysis.jsonl",
        [
            {
                "rapporteur": "MIN. TESTE",
                "decision_year": 2026,
                "n_decisions": 12,
                "n_favorable": 7,
                "n_unfavorable": 5,
                "autocorrelation_lag1": 0.2,
                "streak_effect_3": 0.1,
                "streak_effect_5": None,
                "base_favorable_rate": 0.583333,
                "post_streak_favorable_rate_3": 0.68,
                "post_streak_favorable_rate_5": None,
                "sequential_bias_flag": True,
            }
        ],
    )
    _write_json(analytics_dir / "sequential_analysis_summary.json", {"total_analyses": 1, "bias_flagged_count": 1})
    _write_jsonl(
        analytics_dir / "assignment_audit.jsonl",
        [
            {
                "process_class": "ADI",
                "decision_year": 2026,
                "rapporteur_count": 2,
                "event_count": 10,
                "rapporteur_distribution": {"MIN. TESTE": 7, "MIN. OUTRO": 3},
                "chi2_statistic": 1.6,
                "p_value_approx": 0.2,
                "uniformity_flag": True,
                "most_overrepresented_rapporteur": "MIN. TESTE",
                "most_underrepresented_rapporteur": "MIN. OUTRO",
            }
        ],
    )
    _write_json(analytics_dir / "assignment_audit_summary.json", {"total_audits": 1, "uniform_count": 1})
    _write_jsonl(
        analytics_dir / "ml_outlier_score.jsonl",
        [
            {
                "decision_event_id": "evt_1",
                "comparison_group_id": "grp_1",
                "ml_anomaly_score": -0.12,
                "ml_rarity_score": 0.82,
                "ensemble_score": 0.85,
                "n_features": 4,
                "n_samples": 25,
                "generated_at": "2026-01-31T12:10:00+00:00",
            }
        ],
    )
    _write_json(analytics_dir / "ml_outlier_score_summary.json", {"record_count": 1, "groups_processed": 1})
    _write_jsonl(
        analytics_dir / "temporal_analysis.jsonl",
        [
            {
                "analysis_kind": "monthly_minister",
                "record_id": "temp_month_1",
                "rapporteur": "MIN. TESTE",
                "decision_month": "2026-01",
                "decision_year": 2026,
                "decision_count": 6,
                "favorable_count": 4,
                "unfavorable_count": 2,
                "favorable_rate": 0.666667,
                "rolling_favorable_rate_6m": 0.55,
                "breakpoint_score": 2.8,
                "breakpoint_flag": True,
                "generated_at": "2026-01-31T12:20:00+00:00",
            },
            {
                "analysis_kind": "yoy_process_class",
                "record_id": "temp_yoy_1",
                "rapporteur": "MIN. TESTE",
                "process_class": "ADI",
                "decision_year": 2026,
                "decision_count": 8,
                "favorable_count": 6,
                "unfavorable_count": 2,
                "favorable_rate": 0.75,
                "prior_decision_count": 5,
                "prior_favorable_rate": 0.4,
                "delta_vs_prior_year": 0.35,
                "generated_at": "2026-01-31T12:20:00+00:00",
            },
            {
                "analysis_kind": "seasonality",
                "record_id": "temp_season_1",
                "rapporteur": "MIN. TESTE",
                "month_of_year": 1,
                "decision_count": 9,
                "favorable_count": 6,
                "unfavorable_count": 3,
                "favorable_rate": 0.666667,
                "delta_vs_overall": 0.12,
                "generated_at": "2026-01-31T12:20:00+00:00",
            },
            {
                "analysis_kind": "event_window",
                "record_id": "temp_event_1",
                "rapporteur": "MIN. TESTE",
                "event_id": "event_1",
                "event_type": "nomeacao",
                "event_scope": "minister",
                "event_date": "2026-01-01",
                "event_title": "Nomeação sintética",
                "source": "DIÁRIO OFICIAL",
                "source_url": "https://example.com/event-1",
                "status": "comparativo",
                "before_decision_count": 6,
                "before_favorable_rate": 0.2,
                "after_decision_count": 6,
                "after_favorable_rate": 0.7,
                "delta_before_after": 0.5,
                "generated_at": "2026-01-31T12:20:00+00:00",
            },
            {
                "analysis_kind": "corporate_link_timeline",
                "record_id": "temp_corp_1",
                "rapporteur": "MIN. TESTE",
                "linked_entity_type": "party",
                "linked_entity_id": "party_1",
                "linked_entity_name": "AUTOR A",
                "company_cnpj_basico": "11111111",
                "company_name": "EMPRESA TEMPORAL LTDA",
                "link_degree": 1,
                "link_start_date": "2025-03-01",
                "link_status": "ativo_desde_entrada",
                "decision_count": 3,
                "favorable_count": 2,
                "unfavorable_count": 1,
                "favorable_rate": 0.666667,
                "generated_at": "2026-01-31T12:20:00+00:00",
            },
        ],
    )
    _write_json(
        analytics_dir / "temporal_analysis_summary.json",
        {
            "generated_at": "2026-01-31T12:20:00+00:00",
            "total_records": 5,
            "counts_by_kind": {
                "monthly_minister": 1,
                "yoy_process_class": 1,
                "seasonality": 1,
                "event_window": 1,
                "corporate_link_timeline": 1,
            },
            "ministers_covered": 1,
            "events_covered": 1,
            "rolling_window_months": 6,
            "event_window_days": 180,
        },
    )

    database_url = f"sqlite+pysqlite:///{tmp_path / 'serving.db'}"
    build_serving_database(
        database_url=database_url,
        curated_dir=curated_dir,
        analytics_dir=analytics_dir,
    )

    with managed_engine(database_url) as engine:
        with Session(engine) as session:
            assert session.scalar(select(ServingMlOutlierScore.id)) is not None
    return database_url
