from __future__ import annotations

import json
from pathlib import Path

import pytest

from atlas_stf.analytics.temporal_analysis import build_temporal_analysis


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _records_for_kind(records: list[dict], kind: str) -> list[dict]:
    return [record for record in records if record["analysis_kind"] == kind]


def test_build_temporal_analysis_materializes_expected_views(tmp_path: Path) -> None:
    curated_dir = tmp_path / "curated"
    analytics_dir = tmp_path / "analytics"
    events_dir = tmp_path / "raw" / "external_events"
    rfb_dir = tmp_path / "raw" / "rfb"

    _write_json(
        curated_dir / "minister_bio.json",
        {
            "m1": {
                "minister_name": "MIN. TESTE",
                "appointment_date": "2020-01-01",
                "appointing_president": "PRESIDENTE X",
            }
        },
    )
    _write_jsonl(
        curated_dir / "process.jsonl",
        [
            {
                "process_id": "proc_a",
                "process_class": "ADI",
                "subjects_normalized": ["TEMA A"],
                "branch_of_law": "DIREITO A",
            },
            {
                "process_id": "proc_b",
                "process_class": "ADI",
                "subjects_normalized": ["TEMA B"],
                "branch_of_law": "DIREITO A",
            },
        ],
    )

    decision_rows: list[dict] = []
    for idx, month in enumerate(range(1, 7), start=1):
        decision_rows.append(
            {
                "decision_event_id": f"evt_before_{idx}",
                "process_id": "proc_a",
                "decision_date": f"2025-{month:02d}-15",
                "decision_year": 2025,
                "current_rapporteur": "MIN. TESTE",
                "decision_progress": "Improcedente",
                "judging_body": "PLENO",
                "is_collegiate": True,
            }
        )
    for idx, month in enumerate(range(7, 13), start=1):
        decision_rows.append(
            {
                "decision_event_id": f"evt_after_{idx}",
                "process_id": "proc_a",
                "decision_date": f"2025-{month:02d}-15",
                "decision_year": 2025,
                "current_rapporteur": "MIN. TESTE",
                "decision_progress": "Procedente",
                "judging_body": "PLENO",
                "is_collegiate": True,
            }
        )
    for idx, month in enumerate(range(1, 7), start=1):
        decision_rows.append(
            {
                "decision_event_id": f"evt_yoy_{idx}",
                "process_id": "proc_b",
                "decision_date": f"2026-{month:02d}-20",
                "decision_year": 2026,
                "current_rapporteur": "MIN. TESTE",
                "decision_progress": "Procedente",
                "judging_body": "PLENO",
                "is_collegiate": True,
            }
        )
    _write_jsonl(curated_dir / "decision_event.jsonl", decision_rows)

    _write_jsonl(
        curated_dir / "party.jsonl",
        [{"party_id": "party_1", "party_name_raw": "AUTOR A", "party_name_normalized": "AUTOR A"}],
    )
    _write_jsonl(
        curated_dir / "counsel.jsonl",
        [{"counsel_id": "coun_1", "counsel_name_raw": "ADV A", "counsel_name_normalized": "ADV A"}],
    )
    _write_jsonl(
        curated_dir / "process_party_link.jsonl",
        [
            {"link_id": "pp_1", "process_id": "proc_a", "party_id": "party_1", "role_in_case": "REQTE.(S)"},
            {"link_id": "pp_2", "process_id": "proc_b", "party_id": "party_1", "role_in_case": "REQTE.(S)"},
        ],
    )
    _write_jsonl(curated_dir / "process_counsel_link.jsonl", [])

    _write_jsonl(
        events_dir / "events.jsonl",
        [
            {
                "event_id": "event_1",
                "event_date": "2025-07-01",
                "event_type": "nomeacao",
                "event_scope": "minister",
                "minister_name": "MIN. TESTE",
                "title": "Nomeação sintética",
                "source": "DIÁRIO OFICIAL",
                "source_url": "https://example.com/event-1",
                "summary": "Evento de teste.",
                "editorial_confidence": "alto",
            }
        ],
    )

    _write_jsonl(
        rfb_dir / "partners_raw.jsonl",
        [
            {
                "cnpj_basico": "11111111",
                "partner_type": "2",
                "partner_name": "MIN. TESTE",
                "partner_name_normalized": "MIN. TESTE",
                "partner_cpf_cnpj": "00011122233",
                "qualification_code": "49",
                "entry_date": "20240101",
            },
            {
                "cnpj_basico": "11111111",
                "partner_type": "2",
                "partner_name": "AUTOR A",
                "partner_name_normalized": "AUTOR A",
                "partner_cpf_cnpj": "99988877766",
                "qualification_code": "22",
                "entry_date": "20250301",
            },
        ],
    )
    _write_jsonl(
        rfb_dir / "companies_raw.jsonl",
        [{"cnpj_basico": "11111111", "razao_social": "EMPRESA TEMPORAL LTDA"}],
    )

    output_path = build_temporal_analysis(
        decision_event_path=curated_dir / "decision_event.jsonl",
        process_path=curated_dir / "process.jsonl",
        minister_bio_path=curated_dir / "minister_bio.json",
        party_path=curated_dir / "party.jsonl",
        counsel_path=curated_dir / "counsel.jsonl",
        process_party_link_path=curated_dir / "process_party_link.jsonl",
        process_counsel_link_path=curated_dir / "process_counsel_link.jsonl",
        external_events_dir=events_dir,
        rfb_dir=rfb_dir,
        output_dir=analytics_dir,
    )

    assert output_path.exists()
    records = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]

    monthly = _records_for_kind(records, "monthly_minister")
    yoy = _records_for_kind(records, "yoy_process_class")
    seasonality = _records_for_kind(records, "seasonality")
    event_windows = _records_for_kind(records, "event_window")
    corporate = _records_for_kind(records, "corporate_link_timeline")

    assert len(monthly) == 18
    july_2025 = next(
        record for record in monthly if record["rapporteur"] == "MIN. TESTE" and record["decision_month"] == "2025-07"
    )
    assert july_2025["breakpoint_flag"] is True
    assert july_2025["rolling_favorable_rate_6m"] == pytest.approx(round(1 / 6, 6), rel=1e-6)

    yoy_record = next(
        record
        for record in yoy
        if record["rapporteur"] == "MIN. TESTE" and record["process_class"] == "ADI" and record["decision_year"] == 2026
    )
    assert yoy_record["prior_favorable_rate"] == pytest.approx(0.5)
    assert yoy_record["current_favorable_rate"] == pytest.approx(1.0)
    assert yoy_record["delta_vs_prior_year"] == pytest.approx(0.5)

    january = next(
        record for record in seasonality if record["rapporteur"] == "MIN. TESTE" and record["month_of_year"] == 1
    )
    assert january["decision_count"] == 2

    event_record = next(record for record in event_windows if record["event_id"] == "event_1")
    assert event_record["status"] == "comparativo"
    assert event_record["before_favorable_rate"] == pytest.approx(0.0)
    assert event_record["after_favorable_rate"] == pytest.approx(1.0)
    assert event_record["delta_before_after"] == pytest.approx(1.0)

    corporate_record = next(record for record in corporate if record["linked_entity_id"] == "party_1")
    assert corporate_record["link_start_date"] == "2025-03-01"
    assert corporate_record["link_status"] == "ativo_desde_entrada"

    summary = json.loads((analytics_dir / "temporal_analysis_summary.json").read_text(encoding="utf-8"))
    assert summary["total_records"] == len(records)
    assert summary["counts_by_kind"]["monthly_minister"] == len(monthly)
