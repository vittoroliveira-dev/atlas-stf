from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.cli import main


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_cli_analytics_build_temporal_analysis(tmp_path: Path) -> None:
    curated_dir = tmp_path / "curated"
    output_dir = tmp_path / "analytics"
    external_events_dir = tmp_path / "external_events"
    rfb_dir = tmp_path / "rfb"

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
                "process_id": "proc_1",
                "process_class": "ADI",
                "subjects_normalized": ["TEMA A"],
                "branch_of_law": "DIREITO A",
            }
        ],
    )
    _write_jsonl(
        curated_dir / "decision_event.jsonl",
        [
            {
                "decision_event_id": "evt_1",
                "process_id": "proc_1",
                "decision_date": "2025-01-10",
                "decision_year": 2025,
                "current_rapporteur": "MIN. TESTE",
                "decision_progress": "Improcedente",
                "judging_body": "PLENO",
                "is_collegiate": True,
            },
            {
                "decision_event_id": "evt_2",
                "process_id": "proc_1",
                "decision_date": "2025-07-10",
                "decision_year": 2025,
                "current_rapporteur": "MIN. TESTE",
                "decision_progress": "Procedente",
                "judging_body": "PLENO",
                "is_collegiate": True,
            },
        ],
    )
    _write_jsonl(curated_dir / "party.jsonl", [])
    _write_jsonl(curated_dir / "counsel.jsonl", [])
    _write_jsonl(curated_dir / "process_party_link.jsonl", [])
    _write_jsonl(curated_dir / "process_counsel_link.jsonl", [])
    _write_jsonl(
        external_events_dir / "events.jsonl",
        [
            {
                "event_id": "event_1",
                "event_date": "2025-07-01",
                "event_type": "nomeacao",
                "event_scope": "minister",
                "minister_name": "MIN. TESTE",
                "title": "Nomeação",
                "source": "DIÁRIO OFICIAL",
                "source_url": "https://example.com/event-1",
                "summary": "Evento sintético",
                "editorial_confidence": "alto",
            }
        ],
    )
    _write_jsonl(rfb_dir / "partners_raw.jsonl", [])
    _write_jsonl(rfb_dir / "companies_raw.jsonl", [])

    code = main(
        [
            "analytics",
            "build-temporal-analysis",
            "--decision-event-path",
            str(curated_dir / "decision_event.jsonl"),
            "--process-path",
            str(curated_dir / "process.jsonl"),
            "--minister-bio-path",
            str(curated_dir / "minister_bio.json"),
            "--party-path",
            str(curated_dir / "party.jsonl"),
            "--counsel-path",
            str(curated_dir / "counsel.jsonl"),
            "--process-party-link-path",
            str(curated_dir / "process_party_link.jsonl"),
            "--process-counsel-link-path",
            str(curated_dir / "process_counsel_link.jsonl"),
            "--external-events-dir",
            str(external_events_dir),
            "--rfb-dir",
            str(rfb_dir),
            "--output-dir",
            str(output_dir),
        ]
    )

    assert code == 0
    assert (output_dir / "temporal_analysis.jsonl").exists()
    assert (output_dir / "temporal_analysis_summary.json").exists()
