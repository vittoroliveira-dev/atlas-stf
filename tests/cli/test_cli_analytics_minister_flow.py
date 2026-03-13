from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.cli import main


def test_cli_analytics_minister_flow(tmp_path: Path):
    decision_event_path = tmp_path / "decision_event.jsonl"
    process_path = tmp_path / "process.jsonl"
    output = tmp_path / "minister_flow.json"
    decision_event_path.write_text(
        json.dumps(
            {
                "decision_event_id": "de_1",
                "process_id": "proc_1",
                "decision_date": "2026-01-10",
                "current_rapporteur": "MIN. DIAS TOFFOLI",
                "decision_type": "Despacho",
                "decision_progress": "DESPACHO",
                "judging_body": "MONOCRÁTICA",
                "is_collegiate": False,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    process_path.write_text(
        json.dumps(
            {
                "process_id": "proc_1",
                "process_class": "AC",
                "subjects_normalized": ["TEMA A"],
                "branch_of_law": None,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    code = main(
        [
            "analytics",
            "minister-flow",
            "--minister",
            "TOFFOLI",
            "--year",
            "2026",
            "--month",
            "1",
            "--decision-event-path",
            str(decision_event_path),
            "--process-path",
            str(process_path),
            "--output",
            str(output),
        ]
    )

    assert code == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["status"] == "ok"
    assert payload["event_count"] == 1
    assert payload["process_class_distribution"] == {"AC": 1}


def test_cli_analytics_minister_flow_monocratic_filter(tmp_path: Path):
    decision_event_path = tmp_path / "decision_event.jsonl"
    process_path = tmp_path / "process.jsonl"
    output = tmp_path / "minister_flow.json"
    decision_event_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "decision_event_id": "de_1",
                        "process_id": "proc_1",
                        "decision_date": "2026-01-10",
                        "current_rapporteur": "MIN. DIAS TOFFOLI",
                        "decision_type": "Despacho",
                        "decision_progress": "DESPACHO",
                        "judging_body": "MONOCRÁTICA",
                        "is_collegiate": False,
                    }
                ),
                json.dumps(
                    {
                        "decision_event_id": "de_2",
                        "process_id": "proc_2",
                        "decision_date": "2026-01-10",
                        "current_rapporteur": "MIN. DIAS TOFFOLI",
                        "decision_type": "Decisão Final",
                        "decision_progress": "NEGOU PROVIMENTO",
                        "judging_body": "TURMA",
                        "is_collegiate": True,
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    process_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "process_id": "proc_1",
                        "process_class": "AC",
                        "subjects_normalized": ["TEMA A"],
                        "branch_of_law": None,
                    }
                ),
                json.dumps(
                    {
                        "process_id": "proc_2",
                        "process_class": "RCL",
                        "subjects_normalized": [],
                        "branch_of_law": "DIREITO X",
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    code = main(
        [
            "analytics",
            "minister-flow",
            "--minister",
            "TOFFOLI",
            "--year",
            "2026",
            "--month",
            "1",
            "--monocratic-only",
            "--decision-event-path",
            str(decision_event_path),
            "--process-path",
            str(process_path),
            "--output",
            str(output),
        ]
    )

    assert code == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["collegiate_filter"] == "monocratico"
    assert payload["event_count"] == 1
    assert payload["process_class_distribution"] == {"AC": 1}
