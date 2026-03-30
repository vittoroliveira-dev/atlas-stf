from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics.rapporteur_change import build_rapporteur_changes


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def test_detects_rapporteur_change(tmp_path: Path):
    event_path = tmp_path / "event.jsonl"
    process_path = tmp_path / "process.jsonl"
    output_dir = tmp_path / "analytics"
    output_dir.mkdir()

    _write_jsonl(
        event_path,
        [
            {
                "decision_event_id": "de_1",
                "process_id": "proc_1",
                "decision_date": "2020-01-01",
                "decision_year": 2020,
                "current_rapporteur": "MIN A",
                "decision_progress": "Negou provimento",
            },
            {
                "decision_event_id": "de_2",
                "process_id": "proc_1",
                "decision_date": "2020-06-01",
                "decision_year": 2020,
                "current_rapporteur": "MIN B",
                "decision_progress": "Provido",
            },
        ],
    )
    _write_jsonl(
        process_path,
        [{"process_id": "proc_1", "process_class": "RE"}],
    )

    result = build_rapporteur_changes(
        decision_event_path=event_path,
        process_path=process_path,
        output_dir=output_dir,
    )

    assert result.exists()
    records = [json.loads(line) for line in result.read_text().strip().split("\n") if line]
    assert len(records) == 1
    assert records[0]["previous_rapporteur"] == "MIN A"
    assert records[0]["new_rapporteur"] == "MIN B"
    assert records[0]["process_id"] == "proc_1"


def test_no_change_no_records(tmp_path: Path):
    event_path = tmp_path / "event.jsonl"
    process_path = tmp_path / "process.jsonl"
    output_dir = tmp_path / "analytics"
    output_dir.mkdir()

    _write_jsonl(
        event_path,
        [
            {
                "decision_event_id": "de_1",
                "process_id": "proc_1",
                "decision_date": "2020-01-01",
                "decision_year": 2020,
                "current_rapporteur": "MIN A",
                "decision_progress": "Provido",
            },
            {
                "decision_event_id": "de_2",
                "process_id": "proc_1",
                "decision_date": "2020-06-01",
                "decision_year": 2020,
                "current_rapporteur": "MIN A",
                "decision_progress": "Negou provimento",
            },
        ],
    )
    _write_jsonl(process_path, [{"process_id": "proc_1", "process_class": "RE"}])

    result = build_rapporteur_changes(
        decision_event_path=event_path,
        process_path=process_path,
        output_dir=output_dir,
    )

    assert result.read_text() == ""


def test_red_flag_with_high_favorable_rate(tmp_path: Path):
    event_path = tmp_path / "event.jsonl"
    process_path = tmp_path / "process.jsonl"
    output_dir = tmp_path / "analytics"
    output_dir.mkdir()

    # MIN B has many unfavorable decisions overall (low baseline)
    events = [
        {
            "decision_event_id": "de_0",
            "process_id": "proc_1",
            "decision_date": "2020-01-01",
            "decision_year": 2020,
            "current_rapporteur": "MIN A",
            "decision_progress": "Negou provimento",
        },
        {
            "decision_event_id": "de_1",
            "process_id": "proc_1",
            "decision_date": "2020-06-01",
            "decision_year": 2020,
            "current_rapporteur": "MIN B",
            "decision_progress": "Provido",
        },
        {
            "decision_event_id": "de_2",
            "process_id": "proc_1",
            "decision_date": "2020-07-01",
            "decision_year": 2020,
            "current_rapporteur": "MIN B",
            "decision_progress": "Provido",
        },
        {
            "decision_event_id": "de_3",
            "process_id": "proc_1",
            "decision_date": "2020-08-01",
            "decision_year": 2020,
            "current_rapporteur": "MIN B",
            "decision_progress": "Provido",
        },
    ]
    # Add many unfavorable decisions for MIN B to create low baseline
    for i in range(10):
        events.append(
            {
                "decision_event_id": f"de_bg_{i}",
                "process_id": f"proc_bg_{i}",
                "decision_date": f"2020-0{min(i + 1, 9):d}-01",
                "decision_year": 2020,
                "current_rapporteur": "MIN B",
                "decision_progress": "Negou provimento",
            }
        )

    _write_jsonl(event_path, events)
    _write_jsonl(process_path, [{"process_id": "proc_1", "process_class": "RE"}])

    result = build_rapporteur_changes(
        decision_event_path=event_path,
        process_path=process_path,
        output_dir=output_dir,
    )

    records = [json.loads(line) for line in result.read_text().strip().split("\n") if line]
    assert len(records) == 1
    # Post-change rate = 100% (3/3), baseline for MIN B is ~0% (0/10)
    assert records[0]["red_flag"] is True
    assert records[0]["delta_vs_baseline"] is not None
    assert records[0]["delta_vs_baseline"] > 0.15


def test_summary_file(tmp_path: Path):
    event_path = tmp_path / "event.jsonl"
    process_path = tmp_path / "process.jsonl"
    output_dir = tmp_path / "analytics"
    output_dir.mkdir()

    _write_jsonl(event_path, [])
    _write_jsonl(process_path, [])

    build_rapporteur_changes(
        decision_event_path=event_path,
        process_path=process_path,
        output_dir=output_dir,
    )

    summary = json.loads((output_dir / "rapporteur_change_summary.json").read_text())
    assert summary["total_changes"] == 0
