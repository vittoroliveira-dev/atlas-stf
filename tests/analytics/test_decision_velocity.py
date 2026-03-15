from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics.decision_velocity import _percentile, build_decision_velocity


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def test_percentile_basic():
    assert _percentile([1.0, 2.0, 3.0, 4.0, 5.0], 50) == 3.0
    assert _percentile([1.0, 2.0, 3.0, 4.0, 5.0], 0) == 1.0
    assert _percentile([1.0, 2.0, 3.0, 4.0, 5.0], 100) == 5.0


def test_percentile_empty():
    assert _percentile([], 50) == 0.0


def test_build_decision_velocity_flags_queue_jump(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    event_path = tmp_path / "event.jsonl"
    output_dir = tmp_path / "analytics"
    output_dir.mkdir()

    # Create 15 processes, one with very fast decision
    processes = []
    events = []
    for i in range(15):
        pid = f"proc_{i}"
        processes.append(
            {
                "process_id": pid,
                "process_class": "RE",
                "filing_date": "2020-01-01",
                "subjects_normalized": ["TRIBUTÁRIO"],
            }
        )
        # Most take ~365 days, one takes 5 days
        days_offset = 5 if i == 0 else 365
        decision_date = f"2020-{1 + days_offset // 30:02d}-{1 + days_offset % 28:02d}"
        if i == 0:
            decision_date = "2020-01-06"
        else:
            decision_date = "2021-01-01"
        events.append(
            {
                "decision_event_id": f"de_{i}",
                "process_id": pid,
                "decision_date": decision_date,
                "decision_year": 2020 if i == 0 else 2021,
                "current_rapporteur": "MIN X",
                "decision_type": "Decisão Final",
                "decision_progress": "Negou provimento",
                "is_collegiate": True,
                "judging_body": "1ª Turma",
            }
        )

    # Need all events in same year for grouping
    for ev in events:
        ev["decision_year"] = 2020
        ev["decision_date"] = "2020-01-06" if ev["process_id"] == "proc_0" else "2021-01-01"

    _write_jsonl(process_path, processes)
    _write_jsonl(event_path, events)

    result = build_decision_velocity(
        process_path=process_path,
        decision_event_path=event_path,
        output_dir=output_dir,
        min_group_size=10,
    )

    assert result.exists()
    records = [json.loads(line) for line in result.read_text().strip().split("\n") if line]
    assert len(records) == 15

    flagged = [r for r in records if r["velocity_flag"]]
    queue_jumps = [r for r in flagged if r["velocity_flag"] == "queue_jump"]
    assert len(queue_jumps) >= 1
    assert queue_jumps[0]["days_to_decision"] == 5


def test_build_decision_velocity_skips_small_groups(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    event_path = tmp_path / "event.jsonl"
    output_dir = tmp_path / "analytics"
    output_dir.mkdir()

    _write_jsonl(
        process_path,
        [
            {"process_id": "p1", "process_class": "RE", "filing_date": "2020-01-01"},
            {"process_id": "p2", "process_class": "RE", "filing_date": "2020-01-01"},
        ],
    )
    _write_jsonl(
        event_path,
        [
            {
                "decision_event_id": "de_1",
                "process_id": "p1",
                "decision_date": "2020-06-01",
                "decision_year": 2020,
                "current_rapporteur": "X",
            },
            {
                "decision_event_id": "de_2",
                "process_id": "p2",
                "decision_date": "2020-06-01",
                "decision_year": 2020,
                "current_rapporteur": "X",
            },
        ],
    )

    result = build_decision_velocity(
        process_path=process_path,
        decision_event_path=event_path,
        output_dir=output_dir,
        min_group_size=10,
    )

    content = result.read_text()
    assert content == ""


def test_build_decision_velocity_summary(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    event_path = tmp_path / "event.jsonl"
    output_dir = tmp_path / "analytics"
    output_dir.mkdir()

    _write_jsonl(process_path, [])
    _write_jsonl(event_path, [])

    build_decision_velocity(
        process_path=process_path,
        decision_event_path=event_path,
        output_dir=output_dir,
    )

    summary = json.loads((output_dir / "decision_velocity_summary.json").read_text())
    assert summary["total_records"] == 0
    assert summary["flagged_count"] == 0
