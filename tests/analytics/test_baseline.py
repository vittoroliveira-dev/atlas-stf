from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics.baseline import build_baseline


def _write_jsonl(path: Path, rows: list[dict]):
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def test_build_baseline(tmp_path: Path):
    group_path = tmp_path / "comparison_group.jsonl"
    link_path = tmp_path / "decision_event_group_link.jsonl"
    event_path = tmp_path / "decision_event.jsonl"
    output = tmp_path / "baseline.jsonl"
    summary = tmp_path / "baseline_summary.json"

    _write_jsonl(
        group_path,
        [
            {
                "comparison_group_id": "grp_1",
                "rule_version": "comparison-group-v1",
                "selection_criteria": {},
                "time_window": "2026",
                "case_count": 5,
                "baseline_notes": None,
                "status": "valid",
                "blocked_reason": None,
                "created_at": "2026-03-07T00:00:00+00:00",
                "updated_at": "2026-03-07T00:00:00+00:00",
            }
        ],
    )
    _write_jsonl(
        link_path,
        [
            {
                "decision_event_id": "de_1",
                "comparison_group_id": "grp_1",
                "process_id": "proc_1",
                "linked_at": "2026-03-07T00:00:00+00:00",
            },
            {
                "decision_event_id": "de_2",
                "comparison_group_id": "grp_1",
                "process_id": "proc_2",
                "linked_at": "2026-03-07T00:00:00+00:00",
            },
        ],
    )
    _write_jsonl(
        event_path,
        [
            {
                "decision_event_id": "de_1",
                "source_row_id": "1",
                "process_id": "proc_1",
                "decision_date": "2026-03-07",
                "decision_year": 2026,
                "current_rapporteur": "MIN X",
                "decision_origin": None,
                "decision_type": "Decisão Final",
                "decision_progress": "NEGOU PROVIMENTO",
                "decision_note": None,
                "panel_indicator_raw": "COLEGIADA",
                "is_collegiate": True,
                "judging_body": "TURMA",
                "time_bucket": "2026-03",
                "raw_fields": {},
                "normalization_version": "decision-event-v1",
                "source_id": "STF-TRANSP-REGDIST",
                "created_at": "2026-03-07T00:00:00+00:00",
                "updated_at": "2026-03-07T00:00:00+00:00",
            },
            {
                "decision_event_id": "de_2",
                "source_row_id": "2",
                "process_id": "proc_2",
                "decision_date": "2026-03-08",
                "decision_year": 2026,
                "current_rapporteur": "MIN X",
                "decision_origin": None,
                "decision_type": "Decisão Final",
                "decision_progress": "DEFERIDO",
                "decision_note": None,
                "panel_indicator_raw": "COLEGIADA",
                "is_collegiate": True,
                "judging_body": "TURMA",
                "time_bucket": "2026-03",
                "raw_fields": {},
                "normalization_version": "decision-event-v1",
                "source_id": "STF-TRANSP-REGDIST",
                "created_at": "2026-03-07T00:00:00+00:00",
                "updated_at": "2026-03-07T00:00:00+00:00",
            },
        ],
    )

    build_baseline(group_path, link_path, event_path, output, summary)

    payload = json.loads(output.read_text(encoding="utf-8").strip())
    assert payload["comparison_group_id"] == "grp_1"
    assert payload["event_count"] == 2
    assert payload["process_count"] == 2
    assert payload["expected_decision_progress_distribution"]["NEGOU PROVIMENTO"] == 1
    assert payload["expected_decision_progress_distribution"]["DEFERIDO"] == 1
    assert payload["favorable_rate"] == 0.5
    assert payload["low_confidence"] is True
    assert json.loads(summary.read_text(encoding="utf-8"))["baseline_count"] == 1


def test_build_baseline_marks_large_groups_as_reliable(tmp_path: Path):
    group_path = tmp_path / "comparison_group.jsonl"
    link_path = tmp_path / "decision_event_group_link.jsonl"
    event_path = tmp_path / "decision_event.jsonl"
    output = tmp_path / "baseline.jsonl"
    summary = tmp_path / "baseline_summary.json"

    _write_jsonl(
        group_path,
        [
            {
                "comparison_group_id": "grp_1",
                "rule_version": "comparison-group-v1",
                "selection_criteria": {"process_class": "AC"},
                "time_window": "2026",
                "case_count": 10,
                "baseline_notes": None,
                "status": "valid",
                "blocked_reason": None,
                "created_at": "2026-03-07T00:00:00+00:00",
                "updated_at": "2026-03-07T00:00:00+00:00",
            }
        ],
    )
    _write_jsonl(
        link_path,
        [
            {
                "decision_event_id": f"de_{idx}",
                "comparison_group_id": "grp_1",
                "process_id": f"proc_{idx}",
                "linked_at": "2026-03-07T00:00:00+00:00",
            }
            for idx in range(1, 11)
        ],
    )
    _write_jsonl(
        event_path,
        [
            {
                "decision_event_id": f"de_{idx}",
                "source_row_id": str(idx),
                "process_id": f"proc_{idx}",
                "decision_date": f"2026-03-{idx:02d}",
                "decision_year": 2026,
                "current_rapporteur": "MIN X",
                "decision_origin": None,
                "decision_type": "Decisão Final",
                "decision_progress": "NEGOU PROVIMENTO",
                "decision_note": None,
                "panel_indicator_raw": "COLEGIADA",
                "is_collegiate": True,
                "judging_body": "TURMA",
                "time_bucket": "2026-03",
                "raw_fields": {},
                "normalization_version": "decision-event-v1",
                "source_id": "STF-TRANSP-REGDIST",
                "created_at": "2026-03-07T00:00:00+00:00",
                "updated_at": "2026-03-07T00:00:00+00:00",
            }
            for idx in range(1, 11)
        ],
    )

    build_baseline(group_path, link_path, event_path, output, summary)

    payload = json.loads(output.read_text(encoding="utf-8").strip())
    assert payload["process_count"] == 10
    assert payload["low_confidence"] is False
    assert payload["favorable_rate"] == 0.083333
