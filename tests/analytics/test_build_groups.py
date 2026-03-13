from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics.build_groups import build_groups


def _write_jsonl(path: Path, rows: list[dict]):
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def test_build_groups_links_only_valid_groups(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    decision_event_path = tmp_path / "decision_event.jsonl"
    output_dir = tmp_path / "analytics"

    _write_jsonl(
        process_path,
        [
            {
                "process_id": f"proc_{i}",
                "process_number": f"AC {i}",
                "process_class": "AC",
                "filing_date": None,
                "closing_date": None,
                "origin_description": None,
                "origin_court_or_body": None,
                "branch_of_law": "DIREITO X",
                "subjects_raw": ["A"],
                "subjects_normalized": ["A"],
                "case_environment": None,
                "procedural_status": None,
                "raw_fields": {},
                "normalization_version": "process-v1",
                "source_id": "STF-TRANSP-REGDIST",
                "source_record_hash": str(i),
                "created_at": "2026-03-07T00:00:00+00:00",
                "updated_at": "2026-03-07T00:00:00+00:00",
            }
            for i in range(1, 7)
        ],
    )
    _write_jsonl(
        decision_event_path,
        [
            {
                "decision_event_id": f"de_{i}",
                "source_row_id": str(i),
                "process_id": f"proc_{i}",
                "decision_date": "2026-03-07",
                "decision_year": 2026,
                "current_rapporteur": None,
                "decision_origin": None,
                "decision_type": "Decisão Final",
                "decision_progress": None,
                "decision_note": None,
                "panel_indicator_raw": "MONOCRÁTICA",
                "is_collegiate": False,
                "judging_body": None,
                "time_bucket": "2026-03",
                "raw_fields": {},
                "normalization_version": "decision-event-v1",
                "source_id": "STF-TRANSP-REGDIST",
                "created_at": "2026-03-07T00:00:00+00:00",
                "updated_at": "2026-03-07T00:00:00+00:00",
            }
            for i in range(1, 7)
        ],
    )

    groups_path, links_path, summary_path = build_groups(
        process_path=process_path,
        decision_event_path=decision_event_path,
        output_dir=output_dir,
    )

    groups = [json.loads(line) for line in groups_path.read_text(encoding="utf-8").splitlines()]
    links = [json.loads(line) for line in links_path.read_text(encoding="utf-8").splitlines()]
    summary = json.loads(summary_path.read_text(encoding="utf-8"))

    assert len(groups) == 1
    assert groups[0]["status"] == "valid"
    assert len(links) == 6
    assert summary["linked_event_count"] == 6


def test_build_groups_marks_small_groups_as_blocked(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    decision_event_path = tmp_path / "decision_event.jsonl"
    output_dir = tmp_path / "analytics"

    _write_jsonl(
        process_path,
        [
            {
                "process_id": "proc_1",
                "process_number": "AC 1",
                "process_class": "AC",
                "filing_date": None,
                "closing_date": None,
                "origin_description": None,
                "origin_court_or_body": None,
                "branch_of_law": "DIREITO X",
                "subjects_raw": ["A"],
                "subjects_normalized": ["A"],
                "case_environment": None,
                "procedural_status": None,
                "raw_fields": {},
                "normalization_version": "process-v1",
                "source_id": "STF-TRANSP-REGDIST",
                "source_record_hash": "1",
                "created_at": "2026-03-07T00:00:00+00:00",
                "updated_at": "2026-03-07T00:00:00+00:00",
            }
        ],
    )
    _write_jsonl(
        decision_event_path,
        [
            {
                "decision_event_id": "de_1",
                "source_row_id": "1",
                "process_id": "proc_1",
                "decision_date": "2026-03-07",
                "decision_year": 2026,
                "current_rapporteur": None,
                "decision_origin": None,
                "decision_type": "Decisão Final",
                "decision_progress": None,
                "decision_note": None,
                "panel_indicator_raw": "MONOCRÁTICA",
                "is_collegiate": False,
                "judging_body": None,
                "time_bucket": "2026-03",
                "raw_fields": {},
                "normalization_version": "decision-event-v1",
                "source_id": "STF-TRANSP-REGDIST",
                "created_at": "2026-03-07T00:00:00+00:00",
                "updated_at": "2026-03-07T00:00:00+00:00",
            }
        ],
    )

    groups_path, links_path, summary_path = build_groups(
        process_path=process_path,
        decision_event_path=decision_event_path,
        output_dir=output_dir,
    )
    groups = [json.loads(line) for line in groups_path.read_text(encoding="utf-8").splitlines()]
    links_content = links_path.read_text(encoding="utf-8").strip()

    assert groups[0]["status"] == "insufficient_cases"
    assert groups[0]["blocked_reason"] == "below_min_case_count"
    assert links_content == ""
