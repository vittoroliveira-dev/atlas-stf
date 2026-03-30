"""Tests for assignment audit builder."""

from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics.assignment_audit import build_assignment_audit


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


class TestBuildAssignmentAudit:
    def test_uniform_distribution(self, tmp_path: Path):
        events = []
        for i in range(100):
            rapporteur = f"MIN_{i % 5}"
            events.append(
                {
                    "decision_event_id": f"evt_{i}",
                    "process_id": f"proc_{i}",
                    "decision_year": 2024,
                    "current_rapporteur": rapporteur,
                    "decision_date": "2024-06-15",
                }
            )

        evt_path = tmp_path / "decision_event.jsonl"
        proc_path = tmp_path / "process.jsonl"
        out_dir = tmp_path / "output"
        _write_jsonl(evt_path, events)
        _write_jsonl(proc_path, [{"process_id": f"proc_{i}", "process_class": "ADI"} for i in range(100)])

        result = build_assignment_audit(
            decision_event_path=evt_path,
            process_path=proc_path,
            output_dir=out_dir,
            min_events=50,
        )

        assert result.exists()
        records = [json.loads(line) for line in result.read_text().strip().split("\n")]
        assert len(records) == 1
        assert records[0]["uniformity_flag"] is True
        assert records[0]["rapporteur_count"] == 5
        assert records[0]["event_count"] == 100

    def test_non_uniform_distribution(self, tmp_path: Path):
        events = []
        # MIN_A gets 80 events, MIN_B gets 10, MIN_C gets 10
        for i in range(80):
            events.append(
                {
                    "decision_event_id": f"evt_a_{i}",
                    "process_id": f"proc_a_{i}",
                    "decision_year": 2024,
                    "current_rapporteur": "MIN_A",
                    "decision_date": "2024-06-15",
                }
            )
        for i in range(10):
            events.append(
                {
                    "decision_event_id": f"evt_b_{i}",
                    "process_id": f"proc_b_{i}",
                    "decision_year": 2024,
                    "current_rapporteur": "MIN_B",
                    "decision_date": "2024-06-15",
                }
            )
        for i in range(10):
            events.append(
                {
                    "decision_event_id": f"evt_c_{i}",
                    "process_id": f"proc_c_{i}",
                    "decision_year": 2024,
                    "current_rapporteur": "MIN_C",
                    "decision_date": "2024-06-15",
                }
            )

        all_procs = (
            [{"process_id": f"proc_a_{i}", "process_class": "RE"} for i in range(80)]
            + [{"process_id": f"proc_b_{i}", "process_class": "RE"} for i in range(10)]
            + [{"process_id": f"proc_c_{i}", "process_class": "RE"} for i in range(10)]
        )

        evt_path = tmp_path / "decision_event.jsonl"
        proc_path = tmp_path / "process.jsonl"
        out_dir = tmp_path / "output"
        _write_jsonl(evt_path, events)
        _write_jsonl(proc_path, all_procs)

        result = build_assignment_audit(
            decision_event_path=evt_path,
            process_path=proc_path,
            output_dir=out_dir,
            min_events=50,
        )

        records = [json.loads(line) for line in result.read_text().strip().split("\n")]
        assert len(records) == 1
        assert records[0]["uniformity_flag"] is False
        assert records[0]["most_overrepresented_rapporteur"] == "MIN_A"

    def test_skips_small_groups(self, tmp_path: Path):
        events = [
            {
                "decision_event_id": f"evt_{i}",
                "process_id": f"proc_{i}",
                "decision_year": 2024,
                "current_rapporteur": f"MIN_{i % 2}",
                "decision_date": "2024-06-15",
            }
            for i in range(10)
        ]

        evt_path = tmp_path / "decision_event.jsonl"
        proc_path = tmp_path / "process.jsonl"
        out_dir = tmp_path / "output"
        _write_jsonl(evt_path, events)
        _write_jsonl(proc_path, [{"process_id": f"proc_{i}", "process_class": "ADI"} for i in range(10)])

        result = build_assignment_audit(
            decision_event_path=evt_path,
            process_path=proc_path,
            output_dir=out_dir,
            min_events=50,
        )

        records = [json.loads(line) for line in result.read_text().strip().split("\n") if line.strip()]
        assert len(records) == 0

    def test_writes_summary(self, tmp_path: Path):
        events = [
            {
                "decision_event_id": f"evt_{i}",
                "process_id": f"proc_{i}",
                "decision_year": 2024,
                "current_rapporteur": f"MIN_{i % 3}",
                "decision_date": "2024-06-15",
            }
            for i in range(60)
        ]

        evt_path = tmp_path / "decision_event.jsonl"
        proc_path = tmp_path / "process.jsonl"
        out_dir = tmp_path / "output"
        _write_jsonl(evt_path, events)
        _write_jsonl(proc_path, [{"process_id": f"proc_{i}", "process_class": "ADI"} for i in range(60)])

        build_assignment_audit(
            decision_event_path=evt_path,
            process_path=proc_path,
            output_dir=out_dir,
            min_events=50,
        )

        summary_path = out_dir / "assignment_audit_summary.json"
        assert summary_path.exists()
        summary = json.loads(summary_path.read_text())
        assert "total_audits" in summary
        assert "uniform_count" in summary

    def test_uses_process_jsonl_as_canonical_source(self, tmp_path: Path):
        """process_class comes exclusively from process.jsonl, not decision_event."""
        events = []
        for i in range(100):
            events.append(
                {
                    "decision_event_id": f"evt_{i}",
                    "process_id": f"proc_{i}",
                    "decision_year": 2024,
                    "current_rapporteur": f"MIN_{i % 4}",
                    "decision_date": "2024-06-15",
                }
            )

        processes = [{"process_id": f"proc_{i}", "process_class": "ADI"} for i in range(100)]

        evt_path = tmp_path / "decision_event.jsonl"
        proc_path = tmp_path / "process.jsonl"
        out_dir = tmp_path / "output"
        _write_jsonl(evt_path, events)
        _write_jsonl(proc_path, processes)

        result = build_assignment_audit(
            decision_event_path=evt_path,
            process_path=proc_path,
            output_dir=out_dir,
            min_events=50,
        )

        records = [json.loads(line) for line in result.read_text().strip().split("\n") if line.strip()]
        assert len(records) == 1
        assert records[0]["process_class"] == "ADI"
        assert records[0]["event_count"] == 100
