"""Tests for rapporteur profile builder."""

from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics.rapporteur_profile import build_rapporteur_profiles


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


def _make_events(
    rapporteur: str,
    progress: str,
    count: int,
    year: int = 2024,
    process_class: str = "ADI",
) -> list[dict]:
    return [
        {
            "decision_event_id": f"evt_{rapporteur}_{progress}_{i}",
            "process_id": f"proc_{i}",
            "current_rapporteur": rapporteur,
            "decision_progress": progress,
            "decision_year": year,
            "decision_date": f"{year}-06-15",
            "decision_type": "DECISÃO",
            "is_collegiate": True,
            "process_class": process_class,
        }
        for i in range(count)
    ]


def _make_processes(count: int, process_class: str = "ADI") -> list[dict]:
    return [
        {
            "process_id": f"proc_{i}",
            "process_class": process_class,
            "subjects_normalized": ["Civil"],
            "branch_of_law": "Civil",
        }
        for i in range(count)
    ]


class TestBuildRapporteurProfiles:
    def test_builds_profiles_with_deviation(self, tmp_path: Path):
        events = (
            _make_events("MINISTRO_A", "Provido", 25)
            + _make_events("MINISTRO_A", "Desprovido", 5)
            + _make_events("MINISTRO_B", "Provido", 10)
            + _make_events("MINISTRO_B", "Desprovido", 20)
        )
        # Reindex process ids to avoid duplication
        for i, evt in enumerate(events):
            evt["process_id"] = f"proc_{i}"

        processes = [
            {
                "process_id": f"proc_{i}",
                "process_class": "ADI",
                "subjects_normalized": ["Civil"],
                "branch_of_law": "Civil",
            }
            for i in range(len(events))
        ]

        evt_path = tmp_path / "decision_event.jsonl"
        proc_path = tmp_path / "process.jsonl"
        out_dir = tmp_path / "output"
        _write_jsonl(evt_path, events)
        _write_jsonl(proc_path, processes)

        result = build_rapporteur_profiles(
            decision_event_path=evt_path,
            process_path=proc_path,
            output_dir=out_dir,
            min_group_size=10,
        )

        assert result.exists()
        records = [json.loads(line) for line in result.read_text().strip().split("\n")]
        assert len(records) == 2

        a_record = next(r for r in records if r["rapporteur"] == "MINISTRO_A")
        b_record = next(r for r in records if r["rapporteur"] == "MINISTRO_B")

        # Both should have chi2 computed
        assert a_record["chi2_statistic"] is not None
        assert b_record["chi2_statistic"] is not None

        # At least one should be flagged given the extreme distributions
        assert a_record["deviation_flag"] or b_record["deviation_flag"]

    def test_skips_small_groups(self, tmp_path: Path):
        events = _make_events("MIN_X", "Provido", 5)
        for i, evt in enumerate(events):
            evt["process_id"] = f"proc_{i}"
        processes = _make_processes(5)

        evt_path = tmp_path / "decision_event.jsonl"
        proc_path = tmp_path / "process.jsonl"
        out_dir = tmp_path / "output"
        _write_jsonl(evt_path, events)
        _write_jsonl(proc_path, processes)

        result = build_rapporteur_profiles(
            decision_event_path=evt_path,
            process_path=proc_path,
            output_dir=out_dir,
            min_group_size=30,
        )

        records = [json.loads(line) for line in result.read_text().strip().split("\n") if line.strip()]
        assert len(records) == 0

    def test_writes_summary(self, tmp_path: Path):
        events = _make_events("MIN_A", "Provido", 20) + _make_events("MIN_B", "Desprovido", 15)
        for i, evt in enumerate(events):
            evt["process_id"] = f"proc_{i}"
        processes = [
            {
                "process_id": f"proc_{i}",
                "process_class": "ADI",
                "subjects_normalized": ["Civil"],
                "branch_of_law": "Civil",
            }
            for i in range(35)
        ]

        evt_path = tmp_path / "decision_event.jsonl"
        proc_path = tmp_path / "process.jsonl"
        out_dir = tmp_path / "output"
        _write_jsonl(evt_path, events)
        _write_jsonl(proc_path, processes)

        build_rapporteur_profiles(
            decision_event_path=evt_path,
            process_path=proc_path,
            output_dir=out_dir,
            min_group_size=10,
        )

        summary_path = out_dir / "rapporteur_profile_summary.json"
        assert summary_path.exists()
        summary = json.loads(summary_path.read_text())
        assert "total_profiles" in summary
        assert "deviation_count" in summary
