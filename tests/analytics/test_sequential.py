"""Tests for sequential analysis builder."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from atlas_stf.analytics.sequential import (
    _classify_outcome,
    _streak_effect,
    build_sequential_analysis,
)


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


class TestClassifyOutcome:
    @pytest.mark.parametrize(
        "progress,expected",
        [
            ("Provido", 1),
            ("Parcialmente provido", 1),
            ("Deferido", 1),
            ("Desprovido", 0),
            ("Indeferido", 0),
            ("Negado provimento", 0),
            ("Homologado", 1),
            (None, None),
            ("", None),
            ("Prejudicado", None),
            ("Extinto sem julgamento", None),
        ],
    )
    def test_classification(self, progress, expected):
        assert _classify_outcome(progress) == expected


class TestStreakEffect:
    def test_with_streaks(self):
        # Alternating favorable streaks of 3 followed by unfavorable
        series = [1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1, 0]
        effect, rate = _streak_effect(series, 3)
        assert effect is not None
        assert rate is not None
        # After 3 favorable, all next are 0, so post_streak_rate = 0
        assert rate == 0.0

    def test_insufficient_data(self):
        series = [1, 0, 1]
        effect, rate = _streak_effect(series, 3)
        assert effect is None
        assert rate is None

    def test_no_streaks_found(self):
        # Alternating, no streak of 3
        series = [1, 0, 1, 0, 1, 0, 1, 0, 1, 0]
        effect, rate = _streak_effect(series, 3)
        assert effect is None  # fewer than 3 post-streak observations


class TestBuildSequentialAnalysis:
    def test_builds_analysis(self, tmp_path: Path):
        events = []
        for i in range(60):
            progress = "Provido" if i % 3 != 0 else "Desprovido"
            events.append(
                {
                    "decision_event_id": f"evt_{i}",
                    "process_id": f"proc_{i}",
                    "current_rapporteur": "MIN_A",
                    "decision_year": 2024,
                    "decision_date": f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
                    "decision_progress": progress,
                }
            )

        evt_path = tmp_path / "decision_event.jsonl"
        out_dir = tmp_path / "output"
        _write_jsonl(evt_path, events)

        result = build_sequential_analysis(
            decision_event_path=evt_path,
            output_dir=out_dir,
            min_decisions=50,
        )

        assert result.exists()
        records = [json.loads(line) for line in result.read_text().strip().split("\n")]
        assert len(records) == 1
        r = records[0]
        assert r["rapporteur"] == "MIN_A"
        assert r["n_decisions"] == 60
        assert -1.0 <= r["autocorrelation_lag1"] <= 1.0
        assert r["base_favorable_rate"] > 0.0

    def test_excludes_unknown_outcomes(self, tmp_path: Path):
        events = []
        for i in range(60):
            events.append(
                {
                    "decision_event_id": f"evt_{i}",
                    "process_id": f"proc_{i}",
                    "current_rapporteur": "MIN_B",
                    "decision_year": 2024,
                    "decision_date": f"2024-06-{(i % 28) + 1:02d}",
                    "decision_progress": "Extinto sem julgamento",
                }
            )

        evt_path = tmp_path / "decision_event.jsonl"
        out_dir = tmp_path / "output"
        _write_jsonl(evt_path, events)

        result = build_sequential_analysis(
            decision_event_path=evt_path,
            output_dir=out_dir,
            min_decisions=50,
        )

        records = [json.loads(line) for line in result.read_text().strip().split("\n") if line.strip()]
        assert len(records) == 0

    def test_skips_small_groups(self, tmp_path: Path):
        events = [
            {
                "decision_event_id": f"evt_{i}",
                "process_id": f"proc_{i}",
                "current_rapporteur": "MIN_C",
                "decision_year": 2024,
                "decision_date": f"2024-06-{(i % 28) + 1:02d}",
                "decision_progress": "Provido",
            }
            for i in range(10)
        ]

        evt_path = tmp_path / "decision_event.jsonl"
        out_dir = tmp_path / "output"
        _write_jsonl(evt_path, events)

        result = build_sequential_analysis(
            decision_event_path=evt_path,
            output_dir=out_dir,
            min_decisions=50,
        )

        records = [json.loads(line) for line in result.read_text().strip().split("\n") if line.strip()]
        assert len(records) == 0

    def test_writes_summary(self, tmp_path: Path):
        events = [
            {
                "decision_event_id": f"evt_{i}",
                "process_id": f"proc_{i}",
                "current_rapporteur": "MIN_D",
                "decision_year": 2024,
                "decision_date": f"2024-{(i % 12) + 1:02d}-15",
                "decision_progress": "Provido" if i % 2 == 0 else "Desprovido",
            }
            for i in range(60)
        ]

        evt_path = tmp_path / "decision_event.jsonl"
        out_dir = tmp_path / "output"
        _write_jsonl(evt_path, events)

        build_sequential_analysis(
            decision_event_path=evt_path,
            output_dir=out_dir,
            min_decisions=50,
        )

        summary_path = out_dir / "sequential_analysis_summary.json"
        assert summary_path.exists()
        summary = json.loads(summary_path.read_text())
        assert "total_analyses" in summary
        assert "bias_flagged_count" in summary

    def test_same_day_events_use_decision_event_id_as_tie_breaker(self, tmp_path: Path):
        events = []
        for index in range(0, 60, 2):
            events.append(
                {
                    "decision_event_id": f"evt_{index:02d}",
                    "process_id": f"proc_even_{index}",
                    "current_rapporteur": "MIN_E",
                    "decision_year": 2024,
                    "decision_date": "2024-06-15",
                    "decision_progress": "Provido",
                }
            )
        for index in range(1, 60, 2):
            events.append(
                {
                    "decision_event_id": f"evt_{index:02d}",
                    "process_id": f"proc_{index}",
                    "current_rapporteur": "MIN_E",
                    "decision_year": 2024,
                    "decision_date": "2024-06-15",
                    "decision_progress": "Desprovido",
                }
            )

        evt_path = tmp_path / "decision_event.jsonl"
        out_dir = tmp_path / "output"
        _write_jsonl(evt_path, events)

        result = build_sequential_analysis(
            decision_event_path=evt_path,
            output_dir=out_dir,
            min_decisions=50,
        )

        records = [json.loads(line) for line in result.read_text().strip().split("\n") if line.strip()]
        assert len(records) == 1
        assert records[0]["base_favorable_rate"] == pytest.approx(0.5)
        assert records[0]["autocorrelation_lag1"] < 0
