"""Tests for counsel affinity analytics builder."""

from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics.counsel_affinity import _build_rapporteur_map, build_counsel_affinity


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")


class TestBuildCounselAffinity:
    def _setup(self, tmp_path: Path) -> dict[str, Path]:
        curated = tmp_path / "curated"
        analytics = tmp_path / "analytics"

        _write_jsonl(
            curated / "counsel.jsonl",
            [
                {"counsel_id": "c1", "counsel_name_raw": "ADV SILVA", "counsel_name_normalized": "ADV SILVA"},
                {"counsel_id": "c2", "counsel_name_raw": "ADV COSTA", "counsel_name_normalized": "ADV COSTA"},
            ],
        )
        _write_jsonl(
            curated / "process.jsonl", [{"process_id": f"proc_{i}", "process_class": "ADI"} for i in range(1, 8)]
        )

        # 6 processes with same rapporteur + counsel c1
        events = []
        for i in range(1, 7):
            events.append(
                {
                    "decision_event_id": f"e{i}",
                    "process_id": f"proc_{i}",
                    "current_rapporteur": "MIN. TESTE",
                    "decision_progress": "Procedente" if i <= 5 else "Improcedente",
                }
            )
        # 1 process with same rapporteur + counsel c2
        events.append(
            {
                "decision_event_id": "e7",
                "process_id": "proc_7",
                "current_rapporteur": "MIN. TESTE",
                "decision_progress": "Improcedente",
            }
        )
        _write_jsonl(curated / "decision_event.jsonl", events)

        links = [
            {"link_id": f"pc_{i}", "process_id": f"proc_{i}", "counsel_id": "c1", "side_in_case": "REQTE.(S)"}
            for i in range(1, 7)
        ]
        links.append({"link_id": "pc_7", "process_id": "proc_7", "counsel_id": "c2", "side_in_case": "REQTE.(S)"})
        _write_jsonl(curated / "process_counsel_link.jsonl", links)

        return {
            "decision_event_path": curated / "decision_event.jsonl",
            "process_path": curated / "process.jsonl",
            "counsel_path": curated / "counsel.jsonl",
            "process_counsel_link_path": curated / "process_counsel_link.jsonl",
            "output_dir": analytics,
        }

    def test_builds_affinities(self, tmp_path: Path) -> None:
        paths = self._setup(tmp_path)
        result = build_counsel_affinity(**paths)
        assert result.exists()
        affinities = [json.loads(line) for line in result.read_text(encoding="utf-8").strip().split("\n")]
        # c1 has 6 cases with MIN. TESTE, so it should appear
        c1_records = [a for a in affinities if a["counsel_id"] == "c1"]
        assert len(c1_records) == 1
        assert c1_records[0]["rapporteur"] == "MIN. TESTE"
        assert c1_records[0]["shared_case_count"] == 6

    def test_pair_favorable_rate(self, tmp_path: Path) -> None:
        paths = self._setup(tmp_path)
        result = build_counsel_affinity(**paths)
        affinities = [json.loads(line) for line in result.read_text(encoding="utf-8").strip().split("\n")]
        c1 = [a for a in affinities if a["counsel_id"] == "c1"][0]
        # 5 favorable, 1 unfavorable -> ~0.833
        assert c1["pair_favorable_rate"] is not None
        assert c1["favorable_count"] == 5
        assert c1["unfavorable_count"] == 1

    def test_summary_written(self, tmp_path: Path) -> None:
        paths = self._setup(tmp_path)
        build_counsel_affinity(**paths)
        summary_path = paths["output_dir"] / "counsel_affinity_summary.json"
        assert summary_path.exists()
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        assert "total_pairs_analyzed" in summary

    def test_min_cases_threshold(self, tmp_path: Path) -> None:
        """Pairs with <2 cases should not appear."""
        curated = tmp_path / "curated"
        analytics = tmp_path / "analytics"

        _write_jsonl(curated / "counsel.jsonl", [{"counsel_id": "c1", "counsel_name_normalized": "ADV"}])
        _write_jsonl(curated / "process.jsonl", [{"process_id": "proc_1", "process_class": "ADI"}])
        _write_jsonl(
            curated / "decision_event.jsonl",
            [
                {
                    "decision_event_id": "e1",
                    "process_id": "proc_1",
                    "current_rapporteur": "MIN",
                    "decision_progress": "Procedente",
                },
            ],
        )
        _write_jsonl(
            curated / "process_counsel_link.jsonl",
            [
                {"link_id": "pc_1", "process_id": "proc_1", "counsel_id": "c1", "side_in_case": "REQTE.(S)"},
            ],
        )

        result = build_counsel_affinity(
            decision_event_path=curated / "decision_event.jsonl",
            process_path=curated / "process.jsonl",
            counsel_path=curated / "counsel.jsonl",
            process_counsel_link_path=curated / "process_counsel_link.jsonl",
            output_dir=analytics,
        )
        content = result.read_text(encoding="utf-8").strip()
        assert content == ""  # No pairs with only 1 case

    def test_top_process_classes(self, tmp_path: Path) -> None:
        paths = self._setup(tmp_path)
        result = build_counsel_affinity(**paths)
        affinities = [json.loads(line) for line in result.read_text(encoding="utf-8").strip().split("\n")]
        c1 = [a for a in affinities if a["counsel_id"] == "c1"][0]
        assert "ADI" in c1["top_process_classes"]

    def test_rapporteur_map_prefers_latest_decision_date_over_file_order(self, tmp_path: Path) -> None:
        rows = [
            {
                "decision_event_id": "evt_new",
                "process_id": "proc_1",
                "current_rapporteur": "MIN. NOVO",
                "decision_date": "2024-02-01",
            },
            {
                "decision_event_id": "evt_old",
                "process_id": "proc_1",
                "current_rapporteur": "MIN. ANTIGO",
                "decision_date": "2024-01-01",
            },
        ]

        event_path = tmp_path / "decision_event.jsonl"
        _write_jsonl(event_path, rows)
        assert _build_rapporteur_map(event_path)["proc_1"] == "MIN. NOVO"
