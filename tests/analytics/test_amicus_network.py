"""Tests for the amicus curiae network analytics builder."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from atlas_stf.analytics.amicus_network import build_amicus_network


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    return [json.loads(line) for line in text.split("\n")]


def test_amicus_network_empty_input(tmp_path: Path) -> None:
    curated = tmp_path / "curated"
    curated.mkdir()
    output = tmp_path / "analytics"

    result = build_amicus_network(curated_dir=curated, output_dir=output)

    records = _read_jsonl(result)
    assert records == []


def test_amicus_network_nonempty_records(tmp_path: Path) -> None:
    """Ensure non-empty records do not raise TypeError (bug: unhashable dict)."""
    curated = tmp_path / "curated"
    curated.mkdir()
    output = tmp_path / "analytics"

    _write_jsonl(curated / "lawyer_entity.jsonl", [
        {"lawyer_id": "law_abc", "lawyer_name_normalized": "JOAO DA SILVA"},
    ])
    _write_jsonl(curated / "representation_edge.jsonl", [
        {
            "lawyer_id": "law_abc",
            "process_id": "proc_1",
            "role_type": "amicus_representative",
            "start_date": "2025-01-01",
            "end_date": "2025-06-01",
        },
        {
            "lawyer_id": "law_abc",
            "process_id": "proc_2",
            "role_type": "amicus_representative",
            "start_date": "2025-03-01",
            "end_date": None,
        },
    ])
    _write_jsonl(curated / "representation_event.jsonl", [])
    _write_jsonl(curated / "process.jsonl", [
        {"process_id": "proc_1", "process_class": "ADI"},
        {"process_id": "proc_2", "process_class": "ADPF"},
    ])
    _write_jsonl(curated / "decision_event.jsonl", [
        {"process_id": "proc_1", "current_rapporteur": "MINISTRO A", "decision_date": "2025-02-01"},
    ])

    result = build_amicus_network(curated_dir=curated, output_dir=output)

    records = _read_jsonl(result)
    assert len(records) == 1
    assert records[0]["lawyer_id"] == "law_abc"
    assert records[0]["process_count"] == 2
    assert records[0]["edge_count"] == 2
    assert records[0]["lawyer_name"] == "JOAO DA SILVA"

    # Summary must have correct total_processes
    summary_path = output / "amicus_network_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["total_processes"] == 2
    assert summary["total_amicus_lawyers"] == 1
