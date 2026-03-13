from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics.counsel_network import (
    _find_connected_components,
    build_counsel_network,
)


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def test_find_connected_components_basic():
    counsel_clients = {
        "c1": {"p1", "p2", "p3"},
        "c2": {"p2", "p3", "p4"},
        "c3": {"p5", "p6"},
    }
    components = _find_connected_components(counsel_clients, min_shared_clients=2)
    assert len(components) == 1
    assert components[0] == {"c1", "c2"}


def test_find_connected_components_no_overlap():
    counsel_clients = {
        "c1": {"p1"},
        "c2": {"p2"},
        "c3": {"p3"},
    }
    components = _find_connected_components(counsel_clients, min_shared_clients=2)
    assert len(components) == 0


def test_build_counsel_network_produces_clusters(tmp_path: Path):
    curated_dir = tmp_path / "curated"
    curated_dir.mkdir()
    output_dir = tmp_path / "analytics"
    output_dir.mkdir()

    # Two counsel share 3 clients via processes
    _write_jsonl(
        curated_dir / "counsel.jsonl",
        [
            {"counsel_id": "c1", "counsel_name_normalized": "ADV SILVA"},
            {"counsel_id": "c2", "counsel_name_normalized": "ADV SANTOS"},
        ],
    )
    _write_jsonl(
        curated_dir / "process_counsel_link.jsonl",
        [
            {"counsel_id": "c1", "process_id": "p1"},
            {"counsel_id": "c1", "process_id": "p2"},
            {"counsel_id": "c1", "process_id": "p3"},
            {"counsel_id": "c2", "process_id": "p1"},
            {"counsel_id": "c2", "process_id": "p2"},
            {"counsel_id": "c2", "process_id": "p3"},
        ],
    )
    _write_jsonl(
        curated_dir / "process_party_link.jsonl",
        [
            {"process_id": "p1", "party_id": "party_a"},
            {"process_id": "p2", "party_id": "party_b"},
            {"process_id": "p3", "party_id": "party_c"},
        ],
    )
    _write_jsonl(
        curated_dir / "decision_event.jsonl",
        [
            {
                "decision_event_id": "de_1", "process_id": "p1",
                "decision_progress": "Provido", "current_rapporteur": "MIN X",
            },
            {
                "decision_event_id": "de_2", "process_id": "p2",
                "decision_progress": "Provido", "current_rapporteur": "MIN X",
            },
            {
                "decision_event_id": "de_3", "process_id": "p3",
                "decision_progress": "Provido", "current_rapporteur": "MIN Y",
            },
        ],
    )

    result = build_counsel_network(
        curated_dir=curated_dir,
        output_dir=output_dir,
        min_shared_clients=2,
    )

    assert result.exists()
    records = [json.loads(line) for line in result.read_text().strip().split("\n") if line]
    assert len(records) == 1
    assert set(records[0]["counsel_ids"]) == {"c1", "c2"}
    assert records[0]["cluster_size"] == 2
    assert records[0]["shared_client_count"] == 3


def test_build_counsel_network_empty_when_no_links(tmp_path: Path):
    curated_dir = tmp_path / "curated"
    curated_dir.mkdir()
    output_dir = tmp_path / "analytics"
    output_dir.mkdir()

    _write_jsonl(curated_dir / "counsel.jsonl", [])
    _write_jsonl(curated_dir / "process_counsel_link.jsonl", [])
    _write_jsonl(curated_dir / "process_party_link.jsonl", [])
    _write_jsonl(curated_dir / "decision_event.jsonl", [])

    result = build_counsel_network(
        curated_dir=curated_dir,
        output_dir=output_dir,
    )

    assert result.read_text() == ""


def test_summary_file(tmp_path: Path):
    curated_dir = tmp_path / "curated"
    curated_dir.mkdir()
    output_dir = tmp_path / "analytics"
    output_dir.mkdir()

    _write_jsonl(curated_dir / "counsel.jsonl", [])
    _write_jsonl(curated_dir / "process_counsel_link.jsonl", [])
    _write_jsonl(curated_dir / "process_party_link.jsonl", [])
    _write_jsonl(curated_dir / "decision_event.jsonl", [])

    build_counsel_network(curated_dir=curated_dir, output_dir=output_dir)

    summary = json.loads((output_dir / "counsel_network_cluster_summary.json").read_text())
    assert summary["total_clusters"] == 0
