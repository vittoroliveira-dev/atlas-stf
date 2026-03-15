from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics.representation_graph import build_representation_graph


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def _read_output(path: Path) -> list[dict]:
    text = path.read_text().strip()
    if not text:
        return []
    return [json.loads(line) for line in text.split("\n")]


# -- Empty / missing inputs --------------------------------------------------


def test_empty_input_no_files(tmp_path: Path):
    """When no curated files exist, output should be empty."""
    curated_dir = tmp_path / "curated"
    curated_dir.mkdir()
    output_dir = tmp_path / "analytics"

    result = build_representation_graph(curated_dir=curated_dir, output_dir=output_dir)

    assert result.exists()
    assert _read_output(result) == []
    summary = json.loads((output_dir / "representation_graph_summary.json").read_text())
    assert summary["total_edges"] == 0
    assert summary["total_lawyers"] == 0
    assert summary["total_firms"] == 0
    assert summary["total_events"] == 0


def test_lawyers_only_no_edges(tmp_path: Path):
    """When lawyers exist but no edges, output should be empty."""
    curated_dir = tmp_path / "curated"
    curated_dir.mkdir()
    output_dir = tmp_path / "analytics"

    _write_jsonl(
        curated_dir / "lawyer_entity.jsonl",
        [
            {"lawyer_id": "law-1", "name": "ADV SILVA"},
            {"lawyer_id": "law-2", "name": "ADV SANTOS"},
        ],
    )

    result = build_representation_graph(curated_dir=curated_dir, output_dir=output_dir)

    records = _read_output(result)
    assert records == []
    summary = json.loads((output_dir / "representation_graph_summary.json").read_text())
    assert summary["total_lawyers"] == 2
    assert summary["total_edges"] == 0


# -- Edges and events ---------------------------------------------------------


def test_edges_with_events(tmp_path: Path):
    """Edges with events should produce records with aggregated event data."""
    curated_dir = tmp_path / "curated"
    curated_dir.mkdir()
    output_dir = tmp_path / "analytics"

    _write_jsonl(
        curated_dir / "lawyer_entity.jsonl",
        [{"lawyer_id": "law-1", "name": "ADV SILVA"}],
    )
    _write_jsonl(
        curated_dir / "representation_edge.jsonl",
        [
            {
                "edge_id": "e1",
                "process_id": "proc-1",
                "representative_entity_id": "law-1",
                "representative_kind": "lawyer",
                "lawyer_id": "law-1",
                "party_id": "party-a",
                "evidence_count": 3,
            },
        ],
    )
    _write_jsonl(
        curated_dir / "representation_event.jsonl",
        [
            {"edge_id": "e1", "event_type": "habilitacao", "event_date": "2020-01-15"},
            {"edge_id": "e1", "event_type": "substabelecimento", "event_date": "2020-06-20"},
            {"edge_id": "e1", "event_type": "habilitacao", "event_date": "2020-03-10"},
        ],
    )

    result = build_representation_graph(curated_dir=curated_dir, output_dir=output_dir)

    records = _read_output(result)
    assert len(records) == 1
    rec = records[0]
    assert rec["edge_id"] == "e1"
    assert rec["event_count"] == 3
    assert rec["event_types"]["habilitacao"] == 2
    assert rec["event_types"]["substabelecimento"] == 1
    assert rec["first_event_date"] == "2020-01-15"
    assert rec["last_event_date"] == "2020-06-20"
    assert rec["evidence_count"] == 3


def test_event_aggregation_by_edge(tmp_path: Path):
    """Events should be aggregated per edge, not globally."""
    curated_dir = tmp_path / "curated"
    curated_dir.mkdir()
    output_dir = tmp_path / "analytics"

    _write_jsonl(
        curated_dir / "representation_edge.jsonl",
        [
            {
                "edge_id": "e1",
                "process_id": "proc-1",
                "representative_entity_id": "law-1",
                "representative_kind": "lawyer",
                "lawyer_id": "law-1",
                "party_id": "party-a",
            },
            {
                "edge_id": "e2",
                "process_id": "proc-2",
                "representative_entity_id": "law-2",
                "representative_kind": "lawyer",
                "lawyer_id": "law-2",
                "party_id": "party-b",
            },
        ],
    )
    _write_jsonl(
        curated_dir / "representation_event.jsonl",
        [
            {"edge_id": "e1", "event_type": "habilitacao", "event_date": "2020-01-01"},
            {"edge_id": "e1", "event_type": "habilitacao", "event_date": "2020-02-01"},
            {"edge_id": "e2", "event_type": "substabelecimento", "event_date": "2021-05-01"},
        ],
    )

    result = build_representation_graph(curated_dir=curated_dir, output_dir=output_dir)

    records = _read_output(result)
    assert len(records) == 2
    rec_e1 = [r for r in records if r["edge_id"] == "e1"][0]
    rec_e2 = [r for r in records if r["edge_id"] == "e2"][0]
    assert rec_e1["event_count"] == 2
    assert rec_e2["event_count"] == 1
    assert rec_e1["event_types"] == {"habilitacao": 2}
    assert rec_e2["event_types"] == {"substabelecimento": 1}


# -- Active span calculation ---------------------------------------------------


def test_active_span_days_calculation(tmp_path: Path):
    """Active span should be the difference between first and last event dates."""
    curated_dir = tmp_path / "curated"
    curated_dir.mkdir()
    output_dir = tmp_path / "analytics"

    _write_jsonl(
        curated_dir / "representation_edge.jsonl",
        [
            {
                "edge_id": "e1",
                "process_id": "proc-1",
                "representative_entity_id": "law-1",
                "representative_kind": "lawyer",
                "lawyer_id": "law-1",
                "party_id": "party-a",
            },
        ],
    )
    _write_jsonl(
        curated_dir / "representation_event.jsonl",
        [
            {"edge_id": "e1", "event_type": "habilitacao", "event_date": "2020-01-01"},
            {"edge_id": "e1", "event_type": "habilitacao", "event_date": "2020-04-10"},
        ],
    )

    result = build_representation_graph(curated_dir=curated_dir, output_dir=output_dir)

    records = _read_output(result)
    assert records[0]["active_span_days"] == 100  # Jan 1 to Apr 10 = 100 days


def test_active_span_zero_single_event(tmp_path: Path):
    """Active span should be 0 when there is only one event."""
    curated_dir = tmp_path / "curated"
    curated_dir.mkdir()
    output_dir = tmp_path / "analytics"

    _write_jsonl(
        curated_dir / "representation_edge.jsonl",
        [
            {
                "edge_id": "e1",
                "process_id": "proc-1",
                "representative_entity_id": "law-1",
                "representative_kind": "lawyer",
                "lawyer_id": "law-1",
                "party_id": "party-a",
            },
        ],
    )
    _write_jsonl(
        curated_dir / "representation_event.jsonl",
        [{"edge_id": "e1", "event_type": "habilitacao", "event_date": "2020-06-15"}],
    )

    result = build_representation_graph(curated_dir=curated_dir, output_dir=output_dir)

    records = _read_output(result)
    assert records[0]["active_span_days"] == 0


def test_active_span_zero_no_events(tmp_path: Path):
    """Active span should be 0 when there are no events for the edge."""
    curated_dir = tmp_path / "curated"
    curated_dir.mkdir()
    output_dir = tmp_path / "analytics"

    _write_jsonl(
        curated_dir / "representation_edge.jsonl",
        [
            {
                "edge_id": "e1",
                "process_id": "proc-1",
                "representative_entity_id": "law-1",
                "representative_kind": "lawyer",
                "lawyer_id": "law-1",
                "party_id": "party-a",
            },
        ],
    )

    result = build_representation_graph(curated_dir=curated_dir, output_dir=output_dir)

    records = _read_output(result)
    assert records[0]["active_span_days"] == 0
    assert records[0]["event_count"] == 0
    assert records[0]["first_event_date"] is None
    assert records[0]["last_event_date"] is None


# -- Co-lawyer detection -------------------------------------------------------


def test_co_lawyer_ids_detection(tmp_path: Path):
    """Co-lawyers on the same side of the same process should be detected."""
    curated_dir = tmp_path / "curated"
    curated_dir.mkdir()
    output_dir = tmp_path / "analytics"

    _write_jsonl(
        curated_dir / "representation_edge.jsonl",
        [
            {
                "edge_id": "e1",
                "process_id": "proc-1",
                "representative_entity_id": "law-1",
                "representative_kind": "lawyer",
                "lawyer_id": "law-1",
                "party_id": "party-a",
            },
            {
                "edge_id": "e2",
                "process_id": "proc-1",
                "representative_entity_id": "law-2",
                "representative_kind": "lawyer",
                "lawyer_id": "law-2",
                "party_id": "party-a",
            },
            {
                "edge_id": "e3",
                "process_id": "proc-1",
                "representative_entity_id": "law-3",
                "representative_kind": "lawyer",
                "lawyer_id": "law-3",
                "party_id": "party-b",  # different party
            },
        ],
    )

    result = build_representation_graph(curated_dir=curated_dir, output_dir=output_dir)

    records = _read_output(result)
    rec_e1 = [r for r in records if r["edge_id"] == "e1"][0]
    rec_e2 = [r for r in records if r["edge_id"] == "e2"][0]
    rec_e3 = [r for r in records if r["edge_id"] == "e3"][0]

    # law-1 and law-2 share party-a on proc-1
    assert rec_e1["co_lawyer_ids"] == ["law-2"]
    assert rec_e2["co_lawyer_ids"] == ["law-1"]
    # law-3 is on party-b, so no co-lawyers
    assert rec_e3["co_lawyer_ids"] == []


def test_co_lawyer_ids_different_processes(tmp_path: Path):
    """Lawyers on different processes should not be co-lawyers."""
    curated_dir = tmp_path / "curated"
    curated_dir.mkdir()
    output_dir = tmp_path / "analytics"

    _write_jsonl(
        curated_dir / "representation_edge.jsonl",
        [
            {
                "edge_id": "e1",
                "process_id": "proc-1",
                "representative_entity_id": "law-1",
                "representative_kind": "lawyer",
                "lawyer_id": "law-1",
                "party_id": "party-a",
            },
            {
                "edge_id": "e2",
                "process_id": "proc-2",
                "representative_entity_id": "law-2",
                "representative_kind": "lawyer",
                "lawyer_id": "law-2",
                "party_id": "party-a",
            },
        ],
    )

    result = build_representation_graph(curated_dir=curated_dir, output_dir=output_dir)

    records = _read_output(result)
    for rec in records:
        assert rec["co_lawyer_ids"] == []


# -- Output files --------------------------------------------------------------


def test_output_file_creation(tmp_path: Path):
    """Both output files should be created even with no data."""
    curated_dir = tmp_path / "curated"
    curated_dir.mkdir()
    output_dir = tmp_path / "analytics"

    result = build_representation_graph(curated_dir=curated_dir, output_dir=output_dir)

    assert result.exists()
    assert (output_dir / "representation_graph.jsonl").exists()
    assert (output_dir / "representation_graph_summary.json").exists()


def test_output_dir_created_automatically(tmp_path: Path):
    """Output directory should be created if it does not exist."""
    curated_dir = tmp_path / "curated"
    curated_dir.mkdir()
    output_dir = tmp_path / "analytics" / "nested"

    result = build_representation_graph(curated_dir=curated_dir, output_dir=output_dir)

    assert result.exists()
    assert output_dir.exists()


def test_summary_file_contents(tmp_path: Path):
    """Summary file should contain correct aggregate counts."""
    curated_dir = tmp_path / "curated"
    curated_dir.mkdir()
    output_dir = tmp_path / "analytics"

    _write_jsonl(
        curated_dir / "lawyer_entity.jsonl",
        [{"lawyer_id": "law-1", "name": "ADV SILVA"}],
    )
    _write_jsonl(
        curated_dir / "law_firm_entity.jsonl",
        [{"firm_id": "firm-1", "name": "ESCRITORIO X"}],
    )
    _write_jsonl(
        curated_dir / "representation_edge.jsonl",
        [
            {
                "edge_id": "e1",
                "process_id": "proc-1",
                "representative_entity_id": "law-1",
                "representative_kind": "lawyer",
                "lawyer_id": "law-1",
                "party_id": "party-a",
            },
            {
                "edge_id": "e2",
                "process_id": "proc-2",
                "representative_entity_id": "firm-1",
                "representative_kind": "law_firm",
                "firm_id": "firm-1",
                "party_id": "party-b",
            },
        ],
    )
    _write_jsonl(
        curated_dir / "representation_event.jsonl",
        [
            {"edge_id": "e1", "event_type": "habilitacao", "event_date": "2020-01-01"},
            {"edge_id": "e1", "event_type": "habilitacao", "event_date": "2020-02-01"},
        ],
    )

    build_representation_graph(curated_dir=curated_dir, output_dir=output_dir)

    summary = json.loads((output_dir / "representation_graph_summary.json").read_text())
    assert summary["total_edges"] == 2
    assert summary["total_lawyers"] == 1
    assert summary["total_firms"] == 1
    assert summary["total_events"] == 2
    assert summary["edges_with_events"] == 1
    assert "generated_at" in summary


# -- Missing optional fields --------------------------------------------------


def test_missing_optional_fields(tmp_path: Path):
    """Edges with missing optional fields should still produce valid records."""
    curated_dir = tmp_path / "curated"
    curated_dir.mkdir()
    output_dir = tmp_path / "analytics"

    _write_jsonl(
        curated_dir / "representation_edge.jsonl",
        [
            {
                "edge_id": "e1",
                "process_id": "proc-1",
                "representative_entity_id": "law-1",
                "representative_kind": "lawyer",
                # no lawyer_id, no firm_id, no party_id, no evidence_count
            },
        ],
    )

    result = build_representation_graph(curated_dir=curated_dir, output_dir=output_dir)

    records = _read_output(result)
    assert len(records) == 1
    rec = records[0]
    assert rec["lawyer_id"] is None
    assert rec["firm_id"] is None
    assert rec["party_id"] == ""
    assert rec["evidence_count"] == 0
    assert rec["co_lawyer_ids"] == []


# -- Progress callback --------------------------------------------------------


def test_progress_callback(tmp_path: Path):
    """Progress callback should be called with increasing steps."""
    curated_dir = tmp_path / "curated"
    curated_dir.mkdir()
    output_dir = tmp_path / "analytics"

    calls: list[tuple[int, int, str]] = []

    def on_progress(current: int, total: int, desc: str) -> None:
        calls.append((current, total, desc))

    build_representation_graph(
        curated_dir=curated_dir, output_dir=output_dir, on_progress=on_progress
    )

    assert len(calls) == 5
    assert all(t == 5 for _, t, _ in calls)
    steps = [c for c, _, _ in calls]
    assert steps == [0, 1, 2, 3, 4]
