from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics.procedural_timeline import build_procedural_timeline


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def _setup_curated(tmp_path: Path) -> Path:
    """Create a curated directory with minimal required files."""
    curated = tmp_path / "curated"
    curated.mkdir()
    return curated


def test_builds_timeline_from_movements(tmp_path: Path):
    curated = _setup_curated(tmp_path)
    output_dir = tmp_path / "analytics"
    output_dir.mkdir()

    _write_jsonl(
        curated / "movement.jsonl",
        [
            {
                "movement_id": "m1",
                "process_id": "proc_1",
                "movement_category": "distribuicao",
                "movement_date": "2020-01-15",
                "movement_raw_description": "Distribuído por sorteio",
            },
            {
                "movement_id": "m2",
                "process_id": "proc_1",
                "movement_category": "vista",
                "movement_date": "2020-03-01",
                "movement_raw_description": "Vista ao Ministério Público",
            },
            {
                "movement_id": "m3",
                "process_id": "proc_1",
                "movement_category": "decisao",
                "movement_date": "2020-06-01",
                "movement_raw_description": "Decisão monocrática",
            },
        ],
    )
    _write_jsonl(
        curated / "session_event.jsonl",
        [
            {
                "session_event_id": "se1",
                "process_id": "proc_1",
                "event_type": "pedido_de_vista",
                "event_date": "2020-03-01",
                "rapporteur_at_event": "MIN A",
                "vista_duration_days": 30,
            },
            {
                "session_event_id": "se2",
                "process_id": "proc_1",
                "event_type": "pauta_inclusion",
                "event_date": "2020-04-01",
                "rapporteur_at_event": "MIN A",
            },
        ],
    )
    _write_jsonl(
        curated / "process.jsonl",
        [
            {
                "process_id": "proc_1",
                "process_number": "RE 12345",
                "process_class": "RE",
                "first_distribution_date": "2020-01-15",
                "filing_date": "2020-01-10",
            },
        ],
    )
    _write_jsonl(
        curated / "decision_event.jsonl",
        [
            {
                "decision_event_id": "de1",
                "process_id": "proc_1",
                "decision_date": "2020-06-01",
            },
        ],
    )

    result = build_procedural_timeline(curated_dir=curated, output_dir=output_dir)
    assert result.exists()

    records = [json.loads(line) for line in result.read_text().strip().split("\n") if line]
    assert len(records) == 1

    rec = records[0]
    assert rec["process_id"] == "proc_1"
    assert rec["total_movement_count"] == 3
    assert rec["vista_count"] == 1
    assert rec["days_in_vista_total"] == 30
    assert rec["pauta_inclusion_count"] == 1
    assert rec["pauta_withdrawal_count"] == 0
    assert rec["pauta_cycle_count"] == 0
    assert rec["first_distribution_date"] == "2020-01-15"
    assert rec["first_decision_date"] == "2020-06-01"
    assert rec["days_distribution_to_first_decision"] is not None
    assert rec["days_distribution_to_first_decision"] > 0


def test_peer_group_percentiles(tmp_path: Path):
    """Test peer group percentile calculation with enough peers."""
    curated = _setup_curated(tmp_path)
    output_dir = tmp_path / "analytics"
    output_dir.mkdir()

    movements = []
    processes = []
    decisions = []
    # Create 10 processes with same class and decision year for peer grouping
    for i in range(10):
        pid = f"proc_{i}"
        movements.append(
            {
                "movement_id": f"m_{i}",
                "process_id": pid,
                "movement_category": "distribuicao",
                "movement_date": "2020-01-01",
            }
        )
        processes.append(
            {
                "process_id": pid,
                "process_class": "RE",
                "first_distribution_date": "2020-01-01",
            }
        )
        # Spread decision dates: i * 30 days
        day = 1 + i * 3
        month = 1 + (i * 30) // 28
        if month > 12:
            month = 12
        decisions.append(
            {
                "decision_event_id": f"de_{i}",
                "process_id": pid,
                "decision_date": f"2020-{month:02d}-{min(day, 28):02d}",
            }
        )

    _write_jsonl(curated / "movement.jsonl", movements)
    _write_jsonl(curated / "session_event.jsonl", [])
    _write_jsonl(curated / "process.jsonl", processes)
    _write_jsonl(curated / "decision_event.jsonl", decisions)

    result = build_procedural_timeline(curated_dir=curated, output_dir=output_dir, min_peer_group_size=5)

    records = [json.loads(line) for line in result.read_text().strip().split("\n") if line]
    assert len(records) == 10

    # All should have peer group stats since we have 10 peers
    with_peers = [r for r in records if r["peer_group_key"] is not None]
    assert len(with_peers) == 10
    assert with_peers[0]["peer_group_size"] == 10
    assert with_peers[0]["peer_median_days_to_decision"] is not None


def test_red_flag_vista_above_p95(tmp_path: Path):
    """Test vista flag is set when days_in_vista_total exceeds peer P95."""
    curated = _setup_curated(tmp_path)
    output_dir = tmp_path / "analytics"
    output_dir.mkdir()

    movements = []
    processes = []
    decisions = []
    session_events = []

    # Create 10 processes, one with extreme vista duration
    for i in range(10):
        pid = f"proc_{i}"
        movements.append(
            {
                "movement_id": f"m_{i}",
                "process_id": pid,
                "movement_category": "distribuicao",
                "movement_date": "2020-01-01",
            }
        )
        processes.append(
            {
                "process_id": pid,
                "process_class": "RE",
                "first_distribution_date": "2020-01-01",
            }
        )
        decisions.append(
            {
                "decision_event_id": f"de_{i}",
                "process_id": pid,
                "decision_date": "2020-06-01",
            }
        )
        # proc_0 has extreme vista: 500 days; others have 0-5 days
        if i == 0:
            session_events.append(
                {
                    "session_event_id": f"se_{i}",
                    "process_id": pid,
                    "event_type": "pedido_de_vista",
                    "event_date": "2020-02-01",
                    "rapporteur_at_event": "MIN A",
                    "vista_duration_days": 500,
                }
            )
        elif i < 5:
            session_events.append(
                {
                    "session_event_id": f"se_{i}",
                    "process_id": pid,
                    "event_type": "pedido_de_vista",
                    "event_date": "2020-02-01",
                    "rapporteur_at_event": "MIN A",
                    "vista_duration_days": i,
                }
            )

    _write_jsonl(curated / "movement.jsonl", movements)
    _write_jsonl(curated / "session_event.jsonl", session_events)
    _write_jsonl(curated / "process.jsonl", processes)
    _write_jsonl(curated / "decision_event.jsonl", decisions)

    result = build_procedural_timeline(curated_dir=curated, output_dir=output_dir, min_peer_group_size=5)

    records = [json.loads(line) for line in result.read_text().strip().split("\n") if line]
    flagged = [r for r in records if r["vista_flag"]]
    assert len(flagged) >= 1
    # proc_0 should be flagged
    proc0 = [r for r in records if r["process_id"] == "proc_0"]
    assert proc0[0]["vista_flag"] is True


def test_red_flag_pauta_cycles(tmp_path: Path):
    """Test pauta_cycle_flag is set when pauta_cycle_count exceeds peer P95."""
    curated = _setup_curated(tmp_path)
    output_dir = tmp_path / "analytics"
    output_dir.mkdir()

    movements = []
    processes = []
    decisions = []
    session_events = []

    # Create 10 processes, one with many pauta cycles
    for i in range(10):
        pid = f"proc_{i}"
        movements.append(
            {
                "movement_id": f"m_{i}",
                "process_id": pid,
                "movement_category": "distribuicao",
                "movement_date": "2020-01-01",
            }
        )
        processes.append(
            {
                "process_id": pid,
                "process_class": "RE",
                "first_distribution_date": "2020-01-01",
            }
        )
        decisions.append(
            {
                "decision_event_id": f"de_{i}",
                "process_id": pid,
                "decision_date": "2020-06-01",
            }
        )

        # proc_0 has 10 pauta in/out cycles; others have 0
        if i == 0:
            for j in range(10):
                session_events.append(
                    {
                        "session_event_id": f"se_inc_{i}_{j}",
                        "process_id": pid,
                        "event_type": "pauta_inclusion",
                        "event_date": f"2020-0{min(j + 1, 9)}-01",
                        "rapporteur_at_event": "MIN A",
                    }
                )
                session_events.append(
                    {
                        "session_event_id": f"se_wdr_{i}_{j}",
                        "process_id": pid,
                        "event_type": "pauta_withdrawal",
                        "event_date": f"2020-0{min(j + 1, 9)}-15",
                        "rapporteur_at_event": "MIN A",
                    }
                )

    _write_jsonl(curated / "movement.jsonl", movements)
    _write_jsonl(curated / "session_event.jsonl", session_events)
    _write_jsonl(curated / "process.jsonl", processes)
    _write_jsonl(curated / "decision_event.jsonl", decisions)

    result = build_procedural_timeline(curated_dir=curated, output_dir=output_dir, min_peer_group_size=5)

    records = [json.loads(line) for line in result.read_text().strip().split("\n") if line]
    proc0 = [r for r in records if r["process_id"] == "proc_0"]
    assert proc0[0]["pauta_cycle_count"] == 10
    assert proc0[0]["pauta_cycle_flag"] is True


def test_empty_input(tmp_path: Path):
    curated = _setup_curated(tmp_path)
    output_dir = tmp_path / "analytics"
    output_dir.mkdir()

    _write_jsonl(curated / "movement.jsonl", [])
    _write_jsonl(curated / "session_event.jsonl", [])
    _write_jsonl(curated / "process.jsonl", [])
    _write_jsonl(curated / "decision_event.jsonl", [])

    result = build_procedural_timeline(curated_dir=curated, output_dir=output_dir)
    assert result.exists()
    assert result.read_text() == ""

    summary = json.loads((output_dir / "procedural_timeline_summary.json").read_text())
    assert summary["total_records"] == 0


def test_processes_without_movements_skipped(tmp_path: Path):
    """Processes not in movement.jsonl should not appear in output."""
    curated = _setup_curated(tmp_path)
    output_dir = tmp_path / "analytics"
    output_dir.mkdir()

    # Only proc_1 has movements; proc_2 does not
    _write_jsonl(
        curated / "movement.jsonl",
        [
            {
                "movement_id": "m1",
                "process_id": "proc_1",
                "movement_category": "distribuicao",
                "movement_date": "2020-01-01",
            },
        ],
    )
    _write_jsonl(curated / "session_event.jsonl", [])
    _write_jsonl(
        curated / "process.jsonl",
        [
            {"process_id": "proc_1", "process_class": "RE"},
            {"process_id": "proc_2", "process_class": "RE"},
        ],
    )
    _write_jsonl(
        curated / "decision_event.jsonl",
        [
            {"decision_event_id": "de1", "process_id": "proc_1", "decision_date": "2020-06-01"},
            {"decision_event_id": "de2", "process_id": "proc_2", "decision_date": "2020-06-01"},
        ],
    )

    result = build_procedural_timeline(curated_dir=curated, output_dir=output_dir)
    records = [json.loads(line) for line in result.read_text().strip().split("\n") if line]

    pids = [r["process_id"] for r in records]
    assert "proc_1" in pids
    assert "proc_2" not in pids


def test_missing_movement_file(tmp_path: Path):
    """When movement.jsonl does not exist, output should be empty."""
    curated = _setup_curated(tmp_path)
    output_dir = tmp_path / "analytics"
    output_dir.mkdir()

    # No movement.jsonl created
    result = build_procedural_timeline(curated_dir=curated, output_dir=output_dir)
    assert result.exists()
    assert result.read_text() == ""


def test_redistribution_count(tmp_path: Path):
    curated = _setup_curated(tmp_path)
    output_dir = tmp_path / "analytics"
    output_dir.mkdir()

    _write_jsonl(
        curated / "movement.jsonl",
        [
            {
                "movement_id": "m1",
                "process_id": "proc_1",
                "movement_category": "distribuicao",
                "movement_date": "2020-01-01",
                "movement_raw_description": "Distribuído",
            },
            {
                "movement_id": "m2",
                "process_id": "proc_1",
                "movement_category": "deslocamento",
                "movement_date": "2020-02-01",
                "movement_raw_description": "Redistribuição ao Min. X",
            },
            {
                "movement_id": "m3",
                "process_id": "proc_1",
                "movement_category": "deslocamento",
                "movement_date": "2020-03-01",
                "movement_raw_description": "Remetido ao Plenário",
            },
        ],
    )
    _write_jsonl(curated / "session_event.jsonl", [])
    _write_jsonl(curated / "process.jsonl", [{"process_id": "proc_1", "process_class": "RE"}])
    _write_jsonl(curated / "decision_event.jsonl", [])

    result = build_procedural_timeline(curated_dir=curated, output_dir=output_dir)
    records = [json.loads(line) for line in result.read_text().strip().split("\n") if line]

    assert len(records) == 1
    # Only the first deslocamento is a redistribution
    assert records[0]["redistribution_count"] == 1
