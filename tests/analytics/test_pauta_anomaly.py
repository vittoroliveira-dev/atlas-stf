from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics.pauta_anomaly import build_pauta_anomaly


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def test_vista_frequency_z_score(tmp_path: Path):
    """Minister with many vista requests should have high frequency z-score."""
    se_path = tmp_path / "session_event.jsonl"
    output_dir = tmp_path / "analytics"
    output_dir.mkdir()

    events = []
    # MIN A: 20 vista requests in 2020
    for i in range(20):
        events.append(
            {
                "session_event_id": f"se_a_{i}",
                "process_id": f"proc_a_{i}",
                "event_type": "pedido_de_vista",
                "event_date": f"2020-{min(i + 1, 12):02d}-01",
                "rapporteur_at_event": "MIN A",
                "vista_duration_days": 15,
            }
        )
    # MIN B: 1 vista request in 2020
    events.append(
        {
            "session_event_id": "se_b_0",
            "process_id": "proc_b_0",
            "event_type": "pedido_de_vista",
            "event_date": "2020-01-01",
            "rapporteur_at_event": "MIN B",
            "vista_duration_days": 10,
        }
    )
    # MIN C: 2 vista requests in 2020
    for i in range(2):
        events.append(
            {
                "session_event_id": f"se_c_{i}",
                "process_id": f"proc_c_{i}",
                "event_type": "pedido_de_vista",
                "event_date": "2020-03-01",
                "rapporteur_at_event": "MIN C",
                "vista_duration_days": 12,
            }
        )

    _write_jsonl(se_path, events)

    result = build_pauta_anomaly(session_event_path=se_path, output_dir=output_dir)
    records = [json.loads(line) for line in result.read_text().strip().split("\n") if line]

    # All 3 ministers in 2020
    assert len(records) == 3
    min_a = [r for r in records if r["rapporteur"] == "MIN A"][0]
    assert min_a["vista_request_count"] == 20
    assert min_a["vista_frequency_z_score"] is not None
    # MIN A should have high z-score (many more requests than others)
    assert min_a["vista_frequency_z_score"] > 1.0


def test_vista_duration_z_score(tmp_path: Path):
    """Minister with long average vista duration should have high duration z-score."""
    se_path = tmp_path / "session_event.jsonl"
    output_dir = tmp_path / "analytics"
    output_dir.mkdir()

    events = []
    # MIN A: very long vista durations (200 days average)
    for i in range(3):
        events.append(
            {
                "session_event_id": f"se_a_{i}",
                "process_id": f"proc_a_{i}",
                "event_type": "pedido_de_vista",
                "event_date": "2020-01-01",
                "rapporteur_at_event": "MIN A",
                "vista_duration_days": 200,
            }
        )
    # MIN B: short durations (10 days)
    for i in range(3):
        events.append(
            {
                "session_event_id": f"se_b_{i}",
                "process_id": f"proc_b_{i}",
                "event_type": "pedido_de_vista",
                "event_date": "2020-02-01",
                "rapporteur_at_event": "MIN B",
                "vista_duration_days": 10,
            }
        )
    # MIN C: short durations (15 days)
    for i in range(3):
        events.append(
            {
                "session_event_id": f"se_c_{i}",
                "process_id": f"proc_c_{i}",
                "event_type": "pedido_de_vista",
                "event_date": "2020-03-01",
                "rapporteur_at_event": "MIN C",
                "vista_duration_days": 15,
            }
        )

    _write_jsonl(se_path, events)

    result = build_pauta_anomaly(session_event_path=se_path, output_dir=output_dir)
    records = [json.loads(line) for line in result.read_text().strip().split("\n") if line]

    min_a = [r for r in records if r["rapporteur"] == "MIN A"][0]
    assert min_a["vista_avg_duration_days"] == 200.0
    assert min_a["vista_duration_z_score"] is not None
    assert min_a["vista_duration_z_score"] > 1.0
    assert min_a["vista_max_duration_days"] == 200


def test_pauta_withdrawal_no_rejudge(tmp_path: Path):
    """Pauta withdrawal without re-inclusion within 90 days should be counted."""
    se_path = tmp_path / "session_event.jsonl"
    output_dir = tmp_path / "analytics"
    output_dir.mkdir()

    events = [
        # Withdrawal with no subsequent re-inclusion
        {
            "session_event_id": "se1",
            "process_id": "proc_1",
            "event_type": "pauta_withdrawal",
            "event_date": "2020-03-01",
            "rapporteur_at_event": "MIN A",
        },
        # Withdrawal WITH re-inclusion within 90 days
        {
            "session_event_id": "se2",
            "process_id": "proc_2",
            "event_type": "pauta_withdrawal",
            "event_date": "2020-04-01",
            "rapporteur_at_event": "MIN A",
        },
        {
            "session_event_id": "se3",
            "process_id": "proc_2",
            "event_type": "pauta_inclusion",
            "event_date": "2020-05-01",
            "rapporteur_at_event": "MIN A",
        },
    ]

    _write_jsonl(se_path, events)

    result = build_pauta_anomaly(session_event_path=se_path, output_dir=output_dir)
    records = [json.loads(line) for line in result.read_text().strip().split("\n") if line]

    assert len(records) == 1
    rec = records[0]
    assert rec["rapporteur"] == "MIN A"
    assert rec["pauta_withdrawal_count"] == 2
    # Only proc_1 withdrawal has no re-inclusion
    assert rec["pauta_no_rejudge_90d_count"] == 1
    assert rec["pauta_stall_flag"] is True


def test_flag_setting(tmp_path: Path):
    """Flags should be set correctly based on z-score thresholds."""
    se_path = tmp_path / "session_event.jsonl"
    output_dir = tmp_path / "analytics"
    output_dir.mkdir()

    events = []
    # MIN A: extreme outlier — 100 vistas of 365 days each
    for i in range(100):
        events.append(
            {
                "session_event_id": f"se_a_{i}",
                "process_id": f"proc_a_{i}",
                "event_type": "pedido_de_vista",
                "event_date": "2020-01-01",
                "rapporteur_at_event": "MIN A",
                "vista_duration_days": 365,
            }
        )
    # MIN B through MIN K: normal — 1 vista of 10 days
    for m_idx in range(10):
        minister = f"MIN {chr(66 + m_idx)}"
        events.append(
            {
                "session_event_id": f"se_{minister}_0",
                "process_id": f"proc_{minister}_0",
                "event_type": "pedido_de_vista",
                "event_date": "2020-02-01",
                "rapporteur_at_event": minister,
                "vista_duration_days": 10,
            }
        )

    _write_jsonl(se_path, events)

    result = build_pauta_anomaly(session_event_path=se_path, output_dir=output_dir)
    records = [json.loads(line) for line in result.read_text().strip().split("\n") if line]

    min_a = [r for r in records if r["rapporteur"] == "MIN A"][0]
    assert min_a["vista_duration_flag"] is True
    assert min_a["vista_frequency_flag"] is True


def test_empty_input(tmp_path: Path):
    se_path = tmp_path / "session_event.jsonl"
    output_dir = tmp_path / "analytics"
    output_dir.mkdir()

    _write_jsonl(se_path, [])

    result = build_pauta_anomaly(session_event_path=se_path, output_dir=output_dir)
    assert result.exists()
    assert result.read_text() == ""

    summary = json.loads((output_dir / "pauta_anomaly_summary.json").read_text())
    assert summary["total_records"] == 0
    assert summary["ministers_analyzed"] == 0


def test_missing_file(tmp_path: Path):
    """When session_event.jsonl does not exist, output should be empty."""
    se_path = tmp_path / "nonexistent.jsonl"
    output_dir = tmp_path / "analytics"
    output_dir.mkdir()

    result = build_pauta_anomaly(session_event_path=se_path, output_dir=output_dir)
    assert result.exists()
    assert result.read_text() == ""


def test_summary_file(tmp_path: Path):
    se_path = tmp_path / "session_event.jsonl"
    output_dir = tmp_path / "analytics"
    output_dir.mkdir()

    events = [
        {
            "session_event_id": "se1",
            "process_id": "proc_1",
            "event_type": "pedido_de_vista",
            "event_date": "2020-01-01",
            "rapporteur_at_event": "MIN A",
            "vista_duration_days": 30,
        },
    ]
    _write_jsonl(se_path, events)

    build_pauta_anomaly(session_event_path=se_path, output_dir=output_dir)

    summary = json.loads((output_dir / "pauta_anomaly_summary.json").read_text())
    assert summary["total_records"] == 1
    assert summary["ministers_analyzed"] == 1
    assert summary["periods_analyzed"] == 1
