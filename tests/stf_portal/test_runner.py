"""Tests for STF portal runner."""

from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.stf_portal._runner import (
    _load_process_list,
    _prioritize_processes,
    _sanitize_filename,
    _should_refetch,
)


def test_load_process_list(tmp_path: Path):
    path = tmp_path / "process.jsonl"
    records = [
        {"process_id": "proc_1", "process_number": "ADI 1234"},
        {"process_id": "proc_2", "process_number": "HC 999"},
    ]
    path.write_text(
        "\n".join(json.dumps(r) for r in records) + "\n",
        encoding="utf-8",
    )
    result = _load_process_list(tmp_path)
    assert len(result) == 2
    assert result[0]["process_number"] == "ADI 1234"


def test_load_process_list_missing(tmp_path: Path):
    result = _load_process_list(tmp_path / "nonexistent")
    assert result == []


def test_prioritize_processes():
    processes = [
        {"process_id": "proc_1", "filing_date": "2020-01-01"},
        {"process_id": "proc_2", "filing_date": "2025-06-15"},
        {"process_id": "proc_3", "filing_date": "2022-03-10"},
    ]
    alert_ids = {"proc_3"}

    result = _prioritize_processes(processes, alert_ids)
    # Alert processes first
    assert result[0]["process_id"] == "proc_3"


def test_sanitize_filename():
    assert _sanitize_filename("ADI 1234") == "ADI_1234"
    assert _sanitize_filename("RE/ARE 5555") == "RE_ARE_5555"


def test_should_refetch_missing(tmp_path: Path):
    assert _should_refetch(tmp_path / "nonexistent.json", 30) is True


def test_should_refetch_recent(tmp_path: Path):
    from datetime import datetime, timezone

    path = tmp_path / "test.json"
    doc = {"fetched_at": datetime.now(timezone.utc).isoformat()}
    path.write_text(json.dumps(doc), encoding="utf-8")
    assert _should_refetch(path, 30) is False


def test_should_refetch_stale(tmp_path: Path):
    path = tmp_path / "test.json"
    doc = {"fetched_at": "2020-01-01T00:00:00+00:00"}
    path.write_text(json.dumps(doc), encoding="utf-8")
    assert _should_refetch(path, 30) is True
