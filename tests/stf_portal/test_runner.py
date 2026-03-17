"""Tests for STF portal runner."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import patch

from atlas_stf.stf_portal._checkpoint import load_checkpoint
from atlas_stf.stf_portal._config import StfPortalConfig
from atlas_stf.stf_portal._runner import (
    _load_process_list,
    _prioritize_processes,
    _sanitize_filename,
    _should_refetch,
    run_extraction,
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


def test_prioritize_processes_newer_dates_first():
    processes = [
        {"process_id": "proc_a", "filing_date": "2020-01-01"},
        {"process_id": "proc_b", "filing_date": "2025-06-15"},
        {"process_id": "proc_c", "filing_date": "2022-03-10"},
    ]
    alert_ids: set[str] = set()
    result = _prioritize_processes(processes, alert_ids)
    dates = [p["filing_date"] for p in result]
    assert dates == ["2025-06-15", "2022-03-10", "2020-01-01"]


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


# --- Phase 2.5: run_extraction integration tests ---


def _write_process_jsonl(curated_dir: Path, processes: list[dict[str, Any]]) -> None:
    curated_dir.mkdir(parents=True, exist_ok=True)
    path = curated_dir / "process.jsonl"
    path.write_text(
        "\n".join(json.dumps(p) for p in processes) + "\n",
        encoding="utf-8",
    )


def _fake_extract_process(process_number: str, incidente: str | None = None) -> dict[str, Any] | None:
    """Fake extractor that returns a minimal doc."""
    return {
        "process_number": process_number,
        "source_system": "stf_portal",
        "source_url": f"https://portal.stf.jus.br/processos/listarProcessos.asp?classe=&processo={process_number}",
        "fetched_at": "2026-03-17T00:00:00+00:00",
        "raw_html_hash": "fakehash",
        "andamentos": [],
        "deslocamentos": [],
        "peticoes": [],
        "sessao_virtual": [],
        "informacoes": {},
    }


class _FakeExtractor:
    """Fake PortalExtractor that returns deterministic results."""

    def __init__(self, **_kwargs: Any) -> None:
        pass

    def extract_process(self, process_number: str, incidente: str | None = None) -> dict[str, Any] | None:
        return _fake_extract_process(process_number, incidente)

    def close(self) -> None:
        pass

    def __enter__(self) -> _FakeExtractor:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def test_run_extraction_sequential(tmp_path: Path):
    """run_extraction with workers=1 fetches all processes and saves checkpoint."""
    curated_dir = tmp_path / "curated"
    output_dir = tmp_path / "portal"
    checkpoint_file = output_dir / ".checkpoint.json"

    processes = [{"process_id": f"proc_{i}", "process_number": f"ADI {i}"} for i in range(5)]
    _write_process_jsonl(curated_dir, processes)

    config = StfPortalConfig(
        output_dir=output_dir,
        curated_dir=curated_dir,
        checkpoint_file=checkpoint_file,
        max_concurrent=1,
        rate_limit_seconds=0.0,
    )

    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _FakeExtractor):
        fetched = run_extraction(config)

    assert fetched == 5

    # Verify output files
    json_files = list(output_dir.glob("ADI_*.json"))
    assert len(json_files) == 5

    # Verify checkpoint
    cp = load_checkpoint(checkpoint_file)
    assert cp.total_fetched == 5
    assert len(cp.completed_processes) == 5


def test_run_extraction_concurrent(tmp_path: Path):
    """run_extraction with workers=4 fetches all processes correctly."""
    curated_dir = tmp_path / "curated"
    output_dir = tmp_path / "portal"
    checkpoint_file = output_dir / ".checkpoint.json"

    processes = [{"process_id": f"proc_{i}", "process_number": f"HC {i}"} for i in range(20)]
    _write_process_jsonl(curated_dir, processes)

    config = StfPortalConfig(
        output_dir=output_dir,
        curated_dir=curated_dir,
        checkpoint_file=checkpoint_file,
        max_concurrent=4,
        rate_limit_seconds=0.0,
    )

    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _FakeExtractor):
        fetched = run_extraction(config)

    assert fetched == 20

    # Verify no duplicates
    json_files = list(output_dir.glob("HC_*.json"))
    assert len(json_files) == 20

    # Verify checkpoint
    cp = load_checkpoint(checkpoint_file)
    assert cp.total_fetched == 20
    assert len(cp.completed_processes) == 20


def test_run_extraction_skips_completed(tmp_path: Path):
    """run_extraction skips already completed processes."""
    curated_dir = tmp_path / "curated"
    output_dir = tmp_path / "portal"
    checkpoint_file = output_dir / ".checkpoint.json"

    processes = [
        {"process_id": "proc_1", "process_number": "ADI 100"},
        {"process_id": "proc_2", "process_number": "ADI 200"},
    ]
    _write_process_jsonl(curated_dir, processes)

    # Pre-populate checkpoint and output for ADI 100
    from atlas_stf.stf_portal._checkpoint import PortalCheckpoint, save_checkpoint

    cp = PortalCheckpoint()
    cp.mark_completed("ADI 100")
    save_checkpoint(cp, checkpoint_file)
    (output_dir / "ADI_100.json").write_text(
        json.dumps({"fetched_at": "2026-03-17T00:00:00+00:00"}),
        encoding="utf-8",
    )

    config = StfPortalConfig(
        output_dir=output_dir,
        curated_dir=curated_dir,
        checkpoint_file=checkpoint_file,
        max_concurrent=1,
        rate_limit_seconds=0.0,
    )

    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _FakeExtractor):
        fetched = run_extraction(config)

    # Only ADI 200 should have been fetched
    assert fetched == 1

    cp = load_checkpoint(checkpoint_file)
    assert cp.is_completed("ADI 100")
    assert cp.is_completed("ADI 200")
    assert cp.total_fetched == 2  # 1 from pre-populated + 1 new


def test_run_extraction_dry_run(tmp_path: Path):
    """Dry run returns 0 and doesn't create files."""
    curated_dir = tmp_path / "curated"
    output_dir = tmp_path / "portal"

    processes = [{"process_id": "proc_1", "process_number": "ADI 100"}]
    _write_process_jsonl(curated_dir, processes)

    config = StfPortalConfig(
        output_dir=output_dir,
        curated_dir=curated_dir,
        checkpoint_file=output_dir / ".checkpoint.json",
    )

    result = run_extraction(config, dry_run=True)
    assert result == 0
    assert not (output_dir / ".checkpoint.json").exists()
