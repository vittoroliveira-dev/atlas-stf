"""Tests for STF portal extraction metrics."""

from __future__ import annotations

import json
import threading
from pathlib import Path

from atlas_stf.stf_portal._metrics import ExtractionMetrics


def test_fresh_metrics():
    m = ExtractionMetrics()
    assert m.requests_total == 0
    assert m.processes_completed == 0


def test_increment_counters():
    m = ExtractionMetrics()
    m.inc("requests_total")
    m.inc("requests_total")
    m.inc("http_403_total")
    assert m.requests_total == 2
    assert m.http_403_total == 1


def test_increment_with_delta():
    m = ExtractionMetrics()
    m.inc("tabs_downloaded_fresh", 5)
    assert m.tabs_downloaded_fresh == 5


def test_concurrent_increments():
    m = ExtractionMetrics()
    n_per_thread = 500
    n_threads = 8

    def worker() -> None:
        for _ in range(n_per_thread):
            m.inc("requests_total")

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert m.requests_total == n_per_thread * n_threads


def test_record_resolve_ms():
    m = ExtractionMetrics()
    m.record_resolve_ms(100.0)
    m.record_resolve_ms(200.0)
    assert m.avg_resolve_ms == 150.0


def test_record_tab_ms():
    m = ExtractionMetrics()
    m.record_tab_ms(50.0)
    m.record_tab_ms(150.0)
    assert m.avg_tab_ms == 100.0


def test_timing_buffer_does_not_overflow():
    m = ExtractionMetrics()
    for i in range(2000):
        m.record_resolve_ms(float(i))
    # Buffer capped at 1000 — only last 1000 entries
    assert m.avg_resolve_ms > 0
    # Verify buffer size (access internal for test)
    assert len(m._resolve_ms) == 1000


def test_avg_resolve_ms_empty():
    m = ExtractionMetrics()
    assert m.avg_resolve_ms == 0.0


def test_effective_requests_per_hour():
    m = ExtractionMetrics()
    m.requests_total = 100
    m.elapsed_seconds = 3600.0
    assert m.effective_requests_per_hour == 100.0


def test_effective_requests_per_hour_zero_elapsed():
    m = ExtractionMetrics()
    m.requests_total = 100
    m.elapsed_seconds = 0.0
    assert m.effective_requests_per_hour == 0.0


def test_effective_processes_per_hour():
    m = ExtractionMetrics()
    m.processes_completed = 50
    m.elapsed_seconds = 1800.0  # 30 min
    assert m.effective_processes_per_hour == 100.0


def test_average_requests_per_completed_process():
    m = ExtractionMetrics()
    m.requests_total = 600
    m.processes_completed = 100
    assert m.average_requests_per_completed_process == 6.0


def test_average_requests_per_completed_zero():
    m = ExtractionMetrics()
    assert m.average_requests_per_completed_process == 0.0


def test_record_http_status():
    m = ExtractionMetrics()
    m.record_http_status(200)
    m.record_http_status(200)
    m.record_http_status(403)
    assert m.http_status_counts == {200: 2, 403: 1}


def test_to_dict():
    m = ExtractionMetrics()
    m.requests_total = 10
    m.processes_completed = 2
    m.elapsed_seconds = 60.0
    d = m.to_dict()
    assert d["requests_total"] == 10
    assert d["processes_completed"] == 2
    assert d["elapsed_seconds"] == 60.0
    assert "effective_requests_per_hour" in d
    assert "avg_resolve_ms" in d


def test_summary_line():
    m = ExtractionMetrics()
    m.processes_completed = 5
    m.processes_failed = 1
    m.http_403_total = 2
    line = m.summary_line()
    assert "completed=5" in line
    assert "failed=1" in line
    assert "403s=2" in line


def test_save_atomic(tmp_path: Path):
    m = ExtractionMetrics()
    m.requests_total = 42
    m.elapsed_seconds = 10.0
    path = tmp_path / ".metrics.json"
    m.save(path)

    assert path.exists()
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["requests_total"] == 42
    # No .tmp files should remain
    assert list(tmp_path.glob("*.tmp")) == []


def test_save_creates_parent_dir(tmp_path: Path):
    m = ExtractionMetrics()
    path = tmp_path / "subdir" / ".metrics.json"
    m.save(path)
    assert path.exists()
