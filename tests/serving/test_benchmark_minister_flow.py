"""Smoke test for the minister flow benchmark script."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path("scripts/benchmark_minister_flow.py")


@pytest.mark.skipif(
    not Path("data/serving/atlas_stf.db").exists(),
    reason="serving DB not available",
)
def test_benchmark_smoke_generates_valid_json(tmp_path: Path) -> None:
    """Run benchmark with --tasks 10 and verify output is valid JSON with required fields."""
    output = tmp_path / "bench.json"
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--tasks", "10", "--output", str(output)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, f"Benchmark failed: {result.stderr}"
    assert output.exists(), "Output file not created"

    report = json.loads(output.read_text())

    # Required top-level fields
    for field in (
        "benchmark",
        "commit",
        "timestamp",
        "machine",
        "workers",
        "scenario",
        "tasks_computed",
        "durations_seconds",
        "throughput",
        "output_equivalence",
    ):
        assert field in report, f"Missing required field: {field}"

    assert report["tasks_computed"] == 10
    assert report["scenario"] == "subset_10"

    # Duration sub-fields
    dur = report["durations_seconds"]
    for sub in ("load", "index", "enumerate", "hist_cache", "compute", "total"):
        assert sub in dur, f"Missing duration sub-field: {sub}"
        assert isinstance(dur[sub], (int, float))
        assert dur[sub] >= 0

    # Equivalence check
    eq = report["output_equivalence"]
    assert "fingerprint" in eq
    assert len(eq["fingerprint"]) == 16
    assert "method" in eq
