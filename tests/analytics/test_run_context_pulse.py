"""Tests for RunContext.pulse() structured extra and semantic separation from advance."""

from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics._run_context import RunContext


class TestPulseDoesNotContaminateAdvance:
    def test_pulse_preserves_last_advance_at(self, tmp_path: Path) -> None:
        ctx = RunContext("test-pulse", tmp_path, 1)
        ctx.start_step(1, "step 1", total_items=10)
        ctx.advance(1)

        status = json.loads((ctx._run_dir / "status.json").read_text())
        advance_at = status["step_progress"]["last_advance_at"]

        ctx.pulse("heartbeat", extra={"phase": "scan"})

        # Force snapshot to capture pulse state
        ctx._write_snapshot()
        status_after = json.loads((ctx._run_dir / "status.json").read_text())
        # pulse should NOT update last_advance_at
        assert status_after["step_progress"]["last_advance_at"] == advance_at
        ctx.finish()

    def test_pulse_updates_last_pulse_at(self, tmp_path: Path) -> None:
        ctx = RunContext("test-pulse", tmp_path, 1)
        ctx.start_step(1, "step 1")

        # Check in-memory to avoid race with heartbeat thread on status.json
        assert ctx._last_pulse_at is None

        ctx.pulse("heartbeat")
        assert ctx._last_pulse_at is not None

        # Verify snapshot after finish (heartbeat stopped, no file I/O race)
        ctx.finish()
        status = json.loads((ctx._run_dir / "status.json").read_text())
        assert status["last_pulse_at"] is not None

    def test_pulse_extra_appears_in_snapshot(self, tmp_path: Path) -> None:
        ctx = RunContext("test-pulse", tmp_path, 1)
        ctx.start_step(1, "step 1")
        ctx.pulse("scan", extra={"phase": "scan", "count": 42})

        ctx._write_snapshot()
        status = json.loads((ctx._run_dir / "status.json").read_text())
        assert status["last_pulse_extra"]["phase"] == "scan"
        assert status["last_pulse_extra"]["count"] == 42
        ctx.finish()

    def test_pulse_extra_sanitizes_large_values(self, tmp_path: Path) -> None:
        ctx = RunContext("test-pulse", tmp_path, 1)
        ctx.start_step(1, "step 1")
        ctx.pulse("big", extra={"big": list(range(500))})

        ctx._write_snapshot()
        status = json.loads((ctx._run_dir / "status.json").read_text())
        assert "<truncated" in str(status["last_pulse_extra"]["big"])
        ctx.finish()

    def test_pulse_extra_in_events_jsonl(self, tmp_path: Path) -> None:
        ctx = RunContext("test-pulse", tmp_path, 1)
        ctx.start_step(1, "step 1")
        ctx.pulse("ev", extra={"k": "v"})

        events_path = ctx._run_dir / "events.jsonl"
        lines = events_path.read_text().strip().split("\n")
        pulse_events = [json.loads(line) for line in lines if json.loads(line).get("event") == "pulse"]
        assert len(pulse_events) >= 1
        assert pulse_events[-1]["extra"]["k"] == "v"
        ctx.finish()
