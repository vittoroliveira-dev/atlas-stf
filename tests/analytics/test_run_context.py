"""Tests for the RunContext observable execution runtime."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from atlas_stf.analytics._run_context import RunContext, RunManifest, _read_rss_mb


def test_read_rss_mb_returns_positive() -> None:
    rss = _read_rss_mb()
    assert rss > 0


class TestRunContextLifecycle:
    def test_creates_run_directory(self, tmp_path: Path) -> None:
        ctx = RunContext("test-builder", tmp_path, total_steps=2)
        runs_root = tmp_path / ".runs"
        assert runs_root.exists()
        assert (runs_root / ctx.run_id).is_dir()
        ctx.finish()

    def test_run_id_format(self, tmp_path: Path) -> None:
        ctx = RunContext("my-builder", tmp_path, total_steps=1)
        assert ctx.run_id.startswith("my-builder-")
        parts = ctx.run_id.split("-")
        assert len(parts) >= 3
        ctx.finish()

    def test_status_json_created(self, tmp_path: Path) -> None:
        ctx = RunContext("test-builder", tmp_path, total_steps=1)
        status_path = tmp_path / ".runs" / ctx.run_id / "status.json"
        assert status_path.exists()
        data: dict[str, Any] = json.loads(status_path.read_text())
        assert data["state"] == "RUNNING"
        assert data["builder"] == "test-builder"
        ctx.finish()

    def test_events_jsonl_created(self, tmp_path: Path) -> None:
        ctx = RunContext("test-builder", tmp_path, total_steps=1)
        events_path = tmp_path / ".runs" / ctx.run_id / "events.jsonl"
        assert events_path.exists()
        lines = [json.loads(line) for line in events_path.read_text().splitlines() if line.strip()]
        assert any(e["event"] == "run_started" for e in lines)
        ctx.finish()

    def test_index_jsonl_created(self, tmp_path: Path) -> None:
        ctx = RunContext("test-builder", tmp_path, total_steps=1)
        index_path = tmp_path / ".runs" / "_index.jsonl"
        assert index_path.exists()
        lines = [json.loads(line) for line in index_path.read_text().splitlines() if line.strip()]
        assert any(e["run_id"] == ctx.run_id for e in lines)
        ctx.finish()

    def test_finish_creates_manifest(self, tmp_path: Path) -> None:
        ctx = RunContext("test-builder", tmp_path, total_steps=2)
        ctx.start_step(1, "Step one")
        ctx.start_step(2, "Step two")
        manifest = ctx.finish(outputs=["output.jsonl"])
        assert isinstance(manifest, RunManifest)
        assert manifest.state == "FINISHED"
        assert manifest.builder_name == "test-builder"
        assert "output.jsonl" in manifest.outputs

        manifest_path = tmp_path / ".runs" / ctx.run_id / "manifest.json"
        assert manifest_path.exists()

    def test_finish_updates_index_state(self, tmp_path: Path) -> None:
        ctx = RunContext("test-builder", tmp_path, total_steps=1)
        ctx.finish()
        index_path = tmp_path / ".runs" / "_index.jsonl"
        lines = [json.loads(line) for line in index_path.read_text().splitlines() if line.strip()]
        matching = [e for e in lines if e["run_id"] == ctx.run_id]
        assert matching[0]["state"] == "FINISHED"


class TestRunContextProgress:
    def test_start_step_calls_on_progress(self, tmp_path: Path) -> None:
        calls: list[tuple[int, int, str]] = []
        ctx = RunContext("test-builder", tmp_path, total_steps=3, on_progress=lambda c, t, d: calls.append((c, t, d)))
        ctx.start_step(1, "Loading data")
        assert len(calls) == 1
        assert calls[0] == (1, 3, "Loading data")
        ctx.finish()

    def test_advance_updates_items_done(self, tmp_path: Path) -> None:
        ctx = RunContext("test-builder", tmp_path, total_steps=1)
        ctx.start_step(1, "Processing", total_items=100, unit="records")
        for _ in range(50):
            ctx.advance(1)
        status_path = tmp_path / ".runs" / ctx.run_id / "status.json"
        data: dict[str, Any] = json.loads(status_path.read_text())
        sp = data.get("step_progress")
        assert sp is not None
        assert sp["items_done"] == 50
        ctx.finish()

    def test_pulse_updates_last_advance(self, tmp_path: Path) -> None:
        ctx = RunContext("test-builder", tmp_path, total_steps=1)
        ctx.start_step(1, "BFS traversal")
        ctx.pulse("Visited 1000 nodes")
        events_path = tmp_path / ".runs" / ctx.run_id / "events.jsonl"
        lines = [json.loads(line) for line in events_path.read_text().splitlines() if line.strip()]
        assert any(e["event"] == "pulse" for e in lines)
        ctx.finish()


class TestRunContextMemory:
    def test_log_memory_records_mark(self, tmp_path: Path) -> None:
        ctx = RunContext("test-builder", tmp_path, total_steps=1)
        ctx.log_memory("after loading data", structure_count=50000)
        events_path = tmp_path / ".runs" / ctx.run_id / "events.jsonl"
        lines = [json.loads(line) for line in events_path.read_text().splitlines() if line.strip()]
        marks = [e for e in lines if e["event"] == "memory_mark"]
        assert len(marks) == 1
        assert marks[0]["label"] == "after loading data"
        assert marks[0]["structure_count"] == 50000
        assert marks[0]["rss_mb"] > 0
        ctx.finish()

    def test_manifest_includes_memory_marks(self, tmp_path: Path) -> None:
        ctx = RunContext("test-builder", tmp_path, total_steps=1)
        ctx.log_memory("test mark", structure_count=100)
        manifest = ctx.finish()
        assert len(manifest.memory_marks) == 1
        assert manifest.memory_marks[0]["label"] == "test mark"


class TestRunContextCheckpoint:
    def test_save_and_load_checkpoint(self, tmp_path: Path) -> None:
        ctx = RunContext("test-builder", tmp_path, total_steps=3)
        ctx.start_step(1, "Phase 1")
        ctx.save_checkpoint("phase1_done", counts={"items": 100})

        loaded = ctx.load_checkpoint()
        assert loaded is not None
        assert loaded["phase"] == "phase1_done"
        assert loaded["counts"] == {"items": 100}
        assert loaded["schema_version"] == 1
        ctx.finish()

    def test_checkpoint_history_appended(self, tmp_path: Path) -> None:
        ctx = RunContext("test-builder", tmp_path, total_steps=3)
        ctx.save_checkpoint("phase1")
        ctx.save_checkpoint("phase2")

        history_path = tmp_path / ".runs" / ctx.run_id / "checkpoints.jsonl"
        lines = [json.loads(line) for line in history_path.read_text().splitlines() if line.strip()]
        assert len(lines) == 2
        assert lines[0]["phase"] == "phase1"
        assert lines[1]["phase"] == "phase2"
        ctx.finish()

    def test_load_checkpoint_returns_none_when_absent(self, tmp_path: Path) -> None:
        ctx = RunContext("test-builder", tmp_path, total_steps=1)
        assert ctx.load_checkpoint() is None
        ctx.finish()


class TestRunContextConcurrency:
    def test_two_runs_have_separate_dirs(self, tmp_path: Path) -> None:
        ctx1 = RunContext("builder-a", tmp_path, total_steps=1)
        time.sleep(0.01)
        ctx2 = RunContext("builder-b", tmp_path, total_steps=1)
        assert ctx1.run_id != ctx2.run_id
        assert (tmp_path / ".runs" / ctx1.run_id).is_dir()
        assert (tmp_path / ".runs" / ctx2.run_id).is_dir()
        ctx1.finish()
        ctx2.finish()


class TestRunContextStallDetection:
    def test_stall_warning_in_snapshot(self, tmp_path: Path) -> None:
        ctx = RunContext("test-builder", tmp_path, total_steps=1)
        ctx.start_step(1, "Slow step")
        # Read initial snapshot — should not have stall warning
        status_path = tmp_path / ".runs" / ctx.run_id / "status.json"
        data: dict[str, Any] = json.loads(status_path.read_text())
        assert data["stall_warning"] is False
        ctx.finish()


class TestFindResumableRun:
    def test_finds_no_runs_when_empty(self, tmp_path: Path) -> None:
        result = RunContext.find_resumable_run("builder-x", tmp_path)
        assert result is None

    def test_skips_finished_runs(self, tmp_path: Path) -> None:
        ctx = RunContext("test-builder", tmp_path, total_steps=1)
        ctx.finish()
        result = RunContext.find_resumable_run("test-builder", tmp_path)
        assert result is None

    def test_finds_aborted_run(self, tmp_path: Path) -> None:
        ctx = RunContext("test-builder", tmp_path, total_steps=1)
        run_id = ctx.run_id
        ctx._finished.set()  # Stop heartbeat but don't write manifest

        # Simulate PID no longer exists by changing the run_id's PID to a non-existent one
        index_path = tmp_path / ".runs" / "_index.jsonl"
        lines = index_path.read_text().splitlines()
        updated: list[str] = []
        for line in lines:
            entry = json.loads(line)
            if entry.get("run_id") == run_id:
                # Replace PID with a non-existent one
                fake_id = run_id.rsplit("-", 1)[0] + "-999999"
                entry["run_id"] = fake_id
                # Rename directory
                old_dir = tmp_path / ".runs" / run_id
                new_dir = tmp_path / ".runs" / fake_id
                old_dir.rename(new_dir)
            updated.append(json.dumps(entry))
        index_path.write_text("\n".join(updated) + "\n")

        result = RunContext.find_resumable_run("test-builder", tmp_path)
        assert result is not None
        assert result.endswith("-999999")


class TestResumeRun:
    def test_resume_continues_same_run(self, tmp_path: Path) -> None:
        ctx1 = RunContext("test-builder", tmp_path, total_steps=3)
        ctx1.save_checkpoint("phase1_done", counts={"items": 50})
        run_id = ctx1.run_id
        ctx1._finished.set()  # Simulate crash (stop heartbeat without manifest)

        ctx2 = RunContext.resume(run_id, tmp_path, total_steps=3)
        assert ctx2.run_id == run_id

        loaded = ctx2.load_checkpoint()
        assert loaded is not None
        assert loaded["phase"] == "phase1_done"
        ctx2.finish()

        events_path = tmp_path / ".runs" / run_id / "events.jsonl"
        lines = [json.loads(line) for line in events_path.read_text().splitlines() if line.strip()]
        assert any(e["event"] == "run_resumed" for e in lines)
