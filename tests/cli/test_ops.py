"""Tests for CLI operational handlers (runs, status, explain-run, tail-run, resume)."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from atlas_stf.cli._handlers_ops import (
    _elapsed_since,
    _format_bytes_gb,
    _format_duration,
    _handle_explain_run,
    _handle_resume,
    _handle_runs,
    _handle_status,
    _handle_tail_run,
    _is_pid_alive,
    _read_index,
    _read_json_safe,
    _validate_run_id,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _ns(analytics_dir: Path, **kwargs) -> argparse.Namespace:
    defaults: dict = {"analytics_dir": analytics_dir, "builder": None, "limit": 20}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def _write_index(analytics_dir: Path, entries: list[dict]) -> None:
    index_path = analytics_dir / ".runs" / "_index.jsonl"
    index_path.parent.mkdir(parents=True, exist_ok=True)
    index_path.write_text("".join(json.dumps(e) + "\n" for e in entries), encoding="utf-8")


def _write_run_file(analytics_dir: Path, run_id: str, filename: str, data: dict) -> None:
    run_dir = analytics_dir / ".runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / filename).write_text(json.dumps(data), encoding="utf-8")


# ---------------------------------------------------------------------------
# _format_duration
# ---------------------------------------------------------------------------


def test_format_duration_zero():
    assert _format_duration(0) == "0s"


def test_format_duration_under_minute():
    assert _format_duration(30) == "30s"


def test_format_duration_minutes_and_seconds():
    assert _format_duration(90) == "1m30s"


def test_format_duration_hours_and_minutes():
    result = _format_duration(3661)
    # 3661s = 1h 1m 1s  → "1h01m"
    assert result == "1h01m"


def test_format_duration_exact_hour():
    result = _format_duration(3600)
    assert result == "1h00m"


# ---------------------------------------------------------------------------
# _format_bytes_gb
# ---------------------------------------------------------------------------


def test_format_bytes_gb_zero_returns_dash():
    assert _format_bytes_gb(0.0) == "-"


def test_format_bytes_gb_one_gb():
    assert _format_bytes_gb(1024.0) == "1.0 GB"


def test_format_bytes_gb_non_zero():
    result = _format_bytes_gb(512.0)
    assert "0.5 GB" == result


# ---------------------------------------------------------------------------
# _is_pid_alive
# ---------------------------------------------------------------------------


def test_is_pid_alive_process_not_found():
    with patch("os.kill", side_effect=ProcessLookupError):
        assert _is_pid_alive(99999) is False


def test_is_pid_alive_permission_error_treated_as_alive():
    with patch("os.kill", side_effect=PermissionError):
        assert _is_pid_alive(1) is True


def test_is_pid_alive_success():
    with patch("os.kill", return_value=None):
        assert _is_pid_alive(12345) is True


# ---------------------------------------------------------------------------
# _read_json_safe
# ---------------------------------------------------------------------------


def test_read_json_safe_valid_file(tmp_path: Path):
    p = tmp_path / "data.json"
    p.write_text('{"key": "value"}', encoding="utf-8")
    result = _read_json_safe(p)
    assert result == {"key": "value"}


def test_read_json_safe_invalid_json(tmp_path: Path):
    p = tmp_path / "bad.json"
    p.write_text("not-json", encoding="utf-8")
    assert _read_json_safe(p) is None


def test_read_json_safe_missing_file(tmp_path: Path):
    assert _read_json_safe(tmp_path / "nonexistent.json") is None


def test_read_json_safe_non_dict_returns_none(tmp_path: Path):
    p = tmp_path / "list.json"
    p.write_text("[1, 2, 3]", encoding="utf-8")
    assert _read_json_safe(p) is None


# ---------------------------------------------------------------------------
# _read_index
# ---------------------------------------------------------------------------


def test_read_index_empty_file(tmp_path: Path):
    index_path = tmp_path / ".runs" / "_index.jsonl"
    index_path.parent.mkdir(parents=True, exist_ok=True)
    index_path.write_text("", encoding="utf-8")
    assert _read_index(tmp_path) == []


def test_read_index_valid_entries(tmp_path: Path):
    entries = [
        {"run_id": "builder-20260301-1111", "builder": "builder", "started_at": "2026-03-01T00:00:00"},
        {"run_id": "builder-20260302-2222", "builder": "builder", "started_at": "2026-03-02T00:00:00"},
    ]
    _write_index(tmp_path, entries)
    result = _read_index(tmp_path)
    assert len(result) == 2
    assert result[0]["run_id"] == "builder-20260301-1111"


def test_read_index_malformed_lines_are_skipped(tmp_path: Path):
    index_path = tmp_path / ".runs" / "_index.jsonl"
    index_path.parent.mkdir(parents=True, exist_ok=True)
    index_path.write_text(
        '{"run_id": "good-run-1234", "builder": "foo"}\nnot-json\n{"run_id": "good-run-5678", "builder": "foo"}\n',
        encoding="utf-8",
    )
    result = _read_index(tmp_path)
    assert len(result) == 2


def test_read_index_missing_file(tmp_path: Path):
    assert _read_index(tmp_path) == []


# ---------------------------------------------------------------------------
# _elapsed_since
# ---------------------------------------------------------------------------


def test_elapsed_since_valid_iso_contains_ago():
    # Use a timestamp clearly in the past
    result = _elapsed_since("2020-01-01T00:00:00+00:00")
    assert "ago" in result


def test_elapsed_since_invalid_returns_question_mark():
    assert _elapsed_since("not-a-date") == "?"


def test_elapsed_since_empty_returns_question_mark():
    assert _elapsed_since("") == "?"


# ---------------------------------------------------------------------------
# _handle_runs
# ---------------------------------------------------------------------------


def test_handle_runs_empty_index_prints_header(tmp_path: Path, capsys):
    code = _handle_runs(_ns(tmp_path))
    out = capsys.readouterr().out
    assert code == 0
    assert "RUN ID" in out
    assert "STATE" in out


def test_handle_runs_with_builder_filter(tmp_path: Path, capsys):
    entries = [
        {"run_id": "alpha-20260301-1111", "builder": "alpha", "started_at": "2026-03-01T00:00:00"},
        {"run_id": "beta-20260302-2222", "builder": "beta", "started_at": "2026-03-02T00:00:00"},
    ]
    _write_index(tmp_path, entries)
    code = _handle_runs(_ns(tmp_path, builder="alpha"))
    out = capsys.readouterr().out
    assert code == 0
    assert "alpha-20260301-1111" in out
    assert "beta-20260302-2222" not in out


def test_handle_runs_respects_limit(tmp_path: Path, capsys):
    entries = [
        {"run_id": f"run-2026030{i}-{1000 + i}", "builder": "b", "started_at": f"2026-03-0{i}T00:00:00"}
        for i in range(1, 6)
    ]
    _write_index(tmp_path, entries)
    code = _handle_runs(_ns(tmp_path, limit=2))
    out = capsys.readouterr().out
    assert code == 0
    # Only 2 run IDs should appear (plus header lines)
    run_lines = [ln for ln in out.splitlines() if "run-2026" in ln]
    assert len(run_lines) == 2


def test_handle_runs_manifest_takes_priority_over_status(tmp_path: Path, capsys):
    run_id = "mybuilder-20260310-9999"
    entries = [{"run_id": run_id, "builder": "mybuilder", "started_at": "2026-03-10T10:00:00"}]
    _write_index(tmp_path, entries)
    _write_run_file(
        tmp_path,
        run_id,
        "status.json",
        {
            "state": "RUNNING",
            "rss_peak_mb": 512.0,
            "step_progress": {"step": 1, "items_done": 50, "items_total": 100},
        },
    )
    _write_run_file(
        tmp_path,
        run_id,
        "manifest.json",
        {
            "state": "FINISHED",
            "rss_peak_mb": 1024.0,
            "step_durations": {"1": 60.0, "2": 30.0},
        },
    )
    code = _handle_runs(_ns(tmp_path))
    out = capsys.readouterr().out
    assert code == 0
    # Manifest state should appear, not status state
    assert "FINISHED" in out
    assert "RUNNING" not in out


def test_handle_runs_status_used_when_no_manifest(tmp_path: Path, capsys):
    run_id = "mybuilder-20260311-8888"
    entries = [{"run_id": run_id, "builder": "mybuilder", "started_at": "2026-03-11T10:00:00"}]
    _write_index(tmp_path, entries)
    _write_run_file(
        tmp_path,
        run_id,
        "status.json",
        {
            "state": "RUNNING",
            "rss_peak_mb": 256.0,
            "step_progress": {"step": 2, "items_done": 40, "items_total": 100},
        },
    )
    code = _handle_runs(_ns(tmp_path))
    out = capsys.readouterr().out
    assert code == 0
    assert "RUNNING" in out


def test_handle_runs_fallback_to_index_data_when_no_files(tmp_path: Path, capsys):
    entries = [
        {"run_id": "nofiles-20260312-7777", "builder": "b", "state": "UNKNOWN", "started_at": "2026-03-12T00:00:00"}
    ]
    _write_index(tmp_path, entries)
    code = _handle_runs(_ns(tmp_path))
    out = capsys.readouterr().out
    assert code == 0
    assert "nofiles-20260312-7777" in out


def test_handle_runs_malformed_json_in_index_does_not_crash(tmp_path: Path, capsys):
    index_path = tmp_path / ".runs" / "_index.jsonl"
    index_path.parent.mkdir(parents=True, exist_ok=True)
    index_path.write_text(
        '{"run_id": "good-20260301-1234", "builder": "b", "started_at": "2026-03-01T00:00:00"}\nCORRUPTED LINE\n',
        encoding="utf-8",
    )
    code = _handle_runs(_ns(tmp_path))
    assert code == 0


# ---------------------------------------------------------------------------
# _handle_status
# ---------------------------------------------------------------------------


def test_handle_status_no_runs_prints_none_placeholders(tmp_path: Path, capsys):
    code = _handle_status(_ns(tmp_path))
    out = capsys.readouterr().out
    assert code == 0
    assert "ACTIVE" in out
    assert "(none)" in out
    assert "LAST FINISHED" in out


def test_handle_status_pid_alive_appears_as_active(tmp_path: Path, capsys):
    run_id = "compound-risk-20260315-1234"
    entries = [{"run_id": run_id, "builder": "compound-risk", "started_at": "2026-03-15T08:00:00"}]
    _write_index(tmp_path, entries)
    _write_run_file(
        tmp_path,
        run_id,
        "status.json",
        {
            "run_id": run_id,
            "state": "RUNNING",
            "rss_peak_mb": 512.0,
            "last_heartbeat": "2026-03-15T08:05:00+00:00",
        },
    )
    with patch("os.kill", return_value=None) as mock_kill:
        code = _handle_status(_ns(tmp_path))
    out = capsys.readouterr().out
    assert code == 0
    assert run_id in out
    assert "ACTIVE" in out
    mock_kill.assert_called_once_with(1234, 0)


def test_handle_status_pid_dead_does_not_appear_as_active(tmp_path: Path, capsys):
    run_id = "compound-risk-20260315-5678"
    entries = [{"run_id": run_id, "builder": "compound-risk", "started_at": "2026-03-15T09:00:00"}]
    _write_index(tmp_path, entries)
    _write_run_file(
        tmp_path,
        run_id,
        "status.json",
        {
            "run_id": run_id,
            "state": "RUNNING",
            "rss_peak_mb": 512.0,
            "last_heartbeat": "2026-03-15T09:05:00+00:00",
        },
    )
    with patch("os.kill", side_effect=ProcessLookupError) as mock_kill:
        code = _handle_status(_ns(tmp_path))
    out = capsys.readouterr().out
    assert code == 0
    # PID is dead → run ID should NOT appear; the "ACTIVE RUNS: (none)" line is printed
    assert run_id not in out
    assert "(none)" in out
    mock_kill.assert_called_once_with(5678, 0)


def test_handle_status_permission_error_treated_as_alive(tmp_path: Path, capsys):
    run_id = "compound-risk-20260316-9012"
    entries = [{"run_id": run_id, "builder": "compound-risk", "started_at": "2026-03-16T10:00:00"}]
    _write_index(tmp_path, entries)
    _write_run_file(
        tmp_path,
        run_id,
        "status.json",
        {
            "run_id": run_id,
            "state": "RUNNING",
            "rss_peak_mb": 200.0,
            "last_heartbeat": "2026-03-16T10:01:00+00:00",
        },
    )
    with patch("os.kill", side_effect=PermissionError) as mock_kill:
        code = _handle_status(_ns(tmp_path))
    out = capsys.readouterr().out
    assert code == 0
    assert "ACTIVE RUNS:" in out
    assert run_id in out
    mock_kill.assert_called_once_with(9012, 0)


def test_handle_status_manifest_goes_to_finished_skips_pid(tmp_path: Path, capsys):
    run_id = "sanction-match-20260317-3456"
    entries = [{"run_id": run_id, "builder": "sanction-match", "started_at": "2026-03-17T07:00:00"}]
    _write_index(tmp_path, entries)
    _write_run_file(
        tmp_path,
        run_id,
        "manifest.json",
        {
            "run_id": run_id,
            "state": "FINISHED",
            "builder_name": "sanction-match",
            "started_at": "2026-03-17T07:00:00",
            "finished_at": "2026-03-17T08:00:00",
            "total_wall_time_s": 3600.0,
            "rss_peak_mb": 800.0,
        },
    )
    # os.kill should NOT be called because manifest is present
    with patch("os.kill") as mock_kill:
        code = _handle_status(_ns(tmp_path))
    out = capsys.readouterr().out
    assert code == 0
    mock_kill.assert_not_called()
    assert "LAST FINISHED" in out
    assert run_id in out


# ---------------------------------------------------------------------------
# _validate_run_id
# ---------------------------------------------------------------------------


def test_validate_run_id_normal(tmp_path: Path):
    runs_root = tmp_path / ".runs"
    runs_root.mkdir()
    assert _validate_run_id("compound-risk-20260315-1234", runs_root) is True


def test_validate_run_id_rejects_path_traversal(tmp_path: Path):
    runs_root = tmp_path / ".runs"
    runs_root.mkdir()
    assert _validate_run_id("../../etc/passwd", runs_root) is False
    assert _validate_run_id("../sibling", runs_root) is False


def test_validate_run_id_rejects_absolute_path(tmp_path: Path):
    runs_root = tmp_path / ".runs"
    runs_root.mkdir()
    assert _validate_run_id("/etc/passwd", runs_root) is False


def test_validate_run_id_rejects_internal_subpath(tmp_path: Path):
    runs_root = tmp_path / ".runs"
    runs_root.mkdir()
    assert _validate_run_id("foo/bar", runs_root) is False


def test_validate_run_id_rejects_empty(tmp_path: Path):
    runs_root = tmp_path / ".runs"
    runs_root.mkdir()
    assert _validate_run_id("", runs_root) is False


def test_handle_explain_run_rejects_traversal(tmp_path: Path, capsys):
    code = _handle_explain_run(_ns(tmp_path, run_id="../../etc"))
    err = capsys.readouterr().err
    assert code == 1
    assert "Invalid run ID" in err


def test_handle_tail_run_rejects_traversal(tmp_path: Path, capsys):
    code = _handle_tail_run(_ns(tmp_path, run_id="../../../etc"))
    err = capsys.readouterr().err
    assert code == 1
    assert "Invalid run ID" in err


# ---------------------------------------------------------------------------
# _handle_explain_run
# ---------------------------------------------------------------------------


def test_handle_explain_run_with_manifest_prints_complete_fields(tmp_path: Path, capsys):
    run_id = "donation-match-20260310-1111"
    _write_run_file(
        tmp_path,
        run_id,
        "manifest.json",
        {
            "run_id": run_id,
            "builder_name": "donation-match",
            "state": "FINISHED",
            "started_at": "2026-03-10T06:00:00",
            "finished_at": "2026-03-10T07:00:00",
            "total_wall_time_s": 3600.0,
            "rss_peak_mb": 512.0,
            "session_count": 1,
            "resume_count": 0,
            "total_items_processed": 50000,
            "step_durations": {"1": 120.0, "2": 60.0},
            "outputs": ["donation_match.jsonl"],
            "memory_marks": [{"label": "after_load", "rss_mb": 400.0, "structure_count": 50000}],
        },
    )
    code = _handle_explain_run(_ns(tmp_path, run_id=run_id))
    out = capsys.readouterr().out
    assert code == 0
    assert run_id in out
    assert "donation-match" in out
    assert "FINISHED" in out
    assert "50,000" in out
    assert "after_load" in out
    assert "donation_match.jsonl" in out


def test_handle_explain_run_with_only_status_prints_partial_fields(tmp_path: Path, capsys):
    run_id = "sanction-match-20260311-2222"
    _write_run_file(
        tmp_path,
        run_id,
        "status.json",
        {
            "run_id": run_id,
            "state": "RUNNING",
            "builder": "sanction-match",
            "started_at": "2026-03-11T10:00:00",
            "rss_mb": 300.0,
            "rss_peak_mb": 350.0,
        },
    )
    code = _handle_explain_run(_ns(tmp_path, run_id=run_id))
    out = capsys.readouterr().out
    assert code == 0
    assert run_id in out
    assert "RUNNING" in out
    assert "no manifest" in out


def test_handle_explain_run_nonexistent_run_returns_1(tmp_path: Path, capsys):
    code = _handle_explain_run(_ns(tmp_path, run_id="ghost-run-99999-0000"))
    err = capsys.readouterr().err
    assert code == 1
    assert "No data found" in err


def test_handle_explain_run_optional_fields_absent_no_crash(tmp_path: Path, capsys):
    run_id = "minimal-20260312-3333"
    _write_run_file(
        tmp_path,
        run_id,
        "manifest.json",
        {
            "run_id": run_id,
            "builder_name": "minimal",
            "state": "FINISHED",
            "started_at": "2026-03-12T00:00:00",
            "finished_at": "2026-03-12T01:00:00",
            "total_wall_time_s": 3600.0,
            "rss_peak_mb": 0.0,
            "session_count": 1,
            "total_items_processed": 0,
            # resume_count, step_durations, outputs, memory_marks are absent
        },
    )
    code = _handle_explain_run(_ns(tmp_path, run_id=run_id))
    assert code == 0


# ---------------------------------------------------------------------------
# _handle_tail_run
# ---------------------------------------------------------------------------


def test_handle_tail_run_file_not_found_returns_1(tmp_path: Path, capsys):
    code = _handle_tail_run(_ns(tmp_path, run_id="ghost-20260301-0000"))
    err = capsys.readouterr().err
    assert code == 1
    assert "events.jsonl not found" in err


def test_handle_tail_run_keyboard_interrupt_returns_0(tmp_path: Path):
    run_id = "myrun-20260301-4444"
    events_path = tmp_path / ".runs" / run_id / "events.jsonl"
    events_path.parent.mkdir(parents=True, exist_ok=True)
    events_path.write_text('{"event": "start"}\n', encoding="utf-8")

    with patch("subprocess.run", side_effect=KeyboardInterrupt):
        code = _handle_tail_run(_ns(tmp_path, run_id=run_id))
    assert code == 0


# ---------------------------------------------------------------------------
# _handle_resume
# ---------------------------------------------------------------------------


def _make_parser() -> argparse.ArgumentParser:
    """Return a minimal parser whose error() raises SystemExit."""
    return argparse.ArgumentParser()


def test_handle_resume_no_run_id_no_builder_raises_system_exit(tmp_path: Path):
    parser = _make_parser()
    args = _ns(tmp_path, run_id=None, builder=None)
    with pytest.raises(SystemExit):
        _handle_resume(parser, args)


def test_handle_resume_builder_no_candidates_returns_1(tmp_path: Path, capsys):
    # Index exists but no entries for this builder
    _write_index(
        tmp_path,
        [
            {"run_id": "other-20260301-1111", "builder": "other", "started_at": "2026-03-01T00:00:00"},
        ],
    )
    parser = _make_parser()
    code = _handle_resume(parser, _ns(tmp_path, run_id=None, builder="missing-builder"))
    assert code == 1


def test_handle_resume_builder_picks_most_recent_by_started_at(tmp_path: Path, capsys):
    # 3 candidates with distinct started_at; most recent should be picked (PID dead, no manifest)
    entries = [
        {"run_id": "compound-risk-20260301-1111", "builder": "compound-risk", "started_at": "2026-03-01T00:00:00"},
        {"run_id": "compound-risk-20260302-2222", "builder": "compound-risk", "started_at": "2026-03-02T00:00:00"},
        {"run_id": "compound-risk-20260303-3333", "builder": "compound-risk", "started_at": "2026-03-03T00:00:00"},
    ]
    _write_index(tmp_path, entries)
    # Write status.json for all (no manifest → not finished)
    for e in entries:
        _write_run_file(tmp_path, e["run_id"], "status.json", {"state": "ABORTED"})

    with patch("os.kill", side_effect=ProcessLookupError):
        code = _handle_resume(_make_parser(), _ns(tmp_path, run_id=None, builder="compound-risk"))
    out = capsys.readouterr().out
    assert code == 0
    # Most recent should be selected
    assert "compound-risk-20260303-3333" in out


def test_handle_resume_skips_candidate_with_manifest(tmp_path: Path, capsys):
    # Two candidates: most recent has manifest (finished), older does not
    entries = [
        {"run_id": "compound-risk-20260301-1111", "builder": "compound-risk", "started_at": "2026-03-01T00:00:00"},
        {"run_id": "compound-risk-20260302-2222", "builder": "compound-risk", "started_at": "2026-03-02T00:00:00"},
    ]
    _write_index(tmp_path, entries)
    _write_run_file(tmp_path, "compound-risk-20260302-2222", "manifest.json", {"state": "FINISHED"})
    _write_run_file(tmp_path, "compound-risk-20260301-1111", "status.json", {"state": "ABORTED"})

    with patch("os.kill", side_effect=ProcessLookupError):
        code = _handle_resume(_make_parser(), _ns(tmp_path, run_id=None, builder="compound-risk"))
    out = capsys.readouterr().out
    assert code == 0
    # Older run should be selected, not the finished one
    assert "compound-risk-20260301-1111" in out


def test_handle_resume_skips_candidate_with_pid_alive(tmp_path: Path, capsys):
    # Two candidates: most recent has alive PID, older has dead PID
    entries = [
        {"run_id": "compound-risk-20260301-1111", "builder": "compound-risk", "started_at": "2026-03-01T00:00:00"},
        {"run_id": "compound-risk-20260302-9999", "builder": "compound-risk", "started_at": "2026-03-02T00:00:00"},
    ]
    _write_index(tmp_path, entries)
    for e in entries:
        _write_run_file(tmp_path, e["run_id"], "status.json", {"state": "RUNNING"})

    def fake_kill(pid: int, sig: int) -> None:
        if pid == 9999:
            return  # alive
        raise ProcessLookupError

    with patch("os.kill", side_effect=fake_kill):
        code = _handle_resume(_make_parser(), _ns(tmp_path, run_id=None, builder="compound-risk"))
    out = capsys.readouterr().out
    assert code == 0
    # 9999 is alive → skip; 1111 is dead → selected
    assert "compound-risk-20260301-1111" in out


def test_handle_resume_with_explicit_run_id_succeeds(tmp_path: Path, capsys):
    # When run_id is directly provided, no index traversal occurs
    code = _handle_resume(
        _make_parser(),
        _ns(tmp_path, run_id="compound-risk-20260310-7777", builder=None),
    )
    out = capsys.readouterr().out
    assert code == 0
    assert "compound-risk-20260310-7777" in out
