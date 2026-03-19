"""Operational subparsers: runs, status, explain-run, tail-run, resume."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from . import DEFAULT_ANALYTICS_DIR


def _add_ops_parsers(subparsers: Any) -> None:
    runs_parser = subparsers.add_parser("runs", help="List recent builder runs")
    runs_parser.add_argument("--builder", default=None, help="Filter by builder name")
    runs_parser.add_argument("--limit", type=int, default=20, help="Max runs to show (default: 20)")
    runs_parser.add_argument(
        "--analytics-dir", type=Path, default=DEFAULT_ANALYTICS_DIR, help="Analytics artifact directory"
    )

    status_parser = subparsers.add_parser("status", help="Show active builder runs")
    status_parser.add_argument("--builder", default=None, help="Filter by builder name")
    status_parser.add_argument(
        "--analytics-dir", type=Path, default=DEFAULT_ANALYTICS_DIR, help="Analytics artifact directory"
    )

    explain_parser = subparsers.add_parser("explain-run", help="Show run manifest (durations, peak RSS, outputs)")
    explain_parser.add_argument("run_id", help="Run ID to explain")
    explain_parser.add_argument(
        "--analytics-dir", type=Path, default=DEFAULT_ANALYTICS_DIR, help="Analytics artifact directory"
    )

    tail_parser = subparsers.add_parser("tail-run", help="Follow run events in real time")
    tail_parser.add_argument("run_id", help="Run ID to tail")
    tail_parser.add_argument(
        "--analytics-dir", type=Path, default=DEFAULT_ANALYTICS_DIR, help="Analytics artifact directory"
    )

    resume_parser = subparsers.add_parser("resume", help="Resume a failed/aborted builder run from checkpoint")
    resume_parser.add_argument("--run-id", default=None, help="Specific run ID to resume")
    resume_parser.add_argument("--builder", default=None, help="Resume latest failed run for this builder")
    resume_parser.add_argument(
        "--analytics-dir", type=Path, default=DEFAULT_ANALYTICS_DIR, help="Analytics artifact directory"
    )
