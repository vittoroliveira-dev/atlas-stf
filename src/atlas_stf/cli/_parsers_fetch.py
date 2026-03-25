"""CLI parsers for unified fetch manifest commands."""

from __future__ import annotations

import argparse
from pathlib import Path


def _add_fetch_parsers(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    fetch_parser = subparsers.add_parser("fetch", help="Unified fetch manifest operations")
    fetch_sub = fetch_parser.add_subparsers(dest="fetch_target", required=True)

    # --- fetch plan ---
    plan_p = fetch_sub.add_parser("plan", help="Generate a fetch plan (read-only)")
    plan_p.add_argument("--sources", nargs="*", help="Restrict to specific sources")
    plan_p.add_argument("--force-refresh", action="store_true", help="Ignore freshness, re-probe all")
    plan_p.add_argument("--output-dir", type=Path, default=Path("data/raw"), help="Base data directory")
    plan_p.add_argument("--json", action="store_true", dest="json_output", help="Output plan as JSON")
    plan_p.add_argument(
        "--process-path", type=Path, default=Path("data/curated/process.jsonl"),
        help="Process JSONL for DataJud discovery",
    )

    # --- fetch status ---
    status_p = fetch_sub.add_parser("status", help="Show manifest status")
    status_p.add_argument("--sources", nargs="*", help="Restrict to specific sources")
    status_p.add_argument("--output-dir", type=Path, default=Path("data/raw"), help="Base data directory")
    status_p.add_argument("--json", action="store_true", dest="json_output", help="Output as JSON")

    # --- fetch run ---
    run_p = fetch_sub.add_parser("run", help="Execute a fetch plan (plan inline + execute)")
    run_p.add_argument("--sources", nargs="*", help="Restrict to specific sources")
    run_p.add_argument("--force-refresh", action="store_true", help="Ignore freshness, re-probe all")
    run_p.add_argument("--output-dir", type=Path, default=Path("data/raw"), help="Base data directory")
    run_p.add_argument(
        "--plan", type=Path, default=None,
        help="Pre-generated plan JSON (fail-closed for non-deferred sources)",
    )
    run_p.add_argument("--api-key", default="", help="DataJud API key (or set DATAJUD_API_KEY)")
    run_p.add_argument(
        "--process-path", type=Path, default=Path("data/curated/process.jsonl"),
        help="Process JSONL for DataJud discovery",
    )

    # --- fetch migrate ---
    migrate_p = fetch_sub.add_parser("migrate", help="Migrate legacy checkpoints to manifests")
    migrate_p.add_argument("--sources", nargs="*", help="Restrict to specific sources")
    migrate_p.add_argument("--output-dir", type=Path, default=Path("data/raw"), help="Base data directory")
    migrate_p.add_argument("--dry-run", action="store_true", help="Show what would be migrated")
