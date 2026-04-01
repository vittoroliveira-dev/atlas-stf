"""External data and serving subparsers: evidence, datajud, cgu, tse, cvm, rfb, serving, api."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from . import (
    DEFAULT_ANALYTICS_DIR,
    DEFAULT_CURATED_DIR,
    DEFAULT_DATABASE_ENV,
)
from ._parsers_portal import _add_portal_parsers
from ._parsers_sources import _add_source_parsers


def _add_external_parsers(subparsers: Any) -> None:
    evidence = subparsers.add_parser("evidence", help="Build evidence-layer artifacts for alerts")
    evidence_sub = evidence.add_subparsers(dest="evidence_target", required=True)
    evidence_build = evidence_sub.add_parser("build", help="Build evidence bundle for a single alert")
    evidence_build.add_argument("--alert-id", required=True, help="Alert identifier")
    evidence_build.add_argument(
        "--alert-path",
        type=Path,
        default=Path("data/analytics/outlier_alert.jsonl"),
        help="Outlier alert JSONL path",
    )
    evidence_build.add_argument(
        "--baseline-path",
        type=Path,
        default=Path("data/analytics/baseline.jsonl"),
        help="Baseline JSONL path",
    )
    evidence_build.add_argument(
        "--comparison-group-path",
        type=Path,
        default=Path("data/analytics/comparison_group.jsonl"),
        help="Comparison group JSONL path",
    )
    evidence_build.add_argument(
        "--decision-event-path",
        type=Path,
        default=Path("data/curated/decision_event.jsonl"),
        help="Decision-event JSONL path",
    )
    evidence_build.add_argument(
        "--process-path",
        type=Path,
        default=Path("data/curated/process.jsonl"),
        help="Process JSONL path",
    )
    evidence_build.add_argument(
        "--evidence-dir",
        type=Path,
        default=Path("data/evidence"),
        help="Output directory for evidence JSON bundles",
    )
    evidence_build.add_argument(
        "--report-dir",
        type=Path,
        default=Path("reports/anomaly-reports"),
        help="Output directory for evidence markdown reports",
    )

    evidence_build_all = evidence_sub.add_parser("build-all", help="Build evidence bundles for all alerts")
    evidence_build_all.add_argument(
        "--alert-path",
        type=Path,
        default=Path("data/analytics/outlier_alert.jsonl"),
        help="Outlier alert JSONL path",
    )
    evidence_build_all.add_argument(
        "--baseline-path",
        type=Path,
        default=Path("data/analytics/baseline.jsonl"),
        help="Baseline JSONL path",
    )
    evidence_build_all.add_argument(
        "--comparison-group-path",
        type=Path,
        default=Path("data/analytics/comparison_group.jsonl"),
        help="Comparison group JSONL path",
    )
    evidence_build_all.add_argument(
        "--decision-event-path",
        type=Path,
        default=Path("data/curated/decision_event.jsonl"),
        help="Decision-event JSONL path",
    )
    evidence_build_all.add_argument(
        "--process-path",
        type=Path,
        default=Path("data/curated/process.jsonl"),
        help="Process JSONL path",
    )
    evidence_build_all.add_argument(
        "--evidence-dir",
        type=Path,
        default=Path("data/evidence"),
        help="Output directory for evidence JSON bundles",
    )
    evidence_build_all.add_argument(
        "--report-dir",
        type=Path,
        default=Path("reports/anomaly-reports"),
        help="Output directory for evidence markdown reports",
    )

    serving = subparsers.add_parser("serving", help="Build serving-layer database artifacts")
    serving_sub = serving.add_subparsers(dest="serving_target", required=True)
    serving_build = serving_sub.add_parser("build", help="Build serving database from materialized artifacts")
    serving_build.add_argument(
        "--database-url",
        default=None,
        help=f"Serving database URL. Falls back to {DEFAULT_DATABASE_ENV}.",
    )
    serving_build.add_argument(
        "--curated-dir",
        type=Path,
        default=DEFAULT_CURATED_DIR,
        help="Curated JSONL directory",
    )
    serving_build.add_argument(
        "--analytics-dir",
        type=Path,
        default=DEFAULT_ANALYTICS_DIR,
        help="Analytics artifact directory",
    )
    serving_validate = serving_sub.add_parser(
        "validate-inputs", help="Validate JSONL input artifacts before serving build"
    )
    serving_validate.add_argument(
        "--curated-dir",
        type=Path,
        default=DEFAULT_CURATED_DIR,
        help="Curated JSONL directory",
    )
    serving_validate.add_argument(
        "--analytics-dir",
        type=Path,
        default=DEFAULT_ANALYTICS_DIR,
        help="Analytics artifact directory",
    )
    serving_validate.add_argument(
        "--report-path",
        type=Path,
        default=Path("data/serving/validation_report.json"),
        help="Output path for validation report JSON",
    )

    _add_source_parsers(subparsers)
    _add_portal_parsers(subparsers)

    api = subparsers.add_parser("api", help="Serve the HTTP API over the serving database")
    api_sub = api.add_subparsers(dest="api_target", required=True)
    api_serve = api_sub.add_parser("serve", help="Start the FastAPI application with Uvicorn")
    api_serve.add_argument(
        "--database-url",
        default=None,
        help=f"Serving database URL. Falls back to {DEFAULT_DATABASE_ENV}.",
    )
    api_serve.add_argument("--host", default="127.0.0.1", help="Bind host")
    api_serve.add_argument("--port", type=int, default=8000, help="Bind port")
    api_serve.add_argument("--reload", action="store_true", help="Enable Uvicorn reload mode")
