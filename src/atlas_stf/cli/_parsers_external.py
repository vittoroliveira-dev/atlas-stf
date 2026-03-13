"""External data and serving subparsers: evidence, datajud, cgu, tse, cvm, rfb, serving, api."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from . import (
    DEFAULT_ANALYTICS_DIR,
    DEFAULT_CURATED_DIR,
    DEFAULT_DATABASE_ENV,
)


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

    datajud = subparsers.add_parser("datajud", help="Fetch and process CNJ DataJud data")
    datajud_sub = datajud.add_subparsers(dest="datajud_target", required=True)
    datajud_fetch = datajud_sub.add_parser("fetch", help="Fetch aggregated data from DataJud API")
    datajud_fetch.add_argument("--api-key", default=None, help="DataJud API key (or set DATAJUD_API_KEY)")
    datajud_fetch.add_argument(
        "--process-path",
        type=Path,
        default=Path("data/curated/process.jsonl"),
        help="Curated process JSONL path",
    )
    datajud_fetch.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/raw/datajud"),
        help="Output directory for raw DataJud JSON files",
    )
    datajud_fetch.add_argument("--dry-run", action="store_true", help="List indices without querying")
    datajud_build = datajud_sub.add_parser("build-context", help="Build origin context analytics")
    datajud_build.add_argument(
        "--datajud-dir",
        type=Path,
        default=Path("data/raw/datajud"),
        help="Raw DataJud JSON directory",
    )
    datajud_build.add_argument(
        "--process-path",
        type=Path,
        default=Path("data/curated/process.jsonl"),
        help="Curated process JSONL path",
    )
    datajud_build.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_ANALYTICS_DIR,
        help="Output directory for analytics artifacts",
    )

    cgu = subparsers.add_parser("cgu", help="Fetch and process CGU CEIS/CNEP sanction data")
    cgu_sub = cgu.add_subparsers(dest="cgu_target", required=True)
    cgu_fetch = cgu_sub.add_parser("fetch", help="Fetch CEIS/CNEP data from Portal da Transparencia")
    cgu_fetch.add_argument("--api-key", default=None, help="CGU API key (or set CGU_API_KEY)")
    cgu_fetch.add_argument(
        "--party-path",
        type=Path,
        default=Path("data/curated/party.jsonl"),
        help="Curated party JSONL path",
    )
    cgu_fetch.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/raw/cgu"),
        help="Output directory for raw CGU data",
    )
    cgu_fetch.add_argument("--dry-run", action="store_true", help="List names without querying")
    cgu_build = cgu_sub.add_parser("build-matches", help="Build sanction match analytics")
    cgu_build.add_argument(
        "--cgu-dir",
        type=Path,
        default=Path("data/raw/cgu"),
        help="Raw CGU data directory",
    )
    cgu_build.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_ANALYTICS_DIR,
        help="Output directory for analytics artifacts",
    )

    tse = subparsers.add_parser("tse", help="Fetch and process TSE campaign donation data")
    tse_sub = tse.add_subparsers(dest="tse_target", required=True)
    tse_fetch = tse_sub.add_parser("fetch", help="Download receitas CSVs from TSE open data")
    tse_fetch.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/raw/tse"),
        help="Output directory for raw TSE data",
    )
    tse_fetch.add_argument("--years", nargs="*", type=int, default=None, help="Election years to fetch")
    tse_fetch.add_argument("--dry-run", action="store_true", help="List URLs without downloading")
    tse_build = tse_sub.add_parser("build-matches", help="Build donation match analytics")
    tse_build.add_argument(
        "--tse-dir",
        type=Path,
        default=Path("data/raw/tse"),
        help="Raw TSE data directory",
    )
    tse_build.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_ANALYTICS_DIR,
        help="Output directory for analytics artifacts",
    )

    cvm = subparsers.add_parser("cvm", help="Fetch and process CVM sanction data")
    cvm_sub = cvm.add_subparsers(dest="cvm_target", required=True)
    cvm_fetch = cvm_sub.add_parser("fetch", help="Download processo sancionador CSVs from CVM open data")
    cvm_fetch.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/raw/cvm"),
        help="Output directory for raw CVM data",
    )
    cvm_fetch.add_argument("--dry-run", action="store_true", help="Show URL without downloading")
    cvm_build = cvm_sub.add_parser("build-matches", help="Rebuild sanction matches (includes CVM)")
    cvm_build.add_argument(
        "--cvm-dir",
        type=Path,
        default=Path("data/raw/cvm"),
        help="Raw CVM data directory",
    )
    cvm_build.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_ANALYTICS_DIR,
        help="Output directory for analytics artifacts",
    )

    rfb = subparsers.add_parser("rfb", help="Fetch and process RFB CNPJ partner data")
    rfb_sub = rfb.add_subparsers(dest="rfb_target", required=True)
    rfb_fetch = rfb_sub.add_parser("fetch", help="Download Socios/Empresas from RFB open data")
    rfb_fetch.add_argument("--output-dir", type=Path, default=Path("data/raw/rfb"))
    rfb_fetch.add_argument("--dry-run", action="store_true")
    rfb_build = rfb_sub.add_parser("build-network", help="Build corporate network analytics")
    rfb_build.add_argument("--rfb-dir", type=Path, default=Path("data/raw/rfb"))
    rfb_build.add_argument("--output-dir", type=Path, default=DEFAULT_ANALYTICS_DIR)
    rfb_build.add_argument("--max-degree", type=int, default=3, help="Max BFS link degree (1-6)")

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
