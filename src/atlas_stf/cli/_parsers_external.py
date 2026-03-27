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
    cgu_fetch.add_argument(
        "--force-refresh",
        action="store_true",
        help="Clear checkpoint and re-download all datasets",
    )
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

    cgu_corporate_links = cgu_sub.add_parser(
        "build-corporate-links", help="Build sanction → corporate → STF links via RFB bridge"
    )
    cgu_corporate_links.add_argument("--cgu-dir", type=Path, default=Path("data/raw/cgu"))
    cgu_corporate_links.add_argument("--cvm-dir", type=Path, default=Path("data/raw/cvm"))
    cgu_corporate_links.add_argument("--rfb-dir", type=Path, default=Path("data/raw/rfb"))
    cgu_corporate_links.add_argument("--output-dir", type=Path, default=DEFAULT_ANALYTICS_DIR)

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
    tse_fetch.add_argument(
        "--force-refresh",
        action="store_true",
        help="Re-download all requested years even if already cached",
    )
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
    tse_expenses = tse_sub.add_parser("fetch-expenses", help="Download despesas CSVs from TSE open data")
    tse_expenses.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/raw/tse"),
        help="Output directory for raw TSE data",
    )
    tse_expenses.add_argument("--years", nargs="*", type=int, default=None, help="Election years to fetch")
    tse_expenses.add_argument("--dry-run", action="store_true", help="List URLs without downloading")
    tse_expenses.add_argument(
        "--force-refresh",
        action="store_true",
        help="Re-download all requested years even if already cached",
    )

    tse_party_org = tse_sub.add_parser("fetch-party-org", help="Download party organ finance CSVs from TSE open data")
    tse_party_org.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/raw/tse"),
        help="Output directory for raw TSE data",
    )
    tse_party_org.add_argument("--years", nargs="*", type=int, default=None, help="Election years to fetch (2018+)")
    tse_party_org.add_argument("--dry-run", action="store_true", help="List URLs without downloading")
    tse_party_org.add_argument(
        "--force-refresh",
        action="store_true",
        help="Re-download all requested years even if already cached",
    )

    tse_counterparties = tse_sub.add_parser("build-counterparties", help="Build payment counterparty rollup analytics")
    tse_counterparties.add_argument("--tse-dir", type=Path, default=Path("data/raw/tse"))
    tse_counterparties.add_argument("--output-dir", type=Path, default=DEFAULT_ANALYTICS_DIR)

    tse_donor_links = tse_sub.add_parser(
        "build-donor-links", help="Build donor → corporate links via CPF/CNPJ join with RFB data"
    )
    tse_donor_links.add_argument("--tse-dir", type=Path, default=Path("data/raw/tse"))
    tse_donor_links.add_argument("--rfb-dir", type=Path, default=Path("data/raw/rfb"))
    tse_donor_links.add_argument("--output-dir", type=Path, default=DEFAULT_ANALYTICS_DIR)

    tse_empirical = tse_sub.add_parser("empirical-report", help="Build donation empirical metrics report")
    tse_empirical.add_argument("--tse-dir", type=Path, default=Path("data/raw/tse"))
    tse_empirical.add_argument("--analytics-dir", type=Path, default=DEFAULT_ANALYTICS_DIR)
    tse_empirical.add_argument("--output-dir", type=Path, default=DEFAULT_ANALYTICS_DIR)

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
    cvm_fetch.add_argument(
        "--force-refresh",
        action="store_true",
        help="Clear checkpoint and re-download even if unchanged",
    )
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
    rfb_fetch.add_argument("--force-refresh", action="store_true", help="Clear checkpoint and re-download all files")
    rfb_build = rfb_sub.add_parser("build-network", help="Build corporate network analytics")
    rfb_build.add_argument("--rfb-dir", type=Path, default=Path("data/raw/rfb"))
    rfb_build.add_argument("--output-dir", type=Path, default=DEFAULT_ANALYTICS_DIR)
    rfb_build.add_argument("--max-degree", type=int, default=3, help="Max BFS link degree (1-6)")
    rfb_groups = rfb_sub.add_parser("build-groups", help="Build economic group analytics")
    rfb_groups.add_argument("--rfb-dir", type=Path, default=Path("data/raw/rfb"))
    rfb_groups.add_argument("--output-dir", type=Path, default=DEFAULT_ANALYTICS_DIR)

    stf_portal = subparsers.add_parser("stf-portal", help="Extract timeline data from STF portal")
    stf_portal_sub = stf_portal.add_subparsers(dest="stf_portal_target", required=True)
    stf_portal_fetch = stf_portal_sub.add_parser("fetch", help="Fetch process timeline from STF portal")
    stf_portal_fetch.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/raw/stf_portal"),
        help="Output directory for raw portal data",
    )
    stf_portal_fetch.add_argument(
        "--curated-dir",
        type=Path,
        default=DEFAULT_CURATED_DIR,
        help="Curated JSONL directory (for process list)",
    )
    stf_portal_fetch.add_argument(
        "--max-processes",
        type=int,
        default=None,
        help="Limit number of processes to fetch",
    )
    stf_portal_fetch.add_argument(
        "--rate-limit",
        type=float,
        default=2.0,
        help="Seconds between requests per IP (default: 2.0)",
    )
    stf_portal_fetch.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of concurrent workers (default: 1)",
    )
    stf_portal_fetch.add_argument("--ignore-tls", action="store_true", help="Bypass TLS certificate verification")
    stf_portal_fetch.add_argument(
        "--proxies",
        default=None,
        help="Comma-separated SOCKS5 proxy URLs for IP rotation (e.g. socks5://localhost:1080,socks5://localhost:1081)",
    )
    stf_portal_fetch.add_argument("--dry-run", action="store_true", help="List processes without fetching")
    stf_portal_fetch.add_argument(
        "--max-in-flight",
        type=int,
        default=4,
        help="Max simultaneous HTTP requests across all workers (default: 4)",
    )
    stf_portal_fetch.add_argument(
        "--tab-concurrency",
        type=int,
        default=2,
        help="Max concurrent tab fetches per process (default: 2)",
    )
    stf_portal_fetch.add_argument(
        "--max-retries",
        type=int,
        default=4,
        help="HTTP retry attempts per request (default: 4)",
    )
    stf_portal_fetch.add_argument(
        "--retry-delay",
        type=float,
        default=8.0,
        help="Base retry backoff delay in seconds (default: 8.0)",
    )
    stf_portal_fetch.add_argument(
        "--circuit-breaker-threshold",
        type=int,
        default=5,
        help="Consecutive 403s to open circuit breaker (default: 5)",
    )
    stf_portal_fetch.add_argument(
        "--circuit-breaker-cooldown",
        type=float,
        default=120.0,
        help="Circuit breaker cooldown in seconds (default: 120.0)",
    )
    stf_portal_fetch.add_argument(
        "--max-process-retries",
        type=int,
        default=10,
        help="Max retries per process before permanent failure (default: 10)",
    )
    stf_portal_fetch.add_argument(
        "--partial-dir",
        type=Path,
        default=None,
        help="Directory for partial cache (default: {output_dir}/.partial)",
    )

    oab = subparsers.add_parser("oab", help="Validate OAB numbers against CNA/CNSA")
    oab_sub = oab.add_subparsers(dest="oab_target", required=True)
    oab_validate = oab_sub.add_parser("validate", help="Validate OAB numbers")
    oab_validate.add_argument(
        "--provider",
        choices=["cna", "cnsa", "null", "format"],
        default="null",
        help="Validation provider (default: null)",
    )
    oab_validate.add_argument("--api-key", default=None, help="OAB API key (or set OAB_API_KEY)")
    oab_validate.add_argument(
        "--curated-dir",
        type=Path,
        default=DEFAULT_CURATED_DIR,
        help="Curated JSONL directory containing lawyer_entity.jsonl",
    )

    doc_extract = subparsers.add_parser("doc-extract", help="Extract representation data from PDF documents")
    doc_extract_sub = doc_extract.add_subparsers(dest="doc_extract_target", required=True)
    doc_extract_run = doc_extract_sub.add_parser("run", help="Run selective document extraction")
    doc_extract_run.add_argument(
        "--curated-dir",
        type=Path,
        default=DEFAULT_CURATED_DIR,
        help="Curated JSONL directory",
    )
    doc_extract_run.add_argument(
        "--min-confidence",
        type=float,
        default=0.7,
        help="Only process edges below this confidence (default: 0.7)",
    )
    doc_extract_run.add_argument("--max-documents", type=int, default=None, help="Limit documents to process")

    transparencia = subparsers.add_parser("transparencia", help="Download CSVs from STF transparency portal")
    transparencia_sub = transparencia.add_subparsers(dest="transparencia_target", required=True)
    transparencia_fetch = transparencia_sub.add_parser("fetch", help="Download transparency panel CSVs")
    transparencia_fetch.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/raw/transparencia"),
        help="Output directory for raw CSVs",
    )
    transparencia_fetch.add_argument(
        "--paineis",
        nargs="*",
        default=None,
        help="Panel slugs to download (default: all)",
    )
    transparencia_fetch.add_argument("--headless", action="store_true", help="Run browser without GUI")
    transparencia_fetch.add_argument("--ignore-tls", action="store_true", help="Bypass TLS certificate verification")
    transparencia_fetch.add_argument("--dry-run", action="store_true", help="List panels without downloading")

    agenda = subparsers.add_parser("agenda", help="Fetch and process ministerial agenda data")
    agenda_sub = agenda.add_subparsers(dest="agenda_target", required=True)
    agenda_fetch = agenda_sub.add_parser("fetch", help="Fetch agenda data from STF GraphQL API")
    agenda_fetch.add_argument("--start-year", type=int, default=2024, help="Start year (default: 2024)")
    agenda_fetch.add_argument("--end-year", type=int, default=None, help="End year (default: current)")
    agenda_fetch.add_argument("--rate-limit", type=float, default=1.0, help="Seconds between requests")
    agenda_fetch.add_argument("--output-dir", type=Path, default=Path("data/raw/agenda"), help="Output directory")
    agenda_fetch.add_argument("--dry-run", action="store_true", help="List months without fetching")
    agenda_build = agenda_sub.add_parser("build-events", help="Build curated agenda events and coverage")
    agenda_build.add_argument(
        "--raw-dir",
        type=Path,
        default=Path("data/raw/agenda"),
        help="Raw agenda JSONL directory",
    )
    agenda_build.add_argument("--curated-dir", type=Path, default=DEFAULT_CURATED_DIR, help="Curated output directory")

    deoab = subparsers.add_parser("deoab", help="Fetch and parse OAB Electronic Gazette (DEOAB) for law firm data")
    deoab_sub = deoab.add_subparsers(dest="deoab_target", required=True)
    deoab_fetch = deoab_sub.add_parser("fetch", help="Download and parse DEOAB gazette PDFs")
    deoab_fetch.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/raw/deoab"),
        help="Output directory for raw DEOAB data",
    )
    deoab_fetch.add_argument("--start-year", type=int, default=2019, help="Start year (default: 2019)")
    deoab_fetch.add_argument("--end-year", type=int, default=None, help="End year (default: current)")
    deoab_fetch.add_argument("--dry-run", action="store_true", help="List pending dates without fetching")
    deoab_fetch.add_argument(
        "--force-reprocess",
        action="store_true",
        help="Re-parse all PDFs regardless of parser version",
    )

    oab_sp = subparsers.add_parser("oab-sp", help="Fetch law firm details from OAB/SP")
    oab_sp_sub = oab_sp.add_subparsers(dest="oab_sp_target", required=True)
    oab_sp_fetch = oab_sp_sub.add_parser("fetch", help="Fetch society details by registration number")
    oab_sp_fetch.add_argument("--output-dir", type=Path, default=Path("data/raw/oab_sp"))
    oab_sp_fetch.add_argument("--deoab-dir", type=Path, default=Path("data/raw/deoab"))
    oab_sp_fetch.add_argument("--rate-limit", type=float, default=1.5)
    oab_sp_fetch.add_argument("--max-retries", type=int, default=3)
    oab_sp_fetch.add_argument("--dry-run", action="store_true")

    oab_sp_lookup = oab_sp_sub.add_parser("lookup", help="Lookup lawyers in OAB/SP inscritos registry")
    oab_sp_lookup.add_argument("--output-dir", type=Path, default=Path("data/raw/oab_sp"))
    oab_sp_lookup.add_argument("--deoab-dir", type=Path, default=Path("data/raw/deoab"))
    oab_sp_lookup.add_argument("--curated-dir", type=Path, default=DEFAULT_CURATED_DIR)
    oab_sp_lookup.add_argument("--rate-limit", type=float, default=1.5)
    oab_sp_lookup.add_argument("--max-retries", type=int, default=3)
    oab_sp_lookup.add_argument("--dry-run", action="store_true")

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
