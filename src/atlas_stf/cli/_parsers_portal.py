"""Portal and external tool subparsers: stf-portal, oab, doc-extract, transparencia, agenda, deoab, oab-sp."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from . import DEFAULT_CURATED_DIR


def _add_portal_parsers(subparsers: Any) -> None:
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
