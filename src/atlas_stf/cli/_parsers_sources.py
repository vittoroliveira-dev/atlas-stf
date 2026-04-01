"""Data source subparsers: datajud, cgu, tse, cvm, rfb."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from . import DEFAULT_ANALYTICS_DIR


def _add_source_parsers(subparsers: Any) -> None:
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
