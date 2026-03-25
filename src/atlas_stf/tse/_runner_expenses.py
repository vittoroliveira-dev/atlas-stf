"""TSE campaign expense fetch runner.

Downloads ZIPs from the TSE CDN, parses despesas CSVs, and writes
``campaign_expenses_raw.jsonl``.

Imports helpers from ``_runner.py`` (same package). This is conscious
technical debt: ``_download_year_zip`` and ``_extract_zip`` are private
helpers, imported cross-module to avoid duplication. If the expense
pipeline stabilises, shared helpers can be extracted to ``_common.py``.
"""

from __future__ import annotations

import json
import logging
import shutil
import uuid
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.fetch_lock import FetchLock
from ..core.fetch_result import FetchTimer
from ..fetch._manifest_model import FetchUnit, RemoteState, SourceManifest, build_unit_id
from ..fetch._manifest_store import load_manifest, write_manifest_unlocked
from ..ingest_manifest import capture_csv_manifest, write_manifest
from ._config import TseExpenseFetchConfig
from ._parser_expenses import _iter_despesas_csv, normalize_expense_record
from ._runner import (
    _build_zip_urls,
    _extract_zip,
    _record_content_hash,
)
from ._runner import (
    _download_year_zip as _download_year_zip_base,
)

logger = logging.getLogger(__name__)

# Supported years determined by evidence discovery (see docs/tse_despesas_evidence.md).
# Schema groups: Gen1 (2002), Gen2 (2004), Gen3 (2006), Gen4 (2008), Gen5 (2010), Gen6 (2022-2024).
#
# Excluded years:
#   2018: only despesas_pagas available — no candidate identification (SQ_PRESTADOR_CONTAS only,
#         requires join with candidatura file which is outside this pipeline's scope).
#   2012/2014/2016/2020: not inspected in this version.
_SUPPORTED_EXPENSE_YEARS: tuple[int, ...] = (2002, 2004, 2006, 2008, 2010, 2022, 2024)

# File pattern for 2022+ (despesas_contratadas has full candidate info).
# despesas_pagas excluded: no candidate name/CPF/party/position.
_DESPESAS_CONTRATADAS_PATTERN = "despesas_contratadas_candidatos_{year}_BRASIL.csv"


def _validate_years(years: tuple[int, ...]) -> None:
    """Validate that all requested years are in the supported set.

    Raises ValueError with specific reason for unsupported years.
    """
    for year in years:
        if year == 2018:
            msg = (
                f"TSE expense data for year {year} is not supported. "
                "Only despesas_pagas is available for 2018, which lacks candidate identification "
                "(requires external join by SQ_PRESTADOR_CONTAS). "
                f"Supported years: {_SUPPORTED_EXPENSE_YEARS}. See docs/tse_despesas_evidence.md."
            )
            raise ValueError(msg)
        if year not in _SUPPORTED_EXPENSE_YEARS:
            msg = (
                f"TSE expense data for year {year} is not implemented in this version. "
                f"Supported years: {_SUPPORTED_EXPENSE_YEARS}. See docs/tse_despesas_evidence.md."
            )
            raise ValueError(msg)


def _find_despesas_files(extracted_dir: Path, year: int) -> list[Path]:
    """Locate candidate expense data files inside the extracted ZIP directory.

    TSE ZIPs vary across years:
      2002-2006: subdir CSV (e.g. ``Candidato/Despesa/DespesaCandidato.csv``)
      2008:      flat CSV   (e.g. ``despesas_candidatos_2008_brasil.csv``)
      2010:      per-UF TXT (e.g. ``candidato/PE/DespesasCandidatos.txt``)
      2022+:     flat CSV   (e.g. ``despesas_contratadas_candidatos_2022_BRASIL.csv``)

    Returns a list of paths (may be multiple files for per-UF years).
    """
    # 1. Try despesas_contratadas BRASIL pattern (2022+, flat at top level)
    candidate = extracted_dir / _DESPESAS_CONTRATADAS_PATTERN.format(year=year)
    if candidate.exists():
        return [candidate]

    # 2. Recursive search for candidate expense files
    matches: list[Path] = []
    for ext in ("*.csv", "*.txt"):
        for f in extracted_dir.rglob(ext):
            lower = f.name.lower()
            parts_lower = str(f.relative_to(extracted_dir)).lower()
            # Exclude comite/partido directories and files
            if "comit" in parts_lower or "partido" in parts_lower:
                continue
            # Exclude despesas_pagas (no candidate info in modern format)
            if "pagas" in lower:
                continue
            if "despesa" in lower and "candidato" in lower:
                matches.append(f)
    if matches:
        return sorted(matches)

    return []


def _iter_year_expense_records(
    year: int,
    zip_path: Path,
    extract_dir: Path,
    *,
    source_url: str = "",
) -> Iterator[dict[str, Any]]:
    """Extract ZIP, find despesas files, yield normalized records."""
    if _extract_zip(zip_path, extract_dir) is None:
        return

    files = _find_despesas_files(extract_dir, year)
    if not files:
        logger.warning("No expense files found for year %d", year)
        return

    logger.info("Found %d expense file(s) for year %d", len(files), year)
    for csv_path in files:
        logger.info("Parsing %s for year %d", csv_path.name, year)
        relative_path = str(csv_path.relative_to(extract_dir))
        try:
            manifest = capture_csv_manifest(
                csv_path,
                source="tse",
                year_or_cycle=str(year),
                origin_url=source_url,
                parser_version="2.0",
            )
            write_manifest(manifest, csv_path.parent / "_source_manifests")
        except Exception:
            logger.warning("Failed to capture expense manifest for %s — continuing", csv_path.name)
        for raw in _iter_despesas_csv(csv_path):
            normalized = normalize_expense_record(raw, year)
            normalized["record_hash"] = _record_content_hash(normalized)
            normalized["source_file"] = relative_path
            normalized["source_url"] = source_url
            yield normalized


def fetch_expense_data(
    config: TseExpenseFetchConfig,
    *,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Fetch TSE campaign expense data: download ZIPs, parse CSVs, write JSONL.

    Returns the output directory path.
    """
    years = config.years or _SUPPORTED_EXPENSE_YEARS
    _validate_years(years)

    config.output_dir.mkdir(parents=True, exist_ok=True)

    if config.dry_run:
        logger.info("[dry-run] Would download expense CSVs for years: %s", list(years))
        for year in years:
            logger.info("[dry-run] %s", _build_zip_urls(year)[0])
        return config.output_dir

    with FetchLock(config.output_dir, "tse_expenses"):
        return _fetch_expense_data_locked(config, years=years, on_progress=on_progress)


def _fetch_expense_data_locked(
    config: TseExpenseFetchConfig,
    *,
    years: tuple[int, ...],
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Inner implementation guarded by FetchLock."""
    timer = FetchTimer("tse_expenses")
    timer.start()
    try:
        manifest = load_manifest("tse_expenses", config.output_dir) or SourceManifest(source="tse_expenses")
        if config.force_refresh:
            logger.info("TSE expenses: force-refresh — clearing manifest for requested years")
            for year in years:
                uid = build_unit_id("tse_expenses", str(year))
                manifest.units.pop(uid, None)

        output_path = config.output_dir / "campaign_expenses_raw.jsonl"
        total_record_count = 0

        committed_years = {int(uid.split(":")[-1]) for uid, u in manifest.units.items() if u.status == "committed"}
        pending_years = [y for y in years if y not in committed_years]
        total_years = len(years)

        if on_progress:
            cached = total_years - len(pending_years)
            if cached:
                on_progress(cached, total_years, f"TSE Despesas: {cached} anos em cache")

        # Download ZIPs in parallel (I/O-bound).
        # manifest skip logic handled by pending_years filter above.
        max_downloads = min(4, len(pending_years)) if pending_years else 1
        downloaded: dict[int, tuple[Path, dict[str, Any]]] = {}
        skipped = 0

        if pending_years:
            logger.info("Checking/downloading %d years in parallel (%d threads)", len(pending_years), max_downloads)
            with ThreadPoolExecutor(max_workers=max_downloads) as pool:
                futures = {
                    pool.submit(
                        _download_year_zip_base,
                        year,
                        config.output_dir,
                        config.timeout_seconds,
                        zip_prefix="tse_expenses",
                    ): year
                    for year in pending_years
                }
                for future in as_completed(futures):
                    year = futures[future]
                    zip_path, meta = future.result()
                    if zip_path is not None and meta is not None:
                        downloaded[year] = (zip_path, meta)
                    else:
                        skipped += 1
                    if on_progress:
                        done = (total_years - len(pending_years)) + len(downloaded) + skipped
                        on_progress(done, total_years, f"TSE Despesas: Baixou {year}")

        # Provenance metadata
        run_id = str(uuid.uuid4())
        run_collected_at = datetime.now(timezone.utc).isoformat()

        # Stream results to disk, excluding years being replaced
        years_being_replaced = set(downloaded.keys())
        tmp_path = output_path.with_suffix(".jsonl.tmp")
        with tmp_path.open("w", encoding="utf-8") as out:
            if output_path.exists() and committed_years:
                existing_count = 0
                excluded_count = 0
                with output_path.open("r", encoding="utf-8") as fh:
                    for line in fh:
                        line = line.strip()
                        if not line:
                            continue
                        if years_being_replaced:
                            try:
                                record = json.loads(line)
                                if record.get("election_year") in years_being_replaced:
                                    excluded_count += 1
                                    continue
                            except json.JSONDecodeError:
                                pass
                        out.write(line + "\n")
                        existing_count += 1
                total_record_count += existing_count
                logger.info(
                    "Copied %d existing records (excluded %d from refreshed years)",
                    existing_count,
                    excluded_count,
                )

            manifest_pending: list[tuple[int, dict[str, Any]]] = []
            for year in years:
                if year not in downloaded:
                    continue

                zip_path, meta = downloaded[year]
                extract_dir = config.output_dir / f"extracted_expenses_{year}"
                year_count = 0
                try:
                    for record in _iter_year_expense_records(year, zip_path, extract_dir, source_url=meta["url"]):
                        record["collected_at"] = run_collected_at
                        record["ingest_run_id"] = run_id
                        out.write(json.dumps(record, ensure_ascii=False) + "\n")
                        year_count += 1
                finally:
                    zip_path.unlink(missing_ok=True)
                    shutil.rmtree(extract_dir, ignore_errors=True)

                if year_count:
                    total_record_count += year_count
                    logger.info("Wrote %d expense records for year %d", year_count, year)
                    manifest_pending.append((year, meta))
                else:
                    logger.warning("Year %d returned 0 expense records — not marking as completed", year)

        # Atomic rename — only after this succeeds do we persist the manifest.
        tmp_path.replace(output_path)

        for year, meta in manifest_pending:
            uid = build_unit_id("tse_expenses", str(year))
            manifest.units[uid] = FetchUnit(
                unit_id=uid,
                source="tse_expenses",
                label=f"TSE expenses {year}",
                remote_url=meta["url"],
                remote_state=RemoteState(
                    url=meta["url"],
                    etag=meta.get("etag", ""),
                    content_length=meta.get("content_length", 0),
                ),
                status="committed",
                fetch_date=datetime.now(timezone.utc).isoformat(),
            )
        if manifest_pending:
            manifest.last_updated = datetime.now(timezone.utc).isoformat()
            write_manifest_unlocked(manifest, config.output_dir)

        if on_progress:
            on_progress(total_years, total_years, "TSE Despesas: Concluído")

        timer.log_success(records_written=total_record_count)
        return config.output_dir
    except Exception as exc:
        timer.log_failure(exc)
        raise
