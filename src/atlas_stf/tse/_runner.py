"""TSE fetch runner: downloads receitas CSVs from TSE open data CDN."""

from __future__ import annotations

import hashlib
import json
import logging
import shutil
import uuid
import zipfile
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from ..core.fetch_lock import FetchLock
from ..core.fetch_result import FetchTimer
from ..core.http_stream_safety import write_limited_stream_to_file
from ..core.zip_safety import enforce_max_uncompressed_size, is_safe_zip_member
from ..fetch._manifest_model import FetchUnit, RemoteState, SourceManifest, build_unit_id
from ..fetch._manifest_store import load_manifest, write_manifest_unlocked
from ..ingest_manifest import capture_csv_manifest, write_manifest
from ._config import TSE_CDN_BASE_URL, TseFetchConfig
from ._parser import _iter_receitas_csv, normalize_donation_record

logger = logging.getLogger(__name__)

_TSE_MAX_ZIP_UNCOMPRESSED_BYTES = 16 * 1024 * 1024 * 1024
_TSE_MAX_DOWNLOAD_BYTES = 16 * 1024 * 1024 * 1024

# Possible file name patterns inside TSE ZIPs
_RECEITAS_PATTERNS = (
    "receitas_candidatos_{year}_BRASIL.csv",
    "receitas_candidatos_{year}.csv",
    "receitas_{year}_BRASIL.csv",
    "consulta_cand_{year}_BRASIL.csv",
)


def _build_zip_urls(year: int) -> list[str]:
    """Build candidate download URLs for a given election year.

    The TSE CDN uses different naming conventions by period:
      2002-2010: prestacao_contas_{year}.zip
      2012-2014: prestacao_final_{year}.zip
      2016:      prestacao_contas_{year}.zip
      2018+:     prestacao_de_contas_eleitorais_candidatos_{year}.zip
    We try all patterns and use the first that succeeds.
    """
    return [
        f"{TSE_CDN_BASE_URL}/prestacao_de_contas_eleitorais_candidatos_{year}.zip",
        f"{TSE_CDN_BASE_URL}/prestacao_contas_{year}.zip",
        f"{TSE_CDN_BASE_URL}/prestacao_final_{year}.zip",
    ]


def _resolve_url(year: int, timeout: int) -> tuple[str, httpx.Headers] | None:
    """Find the working URL for a year via HEAD requests."""
    for url in _build_zip_urls(year):
        try:
            r = httpx.head(url, timeout=timeout, follow_redirects=True)
            if r.status_code == 200:
                return url, r.headers
        except httpx.RequestError:
            continue
    return None


def _download_year_zip(
    year: int,
    output_dir: Path,
    timeout: int,
    *,
    zip_prefix: str = "tse",
) -> tuple[Path | None, dict[str, Any] | None]:
    """Download ZIP from TSE CDN for a given year, trying multiple URL patterns.

    Returns (zip_path, meta) on success, (None, None) on failure.
    Use ``zip_prefix`` to isolate download paths between donation and expense
    runners, preventing race conditions when both run concurrently.

    The returned meta dict has keys: ``url``, ``content_length``, ``etag``.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    zip_path = output_dir / f"{zip_prefix}_{year}.zip"

    for url in _build_zip_urls(year):
        logger.info("Trying TSE %d: %s", year, url)
        try:
            with httpx.stream("GET", url, timeout=timeout, follow_redirects=True) as response:
                response.raise_for_status()
                etag = response.headers.get("etag", "")
                content_length = int(response.headers.get("content-length", "0"))
                actual_size = write_limited_stream_to_file(
                    response,
                    zip_path,
                    max_download_bytes=_TSE_MAX_DOWNLOAD_BYTES,
                )
            logger.info("Downloaded %s (%d bytes)", zip_path.name, actual_size)
            meta: dict[str, Any] = {"url": url, "content_length": content_length or actual_size, "etag": etag}
            return zip_path, meta
        except ValueError as exc:
            logger.warning("Failed to download TSE %d: %s", year, exc)
            zip_path.unlink(missing_ok=True)
            return None, None
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                continue
            logger.warning("Failed to download TSE %d: %s", year, exc)
            zip_path.unlink(missing_ok=True)
            return None, None
        except httpx.RequestError as exc:
            logger.warning("Failed to download TSE %d: %s", year, exc)
            zip_path.unlink(missing_ok=True)
            return None, None

    logger.warning("No valid URL found for TSE %d", year)
    return None, None


def _find_receitas_files(extracted_dir: Path, year: int) -> list[Path]:
    """Locate receitas data files inside the extracted ZIP directory.

    TSE ZIPs vary wildly across years:
      2002-2006: subdir CSV (e.g. ``2004/Candidato/Receita/ReceitaCandidato.csv``)
      2008:      flat CSV   (e.g. ``receitas_candidatos_2008_brasil.csv``)
      2010:      per-UF TXT (e.g. ``candidato/PE/ReceitasCandidatos.txt``)
      2012:      per-UF TXT (e.g. ``receitas_candidatos_2012_AC.txt``)
      2014:      per-UF TXT (e.g. ``receitas_candidatos_2014_AC.txt``)
      2016:      per-UF CSV/TXT
      2018+:     flat CSV   (e.g. ``receitas_candidatos_2018_BRASIL.csv``)

    Returns a list of paths (may be multiple files for per-UF years).
    """
    # Try known flat patterns first (2018+)
    for pattern in _RECEITAS_PATTERNS:
        candidate = extracted_dir / pattern.format(year=year)
        if candidate.exists():
            return [candidate]

    # Recursive search for CSV or TXT files with "receita" + "candidato"
    matches = []
    for ext in ("*.csv", "*.txt"):
        for f in extracted_dir.rglob(ext):
            lower = f.name.lower()
            if "receita" in lower and "candidato" in lower:
                matches.append(f)
    if matches:
        return sorted(matches)

    # Broader: any file with "receita" in the name
    for ext in ("*.csv", "*.txt"):
        for f in extracted_dir.rglob(ext):
            if "receita" in f.name.lower():
                matches.append(f)
    if matches:
        return sorted(matches)

    # Last resort: any CSV/TXT file at any depth
    for ext in ("*.csv", "*.txt"):
        matches.extend(extracted_dir.rglob(ext))
    return sorted(matches) if matches else []


def _extract_zip(zip_path: Path, extract_dir: Path) -> Path | None:
    """Extract a ZIP file and return the extraction directory."""
    extract_dir.mkdir(parents=True, exist_ok=True)
    try:
        with zipfile.ZipFile(zip_path) as zf:
            safe_members: list[zipfile.ZipInfo] = []
            for info in zf.infolist():
                if not is_safe_zip_member(info.filename, extract_dir, external_attr=info.external_attr):
                    logger.warning("Skipping unsafe ZIP member: %s", info.filename)
                    continue
                safe_members.append(info)
            enforce_max_uncompressed_size(
                safe_members,
                max_total_uncompressed_bytes=_TSE_MAX_ZIP_UNCOMPRESSED_BYTES,
            )
            for member in safe_members:
                zf.extract(member, extract_dir)
    except zipfile.BadZipFile:
        logger.warning("Invalid ZIP file %s", zip_path)
        return None
    except ValueError as exc:
        logger.warning("Refusing ZIP %s: %s", zip_path, exc)
        return None
    return extract_dir


def _record_content_hash(record: dict[str, Any]) -> str:
    """Compute SHA-256 hash of the normalized record content (21 fields).

    Computed BEFORE provenance fields are added, so the hash is deterministic
    across rebuilds with the same source data regardless of run metadata.
    """
    return hashlib.sha256(json.dumps(record, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def _iter_year_records(
    year: int,
    zip_path: Path,
    extract_dir: Path,
    *,
    source_url: str = "",
) -> Iterator[dict[str, Any]]:
    """Extract ZIP, find receitas files, yield normalized records one by one.

    Generator approach: never accumulates all records in memory at once.
    """
    if _extract_zip(zip_path, extract_dir) is None:
        return

    files = _find_receitas_files(extract_dir, year)
    if not files:
        logger.warning("No receitas files found for year %d", year)
        return

    logger.info("Found %d receitas file(s) for year %d", len(files), year)
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
            logger.warning("Failed to capture manifest for %s — continuing", csv_path.name)
        for raw in _iter_receitas_csv(csv_path):
            normalized = normalize_donation_record(raw, year)
            normalized["record_hash"] = _record_content_hash(normalized)
            normalized["source_file"] = relative_path
            normalized["source_url"] = source_url
            yield normalized


def fetch_donation_data(
    config: TseFetchConfig,
    *,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Fetch TSE donation data: download ZIPs, parse CSVs, write donations_raw.jsonl.

    Returns the output directory path.
    """
    config.output_dir.mkdir(parents=True, exist_ok=True)

    if config.dry_run:
        logger.info("[dry-run] Would download receitas CSVs for years: %s", list(config.years))
        for year in config.years:
            logger.info("[dry-run] %s", _build_zip_urls(year)[0])
        return config.output_dir

    with FetchLock(config.output_dir, "tse_donations"):
        return _fetch_donation_data_locked(config, on_progress=on_progress)


def _fetch_donation_data_locked(
    config: TseFetchConfig,
    *,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Inner implementation guarded by FetchLock."""
    timer = FetchTimer("tse_donations")
    timer.start()
    try:
        manifest = load_manifest("tse_donations", config.output_dir) or SourceManifest(source="tse_donations")

        if config.force_refresh:
            logger.info("TSE: force-refresh enabled — clearing manifest for requested years")
            for year in config.years:
                uid = build_unit_id("tse_donations", str(year))
                manifest.units.pop(uid, None)

        output_path = config.output_dir / "donations_raw.jsonl"
        total_record_count = 0

        committed_years = {
            int(uid.split(":")[-1])
            for uid, u in manifest.units.items()
            if u.status == "committed"
        }
        pending_years = [y for y in config.years if y not in committed_years]
        total_years = len(config.years)

        if on_progress:
            cached = total_years - len(pending_years)
            if cached:
                on_progress(cached, total_years, f"TSE: {cached} anos em cache")

        # Download ZIPs in parallel (I/O-bound — threads are ideal)
        max_downloads = min(4, len(pending_years)) if pending_years else 1
        downloaded: dict[int, tuple[Path, dict[str, Any]]] = {}
        skipped = 0

        if pending_years:
            logger.info(
                "Checking/downloading %d years in parallel (%d threads)", len(pending_years), max_downloads
            )
            with ThreadPoolExecutor(max_workers=max_downloads) as pool:
                futures = {
                    pool.submit(
                        _download_year_zip,
                        year,
                        config.output_dir,
                        config.timeout_seconds,
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
                        on_progress(done, total_years, f"TSE: Baixou {year}")

        # Provenance metadata: shared across all records in this run
        run_id = str(uuid.uuid4())
        run_collected_at = datetime.now(timezone.utc).isoformat()

        # Stream results to disk incrementally (avoids loading all records in RAM).
        # IMPORTANT: records from years being re-downloaded must be excluded
        # to prevent duplication (e.g. force-refresh or remote-change scenarios).
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
                    "Copied %d existing records from %d completed years (excluded %d from refreshed years)",
                    existing_count,
                    len(committed_years),
                    excluded_count,
                )

            manifest_pending: list[tuple[int, dict[str, Any]]] = []
            for year in config.years:
                if year not in downloaded:
                    continue

                zip_path, meta = downloaded[year]
                extract_dir = config.output_dir / f"extracted_{year}"
                year_count = 0
                try:
                    for record in _iter_year_records(year, zip_path, extract_dir, source_url=meta["url"]):
                        record["collected_at"] = run_collected_at
                        record["ingest_run_id"] = run_id
                        out.write(json.dumps(record, ensure_ascii=False) + "\n")
                        year_count += 1
                finally:
                    zip_path.unlink(missing_ok=True)
                    shutil.rmtree(extract_dir, ignore_errors=True)

                if year_count:
                    total_record_count += year_count
                    logger.info("Wrote %d records for year %d", year_count, year)
                    manifest_pending.append((year, meta))
                else:
                    logger.warning("Year %d returned 0 records — not marking as completed", year)

        # Atomic rename — only after this succeeds do we persist the manifest.
        tmp_path.replace(output_path)

        for year, meta in manifest_pending:
            uid = build_unit_id("tse_donations", str(year))
            manifest.units[uid] = FetchUnit(
                unit_id=uid,
                source="tse_donations",
                label=f"TSE donations {year}",
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
            on_progress(total_years, total_years, "TSE: Concluído")

        timer.log_success(records_written=total_record_count)
        return config.output_dir
    except Exception as exc:
        timer.log_failure(exc)
        raise
