"""TSE party organ finance fetch runner.

Downloads ZIPs for party organ financial data (2018+) from the TSE CDN,
parses receitas and despesas_contratadas CSVs, and writes a unified
``party_org_finance_raw.jsonl``.
"""

from __future__ import annotations

import json
import logging
import shutil
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from ..core.http_stream_safety import write_limited_stream_to_file
from ._config import TSE_CDN_BASE_URL, TsePartyOrgFetchConfig
from ._parser_party_org import iter_despesas_csv, iter_receitas_csv, normalize_party_org_record
from ._runner import _extract_zip, _record_content_hash, _YearMeta

logger = logging.getLogger(__name__)

_TSE_MAX_DOWNLOAD_BYTES = 16 * 1024 * 1024 * 1024

# File patterns inside party org ZIPs (only _BRASIL.csv — national aggregate)
_RECEITAS_PATTERN = "receitas_orgaos_partidarios_{year}_BRASIL.csv"
_DESPESAS_PATTERN = "despesas_contratadas_orgaos_partidarios_{year}_BRASIL.csv"


def _build_zip_url(year: int) -> str:
    """Build the download URL for a party organ ZIP."""
    return f"{TSE_CDN_BASE_URL}/prestacao_de_contas_eleitorais_orgaos_partidarios_{year}.zip"


def _resolve_url(year: int, timeout: int) -> tuple[str, httpx.Headers] | None:
    """Find the working URL for a year via HEAD request."""
    url = _build_zip_url(year)
    try:
        r = httpx.head(url, timeout=timeout, follow_redirects=True)
        if r.status_code == 200:
            return url, r.headers
    except httpx.RequestError:
        pass
    return None


@dataclass
class _Checkpoint:
    """Persistent state for party org fetch runs (separate from candidate checkpoint)."""

    completed_years: set[int] = field(default_factory=set)
    year_meta: dict[int, _YearMeta] = field(default_factory=dict)

    _FILENAME = "_checkpoint_party_org.json"

    def to_dict(self) -> dict[str, Any]:
        return {
            "completed_years": sorted(self.completed_years),
            "year_meta": {str(y): m.to_dict() for y, m in self.year_meta.items()},
        }

    @classmethod
    def load(cls, output_dir: Path) -> _Checkpoint:
        path = output_dir / cls._FILENAME
        if not path.exists():
            return cls()
        data = json.loads(path.read_text(encoding="utf-8"))
        meta = {}
        for k, v in data.get("year_meta", {}).items():
            meta[int(k)] = _YearMeta.from_dict(v)
        return cls(completed_years=set(data.get("completed_years", [])), year_meta=meta)

    def save(self, output_dir: Path) -> None:
        path = output_dir / self._FILENAME
        path.write_text(json.dumps(self.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")


def _remote_unchanged(year: int, checkpoint: _Checkpoint, timeout: int) -> bool:
    """Check if remote file matches cached metadata (HEAD only, no download)."""
    meta = checkpoint.year_meta.get(year)
    if meta is None:
        return False
    resolved = _resolve_url(year, timeout)
    if resolved is None:
        return False
    _url, headers = resolved
    return meta.matches(headers)


def _download_year_zip(
    year: int,
    output_dir: Path,
    timeout: int,
    checkpoint: _Checkpoint | None = None,
) -> tuple[Path | None, _YearMeta | None]:
    """Download party organ ZIP from TSE CDN for a given year.

    Returns (zip_path, meta) on success, (None, None) on failure/skip.
    """
    if checkpoint and year in checkpoint.completed_years:
        if _remote_unchanged(year, checkpoint, timeout):
            logger.info("TSE party org %d: unchanged on server, skipping download", year)
            return None, None

    output_dir.mkdir(parents=True, exist_ok=True)
    zip_path = output_dir / f"tse_party_org_{year}.zip"
    url = _build_zip_url(year)

    logger.info("Downloading TSE party org %d: %s", year, url)
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
        meta = _YearMeta(url=url, content_length=content_length or actual_size, etag=etag)
        return zip_path, meta
    except ValueError as exc:
        logger.warning("Failed to download TSE party org %d: %s", year, exc)
        zip_path.unlink(missing_ok=True)
        return None, None
    except httpx.HTTPStatusError as exc:
        logger.warning("Failed to download TSE party org %d: HTTP %d", year, exc.response.status_code)
        zip_path.unlink(missing_ok=True)
        return None, None
    except httpx.RequestError as exc:
        logger.warning("Failed to download TSE party org %d: %s", year, exc)
        zip_path.unlink(missing_ok=True)
        return None, None


def _find_party_org_files(extracted_dir: Path, year: int) -> list[tuple[Path, str]]:
    """Locate party org data files inside the extracted ZIP directory.

    Returns list of (path, record_kind) tuples.
    Only uses _BRASIL.csv aggregate files to avoid double-counting.
    """
    results: list[tuple[Path, str]] = []

    receitas_path = extracted_dir / _RECEITAS_PATTERN.format(year=year)
    if receitas_path.exists():
        results.append((receitas_path, "revenue"))

    despesas_path = extracted_dir / _DESPESAS_PATTERN.format(year=year)
    if despesas_path.exists():
        results.append((despesas_path, "expense"))

    if not results:
        # Fallback: case-insensitive search
        for f in extracted_dir.iterdir():
            lower = f.name.lower()
            if not lower.endswith(".csv") or "brasil" not in lower:
                continue
            if "receitas_orgaos_partidarios" in lower:
                results.append((f, "revenue"))
            elif "despesas_contratadas_orgaos_partidarios" in lower:
                results.append((f, "expense"))

    return sorted(results, key=lambda t: t[0].name)


def _iter_year_records(
    year: int,
    zip_path: Path,
    extract_dir: Path,
    *,
    source_url: str = "",
) -> Iterator[dict[str, Any]]:
    """Extract ZIP, find party org files, yield normalized records."""
    if _extract_zip(zip_path, extract_dir) is None:
        return

    files = _find_party_org_files(extract_dir, year)
    if not files:
        logger.warning("No party org files found for year %d", year)
        return

    logger.info("Found %d party org file(s) for year %d", len(files), year)
    for csv_path, record_kind in files:
        logger.info("Parsing %s (%s) for year %d", csv_path.name, record_kind, year)
        relative_path = str(csv_path.relative_to(extract_dir))
        iterator = iter_receitas_csv(csv_path) if record_kind == "revenue" else iter_despesas_csv(csv_path)
        for raw in iterator:
            normalized = normalize_party_org_record(raw, year)
            normalized["record_hash"] = _record_content_hash(normalized)
            normalized["source_file"] = relative_path
            normalized["source_url"] = source_url
            yield normalized


def fetch_party_org_data(
    config: TsePartyOrgFetchConfig,
    *,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Fetch TSE party organ finance data: download ZIPs, parse CSVs, write JSONL.

    Returns the output directory path.
    """
    config.output_dir.mkdir(parents=True, exist_ok=True)

    if config.dry_run:
        logger.info("[dry-run] Would download party org ZIPs for years: %s", list(config.years))
        for year in config.years:
            logger.info("[dry-run] %s", _build_zip_url(year))
        return config.output_dir

    checkpoint = _Checkpoint.load(config.output_dir)
    if config.force_refresh:
        logger.info("TSE party org: force-refresh enabled — clearing checkpoint for requested years")
        for year in config.years:
            checkpoint.completed_years.discard(year)
            checkpoint.year_meta.pop(year, None)
        checkpoint.save(config.output_dir)

    output_path = config.output_dir / "party_org_finance_raw.jsonl"
    total_record_count = 0

    pending_years = [y for y in config.years if y not in checkpoint.completed_years]
    total_years = len(config.years)

    if on_progress:
        cached = total_years - len(pending_years)
        if cached:
            on_progress(cached, total_years, f"TSE party org: {cached} anos em cache")

    # Download ZIPs sequentially (party org ZIPs are large, ~60-260MB each)
    downloaded: dict[int, tuple[Path, _YearMeta]] = {}
    for year in pending_years:
        zip_path, meta = _download_year_zip(year, config.output_dir, config.timeout_seconds, checkpoint)
        if zip_path is not None and meta is not None:
            downloaded[year] = (zip_path, meta)
        if on_progress:
            done = (total_years - len(pending_years)) + len(downloaded)
            on_progress(done, total_years, f"TSE party org: Baixou {year}")

    # Provenance metadata
    run_id = str(uuid.uuid4())
    run_collected_at = datetime.now(timezone.utc).isoformat()

    # Stream results to disk, excluding years being replaced
    years_being_replaced = set(downloaded.keys())
    tmp_path = output_path.with_suffix(".jsonl.tmp")
    with tmp_path.open("w", encoding="utf-8") as out:
        # Copy existing records, excluding years that will be re-processed
        if output_path.exists() and checkpoint.completed_years:
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

        # Process downloaded ZIPs
        checkpoint_pending: list[tuple[int, _YearMeta]] = []
        for year in config.years:
            if year not in downloaded:
                continue

            zip_path, meta = downloaded[year]
            extract_dir = config.output_dir / f"extracted_party_org_{year}"
            year_count = 0
            try:
                for record in _iter_year_records(year, zip_path, extract_dir, source_url=meta.url):
                    record["collected_at"] = run_collected_at
                    record["ingest_run_id"] = run_id
                    out.write(json.dumps(record, ensure_ascii=False) + "\n")
                    year_count += 1
            finally:
                zip_path.unlink(missing_ok=True)
                shutil.rmtree(extract_dir, ignore_errors=True)

            if year_count:
                total_record_count += year_count
                logger.info("Wrote %d party org records for year %d", year_count, year)
                checkpoint_pending.append((year, meta))
            else:
                logger.warning("Year %d returned 0 party org records — not marking as completed", year)

    # Atomic rename
    tmp_path.replace(output_path)

    for year, meta in checkpoint_pending:
        checkpoint.completed_years.add(year)
        checkpoint.year_meta[year] = meta
    if checkpoint_pending:
        checkpoint.save(config.output_dir)

    if on_progress:
        on_progress(total_years, total_years, "TSE party org: Concluído")

    logger.info(
        "TSE party org fetch complete: %d records written to %s",
        total_record_count,
        output_path,
    )
    return config.output_dir
