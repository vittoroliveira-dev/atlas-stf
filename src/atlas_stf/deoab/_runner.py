"""Orchestrator for DEOAB gazette extraction: download → parse → write JSONL."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import httpx

from ._checkpoint import DateEntry, load_checkpoint, save_checkpoint
from ._config import (
    DEOAB_BASE_URL,
    DEOAB_PDF_PATTERN,
    PARSER_VERSION,
    DeoabFetchConfig,
)
from ._parser import extract_text_from_pdf, parse_sociedade_records

logger = logging.getLogger(__name__)

# Suppress per-request httpx logging
logging.getLogger("httpx").setLevel(logging.WARNING)


def _generate_dates(start_year: int, end_year: int) -> list[date]:
    """Generate all dates from start_year to end_year (inclusive)."""
    dates: list[date] = []
    current = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    today = date.today()
    if end > today:
        end = today
    while current <= end:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def _date_to_str(d: date) -> str:
    """Format date as DD-MM-YYYY for DEOAB URL."""
    return d.strftime("%d-%m-%Y")


def _date_to_iso(d: date) -> str:
    """Format date as YYYY-MM-DD for records."""
    return d.isoformat()


def _build_url(d: date) -> str:
    """Build DEOAB PDF URL for a given date."""
    filename = DEOAB_PDF_PATTERN.format(date=_date_to_str(d))
    return f"{DEOAB_BASE_URL}/{filename}"


def _probe_pdf(client: httpx.Client, url: str) -> int:
    """HEAD request to check if PDF exists. Returns content_length, 0 if not found."""
    try:
        resp = client.head(url)
        if resp.status_code == 200:
            length = int(resp.headers.get("content-length", "0"))
            # Real PDFs are >10KB; the SPA HTML fallback is ~1.4KB
            if length > 5000:
                return length
        return 0
    except httpx.HTTPError:
        return 0


def _download_pdf(client: httpx.Client, url: str, dest: Path, max_retries: int, retry_delay: float) -> bool:
    """Download PDF to dest. Returns True on success."""
    for attempt in range(max_retries):
        try:
            with client.stream("GET", url) as resp:
                resp.raise_for_status()
                dest.parent.mkdir(parents=True, exist_ok=True)
                with dest.open("wb") as f:
                    for chunk in resp.iter_bytes(chunk_size=65536):
                        f.write(chunk)
            return True
        except httpx.HTTPError as exc:
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (2**attempt))
                logger.warning("Download %s failed (attempt %d): %s", url, attempt + 1, exc)
            else:
                logger.warning("Download %s failed after %d attempts", url, max_retries)
    return False


def run_deoab_fetch(
    config: DeoabFetchConfig,
    *,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> int:
    """Main extraction loop. Returns count of dates processed.

    For each date:
    1. HEAD to check if PDF exists and get content_length
    2. Download if new or content_length changed
    3. Parse with pdftotext + regex
    4. Write sociedade records to JSONL
    """
    end_year = config.end_year or datetime.now(timezone.utc).year
    all_dates = _generate_dates(config.start_year, end_year)
    total = len(all_dates)

    logger.info(
        "DEOAB fetch: %d dates (%d-%d), output=%s",
        total,
        config.start_year,
        end_year,
        config.output_dir,
    )

    if config.dry_run:
        checkpoint = load_checkpoint(config.checkpoint_file)
        pending = sum(1 for d in all_dates if checkpoint.needs_parse(_date_to_iso(d), PARSER_VERSION))
        logger.info("[DRY] %d dates total, %d pending", total, pending)
        stats = checkpoint.stats
        for status, count in sorted(stats.items()):
            logger.info("[DRY] %s: %d", status, count)
        return 0

    checkpoint = load_checkpoint(config.checkpoint_file)
    pdf_dir = config.output_dir / "pdfs"
    pdf_dir.mkdir(parents=True, exist_ok=True)
    output_path = config.output_dir / "oab_sociedade_vinculo.jsonl"

    # Collect all records across dates
    all_records: list[dict[str, object]] = []
    processed = 0

    with httpx.Client(timeout=60.0, follow_redirects=True) as client:
        for i, d in enumerate(all_dates):
            date_str = _date_to_iso(d)
            url = _build_url(d)

            if on_progress:
                on_progress(i, total, f"DEOAB: {date_str}")

            # Check if already parsed with current parser version
            if not config.force_reprocess and not checkpoint.needs_parse(date_str, PARSER_VERSION):
                continue

            # Probe PDF existence
            content_length = _probe_pdf(client, url)
            time.sleep(config.rate_limit_seconds * 0.3)  # Light rate limit for HEAD

            if content_length == 0:
                checkpoint.set(date_str, DateEntry(status="missing", source_url=url))
                continue

            # Check if download needed
            pdf_path = pdf_dir / f"{_date_to_str(d)}.pdf"
            if checkpoint.needs_download(date_str, content_length) or not pdf_path.exists():
                time.sleep(config.rate_limit_seconds)
                success = _download_pdf(client, url, pdf_path, config.max_retries, config.retry_delay_seconds)
                if not success:
                    checkpoint.set(
                        date_str,
                        DateEntry(status="failed", source_url=url, content_length=content_length, error="download"),
                    )
                    continue
                checkpoint.set(
                    date_str,
                    DateEntry(status="downloaded", source_url=url, content_length=content_length),
                )

            # Parse
            text = extract_text_from_pdf(pdf_path)
            if text is None:
                checkpoint.set(
                    date_str,
                    DateEntry(
                        status="failed",
                        source_url=url,
                        content_length=content_length,
                        error="pdftotext",
                    ),
                )
                continue

            records = parse_sociedade_records(text, url, date_str)
            all_records.extend(records)

            # Remove PDF after successful parse (saves ~6GB over full history)
            pdf_path.unlink(missing_ok=True)

            checkpoint.set(
                date_str,
                DateEntry(
                    status="parsed",
                    source_url=url,
                    content_length=content_length,
                    parser_version=PARSER_VERSION,
                ),
            )
            processed += 1

            if processed > 0 and processed % 50 == 0:
                save_checkpoint(checkpoint, config.checkpoint_file)
                logger.info("Progress: %d dates parsed, %d records so far", processed, len(all_records))

    # Merge with existing records to avoid data loss across incremental runs
    existing_records: list[dict[str, object]] = []
    if output_path.exists():
        try:
            with output_path.open("r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        existing_records.append(json.loads(line))
        except json.JSONDecodeError, OSError:
            logger.warning("Could not read existing %s, starting fresh", output_path)

    def _dedup_key(r: dict[str, object]) -> str:
        return (
            f"{r.get('data_publicacao')}:{r.get('sociedade_nome')}"
            f":{r.get('oab_number')}:{r.get('sociedade_registro')}"
        )

    seen: dict[str, dict[str, object]] = {}
    for record in existing_records:
        seen[_dedup_key(record)] = record
    for record in all_records:
        seen[_dedup_key(record)] = record  # new records take precedence

    merged = list(seen.values())
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(".jsonl.tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        for record in merged:
            f.write(json.dumps(record, ensure_ascii=False))
            f.write("\n")
    tmp_path.replace(output_path)
    logger.info(
        "Wrote %d records (%d new, %d existing) to %s",
        len(merged), len(all_records), len(existing_records), output_path,
    )

    save_checkpoint(checkpoint, config.checkpoint_file)
    stats = checkpoint.stats
    logger.info(
        "DEOAB fetch complete: %d dates parsed, %d records. Status: %s",
        processed,
        len(all_records),
        ", ".join(f"{k}={v}" for k, v in sorted(stats.items())),
    )

    if on_progress:
        on_progress(total, total, "DEOAB: Concluído")

    return processed
