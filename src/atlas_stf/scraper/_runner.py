"""Scraper orchestration and CLI."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path

from ._api import build_search_body, extract_hits, extract_search_after, extract_total, search
from ._audit import ScrapeAuditRecord, append_audit, logger, setup_logging
from ._checkpoint import load_checkpoint, mark_partition_complete, save_checkpoint
from ._config import TARGETS, CheckpointState, ScrapeConfig
from ._pagination import default_date_range, generate_month_partitions
from ._session import ApiError, ApiSession
from ._transform import clean_record


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65_536), b""):
            h.update(chunk)
    return h.hexdigest()


def _date_extremes(path: Path) -> tuple[str | None, str | None]:
    """Read first and last publicacao_data from a JSONL file."""
    first: str | None = None
    last: str | None = None
    with open(path, encoding="utf-8") as f:
        for line in f:
            try:
                doc = json.loads(line)
            except json.JSONDecodeError:
                continue
            val = doc.get("publicacao_data")
            if val:
                if first is None:
                    first = val
                last = val
    return first, last


def scrape_target(config: ScrapeConfig) -> int:
    """Download all partitions for a scrape target. Returns total doc count."""
    target = config.target
    start, end = config.start_date, config.end_date
    if not start or not end:
        default_start, default_end = default_date_range()
        start = start or default_start
        end = end or default_end

    partitions = generate_month_partitions(start, end)
    output_dir = Path(config.output_dir) / target.output_subdir
    output_dir.mkdir(parents=True, exist_ok=True)

    setup_logging(output_dir, verbose=config.verbose)

    logger.info("Scraper: %s — %d partitions (%s → %s)", target.label, len(partitions), start, end)

    if config.dry_run:
        for label, gte, lte in partitions:
            logger.info("  [DRY RUN] %s  (%s → %s)", label, gte, lte)
        return 0

    # Load or create checkpoint
    checkpoint = load_checkpoint(output_dir)
    if checkpoint and checkpoint.target_base != target.base.value:
        logger.warning("Checkpoint target mismatch, starting fresh")
        checkpoint = None
    if not checkpoint:
        checkpoint = CheckpointState(target_base=target.base.value, current_partition="")

    total_docs = checkpoint.total_doc_count

    with ApiSession.create(headless=config.headless, timeout_ms=config.timeout_ms) as session:
        consecutive_403 = 0

        for label, gte, lte in partitions:
            if label in checkpoint.completed_partitions:
                logger.info("  %s — already complete, skipping", label)
                continue

            jsonl_path = output_dir / f"{label}.jsonl"
            partition_start_time = time.monotonic()

            # Quick check: if JSONL exists, query API total and compare line count
            if jsonl_path.exists() and label not in checkpoint.completed_partitions:
                with open(jsonl_path, encoding="utf-8") as fh_count:
                    existing_lines = sum(1 for _ in fh_count)
                if existing_lines > 0:
                    probe_body = build_search_body(target, date_gte=gte, date_lte=lte)
                    probe_body["size"] = 0
                    try:
                        probe_resp = search(session, probe_body)
                        api_count = extract_total(probe_resp)
                        if existing_lines == api_count:
                            logger.info(
                                "  %s — JSONL already has %d docs (matches API), marking complete",
                                label,
                                existing_lines,
                            )
                            mark_partition_complete(checkpoint, label)
                            duration = time.monotonic() - partition_start_time
                            sha = _file_sha256(jsonl_path)
                            first_date, last_date = _date_extremes(jsonl_path)
                            audit_record = ScrapeAuditRecord(
                                target_base=target.base.value,
                                partition=label,
                                doc_count=existing_lines,
                                sha256=sha,
                                first_publicacao_data=first_date,
                                last_publicacao_data=last_date,
                                duration_seconds=round(duration, 2),
                            )
                            append_audit(audit_record, output_dir)
                            checkpoint.total_doc_count += existing_lines
                            total_docs += existing_lines
                            save_checkpoint(checkpoint, output_dir)
                            continue
                    except Exception:
                        pass  # Fall through to normal download

            # Resume or fresh start
            cursor = None
            if checkpoint.current_partition == label and checkpoint.search_after:
                cursor = checkpoint.search_after
                logger.info("  %s — resuming from doc %d", label, checkpoint.partition_doc_count)
            else:
                # Fresh partition: truncate any partial file
                if jsonl_path.exists():
                    jsonl_path.unlink()
                checkpoint.current_partition = label
                checkpoint.partition_doc_count = 0

            page_num = 0
            while True:
                body = build_search_body(target, date_gte=gte, date_lte=lte, search_after=cursor)

                # Retry loop
                response = None
                for attempt in range(config.max_retries):
                    try:
                        response = search(session, body)
                        consecutive_403 = 0
                        break
                    except ApiError as e:
                        if e.status in (202, 403):
                            consecutive_403 += 1
                            if consecutive_403 >= 3:
                                logger.warning("3 consecutive WAF errors — recreating session")
                                session.close()
                                session = ApiSession.create(
                                    headless=config.headless,
                                    timeout_ms=config.timeout_ms,
                                )
                                consecutive_403 = 0
                        wait = config.retry_backoff_base**attempt
                        logger.warning(
                            "  Attempt %d/%d failed (HTTP %d), retrying in %.1fs",
                            attempt + 1,
                            config.max_retries,
                            e.status,
                            wait,
                        )
                        time.sleep(wait)
                    except Exception as e:
                        wait = config.retry_backoff_base**attempt
                        logger.warning(
                            "  Attempt %d/%d failed (%s), retrying in %.1fs",
                            attempt + 1,
                            config.max_retries,
                            e,
                            wait,
                        )
                        time.sleep(wait)

                if response is None:
                    logger.error("  %s — all retries exhausted, aborting partition", label)
                    save_checkpoint(checkpoint, output_dir)
                    return total_docs

                # First page: log total
                if page_num == 0 and cursor is None:
                    api_total = extract_total(response)
                    checkpoint.api_total_hits = api_total
                    logger.info("  %s — API reports %d docs", label, api_total)

                hits = extract_hits(response)
                if not hits:
                    break

                # Clean and write
                with open(jsonl_path, "a", encoding="utf-8") as f:
                    for doc in hits:
                        clean_record(doc, target.text_fields)
                        f.write(json.dumps(doc, ensure_ascii=False) + "\n")

                checkpoint.partition_doc_count += len(hits)
                checkpoint.total_doc_count += len(hits)
                total_docs += len(hits)
                cursor = extract_search_after(response)
                checkpoint.search_after = cursor
                save_checkpoint(checkpoint, output_dir)

                page_num += 1
                logger.debug(
                    "  %s — page %d: %d docs (total partition: %d)",
                    label,
                    page_num,
                    len(hits),
                    checkpoint.partition_doc_count,
                )

                if len(hits) < target.page_size:
                    break

                time.sleep(config.rate_limit_seconds)

            # Partition complete
            duration = time.monotonic() - partition_start_time
            mark_partition_complete(checkpoint, label)
            save_checkpoint(checkpoint, output_dir)

            if jsonl_path.exists() and jsonl_path.stat().st_size > 0:
                sha = _file_sha256(jsonl_path)
                first_date, last_date = _date_extremes(jsonl_path)
                with open(jsonl_path, encoding="utf-8") as fh_count:
                    line_count = sum(1 for _ in fh_count)
                audit_record = ScrapeAuditRecord(
                    target_base=target.base.value,
                    partition=label,
                    doc_count=line_count,
                    sha256=sha,
                    first_publicacao_data=first_date,
                    last_publicacao_data=last_date,
                    duration_seconds=round(duration, 2),
                )
                append_audit(audit_record, output_dir)
                logger.info("  %s — complete: %d docs in %.1fs", label, line_count, duration)
            else:
                logger.info("  %s — no docs found", label)

    logger.info("Scraper finished: %d total docs", total_docs)
    return total_docs


def scrape_decisoes(**kwargs: object) -> int:
    """Convenience: scrape decisões monocráticas."""
    config = ScrapeConfig(target=TARGETS["decisoes"], **kwargs)  # type: ignore[arg-type]
    return scrape_target(config)


def scrape_acordaos(**kwargs: object) -> int:
    """Convenience: scrape acórdãos."""
    config = ScrapeConfig(target=TARGETS["acordaos"], **kwargs)  # type: ignore[arg-type]
    return scrape_target(config)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Atlas STF — Jurisprudência scraper")
    parser.add_argument("target", choices=list(TARGETS.keys()), help="Target base to scrape")
    parser.add_argument("--start-date", help="Start date yyyy-MM-dd (default: 2000-01-01)")
    parser.add_argument("--end-date", help="End date yyyy-MM-dd (default: today)")
    parser.add_argument("--output-dir", default="data/raw/jurisprudencia", help="Base output directory")
    parser.add_argument("--rate-limit", type=float, default=0.5, help="Seconds between pages")
    parser.add_argument("--no-headless", action="store_true", help="Show browser window")
    parser.add_argument("--verbose", "-v", action="store_true", help="Debug-level logging")
    parser.add_argument("--dry-run", action="store_true", help="List partitions without downloading")
    args = parser.parse_args(argv)

    config = ScrapeConfig(
        target=TARGETS[args.target],
        output_dir=args.output_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        rate_limit_seconds=args.rate_limit,
        headless=not args.no_headless,
        verbose=args.verbose,
        dry_run=args.dry_run,
    )

    try:
        scrape_target(config)
    except KeyboardInterrupt:
        logger.info("Interrupted — checkpoint saved")
    except Exception:
        logger.exception("Scraper failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
