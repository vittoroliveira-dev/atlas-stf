"""Orchestrator for OAB/SP society fetch: search → detail → JSONL."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable
from pathlib import Path

from ._checkpoint import load_checkpoint, save_checkpoint
from ._client import OabSpClient
from ._config import OABSP_DETAIL_URL, OabSpFetchConfig
from ._parser import extract_param_from_search, parse_society_detail

logger = logging.getLogger(__name__)

# Suppress per-request httpx logging
logging.getLogger("httpx").setLevel(logging.WARNING)


def _load_pending_registrations(deoab_dir: Path) -> list[str]:
    """Load unique SP registration numbers from DEOAB JSONL."""
    path = deoab_dir / "oab_sociedade_vinculo.jsonl"
    if not path.exists():
        logger.warning("DEOAB file not found: %s", path)
        return []
    seen: set[str] = set()
    registrations: list[str] = []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            seccional = record.get("seccional", "")
            reg = record.get("sociedade_registro")
            if not reg or not seccional:
                continue
            # Case-insensitive match for "SP" (includes "Sp" quirk)
            if seccional.upper() != "SP":
                continue
            reg_str = str(reg).strip()
            if reg_str and reg_str not in seen:
                seen.add(reg_str)
                registrations.append(reg_str)
    logger.info("Loaded %d unique SP registrations from DEOAB", len(registrations))
    return registrations


def _safe_filename_part(value: str) -> str:
    """Sanitize a value for safe use as a filename component."""
    return re.sub(r"[^\w.\-]", "_", value.replace("..", "_"))


def _save_failed_html(output_dir: Path, reg: str, phase: str, attempt: int, html: str) -> None:
    """Save problematic HTML for diagnostic purposes."""
    fail_dir = output_dir / "_failed_html"
    fail_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{_safe_filename_part(reg)}.{phase}.attempt-{attempt}.html"
    (fail_dir / filename).write_text(html, encoding="utf-8")


def run_society_fetch(
    config: OabSpFetchConfig,
    *,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> int:
    """Main fetch loop. Returns count of completed registrations.

    For each registration:
    1. POST search to get param ID
    2. GET detail page with param ID
    3. Parse detail and append to JSONL
    """
    registrations = _load_pending_registrations(config.deoab_dir)
    if not registrations:
        logger.warning("No SP registrations found in DEOAB data")
        return 0

    total = len(registrations)
    checkpoint = load_checkpoint(config.checkpoint_file)

    # Determine pending
    pending = [
        r for r in registrations if not checkpoint.is_resolved(r) or checkpoint.is_retryable(r, config.max_retries)
    ]

    logger.info(
        "OAB/SP society fetch: %d total, %d resolved, %d pending",
        total,
        total - len(pending),
        len(pending),
    )

    if config.dry_run:
        stats = checkpoint.stats
        logger.info("[DRY] Stats: %s", ", ".join(f"{k}={v}" for k, v in sorted(stats.items())))
        logger.info("[DRY] Would process %d registrations", len(pending))
        return 0

    output_path = config.output_dir / "sociedade_detalhe.jsonl"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    completed_count = 0

    with OabSpClient(
        timeout=config.timeout_seconds,
        rate_limit=config.rate_limit_seconds,
        max_retries=config.max_retries,
        retry_delay=config.retry_delay_seconds,
    ) as client:
        for i, reg in enumerate(pending):
            if on_progress:
                on_progress(i, len(pending), f"OAB/SP: {reg}")

            attempt = checkpoint.retry_count(reg) + 1

            # Step 1: Search by registration number
            try:
                search_html = client.search_by_registration(reg)
            except RuntimeError:
                logger.warning("Search failed for reg=%s after retries", reg)
                checkpoint.mark_failed(reg)
                continue

            param, status = extract_param_from_search(search_html)

            if status == "not_found":
                checkpoint.mark_not_found(reg)
                save_checkpoint(checkpoint, config.checkpoint_file)
                logger.debug("Not found: reg=%s", reg)
                continue

            if status == "unexpected":
                _save_failed_html(config.output_dir, reg, "search", attempt, search_html)
                checkpoint.mark_failed(reg)
                save_checkpoint(checkpoint, config.checkpoint_file)
                logger.warning("Unexpected search result for reg=%s — HTML saved", reg)
                continue

            # status == "found", param is not None
            assert param is not None

            # Step 2: Fetch detail page
            try:
                detail_html = client.fetch_detail(param)
            except RuntimeError:
                logger.warning("Detail fetch failed for reg=%s param=%s after retries", reg, param)
                checkpoint.mark_failed(reg)
                continue

            parsed = parse_society_detail(detail_html, reg)
            if parsed is None:
                _save_failed_html(config.output_dir, reg, "detail", attempt, detail_html)
                checkpoint.mark_failed(reg)
                logger.warning("Failed to parse detail for reg=%s param=%s — HTML saved", reg, param)
                continue

            # Validation: registration number consistency
            if parsed["registration_number"] != reg:
                _save_failed_html(config.output_dir, reg, "detail", attempt, detail_html)
                checkpoint.mark_failed(reg)
                logger.warning(
                    "Registration mismatch for reg=%s: page returned %s — HTML saved",
                    reg,
                    parsed["registration_number"],
                )
                continue

            # Fill in caller-provided fields
            parsed["oab_sp_param"] = param
            parsed["detail_url"] = f"{OABSP_DETAIL_URL}?param={param}"

            # Append to JSONL
            with output_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(parsed, ensure_ascii=False))
                fh.write("\n")

            checkpoint.mark_completed(reg)
            save_checkpoint(checkpoint, config.checkpoint_file)
            completed_count += 1

            if completed_count % 100 == 0:
                logger.info("Progress: %d completed so far", completed_count)

    # Promote failed with retries >= max_retries to exhausted
    promoted = checkpoint.promote_exhausted(config.max_retries)
    if promoted:
        logger.info("Promoted %d failed entries to exhausted", promoted)

    save_checkpoint(checkpoint, config.checkpoint_file)

    stats = checkpoint.stats
    resolved_total = stats["completed"] + stats["not_found"] + stats["exhausted"]
    logger.info(
        "OAB/SP society fetch complete:\n"
        "  total: %d | completed: %d | not_found: %d | exhausted: %d | failed: %d\n"
        "  resolved_total: %d | still_retryable: %d",
        total,
        stats["completed"],
        stats["not_found"],
        stats["exhausted"],
        stats["failed"],
        resolved_total,
        stats["failed"],
    )

    if on_progress:
        on_progress(len(pending), len(pending), "OAB/SP: Concluído")

    return completed_count
