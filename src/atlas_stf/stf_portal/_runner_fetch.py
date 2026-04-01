"""Per-process fetch helpers extracted from _runner to keep modules under 500 lines."""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

from ._checkpoint import PortalCheckpoint
from ._config import StfPortalConfig
from ._extractor import PortalExtractor
from ._metrics import ExtractionMetrics
from ._partial_cache import PartialCache
from ._result import ProcessResult

logger = logging.getLogger(__name__)


def _sanitize_filename(process_number: str) -> str:
    """Convert a process number to a safe filename."""
    return process_number.replace(" ", "_").replace("/", "_")


def _atomic_write_json(path: Path, doc: dict[str, Any]) -> None:
    """Write JSON document atomically (tmp + os.replace)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp, path)


def _fetch_process_incremental(
    process_number: str,
    config: StfPortalConfig,
    extractor: PortalExtractor,
    checkpoint: PortalCheckpoint,
    partial_cache: PartialCache,
    metrics: ExtractionMetrics,
) -> ProcessResult:
    """Fetch a process incrementally, preserving partial progress.

    Returns a ``ProcessResult`` with tri-partite status:
    - ``completed``: document assembled, ready to write.
    - ``retry_later``: transient failure, partial saved on disk.
    - ``permanent_failure``: non-recoverable, marked as failed.
    """
    # --- Check retry containment ---
    meta = partial_cache.get_meta(process_number)
    if meta and meta.retry_count >= config.max_process_retries:
        reason = f"max retries exceeded ({config.max_process_retries})"
        logger.warning("Permanent failure for %s: %s", process_number, reason)
        return ProcessResult(status="permanent_failure", reason=reason)

    # --- Resolve incidente ---
    incidente = partial_cache.get_incidente(process_number)
    if incidente:
        metrics.inc("incidente_reused_from_cache")
    else:
        incidente_from_cp = checkpoint.get_incidente(process_number)
        if incidente_from_cp:
            incidente = incidente_from_cp
            partial_cache.save_incidente(process_number, incidente)
            metrics.inc("incidente_reused_from_cache")
        else:
            t0 = time.monotonic()
            resolve = extractor._resolve_incidente(process_number)
            elapsed_ms = (time.monotonic() - t0) * 1000
            metrics.inc("requests_total")
            metrics.inc("requests_resolve")
            metrics.record_resolve_ms(elapsed_ms)

            if resolve.status == "resolved" and resolve.incidente:
                incidente = resolve.incidente
                partial_cache.save_incidente(process_number, incidente)
                checkpoint.set_incidente(process_number, incidente)
            elif resolve.status == "blocked_403":
                metrics.inc("http_403_total")
                metrics.inc("retryable_errors_total")
                partial_cache.increment_retry(process_number, "403 on resolve")
                return ProcessResult(status="retry_later", reason="403 on resolve")
            elif resolve.status == "transient_failure":
                metrics.inc("retryable_errors_total")
                partial_cache.increment_retry(process_number, "transient failure on resolve")
                return ProcessResult(status="retry_later", reason="transient failure on resolve")
            else:
                # not_found_permanent
                metrics.inc("non_retryable_errors_total")
                return ProcessResult(status="permanent_failure", reason="incidente not found")

    # --- Identify missing tabs ---
    cached_tabs = partial_cache.get_cached_tabs(process_number)
    missing = partial_cache.get_missing_tabs(process_number)

    if cached_tabs:
        metrics.inc("tabs_reused_from_partial", len(cached_tabs))

    # --- Fetch missing tabs ---
    if missing:
        def _on_tab_ok(tab: str, html: str) -> None:
            partial_cache.save_tab(process_number, tab, html)
            metrics.inc("requests_tabs")
            metrics.inc("requests_total")
            metrics.inc("tabs_downloaded_fresh")

        batch = extractor._fetch_tabs_concurrent(
            incidente,
            tabs_to_fetch=tuple(missing),
            on_tab_success=_on_tab_ok,
        )

        # Record failures
        for tab in batch.tabs_failed:
            metrics.inc("requests_total")
            metrics.inc("requests_tabs")

        if batch.blocked:
            metrics.inc("http_403_total")
            metrics.inc("retryable_errors_total")
            partial_cache.increment_retry(process_number, "403 on tabs")
            return ProcessResult(
                status="retry_later",
                tabs_fetched=batch.tabs,
                reason="403 on tabs",
            )

        if batch.tabs_failed:
            failed_names = ", ".join(sorted(batch.tabs_failed))
            if batch.retryable:
                metrics.inc("retryable_errors_total")
                partial_cache.increment_retry(process_number, f"retryable: {failed_names}")
                return ProcessResult(
                    status="retry_later",
                    tabs_fetched=batch.tabs,
                    reason=f"retryable failure on {failed_names}",
                )
            metrics.inc("non_retryable_errors_total")
            return ProcessResult(
                status="permanent_failure",
                tabs_fetched=batch.tabs,
                reason=f"permanent failure on {failed_names}",
            )

    # --- Assemble document ---
    if not partial_cache.all_tabs_present(process_number):
        partial_cache.increment_retry(process_number, "incomplete tabs after fetch")
        return ProcessResult(status="retry_later", reason="incomplete tabs")

    all_tabs = partial_cache.get_cached_tabs(process_number)
    doc = extractor.assemble_document(process_number, incidente, all_tabs)

    if doc is None:
        return ProcessResult(status="permanent_failure", reason="parse failure")

    return ProcessResult(status="completed", doc=doc, tabs_fetched=all_tabs)


def _fetch_single_process(
    process_number: str,
    config: StfPortalConfig,
    extractor: PortalExtractor,
    checkpoint: PortalCheckpoint,
) -> bool:
    """Legacy all-or-nothing fetch. Used by backward-compat facade."""
    output_path = config.output_dir / f"{_sanitize_filename(process_number)}.json"
    cached_incidente = checkpoint.get_incidente(process_number)
    doc = extractor.extract_process(process_number, incidente=cached_incidente)

    if doc is not None:
        incidente = doc.get("incidente")
        if isinstance(incidente, str):
            checkpoint.set_incidente(process_number, incidente)
        _atomic_write_json(output_path, doc)
        checkpoint.mark_completed(process_number)
        return True

    checkpoint.mark_failed(process_number)
    return False
