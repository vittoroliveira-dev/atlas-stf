"""Orchestrator for STF portal extraction with partial persistence and metrics."""

from __future__ import annotations

import atexit
import json
import logging
import os
import signal
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ._checkpoint import PortalCheckpoint, load_checkpoint, save_checkpoint
from ._config import StfPortalConfig
from ._extractor import PortalExtractor
from ._metrics import ExtractionMetrics
from ._partial_cache import PartialCache
from ._proxy import ProxyManager
from ._result import ProcessResult

logger = logging.getLogger(__name__)

_shutdown_requested = threading.Event()

logging.getLogger("httpx").setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# Helpers (unchanged public API for tests)
# ---------------------------------------------------------------------------


def _load_process_list(curated_dir: Path) -> list[dict[str, Any]]:
    """Load process records from curated process.jsonl."""
    path = curated_dir / "process.jsonl"
    if not path.exists():
        logger.warning("No process.jsonl found at %s", path)
        return []
    records: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def _load_alert_process_ids(analytics_dir: Path) -> set[str]:
    """Load process IDs that have active alerts (for prioritization)."""
    path = analytics_dir / "outlier_alert.jsonl"
    if not path.exists():
        return set()
    ids: set[str] = set()
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                record = json.loads(line)
                pid = record.get("process_id")
                if isinstance(pid, str):
                    ids.add(pid)
    return ids


def _prioritize_processes(
    processes: list[dict[str, Any]],
    alert_ids: set[str],
) -> list[dict[str, Any]]:
    """Sort processes: alerts first, then by filing_date descending."""
    by_date = sorted(processes, key=lambda p: p.get("filing_date") or "", reverse=True)
    return sorted(by_date, key=lambda p: 0 if p.get("process_id") in alert_ids else 1)


def _should_refetch(output_path: Path, refetch_after_days: int) -> bool:
    """Check if the existing output file is stale and needs re-fetching."""
    if not output_path.exists():
        return True
    try:
        with output_path.open(encoding="utf-8") as f:
            data = json.load(f)
        fetched_at = data.get("fetched_at")
        if not fetched_at:
            return True
        fetched_dt = datetime.fromisoformat(fetched_at)
        age_days = (datetime.now(timezone.utc) - fetched_dt).days
        return age_days >= refetch_after_days
    except json.JSONDecodeError, ValueError, KeyError:
        return True


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


# ---------------------------------------------------------------------------
# Incremental per-process fetch with partial persistence
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Legacy fetch (backward compat for tests using _FakeExtractor)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Main extraction loop
# ---------------------------------------------------------------------------


def run_extraction(
    config: StfPortalConfig,
    *,
    dry_run: bool = False,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> int:
    """Main extraction loop with partial persistence and metrics.

    Returns count of processes fetched (completed).
    """
    processes = _load_process_list(config.curated_dir)
    if not processes:
        logger.error("No processes found in %s", config.curated_dir)
        return 0

    alert_ids = _load_alert_process_ids(config.curated_dir.parent / "analytics")
    prioritized = _prioritize_processes(processes, alert_ids)

    if config.max_processes is not None:
        prioritized = prioritized[: config.max_processes]

    total = len(prioritized)
    logger.info(
        "STF Portal extraction: %d processes (%d with alerts), max=%s, workers=%d",
        total,
        len(alert_ids & {p.get("process_id", "") for p in prioritized}),
        config.max_processes or "all",
        config.max_concurrent,
    )

    if dry_run:
        for proc in prioritized[:20]:
            has_alert = "!" if proc.get("process_id") in alert_ids else " "
            logger.info("  [DRY] %s %s", has_alert, proc.get("process_number", "?"))
        if total > 20:
            logger.info("  ... and %d more", total - 20)
        return 0

    # --- PID file ---
    pid_path = config.output_dir / ".fetch.pid"
    config.output_dir.mkdir(parents=True, exist_ok=True)
    pid_path.write_text(str(os.getpid()), encoding="utf-8")
    atexit.register(lambda: pid_path.unlink(missing_ok=True))

    # --- SIGTERM handler ---
    _shutdown_requested.clear()

    def _handle_sigterm(_signum: int, _frame: object) -> None:
        _shutdown_requested.set()

    previous_handler = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, _handle_sigterm)

    assert config.checkpoint_file is not None  # resolved by __post_init__
    assert config.partial_dir is not None  # resolved by __post_init__
    checkpoint = load_checkpoint(config.checkpoint_file)
    partial_cache = PartialCache(config.partial_dir)
    metrics = ExtractionMetrics()

    # --- Build pending queue ---
    # No bulk clear_failed(). Processes in `failed` (permanent) stay failed.
    # Processes with partial dirs that aren't completed enter naturally.
    pending: list[str] = []
    partial_in_pending = 0
    step = 0
    for proc in prioritized:
        process_number = proc.get("process_number")
        if not process_number:
            step += 1
            continue

        # Skip completed (unless stale)
        output_path = config.output_dir / f"{_sanitize_filename(process_number)}.json"
        if checkpoint.is_completed(process_number):
            if not _should_refetch(output_path, config.refetch_after_days):
                step += 1
                if on_progress:
                    on_progress(step, total, f"STF Portal: {process_number} (cache)")
                continue

        # Skip permanent failures (no partial dir = was permanent)
        if checkpoint.is_failed(process_number) and not partial_cache.has_partial(process_number):
            step += 1
            continue

        # If failed but has partial → eligible for retry (re-enqueue)
        if checkpoint.is_failed(process_number) and partial_cache.has_partial(process_number):
            checkpoint.mark_pending(process_number)

        pending.append(process_number)
        if partial_cache.has_partial(process_number):
            partial_in_pending += 1
        step += 1

    if not pending:
        if on_progress:
            on_progress(total, total, "STF Portal: Concluído (tudo em cache)")
        logger.info("All %d processes already fetched", total)
        return 0

    metrics.processes_resumed_from_partial = partial_in_pending
    logger.info("Pending: %d processos (%d com progresso parcial)", len(pending), partial_in_pending)

    # --- Shared concurrency controls ---
    request_semaphore = threading.Semaphore(config.max_in_flight)
    proxy_manager = ProxyManager(
        proxy_urls=config.proxies,
        per_proxy_rate=config.global_rate_seconds,
        circuit_threshold=config.circuit_breaker_threshold,
        circuit_cooldown=config.circuit_breaker_cooldown,
    )
    logger.info(
        "ProxyManager: %d IPs (local + %d proxies), rate=%.1fs/IP",
        len(proxy_manager),
        len(config.proxies),
        config.global_rate_seconds,
    )

    # Workers: no silent cap. Use configured value directly.
    workers = config.max_concurrent
    min_in_flight = workers * (config.tab_concurrency + 1)
    if config.max_in_flight < min_in_flight:
        logger.warning(
            "max_in_flight=%d pode ser insuficiente para %d workers × %d tab_concurrency "
            "— considere aumentar via --max-in-flight (mínimo sugerido: %d)",
            config.max_in_flight,
            workers,
            config.tab_concurrency,
            min_in_flight,
        )
    logger.info(
        "Configuração efetiva: workers=%d, max_in_flight=%d, tab_concurrency=%d",
        workers,
        config.max_in_flight,
        config.tab_concurrency,
    )

    fetched = 0
    progress_step = total - len(pending)
    progress_lock = threading.Lock()
    start_time = time.monotonic()

    def _make_extractor() -> PortalExtractor:
        return PortalExtractor(
            rate_limit_seconds=config.rate_limit_seconds,
            timeout_seconds=config.navigation_timeout_ms / 1000,
            max_retries=config.max_retries,
            retry_delay_seconds=config.retry_delay_seconds,
            ignore_tls=config.ignore_tls,
            request_semaphore=request_semaphore,
            proxy_manager=proxy_manager,
            tab_concurrency=config.tab_concurrency,
        )

    def _handle_result(process_number: str, result: ProcessResult) -> bool:
        """Process a result. Returns True if completed."""
        output_path = config.output_dir / f"{_sanitize_filename(process_number)}.json"

        if result.status == "completed" and result.doc is not None:
            _atomic_write_json(output_path, result.doc)
            checkpoint.mark_completed(process_number)
            partial_cache.cleanup(process_number)
            metrics.inc("processes_completed")
            return True

        if result.status == "permanent_failure":
            checkpoint.mark_failed(process_number)
            metrics.inc("processes_failed")
            logger.warning("Permanent failure: %s — %s", process_number, result.reason)
            return False

        # retry_later: partial already saved, nothing to mark
        return False

    if workers == 1:
        with _make_extractor() as extractor:
            for process_number in pending:
                if _shutdown_requested.is_set():
                    logger.info("SIGTERM recebido — salvando checkpoint e saindo.")
                    save_checkpoint(checkpoint, config.checkpoint_file)
                    break

                if on_progress:
                    on_progress(progress_step, total, f"STF Portal: {process_number}")

                result = _fetch_process_incremental(
                    process_number, config, extractor, checkpoint, partial_cache, metrics,
                )
                if _handle_result(process_number, result):
                    fetched += 1

                progress_step += 1

                if fetched > 0 and fetched % 50 == 0:
                    save_checkpoint(checkpoint, config.checkpoint_file)
                    logger.info(metrics.summary_line())
    else:
        _thread_extractors: dict[int, PortalExtractor] = {}
        _ext_lock = threading.Lock()

        def _get_thread_extractor() -> PortalExtractor:
            tid = threading.get_ident()
            with _ext_lock:
                if tid not in _thread_extractors:
                    _thread_extractors[tid] = _make_extractor()
                return _thread_extractors[tid]

        def _worker(process_number: str) -> tuple[str, ProcessResult]:
            ext = _get_thread_extractor()
            result = _fetch_process_incremental(
                process_number, config, ext, checkpoint, partial_cache, metrics,
            )
            return process_number, result

        try:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {pool.submit(_worker, pn): pn for pn in pending}
                for future in as_completed(futures):
                    process_number, result = future.result()
                    if _handle_result(process_number, result):
                        fetched += 1

                    with progress_lock:
                        progress_step += 1
                        if on_progress:
                            on_progress(progress_step, total, f"STF Portal: {process_number}")

                    if fetched > 0 and fetched % 50 == 0:
                        save_checkpoint(checkpoint, config.checkpoint_file)
                        logger.info(metrics.summary_line())

                    if _shutdown_requested.is_set():
                        logger.info("SIGTERM recebido — cancelando futures pendentes.")
                        for f in futures:
                            f.cancel()
                        break
        finally:
            for ext in _thread_extractors.values():
                ext.close()

    # --- Finalize ---
    metrics.elapsed_seconds = time.monotonic() - start_time
    save_checkpoint(checkpoint, config.checkpoint_file)
    metrics.save(config.output_dir / ".metrics.json")
    signal.signal(signal.SIGTERM, previous_handler)
    pid_path.unlink(missing_ok=True)
    if on_progress:
        on_progress(total, total, "STF Portal: Concluído")
    logger.info("Extração concluída. %s", metrics.summary_line())
    return fetched
