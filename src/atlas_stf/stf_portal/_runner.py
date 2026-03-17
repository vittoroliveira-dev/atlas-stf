"""Orchestrator for STF portal extraction with checkpoint, prioritization, and concurrency."""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ._checkpoint import PortalCheckpoint, load_checkpoint, save_checkpoint
from ._config import StfPortalConfig
from ._extractor import PortalExtractor
from ._proxy import ProxyManager

logger = logging.getLogger(__name__)

# Suppress per-request httpx logging (floods the log with 7 lines per process)
logging.getLogger("httpx").setLevel(logging.WARNING)


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
    """Sort processes: alerts first, then by filing_date descending (newer first)."""
    by_date = sorted(processes, key=lambda p: p.get("filing_date") or "", reverse=True)
    return sorted(by_date, key=lambda p: 0 if p.get("process_id") in alert_ids else 1)


def _should_refetch(output_path: Path, refetch_after_days: int) -> bool:
    """Check if the existing output file is stale and needs re-fetching."""
    if not output_path.exists():
        return True
    try:
        with output_path.open(encoding="utf-8") as f:
            data = json.loads(f.read())
        fetched_at = data.get("fetched_at")
        if not fetched_at:
            return True
        fetched_dt = datetime.fromisoformat(fetched_at)
        age_days = (datetime.now(timezone.utc) - fetched_dt).days
        return age_days >= refetch_after_days
    except json.JSONDecodeError, ValueError, KeyError:
        return True


def _fetch_single_process(
    process_number: str,
    config: StfPortalConfig,
    extractor: PortalExtractor,
    checkpoint: PortalCheckpoint,
) -> bool:
    """Fetch a single process. Returns True if fetched, False if failed/skipped."""
    output_path = config.output_dir / f"{_sanitize_filename(process_number)}.json"

    # Phase 3: use cached incidente if available
    cached_incidente = checkpoint.get_incidente(process_number)
    doc = extractor.extract_process(process_number, incidente=cached_incidente)

    if doc is not None:
        # Cache the incidente for future retries
        incidente = doc.get("incidente")
        if isinstance(incidente, str):
            checkpoint.set_incidente(process_number, incidente)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)
            f.write("\n")
        checkpoint.mark_completed(process_number)
        return True

    checkpoint.mark_failed(process_number)
    return False


def run_extraction(
    config: StfPortalConfig,
    *,
    dry_run: bool = False,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> int:
    """Main extraction loop. Returns count of processes fetched.

    When ``config.max_concurrent > 1``, uses ThreadPoolExecutor with one
    PortalExtractor per worker thread. All extractors share:
    - A global ``threading.Semaphore`` limiting total in-flight HTTP requests
    - A ``ProxyManager`` with per-proxy rate limiting and circuit breaking

    Previously failed processes are automatically re-queued for retry.
    """
    processes = _load_process_list(config.curated_dir)
    if not processes:
        logger.error("No processes found in %s", config.curated_dir)
        return 0

    alert_ids = _load_alert_process_ids(config.curated_dir.parent / "analytics")
    prioritized = _prioritize_processes(processes, alert_ids)

    # Apply max_processes limit
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

    checkpoint = load_checkpoint(config.checkpoint_file)

    # Clear previously failed processes so they get re-tried
    failed_count = len(checkpoint.failed_processes)
    if failed_count:
        logger.info("Clearing %d previously failed processes for retry", failed_count)
        checkpoint.clear_failed()

    # Filter to pending processes only
    pending: list[str] = []
    step = 0
    for proc in prioritized:
        process_number = proc.get("process_number")
        if not process_number:
            step += 1
            continue

        output_path = config.output_dir / f"{_sanitize_filename(process_number)}.json"
        if checkpoint.is_completed(process_number):
            if not _should_refetch(output_path, config.refetch_after_days):
                step += 1
                if on_progress:
                    on_progress(step, total, f"STF Portal: {process_number} (cache)")
                continue

        pending.append(process_number)
        step += 1

    if not pending:
        if on_progress:
            on_progress(total, total, "STF Portal: Concluído (tudo em cache)")
        logger.info("All %d processes already fetched", total)
        return 0

    logger.info("Pending: %d processes to fetch", len(pending))

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

    # Cap concurrent workers at 2 (each worker fetches tabs concurrently,
    # but global semaphore + per-proxy rate limiter protect against overload)
    workers = max(1, min(config.max_concurrent, 2))
    if config.max_concurrent > 2:
        logger.info("Capping workers at 2 (requested %d) to reduce blocking risk", config.max_concurrent)
    fetched = 0
    progress_step = total - len(pending)
    progress_lock = threading.Lock()

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

    if workers == 1:
        # Sequential path — simpler, no thread overhead
        with _make_extractor() as extractor:
            for process_number in pending:
                if on_progress:
                    on_progress(progress_step, total, f"STF Portal: {process_number}")

                success = _fetch_single_process(process_number, config, extractor, checkpoint)
                if success:
                    fetched += 1

                progress_step += 1

                if fetched > 0 and fetched % 100 == 0:
                    save_checkpoint(checkpoint, config.checkpoint_file)
    else:
        # Concurrent path — one extractor per worker thread
        _thread_extractors: dict[int, PortalExtractor] = {}
        _ext_lock = threading.Lock()

        def _get_thread_extractor() -> PortalExtractor:
            tid = threading.get_ident()
            with _ext_lock:
                if tid not in _thread_extractors:
                    _thread_extractors[tid] = _make_extractor()
                return _thread_extractors[tid]

        def _worker(process_number: str) -> tuple[str, bool]:
            ext = _get_thread_extractor()
            success = _fetch_single_process(process_number, config, ext, checkpoint)
            return process_number, success

        try:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {pool.submit(_worker, pn): pn for pn in pending}
                for future in as_completed(futures):
                    process_number, success = future.result()
                    if success:
                        fetched += 1

                    with progress_lock:
                        progress_step += 1
                        if on_progress:
                            on_progress(progress_step, total, f"STF Portal: {process_number}")

                    if fetched > 0 and fetched % 100 == 0:
                        save_checkpoint(checkpoint, config.checkpoint_file)
        finally:
            for ext in _thread_extractors.values():
                ext.close()

    save_checkpoint(checkpoint, config.checkpoint_file)
    if on_progress:
        on_progress(total, total, "STF Portal: Concluído")
    logger.info("Extraction complete: %d processes fetched", fetched)
    return fetched


def _sanitize_filename(process_number: str) -> str:
    """Convert a process number to a safe filename."""
    return process_number.replace(" ", "_").replace("/", "_")
