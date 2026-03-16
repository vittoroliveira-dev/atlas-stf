"""Orchestrator for STF portal extraction with checkpoint and prioritization."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ._checkpoint import load_checkpoint, save_checkpoint
from ._config import StfPortalConfig
from ._extractor import PortalExtractor

logger = logging.getLogger(__name__)


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


def run_extraction(config: StfPortalConfig, *, dry_run: bool = False) -> int:
    """Main extraction loop. Returns count of processes fetched."""
    processes = _load_process_list(config.curated_dir)
    if not processes:
        logger.error("No processes found in %s", config.curated_dir)
        return 0

    alert_ids = _load_alert_process_ids(config.curated_dir.parent / "analytics")
    prioritized = _prioritize_processes(processes, alert_ids)

    # Apply max_processes limit
    if config.max_processes is not None:
        prioritized = prioritized[: config.max_processes]

    logger.info(
        "STF Portal extraction: %d processes (%d with alerts), max=%s",
        len(prioritized),
        len(alert_ids & {p.get("process_id", "") for p in prioritized}),
        config.max_processes or "all",
    )

    if dry_run:
        for proc in prioritized[:20]:
            has_alert = "!" if proc.get("process_id") in alert_ids else " "
            logger.info("  [DRY] %s %s", has_alert, proc.get("process_number", "?"))
        if len(prioritized) > 20:
            logger.info("  ... and %d more", len(prioritized) - 20)
        return 0

    checkpoint = load_checkpoint(config.checkpoint_file)
    fetched = 0

    with PortalExtractor(
        rate_limit_seconds=config.rate_limit_seconds,
        timeout_seconds=config.navigation_timeout_ms / 1000,
        max_retries=config.max_retries,
        retry_delay_seconds=config.retry_delay_seconds,
    ) as extractor:
        for proc in prioritized:
            process_number = proc.get("process_number")
            if not process_number:
                continue

            # Skip completed processes (unless stale)
            output_path = config.output_dir / f"{_sanitize_filename(process_number)}.json"
            if checkpoint.is_completed(process_number):
                if not _should_refetch(output_path, config.refetch_after_days):
                    continue

            # Extract
            doc = extractor.extract_process(process_number)

            if doc is not None:
                output_path.parent.mkdir(parents=True, exist_ok=True)
                with output_path.open("w", encoding="utf-8") as f:
                    json.dump(doc, f, ensure_ascii=False, indent=2)
                    f.write("\n")
                checkpoint.mark_completed(process_number)
                fetched += 1
            else:
                checkpoint.mark_failed(process_number)

            # Save checkpoint periodically
            if fetched % 10 == 0:
                save_checkpoint(checkpoint, config.checkpoint_file)

    save_checkpoint(checkpoint, config.checkpoint_file)
    logger.info("Extraction complete: %d processes fetched", fetched)
    return fetched


def _sanitize_filename(process_number: str) -> str:
    """Convert a process number to a safe filename."""
    return process_number.replace(" ", "_").replace("/", "_")
