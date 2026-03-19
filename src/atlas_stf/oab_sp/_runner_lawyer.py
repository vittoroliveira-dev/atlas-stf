"""Orchestrator for OAB/SP lawyer lookup: search inscritos → parse → JSONL."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

from ._checkpoint import load_checkpoint, save_checkpoint
from ._city_extractor import fetch_and_save_cities, load_cities
from ._client import OabSpClient
from ._config import OabSpLawyerLookupConfig
from ._name_extractor import build_lookup_candidates, save_candidates
from ._parser_inscritos import classify_inscritos_response

logger = logging.getLogger(__name__)

# Suppress per-request httpx logging
logging.getLogger("httpx").setLevel(logging.WARNING)


def _load_sp_lawyers_by_oab(curated_dir: Path) -> list[dict[str, Any]]:
    """Load lawyer_entity records with oab_state=SP."""
    path = curated_dir / "lawyer_entity.jsonl"
    if not path.exists():
        return []
    results: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            if record.get("oab_state") == "SP" and record.get("oab_number"):
                results.append(record)
    logger.info("Found %d lawyers with oab_state=SP", len(results))
    return results


def _safe_filename_part(value: str) -> str:
    """Sanitize a value for safe use as a filename component."""
    return re.sub(r"[^\w.\-]", "_", value.replace("..", "_"))


def _save_failed_html(output_dir: Path, key: str, phase: str, attempt: int, html: str) -> None:
    """Save problematic HTML for diagnostic purposes."""
    fail_dir = output_dir / "_failed_html_lawyer"
    fail_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{_safe_filename_part(key)}.{phase}.attempt-{attempt}.html"
    (fail_dir / filename).write_text(html, encoding="utf-8")


def run_lawyer_lookup(
    config: OabSpLawyerLookupConfig,
    *,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> int:
    """Main lookup loop. Returns count of completed lookups.

    Phase B1: Lookup by OAB registration number (18 lawyers)
    Phase B2: Lookup by name from individual firm candidates
    """
    # Phase A: Ensure cities are extracted
    cities = load_cities(config.output_dir)
    if not cities:
        cities = fetch_and_save_cities(config.output_dir)

    # Phase B1: Lawyers with OAB SP number
    sp_lawyers = _load_sp_lawyers_by_oab(config.curated_dir)

    # Phase B2: Candidates from individual firms
    candidates = build_lookup_candidates(config.output_dir, cities)
    if not candidates:
        # Try to build candidates if sociedade_detalhe exists
        logger.info("No pre-built candidates found, building from sociedade_detalhe...")
        candidates = build_lookup_candidates(config.output_dir, cities)

    # Save candidates for reference
    if candidates:
        save_candidates(candidates, config.output_dir)

    # Build unified lookup list with unique keys
    lookups: list[dict[str, Any]] = []

    for lawyer in sp_lawyers:
        lookups.append(
            {
                "key": f"oab_{lawyer['oab_number']}",
                "method": "registration",
                "oab_number": lawyer["oab_number"],
                "lawyer_id": lawyer.get("lawyer_id"),
            }
        )

    seen_names: set[str] = set()
    for candidate in candidates:
        name_key = f"name_{candidate['registration_number']}_{candidate['lawyer_name']}"
        if name_key in seen_names:
            continue
        seen_names.add(name_key)
        lookups.append(
            {
                "key": name_key,
                "method": "name",
                "lawyer_name": candidate["lawyer_name"],
                "city_id": candidate["city_id"],
                "registration_number": candidate["registration_number"],
            }
        )

    total = len(lookups)
    checkpoint = load_checkpoint(config.checkpoint_file)

    pending = [
        item
        for item in lookups
        if not checkpoint.is_resolved(item["key"]) or checkpoint.is_retryable(item["key"], config.max_retries)
    ]

    logger.info(
        "OAB/SP lawyer lookup: %d total (%d by registration, %d by name), %d resolved, %d pending",
        total,
        len(sp_lawyers),
        len(lookups) - len(sp_lawyers),
        total - len(pending),
        len(pending),
    )

    if config.dry_run:
        stats = checkpoint.stats
        logger.info("[DRY] Stats: %s", ", ".join(f"{k}={v}" for k, v in sorted(stats.items())))
        logger.info("[DRY] Would process %d lookups", len(pending))
        return 0

    output_path = config.output_dir / "advogado_consulta.jsonl"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    completed_count = 0

    with OabSpClient(
        timeout=config.timeout_seconds,
        rate_limit=config.rate_limit_seconds,
        max_retries=config.max_retries,
        retry_delay=config.retry_delay_seconds,
    ) as client:
        for i, item in enumerate(pending):
            key = item["key"]
            if on_progress:
                on_progress(i, len(pending), f"OAB/SP Lawyer: {key[:30]}")

            attempt = checkpoint.retry_count(key) + 1

            try:
                if item["method"] == "registration":
                    html = client.search_inscrito(registration_number=item["oab_number"])
                else:
                    html = client.search_inscrito(
                        name=item["lawyer_name"],
                        city_id=item.get("city_id", "0"),
                    )
            except RuntimeError:
                logger.warning("Lookup failed for key=%s after retries", key)
                checkpoint.mark_failed(key)
                save_checkpoint(checkpoint, config.checkpoint_file)
                continue

            status, records = classify_inscritos_response(html)

            if status == "not_found":
                checkpoint.mark_not_found(key)
                save_checkpoint(checkpoint, config.checkpoint_file)
                logger.debug("Not found: key=%s", key)
                continue

            if status == "unexpected":
                _save_failed_html(config.output_dir, key, "search", attempt, html)
                checkpoint.mark_failed(key)
                save_checkpoint(checkpoint, config.checkpoint_file)
                logger.warning("Unexpected response for key=%s — HTML saved", key)
                continue

            if status == "multi_match":
                # Cardinality > 1 is a permanent condition, not transient
                checkpoint.mark_not_found(key)
                save_checkpoint(checkpoint, config.checkpoint_file)
                logger.debug("Multi match (%d results) for key=%s — rejected", len(records), key)
                continue

            # single_match
            record = records[0]
            record["lookup_key"] = key
            record["lookup_method"] = item["method"]
            if item["method"] == "name":
                record["source_registration_number"] = item.get("registration_number")

            with output_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, ensure_ascii=False))
                fh.write("\n")

            checkpoint.mark_completed(key)
            save_checkpoint(checkpoint, config.checkpoint_file)
            completed_count += 1

            if completed_count % 100 == 0:
                logger.info("Progress: %d completed so far", completed_count)

    promoted = checkpoint.promote_exhausted(config.max_retries)
    if promoted:
        logger.info("Promoted %d failed entries to exhausted", promoted)

    save_checkpoint(checkpoint, config.checkpoint_file)

    stats = checkpoint.stats
    logger.info(
        "OAB/SP lawyer lookup complete:\n  total: %d | completed: %d | not_found: %d | exhausted: %d | failed: %d",
        total,
        stats["completed"],
        stats["not_found"],
        stats["exhausted"],
        stats["failed"],
    )

    if on_progress:
        on_progress(len(pending), len(pending), "OAB/SP Lawyer: Concluído")

    return completed_count
